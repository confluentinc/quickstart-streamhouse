#!/usr/bin/env python3
"""
Deploy the Streamhouse labs to Confluent Cloud: Flight Disruptions, Agentic Pit Decisions, or both.

Both labs share one environment, cluster, and Flink pool (terraform/core) and add their own
Terraform root on top. The choice is asked once and saved to credentials.env (DEPLOY_LABS).

Agentic Pit Decisions (terraform/agentic-pit-decisions): the race tables and Claude hosted in
Confluent, then the Global API key, the lab's credential card, and the driver_race_history seed.

Flight Disruptions runs, in order:
  1. terraform/core        — environment, cluster, Flink pool, service accounts,
                             API keys, RTCE reader.
  2. terraform/airline-demo — the River Air Flink pipeline (source tables,
                             passenger_state, flight_impact) and, with AWS
                             creds, Tableflow to Iceberg in S3 with AWS Glue.
  3. The data generator (same data as `uv run airport-datagen`): 30 days of
     history and today's flights, then a 90-minute live stream in the background.
  4. Lightning Tables and RTCE on the demo topics, plus the RTCE MCP server for
     the coding agent chosen up front (same as `uv run setup-rtce`).

Both labs deploy their attendee starting point by default and prebuild the rest with
--with-labs. Flight Disruptions' starting point is the workshop: steps 1 and 2 build only
the three source feeds, passenger_recommendations, and the agent's hosted model, and deploy
stops after the Global API key. Attendees publish the data, build passenger_state, flight_impact,
and the recovery agent, and turn on Lightning Tables and RTCE themselves
(docs/flight-disruptions/workshop.md). Steps 1 to 4 above are the full demo, which
--with-labs, --video-2, and the bots deploy. The choice is saved to credentials.env, so
later runs stay in workshop mode, and a checkout that already has the full demo keeps it.

AWS-only by design (RTCE + Flink Native Inference are AWS-only), so there is no
cloud or region choice. Resource names use TF_VAR_resource_prefix from
credentials.env, or DEFAULT_PREFIX when unset (saved on first run so later
runs keep the same names), plus a random suffix. For a recorded demo, set
TF_VAR_deployment_name=RIVER-AIR-PROD instead to get fixed names such as
RIVER-AIR-PROD-ENV and RIVER-AIR-PROD-CLUSTER.

Usage:
  uv run deploy               # interactive; the only command a person needs
  uv run deploy --lab agentic-pit-decisions   # skip the lab question
  uv run deploy --with-labs   # presenters: prebuild what attendees write (the full Flight Disruptions demo)
  uv run deploy --workshop    # the Flight Disruptions workshop without the lab question
  uv run deploy --automated   # bots: non-interactive from credentials.env, registers Claude Code
  uv run deploy --testing     # bots: non-interactive from credentials.env, no MCP registration
  uv run deploy --always-on   # operators: the full demo running around the clock on AWS (Lambda + ECS)
  uv run deploy --always-on --reset-data   # same, after tombstoning every generated key
"""

import argparse
import json
import os
import shutil
import subprocess
import sys
import time
from datetime import timedelta

from dotenv import dotenv_values, set_key

from scripts import setup_rtce
from scripts.credentials import (
    generate_confluent_api_keys,
    load_or_create_credentials_file,
)
from scripts.login_checks import ensure_confluent_login, prompt_and_save_login
from scripts.terraform import get_project_root, run_terraform_output
from scripts.terraform_runner import run_terraform
from scripts.tfvars import write_tfvars_for_deployment
from scripts.ui import prompt_choice, prompt_with_default, prompt_yes_no

DEFAULT_REGION = "us-east-1"  # RTCE-supported; Confluent-hosted Claude available here
DEFAULT_PREFIX = "streamhouse"
# Lab id -> (display name, Terraform root). Both build on terraform/core.
LABS = {
    "flight-disruptions": ("Flight Disruptions", "airline-demo"),
    "agentic-pit-decisions": ("Agentic Pit Decisions", "agentic-pit-decisions"),
}
FLIGHT_DISRUPTIONS, PIT_DECISIONS = LABS
DEPLOY_LABS = "DEPLOY_LABS"  # credentials.env: the labs the last deploy chose, comma-separated
WORKSHOP = "TF_VAR_workshop"  # credentials.env flag that keeps a checkout in workshop mode
# The recovery agent's statements in terraform/airline-demo, applied once RTCE is up.
AGENT_TARGETS = (
    "confluent_flink_statement.agent_setup",
    "confluent_flink_statement.passenger_state_changes",
    "confluent_flink_statement.impacted_passengers",
    "confluent_flink_statement.recovery_agent",
    "confluent_flink_statement.recovery_offers",
    # Conditional (count) outside the workshop or on a Dedicated cluster, so Terraform moves them
    # to [0] in state and rejects a targeted plan that leaves them out.
    "confluent_flink_statement.passenger_state",
    "confluent_flink_statement.flight_impact",
    "confluent_api_key.hotel_webhook",
    "confluent_connector.hotel_webhook",
    "confluent_flink_statement.hotel_inventory_feed",
)
# --video-2 stops short of what Demo 2 creates on screen (the model, tool, agent, and INSERT): only the
# two staging tables the INSERT reads.
VIDEO_2_TARGETS = (
    "confluent_flink_statement.passenger_state_changes",
    "confluent_flink_statement.impacted_passengers",
)

# --always-on: the full demo running around the clock on AWS (terraform/always-on). Saved to credentials.env,
# so later plain `uv run deploy` runs keep it; `uv run destroy` clears it.
ALWAYS_ON = "TF_VAR_always_on"
CONTINUOUS = "TF_VAR_continuous"  # terraform/airline-demo's var.continuous
ALWAYS_ON_NAME = "river-air-demo"  # TF_VAR_deployment_name when none is saved
DEMO_USER_EMAILS = "DEMO_USER_EMAILS"  # credentials.env: the demo users (invited by hand), comma-separated
DEMO_USER_ROLE = "EnvironmentAdmin"
ALWAYS_ON_CARD = "tmp/always-on-card.env"
# Compacted, so a source feed keeps each flight's, passenger's, and hotel's latest row however long it runs.
COMPACTED_TOPICS = ("flight_status", "passenger_itineraries", "hotel_inventory")
# terraform/always-on creates the Lambda and ECS roles, so the operator must be able to.
ALWAYS_ON_IAM_ACTIONS = (
    "iam:CreateRole",
    "iam:PutRolePolicy",
    "iam:AttachRolePolicy",
    "iam:CreatePolicy",
    "iam:PassRole",
)

# Module-level buffer for Plan B fallback when set_key() fails (e.g. Windows locks).
_pending_writes: dict = {}
_write_failed: bool = False


def _detect_ambient_aws_credentials() -> tuple[str, str, str] | None:
    """Resolve AWS credentials via boto3's standard chain (env vars, `aws sso
    login` cache, shared config/profile, IMDS, ...). Returns
    (access_key, secret_key, session_token) — session_token is "" when the
    credentials aren't temporary — or None if nothing resolves.

    Preferred over asking the user to paste one in: a temporary session
    token is a long (~300-400 char) string, and pasting that into a plain
    terminal prompt can freeze some terminals. Most people running this
    already have valid AWS credentials in their shell (SSO login, env vars,
    a profile), so this picks those up directly instead.
    """
    try:
        import boto3

        resolved = boto3.Session().get_credentials()
        if resolved is None:
            return None
        frozen = resolved.get_frozen_credentials()
        if not frozen.access_key or not frozen.secret_key:
            return None
        return frozen.access_key, frozen.secret_key, frozen.token or ""
    except Exception:
        return None


def _save_env_safe(creds_file, key: str, value: str) -> None:
    """Write a key to credentials.env with retry + in-memory fallback."""
    global _write_failed
    if _write_failed:
        _pending_writes[key] = value
        return
    for attempt in range(3):
        result = set_key(str(creds_file), key, value)
        if result[0] is not None:
            check = dotenv_values(str(creds_file))
            if check.get(key) == value:
                return
        if attempt < 2:
            time.sleep(0.3 * (attempt + 1))
    _write_failed = True
    _pending_writes[key] = value


def _flush_pending_writes(creds_file) -> None:
    """Bulk-write any buffered credentials at the end of the flow (Plan B)."""
    if not _pending_writes:
        return
    try:
        existing = dotenv_values(str(creds_file)) if creds_file.exists() else {}
        existing.update(_pending_writes)
        with open(creds_file, "w", encoding="utf-8", newline="\n") as f:
            for k, v in existing.items():
                f.write(f"{k}='{v}'\n")
        check = dotenv_values(str(creds_file))
        for k, v in _pending_writes.items():
            if check.get(k) != v:
                raise ValueError(f"Read-back verification failed for {k}")
    except Exception as e:
        print(f"\n✗ Could not write credentials.env: {e}", file=sys.stderr)
        sys.exit(1)


def _deploy(root, targets, video_2: bool = False) -> None:
    print("\n=== Starting Deployment ===")
    # A redeploy keeps the recovery agent when its RTCE connection already exists.
    # --video-2 never does: Demo 2 creates the agent on screen.
    infra = setup_rtce.core_infra(root / "credentials.env") if "airline-demo" in targets else None
    if not video_2 and infra and setup_rtce.agent_connection_exists(infra):
        os.environ["TF_VAR_enable_recovery_agent"] = "true"
    for env in targets:
        env_path = root / "terraform" / env
        if not env_path.exists():
            print(f"Warning: {env_path} does not exist, skipping.")
            continue
        if not run_terraform(env_path):
            print(f"\nDeployment failed at {env}. Stopping.")
            sys.exit(1)
    print("\n✓ All deployments completed successfully!")


def _workshop_mode(requested: bool, full: bool, creds_file, creds: dict, default: bool = True) -> bool:
    """True for a Flight Disruptions workshop deploy; saved so later runs stay in it.

    The workshop is the default (bots pass default=False), --workshop asks for it, and full (--with-labs or
    --video-2) asks for the full demo. A checkout can't switch either way: a workshop apply destroys what the
    full one built, and a full apply collides with the tables attendees created. `uv run destroy` clears it.
    """
    if creds.get(WORKSHOP) == "true":
        if full:
            sys.exit(
                "This checkout has the workshop, where attendees build the pipeline themselves. "
                "Run `uv run destroy` first, or deploy the full demo from another clone."
            )
        return True
    if (creds_file.parent / "terraform" / "airline-demo" / "terraform.tfstate").exists():
        if requested:
            sys.exit(
                "This checkout already has a full deployment, which --workshop would partly destroy. "
                "Run `uv run destroy` first, or deploy the workshop from another clone."
            )
        return False
    if full or not (requested or default):
        return False
    _save_env_safe(creds_file, WORKSHOP, "true")
    creds[WORKSHOP] = "true"
    return True


def _workshop_saved(args, creds: dict) -> bool:
    """--workshop, now or on an earlier run: deploy only Flight Disruptions without asking which lab."""
    return args.workshop or creds.get(WORKSHOP) == "true"


def _always_on_mode(args, creds_file, creds: dict) -> bool:
    """True for --always-on, now or on an earlier run. It implies the full Flight Disruptions demo (--with-labs
    --lab flight-disruptions). Nothing is saved here: _save_always_on runs once the deploy goes ahead."""
    if not (args.always_on or creds.get(ALWAYS_ON) == "true"):
        if args.reset_data:
            sys.exit("--reset-data only applies with --always-on.")
        return False
    if args.workshop or args.video_2:
        sys.exit("This checkout runs the always-on demo, the full demo; drop --workshop and --video-2.")
    if creds.get(WORKSHOP) == "true":
        sys.exit(
            "This checkout has the workshop, which --always-on would partly destroy. "
            "Run `uv run destroy` first, or deploy the always-on demo from another clone."
        )
    if args.lab not in (None, FLIGHT_DISRUPTIONS):
        sys.exit("--always-on deploys the Flight Disruptions lab; drop --lab or pick flight-disruptions.")
    args.with_labs, args.lab = True, FLIGHT_DISRUPTIONS
    return True


def _save_always_on(creds_file, creds: dict) -> None:
    """Save the mode, var.continuous, and a fixed deployment name, so later plain runs stay always-on."""
    for key in (ALWAYS_ON, CONTINUOUS):
        _save_env_safe(creds_file, key, "true")
        creds[key] = "true"
    if not creds.get("TF_VAR_deployment_name"):
        if (creds_file.parent / "terraform" / "core" / "terraform.tfstate").exists():
            # A new name would rename (and partly replace) what core already built.
            print("Always-on: keeping this deployment's current names (TF_VAR_deployment_name is unset).")
        else:
            _save_env_safe(creds_file, "TF_VAR_deployment_name", ALWAYS_ON_NAME)
            creds["TF_VAR_deployment_name"] = ALWAYS_ON_NAME
    _flush_pending_writes(creds_file)


def _demo_emails(creds: dict) -> list[str]:
    return [email.strip() for email in (creds.get(DEMO_USER_EMAILS) or "").split(",") if email.strip()]


def _ask_demo_emails(creds_file, creds: dict) -> None:
    """Ask once for the demo users' emails; they were invited to the organization by hand."""
    if creds.get(DEMO_USER_EMAILS):
        return
    emails = prompt_with_default(
        "Demo users' emails, comma-separated (already invited; they get EnvironmentAdmin)", "", required=False
    )
    if emails:
        _save_env_safe(creds_file, DEMO_USER_EMAILS, emails)
        creds[DEMO_USER_EMAILS] = emails


def _targets(labs: list[str]) -> list[str]:
    return ["core", *(LABS[lab][1] for lab in labs)]


def _select_labs(requested: str | None, creds_file, creds: dict, workshop: bool, interactive: bool) -> list[str]:
    """The labs to deploy: --lab, else asked (interactive), else the saved choice. Saved for the next run.

    The workshop is Flight Disruptions', so it never asks; it keeps Pit Decisions when the last run picked
    both. Bots with nothing saved get Flight Disruptions,
    as before this repo had a second lab.
    """
    saved = [lab for lab in (creds.get(DEPLOY_LABS) or "").split(",") if lab in LABS]
    if requested:
        labs = list(LABS) if requested == "both" else [requested]
    elif workshop:
        labs = saved if FLIGHT_DISRUPTIONS in saved else [FLIGHT_DISRUPTIONS]
    elif not interactive:
        labs = saved or [FLIGHT_DISRUPTIONS]
    else:
        options = [*(name for name, _ in LABS.values()), "Both"]
        default = options[-1] if not saved or len(saved) == len(LABS) else LABS[saved[0]][0]
        last_time = f" (last time: {default})" if saved else ""
        choice = prompt_choice(f"Which lab do you want to deploy?{last_time}", options)
        labs = list(LABS) if choice == "Both" else [lab for lab, (name, _) in LABS.items() if name == choice]
    if workshop and FLIGHT_DISRUPTIONS not in labs:
        sys.exit("--workshop deploys the Flight Disruptions lab; drop --lab or pick flight-disruptions.")
    _save_env_safe(creds_file, DEPLOY_LABS, ",".join(labs))
    creds[DEPLOY_LABS] = ",".join(labs)
    return labs


def main():
    parser = argparse.ArgumentParser(description="Deploy the Streamhouse labs")
    parser.add_argument(
        "--lab",
        choices=(*LABS, "both"),
        help="Which lab to deploy (default: ask, then remember the answer in credentials.env)",
    )
    parser.add_argument(
        "--with-labs",
        action="store_true",
        help="Prebuild what attendees write, for a demo: Flight Disruptions' full pipeline, stream, agent, and RTCE; "
        "Agentic Pit Decisions' Steps 1 and 3 (car_state, the agent, pit_decisions)",
    )
    parser.add_argument(
        "--workshop",
        action="store_true",
        help="Deploy only the Flight Disruptions workshop, skipping the lab question",
    )
    parser.add_argument(
        "--automated",
        action="store_true",
        help="Bots: load credentials.env, skip prompts, register the RTCE MCP server with Claude Code",
    )
    parser.add_argument(
        "--testing",
        action="store_true",
        help="Bots: load credentials.env, skip prompts, no MCP registration",
    )
    # Not advertised: for recording, deploys everything fresh up to where Demo 2's statements run live.
    parser.add_argument("--video-2", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--speed", type=float, default=1.0, help=argparse.SUPPRESS)
    parser.add_argument(
        "--always-on",
        action="store_true",
        help="Operators: run the full Flight Disruptions demo around the clock on AWS (saved for later runs)",
    )
    parser.add_argument(
        "--reset-data",
        action="store_true",
        help="With --always-on: tombstone every generated key before publishing the data again",
    )
    args = parser.parse_args()
    if args.always_on and (args.workshop or args.video_2):
        parser.error("--always-on deploys the full demo; drop --workshop and --video-2")
    if args.automated and args.testing:
        parser.error("--automated and --testing are mutually exclusive")
    if args.workshop and (args.video_2 or args.with_labs):
        parser.error("--workshop deploys the starting point; drop --with-labs and --video-2")
    if args.speed <= 0:
        parser.error("--speed must be greater than 0")

    print("=== Streamhouse Labs — Deploy ===\n")
    root = get_project_root()
    print(f"Project root: {root}")

    # ---- Non-interactive -------------------------------------------------
    if args.automated or args.testing:
        creds_file = root / "credentials.env"
        if not creds_file.exists():
            print("Error: credentials.env not found. Run `uv run deploy` interactively first.")
            sys.exit(1)
        creds = dotenv_values(str(creds_file))

        missing = [
            label
            for key, label in {
                "TF_VAR_confluent_cloud_api_key": "Confluent Cloud API Key",
                "TF_VAR_confluent_cloud_api_secret": "Confluent Cloud API Secret",
            }.items()
            if not (creds.get(key) or "").strip()
        ]
        if missing:
            print("Error: credentials.env is missing:")
            for m in missing:
                print(f"  - {m}")
            sys.exit(1)

        region = creds.get("TF_VAR_cloud_region", DEFAULT_REGION) or DEFAULT_REGION
        prefix = creds.get("TF_VAR_resource_prefix", "") or ""
        always_on = _always_on_mode(args, creds_file, creds)
        labs = _select_labs(args.lab, creds_file, creds, _workshop_saved(args, creds), interactive=False)
        workshop = FLIGHT_DISRUPTIONS in labs and _workshop_mode(
            args.workshop, args.with_labs or args.video_2, creds_file, creds, default=False
        )
        if always_on:
            _save_always_on(creds_file, creds)
        _flush_pending_writes(creds_file)
        if always_on:
            _export_aws_credentials(creds)
            _preflight_always_on(creds)
        ensure_confluent_login(creds)
        print(
            f"✓ Credentials loaded. Region: {region}. Deploying: {', '.join(_targets(labs))}"
            f"{' (workshop)' if workshop else ''}\n"
        )
        write_tfvars_for_deployment(root, region, creds, resource_prefix=prefix)
        for key, value in creds.items():
            if value:
                os.environ[key] = value
        _deploy(root, _targets(labs), args.video_2)
        if always_on:
            _finish_always_on(root, args.reset_data)
            return
        if PIT_DECISIONS in labs:
            _finish_pit_decisions(root, args.with_labs)
        if workshop:
            _finish_workshop(root)
        elif FLIGHT_DISRUPTIONS in labs:
            _finish(root, "claude" if args.automated else "none", args.video_2, args.speed)
        return

    # ---- Interactive -----------------------------------------------------
    creds_file, creds = load_or_create_credentials_file(root)
    always_on = _always_on_mode(args, creds_file, creds)
    labs = _select_labs(args.lab, creds_file, creds, _workshop_saved(args, creds), interactive=True)
    flights = FLIGHT_DISRUPTIONS in labs
    workshop = flights and _workshop_mode(args.workshop, args.with_labs or args.video_2, creds_file, creds)
    if always_on:
        print("\nAlways-on: deploys the full demo, then runs the flight board and the generator on AWS.")
    elif workshop:
        print("\nWorkshop mode: deploys the source feeds only; docs/flight-disruptions/workshop.md builds the rest.")
    elif flights:
        print("\nFull demo: deploys the whole Flight Disruptions pipeline, stream, and recovery agent.")

    # Confluent Cloud login (saved for auto-login). A saved login that stopped working is asked for again.
    prompted = False
    if not (creds.get("CONFLUENT_EMAIL") and creds.get("CONFLUENT_PASSWORD")):
        prompt_and_save_login(creds_file, creds)
        prompted = True
    ensure_confluent_login(creds, creds_file, interactive=not prompted)
    print("✓ Confluent CLI logged in")

    # No prompt: us-east-1 unless the user set TF_VAR_cloud_region in credentials.env.
    region = creds.get("TF_VAR_cloud_region") or DEFAULT_REGION
    print(f"\nRegion: {region} (AWS-only; RTCE + Confluent-hosted Claude supported here)")

    # No prompt: reuse the saved prefix, else the default. Persisting it keeps
    # resource names stable if DEFAULT_PREFIX changes later.
    prefix = creds.get("TF_VAR_resource_prefix", "") or DEFAULT_PREFIX
    _save_env_safe(creds_file, "TF_VAR_resource_prefix", prefix)
    if creds.get("TF_VAR_deployment_name"):
        print(f"Deployment name: {creds['TF_VAR_deployment_name'].upper()} (fixed, from TF_VAR_deployment_name)")
    else:
        print(f"Resource name prefix: {prefix} (set TF_VAR_resource_prefix in credentials.env to change)")

    # Optionally generate fresh Confluent Cloud API keys; new keys are used as-is, without asking for them again.
    print()
    api_key = api_secret = None
    if prompt_yes_no("Generate new Confluent Cloud API keys?", default=False):
        api_key, api_secret = generate_confluent_api_keys(prefix)
        if not (api_key and api_secret):
            print("✗ Could not generate API keys; enter an existing OrganizationAdmin key below.")

    print("\n--- Credential Configuration ---")
    if api_key and api_secret:
        print(f"Using the new Confluent Cloud API key {api_key}")
    else:
        api_key = prompt_with_default("Confluent Cloud API Key", creds.get("TF_VAR_confluent_cloud_api_key", ""))
        api_secret = prompt_with_default(
            "Confluent Cloud API Secret", creds.get("TF_VAR_confluent_cloud_api_secret", ""), secret=True
        )
    _save_env_safe(creds_file, "TF_VAR_confluent_cloud_api_key", api_key)
    _save_env_safe(creds_file, "TF_VAR_confluent_cloud_api_secret", api_secret)

    # Both labs' agents run on a Confluent-hosted model; only Flight Disruptions has a Tableflow scene.
    if flights and not workshop:
        _prompt_aws_credentials(creds_file, creds)
    if always_on:
        _ask_demo_emails(creds_file, creds)

    _flush_pending_writes(creds_file)

    final_creds = dotenv_values(creds_file)
    if always_on:
        _export_aws_credentials(final_creds)
        _preflight_always_on(final_creds)
    print("\n--- Configuration Summary ---")
    print(f"Resource name prefix: {prefix}")
    print(f"Region: {region}")
    api_key_state = "set" if final_creds.get("TF_VAR_confluent_cloud_api_key") else "MISSING"
    print(f"Confluent Cloud API key: {api_key_state}")
    print(f"Confluent CLI auto-login: {'saved' if final_creds.get('CONFLUENT_EMAIL') else 'not saved'}")
    print(f"Labs: {', '.join(LABS[lab][0] for lab in labs)}")
    if PIT_DECISIONS in labs:
        print("Pit strategy agent: Claude hosted in Confluent (no AWS credentials needed)")
    if not flights:
        print(f"Deploying: {', '.join(_targets(labs))}, then the lab's credential card and race history")
    elif workshop:
        print("Workshop: source feeds only; you build the Flink tables and the agent, and turn on RTCE")
        print(f"Deploying: {', '.join(_targets(labs))}")
    else:
        print("Recovery agent: Claude hosted in Confluent (no AWS credentials needed)")
        analytics_state = (
            "ENABLED" if final_creds.get("TF_VAR_aws_tableflow_access_key") else "skipped (no Tableflow AWS creds)"
        )
        print(f"Tableflow S3/Glue analytics (Demo 3): {analytics_state}")
        print(f"Deploying: {', '.join(_targets(labs))}, then demo data and RTCE")
    if always_on:
        print(f"Always-on: then terraform/always-on, and {DEMO_USER_ROLE} for {len(_demo_emails(final_creds))} users")

    client = "none" if workshop or not flights or always_on else setup_rtce._pick_client()

    print()
    if not prompt_yes_no("Ready to deploy?"):
        print("Deployment cancelled.")
        sys.exit(0)
    if always_on:
        _save_always_on(creds_file, final_creds)

    write_tfvars_for_deployment(root, region, final_creds, resource_prefix=prefix)
    for key, value in final_creds.items():
        if value:
            os.environ[key] = value
    _deploy(root, _targets(labs), args.video_2)
    if always_on:
        _finish_always_on(root, args.reset_data)
        return
    if PIT_DECISIONS in labs:
        _finish_pit_decisions(root, args.with_labs)
    if workshop:
        _finish_workshop(root)
        return
    if not flights:
        return
    _finish(root, client, args.video_2, args.speed)
    print()
    if prompt_yes_no("Start the app at http://127.0.0.1:8000 now?", default=True):
        from scripts.airport_app import main as run_app

        run_app([])
    else:
        print("Start it later with: uv run airport-app")


def _prompt_aws_credentials(creds_file, creds: dict) -> None:
    """Ask for the optional AWS credentials and save them to credentials.env.

    AWS credentials power the Tableflow S3/Glue analytics path (Demo 3) and are OPTIONAL.
    Try the ambient AWS credential chain first: pasting a long temporary
    session token into a plain terminal prompt can freeze some terminals,
    and most people running this already have valid AWS creds in their
    shell (`aws sso login`, env vars, a profile).
    """
    print(
        "\nAWS credentials power the Tableflow S3/Glue analytics path (Demo 3)."
        "\nThey're optional — without them, everything else still deploys, the"
        "\nrecovery agent included."
    )
    access_key = secret_key = token = ""
    ambient = _detect_ambient_aws_credentials()
    if ambient:
        detected_key, detected_secret, detected_token = ambient
        masked = f"{detected_key[:4]}...{detected_key[-4:]}" if len(detected_key) > 8 else detected_key
        if prompt_yes_no(f"Detected AWS credentials in your environment ({masked}). Use these?", default=True):
            access_key, secret_key, token = detected_key, detected_secret, detected_token

    if not access_key:
        access_key = prompt_with_default(
            "AWS Access Key (optional)",
            creds.get("TF_VAR_aws_tableflow_access_key", ""),
            required=False,
        )
        if access_key:
            secret_key = prompt_with_default(
                "AWS Secret Key", creds.get("TF_VAR_aws_tableflow_secret_key", ""), secret=True
            )
            if access_key.startswith("ASIA"):
                print(
                    "Temporary credentials need a session token too — that's a long string"
                    "\n(300-400 chars) that can freeze some terminals on paste. If it does,"
                    "\nCtrl-C and re-run after `export AWS_SESSION_TOKEN=...` (and"
                    "\nAWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY) in your shell, so this can"
                    "\nauto-detect it instead of needing it pasted here."
                )
                token = prompt_with_default(
                    "AWS Session Token (temp creds)", creds.get("TF_VAR_aws_tableflow_session_token", ""), secret=True
                )

    if access_key and secret_key:
        _save_env_safe(creds_file, "TF_VAR_aws_tableflow_access_key", access_key)
        _save_env_safe(creds_file, "TF_VAR_aws_tableflow_secret_key", secret_key)
        if token:
            _save_env_safe(creds_file, "TF_VAR_aws_tableflow_session_token", token)


def _finish(root, client: str, video_2: bool = False, speed: float = 1.0) -> None:
    """Publish the demo data, start the live stream, enable Lightning Tables/RTCE, then the agent.

    With video_2, the agent stays unstarted and the stream starts last, so Demo 2's clock begins
    when everything is ready (as `uv run reset-demo-2` leaves it).
    """
    print("\n=== Publishing demo data ===")
    _run_datagen(root, agent=True, speed=speed, stream=not video_2)
    print("\n=== Enabling Lightning Tables and RTCE ===")
    setup_rtce.main(["--client", client])
    if video_2:
        print("\n=== Preparing Demo 2 ===")
        if _start_agent(root, VIDEO_2_TARGETS):
            clock = _run_datagen(root, agent=True, history=False, speed=speed)
            print_video_2_clock(clock, speed)
    else:
        print("\n=== Starting the recovery agent ===")
        _start_agent(root)
    _print_env_name(root)


def _finish_workshop(root) -> None:
    """Stop where docs/flight-disruptions/workshop.md begins, adding only the Global API key for Lightning reads.

    Attendees publish the data, build passenger_state, flight_impact, and the recovery agent, and turn on
    Lightning Tables and RTCE in the labs.
    """
    print("\n=== Creating the Lightning and RTCE API key ===")
    creds_file = root / "credentials.env"
    if not setup_rtce.ensure_global_key(creds_file, dotenv_values(str(creds_file))):
        print("⚠ No Global API key yet. Rerun `uv run deploy` to try again.")
    _print_env_name(root)
    print('\n✓ Your workshop environment is ready. Next: "Start the datagen" in docs/flight-disruptions/workshop.md.')


def _finish_pit_decisions(root, with_labs: bool = False) -> bool:
    """Write the Agentic Pit Decisions credential card and seed driver_race_history.

    The card carries core's keys plus the Global API key RTCE queries need, so `uv run f1-race`,
    `f1-pitwall`, and `f1-sql` need no flags.
    """
    from scripts.pit_decisions import card
    from scripts.pit_decisions.labs import create_lab_objects
    from scripts.pit_decisions.seed import ensure_driver_race_history

    print("\n=== Agentic Pit Decisions: credential card and race history ===")
    creds_file = root / "credentials.env"
    creds = dotenv_values(str(creds_file))
    if not setup_rtce.ensure_global_key(creds_file, creds):
        print("⚠ No Global API key yet, so RTCE queries won't work. Rerun `uv run deploy` to try again.")
    creds = dotenv_values(str(creds_file))
    previous = card.card_path(root)
    seconds = dotenv_values(previous).get("F1_SECONDS_PER_LAP") if previous.exists() else None
    fields = card.card_fields(
        run_terraform_output(root / "terraform" / "core" / "terraform.tfstate"),
        creds.get("CONFLUENT_RTCE_API_KEY") or "",
        creds.get("CONFLUENT_RTCE_API_SECRET") or "",
        int(seconds or card.DEFAULT_SECONDS_PER_LAP),
    )
    path = card.write_card(root, fields)
    print(f"✓ Wrote the credential card {path.relative_to(root)}")
    lab_card = {f"F1_{key.upper()}": value for key, value in fields.items()}
    ok = ensure_driver_race_history(lab_card, root)
    if ok and with_labs:
        print("\n=== Building Steps 1 and 3 ===")
        ok = create_lab_objects(lab_card, root)
    print(f"""
✓ Agentic Pit Decisions is ready{"" if ok else " (see the warnings above)"}.
  In the SQL workspace, use catalog {fields["catalog"]} and database {fields["database"]}.
  In two terminals:
     uv run f1-race       # {fields["seconds_per_lap"]}s/lap race feed
     uv run f1-pitwall    # dashboard at http://localhost:8001
  Then follow docs/agentic-pit-decisions/walkthrough.md.""")
    return ok


def print_video_2_clock(clock, speed: float) -> None:
    def at(minutes: float) -> str:
        return f"{clock + timedelta(minutes=minutes / speed):%H:%M:%S}"

    print(f"""
✓ Ready for Demo 2. The stream started at {at(0)} UTC:
  {at(6)} to {at(16.5)}  RA417's passengers turn HIGH (record 2.1 here)
  {at(26)} to {at(32)}  RA183's passengers turn HIGH
  {at(40)}           Grand Hyatt sells out (record 2.4 from {at(38)})
  {at(52.5)} to {at(54.5)}  RA185's passengers turn HIGH
Run CREATE MODEL, CREATE TOOL, CREATE AGENT, then the AI_RUN_AGENT INSERT on screen.""")


def _start_agent(root, targets=AGENT_TARGETS) -> bool:
    """Create the agent's RTCE connection, then apply only the given Flink statements."""
    creds_file = root / "credentials.env"
    creds = dotenv_values(str(creds_file))
    infra = setup_rtce.core_infra(creds_file)
    key, secret = creds.get("CONFLUENT_RTCE_API_KEY") or "", creds.get("CONFLUENT_RTCE_API_SECRET") or ""
    started = False
    if infra and key and secret and setup_rtce.create_agent_connection(infra, key, secret):
        print(f"✓ Flink connection '{setup_rtce.AGENT_CONNECTION}' reaches RTCE")
        os.environ["TF_VAR_enable_recovery_agent"] = "true"
        started = run_terraform(root / "terraform" / "airline-demo", targets=targets)
    if not started and creds.get(ALWAYS_ON) == "true":
        # The generator's offers follow the scripted 90-minute plan, which doesn't belong in the always-on topics.
        print("⚠ The recovery agent did not start, so no new offers are written. Rerun `uv run deploy --always-on`.")
    elif not started:
        print(
            "⚠ The recovery agent did not start, so RA417, RA183, and RA185 have no offers yet. "
            "Run `uv run airport-datagen --offers generator` to have the generator write them."
        )
    return started


def _wait_for_demo_schemas(root) -> None:
    """Wait until Terraform's four demo schemas are registered, so the first publish finds them."""
    from confluent_kafka.schema_registry import SchemaRegistryClient

    from .terraform import extract_kafka_credentials

    credentials = extract_kafka_credentials("aws", root)
    registry = SchemaRegistryClient(
        {
            "url": credentials["schema_registry_url"],
            "basic.auth.user.info": (
                f"{credentials['schema_registry_api_key']}:{credentials['schema_registry_api_secret']}"
            ),
        }
    )
    subjects = (
        "flight_status-value",
        "passenger_itineraries-value",
        "hotel_inventory-value",
        "passenger_recommendations-value",
    )
    for attempt in range(30):
        try:
            for subject in subjects:
                registry.get_latest_version(subject)
            break
        except Exception:
            if attempt == 29:
                raise RuntimeError("Demo schemas did not become ready after deployment") from None
            time.sleep(2)


def _run_datagen(root, agent: bool, history: bool = True, speed: float = 1.0, stream: bool = True):
    """Publish history and today's flights, then stream live updates in the background.

    When the recovery agent will run, it writes the agent flights' live offers and the generator skips them.
    Returns the stream start.
    """
    from .airport_datagen import STREAM_MINUTES, _clock, run

    _wait_for_demo_schemas(root)
    start = _clock(None)
    offers = "agent" if agent else "generator"
    run(start, history=history, minutes=0, agent_offers=agent)
    if not stream:
        return start
    log = root / "tmp" / "datagen.log"
    log.parent.mkdir(exist_ok=True)
    with open(log, "a", encoding="utf-8") as out:
        subprocess.Popen(
            [
                "uv",
                "run",
                "airport-datagen",
                "--now",
                start.isoformat(),
                "--stream-only",
                "--offers",
                offers,
                "--speed",
                str(speed),
            ],
            cwd=root,
            stdin=subprocess.DEVNULL,
            stdout=out,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
    print(
        f"✓ Streaming live flight updates for {STREAM_MINUTES} minutes"
        f"{f' at {speed:g}x' if speed != 1 else ''} in the background "
        f"(log: {log.relative_to(root)}; restart with `uv run airport-datagen`)"
    )
    return start


# ---------------------------------------------------------------------------
# --always-on: the flight board on Lambda and the generator on ECS Fargate (terraform/always-on)
# ---------------------------------------------------------------------------


def _export_aws_credentials(creds: dict) -> None:
    """Without ambient AWS credentials, hand the saved Tableflow ones to the aws CLI, docker login, and Terraform."""
    if _detect_ambient_aws_credentials():
        return
    key = (creds.get("TF_VAR_aws_tableflow_access_key") or "").strip()
    secret = (creds.get("TF_VAR_aws_tableflow_secret_key") or "").strip()
    if not (key and secret):
        return
    os.environ["AWS_ACCESS_KEY_ID"], os.environ["AWS_SECRET_ACCESS_KEY"] = key, secret
    token = (creds.get("TF_VAR_aws_tableflow_session_token") or "").strip()
    if token:
        os.environ["AWS_SESSION_TOKEN"] = token
    else:
        os.environ.pop("AWS_SESSION_TOKEN", None)


def _preflight_always_on(creds: dict) -> None:
    """Stop before anything is created when the always-on deploy can't finish on this machine or AWS account."""
    missing = [tool for tool in ("docker", "aws") if not shutil.which(tool)]
    if missing:
        sys.exit(f"--always-on needs {' and '.join(missing)} on your PATH to build and push the demo image.")
    if not (creds.get("TF_VAR_aws_tableflow_access_key") or "").strip():
        sys.exit(
            "--always-on needs AWS credentials (TF_VAR_aws_tableflow_access_key in credentials.env): "
            "it runs the demo on AWS and always deploys Tableflow. Rerun `uv run deploy --always-on` and enter them."
        )
    if subprocess.run(["docker", "info"], capture_output=True, text=True).returncode != 0:
        sys.exit("Docker is installed but not running. Start Docker Desktop and rerun.")
    identity = subprocess.run(
        ["aws", "sts", "get-caller-identity", "--output", "json"], capture_output=True, text=True, timeout=60
    )
    if identity.returncode != 0:
        sys.exit(f"`aws sts get-caller-identity` failed, so the AWS credentials don't work:\n{identity.stderr.strip()}")
    caller = json.loads(identity.stdout or "{}").get("Arn", "")

    # simulate-principal-policy needs the IAM role behind an assumed-role session (SSO roles have a path).
    principal = caller
    if ":assumed-role/" in caller:
        role = caller.split(":assumed-role/", 1)[1].split("/")[0]
        found = subprocess.run(
            ["aws", "iam", "get-role", "--role-name", role, "--query", "Role.Arn", "--output", "text"],
            capture_output=True,
            text=True,
            timeout=60,
        )
        principal = found.stdout.strip() if found.returncode == 0 else ""
    simulated = (
        subprocess.run(
            [
                "aws",
                "iam",
                "simulate-principal-policy",
                "--policy-source-arn",
                principal,
                "--action-names",
                *ALWAYS_ON_IAM_ACTIONS,
                "--output",
                "json",
            ],
            capture_output=True,
            text=True,
            timeout=60,
        )
        if principal
        else None
    )
    if simulated is None or simulated.returncode != 0:
        print(f"⚠ couldn't check {caller or 'the caller'}'s IAM permissions; continuing")
        return
    results = json.loads(simulated.stdout or "{}").get("EvaluationResults", [])
    denied = [r.get("EvalActionName", "") for r in results if r.get("EvalDecision") != "allowed"]
    if denied:
        sys.exit(
            f"{caller} can't {', '.join(denied)}, which terraform/always-on needs to create the Lambda and "
            "ECS roles. Use AWS credentials with IAM permissions."
        )
    print(f"✓ AWS caller {caller} can create the always-on roles")


def _finish_always_on(root, reset_data: bool = False) -> None:
    """Publish the data once from here, then hand the stream to ECS and the flight board to Lambda.

    History goes out only on the first always-on deploy or with --reset-data: on a rerun it would overwrite the
    agent's offers and the customers' answers for D-30..D-2 with the generator's."""
    from .airport_datagen import SEED, _wall_clock, run_continuous

    creds_file = root / "credentials.env"
    # Checked before anything below applies terraform/always-on (the image push creates its state).
    history = reset_data or not (root / "terraform" / "always-on" / "terraform.tfstate").exists()
    print(f"\n=== Publishing {'history and ' if history else ''}the current state ===")
    _wait_for_demo_schemas(root)
    if reset_data:
        _reset_always_on_data(root)
    now = _wall_clock().replace(microsecond=0)
    run_continuous(SEED, now=now, history=history, until=now)  # publishes and returns; ECS streams from here on
    print("\n=== Enabling Lightning Tables and RTCE ===")
    setup_rtce.main(["--client", "none"])
    print("\n=== Starting the recovery agent ===")
    _start_agent(root)
    infra = setup_rtce.core_infra(creds_file)
    if not infra:
        sys.exit("Could not read terraform/core's outputs. Rerun `uv run deploy`.")
    print("\n=== Compacting the source feeds ===")
    _compact_sources(infra)

    creds = dotenv_values(str(creds_file))
    key, secret = creds.get("CONFLUENT_RTCE_API_KEY") or "", creds.get("CONFLUENT_RTCE_API_SECRET") or ""
    if not (key and secret):
        sys.exit("No Global API key for Lightning Tables yet, so the flight board can't read. Rerun `uv run deploy`.")
    os.environ["TF_VAR_rtce_api_key"], os.environ["TF_VAR_rtce_api_secret"] = key, secret
    previous = _always_on_outputs(root).get("image_uri")
    print("\n=== Building and pushing the demo image ===")
    image = _push_image(root, infra["region"])
    os.environ["TF_VAR_image_uri"] = image
    print("\n=== Running the flight board and the generator on AWS ===")
    if not run_terraform(root / "terraform" / "always-on"):
        sys.exit("\nDeployment failed at always-on. Stopping.")
    outputs = _always_on_outputs(root)
    if previous == image:
        # A new image redeploys the generator; a changed secret (rotated key or password) alone doesn't. The flight
        # board needs nothing: a new secret version publishes a new Lambda version and moves its alias.
        _redeploy_generator(outputs, infra["region"])

    print("\n=== Granting the demo users their environment role ===")
    emails = _demo_emails(creds)
    missing = _grant_demo_users(infra["env_id"], emails) if emails else []
    if not emails:
        print(f"⚠ No {DEMO_USER_EMAILS} in credentials.env, so no demo user has a role yet.")
    if missing:
        print(f"⚠ Not in the organization yet (invite them, then rerun): {', '.join(missing)}")
    _write_card(root, outputs, emails)


def _reset_always_on_data(root) -> None:
    """Tombstone every key the scripted runs and the continuous days of the last RETENTION_DAYS could have written."""
    from .airport_datagen import RETENTION_DAYS, SEED, Publisher, _expired_keys, _wall_clock, reset
    from .terraform import extract_kafka_credentials

    print("Writing tombstones for every generated key...")
    now = _wall_clock()
    reset(now, SEED, dry_run=False)
    publisher = Publisher(extract_kafka_credentials("aws", root), False)
    for back in range(-1, RETENTION_DAYS + 1):
        for topic, key in _expired_keys(SEED, now.date() - timedelta(days=back)):
            publisher.publish(topic, key, None)
    publisher.flush()
    print("✓ Tombstoned the continuous days")


def _compact_sources(infra: dict[str, str]) -> None:
    for topic in COMPACTED_TOPICS:
        cmd = [
            "confluent",
            "kafka",
            "topic",
            "update",
            topic,
            "--config",
            "cleanup.policy=compact",
            "--cluster",
            infra["cluster_id"],
            "--environment",
            infra["env_id"],
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        if result.returncode == 0:
            print(f"✓ {topic} is compacted")
        else:
            print(f"⚠ Could not compact {topic}: {result.stderr.strip()}\n  Run it by hand: {' '.join(cmd)}")


def _always_on_outputs(root) -> dict:
    """terraform/always-on's outputs, or {} before it's deployed."""
    state = root / "terraform" / "always-on" / "terraform.tfstate"
    if not state.exists():
        return {}
    try:
        return run_terraform_output(state)
    except Exception:
        return {}


def _image_tag(root) -> str:
    """The commit, with -dirty when the worktree has changes the commit doesn't."""
    commit = subprocess.run(["git", "rev-parse", "--short", "HEAD"], cwd=root, capture_output=True, text=True)
    if commit.returncode != 0:
        return f"local-{time.strftime('%Y%m%d%H%M%S')}"
    status = subprocess.run(["git", "status", "--porcelain"], cwd=root, capture_output=True, text=True)
    return commit.stdout.strip() + ("-dirty" if status.stdout.strip() else "")


def _ecr_repository_url(always_on) -> str:
    """The repository URL from state: a targeted apply doesn't always refresh outputs."""
    shown = subprocess.run(["terraform", "show", "-json"], cwd=always_on, capture_output=True, text=True, check=True)
    resources = json.loads(shown.stdout or "{}").get("values", {}).get("root_module", {}).get("resources", [])
    for resource in resources:
        if resource.get("address") == "aws_ecr_repository.images":
            return resource["values"]["repository_url"]
    sys.exit("terraform/always-on has no ECR repository after its bootstrap apply.")


def _push_image(root, region: str) -> str:
    """Create the ECR repository, then build and push the image tagged with the commit.

    Returns repository@digest, so a -dirty tag pushed again with new code still changes what both services run."""
    always_on = root / "terraform" / "always-on"
    # Every variable needs a value even for a targeted apply; the repository doesn't read image_uri.
    os.environ.setdefault("TF_VAR_image_uri", "bootstrap")
    if not run_terraform(always_on, targets=("aws_ecr_repository.images",)):
        sys.exit("\nCould not create the ECR repository. Stopping.")
    repository = _ecr_repository_url(always_on)
    image = f"{repository}:{_image_tag(root)}"
    try:
        password = subprocess.run(
            ["aws", "ecr", "get-login-password", "--region", region], capture_output=True, text=True, check=True
        ).stdout
        subprocess.run(
            ["docker", "login", "--username", "AWS", "--password-stdin", repository.split("/")[0]],
            input=password,
            capture_output=True,
            text=True,
            check=True,
        )
        # One plain image manifest: Lambda rejects the manifest list that build attestations would add.
        build = [
            "docker",
            "build",
            "--platform",
            "linux/amd64",
            "--provenance=false",
            "--sbom=false",
            "-t",
            image,
            str(root),
        ]
        subprocess.run(build, check=True)
        subprocess.run(["docker", "push", image], check=True)
        digests = subprocess.run(
            ["docker", "image", "inspect", "--format", "{{json .RepoDigests}}", image],
            capture_output=True,
            text=True,
            check=True,
        ).stdout
    except subprocess.CalledProcessError as error:
        sys.exit(f"\n✗ `{' '.join(error.cmd[:3])}` failed{f': {error.stderr.strip()}' if error.stderr else ''}")
    pinned = next((d for d in json.loads(digests or "[]") if d.startswith(f"{repository}@")), None)
    if not pinned:
        sys.exit(f"\n✗ `docker push` gave {image} no digest in {repository}.")
    print(f"✓ Pushed {image} ({pinned.split('@', 1)[1]})")
    return pinned


def _redeploy_generator(outputs: dict, region: str) -> None:
    """Restart the generator so it reads the current secret."""
    cmd = [
        "aws",
        "ecs",
        "update-service",
        "--cluster",
        outputs.get("ecs_cluster_name", ""),
        "--service",
        outputs.get("generator_service_name", ""),
        "--force-new-deployment",
        "--region",
        region,
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
    if result.returncode == 0:
        print("✓ Restarted the generator with `aws ecs update-service`")
    else:
        print(f"⚠ Could not restart the generator: {result.stderr.strip()}\n  Run it by hand: {' '.join(cmd)}")


def _confluent_rows(*args: str) -> list[dict]:
    result = subprocess.run(["confluent", *args, "-o", "json"], capture_output=True, text=True, timeout=60)
    if result.returncode != 0:
        return []
    try:
        rows = json.loads(result.stdout or "[]")
    except ValueError:
        return []
    return [row for row in rows if isinstance(row, dict)] if isinstance(rows, list) else []


def _field(row: dict, name: str) -> str:
    """A column from the CLI's JSON, whatever its case (email, Email)."""
    for key, value in row.items():
        if key.lower().replace(" ", "_") == name:
            return str(value or "").strip()
    return ""


def _grant_demo_users(env_id: str, emails: list[str]) -> list[str]:
    """Bind EnvironmentAdmin on the environment for each demo user who lacks it. Returns the emails not found."""
    ids = {}
    for row in _confluent_rows("iam", "user", "list"):
        if _field(row, "email") and _field(row, "id"):
            ids[_field(row, "email").lower()] = _field(row, "id")
    # Invitations not accepted yet: `id` is the invitation (i-...), `user_id` the user it made (u-...).
    for row in _confluent_rows("iam", "user", "invitation", "list"):
        email, user = _field(row, "email").lower(), _field(row, "user_id") or _field(row, "user")
        status = _field(row, "status").lower()
        if email and user and email not in ids and not any(word in status for word in ("declin", "expir")):
            ids[email] = user

    missing = []
    for email in emails:
        user = ids.get(email.lower())
        if not user:
            missing.append(email)
            continue
        principal = f"User:{user}"
        scope = ("--principal", principal, "--environment", env_id)
        bindings = _confluent_rows("iam", "rbac", "role-binding", "list", *scope)
        if any(_field(row, "role") == DEMO_USER_ROLE for row in bindings):
            print(f"✓ {email} already has {DEMO_USER_ROLE}")
            continue
        created = subprocess.run(
            [
                "confluent",
                "iam",
                "rbac",
                "role-binding",
                "create",
                "--principal",
                principal,
                "--role",
                DEMO_USER_ROLE,
                "--environment",
                env_id,
            ],
            capture_output=True,
            text=True,
            timeout=60,
        )
        if created.returncode == 0:
            print(f"✓ {email} is {DEMO_USER_ROLE} on {env_id}")
        else:
            print(f"⚠ Could not give {email} {DEMO_USER_ROLE}: {created.stderr.strip()}")
    return missing


def _write_card(root, outputs: dict, emails: list[str]):
    """Write and print tmp/always-on-card.env (gitignored): what the presenters need."""
    path = root / ALWAYS_ON_CARD
    path.parent.mkdir(exist_ok=True)
    card = {
        "ALWAYS_ON_APP_URL": outputs.get("app_url", ""),
        "ALWAYS_ON_APP_PASSWORD": outputs.get("app_password", ""),
        "ALWAYS_ON_DEMO_USERS": ",".join(emails),
    }
    path.write_text("".join(f"{key}='{value}'\n" for key, value in card.items()), encoding="utf-8")
    path.chmod(0o600)  # it holds the flight board's password
    print(f"\n✓ The always-on demo is running. Card: {ALWAYS_ON_CARD}")
    for key, value in card.items():
        print(f"  {key}={value}")
    print("  The flight board takes any username with this password.")
    return path


def _print_env_name(root) -> None:
    core_state = root / "terraform" / "core" / "terraform.tfstate"
    if not core_state.exists():
        return
    try:
        outputs = run_terraform_output(core_state)
        if "confluent_environment_display_name" in outputs:
            print(f"\nEnvironment name: {outputs['confluent_environment_display_name']}")
        if "confluent_kafka_cluster_display_name" in outputs:
            print(f"Cluster name: {outputs['confluent_kafka_cluster_display_name']}")
    except Exception as e:
        print(f"\n⚠ Could not read Terraform outputs: {e}")


if __name__ == "__main__":
    main()
