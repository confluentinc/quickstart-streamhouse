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
the source feeds (flights, itineraries, hotels, flight telemetry), passenger_recommendations,
and the agent's hosted model, and deploy stops after the Global API key. Attendees publish the
data, build passenger_state, flight_impact, and the recovery agent, and turn on Lightning Tables and RTCE themselves
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
"""

import argparse
import os
import shlex
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


def _check_writable(root) -> None:
    """Stop before any prompt or Terraform run if the checkout can't be written to.

    Without this, a read-only folder lets deploy run on in memory, then Terraform can't save its
    state and later commands fail with "state not found".
    """
    problems = []
    creds_file = root / "credentials.env"
    if creds_file.exists() and not os.access(creds_file, os.W_OK):
        problems.append(f"  - {creds_file}: not writable")
    for folder in (root, root / "terraform" / "core", root / "terraform" / "airline-demo"):
        if not folder.exists():
            continue
        probe = folder / ".write-check"
        try:
            probe.write_text("")
            probe.unlink()
        except OSError as e:
            problems.append(f"  - {folder}: {e.strerror or e}")
    if problems:
        print("✗ Deploy needs to write credentials.env and Terraform state, but cannot write to:")
        print("\n".join(problems))
        quoted = shlex.quote(str(root))
        print("\nTry this, then run `uv run deploy` again:")
        print(f"  chmod -R u+w {quoted}")
        print(
            "If that says 'Operation not permitted' or the folder belongs to another user "
            "(for example it was cloned with sudo), take ownership instead:"
        )
        print(f'  sudo chown -R "$(whoami)" {quoted}')
        print("Or clone the repo into a folder you own, such as your home folder, and deploy from there.")
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
    args = parser.parse_args()
    if args.automated and args.testing:
        parser.error("--automated and --testing are mutually exclusive")
    if args.workshop and (args.video_2 or args.with_labs):
        parser.error("--workshop deploys the starting point; drop --with-labs and --video-2")
    if args.speed <= 0:
        parser.error("--speed must be greater than 0")

    print("=== Streamhouse Labs — Deploy ===\n")
    root = get_project_root()
    print(f"Project root: {root}")
    _check_writable(root)

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
        labs = _select_labs(args.lab, creds_file, creds, _workshop_saved(args, creds), interactive=False)
        workshop = FLIGHT_DISRUPTIONS in labs and _workshop_mode(
            args.workshop, args.with_labs or args.video_2, creds_file, creds, default=False
        )
        _flush_pending_writes(creds_file)
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
        if PIT_DECISIONS in labs:
            _finish_pit_decisions(root, args.with_labs)
        if workshop:
            _finish_workshop(root)
        elif FLIGHT_DISRUPTIONS in labs:
            _finish(root, "claude" if args.automated else "none", args.video_2, args.speed)
        return

    # ---- Interactive -----------------------------------------------------
    creds_file, creds = load_or_create_credentials_file(root)
    labs = _select_labs(args.lab, creds_file, creds, _workshop_saved(args, creds), interactive=True)
    flights = FLIGHT_DISRUPTIONS in labs
    workshop = flights and _workshop_mode(args.workshop, args.with_labs or args.video_2, creds_file, creds)
    if workshop:
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

    _flush_pending_writes(creds_file)

    final_creds = dotenv_values(creds_file)
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

    client = "none" if workshop or not flights else setup_rtce._pick_client()

    print()
    if not prompt_yes_no("Ready to deploy?"):
        print("Deployment cancelled.")
        sys.exit(0)

    write_tfvars_for_deployment(root, region, final_creds, resource_prefix=prefix)
    for key, value in final_creds.items():
        if value:
            os.environ[key] = value
    _deploy(root, _targets(labs), args.video_2)
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
        "\nThey're optional — without them, everything else still deploys, the recovery agent included."
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
                    "Temporary credentials need a session token too — that's a long string (300-400 chars) "
                    "that can freeze some terminals on paste. If it does, Ctrl-C and re-run after "
                    "`export AWS_SESSION_TOKEN=...` (and AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY) in your shell, "
                    "so this can auto-detect it instead of needing it pasted here."
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
    if not started:
        print(
            "⚠ The recovery agent did not start, so RA417, RA183, and RA185 have no offers yet. "
            "Run `uv run airport-datagen --offers generator` to have the generator write them."
        )
    return started


def _run_datagen(root, agent: bool, history: bool = True, speed: float = 1.0, stream: bool = True):
    """Publish history and today's flights, then stream live updates in the background.

    When the recovery agent will run, it writes the agent flights' live offers and the generator skips them.
    Returns the stream start.
    """
    from confluent_kafka.schema_registry import SchemaRegistryClient

    from .airport_datagen import STREAM_MINUTES, _clock, run
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
        "flight_telemetry-value",
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
