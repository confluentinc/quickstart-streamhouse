#!/usr/bin/env python3
"""
Deploy the Streamhouse airline flight-recovery demo to Confluent Cloud.

Always runs, in order:
  1. terraform/core        — environment, cluster, Flink pool, service accounts,
                             API keys, RTCE reader, (optional) Bedrock connection.
  2. terraform/airline-demo — the River Air Flink pipeline (source tables,
                             passenger_state, flight_impact) and, with AWS
                             creds, Tableflow to Iceberg in S3 with AWS Glue.
  3. The data generator (same data as `uv run airport-datagen`): 30 days of
     history and today's flights, then a 90-minute live stream in the background.
  4. Lightning Tables and RTCE on the demo topics, plus the RTCE MCP server for
     the coding agent chosen up front (same as `uv run setup-rtce`).
  5. Topic tag colors through the Catalog API, after the recovery agent setup.

AWS-only by design (RTCE + Flink Native Inference are AWS-only), so there is no
cloud or region choice. Resource names use TF_VAR_resource_prefix from
credentials.env, or DEFAULT_PREFIX when unset (saved on first run so later
runs keep the same names), plus a random suffix. For a recorded demo, set
TF_VAR_deployment_name=RIVER-AIR-PROD instead to get fixed names such as
RIVER-AIR-PROD-ENV and RIVER-AIR-PROD-CLUSTER.

Usage:
  uv run deploy               # interactive; the only command a person needs
  uv run deploy --automated   # bots: non-interactive from credentials.env, registers Claude Code
  uv run deploy --testing     # bots: non-interactive from credentials.env, no MCP registration
"""

import argparse
import getpass
import os
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
from scripts.login_checks import _attempt_login_quiet, ensure_confluent_login
from scripts.terraform import get_project_root, run_terraform_output
from scripts.terraform_runner import run_terraform
from scripts.tfvars import write_tfvars_for_deployment
from scripts.topic_tags import apply_tag_colors
from scripts.ui import prompt_with_default

DEFAULT_REGION = "us-east-1"  # RTCE-supported; Bedrock Claude available here
DEFAULT_PREFIX = "flight-recovery"
DEPLOY_TARGETS = ["core", "airline-demo"]
# The recovery agent's statements in terraform/airline-demo, applied once RTCE is up.
AGENT_TARGETS = (
    "confluent_flink_statement.agent_setup",
    "confluent_flink_statement.passenger_state_changes",
    "confluent_flink_statement.impacted_passengers",
    "confluent_flink_statement.recovery_agent",
    "confluent_flink_statement.recovery_offers",
    # Conditional (count) on a Dedicated cluster, so Terraform moves them to [0] in state and
    # rejects a targeted plan that leaves them out.
    "confluent_api_key.hotel_webhook",
    "confluent_connector.hotel_webhook",
    "confluent_flink_statement.hotel_inventory_feed",
)
# --video-2 stops short of what Demo 2 creates on screen: only the Bedrock model the agent runs on
# and the two staging tables its INSERT reads.
VIDEO_2_TARGETS = (
    'confluent_flink_statement.agent_setup["model"]',
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


def _deploy(root, targets, video_2: bool = False) -> None:
    print("\n=== Starting Deployment ===")
    # A redeploy keeps the recovery agent when its RTCE connection already exists.
    # --video-2 never does: Demo 2 creates the agent on screen.
    infra = setup_rtce.core_infra(root / "credentials.env")
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


def main():
    parser = argparse.ArgumentParser(description="Deploy the Streamhouse airline demo")
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
    if args.speed <= 0:
        parser.error("--speed must be greater than 0")

    print("=== Streamhouse Airline Demo — Deploy ===\n")
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
        ensure_confluent_login(creds)
        print(f"✓ Credentials loaded. Region: {region}. Deploying: {', '.join(DEPLOY_TARGETS)}\n")
        write_tfvars_for_deployment(root, region, creds, resource_prefix=prefix)
        for key, value in creds.items():
            if value:
                os.environ[key] = value
        _deploy(root, DEPLOY_TARGETS, args.video_2)
        _finish(root, "claude" if args.automated else "none", args.video_2, args.speed)
        return

    # ---- Interactive -----------------------------------------------------
    creds_file, creds = load_or_create_credentials_file(root)

    # Confluent Cloud login (saved for auto-login).
    if not (creds.get("CONFLUENT_EMAIL") and creds.get("CONFLUENT_PASSWORD")):
        print("\nConfluent Cloud login (saved to credentials.env for auto-login):")
        for attempt in range(3):
            email = input("  Email (Enter to skip): ").strip()
            if not email:
                print("  Skipped. Run `confluent login` manually if your session expires.")
                break
            password = getpass.getpass("  Password: ")
            if _attempt_login_quiet(email, password):
                _save_env_safe(creds_file, "CONFLUENT_EMAIL", email)
                _save_env_safe(creds_file, "CONFLUENT_PASSWORD", password)
                creds["CONFLUENT_EMAIL"] = email
                creds["CONFLUENT_PASSWORD"] = password
                print("  ✓ Logged in and saved.")
                break
            remaining = 2 - attempt
            print(f"  Login failed. {remaining} attempt(s) left." if remaining else
                  "  Login failed. Skipping (use `confluent login --sso` if you use SSO).")
    ensure_confluent_login(creds)
    print("✓ Confluent CLI logged in")

    # No prompt: us-east-1 unless the user set TF_VAR_cloud_region in credentials.env.
    region = creds.get("TF_VAR_cloud_region") or DEFAULT_REGION
    print(f"\nRegion: {region} (AWS-only; RTCE + Bedrock supported here)")

    # No prompt: reuse the saved prefix, else the default. Persisting it keeps
    # resource names stable if DEFAULT_PREFIX changes later.
    prefix = creds.get("TF_VAR_resource_prefix", "") or DEFAULT_PREFIX
    _save_env_safe(creds_file, "TF_VAR_resource_prefix", prefix)
    if creds.get("TF_VAR_deployment_name"):
        print(f"Deployment name: {creds['TF_VAR_deployment_name'].upper()} (fixed, from TF_VAR_deployment_name)")
    else:
        print(f"Resource name prefix: {prefix} (set TF_VAR_resource_prefix in credentials.env to change)")

    # Optionally generate fresh Confluent Cloud API keys.
    if input("\nGenerate new Confluent Cloud API keys? (y/n): ").strip().lower() == "y":
        api_key, api_secret = generate_confluent_api_keys(prefix)
        if api_key and api_secret:
            _save_env_safe(creds_file, "TF_VAR_confluent_cloud_api_key", api_key)
            _save_env_safe(creds_file, "TF_VAR_confluent_cloud_api_secret", api_secret)
            creds["TF_VAR_confluent_cloud_api_key"] = api_key
            creds["TF_VAR_confluent_cloud_api_secret"] = api_secret

    print("\n--- Credential Configuration ---")
    api_key = prompt_with_default(
        "Confluent Cloud API Key", creds.get("TF_VAR_confluent_cloud_api_key", "")
    )
    api_secret = prompt_with_default(
        "Confluent Cloud API Secret", creds.get("TF_VAR_confluent_cloud_api_secret", ""), secret=True
    )
    _save_env_safe(creds_file, "TF_VAR_confluent_cloud_api_key", api_key)
    _save_env_safe(creds_file, "TF_VAR_confluent_cloud_api_secret", api_secret)

    # AWS credentials power core's Bedrock connection and the Tableflow S3/Glue
    # analytics path (Demo 3) — both OPTIONAL, and both use the same identity
    # in a demo, so one credential covers both.
    # Try the ambient AWS credential chain first: pasting a long temporary
    # session token into a plain terminal prompt can freeze some terminals,
    # and most people running this already have valid AWS creds in their
    # shell (`aws sso login`, env vars, a profile).
    print(
        "\nAWS credentials power the Bedrock connection and the Tableflow S3/Glue"
        "\nanalytics path (Demo 3). Both are optional — without them, everything"
        "\nelse still deploys and those two are skipped."
    )
    access_key = secret_key = token = ""
    ambient = _detect_ambient_aws_credentials()
    if ambient:
        detected_key, detected_secret, detected_token = ambient
        masked = f"{detected_key[:4]}...{detected_key[-4:]}" if len(detected_key) > 8 else detected_key
        if input(f"Detected AWS credentials in your environment ({masked}). Use these? (Y/n): ").strip().lower() in (
            "",
            "y",
        ):
            access_key, secret_key, token = detected_key, detected_secret, detected_token

    if not access_key:
        access_key = prompt_with_default(
            "AWS Access Key (optional)",
            creds.get("TF_VAR_aws_bedrock_access_key", ""),
            required=False,
        )
        if access_key:
            secret_key = prompt_with_default(
                "AWS Secret Key", creds.get("TF_VAR_aws_bedrock_secret_key", ""), secret=True
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
                    "AWS Session Token (temp creds)", creds.get("TF_VAR_aws_session_token", ""), secret=True
                )

    if access_key and secret_key:
        for name in ("aws_bedrock_access_key", "aws_tableflow_access_key"):
            _save_env_safe(creds_file, f"TF_VAR_{name}", access_key)
        for name in ("aws_bedrock_secret_key", "aws_tableflow_secret_key"):
            _save_env_safe(creds_file, f"TF_VAR_{name}", secret_key)
        if token:
            for name in ("aws_session_token", "aws_tableflow_session_token"):
                _save_env_safe(creds_file, f"TF_VAR_{name}", token)

    _flush_pending_writes(creds_file)

    final_creds = dotenv_values(creds_file)
    print("\n--- Configuration Summary ---")
    print(f"Resource name prefix: {prefix}")
    print(f"Region: {region}")
    api_key_state = "set" if final_creds.get("TF_VAR_confluent_cloud_api_key") else "MISSING"
    print(f"Confluent Cloud API key: {api_key_state}")
    print(f"Confluent CLI auto-login: {'saved' if final_creds.get('CONFLUENT_EMAIL') else 'not saved'}")
    bedrock_state = "ENABLED" if final_creds.get("TF_VAR_aws_bedrock_access_key") else "skipped (no Bedrock creds)"
    print(f"Bedrock connection: {bedrock_state}")
    analytics_state = (
        "ENABLED" if final_creds.get("TF_VAR_aws_tableflow_access_key") else "skipped (no Tableflow AWS creds)"
    )
    print(f"Tableflow S3/Glue analytics (Demo 3): {analytics_state}")
    print(f"Deploying: {', '.join(DEPLOY_TARGETS)}, then demo data and RTCE")

    client = setup_rtce._pick_client()

    if input("\nReady to deploy? (y/n): ").strip().lower() != "y":
        print("Deployment cancelled.")
        sys.exit(0)

    write_tfvars_for_deployment(root, region, final_creds, resource_prefix=prefix)
    for key, value in final_creds.items():
        if value:
            os.environ[key] = value
    _deploy(root, DEPLOY_TARGETS, args.video_2)
    _finish(root, client, args.video_2, args.speed)
    if input("\nStart the app at http://127.0.0.1:8000 now? (Y/n): ").strip().lower() in ("", "y"):
        from scripts.airport_app import main as run_app

        run_app([])
    else:
        print("Start it later with: uv run airport-app")


def _finish(root, client: str, video_2: bool = False, speed: float = 1.0) -> None:
    """Publish the demo data, start the live stream, enable Lightning Tables/RTCE, then the agent.

    With video_2, the agent stays unstarted and the stream starts last, so Demo 2's clock begins
    when everything is ready (as `uv run reset-demo-2` leaves it).
    """
    agent = _bedrock_enabled(root)
    if video_2 and not agent:
        print("--video-2 needs the Bedrock connection (AWS credentials); deploying as usual.")
    video_2 = video_2 and agent
    print("\n=== Publishing demo data ===")
    _run_datagen(root, agent, speed=speed, stream=not video_2)
    print("\n=== Enabling Lightning Tables and RTCE ===")
    setup_rtce.main(["--client", client])
    if video_2:
        print("\n=== Preparing Demo 2 ===")
        if _start_agent(root, VIDEO_2_TARGETS):
            clock = _run_datagen(root, agent=True, history=False, speed=speed)
            print_video_2_clock(clock, speed)
    elif agent:
        print("\n=== Starting the recovery agent ===")
        _start_agent(root)
    else:
        print("\nRecovery agent: skipped (no Bedrock credentials); the generator writes all offers.")
    print("\n=== Applying topic tag colors ===")
    apply_tag_colors(root)
    _print_env_name(root)


def _bedrock_enabled(root) -> bool:
    core_state = root / "terraform" / "core" / "terraform.tfstate"
    try:
        return bool(run_terraform_output(core_state).get("bedrock_enabled"))
    except Exception:
        return False


def print_video_2_clock(clock, speed: float) -> None:
    def at(minutes: float) -> str:
        return f"{clock + timedelta(minutes=minutes / speed):%H:%M:%S}"

    print(f"""
✓ Ready for Demo 2. The stream started at {at(0)} UTC:
  {at(6)} to {at(16.5)}  RA417's passengers turn HIGH (record 2.1 here)
  {at(40)}           Grand Hyatt sells out (record 2.4 from {at(38)})
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
        print("⚠ The recovery agent did not start, so RA417 has no offers yet. "
              "Run `uv run airport-datagen --offers generator` to have the generator write them.")
    return started


def _run_datagen(root, agent: bool, history: bool = True, speed: float = 1.0, stream: bool = True):
    """Publish history and today's flights, then stream live updates in the background.

    When the recovery agent will run, it writes RA417's live offers and the generator skips them.
    Returns the stream start.
    """
    from confluent_kafka.schema_registry import SchemaRegistryClient

    from .airport_datagen import STREAM_MINUTES, _clock, run
    from .terraform import extract_kafka_credentials

    credentials = extract_kafka_credentials("aws", root)
    registry = SchemaRegistryClient({
        "url": credentials["schema_registry_url"],
        "basic.auth.user.info": (
            f"{credentials['schema_registry_api_key']}:{credentials['schema_registry_api_secret']}")})
    subjects = ("flight_status-value", "passenger_itineraries-value",
                "hotel_inventory-value", "passenger_recommendations-value")
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
            ["uv", "run", "airport-datagen", "--now", start.isoformat(), "--stream-only", "--offers", offers,
             "--speed", str(speed)],
            cwd=root, stdin=subprocess.DEVNULL, stdout=out, stderr=subprocess.STDOUT, start_new_session=True)
    print(f"✓ Streaming live flight updates for {STREAM_MINUTES} minutes"
          f"{f' at {speed:g}x' if speed != 1 else ''} in the background "
          f"(log: {log.relative_to(root)}; restart with `uv run airport-datagen`)")
    return start


def _print_env_name(root) -> None:
    core_state = root / "terraform" / "core" / "terraform.tfstate"
    if not core_state.exists():
        return
    try:
        outputs = run_terraform_output(core_state)
        if "confluent_environment_display_name" in outputs:
            print(f"\nEnvironment name: {outputs['confluent_environment_display_name']}")
    except Exception as e:
        print(f"\n⚠ Could not read Terraform outputs: {e}")


if __name__ == "__main__":
    main()
