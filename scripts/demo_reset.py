"""Reset Confluent Cloud to where Demo 2 (video 2) starts, so its statements can run live on screen.

Video 1's objects stay: the source tables, passenger_state, flight_impact, Lightning Tables, and RTCE.
The reset

  1. stops the data stream and every statement that creates or runs a Demo 2 object,
  2. drops the agent's model, the RTCE tool, the agent, and the two staging tables,
  3. rebuilds the staging tables off screen (the agent's INSERT reads them),
  4. deletes RA417's offers for today,
  5. sends 200 hotel updates through the hotel-partner-webhook connector (when deployed), and
  6. restarts the stream, so RA417's delay and the Grand Hyatt sellout play out again.

Then CREATE MODEL, CREATE TOOL, CREATE AGENT, and the AI_RUN_AGENT INSERT run live on screen
(see the SQL for each in docs/demo-script-screens.md). Running the reset again is safe.

Usage:
  uv run reset-demo-2            # reset to the start of Demo 2
  uv run reset-demo-2 --speed 4  # same, with the stream clock 4x faster
  uv run reset-demo-2 --restore  # skip the live run: start the agent the way deploy does
  uv run reset-demo-2 --full     # back to a fresh deploy: all data republished, agent running
"""

import argparse
import json
import os
import subprocess
import sys
import time

from dotenv import dotenv_values

from scripts import airport_datagen, deploy
from scripts.login_checks import ensure_confluent_login
from scripts.terraform import get_project_root, run_terraform_output
from scripts.terraform_runner import run_terraform

# Demo 2 creates these on screen. Dropping the model also retires the Bedrock model that deployments
# from before the agent moved to the Confluent-hosted one have under the same name.
DROPS = (
    "DROP AGENT IF EXISTS passenger_recovery_agent",
    "DROP TOOL IF EXISTS hotel_inventory_live_context",
    "DROP MODEL IF EXISTS passenger_recovery_model",
    "DROP MATERIALIZED TABLE IF EXISTS impacted_passengers",
    "DROP MATERIALIZED TABLE IF EXISTS passenger_state_changes",
)
# A statement whose SQL names one of these creates or runs a Demo 2 object, whether Terraform
# or an earlier take on screen created it.
DEMO_2_NAMES = ("AI_RUN_AGENT", "passenger_recovery_agent", "hotel_inventory_live_context", "passenger_recovery_model",
                "passenger_state_changes", "impacted_passengers")
# Terraform's copies of the statements the reset deletes; removed from state so a later apply recreates them.
STATE_ADDRESSES = (
    'confluent_flink_statement.agent_setup["model"]',
    'confluent_flink_statement.agent_setup["tool"]',
    "confluent_flink_statement.recovery_agent[0]",
    "confluent_flink_statement.recovery_offers[0]",
    "confluent_flink_statement.passenger_state_changes[0]",
    "confluent_flink_statement.impacted_passengers[0]",
)
WEBHOOK_WARMUP = 200  # hotel updates the reset sends through the webhook, so its connector shows traffic
STAGING_STATEMENTS = ("airport-agent-passenger-state-changes", "airport-agent-impacted-passengers")
STAGING_TARGETS = (
    "confluent_flink_statement.passenger_state_changes",
    "confluent_flink_statement.impacted_passengers",
)
# The hotel webhook resources became conditional (count), so Terraform moves them to [0] in state
# and refuses a targeted plan that leaves them out. A deployment without a webhook has count 0.
WEBHOOK_TARGETS = (
    "confluent_api_key.hotel_webhook",
    "confluent_connector.hotel_webhook",
    "confluent_flink_statement.hotel_inventory_feed",
)
# passenger_state and flight_impact became conditional the same way (the workshop leaves them out).
SERVING_TARGETS = ("confluent_flink_statement.passenger_state", "confluent_flink_statement.flight_impact")


class Flink:
    """Runs and deletes Flink statements with the Confluent CLI, as the app-manager service account."""

    def __init__(self, core: dict):
        self.scope = ["--cloud", "aws", "--region", core["cloud_region"],
                      "--environment", core["confluent_environment_id"]]
        self.pool = core["confluent_flink_compute_pool_id"]
        self.principal = core["app_manager_service_account_id"]
        self.properties = (f"sql.current-catalog={core['confluent_environment_display_name']},"
                           f"sql.current-database={core['confluent_kafka_cluster_display_name']}")

    def _cli(self, *args: str) -> subprocess.CompletedProcess:
        return subprocess.run(["confluent", "flink", "statement", *args, *self.scope],
                              capture_output=True, text=True, timeout=300)

    def statements(self) -> list[dict]:
        result = self._cli("list", "-o", "json")
        if result.returncode != 0:
            raise RuntimeError(f"Could not list Flink statements: {result.stderr.strip()}")
        statements = json.loads(result.stdout or "[]")
        if statements and not all("name" in s and "statement" in s for s in statements):
            raise RuntimeError("`confluent flink statement list -o json` no longer returns name and statement")
        return statements

    def delete(self, name: str) -> None:
        self._cli("delete", name, "--force")

    def run(self, sql: str) -> None:
        """Run one DDL statement to completion, then delete it so it doesn't clutter the statement list."""
        name = f"reset-demo-2-{int(time.time() * 1000)}"
        created = self._cli("create", name, "--sql", sql, "--compute-pool", self.pool,
                            "--service-account", self.principal, "--property", self.properties, "--wait")
        described = self._cli("describe", name, "-o", "json")
        details = json.loads(described.stdout or "{}") if described.returncode == 0 else {}
        status = details.get("status", "")
        self.delete(name)
        if created.returncode != 0 or status != "COMPLETED":
            detail = details.get("status_detail") or created.stderr.strip() or created.stdout.strip()
            raise RuntimeError(f"{sql} ended {status or 'with an error'}: {detail}")


def demo_2_statements(statements: list[dict]) -> list[str]:
    """Names of the statements that create or run a Demo 2 object, the INSERT first."""
    found = [s for s in statements if any(n in (s.get("statement") or "") for n in DEMO_2_NAMES)]
    found.sort(key=lambda s: "AI_RUN_AGENT(" not in (s.get("statement") or ""))  # comments mention it too
    return [s["name"] for s in found]


def ra417_offer_keys(plan: airport_datagen.Plan) -> list[str]:
    """Every offer key the agent can write for today's RA417."""
    hero = plan.live.hero
    return sorted(f"{p.key}-O{n}" for p in plan.live.passengers if p.inbound is hero and p.connecting for n in (1, 2))


def _forget_in_terraform(demo, keep_staging: bool = False) -> None:
    listed = subprocess.run(["terraform", "state", "list"], cwd=demo, capture_output=True, text=True)
    present = set(listed.stdout.split())
    addresses = [a for a in STATE_ADDRESSES if a in present
                 and not (keep_staging and any(t in a for t in STAGING_TARGETS))]
    if addresses:
        subprocess.run(["terraform", "state", "rm", *addresses], cwd=demo, check=True, capture_output=True)


def _drop_demo_2(demo, flink: Flink) -> None:
    """Stop the stream, then drop every Demo 2 object, whether Terraform or a take on screen made it."""
    print("Stopping the data stream...")
    airport_datagen.stop_previous_stream()

    # The INSERT stops first; the staging tables' own statements go after DROP stops their queries.
    names = demo_2_statements(flink.statements())
    print(f"Deleting {len(names)} Demo 2 statement(s): {', '.join(names) or 'none'}")
    for name in names:
        if name not in STAGING_STATEMENTS:
            flink.delete(name)
    for sql in DROPS:
        print(f"  {sql}")
        flink.run(sql)
    for name in names:
        if name in STAGING_STATEMENTS:
            flink.delete(name)
    _forget_in_terraform(demo)


def reset(root, flink: Flink, speed: float = 1.0) -> None:
    demo = root / "terraform" / "airline-demo"
    _drop_demo_2(demo, flink)

    print("Rebuilding the staging tables off screen...")
    os.environ["TF_VAR_enable_recovery_agent"] = "true"
    if not run_terraform(demo, targets=STAGING_TARGETS + SERVING_TARGETS + WEBHOOK_TARGETS):
        raise RuntimeError("Rebuilding passenger_state_changes and impacted_passengers failed")

    start = airport_datagen._clock(None)
    plan = airport_datagen.build_plan(start, history=False)
    keys = ra417_offer_keys(plan)
    publisher = airport_datagen.Publisher(airport_datagen.extract_kafka_credentials("aws", root), dry_run=False)
    for key in keys:
        publisher.publish(airport_datagen.OFFERS, key, None)
    publisher.flush()
    print(f"Deleted {len(keys)} RA417 offer keys for today")

    if publisher.webhook:
        print("Sending the morning's hotel updates through the hotel-partner-webhook connector...")
        warmup = list(airport_datagen.hotel_warmup(plan, WEBHOOK_WARMUP))
        for topic, name, value in warmup:
            publisher.publish(topic, name, value)
        print(f"Sent {len(warmup)} hotel updates to the webhook")
    else:
        print("No hotel webhook in this deployment (Standard cluster): skipping the hotel warmup")

    print("Restarting the stream (today's flights only; history is unchanged)...")
    clock = deploy._run_datagen(root, agent=True, history=False, speed=speed)

    deploy.print_video_2_clock(clock, speed)


def full_reset(root, flink: Flink, speed: float = 1.0) -> None:
    """Put the demo back where `uv run deploy` leaves it: fresh data and stream, the agent running."""
    _drop_demo_2(root / "terraform" / "airline-demo", flink)
    print("Deleting every generated record...")
    airport_datagen.reset(airport_datagen._clock(None), airport_datagen.SEED, dry_run=False)
    print("Publishing history and today, then starting the stream...")
    deploy._run_datagen(root, agent=True, speed=speed)
    print("Starting the recovery agent...")
    if not deploy._start_agent(root):
        raise RuntimeError("The recovery agent did not start")
    print("\n✓ Reset to a fresh deploy: Video 1's data and Demo 2's agent are running.")


def restore(root, flink: Flink) -> None:
    """Start the agent the way deploy does, after removing any copy started on screen."""
    for name in demo_2_statements(flink.statements()):
        if name not in STAGING_STATEMENTS:
            flink.delete(name)
    _forget_in_terraform(root / "terraform" / "airline-demo", keep_staging=True)
    deploy._start_agent(root)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="uv run reset-demo-2", description=__doc__.split("\n")[0])
    parser.add_argument("--restore", action="store_true", help="start the agent the way deploy does instead")
    parser.add_argument("--full", action="store_true",
                        help="back to a fresh deploy: republish all data, restart the stream, start the agent")
    parser.add_argument("--speed", type=float, default=1.0,
                        help="run the stream clock this many times faster (default 1; 4 is a 22-minute stream)")
    parser.add_argument("--yes", action="store_true", help="skip the confirmation prompt")
    args = parser.parse_args(argv)
    if args.speed <= 0:
        parser.error("--speed must be greater than 0")

    root = get_project_root()
    core_state = root / "terraform" / "core" / "terraform.tfstate"
    if not core_state.exists():
        sys.exit("No deployment found. Run `uv run deploy` first.")
    creds = dotenv_values(root / "credentials.env")
    if creds.get(deploy.WORKSHOP) == "true":
        sys.exit("This checkout is in workshop mode, where attendees build Demo 2's agent themselves. "
                 "Nothing to reset.")
    core = run_terraform_output(core_state)
    ensure_confluent_login(creds, root / "credentials.env", interactive=sys.stdin.isatty())
    flink = Flink(core)

    if args.restore:
        restore(root, flink)
        return
    if args.full:
        print(f"This deletes and republishes every demo record in {core['confluent_environment_display_name']}, "
              f"restarts the stream, and recreates Demo 2's agent, tool, model, and staging tables.")
    else:
        print(f"This drops Demo 2's agent, tool, model, and staging tables in "
              f"{core['confluent_environment_display_name']}, deletes RA417's offers, and restarts the stream.")
    if not args.yes and input("Continue? (y/n): ").strip().lower() != "y":
        sys.exit("Cancelled.")
    try:
        (full_reset if args.full else reset)(root, flink, args.speed)
    except RuntimeError as error:
        sys.exit(f"✗ {error}")


if __name__ == "__main__":
    main()
