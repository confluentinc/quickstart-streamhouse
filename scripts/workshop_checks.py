"""Catch-up checks for attendees who skip a step in the Flight Disruptions workshop.

Each workshop step leaves something behind: a deployment, Flink tables, Lightning Tables, a Flink
connection, a running agent. A command that needs one of them calls ``require_deployment`` or
``ensure_lightning``, which names the missing step and where it is in the guide, then stops, so the
attendee never sees the raw error.

Neither those calls nor ``uv run workshop-status`` runs a step for the attendee, because an attendee
who could run one command to finish the labs would skip the labs. ``workshop-status`` runs every check
in order and reports what's done, which step is next, and where it is in the guide. Bots and tests catch
up with a hidden flag:

    uv run workshop-status             # what's done, then the next step and where to find it
    uv run workshop-status --testing   # run every missing step (not for attendees)

``--testing`` sets WORKSHOP_ASSUME_YES=1, which makes ``ensure_lightning`` and ``ensure_flink_tables``
run the missing step instead of pointing at it. Scripts and tests can set it themselves.
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

import requests

from scripts import processes
from scripts import setup_rtce as rtce
from scripts.pit_decisions.card import card_fields
from scripts.ui import prompt_yes_no

WORKSHOP_GUIDE = "docs/flight-disruptions/workshop.md"
SQL_DIR = Path("terraform") / "airline-demo" / "sql"

# The Lightning Tables Lab 3 enables, in the order the guide lists them.
LIGHTNING_TOPICS = [
    "flight_status",
    "passenger_itineraries",
    "passenger_state",
    "flight_impact",
    "passenger_recommendations",
    "hotel_inventory",
    "flight_telemetry",
]
# What the recovery agent's tool queries through RTCE.
AGENT_TOPICS = ["flight_status", "hotel_inventory"]

# The guide's SQL cells, in order, as (topic or object they create, SQL file, extra statement properties).
UTC = {"sql.local-time-zone": "UTC"}  # the guide's leading SET for statements that read CURRENT_TIMESTAMP
LAB2 = [
    ("passenger_state", "24-serving-passenger-state.sql", None),
    ("flight_impact", "25-serving-flight-impact.sql", None),
]
LAB4_STAGING = [
    ("passenger_state_changes", "26-staging-passenger-state-changes.sql", None),
    ("impacted_passengers", "27-staging-impacted-passengers.sql", UTC),
]
LAB4_TOOL = ("hotel_inventory_live_context", "29-tool-hotel-inventory-live-context.sql", None)
LAB4_AGENT = ("passenger_recovery_agent", "30-agent-passenger-recovery.sql", None)
LAB4_RUN = ("the agent's INSERT", "31-insert-passenger-recommendations.sql", UTC)

LAB2_STEP = "Lab 2, steps 3 and 4"
LAB3_STEP = "Lab 3, steps 1 and 2"

LIGHTNING_WAIT_SECONDS = 600

STATUS_HINT = "Do it yourself in the guide, then run `uv run workshop-status` again to check."
RERUN_HINT = "Do it yourself in the guide, then run this command again. `uv run workshop-status` shows what's left."

NOT_DEPLOYED = (
    "✗ There's no finished deployment in this folder, so `{command}` has nothing to connect to.\n"
    '  Run `uv run deploy` first (the guide\'s "Deploy to Confluent Cloud", step 3) and wait for\n'
    '  "All deployments completed successfully". If it\'s still running in another terminal, wait for it.\n'
    "  Then run `{command}` again."
)


def runs_missing_steps() -> bool:
    """True only for bots and tests (WORKSHOP_ASSUME_YES=1). Attendees do each step themselves."""
    return os.environ.get("WORKSHOP_ASSUME_YES") == "1"


class Workshop:
    """What a running workshop deployment looks like, read from terraform state and credentials.env."""

    def __init__(self, root: Path, creds_file: Path, creds: dict, core: dict) -> None:
        self.root = root
        self.creds_file = creds_file
        self.creds = creds
        self.core = core
        self.infra = {**(rtce.core_infra(creds_file) or {}), "cloud": str(core.get("cloud_provider", "aws")).lower()}
        self.rtce_key = creds.get(rtce._CRED_KEY, "")
        self.rtce_secret = creds.get(rtce._CRED_SECRET, "")
        self._session = None

    @property
    def is_workshop(self) -> bool:
        return self.creds.get("TF_VAR_workshop") == "true"

    @property
    def flink(self):
        """A Flink Statements API session, the one `f1-sql` and `deploy --with-labs` use."""
        if self._session is None:
            from scripts.pit_decisions.sql_shell import FlinkSession

            fields = card_fields(self.core, self.rtce_key, self.rtce_secret, 0)
            self._session = FlinkSession({f"F1_{key.upper()}": value for key, value in fields.items()})
        return self._session

    def phases(self) -> dict[str, str]:
        return rtce.rtce_topic_phases(self.rtce_key, self.rtce_secret, self.infra)

    def kafka_topics(self) -> set[str]:
        from confluent_kafka.admin import AdminClient

        admin = AdminClient(
            {
                "bootstrap.servers": self.core["confluent_kafka_cluster_bootstrap_endpoint"],
                "security.protocol": "SASL_SSL",
                "sasl.mechanism": "PLAIN",
                "sasl.username": self.core["app_manager_kafka_api_key"],
                "sasl.password": self.core["app_manager_kafka_api_secret"],
                "log_level": 3,  # hides librdkafka's info lines, which would print in the middle of a prompt
            }
        )
        return set(admin.list_topics(timeout=20).topics)

    def run_sql(self, filename: str, properties: dict[str, str] | None = None) -> bool:
        """Submit one of the guide's SQL files and wait for it to start. Prints why when it can't."""
        sql = (self.root / SQL_DIR / filename).read_text(encoding="utf-8").strip().rstrip(";").strip()
        try:
            status = self.flink.wait(self.flink.submit(sql, properties), timeout=180)
        except Exception as exc:
            print(f"    ✗ {filename}: {exc}")
            return False
        phase = status["status"]["phase"]
        if phase in ("RUNNING", "COMPLETED"):
            return True
        print(f"    ✗ {filename}: {phase}: {(status['status'].get('detail') or 'no detail returned').strip()[:300]}")
        return False

    def show(self, what: str) -> set[str]:
        """Names from `SHOW TOOLS` or `SHOW AGENTS`."""
        name = self.flink.submit(f"SHOW {what}")
        try:
            self.flink.wait(name, timeout=60)
            return {str(row[0]) for row in self.flink.results(name, 500, timeout=30, idle_pages=2) if row}
        finally:
            requests.delete(f"{self.flink.base}/{name}", auth=self.flink.auth, timeout=30)

    def agent_insert_running(self) -> bool:
        """True when the statement that runs the agent (the guide's INSERT INTO passenger_recommendations) is up."""
        url = f"{self.flink.base}?page_size=100"
        while url:
            page = requests.get(url, auth=self.flink.auth, timeout=30).json()
            for stmt in page.get("data", []):
                if is_agent_insert((stmt.get("spec") or {}).get("statement", "")) and (
                    (stmt.get("status") or {}).get("phase") in ("RUNNING", "PENDING")
                ):
                    return True
            url = (page.get("metadata") or {}).get("next")
        return False


def find_deployment(root: Path | None = None) -> Workshop | None:
    """The Flight Disruptions deployment in this checkout, or None until deploy has finished both roots."""
    creds_file = rtce._find_credentials_file()
    root = root or creds_file.parent
    core = rtce._read_core_tf_outputs(creds_file)
    demo = root / "terraform" / "airline-demo" / "terraform.tfstate"
    airline_outputs = rtce._read_terraform_outputs(demo) if demo.exists() else {}
    if not (core.get("confluent_environment_id") and airline_outputs):
        return None
    return Workshop(root, creds_file, rtce._load_env_file(creds_file), core)


def is_agent_insert(sql: str) -> bool:
    """True for the statement that runs the agent: the guide's INSERT INTO passenger_recommendations."""
    from scripts.pit_decisions.sql_shell import strip_leading_comments

    return strip_leading_comments(sql).replace("`", "").upper().startswith("INSERT INTO PASSENGER_RECOMMENDATIONS")


def require_deployment(command: str, root: Path | None = None) -> Workshop:
    """The deployment in this checkout, or exit with what to do when there isn't a finished one."""
    ws = find_deployment(root)
    if ws is None:
        sys.exit(NOT_DEPLOYED.format(command=command))
    return ws


def require_cluster_or_own(command: str, creds_file: Path) -> None:
    """For setup-rtce, which can also target a cluster you made yourself: stop an attendee who hasn't deployed.

    With no Terraform state it would otherwise ask for an organization, environment, and cluster ID.
    """
    if rtce._read_core_tf_outputs(creds_file):
        return
    print(NOT_DEPLOYED.format(command=command))
    if sys.stdin.isatty() and prompt_yes_no("\nUse a cluster you created yourself instead?", default=False):
        return
    sys.exit(1)


# --- Lab 2: the Flink tables ---------------------------------------------------------------------


def ensure_flink_tables(ws: Workshop, steps, topics: set[str], lab_step: str) -> bool:
    """True when the Flink tables of a guide step are in the cluster. Otherwise point at the step.

    ``steps`` is a list of (table, sql file, properties); a table is missing when its topic isn't there.
    ``lab_step`` is the ``where`` of the guide step that creates them. Bots and tests create them instead.
    """
    missing = [step for step in steps if step[0] not in topics]
    if not missing:
        return True
    names = ", ".join(f"`{table}`" for table, _, _ in missing)
    print(f"\n✗ {names} {'isn' if len(missing) == 1 else 'aren'}'t in your cluster yet.")
    if not runs_missing_steps():
        point_to_step(_step(lab_step), ws.root, closing=RERUN_HINT)
        return False
    for table, filename, properties in missing:
        print(f"  Creating {table}...", flush=True)
        if not ws.run_sql(filename, properties):
            return False
    return True


def require_lab_2(ws: Workshop) -> bool:
    """For `setup-rtce`, which turns Lightning Tables on itself: they can't be turned on before Flink builds them."""
    return ensure_flink_tables(ws, LAB2, ws.kafka_topics(), LAB2_STEP)


# --- Lab 3: the Lightning Tables -----------------------------------------------------------------


def _disable_failed(ws: Workshop, failed: list[str]) -> None:
    """A Lightning Table that was enabled before its topic existed stays FAILED, so turn it off to start over."""
    for topic in failed:
        subprocess.run(
            [
                *(processes.exe("confluent"), "rtce", "rtce-topic", "delete", topic, "--force"),
                *("--cluster", ws.infra["cluster_id"], "--environment", ws.infra["env_id"]),
            ],
            capture_output=True,
            text=True,
            timeout=60,
            encoding="utf-8",
            errors="replace",
        )
    if failed:
        time.sleep(5)  # the delete finishes in the background


def ensure_lightning(ws: Workshop, topics: list[str], needed_for: str, wait: bool = True) -> bool:
    """Make sure every topic in ``topics`` has an ACTIVE Lightning Table, or point at the step that makes it.

    Covers a Lightning Table never turned on (Lab 3 skipped), one stuck FAILED because its Flink table
    didn't exist yet (Lab 2 skipped, then Lab 3), and one still loading. Returns True when all are ACTIVE.
    It waits for a table that is loading, but turns on or recreates one only for bots and tests.
    """
    phases = ws.phases()
    if all(phases.get(t) == "ACTIVE" for t in topics):
        return True

    # A topic Flink should have built (Lab 2) can't be enabled until it exists.
    if any(phases.get(t) != "ACTIVE" for t in ("passenger_state", "flight_impact") if t in topics):
        have = ws.kafka_topics()
        if not ensure_flink_tables(ws, LAB2, have, LAB2_STEP):
            return False
        if any(t not in have for t, _, _ in LAB2):
            phases = ws.phases()

    off = [t for t in topics if t not in phases]
    failed = [t for t in topics if phases.get(t) == "FAILED"]
    if off or failed:
        if off:
            print(f"\n✗ Lightning Tables aren't on for: {', '.join(f'`{t}`' for t in off)}. {needed_for}".rstrip())
        if failed:
            print(f"\n✗ Lightning Tables failed for: {', '.join(f'`{t}`' for t in failed)}. {needed_for}".rstrip())
            print("  Turn each one off and on again in the Console's Topics list.")
        if not runs_missing_steps():
            point_to_step(_step(LAB3_STEP), ws.root, closing=RERUN_HINT)
            return False
        _disable_failed(ws, failed)
        for topic in [*off, *failed]:
            ok = rtce._enable_rtce_topic(
                ws.rtce_key, ws.rtce_secret, ws.infra, topic, f"Data from the {topic} Kafka topic"
            )
            print(f"  {'✓' if ok else '✗'} {topic}")
            if not ok:
                return False
    return _wait_active(ws, topics) if wait else True


def _wait_active(ws: Workshop, topics: list[str]) -> bool:
    deadline = time.monotonic() + LIGHTNING_WAIT_SECONDS
    last = None
    while True:
        phases = ws.phases()
        waiting = [t for t in topics if phases.get(t) != "ACTIVE"]
        if not waiting:
            print("  ✓ Every Lightning Table is Active.")
            return True
        failed = [t for t in waiting if phases.get(t) == "FAILED"]
        if failed:
            print(f"  ✗ {', '.join(failed)} failed to start. Turn it off and on again in the Console's Topics list.")
            return False
        if time.monotonic() > deadline:
            print(
                f"  ✗ Still loading after {LIGHTNING_WAIT_SECONDS // 60} minutes: {', '.join(waiting)}. "
                "Check the Topics list."
            )
            return False
        if waiting != last:
            print(f"  Waiting for Lightning Tables to load (about a minute): {', '.join(waiting)}", flush=True)
            last = waiting
        time.sleep(10)


# --- Lab 4: the recovery agent -------------------------------------------------------------------


def ensure_agent_context(ws: Workshop) -> bool:
    """Before Lab 4's connection: the agent's tool reads flights and hotels through RTCE, so they need Lightning."""
    return ensure_lightning(
        ws, AGENT_TOPICS, "The agent's tool reads flights and hotels through them, so Lab 4 needs them on."
    )


# --- uv run workshop-status ----------------------------------------------------------------------


@dataclass
class Step:
    where: str  # which part of the guide
    what: str  # what it leaves behind
    done: Callable[[Workshop], bool]
    fix: Callable[[Workshop], bool] | None  # None when only the attendee can do it
    manual: str = ""  # what the attendee does when there is no fix
    guide: tuple[str, int] = ("", 0)  # the guide's "## " heading (a prefix of it) and the step's number in it


def _datagen_running(ws: Workshop) -> bool:
    pid_file = ws.root / "tmp" / "datagen.pid"
    try:
        return processes.pid_alive(int(pid_file.read_text(encoding="utf-8").strip()))
    except (OSError, ValueError):
        return False


def _topics_exist(*tables: str) -> Callable[[Workshop], bool]:
    return lambda ws: all(t in ws.kafka_topics() for t in tables)


def _run_all(steps) -> Callable[[Workshop], bool]:
    def run(ws: Workshop) -> bool:
        have = ws.kafka_topics()
        return all(ws.run_sql(f, p) for table, f, p in steps if table not in have)

    return run


def _lightning_fix(ws: Workshop) -> bool:
    return ensure_lightning(ws, LIGHTNING_TOPICS, "")


def _connection_fix(ws: Workshop) -> bool:
    from scripts.login_checks import ensure_confluent_login

    ensure_confluent_login(ws.creds)
    return rtce.create_agent_connection(ws.infra, ws.rtce_key, ws.rtce_secret)


def _tool_and_agent_fix(ws: Workshop) -> bool:
    return all(ws.run_sql(f, p) for _, f, p in (LAB4_TOOL, LAB4_AGENT))


def steps() -> list[Step]:
    return [
        Step(
            "Deploy, step 4",
            "the datagen is running (second terminal)",
            _datagen_running,
            None,
            "Start it in a second terminal: uv run airport-datagen --skip-history",
            ("## Deploy to Confluent Cloud", 4),
        ),
        Step(
            LAB2_STEP,
            "passenger_state and flight_impact exist in Flink",
            _topics_exist("passenger_state", "flight_impact"),
            _run_all(LAB2),
            guide=("## Lab 2", 3),
        ),
        Step(
            LAB3_STEP,
            "all seven Lightning Tables are Active",
            lambda ws: all(ws.phases().get(t) == "ACTIVE" for t in LIGHTNING_TOPICS),
            _lightning_fix,
            guide=("## Lab 3", 1),
        ),
        Step(
            "Lab 4, step 1",
            "the Flink connection rtce-connection exists",
            lambda ws: rtce.agent_connection_exists(ws.infra),
            _connection_fix,
            guide=("## Lab 4", 1),
        ),
        Step(
            "Lab 4, steps 2 and 3",
            "passenger_state_changes and impacted_passengers exist",
            _topics_exist("passenger_state_changes", "impacted_passengers"),
            _run_all(LAB4_STAGING),
            guide=("## Lab 4", 2),
        ),
        Step(
            "Lab 4, steps 4 and 5",
            "the tool and the agent exist",
            lambda ws: LAB4_TOOL[0] in ws.show("TOOLS") and LAB4_AGENT[0] in ws.show("AGENTS"),
            _tool_and_agent_fix,
            guide=("## Lab 4", 4),
        ),
        Step(
            "Lab 4, step 7",
            "the agent's INSERT is running",
            lambda ws: ws.agent_insert_running(),
            lambda ws: ws.run_sql(LAB4_RUN[1], LAB4_RUN[2]),
            guide=("## Lab 4", 7),
        ),
    ]


def guide_location(step: Step, root: Path) -> tuple[str, int] | None:
    """The guide's section title and the line of the step's number in it, or None if the guide has changed."""
    heading, number = step.guide
    try:
        lines = (root / WORKSHOP_GUIDE).read_text(encoding="utf-8").splitlines()
    except OSError:
        return None
    section = next((i for i, line in enumerate(lines) if line.startswith(heading)), None)
    if section is None:
        return None
    for i in range(section + 1, len(lines)):
        if lines[i].startswith("## "):
            break
        if lines[i].startswith(f"{number}. "):
            return lines[section].removeprefix("## "), i + 1
    return None


def _open_guide_commands(line: int | None) -> list[tuple[str, str]]:
    """(what it does, command) pairs that open the guide on this computer."""
    if sys.platform == "win32":
        # Not `start "" ...`, which is Command Prompt's syntax: PowerShell's start (Start-Process) rejects the "".
        return [("Open the guide:", f"start {WORKSHOP_GUIDE.replace('/', chr(92))}")]
    opener = "open" if sys.platform == "darwin" else "xdg-open"
    commands = [("Open the guide:", f"{opener} {WORKSHOP_GUIDE}")]
    if line:
        commands.append(("Read from there:", f"less +{line} {WORKSHOP_GUIDE}   (press q to quit)"))
    return commands


def _step(where: str) -> Step:
    return next(step for step in steps() if step.where == where)


def point_to_step(step: Step, root: Path, closing: str = STATUS_HINT) -> None:
    """Say which step is next and where to find it in the guide. It never runs the step."""
    print(f"\nYour next step is {step.where} (done when: {step.what}).")
    if step.manual:
        print(f"  {step.manual}")
    found = guide_location(step, root)
    section, line = found if found else (None, None)
    if section:
        print(f'  In the guide: "{section}", step {step.guide[1]} (line {line} of {WORKSHOP_GUIDE}).')
    else:
        print(f"  In the guide: {step.where} ({WORKSHOP_GUIDE}).")
    for what, command in _open_guide_commands(line):
        print(f"  {what:<17}{command}")
    print(f"  {closing}")


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="uv run workshop-status",
        description="Check which workshop steps are done, and point to the next one in the guide.",
    )
    # For bots and tests that need a deployment caught up, never for the workshop: it runs the labs for you.
    parser.add_argument("--testing", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--check-only", action="store_true", help=argparse.SUPPRESS)  # the default now
    return parser.parse_args(argv)


def _catch_up(ws: Workshop, todo: list[Step]) -> bool:
    """Run every missing step that has a fix, in order, and say how each went."""
    failed = False
    for step in todo:
        if not step.fix:
            continue
        print(f"\n{step.where}: {step.what}")
        if step.fix(ws):
            print("  ✓ Done")
        else:
            failed = True
            print(f"  ✗ Couldn't finish {step.where}. Later steps may fail until it's done.")
    return failed


def main(argv: list[str] | None = None) -> None:
    args = _parse_args(argv)
    if args.testing:
        os.environ["WORKSHOP_ASSUME_YES"] = "1"
    ws = require_deployment("uv run workshop-status")
    print("Checking your workshop environment...\n")
    results = []
    for step in steps():
        try:
            ok = step.done(ws)
        except Exception as exc:
            print(f"  ? {step.where}: couldn't check ({exc})")
            ok = None
        else:
            print(f"  {'✓' if ok else '✗'} {step.where}: {step.what}")
        results.append((step, ok))

    todo = [step for step, ok in results if ok is False]
    if not todo:
        if any(ok is None for _, ok in results):
            print("\n? Some steps couldn't be checked (see above). Run `uv run workshop-status` again in a minute.")
            sys.exit(1)
        print("\n✓ Every step so far is done.")
        return
    if args.testing:
        print("\n--testing: running the missing steps for you.")
        sys.exit(1 if _catch_up(ws, todo) else 0)
    point_to_step(todo[0], ws.root)
    sys.exit(1)


if __name__ == "__main__":
    main()
