"""Reset the Agentic Pit Decisions lab for a fresh race.

Stops the lab's Flink statements, drops the objects the lab creates — the
`car_state` and `pit_decisions` tables, the `pit_strategy_agent`, and their
backing topics and Schema Registry subjects — and clears the race data already
sitting in the source topics.

    uv run f1-reset                  # blank slate
    uv run f1-reset --with-labs      # ...then rebuild Steps 1 and 3 from docs/agentic-pit-decisions/sql
    uv run f1-reset --keep-source    # keep the accumulated race data

**Only this lab's statements are stopped.** The environment is shared with the
Flight Disruptions lab, so reset stops a running statement only when its SQL
names one of the lab's objects (`LAB_OBJECTS`); the airline pipeline keeps running.

**Stop `f1-race` first.** Clearing the source topics while the simulator is still
producing puts the records straight back, so reset refuses while a local
`f1-race` runs (`--force` overrides).

**Reset does not restart the race.** `race_standings` is read from `latest`, so a
Step 1 statement submitted after the race starts never sees the standings
versions its first laps need, and `car_state` silently drops them. Rebuild Step 1
before `uv run f1-race`; `--with-labs` does the rebuild for you.

The source topics are *truncated*, not deleted: `car_telemetry` and
`race_standings` are Terraform-owned (terraform/agentic-pit-decisions), so
deleting them would break Terraform state and the schemas the simulator produces
against. `race_standings` is compacted, and Kafka refuses delete-records on a
compacted topic; that is reported, not worked around, because lap 0 of the next
race overwrites all 22 keys.

Every step reports whether it worked, and anything that did not work exits nonzero.
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time

import requests

from scripts.pit_decisions.card import load_card
from scripts.pit_decisions.labs import create_lab_objects
from scripts.pit_decisions.sql_shell import FlinkSession
from scripts.terraform import get_project_root

# Topics created by the lab's own statements — deleted (the steps recreate them).
LAB_TOPICS = ["car_state", "pit_decisions"]

# Terraform-owned source topics the simulator produces to. Truncated, never deleted.
SOURCE_TOPICS = ["car_telemetry", "race_standings"]

# A running statement belongs to this lab when its SQL names one of these.
LAB_OBJECTS = (*LAB_TOPICS, *SOURCE_TOPICS, "pit_strategy_agent", "driver_race_history", "llm_textgen_model")

# Dropped after the statements stop, so no reader is left to break.
LAB_DROPS = [
    "DROP TABLE IF EXISTS `car_state`",
    "DROP TABLE IF EXISTS `pit_decisions`",
    "DROP AGENT IF EXISTS `pit_strategy_agent`",
]

TERMINAL_PHASES = {"COMPLETED", "FAILED", "STOPPED"}

# argv tokens that mean a local `uv run f1-race` is producing.
LOCAL_RACE_TOKENS = ("f1-race", "scripts.pit_decisions.race")


def is_local_race(args: str) -> bool:
    """Whether one process command line is a running `f1-race`."""
    for token in args.split():
        for marker in LOCAL_RACE_TOKENS:
            if token == marker or token.endswith(f"/{marker}"):
                return True
    return False


def local_race_processes() -> list[str]:
    """Command lines of any local `uv run f1-race` currently producing (best-effort)."""
    try:
        result = subprocess.run(["ps", "-eo", "pid=,args="], capture_output=True, text=True, timeout=15)
    except Exception:
        return []
    me = os.getpid()
    found = []
    for line in result.stdout.splitlines():
        pid_text, _, args = line.strip().partition(" ")
        if pid_text.isdigit() and int(pid_text) != me and is_local_race(args):
            found.append(f"{pid_text}  {args.strip()}")
    return found


# --- Flink -----------------------------------------------------------------


def _statements(session: FlinkSession) -> list[dict]:
    statements, url = [], f"{session.base}?page_size=100"
    while url:
        page = requests.get(url, auth=session.auth, timeout=30)
        page.raise_for_status()
        body = page.json()
        statements += body.get("data", [])
        url = (body.get("metadata") or {}).get("next") or ""
    return statements


def is_lab_statement(sql: str) -> bool:
    return any(name in sql for name in LAB_OBJECTS)


def stop_lab_statements(session: FlinkSession, timeout: int = 90) -> list[str]:
    """Delete every live statement that touches this lab's objects, then wait for them to go."""
    try:
        running = [
            s["name"]
            for s in _statements(session)
            if (s.get("status") or {}).get("phase") not in (*TERMINAL_PHASES, "DELETING")
            and is_lab_statement((s.get("spec") or {}).get("statement", ""))
        ]
    except Exception as e:
        return [f"could not list Flink statements ({e})"]

    if not running:
        print("  No running lab statements found")
        return []

    problems = []
    for name in running:
        try:
            requests.delete(f"{session.base}/{name}", auth=session.auth, timeout=30).raise_for_status()
            print(f"  {name}: deleted")
        except Exception as e:
            problems.append(f"could not stop Flink statement {name} ({e})")

    deadline, pending = time.time() + timeout, set(running)
    while pending and time.time() < deadline:
        try:
            pending = {
                s["name"]
                for s in _statements(session)
                if s["name"] in pending and (s.get("status") or {}).get("phase") != "DELETING"
            }
        except Exception as e:
            return [*problems, f"could not confirm Flink statements stopped ({e})"]
        if pending:
            time.sleep(3)
    if pending:
        problems.append(f"Flink statements did not stop within {timeout}s: {', '.join(sorted(pending))}")
    return problems


def _wait_terminal(session: FlinkSession, name: str, timeout: int) -> tuple[str, str]:
    phase, detail = "UNKNOWN", ""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            status = requests.get(f"{session.base}/{name}", auth=session.auth, timeout=30).json().get("status") or {}
        except Exception as e:
            return "UNREACHABLE", str(e)
        phase, detail = status.get("phase", "UNKNOWN"), (status.get("detail") or "").strip()
        if phase in TERMINAL_PHASES:
            return phase, detail
        time.sleep(2)
    return f"TIMED OUT after {timeout}s in {phase}", detail


def drop_lab_objects(session: FlinkSession, timeout: int = 180) -> list[str]:
    """Submit each DROP and wait for COMPLETED, so a rebuild right after can't race it."""
    problems = []
    for sql in LAB_DROPS:
        try:
            name = session.submit(sql)
        except Exception as e:
            problems.append(f"{sql} was not submitted ({e})")
            continue
        phase, detail = _wait_terminal(session, name, timeout)
        print(f"  {sql}: {phase}{f' — {detail}' if detail and phase != 'COMPLETED' else ''}")
        if phase != "COMPLETED":
            problems.append(f"{sql} did not complete (last phase {phase}{f': {detail}' if detail else ''})")
        requests.delete(f"{session.base}/{name}", auth=session.auth, timeout=30)
    return problems


# --- Kafka / Schema Registry ------------------------------------------------


def kafka_admin(card: dict[str, str]):
    from confluent_kafka.admin import AdminClient

    return AdminClient(
        {
            "bootstrap.servers": card["F1_KAFKA_BOOTSTRAP"].split("://", 1)[-1],
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": "PLAIN",
            "sasl.username": card["F1_KAFKA_API_KEY"],
            "sasl.password": card["F1_KAFKA_API_SECRET"],
        }
    )


def delete_lab_topics(admin, card: dict[str, str], present: set[str]) -> list[str]:
    """Delete leftover lab topics and hard-delete their Schema Registry subjects.

    Dropping a Flink table deletes its topic but leaves `<topic>-key` and
    `<topic>-value`, and a soft-deleted subject still blocks re-registering an
    incompatible schema, hence the permanent pass.
    """
    problems = []
    leftover = [t for t in LAB_TOPICS if t in present]
    for topic, future in (admin.delete_topics(leftover, operation_timeout=60) if leftover else {}).items():
        try:
            future.result()
            print(f"  Topic {topic}: deleted")
        except Exception as e:
            problems.append(f"could not delete topic {topic} ({e})")
    for topic in LAB_TOPICS:
        if topic not in leftover:
            print(f"  Topic {topic}: already gone")

    sr = card["F1_SCHEMA_REGISTRY_URL"].rstrip("/")
    auth = (card["F1_SR_API_KEY"], card["F1_SR_API_SECRET"])
    for subject in (f"{t}-{part}" for t in LAB_TOPICS for part in ("key", "value")):
        requests.delete(f"{sr}/subjects/{subject}", auth=auth, timeout=30)
        resp = requests.delete(f"{sr}/subjects/{subject}?permanent=true", auth=auth, timeout=30)
        if resp.ok:
            print(f"  SR {subject}: cleaned")
        elif resp.status_code == 404:
            print(f"  SR {subject}: already gone")
        else:
            problems.append(
                f"could not delete Schema Registry subject {subject} (HTTP {resp.status_code}: {resp.text[:150]})"
            )
    return problems


def truncate_topics(admin, present: set[str]) -> list[str]:
    """Delete every record in the source topics, leaving the topics and schemas in place."""
    from confluent_kafka import OFFSET_END, TopicPartition

    problems = []
    for topic in SOURCE_TOPICS:
        if topic not in present:
            print(f"  Topic {topic}: MISSING — nothing to clear")
            problems.append(f"source topic {topic} does not exist (re-run `uv run deploy` to recreate it)")
            continue
        partitions = admin.list_topics(topic=topic, timeout=30).topics[topic].partitions
        futures = admin.delete_records([TopicPartition(topic, p, OFFSET_END) for p in partitions])
        errors = []
        for future in futures.values():
            try:
                future.result()
            except Exception as e:
                errors.append(e)
        if not errors:
            print(f"  Topic {topic}: cleared ({len(partitions)} partition(s))")
        elif any("POLICY_VIOLATION" in str(e) for e in errors):
            print(f"  Topic {topic}: kept (compacted topic — the next race overwrites every key on lap 0)")
        else:
            problems.append(f"could not clear {len(errors)} partition(s) of {topic}")
    return problems


def main() -> None:
    parser = argparse.ArgumentParser(description="Reset the Agentic Pit Decisions lab for a fresh race")
    parser.add_argument("--creds", help="Credential card (default: the one `uv run deploy` wrote)")
    parser.add_argument("--keep-source", action="store_true", help="Leave car_telemetry / race_standings data in place")
    parser.add_argument(
        "--with-labs", action="store_true", help="Also rebuild Steps 1 and 3 from docs/agentic-pit-decisions/sql"
    )
    parser.add_argument("--force", action="store_true", help="Proceed even though a local `uv run f1-race` is running")
    args = parser.parse_args()

    root = get_project_root()
    _, card = load_card(args.creds, root)
    print(f"=== Agentic Pit Decisions reset ({card.get('F1_ENVIRONMENT_ID')}) ===\n")

    local = local_race_processes()
    if local and not args.force:
        print("A local race simulator is still producing:")
        for line in local:
            print(f"  {line}")
        print("\nStop it (Ctrl-C in that terminal) and re-run, or pass --force.")
        sys.exit(1)

    session = FlinkSession(card)
    problems: list[str] = []

    print("1. Stopping the lab's Flink statements...")
    problems += stop_lab_statements(session)

    print("\n2. Dropping car_state, pit_decisions, pit_strategy_agent...")
    problems += drop_lab_objects(session)

    admin = kafka_admin(card)
    try:
        present = set(admin.list_topics(timeout=30).topics)
    except Exception as e:
        present = None
        problems.append(f"could not list topics ({e})")

    if present is not None:
        print("\n3. Dropping lab topics and Schema Registry subjects...")
        problems += delete_lab_topics(admin, card, present)
        if args.keep_source:
            print("\n4. Keeping source topic data (--keep-source).")
        else:
            print("\n4. Clearing race data from the source topics...")
            problems += truncate_topics(admin, present)

    if args.with_labs:
        print("\n5. Rebuilding the lab objects...")
        if problems:
            print("  Skipped — the cleanup above did not finish cleanly.")
        elif not create_lab_objects(card, root):
            problems.append("the lab objects were not rebuilt")

    if problems:
        print("\n=== Reset INCOMPLETE ===")
        for problem in problems:
            print(f"  - {problem}")
        print("\nFix the above and re-run `uv run f1-reset`.")
        sys.exit(1)

    print("\n=== Reset complete ===")
    if args.with_labs:
        print("Lab objects rebuilt. Start a fresh race: uv run f1-race")
    else:
        print("Next: rebuild Step 1 (and Step 3) with `uv run f1-sql`, then `uv run f1-race`.")
        print("In that order: a Step 1 statement submitted after the race starts misses its first laps.")


if __name__ == "__main__":
    main()
