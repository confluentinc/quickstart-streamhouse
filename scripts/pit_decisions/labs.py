"""Rebuild the Agentic Pit Decisions lab objects from the reference SQL.

Shared by ``uv run f1-reset --with-labs`` and ``uv run deploy --with-labs``.

``F1_ANOMALY_FN=ai`` switches Step 1 from the default GA ARIMA implementation to the
foundation-model Granite/``AI_DETECT_ANOMALIES`` one — see ``anomaly_sql_filename``.
"""

from __future__ import annotations

import os
from pathlib import Path

SQL_DIR = Path("docs") / "agentic-pit-decisions" / "sql"

# Step 1 has two implementations emitting the identical `car_state` schema, so
# everything downstream is indifferent to which one ran. The default uses GA
# `ML_DETECT_ANOMALIES` (ARIMA); the foundation-model
# `AI_DETECT_ANOMALIES` implementation remains available as an opt-in.
ANOMALY_SQL = "enrichment_anomaly.sql"
ANOMALY_SQL_AI = "enrichment_anomaly_ai.sql"

# The canonical lab SQL, rebuilt by --with-labs. Dependency-ordered:
# pit_decisions validates against both `car_state` and `pit_strategy_agent`, so a
# failure part-way through stops the rest (there is nothing useful to submit
# after a broken link).
LAB_BUILDS = [
    (ANOMALY_SQL, "car_state"),
    ("streaming_agent_create_agent.sql", "pit_strategy_agent"),
    ("streaming_agent_pit_decisions.sql", "pit_decisions"),
]


def anomaly_sql_filename() -> str:
    """Which Step 1 implementation `--with-labs` should submit.

    ``F1_ANOMALY_FN=ai`` selects the foundation-model Granite variant; unset (or
    ``ml``) keeps the ARIMA default. Deliberately an explicit switch rather than
    anything automatic: the ``ai`` path currently produces a `car_state` that never
    flags an anomaly, and it must be opted into knowingly rather than fallen into.
    """
    return ANOMALY_SQL_AI if os.environ.get("F1_ANOMALY_FN") == "ai" else ANOMALY_SQL


def _hint_anomaly_fallback(filename: str) -> None:
    """Point back at the default when the opted-in Granite statement is what failed.

    Only reachable when someone set ``F1_ANOMALY_FN=ai``. `AI_DETECT_ANOMALIES`
    needs an Early Access Program that a fresh org does not have, and the API's own
    message ("Function ... does not exist or you do not have permission to access
    it") does not say what to do about it.
    """
    if filename != ANOMALY_SQL_AI:
        return
    print(
        "    F1_ANOMALY_FN=ai selected AI_DETECT_ANOMALIES (Granite), which needs the AI-functions Early Access "
        "Program. Unset it to use the GA ARIMA default:"
    )
    print("      unset F1_ANOMALY_FN && <the command you just ran>")


def create_lab_objects(card: dict[str, str], root: Path) -> bool:
    """Submit the canonical lab SQL and wait for each statement to come up.

    Waiting matters: ``CREATE AGENT`` must reach COMPLETED and ``car_state`` must
    exist before the pit_decisions statement will pass validation.
    Fire-and-forget submission would race and fail with "table does not exist".
    """
    from scripts.pit_decisions.sql_shell import FlinkSession

    session = FlinkSession(card)

    for filename, creates in LAB_BUILDS:
        if filename == ANOMALY_SQL:
            filename = anomaly_sql_filename()

        path = root / SQL_DIR / filename
        if not path.exists():
            print(f"  {creates}: missing {path}")
            return False

        # Same normalization as `f1-sql --exec`: the trailing ';' in the .sql file
        # is a shell-shell convention, not part of the statement.
        sql = path.read_text().strip().rstrip(";").strip()

        try:
            name = session.submit(sql)
        except Exception as e:
            print(f"  {creates}: submit failed — {e}")
            _hint_anomaly_fallback(filename)
            return False

        status = session.wait(name, timeout=180)
        phase = status["status"]["phase"]
        if phase in ("RUNNING", "COMPLETED"):
            print(f"  {creates}: {phase}  ({filename})")
            continue

        detail = (status.get("status") or {}).get("detail", "").strip()
        print(f"  {creates}: {phase} — {detail or 'no detail returned'}")
        _hint_anomaly_fallback(filename)
        return False

    return True
