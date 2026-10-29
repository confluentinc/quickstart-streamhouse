"""`uv run reset-demo-2` removes only what Demo 2 creates on screen."""

import json
from datetime import datetime
from pathlib import Path

import pytest

from scripts import airport_datagen, demo_reset

SQL = Path(__file__).parent.parent / "terraform" / "airline-demo" / "sql"
STATEMENTS = {  # every statement deploy creates, by name, with its SQL file
    "airport-table-flight-status": "20", "airport-table-passenger-itineraries": "21",
    "airport-table-hotel-inventory": "22", "airport-table-passenger-recommendations": "23",
    "airport-materialize-passenger-state": "24", "airport-materialize-flight-impact": "25",
    "airport-agent-passenger-state-changes": "26", "airport-agent-impacted-passengers": "27",
    "airport-agent-model": "32", "airport-agent-tool": "29", "airport-agent-passenger-recovery": "30",
    "airport-agent-insert-passenger-recommendations": "31",
    "airport-table-hotel-availability-updates": "33", "airport-insert-hotel-inventory": "34",
}


def _deployed() -> list[dict]:
    return [{"name": name, "statement": next(SQL.glob(f"{n}-*.sql")).read_text()} for name, n in STATEMENTS.items()]


def test_resets_only_demo_2_statements_and_stops_the_insert_first():
    names = demo_reset.demo_2_statements(_deployed())
    assert names[0] == "airport-agent-insert-passenger-recommendations"
    assert set(names) == {"airport-agent-insert-passenger-recommendations", "airport-agent-passenger-recovery",
                          "airport-agent-tool", "airport-agent-model",
                          "airport-agent-passenger-state-changes", "airport-agent-impacted-passengers"}


def test_an_on_screen_insert_is_found_by_its_sql():
    live = {"name": "workspace-2026-10-01-123", "statement": next(SQL.glob("31-*.sql")).read_text()}
    assert demo_reset.demo_2_statements([live]) == ["workspace-2026-10-01-123"]


def test_drops_cover_every_object_demo_2_creates():
    created = "".join(next(SQL.glob(f"{n}-*.sql")).read_text() for n in ("26", "27", "29", "30", "32"))
    for sql in demo_reset.DROPS:
        assert sql.split()[-1] in created
    assert "DROP MODEL IF EXISTS passenger_recovery_model" in demo_reset.DROPS  # 2.3a creates the agent's model


def test_ra417_offer_keys_cover_every_connecting_passenger():
    plan = airport_datagen.build_plan(datetime(2026, 10, 1, 17), history=False)
    keys = demo_reset.ra417_offer_keys(plan)
    assert len(keys) == 2 * airport_datagen.HERO_CONNECTING
    assert all(k.startswith("P-1001-417-") and k[-3:] in ("-O1", "-O2") for k in keys)


class _CLI:
    def __init__(self, status):
        self.calls, self.status = [], status

    def __call__(self, cmd, **_):
        self.calls.append(cmd)
        out = json.dumps({"status": self.status}) if cmd[3] == "describe" else ""
        return type("Result", (), {"stdout": out, "stderr": "", "returncode": 0})()


CORE = {"cloud_region": "us-east-1", "confluent_environment_id": "env-1",
        "confluent_flink_compute_pool_id": "lfcp-1", "app_manager_service_account_id": "sa-1",
        "confluent_environment_display_name": "RIVER-AIR-PROD-ENV",
        "confluent_kafka_cluster_display_name": "RIVER-AIR-PROD-CLUSTER"}


def test_drop_runs_in_the_demo_catalog_then_cleans_up(monkeypatch):
    cli = _CLI("COMPLETED")
    monkeypatch.setattr(demo_reset.subprocess, "run", cli)
    demo_reset.Flink(CORE).run("DROP TOOL IF EXISTS hotel_inventory_live_context")
    create, _, delete = cli.calls
    assert create[create.index("--property") + 1] == (
        "sql.current-catalog=RIVER-AIR-PROD-ENV,sql.current-database=RIVER-AIR-PROD-CLUSTER")
    assert create[create.index("--service-account") + 1] == "sa-1"
    assert delete[3:5] == ["delete", create[4]]


def test_failed_drop_stops_the_reset(monkeypatch):
    monkeypatch.setattr(demo_reset.subprocess, "run", _CLI("FAILED"))
    with pytest.raises(RuntimeError, match="FAILED"):
        demo_reset.Flink(CORE).run("DROP AGENT IF EXISTS passenger_recovery_agent")
