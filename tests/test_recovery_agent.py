"""The recovery agent's SQL, Terraform, and deploy step agree on names and columns."""

import re
from datetime import datetime
from pathlib import Path

from scripts import airport_datagen, deploy, setup_rtce

DEMO = Path(__file__).parent.parent / "terraform" / "airline-demo"
SQL = DEMO / "sql"


def _sql(prefix: str) -> str:
    return next(SQL.glob(f"{prefix}-*.sql")).read_text()


def test_statements_chain_by_name():
    assert "CREATE MODEL IF NOT EXISTS passenger_recovery_model" in _sql("32")
    assert f"USING CONNECTION `{setup_rtce.AGENT_CONNECTION}`" in _sql("29")
    agent = _sql("30")
    assert "CREATE AGENT IF NOT EXISTS passenger_recovery_agent" in agent
    assert "USING MODEL passenger_recovery_model" in agent
    assert "USING TOOLS hotel_inventory_live_context" in agent
    assert "FROM passenger_state_changes" in _sql("27")
    assert "FROM impacted_passengers" in _sql("31")
    assert "AI_RUN_AGENT(\n    'passenger_recovery_agent'" in _sql("31")


def test_agent_runs_on_the_confluent_hosted_model():
    assert "'provider' = 'confluent'" in _sql("32")
    assert not list(SQL.glob("28-*.sql"))  # the Bedrock model is gone
    main = (DEMO / "main.tf").read_text()
    assert '"sql/32-model-passenger-recovery-native.sql"' in main
    assert "bedrock_enabled" not in main


def test_agent_handles_the_generators_hero_flight():
    hero = f"{airport_datagen.AIRLINE}{airport_datagen.HERO_NUMBER}-"
    assert f"inbound_flight_id = CONCAT('{hero}'" in _sql("27")


def test_insert_writes_every_recommendation_column_in_order():
    table = re.findall(r"^  `?(\w+)`? [A-Z]", _sql("23"), re.M)
    final_select = _sql("31").rsplit("\nSELECT\n", 1)[1].split("\nFROM offers")[0]
    written = [line.strip().rstrip(",").split()[-1].strip("`") for line in final_select.splitlines()]
    assert written == [c for c in table if c != "PRIMARY"]


def test_agent_reply_contract_matches_the_parser():
    fields = set(re.findall(r"O[12]_(\w+)=", _sql("30")))
    parsed = set(re.findall(r"CONCAT\('O', o\.n, '_(\w+)=", _sql("31")))
    assert fields == parsed == {"FLIGHT", "HOTEL", "COST"}


def test_tool_and_agent_use_only_supported_options():
    tool_options = set(re.findall(r"'(\w+)' = ", _sql("29")))
    assert tool_options <= {"type", "allowed_tools", "request_timeout", "max_retries", "headers"}
    assert "'handle_exception' = 'continue'" in _sql("30")


class _Run:
    """Stands in for subprocess.run and records each CLI call."""

    def __init__(self, stdout="", returncode=0):
        self.calls, self.stdout, self.returncode = [], stdout, returncode

    def __call__(self, cmd, **_):
        self.calls.append(cmd)
        return type("Result", (), {"stdout": self.stdout, "stderr": "", "returncode": self.returncode})()


def test_stale_rtce_key_is_detected(monkeypatch):
    run = _Run(stdout='[{"key": "NEWKEY"}]')
    monkeypatch.setattr(setup_rtce.subprocess, "run", run)
    assert setup_rtce._key_owned_by("sa-1", "NEWKEY")
    assert not setup_rtce._key_owned_by("sa-1", "OLDKEY")


def test_existing_agent_connection_gets_the_current_key(monkeypatch):
    run = _Run()
    monkeypatch.setattr(setup_rtce.subprocess, "run", run)
    infra = {"env_id": "env-1", "region": "us-east-1", "org_id": "o", "cluster_id": "lkc-1"}
    assert setup_rtce.create_agent_connection(infra, "KEY", "SECRET")
    update = run.calls[-1]
    assert update[:4] == ["confluent", "flink", "connection", "update"]
    assert update[update.index("--username") + 1] == "KEY"
    assert update[update.index("--transport-type") + 1] == "STREAMABLE_HTTP"


def test_deploy_targets_exist_and_sql_files_exist():
    main = (DEMO / "main.tf").read_text() + (DEMO / "webhook.tf").read_text()
    for target in deploy.AGENT_TARGETS:
        kind, name = target.split(".")
        assert f'resource "{kind}" "{name}"' in main
    for path in re.findall(r'"(sql/[\w-]+\.sql)"', main):
        assert (DEMO / path).exists(), path


def test_video_2_deploy_stops_before_what_demo_2_creates_on_screen(monkeypatch, tmp_path):
    main = (DEMO / "main.tf").read_text()
    for target in deploy.VIDEO_2_TARGETS:
        assert target.split(".")[1].split("[")[0] in main
    assert not any("agent_setup" in target for target in deploy.VIDEO_2_TARGETS)  # 2.3a creates the model

    calls = []
    monkeypatch.setattr(deploy.setup_rtce, "main", lambda argv: calls.append("rtce"))
    monkeypatch.setattr(deploy, "_start_agent", lambda root, targets: calls.append(targets) or True)
    monkeypatch.setattr(deploy, "apply_tag_colors", lambda root: calls.append("colors"))
    monkeypatch.setattr(deploy, "_print_env_name", lambda root: None)

    def datagen(root, agent, history=True, speed=1.0, stream=True):
        calls.append(("datagen", history, speed, stream))
        return datetime(2026, 10, 2, 17, 0)

    monkeypatch.setattr(deploy, "_run_datagen", datagen)
    deploy._finish(tmp_path, "none", video_2=True, speed=4)
    assert calls == [("datagen", True, 4, False), "rtce", deploy.VIDEO_2_TARGETS,
                     ("datagen", False, 4, True), "colors"]
