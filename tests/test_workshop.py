"""The self-service workshop: its guide pastes the pipeline's own SQL, and its deploy stops where the guide starts."""

import json
import re
from datetime import datetime
from pathlib import Path

import pytest

from scripts import airport_datagen, demo_reset, deploy, setup_rtce

ROOT = Path(__file__).parent.parent
SQL = ROOT / "terraform" / "airline-demo" / "sql"
GUIDE = ROOT / "docs" / "workshop.md"
UTC = "SET 'sql.local-time-zone' = 'UTC';"
# What attendees paste to build the pipeline, in order.
CHAIN = ("24", "25", "26", "27", "32", "29", "30", "31")


def _statement(sql: str) -> str:
    """The SQL without comment lines or trailing spaces, for comparing the guide with the files."""
    return "\n".join(line.rstrip() for line in sql.strip().splitlines() if not line.lstrip().startswith("--")).strip()


def _sql_blocks() -> list[str]:
    """Every ```sql block in the guide, dedented by its fence's indent the way GitHub renders it."""
    blocks = []
    for indent, body in re.findall(r"^( *)```sql\n(.*?)^\1```", GUIDE.read_text(), re.M | re.S):
        blocks.append("\n".join(line[len(indent):] if line.startswith(indent) else line.lstrip()
                                for line in body.splitlines()))
    return blocks


def _pipeline_blocks() -> list[str]:
    """The guide's CREATE and INSERT statements, the ones that build the pipeline."""
    return [block for block in _sql_blocks()
            if block.removeprefix(UTC).lstrip().startswith(("CREATE", "INSERT"))]


def _files() -> dict[str, str]:
    return {path.name.split("-")[0]: _statement(path.read_text()) for path in SQL.glob("*.sql")}


def test_guide_pastes_the_pipeline_files_in_order():
    files = {sql: number for number, sql in _files().items()}
    pasted = []
    for block in _pipeline_blocks():
        statement = _statement(block.removeprefix(UTC))
        pasted.append(files.get(statement, statement[:60]))
    assert tuple(pasted) == CHAIN


def test_guide_model_is_the_hosted_one_the_agent_uses():
    files = _files()
    agent_model = re.search(r"USING MODEL (\w+)", files["30"]).group(1)
    assert "'provider' = 'confluent'" in files["32"]
    assert f"CREATE MODEL IF NOT EXISTS {agent_model}\n" in files["32"]


def test_statements_that_read_the_clock_run_in_utc():
    for block in _pipeline_blocks():
        assert ("CURRENT_TIMESTAMP" in block) == block.startswith(UTC), block[:80]


def test_workshop_mode_sticks_to_the_checkout(monkeypatch, tmp_path):
    saved = {}
    monkeypatch.setattr(deploy, "_save_env_safe", lambda _file, key, value: saved.update({key: value}))
    creds = {}
    assert not deploy._workshop_mode(False, tmp_path / "credentials.env", creds)
    assert deploy._workshop_mode(True, tmp_path / "credentials.env", creds)
    assert saved == {deploy.WORKSHOP: "true"} == {k: v for k, v in creds.items() if k == deploy.WORKSHOP}
    assert deploy._workshop_mode(False, tmp_path / "credentials.env", creds)  # a later run without the flag


def test_workshop_deploy_stops_where_the_guide_starts(monkeypatch, tmp_path):
    calls = []
    monkeypatch.setattr(deploy.setup_rtce, "ensure_global_key", lambda *_: calls.append("key") or "KEY")
    monkeypatch.setattr(deploy, "apply_tag_colors", lambda root: calls.append("colors"))
    monkeypatch.setattr(deploy, "_print_env_name", lambda root: None)
    for name in ("_run_datagen", "_start_agent"):
        monkeypatch.setattr(deploy, name, lambda *args, **kwargs: pytest.fail("the attendee does this"))
    monkeypatch.setattr(deploy.setup_rtce, "main", lambda argv: pytest.fail("the attendee turns on RTCE"))
    deploy._finish_workshop(tmp_path)
    assert calls == ["key", "colors"]


def test_targeted_applies_include_the_tables_the_workshop_leaves_out():
    for target in ("confluent_flink_statement.passenger_state", "confluent_flink_statement.flight_impact"):
        assert target in deploy.AGENT_TARGETS
        assert target in demo_reset.SERVING_TARGETS


def test_reset_demo_2_leaves_a_workshop_alone(monkeypatch, tmp_path):
    (tmp_path / "terraform" / "core").mkdir(parents=True)
    (tmp_path / "terraform" / "core" / "terraform.tfstate").write_text("{}")
    (tmp_path / "credentials.env").write_text(f"{deploy.WORKSHOP}=true\n")
    monkeypatch.setattr(demo_reset, "get_project_root", lambda: tmp_path)
    monkeypatch.setattr(demo_reset, "run_terraform_output", lambda _: pytest.fail("the attendees' agent stays"))
    with pytest.raises(SystemExit, match="workshop mode"):
        demo_reset.main(["--yes"])


def test_workshop_terraform_builds_only_the_source_tables():
    main = (SQL.parent / "main.tf").read_text()
    for resource in ("passenger_state", "flight_impact"):
        block = main.split(f'resource "confluent_flink_statement" "{resource}" {{', 1)[1]
        assert block.lstrip().startswith("count = var.workshop ? 0 : 1")
    tables = re.search(r"tables = merge\((.*?)\n  \)", main, re.S).group(1)
    always, unless_workshop = tables.split("var.workshop ? {} :")
    assert set(re.findall(r"sql/(\d+)-", always)) == {"20", "21", "22", "23"}
    assert set(re.findall(r"sql/(\d+)-", unless_workshop)) == {"33"}


def test_a_resumed_stream_catches_up_with_each_keys_latest_update():
    start = datetime(2026, 10, 27, 17)
    at = [start.replace(minute=m) for m in (5, 6, 7, 30)]
    events = [(at[0], "flight_status", "RA417", {"delay": 10}), (at[1], "hotel_inventory", "Hilton", {"rooms": 9}),
              (at[2], "flight_status", "RA417", {"delay": 20}), (at[3], "flight_status", "RA417", {"delay": 90})]
    caught_up = airport_datagen._catch_up(events, start, now=start.replace(minute=20), speed=1)
    assert caught_up == [events[1], events[2], events[3]]
    assert airport_datagen._catch_up(events, start, now=start, speed=1) == events


def test_resume_picks_up_the_last_streams_clock(monkeypatch, tmp_path):
    monkeypatch.setattr(airport_datagen, "get_project_root", lambda: tmp_path)
    runs = []
    monkeypatch.setattr(airport_datagen, "run", lambda start, *args, **kwargs: runs.append((start, kwargs)))
    with pytest.raises(SystemExit):
        airport_datagen.main(["--resume"])
    (tmp_path / "tmp").mkdir()
    (tmp_path / "tmp" / "datagen.start").write_text("2026-10-27T17:00:00 2")
    airport_datagen.main(["--resume", "--offers", "generator"])
    start, kwargs = runs[0]
    assert start == datetime(2026, 10, 27, 17)
    assert kwargs["stream_only"] and kwargs["speed"] == 2 and not kwargs["agent_offers"]


def test_workshop_mode_refuses_a_checkout_with_a_full_deployment(tmp_path):
    state = tmp_path / "terraform" / "airline-demo" / "terraform.tfstate"
    state.parent.mkdir(parents=True)
    state.write_text("{}")
    with pytest.raises(SystemExit):
        deploy._workshop_mode(True, tmp_path / "credentials.env", {})
    assert deploy._workshop_mode(False, tmp_path / "credentials.env", {deploy.WORKSHOP: "true"})


def test_stream_names_a_passenger_who_misses_their_connection():
    plan = airport_datagen.build_plan(datetime(2026, 10, 27, 17), history=False)
    passenger = airport_datagen.passenger_to_follow(plan)
    assert passenger.inbound is plan.live.hero
    assert passenger.connecting is not None and passenger.impacted_at is not None


class _Response:
    def __init__(self, body: dict):
        self.body = json.dumps(body).encode()

    def read(self):
        return self.body

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False


def test_lightning_run_returns_rows_like_the_app(monkeypatch):
    sent = {}

    def urlopen(request, timeout):
        sent.update(url=request.full_url, body=json.loads(request.data), auth=request.headers["Authorization"])
        return _Response({"result": {"schema": {"columns": [{"name": "KEY"}, {"name": "AVAILABLE_ROOMS"}]},
                                     "data": [["Grand Hyatt at SFO", 12]]}})

    monkeypatch.setattr(setup_rtce.urllib.request, "urlopen", urlopen)
    infra = {"region": "us-east-1", "org_id": "o", "env_id": "env-1", "cluster_id": "lkc-1"}
    rows = setup_rtce.run_lightning_query(infra, "KEY", "SECRET", "hotel_inventory")
    assert rows == [{"key": "Grand Hyatt at SFO", "available_rooms": 12}]
    assert sent["url"] == "https://sql.us-east-1.aws.confluent.cloud/query/v1alpha1"
    assert sent["body"] == {"catalog_name": "env-1", "database_name": "lkc-1",
                            "query": "SELECT * FROM `hotel_inventory` LIMIT 10"}
    assert sent["auth"].startswith("Basic ")


def test_a_failed_key_check_keeps_the_stored_key(monkeypatch):
    failed = type("Result", (), {"stdout": "", "stderr": "not logged in", "returncode": 1})()
    monkeypatch.setattr(setup_rtce.subprocess, "run", lambda *args, **kwargs: failed)
    assert setup_rtce._key_owned_by("sa-1", "KEY")


@pytest.mark.parametrize("argv", [["--run"], ["--agent-connection", "--lightning", "flight_status"],
                                  ["--agent-connection", "--client", "claude"]])
def test_setup_rtce_rejects_flags_that_dont_combine(argv):
    with pytest.raises(SystemExit):
        setup_rtce._parse_args(argv)
