"""An attendee who skips a workshop step is told which one and where it is in the guide, instead of a raw error."""

import json
import subprocess
from io import BytesIO
from pathlib import Path

import pytest

from scripts import setup_rtce, workshop_checks
from scripts.workshop_checks import LAB2, LIGHTNING_TOPICS, Workshop

CORE = {
    "confluent_environment_id": "env-1",
    "confluent_kafka_cluster_id": "lkc-1",
    "confluent_organization_id": "org-1",
    "cloud_region": "us-east-1",
    "cloud_provider": "AWS",
}


class FakeWorkshop(Workshop):
    """A Workshop whose cluster is a dict: no Confluent CLI, Kafka, or Flink behind it."""

    def __init__(self, tmp_path, phases=None, topics=None):
        super().__init__(tmp_path, tmp_path / "credentials.env", {}, CORE)
        self.infra = {
            "cloud": "aws",
            "org_id": "org-1",
            "env_id": "env-1",
            "cluster_id": "lkc-1",
            "region": "us-east-1",
        }
        self.fake_phases = dict(phases or {})
        self.fake_topics = set(topics or [])
        self.sql_run: list[str] = []

    def phases(self):
        return dict(self.fake_phases)

    def kafka_topics(self):
        return set(self.fake_topics)

    def run_sql(self, filename, properties=None):
        self.sql_run.append(filename)
        return True


@pytest.fixture(autouse=True)
def no_waiting(monkeypatch):
    monkeypatch.setattr(workshop_checks.time, "sleep", lambda _seconds: None)


@pytest.fixture(autouse=True)
def attendee(monkeypatch):
    """Tests run as an attendee unless they ask for the bot switch."""
    monkeypatch.delenv("WORKSHOP_ASSUME_YES", raising=False)


@pytest.fixture
def bot(monkeypatch):
    monkeypatch.setenv("WORKSHOP_ASSUME_YES", "1")


def _active(topics):
    return {topic: "ACTIVE" for topic in topics}


def test_no_deployment_says_to_run_deploy(tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "pyproject.toml").write_text("", encoding="utf-8")
    with pytest.raises(SystemExit) as exit_info:
        workshop_checks.require_deployment("uv run airport-app")
    message = str(exit_info.value)
    assert "uv run deploy" in message and "uv run airport-app" in message


def test_a_finished_deployment_is_found(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "pyproject.toml").write_text("", encoding="utf-8")
    (tmp_path / "credentials.env").write_text("TF_VAR_workshop=true\n", encoding="utf-8")
    for root, outputs in (("core", CORE), ("airline-demo", {"source_topics": ["flight_status"]})):
        (tmp_path / "terraform" / root).mkdir(parents=True)
        state = {"outputs": {key: {"value": value} for key, value in outputs.items()}}
        (tmp_path / "terraform" / root / "terraform.tfstate").write_text(json.dumps(state), encoding="utf-8")
    ws = workshop_checks.require_deployment("uv run airport-app")
    assert ws.is_workshop
    assert ws.infra["env_id"] == "env-1" and ws.infra["cloud"] == "aws"


def test_only_the_bot_switch_runs_missing_steps(monkeypatch):
    assert workshop_checks.runs_missing_steps() is False
    monkeypatch.setenv("WORKSHOP_ASSUME_YES", "0")
    assert workshop_checks.runs_missing_steps() is False
    monkeypatch.setenv("WORKSHOP_ASSUME_YES", "1")
    assert workshop_checks.runs_missing_steps() is True


def test_nothing_asks_the_attendee_to_run_a_step_for_them():
    source = Path(workshop_checks.__file__).read_text(encoding="utf-8")
    assert "for you now" not in source and "def confirm" not in source


def test_active_lightning_tables_pass_without_a_prompt(tmp_path):
    ws = FakeWorkshop(tmp_path, phases=_active(LIGHTNING_TOPICS))
    assert workshop_checks.ensure_lightning(ws, LIGHTNING_TOPICS, "") is True
    assert ws.sql_run == []


def test_skipping_lab_3_points_to_it_and_enables_nothing(tmp_path, monkeypatch, capsys):
    ws = FakeWorkshop(tmp_path, topics=LIGHTNING_TOPICS)
    ws.root = GUIDE_ROOT  # so the pointer can find the guide's line
    monkeypatch.setattr(setup_rtce, "_enable_rtce_topic", lambda *args: pytest.fail("enabled for the attendee"))
    monkeypatch.setattr(workshop_checks.subprocess, "run", lambda *args, **kwargs: pytest.fail("ran the CLI"))
    assert workshop_checks.ensure_lightning(ws, ["flight_status"], "The dashboard reads them.") is False
    out = capsys.readouterr().out
    assert "`flight_status`" in out and "The dashboard reads them." in out
    assert "Your next step is Lab 3, steps 1 and 2" in out
    assert f"of {workshop_checks.WORKSHOP_GUIDE})" in out  # the line in the guide
    assert "run this command again" in out
    assert "for you now" not in out and ws.sql_run == []


def test_a_bot_that_skips_lab_3_gets_the_lightning_tables_enabled(tmp_path, bot, monkeypatch, capsys):
    ws = FakeWorkshop(tmp_path, topics=LIGHTNING_TOPICS)
    enabled = []

    def enable(key, secret, infra, topic, description):
        enabled.append(topic)
        ws.fake_phases[topic] = "ACTIVE"
        return True

    monkeypatch.setattr(setup_rtce, "_enable_rtce_topic", enable)
    assert workshop_checks.ensure_lightning(ws, ["flight_status", "hotel_inventory"], "Lab 4 needs them.") is True
    assert enabled == ["flight_status", "hotel_inventory"]
    assert "Lab 4 needs them." in capsys.readouterr().out


def test_lab_3_before_lab_2_points_to_lab_2_first(tmp_path, monkeypatch, capsys):
    sources = [t for t in LIGHTNING_TOPICS if t not in ("passenger_state", "flight_impact")]
    ws = FakeWorkshop(tmp_path, phases=_active(sources), topics=sources)
    monkeypatch.setattr(setup_rtce, "_enable_rtce_topic", lambda *args: pytest.fail("enabled before Lab 2"))
    assert workshop_checks.ensure_lightning(ws, LIGHTNING_TOPICS, "") is False
    out = capsys.readouterr().out
    assert "`passenger_state`, `flight_impact` aren't in your cluster yet." in out
    assert "Your next step is Lab 2, steps 3 and 4" in out and "Lab 3, steps" not in out
    assert ws.sql_run == []


def test_a_bot_that_skips_lab_2_gets_the_flink_tables_created_first(tmp_path, bot, monkeypatch):
    sources = [t for t in LIGHTNING_TOPICS if t not in ("passenger_state", "flight_impact")]
    ws = FakeWorkshop(tmp_path, phases=_active(sources), topics=sources)

    def run_sql(filename, properties=None):
        ws.sql_run.append(filename)
        ws.fake_topics.update(t for t, f, _ in LAB2 if f == filename)
        return True

    def enable(key, secret, infra, topic, description):
        assert {"passenger_state", "flight_impact"} <= ws.fake_topics, "enabled before Flink built the topic"
        ws.fake_phases[topic] = "ACTIVE"
        return True

    ws.run_sql = run_sql
    monkeypatch.setattr(setup_rtce, "_enable_rtce_topic", enable)
    assert workshop_checks.ensure_lightning(ws, LIGHTNING_TOPICS, "") is True
    assert ws.sql_run == ["24-serving-passenger-state.sql", "25-serving-flight-impact.sql"]


def test_a_failed_lightning_table_tells_the_attendee_to_turn_it_off_and_on(tmp_path, monkeypatch, capsys):
    ws = FakeWorkshop(tmp_path, phases={"passenger_state": "FAILED"}, topics=["passenger_state", "flight_impact"])
    ws.fake_phases["flight_impact"] = "ACTIVE"
    monkeypatch.setattr(workshop_checks.subprocess, "run", lambda *args, **kwargs: pytest.fail("ran the CLI"))
    monkeypatch.setattr(setup_rtce, "_enable_rtce_topic", lambda *args: pytest.fail("enabled for the attendee"))
    assert workshop_checks.ensure_lightning(ws, ["passenger_state", "flight_impact"], "") is False
    out = capsys.readouterr().out
    assert "failed for: `passenger_state`" in out and "off and on again" in out
    assert "Your next step is Lab 3, steps 1 and 2" in out


def test_a_bot_turns_a_failed_lightning_table_off_and_on_again(tmp_path, bot, monkeypatch):
    ws = FakeWorkshop(tmp_path, phases={"passenger_state": "FAILED"}, topics=["passenger_state", "flight_impact"])
    calls = []
    monkeypatch.setattr(workshop_checks.subprocess, "run", lambda argv, **kwargs: calls.append(argv))

    def enable(key, secret, infra, topic, description):
        ws.fake_phases[topic] = "ACTIVE"
        return True

    monkeypatch.setattr(setup_rtce, "_enable_rtce_topic", enable)
    ws.fake_phases["flight_impact"] = "ACTIVE"
    assert workshop_checks.ensure_lightning(ws, ["passenger_state", "flight_impact"], "") is True
    assert calls and calls[0][:5] == ["confluent", "rtce", "rtce-topic", "delete", "passenger_state"]


def test_a_table_that_never_leaves_failed_is_reported(tmp_path, bot, monkeypatch, capsys):
    ws = FakeWorkshop(tmp_path, phases={"flight_status": "PROVISIONING"}, topics=["flight_status"])
    seen = iter(["PROVISIONING", "FAILED"])
    ws.phases = lambda: {"flight_status": next(seen)}
    assert workshop_checks.ensure_lightning(ws, ["flight_status"], "") is False
    assert "failed to start" in capsys.readouterr().out


def test_missing_flink_tables_are_not_created_for_an_attendee(tmp_path, capsys):
    ws = FakeWorkshop(tmp_path, topics=["flight_status"])
    assert workshop_checks.ensure_flink_tables(ws, LAB2, ws.kafka_topics(), workshop_checks.LAB2_STEP) is False
    out = capsys.readouterr().out
    assert "`passenger_state`, `flight_impact` aren't in your cluster yet." in out
    assert "Your next step is Lab 2, steps 3 and 4" in out
    assert ws.sql_run == []


def test_a_bot_gets_missing_flink_tables_created_in_guide_order(tmp_path, bot):
    ws = FakeWorkshop(tmp_path, topics=["flight_status"])
    assert workshop_checks.ensure_flink_tables(ws, LAB2, ws.kafka_topics(), workshop_checks.LAB2_STEP) is True
    assert ws.sql_run == [filename for _, filename, _ in LAB2]


def test_flink_tables_that_exist_are_left_alone(tmp_path):
    ws = FakeWorkshop(tmp_path, topics=["passenger_state", "flight_impact"])
    assert workshop_checks.ensure_flink_tables(ws, LAB2, ws.kafka_topics(), workshop_checks.LAB2_STEP) is True
    assert ws.sql_run == []


def test_every_guide_sql_file_exists():
    for _, filename, _ in [*LAB2, *workshop_checks.LAB4_STAGING, workshop_checks.LAB4_TOOL, workshop_checks.LAB4_AGENT]:
        assert (workshop_checks.SQL_DIR / filename).exists(), filename
    assert (workshop_checks.SQL_DIR / workshop_checks.LAB4_RUN[1]).exists()


def test_status_lists_steps_in_guide_order():
    where = [step.where for step in workshop_checks.steps()]
    assert where == [
        "Deploy, step 4",
        "Lab 2, steps 3 and 4",
        "Lab 3, steps 1 and 2",
        "Lab 4, step 1",
        "Lab 4, steps 2 and 3",
        "Lab 4, steps 4 and 5",
        "Lab 4, step 7",
    ]


def _status_with_missing_lab_4(tmp_path, monkeypatch):
    """A workshop where labs 2 and 3 are done and nothing from lab 4 is."""
    ws = FakeWorkshop(tmp_path, phases=_active(LIGHTNING_TOPICS), topics=["passenger_state", "flight_impact"])
    monkeypatch.setattr(workshop_checks, "require_deployment", lambda command: ws)
    monkeypatch.setattr(setup_rtce, "agent_connection_exists", lambda infra: False)
    ws.show = lambda what: set()
    ws.agent_insert_running = lambda: False
    monkeypatch.setattr(workshop_checks, "_datagen_running", lambda ws: True)
    return ws


def test_status_reports_and_points_to_the_next_step_without_running_anything(tmp_path, monkeypatch, capsys):
    ws = _status_with_missing_lab_4(tmp_path, monkeypatch)
    monkeypatch.setattr(workshop_checks, "ensure_lightning", lambda *args, **kwargs: pytest.fail("ran a step"))
    with pytest.raises(SystemExit) as exit_info:
        workshop_checks.main([])
    assert exit_info.value.code == 1
    out = capsys.readouterr().out
    assert "✓ Lab 2, steps 3 and 4" in out
    assert "✓ Lab 3, steps 1 and 2" in out
    assert "✗ Lab 4, step 1" in out
    assert "✗ Lab 4, step 7" in out
    assert "Your next step is Lab 4, step 1" in out
    assert out.count("Your next step") == 1  # the first missing step only, not a to-do list to rush through
    assert "for you now" not in out
    assert ws.sql_run == []  # nothing was run


def test_status_never_catches_up_unless_a_bot_asks(tmp_path, monkeypatch):
    ws = _status_with_missing_lab_4(tmp_path, monkeypatch)
    monkeypatch.setenv("WORKSHOP_ASSUME_YES", "1")  # even a stray yes must not run the labs
    with pytest.raises(SystemExit):
        workshop_checks.main([])
    assert ws.sql_run == []


def test_status_catches_up_on_the_missing_steps_in_order(tmp_path, monkeypatch):
    ws = FakeWorkshop(tmp_path, phases=_active(LIGHTNING_TOPICS), topics=["passenger_state", "flight_impact"])
    monkeypatch.setattr(workshop_checks, "require_deployment", lambda command: ws)
    monkeypatch.setattr(setup_rtce, "agent_connection_exists", lambda infra: True)
    ws.show = lambda what: set()
    ws.agent_insert_running = lambda: False
    monkeypatch.setattr(workshop_checks, "_datagen_running", lambda ws: True)
    with pytest.raises(SystemExit) as exit_info:
        workshop_checks.main(["--testing"])
    assert exit_info.value.code == 0
    assert ws.sql_run == [
        "26-staging-passenger-state-changes.sql",
        "27-staging-impacted-passengers.sql",
        "29-tool-hotel-inventory-live-context.sql",
        "30-agent-passenger-recovery.sql",
        "31-insert-passenger-recommendations.sql",
    ]


def test_the_catch_up_flag_is_hidden_and_the_old_yes_flag_is_gone(capsys):
    with pytest.raises(SystemExit):
        workshop_checks.main(["--help"])
    help_text = capsys.readouterr().out
    assert "--testing" not in help_text and "--yes" not in help_text
    with pytest.raises(SystemExit) as exit_info:
        workshop_checks._parse_args(["--yes"])
    assert exit_info.value.code == 2


GUIDE_ROOT = Path(workshop_checks.__file__).resolve().parent.parent


def test_every_step_resolves_to_its_numbered_step_in_the_guide():
    lines = (GUIDE_ROOT / workshop_checks.WORKSHOP_GUIDE).read_text(encoding="utf-8").splitlines()
    for step in workshop_checks.steps():
        found = workshop_checks.guide_location(step, GUIDE_ROOT)
        assert found, f"{step.where} isn't where the guide says"
        section, line = found
        assert lines[line - 1].startswith(f"{step.guide[1]}. "), step.where
        assert section.startswith(step.guide[0].removeprefix("## ")), step.where


def test_the_next_step_names_its_line_in_the_guide_and_how_to_open_it(monkeypatch, capsys):
    monkeypatch.setattr(workshop_checks.sys, "platform", "darwin")
    step = next(s for s in workshop_checks.steps() if s.where == "Lab 3, steps 1 and 2")
    workshop_checks.point_to_step(step, GUIDE_ROOT)
    out = capsys.readouterr().out
    line = workshop_checks.guide_location(step, GUIDE_ROOT)[1]
    assert "Lab 3" in out and f"line {line} of {workshop_checks.WORKSHOP_GUIDE}" in out
    assert f"open {workshop_checks.WORKSHOP_GUIDE}" in out
    assert f"less +{line} {workshop_checks.WORKSHOP_GUIDE}" in out
    assert "uv run workshop-status" in out  # how to check again


def test_a_step_missing_from_the_guide_still_points_at_the_guide(tmp_path, capsys):
    step = workshop_checks.steps()[1]
    workshop_checks.point_to_step(step, tmp_path)  # no guide file here
    out = capsys.readouterr().out
    assert "Your next step is Lab 2" in out and workshop_checks.WORKSHOP_GUIDE in out


@pytest.mark.parametrize(("platform", "opener"), [("darwin", "open"), ("linux", "xdg-open")])
def test_the_guide_opens_with_the_computers_own_opener(monkeypatch, platform, opener):
    monkeypatch.setattr(workshop_checks.sys, "platform", platform)
    commands = [command for _, command in workshop_checks._open_guide_commands(12)]
    assert commands[0] == f"{opener} {workshop_checks.WORKSHOP_GUIDE}"
    assert commands[1].startswith("less +12 ")


def test_windows_opens_the_guide_with_start_and_has_no_less(monkeypatch):
    monkeypatch.setattr(workshop_checks.sys, "platform", "win32")
    commands = [command for _, command in workshop_checks._open_guide_commands(12)]
    # Plain `start <file>` opens it from PowerShell and Command Prompt alike; `start "" <file>` fails in PowerShell.
    assert commands == ["start docs\\flight-disruptions\\workshop.md"]


def test_cli_delete_command_targets_the_cluster(tmp_path, monkeypatch):
    ws = FakeWorkshop(tmp_path)
    calls = []
    monkeypatch.setattr(subprocess, "run", lambda argv, **kwargs: calls.append(argv))
    workshop_checks._disable_failed(ws, ["flight_impact"])
    assert calls == [
        [
            *("confluent", "rtce", "rtce-topic", "delete", "flight_impact", "--force"),
            *("--cluster", "lkc-1", "--environment", "env-1"),
        ]
    ]


def test_the_agents_insert_is_recognised_under_its_comment_header():
    sql = (workshop_checks.SQL_DIR / workshop_checks.LAB4_RUN[1]).read_text(encoding="utf-8")
    assert sql.lstrip().startswith("--")
    assert workshop_checks.is_agent_insert(sql)
    assert not workshop_checks.is_agent_insert("INSERT INTO hotel_inventory SELECT 1")


def test_the_app_stops_with_a_pointer_when_lab_3_was_skipped(tmp_path, monkeypatch, capsys):
    from scripts import airport_app

    ws = FakeWorkshop(tmp_path, topics=LIGHTNING_TOPICS)  # no Lightning Tables are on
    monkeypatch.setattr(workshop_checks, "require_deployment", lambda command: ws)
    monkeypatch.setattr(setup_rtce, "_enable_rtce_topic", lambda *args: pytest.fail("enabled for the attendee"))
    monkeypatch.setattr(airport_app, "ensure_port_free", lambda *args: pytest.fail("started the app"))
    with pytest.raises(SystemExit) as exit_info:
        airport_app.main([])
    assert exit_info.value.code == 1
    out = capsys.readouterr().out
    assert "Your next step is Lab 3, steps 1 and 2" in out and "for you now" not in out


def test_the_hosted_app_starts_without_local_terraform_state(monkeypatch):
    import uvicorn

    from scripts import airport_app

    monkeypatch.setenv("STREAMHOUSE_ENVIRONMENT_ID", "env-hosted")
    monkeypatch.setattr(workshop_checks, "require_deployment", lambda *args: pytest.fail("read local deployment"))
    monkeypatch.setattr(airport_app, "load_settings_secret", lambda: None)
    monkeypatch.setattr(airport_app, "ensure_port_free", lambda *args: None)
    starts = []
    monkeypatch.setattr(uvicorn, "run", lambda app, **kwargs: starts.append(app))
    airport_app.main([])
    assert starts == [airport_app.app]


def _run_setup_rtce(tmp_path, monkeypatch, ws, check_result=(7, "")):
    """Run `uv run setup-rtce --client none` against a fake cluster with no Lightning Tables on."""
    enabled = []
    monkeypatch.setattr(setup_rtce, "_find_credentials_file", lambda: tmp_path / "credentials.env")
    monkeypatch.setattr(setup_rtce, "_load_env_file", lambda path: {})
    monkeypatch.setattr(workshop_checks, "require_cluster_or_own", lambda command, creds_file: None)
    monkeypatch.setattr(setup_rtce, "ensure_global_key", lambda *args: "key")
    monkeypatch.setattr(setup_rtce, "_get_credentials", lambda *args: ("key", "secret"))
    monkeypatch.setattr(setup_rtce, "_get_infra", lambda creds_file: ws.infra)
    monkeypatch.setattr(setup_rtce, "_validate_rtce_support", lambda infra: None)
    monkeypatch.setattr(workshop_checks, "find_deployment", lambda: ws)
    monkeypatch.setattr(setup_rtce, "_list_rtce_topics", lambda *args: [])
    monkeypatch.setattr(setup_rtce, "_discover_demo_topics", lambda creds_file: LIGHTNING_TOPICS)
    monkeypatch.setattr(setup_rtce, "_save_to_env_file", lambda *args: None)
    if check_result is not None:
        monkeypatch.setattr(setup_rtce, "_test_mcp_connection", lambda url, token: check_result)

    def enable(key, secret, infra, topic, desc):
        enabled.append(topic)
        ws.fake_phases.setdefault(topic, "ACTIVE")
        return True

    monkeypatch.setattr(setup_rtce, "_enable_rtce_topic", enable)
    setup_rtce.main(["--client", "none"])
    return enabled


def test_setup_rtce_checks_rtce_only_once_the_tables_it_turned_on_are_active(tmp_path, monkeypatch):
    """RTCE answers FE_CUSTOMER_NOT_FOUND until a new cluster's first Lightning Table is up."""
    ws = FakeWorkshop(tmp_path, phases=dict.fromkeys(LIGHTNING_TOPICS, "PROVISIONING"), topics=LIGHTNING_TOPICS)
    ws.creds = {"TF_VAR_workshop": ""}
    loading = dict(ws.fake_phases)
    reads = []
    ws.phases = lambda: reads.append(1) or (loading if len(reads) == 1 else _active(LIGHTNING_TOPICS))
    monkeypatch.setattr(workshop_checks.time, "sleep", lambda seconds: None)

    def check(url, token):
        assert len(reads) >= 2, "checked RTCE before the Lightning Tables were Active"
        return 7, ""

    monkeypatch.setattr(setup_rtce, "_test_mcp_connection", check)
    _run_setup_rtce(tmp_path, monkeypatch, ws, check_result=None)


def test_setup_rtce_still_turns_the_lightning_tables_on_itself(tmp_path, monkeypatch):
    """Master's behavior, which `deploy --with-labs` relies on before any Lightning Table exists."""
    ws = FakeWorkshop(tmp_path, topics=LIGHTNING_TOPICS)
    ws.creds = {"TF_VAR_workshop": ""}  # the full demo, not the workshop
    assert _run_setup_rtce(tmp_path, monkeypatch, ws) == LIGHTNING_TOPICS


@pytest.mark.parametrize("event_stream", [False, True])
@pytest.mark.parametrize("tool_succeeds", [False, True])
def test_mcp_connection_checks_list_topics_after_initialization(monkeypatch, event_stream, tool_succeeds):
    calls = []
    payload = (
        {"status": "success", "rows": {"topics": [{"name": "flight_status"}]}}
        if tool_succeeds
        else {"status": "error", "error": {"code": "FE_CUSTOMER_NOT_FOUND"}}
    )

    def open_request(request, timeout):
        message = json.loads(request.data)
        calls.append(message["method"])
        if message["method"] == "initialize":
            response = BytesIO(b'{"id":1,"result":{"protocolVersion":"2024-11-05"}}')
            response.headers = {"Mcp-Session-Id": "test-session", "Content-Type": "application/json"}
            return response
        assert request.get_header("Mcp-session-id") == "test-session"
        assert request.get_header("Mcp-protocol-version") == "2024-11-05"
        if message["method"] == "notifications/initialized":
            assert "id" not in message
            response = BytesIO()
            response.headers = {}
            return response
        assert message["params"] == {"name": "listTopics", "arguments": {}}
        result = {"isError": not tool_succeeds, "content": [{"type": "text", "text": json.dumps(payload)}]}
        body = json.dumps({"id": 2, "result": result}).encode()
        response = BytesIO(b"event: message\ndata: " + body + b"\n\n" if event_stream else body)
        response.headers = {"Content-Type": "text/event-stream" if event_stream else "application/json"}
        return response

    monkeypatch.setattr(setup_rtce.urllib.request, "urlopen", open_request)

    expected = (1, "") if tool_succeeds else (None, "FE_CUSTOMER_NOT_FOUND")
    assert setup_rtce._test_mcp_connection("https://example.test/mcp", "token") == expected
    assert calls == ["initialize", "notifications/initialized", "tools/call"]


def test_setup_rtce_names_target_when_list_topics_fails(tmp_path, monkeypatch, capsys):
    ws = FakeWorkshop(tmp_path, topics=LIGHTNING_TOPICS)
    _run_setup_rtce(tmp_path, monkeypatch, ws, check_result=(None, "FE_CUSTOMER_NOT_FOUND"))
    output = capsys.readouterr().out
    assert "check failed for env-1 / lkc-1 (FE_CUSTOMER_NOT_FOUND)" in output
    assert "returned" not in output


def test_setup_rtce_skips_the_tables_lab_2_has_not_built_and_keeps_going(tmp_path, monkeypatch, capsys):
    """An F1 attendee with both labs deployed hasn't done the airline Lab 2, and must not be stopped by it."""
    sources = [t for t in LIGHTNING_TOPICS if t not in ("passenger_state", "flight_impact")]
    ws = FakeWorkshop(tmp_path, topics=sources)
    ws.creds = {"TF_VAR_workshop": "true"}
    enabled = _run_setup_rtce(tmp_path, monkeypatch, ws)
    assert "passenger_state" not in enabled and "flight_impact" not in enabled, "turned on before Flink built it"
    assert set(enabled) == set(sources)
    out = capsys.readouterr().out
    assert "Skipping `passenger_state`, `flight_impact`" in out
    assert ws.sql_run == []
