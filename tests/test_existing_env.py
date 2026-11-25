"""Deploying into an attendee's existing environment: core skips what an EnvironmentAdmin can't make."""

import json
import subprocess
from pathlib import Path

import pytest
from dotenv import dotenv_values

from scripts import existing_env, setup_rtce

CORE = Path(__file__).parent.parent / "terraform" / "core"


def _tf(name):
    return (CORE / name).read_text(encoding="utf-8")


COUNTED = [
    "confluent_environment.staging",
    "confluent_service_account.app-manager",
    "confluent_role_binding.app-manager-kafka-cluster-admin",
    "confluent_service_account.rtce-reader",
    "confluent_role_binding.rtce-reader-developer-read",
    "confluent_role_binding.rtce-reader-schema-registry-read",
    "confluent_kafka_acl.app-manager-read-on-topic",
    "confluent_kafka_acl.app-manager-describe-on-cluster",
    "confluent_kafka_acl.app-manager-write-on-topic",
    "confluent_kafka_acl.app-manager-create-topic",
    "confluent_kafka_acl.app-manager-read-on-group",
]


def test_every_resource_the_switch_skips_has_a_moved_block():
    main = _tf("main.tf")
    for address in COUNTED:
        kind, name = address.split(".")
        block = main.split(f'resource "{kind}" "{name}" {{', 1)[1].split("\n}", 1)[0]
        assert "count = local.existing ? 0 : 1" in block, address
        assert f"from = {address}\n" in main and f"to   = {address}[0]" in main, address


def test_core_makes_the_rtce_global_key():
    main = _tf("main.tf")
    block = main.split('resource "confluent_api_key" "rtce-global" {', 1)[1].split("\n}", 1)[0]
    assert 'id          = "global"' in block and 'kind        = "Global"' in block
    assert "rtce_global_api_key" in _tf("outputs.tf") and "rtce_global_api_secret" in _tf("outputs.tf")


def test_no_resource_reads_the_created_environment_directly():
    main = _tf("main.tf")
    # Only the env_id / env_display_name locals and the counted role binding may touch the created environment.
    assert "confluent_environment.staging.id" not in main
    assert "confluent_environment.staging.resource_name" not in main
    assert "confluent_environment.staging.display_name" not in main + _tf("outputs.tf")


def test_every_root_needs_the_provider_that_makes_global_keys():
    root = CORE.parent
    roots = ("core/versions.tf", "airline-demo/providers.tf", "agentic-pit-decisions/providers.tf")
    for f in (root / name for name in roots):
        assert 'version = "~> 2.73"' in f.read_text(encoding="utf-8"), f


def test_global_key_comes_from_core_and_replaces_a_stale_one(monkeypatch, tmp_path):
    creds_file = tmp_path / "credentials.env"
    creds_file.write_text("CONFLUENT_RTCE_API_KEY=OLD\nCONFLUENT_RTCE_API_SECRET=OLDS\n", encoding="utf-8")
    monkeypatch.setattr(
        setup_rtce,
        "_read_core_tf_outputs",
        lambda _: {"rtce_global_api_key": "NEW", "rtce_global_api_secret": "NEWS"},
    )
    monkeypatch.setattr(setup_rtce.subprocess, "run", lambda *a, **k: pytest.fail("no CLI call"))
    creds = {"CONFLUENT_RTCE_API_KEY": "OLD", "CONFLUENT_RTCE_API_SECRET": "OLDS"}
    assert setup_rtce.ensure_global_key(creds_file, creds) == "NEW"
    assert creds["CONFLUENT_RTCE_API_SECRET"] == "NEWS"
    assert dotenv_values(creds_file)["CONFLUENT_RTCE_API_KEY"] == "NEW"


def test_without_a_deployment_the_stored_key_is_kept(monkeypatch, tmp_path):
    monkeypatch.setattr(setup_rtce, "_read_core_tf_outputs", lambda _: {})
    creds = {"CONFLUENT_RTCE_API_KEY": "MINE"}
    assert setup_rtce.ensure_global_key(tmp_path / "credentials.env", creds) == "MINE"


def test_a_pasted_id_is_trimmed_checked_and_saved(monkeypatch, tmp_path, capsys):
    answers = iter(["lkc-123", '  "env-abc12" '])
    monkeypatch.setattr("builtins.input", lambda _: next(answers))
    creds_file, creds = tmp_path / "credentials.env", {}
    chosen = existing_env.choose(None, creds_file, creds, interactive=True, full=False)
    assert chosen == "env-abc12"
    assert creds[existing_env.ENV_KEY] == "env-abc12"
    assert dotenv_values(creds_file)[existing_env.ENV_KEY] == "env-abc12"
    assert "env-" in capsys.readouterr().out


def test_enter_keeps_the_new_environment_flow(monkeypatch, tmp_path):
    monkeypatch.setattr("builtins.input", lambda _: "")
    assert existing_env.choose(None, tmp_path / "c.env", {}, interactive=True, full=False) == ""


def test_full_demos_never_ask_and_refuse_an_existing_environment(monkeypatch, tmp_path):
    monkeypatch.setattr("builtins.input", lambda _: pytest.fail("no prompt for a full demo"))
    assert existing_env.choose(None, tmp_path / "c.env", {}, interactive=True, full=True) == ""
    with pytest.raises(SystemExit, match="stop after 4 hours"):
        existing_env.choose(None, tmp_path / "c.env", {existing_env.ENV_KEY: "env-abc12"}, interactive=False, full=True)


def test_a_bad_flag_exits(tmp_path):
    with pytest.raises(SystemExit, match="env-"):
        existing_env.choose("lkc-123", tmp_path / "c.env", {}, interactive=False, full=False)


def test_a_saved_id_is_used_without_a_write_or_prompt(monkeypatch, tmp_path):
    monkeypatch.setattr("builtins.input", lambda _: pytest.fail("no prompt"))
    creds_file = tmp_path / "c.env"
    creds = {existing_env.ENV_KEY: "env-abc12"}
    assert existing_env.choose(None, creds_file, creds, interactive=True, full=False) == "env-abc12"
    assert existing_env.choose("env-abc12", creds_file, creds, interactive=True, full=False) == "env-abc12"
    assert not creds_file.exists()


def test_a_flag_that_differs_replaces_the_saved_id(tmp_path):
    creds_file = tmp_path / "c.env"
    creds = {existing_env.ENV_KEY: "env-abc12"}
    assert existing_env.choose("env-zzz9", creds_file, creds, interactive=False, full=False) == "env-zzz9"
    assert creds[existing_env.ENV_KEY] == "env-zzz9"
    assert dotenv_values(creds_file)[existing_env.ENV_KEY] == "env-zzz9"


@pytest.mark.parametrize("flights,workshop,expected", [(True, True, False), (True, False, True), (False, False, False)])
def test_only_workshop_deploys_count_as_not_full(flights, workshop, expected):
    result = existing_env.full_demo(with_labs=False, video_2=False, always_on=False, flights=flights, workshop=workshop)
    assert result is expected


@pytest.mark.parametrize(
    "env_id,outputs",
    [
        ("env-abc12", {"confluent_environment_id": "env-new"}),
        ("", {"existing_environment": True, "confluent_environment_id": "env-abc12"}),
        ("env-other", {"existing_environment": True, "confluent_environment_id": "env-abc12"}),
    ],
)
def test_switching_flows_needs_a_destroy_first(env_id, outputs):
    with pytest.raises(SystemExit, match="uv run destroy"):
        existing_env.check_flow_switch(env_id, outputs)


def test_same_flow_or_nothing_deployed_passes():
    existing_env.check_flow_switch("", {})
    existing_env.check_flow_switch("env-abc12", {"existing_environment": True, "confluent_environment_id": "env-abc12"})
    existing_env.check_flow_switch("", {"confluent_environment_id": "env-new"})


def test_an_invalid_saved_id_exits_naming_the_key(tmp_path):
    with pytest.raises(SystemExit, match=existing_env.ENV_KEY):
        existing_env.choose(None, tmp_path / "c.env", {existing_env.ENV_KEY: "lkc-123"}, interactive=False, full=False)


def test_ctrl_d_at_the_prompt_means_a_new_environment(monkeypatch, tmp_path):
    def eof(_):
        raise EOFError

    monkeypatch.setattr("builtins.input", eof)
    assert existing_env.choose(None, tmp_path / "c.env", {}, interactive=True, full=False) == ""


def test_a_saved_id_refusal_says_where_it_came_from(tmp_path):
    with pytest.raises(SystemExit, match="delete that line"):
        existing_env.choose(None, tmp_path / "c.env", {existing_env.ENV_KEY: "env-abc12"}, interactive=False, full=True)


FIXTURES = Path(__file__).parent / "fixtures" / "existing_env"
_DEFAULTS = {
    "env": "environment_describe.json",
    "sr": "schema_registry_describe.json",
    "user": "role_bindings_current_user.json",
    "keys": "global_keys_current_user.json",
}
_COMMANDS = {
    "env": ("environment", "describe"),
    "sr": ("schema-registry", "cluster", "describe"),
    "user": ("iam", "rbac", "role-binding", "list"),
    "keys": ("api-key", "list"),
}


@pytest.fixture
def fake_cli(monkeypatch):
    """Patch the CLI. Each source is a fixture filename, None (fails), or a list/dict returned as JSON."""

    def install(env_package=None, notice="", **sources):
        chosen = {**_DEFAULTS, **sources}
        if env_package:
            env = json.loads((FIXTURES / _DEFAULTS["env"]).read_text(encoding="utf-8"))
            env["stream_governance_package"] = env_package
            chosen["env"] = env
        calls = []

        def run(cmd, **kwargs):
            assert kwargs.get("encoding") == "utf-8" and kwargs.get("errors") == "replace", cmd
            args = tuple(cmd[1:])
            calls.append(args)
            for name, words in _COMMANDS.items():
                if args[: len(words)] == words:
                    source = chosen[name]
                    break
            else:
                pytest.fail(f"unexpected CLI call {args}")
            if source is None:
                return subprocess.CompletedProcess(cmd, 1, "", "boom")
            if isinstance(source, str):
                source = json.loads((FIXTURES / source).read_text(encoding="utf-8"))
            return subprocess.CompletedProcess(cmd, 0, notice + json.dumps(source), "")

        monkeypatch.setattr(existing_env.subprocess, "run", run)
        return calls

    return install


def test_preflight_returns_the_user_id_when_all_checks_pass(fake_cli):
    fake_cli()
    assert existing_env.preflight("env-abc123", {}) == "u-abc123"


@pytest.mark.parametrize(
    "broken,message",
    [("env", "Can't find env-abc123"), ("sr", "Stream Governance"), ("user", "confluent login")],
)
def test_each_failed_check_stops_with_its_message(fake_cli, broken, message):
    fake_cli(**{broken: None})
    with pytest.raises(SystemExit, match=message):
        existing_env.preflight("env-abc123", {})


def test_an_empty_schema_registry_id_counts_as_not_enabled(fake_cli):
    fake_cli(sr={"cluster": ""})
    with pytest.raises(SystemExit, match="Stream Governance"):
        existing_env.preflight("env-abc123", {})


def test_essentials_is_refused(fake_cli):
    fake_cli(env_package="ESSENTIALS")
    with pytest.raises(SystemExit, match="ADVANCED"):
        existing_env.preflight("env-abc123", {})


def test_two_foreign_global_keys_stop_but_our_own_key_does_not(fake_cli):
    fake_cli(keys=[{"key": "A"}, {"key": "B"}])
    with pytest.raises(SystemExit, match=r"A.*B"):
        existing_env.preflight("env-abc123", {})
    assert existing_env.preflight("env-abc123", {"rtce_global_api_key": "B"}) == "u-abc123"


@pytest.mark.parametrize("keys", [None, {"not": "a list"}])
def test_an_unreadable_key_list_does_not_block(fake_cli, keys):
    fake_cli(keys=keys)
    assert existing_env.preflight("env-abc123", {}) == "u-abc123"


def test_the_user_lookup_passes_the_environment(fake_cli):
    calls = fake_cli()
    assert existing_env.current_user_id("env-abc123") == "u-abc123"
    (call,) = [c for c in calls if c[:4] == ("iam", "rbac", "role-binding", "list")]
    assert "--current-user" in call and call[call.index("--environment") + 1] == "env-abc123"


def test_no_user_id_is_an_empty_string(fake_cli):
    fake_cli(user=[])
    assert existing_env.current_user_id("env-abc123") == ""


def test_a_notice_before_the_json_is_skipped(fake_cli):
    fake_cli(notice="A new version of confluent is available: 1.2.3\n")
    assert existing_env.preflight("env-abc123", {}) == "u-abc123"
