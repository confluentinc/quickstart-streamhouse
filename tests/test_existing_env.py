"""Deploying into an attendee's existing environment: core skips what an EnvironmentAdmin can't make."""

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest
from dotenv import dotenv_values

from scripts import deploy, destroy, existing_env, setup_rtce

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
_DEFAULTS = {"user": "role_bindings_current_user.json"}
_COMMANDS = {"user": ("iam", "rbac", "role-binding", "list")}


@pytest.fixture
def fake_cli(monkeypatch):
    """Patch the CLI. Each source is a fixture filename, None (fails), or a list/dict returned as JSON."""

    def install(notice="", **sources):
        chosen = {**_DEFAULTS, **sources}
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


def test_preflight_returns_the_user_id(fake_cli):
    calls = fake_cli()
    assert existing_env.preflight("env-abc123") == "u-abc123"
    assert len(calls) == 1  # one CLI call: the user lookup is the only check


@pytest.mark.parametrize("user", [None, []])
def test_preflight_stops_when_the_login_cant_see_the_environment(fake_cli, user):
    fake_cli(user=user)
    with pytest.raises(SystemExit, match=r"Can.t find env-abc123.*confluent login"):
        existing_env.preflight("env-abc123")


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
    assert existing_env.preflight("env-abc123") == "u-abc123"


def _clear_env(monkeypatch, *names):
    for name in names:  # setenv first, so monkeypatch restores the variable after the test
        monkeypatch.setenv(name, "x")
        monkeypatch.delenv(name)


def test_deploy_passes_the_user_to_terraform_and_refuses_bots_running_the_full_demo(monkeypatch, tmp_path):
    monkeypatch.setattr(deploy.setup_rtce, "_read_core_tf_outputs", lambda _: {})
    monkeypatch.setattr(deploy.existing_env, "preflight", lambda env_id: "u-abc123")
    _clear_env(monkeypatch, "TF_VAR_user_id", existing_env.ENV_KEY)
    args = deploy._parse_args(["--environment", "env-abc12", "--workshop"])
    creds = {}
    assert (
        deploy._existing_environment(args, tmp_path / "c.env", creds, ["flight-disruptions"], True, interactive=False)
        == "env-abc12"
    )
    assert os.environ["TF_VAR_user_id"] == "u-abc123"
    assert os.environ[existing_env.ENV_KEY] == "env-abc12"
    with pytest.raises(SystemExit, match="4 hours"):
        deploy._existing_environment(
            deploy._parse_args(["--testing"]),
            tmp_path / "c.env",
            creds,
            ["flight-disruptions"],
            False,
            interactive=False,
        )


def test_deploy_without_an_environment_still_checks_for_a_flow_switch(monkeypatch, tmp_path):
    outputs = {"existing_environment": True, "confluent_environment_id": "env-abc12"}
    monkeypatch.setattr(deploy.setup_rtce, "_read_core_tf_outputs", lambda _: outputs)
    monkeypatch.setattr(deploy.existing_env, "preflight", lambda *a: pytest.fail("preflight ran"))
    with pytest.raises(SystemExit, match="uv run destroy"):
        deploy._existing_environment(
            deploy._parse_args(["--testing"]), tmp_path / "c.env", {}, ["flight-disruptions"], False, interactive=False
        )


def test_parse_args_keeps_the_existing_checks():
    assert deploy._parse_args([]).environment is None
    with pytest.raises(SystemExit):
        deploy._parse_args(["--automated", "--testing"])
    with pytest.raises(SystemExit):
        deploy._parse_args(["--workshop", "--with-labs"])


def test_destroy_says_the_environment_is_kept(monkeypatch, tmp_path):
    assert destroy._kept_environment(tmp_path) == ""
    (tmp_path / "terraform" / "core").mkdir(parents=True)
    (tmp_path / "terraform" / "core" / "terraform.tfstate").write_text("{}", encoding="utf-8")
    monkeypatch.setattr(
        destroy,
        "run_terraform_output",
        lambda _: {"existing_environment": True, "confluent_environment_id": "env-abc12"},
    )
    assert destroy._kept_environment(tmp_path) == "env-abc12"
    monkeypatch.setattr(
        destroy,
        "run_terraform_output",
        lambda _: {"existing_environment": False, "confluent_environment_id": "env-new"},
    )
    assert destroy._kept_environment(tmp_path) == ""


def test_destroy_gives_core_the_user_so_its_variables_validate(monkeypatch):
    seen = []
    monkeypatch.setattr(destroy.existing_env, "current_user_id", lambda env_id: seen.append(env_id) or "u-abc123")
    _clear_env(monkeypatch, "TF_VAR_user_id")
    assert destroy._user_for_core({}) == ""
    assert "TF_VAR_user_id" not in os.environ
    assert destroy._user_for_core({"TF_VAR_existing_environment_id": "env-abc12"}) == "u-abc123"
    assert os.environ["TF_VAR_user_id"] == "u-abc123"
    assert seen == ["env-abc12"]


def test_destroy_stops_before_terraform_when_core_needs_a_user_it_cannot_find(monkeypatch, tmp_path, capsys):
    monkeypatch.setattr(destroy.existing_env, "current_user_id", lambda env_id: "")
    _clear_env(monkeypatch, "TF_VAR_user_id")
    creds = {"TF_VAR_existing_environment_id": "env-abc12"}
    destroy._ensure_user_for_core({}, tmp_path, ["core"])  # nothing saved: nothing needed
    destroy._ensure_user_for_core(creds, tmp_path, ["core"])  # core never deployed: nothing to stop
    (tmp_path / "terraform" / "core").mkdir(parents=True)
    (tmp_path / "terraform" / "core" / "terraform.tfstate").write_text("{}", encoding="utf-8")
    destroy._ensure_user_for_core(creds, tmp_path, ["airline-demo"])  # a --lab destroy warns and goes on
    assert "Couldn't find your Confluent user ID" in capsys.readouterr().out
    with pytest.raises(SystemExit, match=r"env-abc12.*confluent login.*EnvironmentAdmin"):
        destroy._ensure_user_for_core(creds, tmp_path, ["airline-demo", "core"])


def test_destroy_names_the_account_that_deployed_when_the_login_cant_see_it(monkeypatch, tmp_path):
    monkeypatch.setattr(destroy.existing_env, "current_user_id", lambda env_id: "")
    _clear_env(monkeypatch, "TF_VAR_user_id")
    (tmp_path / "terraform" / "core").mkdir(parents=True)
    (tmp_path / "terraform" / "core" / "terraform.tfstate").write_text("{}", encoding="utf-8")
    creds = {"TF_VAR_existing_environment_id": "env-abc12", "CONFLUENT_DEPLOYED_AS": "owner@example.com"}
    monkeypatch.setattr(destroy.existing_env, "signed_in_email", lambda: "someone-else@example.com")
    with pytest.raises(SystemExit, match=r"another Confluent account \(owner@example.com\).*confluent login"):
        destroy._ensure_user_for_core(creds, tmp_path, ["core"])
    monkeypatch.setattr(destroy.existing_env, "signed_in_email", lambda: "owner@example.com")  # same account
    with pytest.raises(SystemExit, match=r"Couldn't find your Confluent user ID"):
        destroy._ensure_user_for_core(creds, tmp_path, ["core"])


def test_destroy_names_no_account_when_the_signed_in_email_is_unknown(monkeypatch, tmp_path):
    monkeypatch.setattr(destroy.existing_env, "current_user_id", lambda env_id: "")
    monkeypatch.setattr(destroy.existing_env, "signed_in_email", lambda: "")
    _clear_env(monkeypatch, "TF_VAR_user_id")
    (tmp_path / "terraform" / "core").mkdir(parents=True)
    (tmp_path / "terraform" / "core" / "terraform.tfstate").write_text("{}", encoding="utf-8")
    creds = {"TF_VAR_existing_environment_id": "env-abc12", "CONFLUENT_DEPLOYED_AS": "owner@example.com"}
    with pytest.raises(SystemExit, match=r"Couldn't find your Confluent user ID"):
        destroy._ensure_user_for_core(creds, tmp_path, ["core"])


def test_a_full_destroy_forgets_the_existing_environment_for_people_but_not_bots():
    """People are asked again on the next deploy; a bot can't be asked, so it keeps its environment."""
    assert destroy.existing_env.ENV_KEY in destroy._settings_to_forget(testing=False)
    assert destroy.existing_env.ENV_KEY not in destroy._settings_to_forget(testing=True)


def test_deploy_saves_an_environment_only_after_its_checks_pass(monkeypatch, tmp_path):
    creds_file = tmp_path / "credentials.env"
    creds_file.write_text("", encoding="utf-8")
    monkeypatch.setattr(deploy.setup_rtce, "_read_core_tf_outputs", lambda _: {})
    monkeypatch.setattr(deploy.existing_env, "preflight", lambda env_id: sys.exit("Can't find env-typo1"))
    _clear_env(monkeypatch, "TF_VAR_user_id", existing_env.ENV_KEY)
    creds = {}
    args = deploy._parse_args(["--environment", "env-typo1", "--workshop"])
    with pytest.raises(SystemExit, match="Can't find"):
        deploy._existing_environment(args, creds_file, creds, ["flight-disruptions"], True, interactive=True)
    assert existing_env.ENV_KEY not in dotenv_values(creds_file)
    assert existing_env.ENV_KEY not in creds
    monkeypatch.setattr(deploy.existing_env, "preflight", lambda env_id: "u-abc123")
    deploy._existing_environment(args, creds_file, creds, ["flight-disruptions"], True, interactive=True)
    assert dotenv_values(creds_file)[existing_env.ENV_KEY] == creds[existing_env.ENV_KEY] == "env-typo1"


def test_redeploying_a_new_environment_checkout_asks_nothing(monkeypatch, tmp_path):
    outputs = {"existing_environment": False, "confluent_environment_id": "env-new01"}
    monkeypatch.setattr(deploy.setup_rtce, "_read_core_tf_outputs", lambda _: outputs)
    monkeypatch.setattr("builtins.input", lambda *a: pytest.fail("asked"))
    args = deploy._parse_args(["--workshop"])
    assert (
        deploy._existing_environment(args, tmp_path / "c.env", {}, ["flight-disruptions"], True, interactive=True) == ""
    )
    # A saved ID is still checked against the deployed flow.
    with pytest.raises(SystemExit, match="uv run destroy"):
        deploy._existing_environment(
            args,
            tmp_path / "c.env",
            {existing_env.ENV_KEY: "env-abc12"},
            ["flight-disruptions"],
            True,
            interactive=True,
        )


def test_destroy_names_the_other_organization_for_the_same_email(monkeypatch, tmp_path):
    monkeypatch.setattr(destroy.existing_env, "current_user_id", lambda env_id: "")
    monkeypatch.setattr(destroy.existing_env, "signed_in_email", lambda: "owner@example.com")
    monkeypatch.setattr(destroy.existing_env, "signed_in_org_id", lambda: "org-2")
    _clear_env(monkeypatch, "TF_VAR_user_id")
    (tmp_path / "terraform" / "core").mkdir(parents=True)
    (tmp_path / "terraform" / "core" / "terraform.tfstate").write_text("{}", encoding="utf-8")
    creds = {
        "TF_VAR_existing_environment_id": "env-abc12",
        "CONFLUENT_DEPLOYED_AS": "owner@example.com",
        "CONFLUENT_DEPLOYED_ORG": "org-1",
    }
    with pytest.raises(SystemExit, match=r"another Confluent organization.*confluent organization use org-1"):
        destroy._ensure_user_for_core(creds, tmp_path, ["core"])
