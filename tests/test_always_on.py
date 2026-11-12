"""Static checks on the always-on image and the terraform/always-on root."""

import re
from pathlib import Path

ROOT = Path(__file__).parent.parent
TF = ROOT / "terraform/always-on"


def test_image_never_contains_credentials():
    ignored = (ROOT / ".dockerignore").read_text().split()
    assert {"credentials.env", "tmp/", "terraform/", ".git/"} <= set(ignored)


def test_no_email_addresses_in_tracked_terraform():
    text = "".join(p.read_text() for p in TF.glob("*.tf"))
    assert "@confluent.io" not in text


def test_generator_runs_continuous_and_app_runs_on_8000():
    aws = (TF / "aws.tf").read_text()
    assert '"airport-datagen", "--continuous"' in aws and "8000" in aws


def test_length_limited_names_are_checked_against_their_aws_limits():
    text = "".join(p.read_text() for p in TF.glob("*.tf"))
    # App Runner auto-scaling configuration names are 4-32 characters, so "<name>-flight-board" is too long.
    scaling = re.search(r"auto_scaling_configuration_name\s*=\s*(.+)", text).group(1)
    assert "flight-board" not in scaling
    assert re.search(r"flight_board_scaling\s*=\s*\{[^\n]*limit\s*=\s*32", text)
    assert re.search(r"flight_board_service\s*=\s*\{[^\n]*limit\s*=\s*40", text)
    assert re.search(r"generator_execution_role\s*=\s*\{[^\n]*limit\s*=\s*64", text)
    assert "precondition" in text and "names_too_long" in text


# ---------------------------------------------------------------------------
# uv run deploy --always-on / uv run destroy
# ---------------------------------------------------------------------------

import json  # noqa: E402
import subprocess  # noqa: E402
import sys  # noqa: E402

import pytest  # noqa: E402

from scripts import airport_datagen, deploy, destroy  # noqa: E402

INFRA = {"org_id": "o-1", "env_id": "env-1", "cluster_id": "lkc-1", "region": "us-east-1"}


def _stub_finish(monkeypatch, tmp_path, calls):
    (tmp_path / "credentials.env").write_text(
        "CONFLUENT_RTCE_API_KEY=k\nCONFLUENT_RTCE_API_SECRET=s\nDEMO_USER_EMAILS=a+1@x.io, a+2@x.io\n"
    )
    outputs = {"image_uri": "repo:old", "app_url": "https://board", "app_password": "pw"}
    monkeypatch.setattr(deploy, "_wait_for_demo_schemas", lambda root: None)
    monkeypatch.setattr(airport_datagen, "run_continuous", lambda *a, **k: calls.append(("history", a, k)))
    monkeypatch.setattr(deploy.setup_rtce, "main", lambda argv: calls.append(("rtce", argv)))
    monkeypatch.setattr(deploy.setup_rtce, "core_infra", lambda _: INFRA)
    monkeypatch.setattr(deploy, "_start_agent", lambda root: calls.append("agent"))
    monkeypatch.setattr(deploy, "_compact_sources", lambda infra: calls.append("compact"))
    monkeypatch.setattr(deploy, "_push_image", lambda root, region: calls.append("image") or "repo:new")
    monkeypatch.setattr(deploy, "_always_on_outputs", lambda root: outputs)
    monkeypatch.setattr(deploy, "run_terraform", lambda path, **k: calls.append(("terraform", path.name, k)) or True)
    monkeypatch.setattr(deploy, "_redeploy_services", lambda outputs, region: calls.append("redeploy"))
    monkeypatch.setattr(deploy, "_grant_demo_users", lambda env, emails: calls.append(("users", env, emails)) or [])
    monkeypatch.setattr(deploy, "_write_card", lambda root, outputs, emails: calls.append("card"))
    for key in ("TF_VAR_image_uri", "TF_VAR_rtce_api_key", "TF_VAR_rtce_api_secret"):
        monkeypatch.setenv(key, "unset")  # so teardown removes what _finish_always_on sets
        monkeypatch.delenv(key)


def _names(calls):
    return [c if isinstance(c, str) else c[0] for c in calls]


def test_always_on_finish_order(monkeypatch, tmp_path):
    calls = []
    _stub_finish(monkeypatch, tmp_path, calls)
    deploy._finish_always_on(tmp_path)
    assert _names(calls) == ["history", "rtce", "agent", "compact", "image", "terraform", "users", "card"]
    history = calls[0]
    assert history[1] == (airport_datagen.SEED,) and history[2]["history"] is True
    assert history[2]["until"] == history[2]["now"]  # publishes and returns, never streams
    assert calls[1][1] == ["--client", "none"]
    assert calls[5] == ("terraform", "always-on", {})
    assert calls[6] == ("users", "env-1", ["a+1@x.io", "a+2@x.io"])
    assert deploy.os.environ["TF_VAR_image_uri"] == "repo:new"
    assert deploy.os.environ["TF_VAR_rtce_api_key"] == "k"


def test_always_on_redeploys_when_the_image_is_unchanged(monkeypatch, tmp_path):
    # A rotated secret doesn't restart App Runner or ECS on its own; a new image tag does.
    calls = []
    _stub_finish(monkeypatch, tmp_path, calls)
    monkeypatch.setattr(deploy, "_push_image", lambda root, region: "repo:old")
    deploy._finish_always_on(tmp_path)
    assert "redeploy" in calls


def test_always_on_never_starts_the_laptop_stream(monkeypatch, tmp_path):
    calls = []
    _stub_finish(monkeypatch, tmp_path, calls)
    monkeypatch.setattr(deploy, "_run_datagen", lambda *a, **k: pytest.fail("laptop stream started"))
    deploy._finish_always_on(tmp_path)


def _completed(cmd, stdout="", code=0):
    return subprocess.CompletedProcess(cmd, code, stdout=stdout, stderr="")


def test_grant_demo_users_binds_only_missing_roles(monkeypatch):
    runs = []

    def fake_run(cmd, **kwargs):
        runs.append(cmd)
        if cmd[:4] == ["confluent", "iam", "user", "list"]:
            return _completed(cmd, json.dumps([{"id": "u-1", "email": "A+1@x.io"}]))
        if cmd[:5] == ["confluent", "iam", "user", "invitation", "list"]:
            return _completed(
                cmd,
                json.dumps(
                    [
                        {
                            "id": "i-2",
                            "email": "a+2@x.io",
                            "name": "",
                            "status": "INVITE_STATUS_SENT",
                            "user_id": "u-2",
                        },
                        {
                            "id": "i-1",
                            "email": "a+1@x.io",
                            "name": "",
                            "status": "INVITE_STATUS_ACCEPTED",
                            "user_id": "",
                        },
                        {
                            "id": "i-9",
                            "email": "a+9@x.io",
                            "name": "",
                            "status": "INVITE_STATUS_EXPIRED",
                            "user_id": "u-9",
                        },
                    ]
                ),
            )
        if cmd[:5] == ["confluent", "iam", "rbac", "role-binding", "list"]:
            principal = cmd[cmd.index("--principal") + 1]
            rows = [{"principal": "User:u-1", "role": "EnvironmentAdmin"}] if principal == "User:u-1" else []
            return _completed(cmd, json.dumps(rows))
        return _completed(cmd)

    monkeypatch.setattr(deploy.subprocess, "run", fake_run)
    missing = deploy._grant_demo_users("env-1", ["a+1@x.io", "a+2@x.io", "a+9@x.io"])
    assert missing == ["a+9@x.io"]
    creates = [cmd for cmd in runs if cmd[:5] == ["confluent", "iam", "rbac", "role-binding", "create"]]
    assert creates == [
        [
            "confluent",
            "iam",
            "rbac",
            "role-binding",
            "create",
            "--principal",
            "User:u-2",
            "--role",
            "EnvironmentAdmin",
            "--environment",
            "env-1",
        ]
    ]


def test_preflight_stops_without_aws_credentials(monkeypatch):
    monkeypatch.setattr(deploy.shutil, "which", lambda name: f"/usr/bin/{name}")
    with pytest.raises(SystemExit, match="AWS credentials"):
        deploy._preflight_always_on({"TF_VAR_aws_tableflow_access_key": ""})


def test_preflight_stops_without_docker(monkeypatch):
    monkeypatch.setattr(deploy.shutil, "which", lambda name: None if name == "docker" else f"/usr/bin/{name}")
    with pytest.raises(SystemExit, match="docker"):
        deploy._preflight_always_on({"TF_VAR_aws_tableflow_access_key": "AKIA"})


def _aws(monkeypatch, decisions=None, simulate_code=0, caller="arn:aws:iam::1:user/me"):
    runs = []

    def fake_run(cmd, **kwargs):
        runs.append(cmd)
        if cmd[:3] == ["aws", "sts", "get-caller-identity"]:
            return _completed(cmd, json.dumps({"Arn": caller}))
        if cmd[:3] == ["aws", "iam", "get-role"]:
            return _completed(cmd, "arn:aws:iam::1:role/aws-reserved/sso/Admin\n")
        if cmd[:3] == ["aws", "iam", "simulate-principal-policy"]:
            names = cmd[cmd.index("--action-names") + 1 : cmd.index("--output")]
            results = [{"EvalActionName": n, "EvalDecision": (decisions or {}).get(n, "allowed")} for n in names]
            return _completed(cmd, json.dumps({"EvaluationResults": results}), simulate_code)
        return _completed(cmd)

    monkeypatch.setattr(deploy.shutil, "which", lambda name: f"/usr/bin/{name}")
    monkeypatch.setattr(deploy.subprocess, "run", fake_run)
    return runs


def test_preflight_stops_when_iam_roles_are_denied(monkeypatch):
    _aws(monkeypatch, {"iam:CreateRole": "implicitDeny"})
    with pytest.raises(SystemExit, match="iam:CreateRole"):
        deploy._preflight_always_on({"TF_VAR_aws_tableflow_access_key": "AKIA..."})


def test_preflight_simulates_the_role_behind_an_assumed_role_session(monkeypatch):
    runs = _aws(monkeypatch, caller="arn:aws:sts::1:assumed-role/Admin/me@x.io")
    deploy._preflight_always_on({"TF_VAR_aws_tableflow_access_key": "ASIA"})
    simulate = next(cmd for cmd in runs if cmd[:3] == ["aws", "iam", "simulate-principal-policy"])
    assert simulate[simulate.index("--policy-source-arn") + 1] == "arn:aws:iam::1:role/aws-reserved/sso/Admin"


def test_preflight_continues_when_the_simulation_is_denied(monkeypatch, capsys):
    _aws(monkeypatch, simulate_code=254)
    deploy._preflight_always_on({"TF_VAR_aws_tableflow_access_key": "AKIA"})
    assert "couldn't check" in capsys.readouterr().out


def test_always_on_rejects_workshop(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["deploy", "--always-on", "--workshop"])
    with pytest.raises(SystemExit):
        deploy.main()


def test_reset_data_needs_always_on(monkeypatch, tmp_path):
    with pytest.raises(SystemExit, match="--always-on"):
        deploy._always_on_mode(argparse_namespace(reset_data=True), tmp_path / "credentials.env", {})


def argparse_namespace(**overrides):
    import argparse

    values = {"always_on": False, "reset_data": False, "workshop": False, "video_2": False, "lab": None}
    values["with_labs"] = False
    return argparse.Namespace(**{**values, **overrides})


def test_always_on_saves_its_mode_and_implies_the_full_flight_demo(tmp_path):
    creds_file = tmp_path / "credentials.env"
    creds_file.write_text("")
    args = argparse_namespace(always_on=True)
    creds = {}
    assert deploy._always_on_mode(args, creds_file, creds)
    assert args.with_labs and args.lab == "flight-disruptions"
    assert deploy.dotenv_values(str(creds_file)) == {}  # nothing saved until the deploy goes ahead
    deploy._save_always_on(creds_file, creds)
    saved = deploy.dotenv_values(str(creds_file))
    assert saved["TF_VAR_always_on"] == saved["TF_VAR_continuous"] == "true"
    assert saved["TF_VAR_deployment_name"] == "river-air-demo"
    # Saved mode: a later plain run is always-on too, and an existing name is kept.
    creds_file.write_text("TF_VAR_always_on=true\nTF_VAR_deployment_name=mine\n")
    args = argparse_namespace()
    creds = deploy.dotenv_values(str(creds_file))
    assert deploy._always_on_mode(args, creds_file, creds)
    deploy._save_always_on(creds_file, creds)
    assert deploy.dotenv_values(str(creds_file))["TF_VAR_deployment_name"] == "mine"
    assert not deploy._workshop_mode(False, args.with_labs, creds_file, {})


def _interactive(monkeypatch, tmp_path, text, ready):
    creds_file = tmp_path / "credentials.env"
    creds_file.write_text(text)
    monkeypatch.setattr(sys, "argv", ["deploy", "--always-on"])
    monkeypatch.setattr(deploy, "get_project_root", lambda: tmp_path)
    monkeypatch.setattr(
        deploy, "load_or_create_credentials_file", lambda root: (creds_file, deploy.dotenv_values(str(creds_file)))
    )
    monkeypatch.setattr(deploy, "ensure_confluent_login", lambda *a, **k: None)
    monkeypatch.setattr(deploy, "prompt_with_default", lambda text, default="", **k: default or "x")
    monkeypatch.setattr(deploy, "prompt_yes_no", lambda text, default=True: ready if "Ready" in text else False)
    monkeypatch.setattr(deploy, "_prompt_aws_credentials", lambda *a: None)
    monkeypatch.setattr(deploy, "_export_aws_credentials", lambda creds: None)
    monkeypatch.setattr(deploy, "_preflight_always_on", lambda creds: None)
    monkeypatch.setattr(deploy, "_deploy", lambda *a: pytest.fail("deployed"))
    return creds_file


LOGIN = "CONFLUENT_EMAIL=e\nCONFLUENT_PASSWORD=p\nDEMO_USER_EMAILS=a@x.io\n"


def test_always_on_on_a_workshop_checkout_saves_nothing(monkeypatch, tmp_path):
    creds_file = _interactive(monkeypatch, tmp_path, LOGIN + "TF_VAR_workshop=true\n", ready=True)
    with pytest.raises(SystemExit, match="workshop"):
        deploy.main()
    saved = deploy.dotenv_values(str(creds_file))
    assert not {"TF_VAR_always_on", "TF_VAR_continuous", "TF_VAR_deployment_name"} & set(saved)


def test_cancelling_always_on_at_the_confirmation_saves_nothing(monkeypatch, tmp_path):
    creds_file = _interactive(monkeypatch, tmp_path, LOGIN, ready=False)
    with pytest.raises(SystemExit) as stopped:
        deploy.main()
    assert stopped.value.code == 0
    saved = deploy.dotenv_values(str(creds_file))
    assert not {"TF_VAR_always_on", "TF_VAR_continuous", "TF_VAR_deployment_name"} & set(saved)


def test_saved_always_on_rejects_workshop(tmp_path):
    with pytest.raises(SystemExit):
        deploy._always_on_mode(argparse_namespace(workshop=True), tmp_path / "c.env", {"TF_VAR_always_on": "true"})


def test_destroy_takes_down_always_on_first(monkeypatch, tmp_path):
    (tmp_path / "credentials.env").write_text("TF_VAR_always_on=true\nTF_VAR_continuous=true\n")
    for env in ("always-on", "airline-demo", "core"):
        (tmp_path / "terraform" / env).mkdir(parents=True)
        (tmp_path / "terraform" / env / "terraform.tfstate").write_text("{}")
    monkeypatch.setattr(destroy, "get_project_root", lambda: tmp_path)
    monkeypatch.setattr(sys, "argv", ["destroy", "--testing"])
    monkeypatch.setattr(destroy, "_prepare_always_on_destroy", lambda creds: None)
    visited = []
    monkeypatch.setattr(destroy, "run_terraform_destroy", lambda path: visited.append(path.name) or True)
    destroy.main()
    assert visited == ["always-on", "airline-demo", "core"]
    saved = deploy.dotenv_values(str(tmp_path / "credentials.env"))
    assert "TF_VAR_always_on" not in saved and "TF_VAR_continuous" not in saved


def test_destroy_skips_always_on_without_its_state(tmp_path):
    assert "always-on" not in destroy._envs_to_destroy(None, tmp_path)
    (tmp_path / "terraform/always-on").mkdir(parents=True)
    (tmp_path / "terraform/always-on/terraform.tfstate").write_text("{}")
    assert destroy._envs_to_destroy(None, tmp_path)[0] == "always-on"


def _deployed_before(tmp_path):
    (tmp_path / "terraform" / "always-on").mkdir(parents=True)
    (tmp_path / "terraform" / "always-on" / "terraform.tfstate").write_text("{}")


def test_first_always_on_deploy_publishes_history(monkeypatch, tmp_path):
    calls = []
    _stub_finish(monkeypatch, tmp_path, calls)
    deploy._finish_always_on(tmp_path)
    assert calls[0][2]["history"] is True


def test_rerun_publishes_only_the_current_state(monkeypatch, tmp_path):
    # Rewriting D-30..D-2 would replace the agent's offers and the customers' answers with the generator's.
    calls = []
    _stub_finish(monkeypatch, tmp_path, calls)
    _deployed_before(tmp_path)
    deploy._finish_always_on(tmp_path)
    assert calls[0][0] == "history" and calls[0][2]["history"] is False


def test_rerun_with_reset_data_publishes_history_again(monkeypatch, tmp_path):
    calls = []
    _stub_finish(monkeypatch, tmp_path, calls)
    monkeypatch.setattr(deploy, "_reset_always_on_data", lambda root: calls.append("reset"))
    _deployed_before(tmp_path)
    deploy._finish_always_on(tmp_path, reset_data=True)
    assert calls[0] == "reset" and calls[1][2]["history"] is True


def _agent_fails(monkeypatch, tmp_path, env: str):
    (tmp_path / "credentials.env").write_text(env)
    monkeypatch.setattr(deploy.setup_rtce, "core_infra", lambda _: None)
    assert deploy._start_agent(tmp_path) is False


def test_failed_agent_on_always_on_says_to_rerun_deploy(monkeypatch, tmp_path, capsys):
    _agent_fails(monkeypatch, tmp_path, "TF_VAR_always_on=true\n")
    out = capsys.readouterr().out
    assert "uv run deploy --always-on" in out and "--offers generator" not in out


def test_failed_agent_on_the_scripted_demo_suggests_the_generator(monkeypatch, tmp_path, capsys):
    _agent_fails(monkeypatch, tmp_path, "")
    assert "uv run airport-datagen --offers generator" in capsys.readouterr().out


def test_card_is_readable_only_by_its_owner(tmp_path):
    path = deploy._write_card(tmp_path, {"app_url": "https://board", "app_password": "pw"}, ["a@x.io"])
    assert path.stat().st_mode & 0o777 == 0o600
