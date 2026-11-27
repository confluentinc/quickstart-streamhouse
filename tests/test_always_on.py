"""Static checks on the always-on image and the terraform/always-on root."""

import re
from pathlib import Path

ROOT = Path(__file__).parent.parent
TF = ROOT / "terraform/always-on"


def test_image_never_contains_credentials():
    ignored = (ROOT / ".dockerignore").read_text(encoding="utf-8").split()
    assert {"credentials.env", "tmp/", "terraform/", ".git/"} <= set(ignored)


def test_no_email_addresses_in_tracked_terraform():
    text = "".join(p.read_text(encoding="utf-8") for p in TF.glob("*.tf"))
    assert "@confluent.io" not in text


def _block(text: str, address: str) -> str:
    """The body of one `resource "type" "name"` block, up to the next top-level resource."""
    kind, name = address.split(".")
    start = text.index(f'resource "{kind}" "{name}"')
    end = text.find("\nresource ", start + 1)
    return text[start : end if end != -1 else len(text)]


def test_generator_runs_continuous_and_app_runs_on_8000():
    aws = (TF / "aws.tf").read_text(encoding="utf-8")
    assert '"airport-datagen", "--continuous"' in aws
    function = _block(aws, "aws_lambda_function.flight_board")
    assert re.search(r'AWS_LWA_PORT\s*=\s*"8000"', function)
    assert '"--port", "8000"' in function


def test_no_app_runner_left():
    text = "".join(p.read_text(encoding="utf-8") for p in TF.glob("*.tf"))
    assert "aws_apprunner_" not in text and "apprunner" not in text


def test_flight_board_is_a_lambda_behind_a_public_function_url():
    aws = (TF / "aws.tf").read_text(encoding="utf-8")
    function = _block(aws, "aws_lambda_function.flight_board")
    # TCP, not HTTP: with APP_PASSWORD set every HTTP path answers 401, which would fail an HTTP readiness check.
    assert re.search(r'AWS_LWA_READINESS_CHECK_PROTOCOL\s*=\s*"tcp"', function)
    assert re.search(r"publish\s*=\s*true", function)
    assert re.search(r'package_type\s*=\s*"Image"', function)
    assert re.search(r"STREAMHOUSE_SETTINGS_SECRET_ARN\s*=\s*aws_secretsmanager_secret\.settings\.arn", function)
    # A new secret version changes the configuration, so Lambda publishes a version the alias moves to.
    assert re.search(r"STREAMHOUSE_SETTINGS_VERSION\s*=\s*aws_secretsmanager_secret_version\.settings", function)
    # Run the console script, not `uv run`: Lambda's filesystem is read-only outside /tmp.
    assert '"/app/.venv/bin/airport-app"' in function
    assert re.search(r"timeout\s*=\s*120\b", function)  # App Runner's request timeout

    url = _block(aws, "aws_lambda_function_url.flight_board")
    assert re.search(r'authorization_type\s*=\s*"NONE"', url) and "aws_lambda_alias.live.name" in url

    concurrency = _block(aws, "aws_lambda_provisioned_concurrency_config.flight_board")
    assert re.search(r"provisioned_concurrent_executions\s*=\s*1\b", concurrency)
    assert "aws_lambda_alias.live.name" in concurrency

    # AWS needs both statements for a public URL: InvokeFunctionUrl (auth NONE) and InvokeFunction via the URL.
    permissions = re.findall(r'resource "aws_lambda_permission" "\w+" \{.*?\n\}', aws, re.S)
    actions = {re.search(r'action\s*=\s*"([^"]+)"', p).group(1): p for p in permissions}
    assert set(actions) == {"lambda:InvokeFunctionUrl", "lambda:InvokeFunction"}
    assert re.search(r'function_url_auth_type\s*=\s*"NONE"', actions["lambda:InvokeFunctionUrl"])
    assert re.search(r"invoked_via_function_url\s*=\s*true", actions["lambda:InvokeFunction"])
    for permission in permissions:
        assert re.search(r'principal\s*=\s*"\*"', permission) and "aws_lambda_alias.live.name" in permission

    role = _block(aws, "aws_iam_role.flight_board")
    assert "lambda.amazonaws.com" in role
    # It writes only to its own log group and can't create one, so an instance still shutting down after a
    # destroy can't re-create the group and break the next deploy.
    write_logs = _block(aws, "aws_iam_role_policy.flight_board_logs")
    assert '"logs:CreateLogStream", "logs:PutLogEvents"' in write_logs
    assert "aws_cloudwatch_log_group.flight_board.arn" in write_logs
    assert "policy/service-role/AWSLambdaBasicExecutionRole" not in aws and '"logs:CreateLogGroup"' not in aws
    logs = _block(aws, "aws_cloudwatch_log_group.flight_board")
    assert '"/aws/lambda/' in logs and re.search(r"retention_in_days\s*=\s*14", logs)


def test_image_carries_the_lambda_web_adapter():
    dockerfile = (ROOT / "Dockerfile").read_text(encoding="utf-8")
    assert re.search(r"aws-lambda-adapter:\d+\.\d+\.\d+ /lambda-adapter /opt/extensions/lambda-adapter", dockerfile)


def test_length_limited_names_are_checked_against_their_aws_limits():
    text = "".join(p.read_text(encoding="utf-8") for p in TF.glob("*.tf"))
    assert re.search(r"flight_board_function\s*=\s*\{[^\n]*limit\s*=\s*64", text)
    assert re.search(r"flight_board_role\s*=\s*\{[^\n]*limit\s*=\s*64", text)
    assert re.search(r"generator_execution_role\s*=\s*\{[^\n]*limit\s*=\s*64", text)
    assert "precondition" in text and "names_too_long" in text


def test_outputs_name_the_function_and_its_logs():
    outputs = (TF / "outputs.tf").read_text(encoding="utf-8")
    for name in ("app_url", "app_password", "app_function_name", "app_log_group", "ecs_cluster_name"):
        assert f'output "{name}"' in outputs
    assert 'trimsuffix(aws_lambda_function_url.flight_board.function_url, "/")' in outputs


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
        "CONFLUENT_RTCE_API_KEY=k\nCONFLUENT_RTCE_API_SECRET=s\nDEMO_USER_EMAILS=a+1@x.io, a+2@x.io\n", encoding="utf-8"
    )
    outputs = {"image_uri": "repo:old", "app_url": "https://board", "app_password": "pw"}
    monkeypatch.setattr(deploy, "_wait_for_demo_schemas", lambda root: None)
    monkeypatch.setattr(airport_datagen, "run_continuous", lambda *a, **k: calls.append(("history", a, k)))
    monkeypatch.setattr(deploy.setup_rtce, "main", lambda argv: calls.append(("rtce", argv)))
    monkeypatch.setattr(deploy.setup_rtce, "core_infra", lambda _: INFRA)
    monkeypatch.setattr(deploy, "_start_agent", lambda root: calls.append("agent"))
    monkeypatch.setattr(deploy, "_compact_sources", lambda infra: calls.append("compact"))
    monkeypatch.setattr(deploy, "_prepare_quick", lambda root, infra, emails: calls.append(("quick", emails)))
    monkeypatch.setattr(deploy, "_push_image", lambda root, region: calls.append("image") or "repo:new")
    monkeypatch.setattr(deploy, "_always_on_outputs", lambda root: outputs)
    monkeypatch.setattr(deploy, "run_terraform", lambda path, **k: calls.append(("terraform", path.name, k)) or True)
    monkeypatch.setattr(deploy, "_redeploy_generator", lambda outputs, region: calls.append("redeploy"))
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
    assert _names(calls) == ["history", "rtce", "agent", "compact", "quick", "image", "terraform", "users", "card"]
    history = calls[0]
    assert history[1] == (airport_datagen.SEED,) and history[2]["history"] is True
    assert history[2]["until"] == history[2]["now"]  # publishes and returns, never streams
    assert calls[1][1] == ["--client", "none"]
    assert calls[4] == ("quick", ["a+1@x.io", "a+2@x.io"])  # before terraform/always-on, which reads the ARNs
    assert calls[6] == ("terraform", "always-on", {})
    assert calls[7] == ("users", "env-1", ["a+1@x.io", "a+2@x.io"])
    assert deploy.os.environ["TF_VAR_image_uri"] == "repo:new"
    assert deploy.os.environ["TF_VAR_rtce_api_key"] == "k"


def test_always_on_redeploys_when_the_image_is_unchanged(monkeypatch, tmp_path):
    # A rotated secret doesn't restart ECS on its own; a new image does. (Lambda publishes a new version itself.)
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
    creds_file.write_text("", encoding="utf-8")
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
    creds_file.write_text("TF_VAR_always_on=true\nTF_VAR_deployment_name=mine\n", encoding="utf-8")
    args = argparse_namespace()
    creds = deploy.dotenv_values(str(creds_file))
    assert deploy._always_on_mode(args, creds_file, creds)
    deploy._save_always_on(creds_file, creds)
    assert deploy.dotenv_values(str(creds_file))["TF_VAR_deployment_name"] == "mine"
    assert not deploy._workshop_mode(False, args.with_labs, creds_file, {})


def _interactive(monkeypatch, tmp_path, text, ready):
    creds_file = tmp_path / "credentials.env"
    creds_file.write_text(text, encoding="utf-8")
    monkeypatch.chdir(tmp_path)  # core's outputs fall back to the working directory: keep this checkout's state out
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
    (tmp_path / "credentials.env").write_text("TF_VAR_always_on=true\nTF_VAR_continuous=true\n", encoding="utf-8")
    for env in ("always-on", "airline-demo", "core"):
        (tmp_path / "terraform" / env).mkdir(parents=True)
        (tmp_path / "terraform" / env / "terraform.tfstate").write_text("{}", encoding="utf-8")
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
    (tmp_path / "terraform/always-on/terraform.tfstate").write_text("{}", encoding="utf-8")
    assert destroy._envs_to_destroy(None, tmp_path)[0] == "always-on"


def _deployed_before(tmp_path):
    (tmp_path / "terraform" / "always-on").mkdir(parents=True)
    (tmp_path / "terraform" / "always-on" / "terraform.tfstate").write_text("{}", encoding="utf-8")


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
    (tmp_path / "credentials.env").write_text(env, encoding="utf-8")
    monkeypatch.setattr(deploy.setup_rtce, "core_infra", lambda _: None)
    assert deploy._start_agent(tmp_path) is False


def test_failed_agent_on_always_on_says_to_rerun_deploy(monkeypatch, tmp_path, capsys):
    _agent_fails(monkeypatch, tmp_path, "TF_VAR_always_on=true\n")
    out = capsys.readouterr().out
    assert "uv run deploy --always-on" in out and "--offers generator" not in out


def test_failed_agent_on_the_scripted_demo_suggests_the_generator(monkeypatch, tmp_path, capsys):
    _agent_fails(monkeypatch, tmp_path, "")
    assert "uv run airport-datagen --offers generator" in capsys.readouterr().out


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="chmod only sets the read-only bit on Windows; the folder's ACL decides who reads it",
)
def test_card_is_readable_only_by_its_owner(tmp_path):
    path = deploy._write_card(tmp_path, {"app_url": "https://board", "app_password": "pw"}, ["a@x.io"])
    assert path.stat().st_mode & 0o777 == 0o600


def test_redeploy_restarts_only_the_generator(monkeypatch):
    runs = []
    monkeypatch.setattr(deploy.subprocess, "run", lambda cmd, **k: runs.append(cmd) or _completed(cmd))
    deploy._redeploy_generator({"ecs_cluster_name": "c", "generator_service_name": "g"}, "us-east-1")
    assert [cmd[:3] for cmd in runs] == [["aws", "ecs", "update-service"]]


def test_push_image_returns_the_pushed_digest(monkeypatch, tmp_path):
    repo = "1.dkr.ecr.us-east-1.amazonaws.com/demo-images"
    runs = []

    def fake_run(cmd, **kwargs):
        runs.append(cmd)
        if cmd[:3] == ["docker", "image", "inspect"]:
            return _completed(cmd, json.dumps(["other@sha256:aa", f"{repo}@sha256:bb"]))
        return _completed(cmd)

    monkeypatch.setattr(deploy, "run_terraform", lambda *a, **k: True)
    monkeypatch.setattr(deploy, "_ecr_repository_url", lambda path: repo)
    monkeypatch.setattr(deploy, "_image_tag", lambda root: "abc123")
    monkeypatch.setattr(deploy.subprocess, "run", fake_run)
    monkeypatch.setenv("TF_VAR_image_uri", "x")
    assert deploy._push_image(tmp_path, "us-east-1") == f"{repo}@sha256:bb"
    build = next(cmd for cmd in runs if cmd[:2] == ["docker", "build"])
    assert "--sbom=false" in build
    assert "--provenance=false" in build  # Lambda takes a plain image manifest, not a manifest list


# ---------------------------------------------------------------------------
# The flight board's settings on Lambda
# ---------------------------------------------------------------------------


def test_settings_secret_fills_the_environment_without_overwriting(monkeypatch):
    from scripts import airport_app

    requested = []

    class FakeSecrets:
        def get_secret_value(self, SecretId):
            requested.append(SecretId)
            return {"SecretString": json.dumps({"APP_PASSWORD": "from-secret", "STREAMHOUSE_REGION": "us-east-1"})}

    fake_boto3 = type(sys)("boto3")
    fake_boto3.client = lambda service: FakeSecrets() if service == "secretsmanager" else pytest.fail(service)
    monkeypatch.setitem(sys.modules, "boto3", fake_boto3)
    monkeypatch.setenv("STREAMHOUSE_SETTINGS_SECRET_ARN", "arn:aws:secretsmanager:us-east-1:1:secret:demo")
    monkeypatch.setenv("APP_PASSWORD", "already-set")
    monkeypatch.setenv("STREAMHOUSE_REGION", "unset")
    monkeypatch.delenv("STREAMHOUSE_REGION")
    airport_app.load_settings_secret()
    assert requested == ["arn:aws:secretsmanager:us-east-1:1:secret:demo"]
    assert airport_app.os.environ["APP_PASSWORD"] == "already-set"
    assert airport_app.os.environ["STREAMHOUSE_REGION"] == "us-east-1"


def test_settings_secret_is_skipped_without_its_arn(monkeypatch):
    from scripts import airport_app

    monkeypatch.delenv("STREAMHOUSE_SETTINGS_SECRET_ARN", raising=False)
    monkeypatch.setitem(sys.modules, "boto3", None)  # importing it would fail
    airport_app.load_settings_secret()


def test_tableflow_waits_for_its_iam_role():
    # Propagation time for the new role. It didn't cure the 2026-11-12/17 "Unable to assume" failures.
    analytics = (ROOT / "terraform/tableflow/analytics.tf").read_text()
    sleep = _block(analytics, "time_sleep.tableflow_role")
    assert re.search(r'create_duration\s*=\s*"30s"', sleep)
    assert "aws_iam_role_policy_attachment.tableflow_s3" in sleep
    assert "aws_iam_role_policy_attachment.tableflow_glue" in sleep
    for address in ("confluent_tableflow_topic.analytics", "confluent_catalog_integration.glue"):
        assert "time_sleep.tableflow_role" in _block(analytics, address)


# --- Amazon Quick over the Tableflow history ---------------------------------


def _quick() -> str:
    return (TF / "quick.tf").read_text()


def test_quick_never_manages_the_account_subscription():
    # The subscription is shared by the whole AWS account: destroying it would delete every Quick user and asset.
    text = "".join(
        p.read_text(encoding="utf-8") for p in (ROOT / "terraform").rglob("*.tf") if ".terraform" not in p.parts
    )
    assert "aws_quicksight_account_subscription" not in text
    assert "aws_quicksight_account_settings" not in text


def test_quick_reads_athena_through_its_own_role():
    quick = _quick()
    source = _block(quick, "aws_quicksight_data_source.athena")
    assert re.search(r'type\s*=\s*"ATHENA"', source)
    assert "aws_iam_role.quick_athena[0].arn" in source and "aws_athena_workgroup.analytics[0].name" in source
    assert "quicksight.amazonaws.com" in _block(quick, "aws_iam_role.quick_athena")
    for name in ("flight_impact", "passenger_recommendations"):
        dataset = _block(quick, f"aws_quicksight_data_set.{name}")
        assert re.search(r'import_mode\s*=\s*"DIRECT_QUERY"', dataset)
        assert f'"{name}"' in dataset and "local.analytics.glue_database" in dataset
    assert "{delay_minutes} >= 15" in _block(quick, "aws_quicksight_data_set.flight_impact")
    assert "dateDiff({impacted_at}, {recommended_at}" in _block(
        quick, "aws_quicksight_data_set.passenger_recommendations"
    )


def test_athena_results_bucket_force_destroys():
    quick = _quick()
    assert re.search(r"force_destroy\s*=\s*true", _block(quick, "aws_s3_bucket.athena_results"))
    workgroup = _block(quick, "aws_athena_workgroup.analytics")
    assert re.search(r"force_destroy\s*=\s*true", workgroup) and "aws_s3_bucket.athena_results[0]" in workgroup


def test_quick_is_skipped_without_principals():
    variables = (TF / "variables.tf").read_text()
    assert re.search(r'variable "quick_principal_arns" \{[^}]*default\s*=\s*\[\]', variables, re.S)
    assert "length(var.quick_principal_arns) > 0" in _quick()


def test_topic_create_is_idempotent_and_destroy_tolerates_a_missing_topic():
    # The AWS provider has no Topic resource, so terraform_data drives the CLI.
    topic = _block(_quick(), "terraform_data.quick_topic")
    assert "aws quicksight delete-topic" in topic and "aws quicksight create-topic" in topic
    assert topic.index("delete-topic") < topic.index("create-topic")  # a rerun replaces a leftover topic
    assert "update-topic-permissions" in topic
    assert re.search(r"when\s*=\s*destroy", topic) and "|| true" in topic
    assert "aws_quicksight_data_set.flight_impact[0].arn" in topic
    assert "aws_quicksight_data_set.passenger_recommendations[0].arn" in topic


def test_topic_spells_out_the_demo_definitions():
    quick = _quick()
    assert "15 minutes or more" in quick and "BOOKED" in quick
    for synonym in ("hotel spend", "impacted passengers", "recovery time"):
        assert f'"{synonym}"' in quick
    assert 'output "quick_topic_id"' in (TF / "outputs.tf").read_text()


def test_quick_principals_match_emails_case_insensitively(monkeypatch, capsys):
    users = {"UserList": [{"Email": "Me@X.io", "Arn": "arn:q:me"}, {"Email": "other@x.io", "Arn": "arn:q:o"}]}
    seen = []
    monkeypatch.setattr(
        deploy.subprocess, "run", lambda cmd, **k: seen.append(cmd) or _completed(cmd, json.dumps(users))
    )
    assert deploy._quick_principals("1", ["me@x.io", "nobody@x.io"]) == ["arn:q:me"]
    assert "No Quick user for nobody@x.io" in capsys.readouterr().out
    assert seen[0][seen[0].index("--region") + 1] == deploy.QUICK_IDENTITY_REGION == "us-east-1"


def _airline_state(tmp_path, monkeypatch, glue_database="lkc-1"):
    state = tmp_path / "terraform/tableflow/terraform.tfstate"
    state.parent.mkdir(parents=True)
    state.write_text("{}")
    monkeypatch.setattr(deploy, "run_terraform_output", lambda path: {"glue_database": glue_database})
    monkeypatch.setattr(deploy, "_aws_account_id", lambda: "1")
    monkeypatch.delenv("TF_VAR_quick_principal_arns", raising=False)


def test_prepare_quick_shares_with_the_matched_users(monkeypatch, tmp_path):
    _airline_state(tmp_path, monkeypatch)
    waited = []
    monkeypatch.setattr(deploy, "_wait_for_glue_tables", lambda *a, **k: waited.append(a) or True)
    monkeypatch.setattr(deploy, "_quick_principals", lambda account, emails: ["arn:q:me"])
    deploy._prepare_quick(tmp_path, INFRA, ["me@x.io"])
    assert json.loads(deploy.os.environ["TF_VAR_quick_principal_arns"]) == ["arn:q:me"]
    assert waited == [("us-east-1", "lkc-1", deploy.QUICK_TABLES)]
    monkeypatch.delenv("TF_VAR_quick_principal_arns")


def test_prepare_quick_skips_when_glue_tables_never_appear(monkeypatch, tmp_path, capsys):
    _airline_state(tmp_path, monkeypatch)
    monkeypatch.setattr(deploy, "_wait_for_glue_tables", lambda *a, **k: False)
    monkeypatch.setattr(deploy, "_tableflow_errors", lambda infra: {"flight_impact": "Unable to assume the IAM role"})
    monkeypatch.setattr(deploy, "_quick_principals", lambda *a: pytest.fail("shared without tables"))
    deploy._prepare_quick(tmp_path, INFRA, ["me@x.io"])
    out = capsys.readouterr().out
    assert deploy.os.environ["TF_VAR_quick_principal_arns"] == "[]"
    assert "Unable to assume the IAM role" in out
    assert """-replace='confluent_tableflow_topic.analytics["flight_impact"]'""" in out
    monkeypatch.delenv("TF_VAR_quick_principal_arns")


def test_prepare_quick_skips_without_tableflow(monkeypatch, tmp_path, capsys):
    _airline_state(tmp_path, monkeypatch, glue_database="")
    monkeypatch.setattr(deploy, "_wait_for_glue_tables", lambda *a, **k: pytest.fail("waited without Tableflow"))
    deploy._prepare_quick(tmp_path, INFRA, ["me@x.io"])
    assert deploy.os.environ["TF_VAR_quick_principal_arns"] == "[]"
    monkeypatch.delenv("TF_VAR_quick_principal_arns")


def test_wait_for_glue_tables_polls_until_both_exist(monkeypatch):
    found = iter([{"flight_impact"}, {"flight_impact", "passenger_recommendations"}])
    monkeypatch.setattr(deploy, "_glue_tables", lambda region, database: next(found))
    monkeypatch.setattr(deploy.time, "sleep", lambda seconds: None)
    assert deploy._wait_for_glue_tables("us-east-1", "lkc-1", deploy.QUICK_TABLES, timeout_s=60)


def test_wait_for_glue_tables_gives_up(monkeypatch):
    monkeypatch.setattr(deploy, "_glue_tables", lambda region, database: set())
    clock = iter(range(0, 10_000, 30))
    monkeypatch.setattr(deploy.time, "monotonic", lambda: next(clock))
    monkeypatch.setattr(deploy.time, "sleep", lambda seconds: None)
    assert not deploy._wait_for_glue_tables("us-east-1", "lkc-1", deploy.QUICK_TABLES, timeout_s=90)


def test_prepare_quick_keeps_the_existing_quick_when_a_rerun_cant_prepare(monkeypatch, tmp_path, capsys):
    # "[]" on a rerun would make Terraform delete the datasets and the Topic people already use.
    _airline_state(tmp_path, monkeypatch)
    monkeypatch.setattr(deploy, "_wait_for_glue_tables", lambda *a, **k: False)
    monkeypatch.setattr(deploy, "_tableflow_errors", lambda infra: {})
    monkeypatch.setattr(deploy, "_always_on_outputs", lambda root: {"quick_principal_arns": ["arn:q:me"]})
    deploy._prepare_quick(tmp_path, INFRA, ["me@x.io"])
    assert json.loads(deploy.os.environ["TF_VAR_quick_principal_arns"]) == ["arn:q:me"]
    assert "keeping the Quick setup" in capsys.readouterr().out
    monkeypatch.delenv("TF_VAR_quick_principal_arns")


def test_always_on_outputs_the_quick_users_it_shared_with():
    assert re.search(
        r'output "quick_principal_arns" \{\s*value\s*=\s*var\.quick_principal_arns', (TF / "outputs.tf").read_text()
    )


# --- Quick never blocks the always-on deploy (final review fixes) ----------------


def test_preflight_warns_and_skips_quick_when_quick_actions_are_denied(monkeypatch, capsys):
    monkeypatch.delenv(deploy.QUICK_DENIED, raising=False)
    _aws(monkeypatch, {"quicksight:CreateTopic": "implicitDeny"})
    deploy._preflight_always_on({"TF_VAR_aws_tableflow_access_key": "AKIA"})  # no SystemExit
    assert deploy.os.environ[deploy.QUICK_DENIED] == "1"
    assert "quicksight:CreateTopic" in capsys.readouterr().out
    monkeypatch.delenv(deploy.QUICK_DENIED)


def test_prepare_quick_skips_when_the_operator_cant_create_quick(monkeypatch, tmp_path):
    _airline_state(tmp_path, monkeypatch)
    monkeypatch.setenv(deploy.QUICK_DENIED, "1")
    monkeypatch.setattr(deploy, "_wait_for_glue_tables", lambda *a, **k: pytest.fail("waited although denied"))
    assert deploy._prepare_quick(tmp_path, INFRA, ["me@x.io"]) == []
    assert deploy.os.environ["TF_VAR_quick_principal_arns"] == "[]"
    monkeypatch.delenv("TF_VAR_quick_principal_arns")


def test_quick_principals_leave_out_readers(monkeypatch, capsys):
    # Quick rejects owner permissions on a data source or dataset for a Reader, which would fail the whole apply.
    users = {
        "UserList": [
            {"Email": "author@x.io", "Arn": "arn:q:author", "Role": "AUTHOR_PRO"},
            {"Email": "reader@x.io", "Arn": "arn:q:reader", "Role": "READER"},
        ]
    }
    monkeypatch.setattr(deploy.subprocess, "run", lambda cmd, **k: _completed(cmd, json.dumps(users)))
    assert deploy._quick_principals("1", ["author@x.io", "reader@x.io"]) == ["arn:q:author"]
    assert "reader@x.io is a Quick READER" in capsys.readouterr().out


def test_wait_for_glue_tables_stops_when_tableflow_failed(monkeypatch):
    monkeypatch.setattr(deploy, "_glue_tables", lambda region, database: set())
    monkeypatch.setattr(deploy.time, "sleep", lambda seconds: pytest.fail("waited after Tableflow failed"))
    assert not deploy._wait_for_glue_tables("us-east-1", "lkc-1", deploy.QUICK_TABLES, stop=lambda: True)


def test_a_failed_quick_apply_retries_with_the_last_quick_users(monkeypatch, tmp_path, capsys):
    calls = []
    _stub_finish(monkeypatch, tmp_path, calls)

    def prepare(root, infra, emails):
        deploy.os.environ["TF_VAR_quick_principal_arns"] = '["arn:q:new"]'
        return []

    applies = iter([False, True])
    monkeypatch.setattr(deploy, "_prepare_quick", prepare)
    monkeypatch.setattr(
        deploy, "run_terraform", lambda path, **k: calls.append(("terraform", path.name)) or next(applies)
    )
    deploy._finish_always_on(tmp_path)
    assert [c for c in calls if c[0] == "terraform"] == [("terraform", "always-on")] * 2
    assert deploy.os.environ["TF_VAR_quick_principal_arns"] == "[]"
    assert "card" in calls and "Quick" in capsys.readouterr().out
    monkeypatch.delenv("TF_VAR_quick_principal_arns")


def test_the_scene_3_queries_are_saved_in_the_workgroup():
    names = {p.name for p in (TF / "athena").glob("*.sql")}
    assert names == {
        "delays-impact-and-hotel-cost.sql",
        "average-recovery-time.sql",
        "hotel-cost-ra417-vs-other-flights.sql",
        "one-row-per-key.sql",
    }
    query = _block(_quick(), "aws_athena_named_query.history")
    assert 'fileset("${path.module}/athena", "*.sql")' in query and "aws_athena_workgroup.analytics[0].id" in query
