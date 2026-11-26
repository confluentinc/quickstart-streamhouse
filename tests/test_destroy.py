"""A failed airline-demo destroy must not go on to destroy core underneath it,
unless all that's left is the provider integration core's destroy removes."""

import subprocess
import sys

import pytest

from scripts import destroy, setup_rtce


@pytest.fixture(autouse=True)
def _no_real_mcp_cleanup(monkeypatch):
    """A full destroy edits the user's Claude and Codex config; keep tests off the real ones."""
    monkeypatch.setattr(destroy, "unregister_rtce_server", lambda: None)
    monkeypatch.setattr(destroy, "_cleanup_mcp", lambda root: None)


def _deployment(tmp_path, monkeypatch):
    (tmp_path / "credentials.env").write_text("TF_VAR_resource_prefix=test\n", encoding="utf-8")
    for env in ("airline-demo", "core"):
        (tmp_path / "terraform" / env).mkdir(parents=True)
        (tmp_path / "terraform" / env / "terraform.tfstate").write_text("{}", encoding="utf-8")
    monkeypatch.setattr(destroy, "get_project_root", lambda: tmp_path)
    monkeypatch.setattr(sys, "argv", ["destroy", "--testing"])


def test_failed_demo_destroy_keeps_core(tmp_path, monkeypatch):
    _deployment(tmp_path, monkeypatch)

    attempted = []

    def fake_destroy(env_path):
        attempted.append(env_path.name)
        return env_path.name != "airline-demo"

    monkeypatch.setattr(destroy, "run_terraform_destroy", fake_destroy)
    monkeypatch.setattr(destroy, "release_provider_integration", lambda env_path: False)

    with pytest.raises(SystemExit) as exit_info:
        destroy.main()

    assert exit_info.value.code == 1
    assert attempted == ["airline-demo"]
    assert (tmp_path / "terraform" / "core" / "terraform.tfstate").exists()


def test_stuck_provider_integration_is_left_to_core(tmp_path, monkeypatch):
    _deployment(tmp_path, monkeypatch)
    attempted = []
    released = []

    def fake_destroy(env_path):
        attempted.append(env_path.name)
        return env_path.name != "airline-demo" or bool(released)

    monkeypatch.setattr(destroy, "run_terraform_destroy", fake_destroy)
    monkeypatch.setattr(
        destroy, "release_provider_integration", lambda env_path: released.append(env_path.name) or True
    )

    destroy.main()

    assert released == ["airline-demo"]
    assert attempted == ["airline-demo", "airline-demo", "core"]


@pytest.mark.parametrize(
    ("state", "expected"),
    [
        ("data.aws_caller_identity.current[0]\n" + destroy.TABLEFLOW_PROVIDER_INTEGRATION + "\n", True),
        (
            destroy.TABLEFLOW_PROVIDER_INTEGRATION + '\nconfluent_tag.demo["PII"]\nconfluent_tag.demo["RAW_DATA"]\n',
            True,
        ),
        ('confluent_tag.demo["PII"]\n', True),
        (destroy.TABLEFLOW_PROVIDER_INTEGRATION + "\naws_s3_bucket.analytics[0]\n", False),
        ("aws_s3_bucket.analytics[0]\n", False),
    ],
)
def test_release_only_when_integration_and_tags_are_all_that_is_left(tmp_path, monkeypatch, state, expected):
    calls = []

    def fake_run(cmd, **kwargs):
        calls.append(cmd)
        return subprocess.CompletedProcess(cmd, 0, stdout=state if cmd[2] == "list" else "")

    monkeypatch.setattr(destroy.subprocess, "run", fake_run)

    assert destroy.release_provider_integration(tmp_path) is expected
    assert (any(cmd[:3] == ["terraform", "state", "rm"] for cmd in calls)) is expected


def test_destroy_stops_the_local_datagen_stream(tmp_path, monkeypatch):
    _deployment(tmp_path, monkeypatch)
    monkeypatch.setattr(destroy, "run_terraform_destroy", lambda env_path: True)
    pid_file = tmp_path / "tmp" / "datagen.pid"
    pid_file.parent.mkdir()
    pid_file.write_text("999999", encoding="utf-8")  # not a process that exists, so nothing is killed

    destroy.main()

    assert not pid_file.exists()


def test_destroy_unregisters_the_rtce_mcp_server(tmp_path, monkeypatch):
    _deployment(tmp_path, monkeypatch)
    monkeypatch.setattr(destroy, "run_terraform_destroy", lambda env_path: True)
    removed = []
    monkeypatch.setattr(destroy, "unregister_rtce_server", lambda: removed.append(True))

    destroy.main()

    assert removed == [True]


def test_unregister_removes_direct_write_and_codex_entries(tmp_path, monkeypatch):
    import json

    name = setup_rtce._DEFAULT_SERVER_NAME
    monkeypatch.setattr(setup_rtce.Path, "home", lambda: tmp_path)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(setup_rtce.subprocess, "run", lambda *a, **k: None)

    project = setup_rtce._PROJECT_ROOT.as_posix()
    claude_json = tmp_path / ".claude.json"
    claude_json.write_text(
        json.dumps({"projects": {project: {"mcpServers": {name: {"type": "http"}, "other": {}}}}}),
        encoding="utf-8",
    )
    codex = tmp_path / ".codex" / "config.toml"
    codex.parent.mkdir()
    codex.write_text(f'[mcp_servers.{name}]\nurl = "x"\n\n[mcp_servers.other]\nurl = "y"\n', encoding="utf-8")

    setup_rtce.unregister_rtce_server()

    servers = json.loads(claude_json.read_text(encoding="utf-8"))["projects"][project]["mcpServers"]
    assert servers == {"other": {}}
    assert name not in codex.read_text(encoding="utf-8")
    assert "[mcp_servers.other]" in codex.read_text(encoding="utf-8")


def test_unregister_with_nothing_registered_is_a_noop(tmp_path, monkeypatch):
    monkeypatch.setattr(setup_rtce.Path, "home", lambda: tmp_path)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(setup_rtce.subprocess, "run", lambda *a, **k: None)

    setup_rtce.unregister_rtce_server()


def test_destroy_clears_the_deleted_rtce_key_from_credentials(tmp_path, monkeypatch):
    _deployment(tmp_path, monkeypatch)
    creds = tmp_path / "credentials.env"
    creds.write_text(
        "TF_VAR_resource_prefix=test\nCONFLUENT_RTCE_API_KEY=abc\nCONFLUENT_RTCE_API_SECRET=def\n", encoding="utf-8"
    )
    monkeypatch.setattr(destroy, "run_terraform_destroy", lambda env_path: True)

    destroy.main()

    text = creds.read_text(encoding="utf-8")
    assert "CONFLUENT_RTCE_API" not in text
    assert "TF_VAR_resource_prefix=test" in text
