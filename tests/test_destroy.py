"""A failed airline-demo destroy must not go on to destroy core underneath it,
unless all that's left is the provider integration core's destroy removes."""

import subprocess
import sys

import pytest

from scripts import destroy


def _deployment(tmp_path, monkeypatch):
    (tmp_path / "credentials.env").write_text("TF_VAR_resource_prefix=test\n")
    for env in ("airline-demo", "core"):
        (tmp_path / "terraform" / env).mkdir(parents=True)
        (tmp_path / "terraform" / env / "terraform.tfstate").write_text("{}")
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
