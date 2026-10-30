from unittest.mock import patch

import pytest
from dotenv import dotenv_values

from scripts import login_checks

STALE = {"CONFLUENT_EMAIL": "old@example.com", "CONFLUENT_PASSWORD": "old-password"}


def _session(logged_in):
    return patch.object(login_checks, "check_confluent_login", return_value=logged_in)


def test_stale_saved_login_is_asked_for_again_and_replaced(tmp_path):
    creds_file = tmp_path / "credentials.env"
    creds = dict(STALE)
    with (
        _session(False),
        patch.object(login_checks, "attempt_confluent_auto_login", return_value=False),
        patch.object(login_checks, "_attempt_login_quiet", return_value=True),
        patch("builtins.input", return_value="new@example.com"),
        patch.object(login_checks, "prompt_secret", return_value="new-password"),
    ):
        login_checks.ensure_confluent_login(creds, creds_file, interactive=True)
    saved = dotenv_values(creds_file)
    assert saved["CONFLUENT_EMAIL"] == "new@example.com"
    assert saved["CONFLUENT_PASSWORD"] == "new-password"
    assert creds["CONFLUENT_PASSWORD"] == "new-password"


def test_failed_logins_are_never_saved(tmp_path):
    creds_file = tmp_path / "credentials.env"
    with (
        patch.object(login_checks, "_attempt_login_quiet", return_value=False),
        patch("builtins.input", return_value="typo@example.com"),
        patch.object(login_checks, "prompt_secret", return_value="wrong"),
    ):
        assert not login_checks.prompt_and_save_login(creds_file, {})
    assert not creds_file.exists() or "CONFLUENT_PASSWORD" not in dotenv_values(creds_file)


def test_non_interactive_stale_login_exits_without_prompting(tmp_path):
    with (
        _session(False),
        patch.object(login_checks, "attempt_confluent_auto_login", return_value=False),
        patch("builtins.input") as ask,
        pytest.raises(SystemExit),
    ):
        login_checks.ensure_confluent_login(dict(STALE), tmp_path / "credentials.env")
    ask.assert_not_called()


def test_login_passes_credentials_in_env_not_stdin():
    with patch("subprocess.run") as run:
        login_checks._run_login("a@example.com", "pw")
    kwargs = run.call_args.kwargs
    assert kwargs["env"]["CONFLUENT_CLOUD_EMAIL"] == "a@example.com"
    assert kwargs["env"]["CONFLUENT_CLOUD_PASSWORD"] == "pw"
    assert "input" not in kwargs
