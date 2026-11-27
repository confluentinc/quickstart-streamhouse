"""Deploy reuses the saved Cloud API key, or creates one, without asking."""

import pytest

from scripts import deploy

SAVED = {"TF_VAR_confluent_cloud_api_key": "SAVEDKEY", "TF_VAR_confluent_cloud_api_secret": "saved-secret"}


def test_a_saved_key_is_reused_without_creating_one(monkeypatch):
    monkeypatch.setattr(deploy, "_key_in_signed_in_account", lambda key: True)
    monkeypatch.setattr(deploy, "generate_confluent_api_keys", lambda prefix: pytest.fail("created a key"))
    monkeypatch.setattr(deploy, "prompt_with_default", lambda *a, **k: pytest.fail("asked for a key"))
    assert deploy._cloud_api_key(SAVED, "streamhouse", "") == ("SAVEDKEY", "saved-secret")


def test_without_a_saved_key_deploy_creates_one_without_asking(monkeypatch):
    monkeypatch.setattr(deploy, "generate_confluent_api_keys", lambda prefix: ("NEWKEY", "new-secret"))
    monkeypatch.setattr(deploy, "prompt_with_default", lambda *a, **k: pytest.fail("asked for a key"))
    assert deploy._cloud_api_key({}, "streamhouse", "") == ("NEWKEY", "new-secret")


def test_when_creating_fails_deploy_asks_for_a_key(monkeypatch):
    monkeypatch.setattr(deploy, "generate_confluent_api_keys", lambda prefix: (None, None))
    answers = iter(["TYPEDKEY", "typed-secret"])
    monkeypatch.setattr(deploy, "prompt_with_default", lambda *a, **k: next(answers))
    assert deploy._cloud_api_key({}, "streamhouse", "env-abc123") == ("TYPEDKEY", "typed-secret")


def test_a_saved_key_from_another_account_is_replaced(monkeypatch):
    """Signed in to a different Confluent account (or the key was deleted): the saved key would build elsewhere."""
    monkeypatch.setattr(deploy, "_key_in_signed_in_account", lambda key: False)
    monkeypatch.setattr(deploy, "generate_confluent_api_keys", lambda prefix: ("NEWKEY", "new-secret"))
    monkeypatch.setattr(deploy, "prompt_with_default", lambda *a, **k: pytest.fail("asked for a key"))
    assert deploy._cloud_api_key(SAVED, "streamhouse", "") == ("NEWKEY", "new-secret")


def test_the_key_check_asks_the_signed_in_cli(monkeypatch):
    calls = []

    class Result:
        returncode = 1

    monkeypatch.setattr(deploy.subprocess, "run", lambda argv, **kwargs: calls.append(argv) or Result())
    assert deploy._key_in_signed_in_account("SAVEDKEY") is False
    assert calls[0][1:4] == ["api-key", "describe", "SAVEDKEY"]


def test_deploy_shows_the_signed_in_email(monkeypatch):
    context = {"name": "login-someone@example.com-https://confluent.cloud", "platform": "confluent.cloud"}
    monkeypatch.setattr(deploy.existing_env, "_cli_json", lambda *args: context)
    assert deploy._signed_in_email() == "someone@example.com"


def test_no_email_when_the_cli_context_is_unreadable(monkeypatch):
    monkeypatch.setattr(deploy.existing_env, "_cli_json", lambda *args: None)
    assert deploy._signed_in_email() == ""
