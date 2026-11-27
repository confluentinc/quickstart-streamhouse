"""Deploy reuses the saved Cloud API key, or creates one, without asking."""

import pytest

from scripts import deploy

SAVED = {"TF_VAR_confluent_cloud_api_key": "SAVEDKEY", "TF_VAR_confluent_cloud_api_secret": "saved-secret"}


def test_a_saved_key_is_reused_without_creating_one(monkeypatch):
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
