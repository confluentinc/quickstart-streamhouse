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


@pytest.mark.parametrize(
    ("signed_in", "saved", "asked"),
    [
        (True, {}, False),  # the CLI is signed in: no question
        (False, {"CONFLUENT_EMAIL": "e", "CONFLUENT_PASSWORD": "p"}, False),  # a saved login signs in
        (True, {"CONFLUENT_EMAIL": "e", "CONFLUENT_PASSWORD": "p"}, False),
        (False, {}, True),  # neither: ask
    ],
)
def test_deploy_asks_for_the_login_only_when_nothing_signs_in(monkeypatch, tmp_path, signed_in, saved, asked):
    prompts = []
    monkeypatch.setattr(deploy, "check_confluent_login", lambda: signed_in)
    monkeypatch.setattr(deploy, "prompt_and_save_login", lambda creds_file, creds: prompts.append(1) or True)
    monkeypatch.setattr(deploy, "ensure_confluent_login", lambda *a, **k: None)
    deploy._sign_in(tmp_path / "credentials.env", dict(saved))
    assert bool(prompts) is asked


def test_a_deployment_from_another_account_stops_deploy_and_names_its_email(monkeypatch):
    """Replacing the key would leave the deployment unreachable: Terraform and destroy both need its own key."""
    monkeypatch.setattr(deploy, "_key_in_signed_in_account", lambda key: False)
    monkeypatch.setattr(deploy, "generate_confluent_api_keys", lambda prefix: pytest.fail("replaced the key"))
    creds = {**SAVED, "CONFLUENT_DEPLOYED_AS": "owner@example.com"}
    with pytest.raises(SystemExit, match=r"another Confluent account \(owner@example.com\)"):
        deploy._cloud_api_key(creds, "streamhouse", "", deployed=True, email="someone-else@example.com")


def test_a_deleted_key_in_the_same_account_is_replaced_even_with_a_deployment(monkeypatch):
    """Same account that deployed, but the key is gone (or the CLI hiccupped): a new key keeps the deploy going."""
    monkeypatch.setattr(deploy, "_key_in_signed_in_account", lambda key: False)
    monkeypatch.setattr(deploy, "generate_confluent_api_keys", lambda prefix: ("NEWKEY", "new-secret"))
    creds = {**SAVED, "CONFLUENT_DEPLOYED_AS": "owner@example.com"}
    assert deploy._cloud_api_key(creds, "streamhouse", "", deployed=True, email="owner@example.com") == (
        "NEWKEY",
        "new-secret",
    )


def test_without_a_deployment_a_key_from_another_account_is_still_replaced(monkeypatch):
    monkeypatch.setattr(deploy, "_key_in_signed_in_account", lambda key: False)
    monkeypatch.setattr(deploy, "generate_confluent_api_keys", lambda prefix: ("NEWKEY", "new-secret"))
    assert deploy._cloud_api_key(SAVED, "streamhouse", "", deployed=False) == ("NEWKEY", "new-secret")


def test_destroy_forgets_who_deployed():
    from scripts import destroy

    src = (deploy.Path(destroy.__file__)).read_text(encoding="utf-8")
    assert '"CONFLUENT_DEPLOYED_AS"' in src


def test_deploy_names_the_deploying_account_before_checking_the_environment(tmp_path):
    """An existing-environment deployment from account A, rerun as B: say which account, not 'check the ID'."""
    state = tmp_path / "terraform" / "core" / "terraform.tfstate"
    state.parent.mkdir(parents=True)
    state.write_text('{"resources": [{"mode": "managed", "instances": [{}]}]}', encoding="utf-8")
    creds = {"CONFLUENT_DEPLOYED_AS": "owner@example.com"}
    creds_file = tmp_path / "credentials.env"
    with pytest.raises(SystemExit, match=r"another Confluent account \(owner@example.com\)"):
        deploy._check_deploying_account(creds_file, creds, "someone-else@example.com")
    deploy._check_deploying_account(creds_file, creds, "owner@example.com")  # same account: carry on
    deploy._check_deploying_account(creds_file, creds, "")  # email unknown: let the later checks decide
    state.unlink()
    deploy._check_deploying_account(creds_file, creds, "someone-else@example.com")  # nothing deployed


def test_same_email_in_another_organization_is_not_the_same_account(tmp_path, monkeypatch):
    """One email can belong to several orgs: a new key in org 2 would run against org 1's deployment."""
    state = tmp_path / "terraform" / "core" / "terraform.tfstate"
    state.parent.mkdir(parents=True)
    state.write_text('{"resources": [{"mode": "managed", "instances": [{}]}]}', encoding="utf-8")
    creds = {**SAVED, "CONFLUENT_DEPLOYED_AS": "owner@example.com", "CONFLUENT_DEPLOYED_ORG": "org-1"}
    with pytest.raises(SystemExit, match=r"another Confluent organization.*confluent organization use org-1"):
        deploy._check_deploying_account(tmp_path / "credentials.env", creds, "owner@example.com", "org-2")
    deploy._check_deploying_account(tmp_path / "credentials.env", creds, "owner@example.com", "org-1")
    monkeypatch.setattr(deploy, "_key_in_signed_in_account", lambda key: False)
    monkeypatch.setattr(deploy, "generate_confluent_api_keys", lambda prefix: pytest.fail("made a key in org 2"))
    with pytest.raises(SystemExit, match="another Confluent organization"):
        deploy._cloud_api_key(creds, "streamhouse", "", deployed=True, email="owner@example.com", org="org-2")


def test_destroy_forgets_the_deploying_organization():
    from scripts import destroy

    assert "CONFLUENT_DEPLOYED_ORG" in destroy._settings_to_forget(testing=False)


def test_an_unknown_account_gets_an_honest_message_with_the_manual_fix(monkeypatch):
    """No saved email (deployed before this change) or an unreadable one: don't claim it's another account."""
    monkeypatch.setattr(deploy, "_key_in_signed_in_account", lambda key: False)
    with pytest.raises(SystemExit) as stop:
        deploy._cloud_api_key(SAVED, "streamhouse", "", deployed=True, email="", org="")
    message = str(stop.value)
    assert "no longer works" in message and "TF_VAR_confluent_cloud_api_key" in message
    assert "another Confluent account (" not in message


def test_the_ready_question_says_workshop_or_full_demo_and_a_fixed_name():
    assert deploy._ready_question(["flight-disruptions"], True, False, "", "us-east-1", {}) == (
        "Ready to deploy Agentic Airline Operations (workshop) into a new environment in us-east-1?"
    )
    assert deploy._ready_question(
        ["flight-disruptions"], False, False, "", "us-east-1", {"TF_VAR_deployment_name": "river-air-prod"}
    ) == (
        "Ready to deploy Agentic Airline Operations (full demo) as RIVER-AIR-PROD into a new environment in us-east-1?"
    )
    assert deploy._ready_question(["agentic-pit-decisions"], False, False, "env-abc12", "us-east-1", {}) == (
        "Ready to deploy Agentic Pit Decisions into environment env-abc12 in us-east-1?"
    )


def test_people_choose_workshop_without_saving_it_until_ready(tmp_path):
    creds_file = tmp_path / "credentials.env"
    creds = {}
    assert deploy._workshop_mode(False, False, creds_file, creds, save=False)
    assert not creds_file.exists() and deploy.WORKSHOP not in creds
