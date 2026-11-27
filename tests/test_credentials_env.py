import pytest

from scripts import terraform
from scripts.terraform import ENV_CREDENTIALS, credentials_from_env, extract_kafka_credentials


@pytest.fixture(autouse=True)
def clean_env(monkeypatch):
    for name in [*ENV_CREDENTIALS.values(), "STREAMHOUSE_ENVIRONMENT_ID", "STREAMHOUSE_CLUSTER_ID"]:
        monkeypatch.delenv(name, raising=False)


def test_kafka_credentials_come_from_env_when_all_set(monkeypatch, tmp_path):
    monkeypatch.setattr(terraform, "find_state_files", lambda *a, **k: pytest.fail("looked for state"))
    for name in ENV_CREDENTIALS.values():
        monkeypatch.setenv(name, name.lower())
    monkeypatch.setenv("STREAMHOUSE_CLUSTER_ID", "lkc-1")
    creds = extract_kafka_credentials("aws", tmp_path)
    assert creds["kafka_api_key"] == "streamhouse_kafka_api_key"
    assert creds["cluster_id"] == "lkc-1"
    assert creds["environment_id"] == ""
    assert creds["compute_pool_id"] == ""
    assert "hotel_webhook_url" not in creds


def test_partial_env_falls_back_to_state(monkeypatch, tmp_path):
    monkeypatch.setenv("STREAMHOUSE_KAFKA_API_KEY", "x")
    assert credentials_from_env() is None
    with pytest.raises(FileNotFoundError):
        extract_kafka_credentials("aws", tmp_path)


def test_generated_cloud_key_is_owned_by_the_signed_in_user(monkeypatch):
    # Tableflow can't assume the IAM role of a provider integration a service account's key created
    # ("Unable to assume the IAM role"), and Terraform creates the integration with this key.
    import json
    import subprocess

    from scripts import credentials

    runs = []
    body = json.dumps({"api_key": "KEY", "api_secret": "SECRET"})

    def fake_run(cmd, **kwargs):
        runs.append(cmd)
        return subprocess.CompletedProcess(cmd, 0, stdout=f"deprecation notice\n{body}\n", stderr="")

    monkeypatch.setattr(credentials.subprocess, "run", fake_run)
    assert credentials.generate_confluent_api_keys("river-air") == ("KEY", "SECRET")
    assert len(runs) == 1
    assert runs[0][:3] == ["confluent", "api-key", "create"] and "--service-account" not in runs[0]
    assert runs[0][runs[0].index("--resource") + 1] == "cloud"
