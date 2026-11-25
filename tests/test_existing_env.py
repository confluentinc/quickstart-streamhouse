"""Deploying into an attendee's existing environment: core skips what an EnvironmentAdmin can't make."""

from pathlib import Path

import pytest
from dotenv import dotenv_values

from scripts import setup_rtce

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
