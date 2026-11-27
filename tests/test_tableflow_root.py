"""Tableflow lives in its own Terraform root, so a workshop deploy never downloads the AWS provider."""

from pathlib import Path

from scripts import deploy, destroy

TF = Path(__file__).parent.parent / "terraform"


def _declares_aws(root: str) -> bool:
    return any("hashicorp/aws" in f.read_text(encoding="utf-8") for f in (TF / root).glob("*.tf"))


def test_only_tableflow_and_always_on_declare_the_aws_provider():
    assert {r.name for r in TF.iterdir() if r.is_dir() and _declares_aws(r.name)} == {"tableflow", "always-on"}


def test_tableflow_owns_the_analytics_outputs_and_airline_demo_dropped_them():
    outputs = (TF / "tableflow/outputs.tf").read_text(encoding="utf-8")
    airline = (TF / "airline-demo/outputs.tf").read_text(encoding="utf-8")
    for name in ("tableflow_topics", "analytics_bucket", "tableflow_role_name", "glue_database"):
        assert f'output "{name}"' in outputs
        assert f'output "{name}"' not in airline


def test_always_on_reads_tableflow_state():
    quick = (TF / "always-on/quick.tf").read_text(encoding="utf-8")
    assert "../tableflow/terraform.tfstate" in quick and "../airline-demo/terraform.tfstate" not in quick


def test_workshops_and_creds_free_demos_never_init_a_root_with_the_aws_provider(monkeypatch):
    monkeypatch.delenv("TF_VAR_enable_analytics", raising=False)
    for labs, workshop, creds in [
        (["flight-disruptions"], True, {"TF_VAR_aws_tableflow_access_key": "AKIA"}),
        (["agentic-pit-decisions"], False, {"TF_VAR_aws_tableflow_access_key": "AKIA"}),
        (["flight-disruptions", "agentic-pit-decisions"], True, {}),
        (["flight-disruptions"], False, {}),  # --with-labs without Tableflow creds
    ]:
        targets = deploy._targets(labs, deploy._tableflow_on(labs, workshop, creds))
        assert not any(_declares_aws(t) for t in targets), (labs, workshop)


def test_a_full_demo_with_tableflow_creds_applies_tableflow_last(monkeypatch):
    monkeypatch.delenv("TF_VAR_enable_analytics", raising=False)
    labs = ["flight-disruptions"]
    on = deploy._tableflow_on(labs, False, {"TF_VAR_aws_tableflow_access_key": "AKIA"})
    assert deploy._targets(labs, on) == ["core", "airline-demo", "tableflow"]
    monkeypatch.setenv("TF_VAR_enable_analytics", "false")
    assert not deploy._tableflow_on(labs, False, {"TF_VAR_aws_tableflow_access_key": "AKIA"})


def test_destroy_takes_tableflow_down_before_airline_demo():
    assert destroy._envs_to_destroy(None) == ["tableflow", "agentic-pit-decisions", "airline-demo", "core"]
    assert destroy._envs_to_destroy("flight-disruptions") == ["tableflow", "airline-demo"]
    assert destroy._envs_to_destroy("agentic-pit-decisions") == ["agentic-pit-decisions"]


def test_the_replace_hint_points_at_the_tableflow_root():
    src = (Path(__file__).parent.parent / "scripts/deploy.py").read_text(encoding="utf-8")
    assert "-chdir=terraform/tableflow" in src and "-chdir=terraform/airline-demo apply" not in src


def test_destroy_waits_before_deleting_the_provider_integration():
    """Confluent deletes Tableflow topics in the background; deleting their integration right after returns 409."""
    analytics = (TF / "tableflow/analytics.tf").read_text(encoding="utf-8")
    sleep = analytics[analytics.index('resource "time_sleep" "tableflow_role"') :]
    assert "destroy_duration" in sleep[: sleep.index("\n}\n")]
