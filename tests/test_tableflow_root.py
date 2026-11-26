"""Tableflow lives in its own Terraform root, so a workshop deploy never downloads the AWS provider."""

from pathlib import Path

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
