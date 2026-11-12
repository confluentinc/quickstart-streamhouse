"""Static checks on the always-on image and the terraform/always-on root."""

from pathlib import Path

ROOT = Path(__file__).parent.parent
TF = ROOT / "terraform/always-on"


def test_image_never_contains_credentials():
    ignored = (ROOT / ".dockerignore").read_text().split()
    assert {"credentials.env", "tmp/", "terraform/", ".git/"} <= set(ignored)


def test_no_email_addresses_in_tracked_terraform():
    text = "".join(p.read_text() for p in TF.glob("*.tf"))
    assert "@confluent.io" not in text


def test_generator_runs_continuous_and_app_runs_on_8000():
    aws = (TF / "aws.tf").read_text()
    assert '"airport-datagen", "--continuous"' in aws and "8000" in aws
