"""Static checks on the always-on image and the terraform/always-on root."""

import re
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


def test_length_limited_names_are_checked_against_their_aws_limits():
    text = "".join(p.read_text() for p in TF.glob("*.tf"))
    # App Runner auto-scaling configuration names are 4-32 characters, so "<name>-flight-board" is too long.
    scaling = re.search(r'auto_scaling_configuration_name\s*=\s*(.+)', text).group(1)
    assert "flight-board" not in scaling
    assert re.search(r"flight_board_scaling\s*=\s*\{[^\n]*limit\s*=\s*32", text)
    assert re.search(r"flight_board_service\s*=\s*\{[^\n]*limit\s*=\s*40", text)
    assert re.search(r"generator_execution_role\s*=\s*\{[^\n]*limit\s*=\s*64", text)
    assert "precondition" in text and "names_too_long" in text
