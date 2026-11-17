"""
Credential loading and management utilities.

Provides functions for:
- Loading credentials from credentials.env files
- Generating Confluent Cloud API keys via CLI
"""

import shutil
import subprocess
from pathlib import Path

from dotenv import dotenv_values


def load_or_create_credentials_file(root: Path) -> tuple[Path, dict[str, str]]:
    """
    Load existing credentials.env or create from example.

    Args:
        root: Project root directory

    Returns:
        Tuple of (credentials file path, credentials dictionary)
    """
    creds_file = root / "credentials.env"
    example_file = root / "credentials.env.example"

    if creds_file.exists():
        return creds_file, dotenv_values(creds_file)

    if example_file.exists():
        shutil.copy(example_file, creds_file)
        example_file.unlink()
        print(f"\nCreated {creds_file} from example template.")
    else:
        creds_file.touch()
        print(f"\nCreated new {creds_file}.")

    return creds_file, {}


def generate_confluent_api_keys(
    prefix: str = "streamhouse",
) -> tuple[str | None, str | None]:
    """Create a Cloud API key owned by the user signed in to the Confluent CLI (they need OrganizationAdmin).

    Not a service account's key: Tableflow can't assume the IAM role of a provider integration that a
    service account's key created ("Unable to assume the IAM role"), and Terraform creates it with this key.
    Returns (api_key, api_secret), or (None, None) if the CLI fails.
    """
    import json

    print("Creating a Cloud API key owned by your Confluent user...")
    result = subprocess.run(
        [
            "confluent",
            "api-key",
            "create",
            "--resource",
            "cloud",
            "--description",
            f"{prefix} deploy key",
            "-o",
            "json",
        ],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        print(f"Error creating the API key: {result.stderr.strip()}")
        return None, None
    try:
        key = json.loads(result.stdout[result.stdout.index("{") :])  # the CLI can print a notice first
    except ValueError:
        print("Error: couldn't read the new API key from the Confluent CLI.")
        return None, None
    print("✓ API key created")
    return key.get("api_key"), key.get("api_secret")
