"""
Login verification utilities for Confluent and cloud providers.

Provides functions for:
- Checking Confluent CLI login status
- Checking AWS CLI login status
- Checking Azure CLI login status
"""

import os
import subprocess
import sys
from pathlib import Path

from dotenv import dotenv_values, set_key

from scripts.ui import discard_typeahead, prompt_secret


def check_confluent_login() -> bool:
    """
    Check if user is logged into Confluent CLI.

    Returns:
        True if logged in, False otherwise
    """
    try:
        result = subprocess.run(
            ["confluent", "environment", "list"],
            capture_output=True,
            text=True,
            check=True,
        )
        return result.returncode == 0
    except (subprocess.CalledProcessError, FileNotFoundError):
        return False


def _run_login(email: str, password: str) -> subprocess.CompletedProcess:
    """Run `confluent login --save` with the credentials in env vars, never on argv or stdin.

    --save lets the CLI re-authenticate on its own when the session token expires.
    """
    env = {**os.environ, "CONFLUENT_CLOUD_EMAIL": email, "CONFLUENT_CLOUD_PASSWORD": password}
    return subprocess.run(
        ["confluent", "login", "--save"],
        capture_output=True,
        text=True,
        stdin=subprocess.DEVNULL,
        env=env,
    )


def attempt_confluent_auto_login(creds: dict) -> bool:
    """Try to log into Confluent Cloud using a credentials dict.

    Reads CONFLUENT_EMAIL and CONFLUENT_PASSWORD from the provided dict.
    Returns True on success, False if credentials are missing or login fails.
    """
    email = creds.get("CONFLUENT_EMAIL")
    password = creds.get("CONFLUENT_PASSWORD")
    if not email or not password:
        return False
    result = _run_login(email, password)
    if result.returncode != 0:
        print(f"  Auto-login failed (exit {result.returncode}):")
        if result.stderr.strip():
            print(f"  {result.stderr.strip()}")
        if result.stdout.strip():
            print(f"  {result.stdout.strip()}")
        return False
    return check_confluent_login()


def _attempt_login_quiet(email: str, password: str) -> bool:
    """Attempt Confluent login without printing error output. Returns True on success."""
    result = _run_login(email, password)
    if result.returncode != 0:
        return False
    return check_confluent_login()


def prompt_and_save_login(creds_file, creds: dict) -> bool:
    """Ask for the Confluent Cloud login and save it to credentials.env for auto-login.

    Saves only after a login succeeds, so a typo is never stored. Returns True if
    logged in, False if the user skipped or ran out of attempts.
    """
    print("\nConfluent Cloud login (saved to credentials.env for auto-login):")
    for attempt in range(3):
        discard_typeahead()
        email = input("  Email (Enter to skip): ").strip()
        if not email:
            print("  Skipped. Run `confluent login` manually if your session expires.")
            return False
        password = prompt_secret("  Password: ")
        if _attempt_login_quiet(email, password):
            Path(creds_file).touch(exist_ok=True)  # set_key needs the file to exist
            set_key(str(creds_file), "CONFLUENT_EMAIL", email)
            set_key(str(creds_file), "CONFLUENT_PASSWORD", password)
            creds["CONFLUENT_EMAIL"] = email
            creds["CONFLUENT_PASSWORD"] = password
            print("  ✓ Logged in and saved.")
            return True
        remaining = 2 - attempt
        print(f"  Login failed. {remaining} attempt(s) left." if remaining else
              "  Login failed. Skipping (use `confluent login --sso` if you use SSO).")
    return False


def ensure_confluent_login(creds: dict | None = None, creds_file=None, interactive: bool = False) -> None:
    """Check CLI login → auto-login from saved creds → (interactive) ask again → exit(1) with instructions.

    With interactive=True and a creds_file, a saved login that no longer works
    (e.g. after a password change) is asked for again and replaced, instead of
    failing the run.
    """
    if check_confluent_login():
        return
    if creds is None:
        creds = dotenv_values(
            str(Path(__file__).parent.parent / "credentials.env")
        )
    if attempt_confluent_auto_login(creds):
        return
    if interactive and creds_file:
        if creds.get("CONFLUENT_EMAIL"):
            print("  The Confluent Cloud login saved in credentials.env no longer works.")
        if prompt_and_save_login(creds_file, creds):
            return
    print("\nError: Not logged into Confluent Cloud.")
    print("Please run: confluent login --save")
    print("  (or rerun `uv run deploy` to save credentials for auto-login)")
    print("  (SSO accounts: run `confluent login --sso`)")
    sys.exit(1)
