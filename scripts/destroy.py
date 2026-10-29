#!/usr/bin/env python3
"""
Destroy the Streamhouse airline flight-recovery demo from Confluent Cloud.
Uses credentials from credentials.env for destruction via Terraform.
"""

import argparse
import os
import shutil
import subprocess
import sys
from pathlib import Path

from dotenv import dotenv_values, unset_key

from .credentials import load_or_create_credentials_file
from .deploy import WORKSHOP
from .login_checks import ensure_confluent_login
from .terraform import get_project_root
from .terraform_runner import run_terraform_destroy


def cleanup_terraform_artifacts(env_path: Path) -> None:
    """
    Remove all terraform artifacts from a directory after successful destroy.

    Removes:
    - *.tfstate* files
    - *.tfvars* files
    - .terraform/ directory
    - .terraform.lock.hcl file
    - FLINK_SQL_COMMANDS.md (auto-generated summary)
    - mcp_commands.txt (legacy file)

    Does NOT remove credentials.env (which is in project root, not env directories).

    Args:
        env_path: Path to terraform environment directory
    """
    try:
        # Remove all .tfstate files (including backups)
        for tfstate_file in env_path.glob("*.tfstate*"):
            tfstate_file.unlink()

        # Remove all .tfvars files (including backups)
        for tfvars_file in env_path.glob("*.tfvars*"):
            tfvars_file.unlink()

        # Remove .terraform directory
        terraform_dir = env_path / ".terraform"
        if terraform_dir.exists():
            shutil.rmtree(terraform_dir)

        # Remove .terraform.lock.hcl file
        lock_file = env_path / ".terraform.lock.hcl"
        if lock_file.exists():
            lock_file.unlink()

        # Remove auto-generated Flink SQL summary file
        flink_sql_summary = env_path / "FLINK_SQL_COMMANDS.md"
        if flink_sql_summary.exists():
            flink_sql_summary.unlink()

        # Remove legacy mcp_commands.txt file
        mcp_commands = env_path / "mcp_commands.txt"
        if mcp_commands.exists():
            mcp_commands.unlink()

    except Exception:
        # Silently continue if cleanup fails - destroy was successful
        pass


# Confluent can keep listing a Tableflow topic as a user of the provider
# integration after Tableflow is gone, sometimes for good, so deleting it returns
# 409 "integration is being used in some confluent resource". Deleting the
# environment removes the integration regardless, so when it's all that's left
# in airline-demo's state, drop it from state and let core's destroy take it.
TABLEFLOW_PROVIDER_INTEGRATION = "confluent_provider_integration.tableflow[0]"


def release_provider_integration(env_path: Path) -> bool:
    """Drop the provider integration from state if it's the only resource left."""
    listed = subprocess.run(
        ["terraform", "state", "list"], cwd=env_path, capture_output=True, text=True
    )
    if listed.returncode != 0:
        return False
    managed = [r for r in listed.stdout.split() if not r.startswith("data.")]
    if managed != [TABLEFLOW_PROVIDER_INTEGRATION]:
        return False

    removed = subprocess.run(
        ["terraform", "state", "rm", TABLEFLOW_PROVIDER_INTEGRATION], cwd=env_path
    )
    if removed.returncode != 0:
        return False
    print(
        "  ⚠ Confluent still lists the Tableflow provider integration as in use; "
        "core's destroy deletes it with the environment"
    )
    return True


def _cleanup_mcp(root: Path) -> None:
    """Remove MCP server config and registration if MCP was installed."""
    mcp_env = root / "terraform" / "core" / "confluent-mcp.env"
    node_modules = root / "node_modules" / "@confluentinc" / "mcp-confluent"

    if not mcp_env.exists() and not node_modules.exists():
        return

    try:
        if mcp_env.exists():
            mcp_env.unlink()

        subprocess.run(
            ["claude", "mcp", "remove", "confluent-cloud-mcp-server", "-s", "local"],
            capture_output=True,
        )

        # Removes the entire node_modules/ tree — assumes @confluentinc/mcp-confluent
        # is the only npm package installed in this project root.
        node_modules_root = root / "node_modules"
        if node_modules_root.exists():
            shutil.rmtree(node_modules_root)

        package_lock = root / "package-lock.json"
        if package_lock.exists():
            package_lock.unlink()

        print("✓ Removed MCP server config and registration")
    except Exception as e:
        print(f"⚠ MCP cleanup failed: {e}")


def main():
    """Main entry point for destroy."""
    # Parse command-line arguments
    parser = argparse.ArgumentParser(
        description="Destroy the Streamhouse airline demo resources"
    )
    parser.add_argument(
        "--testing",
        action="store_true",
        help="Non-interactive mode: load from credentials.env, skip all prompts (for CI test runs)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force-clean local state files even if terraform destroy fails (use when resources are already gone)",
    )
    args = parser.parse_args()

    print("=== Streamhouse Airline Demo — Destroy ===\n")
    if args.testing:
        print("Running in TESTING mode (non-interactive)\n")

    root = get_project_root()
    print(f"Project root: {root}")

    # TESTING MODE: Load from credentials.env and skip prompts
    if args.testing:
        creds_file = root / "credentials.env"
        if not creds_file.exists():
            print("Error: credentials.env not found.")
            sys.exit(1)

        creds = dotenv_values(str(creds_file))
        # Reverse dependency order: airline-demo before core.
        envs_to_destroy = ["airline-demo", "core"]

        for key, value in creds.items():
            if value:
                os.environ[key] = value

        print("✓ Destroying all resources")
        print(f"  Environments: {', '.join(envs_to_destroy)}")
        print()

    # INTERACTIVE MODE: Original flow
    else:
        # Step 0: Ensure Confluent CLI login
        env_creds = dotenv_values(str(root / "credentials.env"))
        ensure_confluent_login(env_creds)
        print("✓ Confluent CLI logged in")

        # Reverse dependency order: airline-demo before core.
        envs_to_destroy = ["airline-demo", "core"]
        print(f"✓ Will destroy: {', '.join(envs_to_destroy)}")

        # Load credentials file
        creds_file, creds = load_or_create_credentials_file(root)

        # Step 3: Load credentials into environment
        for key, value in creds.items():
            if value:
                os.environ[key] = value

        # Step 4: Show summary and confirm
        print("\n--- Destroy Summary ---")
        print(f"Destroying: {', '.join(envs_to_destroy)}")
        print(
            "\n⚠️  WARNING: This will permanently destroy all airline demo resources (core + airline-demo)!"
        )

        confirm = input("\nAre you sure you want to proceed? (y/n): ").strip().lower()
        if confirm != "y":
            print("Destroy cancelled.")
            sys.exit(0)

    # Step 5: Destroy environments
    print("\n=== Starting Destroy ===")
    for env in envs_to_destroy:
        env_path = root / "terraform" / env
        if not env_path.exists():
            print(f"⊘ Skipping {env}: directory does not exist")
            continue

        # Check if terraform state exists (indicates it was deployed)
        state_file = env_path / "terraform.tfstate"
        if not state_file.exists():
            print(f"⊘ Skipping {env}: no terraform state found (never deployed)")
            continue

        print(f"\n→ Destroying {env}...")
        destroyed = run_terraform_destroy(env_path)
        if not destroyed and env == "airline-demo" and release_provider_integration(env_path):
            destroyed = run_terraform_destroy(env_path)

        if destroyed:
            cleanup_terraform_artifacts(env_path)
        elif args.force:
            print(f"  ⚠ Destroy failed but --force set: cleaning local state for {env}")
            cleanup_terraform_artifacts(env_path)
        else:
            # Destroying core after a failed airline-demo destroy deletes the environment
            # underneath airline-demo's state, so the next deploy fails with 401s.
            print(
                f"\n✗ Destroy failed at {env}. Stopping so later environments keep their state. "
                "Fix the error and rerun, or use --force to clean local state anyway."
            )
            sys.exit(1)

    _cleanup_mcp(root)

    # With nothing deployed, the next deploy can be either kind again.
    creds_file = root / "credentials.env"
    if creds_file.exists() and dotenv_values(str(creds_file)).get(WORKSHOP):
        unset_key(str(creds_file), WORKSHOP)

    print("\n✓ Destroy process completed!")


if __name__ == "__main__":
    main()
