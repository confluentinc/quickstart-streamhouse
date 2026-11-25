#!/usr/bin/env python3
"""
Destroy the Streamhouse labs from Confluent Cloud.
Uses credentials from credentials.env for destruction via Terraform.

    uv run destroy                             # both labs and the shared core environment
    uv run destroy --lab agentic-pit-decisions # one lab's objects; core and the other lab stay

An always-on deployment (terraform/always-on) goes first, since its services read core's cluster.
"""

import argparse
import os
import shutil
import subprocess
import sys
from pathlib import Path

from dotenv import dotenv_values, unset_key

from scripts import existing_env, processes

from .credentials import load_or_create_credentials_file
from .deploy import (
    ALWAYS_ON,
    ALWAYS_ON_CARD,
    CONTINUOUS,
    DEPLOY_LABS,
    FLIGHT_DISRUPTIONS,
    LABS,
    WORKSHOP,
    _export_aws_credentials,
)
from .login_checks import ensure_confluent_login
from .setup_rtce import unregister_rtce_server
from .terraform import get_project_root, run_terraform_output
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

    Does NOT remove credentials.env (which is in project root, not env directories);
    main() asks about that separately.

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
# 409 "integration is being used in some confluent resource". The catalog tags
# 409 the same way while a topic still carries them. Deleting the environment
# removes both regardless, so when only these are left in airline-demo's state,
# drop them from state and let core's destroy take them.
TABLEFLOW_PROVIDER_INTEGRATION = "confluent_provider_integration.tableflow[0]"
CATALOG_TAG_PREFIX = "confluent_tag.demo["


def release_provider_integration(env_path: Path) -> bool:
    """Drop the provider integration and catalog tags from state if nothing else is left."""
    listed = subprocess.run(
        [processes.exe("terraform"), "state", "list"],
        cwd=env_path,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    if listed.returncode != 0:
        return False
    managed = [r for r in listed.stdout.split() if not r.startswith("data.")]
    if not managed or not all(r == TABLEFLOW_PROVIDER_INTEGRATION or r.startswith(CATALOG_TAG_PREFIX) for r in managed):
        return False

    removed = subprocess.run([processes.exe("terraform"), "state", "rm", *managed], cwd=env_path)
    if removed.returncode != 0:
        return False
    print(
        "  ⚠ Confluent still lists the Tableflow provider integration or catalog tags as in use; "
        "core's destroy deletes them with the environment"
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
            [processes.exe("claude"), "mcp", "remove", "confluent-cloud-mcp-server", "-s", "local"],
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


def _envs_to_destroy(lab: str | None, root: Path | None = None) -> list[str]:
    """Terraform roots in reverse dependency order: always-on (when deployed), every lab, then core."""
    always_on = ["always-on"] if root and (root / "terraform" / "always-on" / "terraform.tfstate").exists() else []
    if lab:
        return [*(always_on if lab == FLIGHT_DISRUPTIONS else []), LABS[lab][1]]
    return [*always_on, "agentic-pit-decisions", "airline-demo", "core"]


def _prepare_always_on_destroy(creds: dict) -> None:
    """terraform/always-on's variables need values even to destroy, and its AWS provider needs credentials."""
    _export_aws_credentials(creds)
    os.environ.setdefault("TF_VAR_image_uri", "destroy")
    os.environ.setdefault("TF_VAR_rtce_api_key", creds.get("CONFLUENT_RTCE_API_KEY") or "destroy")
    os.environ.setdefault("TF_VAR_rtce_api_secret", creds.get("CONFLUENT_RTCE_API_SECRET") or "destroy")


def _cleanup_pit_decisions(root: Path) -> None:
    """Delete the Agentic Pit Decisions credential card and seed marker, which point at a gone environment."""
    from .pit_decisions.card import delete_card
    from .pit_decisions.seed import SEED_MARKER

    delete_card(root)
    (root / SEED_MARKER).unlink(missing_ok=True)


def _kept_environment(root: Path) -> str:
    """The environment ID when core was deployed into an existing environment (destroy keeps it), else ""."""
    state = root / "terraform" / "core" / "terraform.tfstate"
    if not state.exists():
        return ""
    try:
        outputs = run_terraform_output(state)
    except Exception:
        return ""
    return (outputs.get("confluent_environment_id") or "") if outputs.get("existing_environment") else ""


def _user_for_core(creds: dict) -> str:
    """Core's variables require the user's ID in an existing environment, even to destroy.

    Sets TF_VAR_user_id and returns the ID; returns "" when no existing environment is saved or the lookup fails.
    """
    env_id = (creds.get(existing_env.ENV_KEY) or "").strip()
    if not env_id:
        return ""
    user_id = existing_env.current_user_id(env_id)
    if user_id:
        os.environ["TF_VAR_user_id"] = user_id
    return user_id


def _ensure_user_for_core(creds: dict, root: Path, envs_to_destroy: list[str]) -> None:
    """Exit before any Terraform runs when core's destroy would fail for want of the user's ID."""
    env_id = (creds.get(existing_env.ENV_KEY) or "").strip()
    if not env_id or _user_for_core(creds):
        return
    if "core" in envs_to_destroy and (root / "terraform" / "core" / "terraform.tfstate").exists():
        sys.exit(
            f"Couldn't find your Confluent user ID for {env_id}; run `confluent login` "
            "(as the user with EnvironmentAdmin on it) and retry."
        )
    print("⚠ Couldn't find your Confluent user ID; a later destroy of core needs it (`confluent login`).")


def main():
    """Main entry point for destroy."""
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="Destroy the Streamhouse lab resources")
    parser.add_argument(
        "--lab",
        choices=tuple(LABS),
        help="Destroy only this lab's Terraform root, keeping core and the other lab",
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

    print("=== Streamhouse Labs — Destroy ===\n")
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
        envs_to_destroy = _envs_to_destroy(args.lab, root)

        for key, value in creds.items():
            if value:
                os.environ[key] = value
        if creds.get(existing_env.ENV_KEY):
            ensure_confluent_login(creds)  # the user's ID comes from the CLI
        _ensure_user_for_core(creds, root, envs_to_destroy)

        print("✓ Destroying all resources")
        print(f"  Environments: {', '.join(envs_to_destroy)}")
        print()

    # INTERACTIVE MODE: Original flow
    else:
        # Step 0: Ensure Confluent CLI login
        env_creds = dotenv_values(str(root / "credentials.env"))
        ensure_confluent_login(env_creds, root / "credentials.env", interactive=True)
        print("✓ Confluent CLI logged in")

        envs_to_destroy = _envs_to_destroy(args.lab, root)
        print(f"✓ Will destroy: {', '.join(envs_to_destroy)}")

        # Load credentials file
        creds_file, creds = load_or_create_credentials_file(root)

        # Step 3: Load credentials into environment
        for key, value in creds.items():
            if value:
                os.environ[key] = value
        _ensure_user_for_core(creds, root, envs_to_destroy)

        # Step 4: Show summary and confirm
        print("\n--- Destroy Summary ---")
        print(f"Destroying: {', '.join(envs_to_destroy)}")
        kept = _kept_environment(root) if "core" in envs_to_destroy else ""
        if kept:
            print(f"\nYour environment {kept} is kept; the cluster, compute pool and keys are deleted.")
        else:
            print(f"\n⚠️  WARNING: This will permanently destroy {', '.join(envs_to_destroy)}!")

        confirm = input("\nAre you sure you want to proceed? (y/n): ").strip().lower()
        if confirm != "y":
            print("Destroy cancelled.")
            sys.exit(0)

    if "always-on" in envs_to_destroy:
        _prepare_always_on_destroy(creds)

    if "airline-demo" in envs_to_destroy:
        from .airport_datagen import stop_previous_stream

        stop_previous_stream(root)  # the live stream would keep writing to topics destroy is about to delete

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
        if destroyed and env == "agentic-pit-decisions":
            _cleanup_pit_decisions(root)
        if destroyed and env == "always-on":
            (root / ALWAYS_ON_CARD).unlink(missing_ok=True)
        if not destroyed and env == "airline-demo" and release_provider_integration(env_path):
            destroyed = run_terraform_destroy(env_path)

        if destroyed:
            cleanup_terraform_artifacts(env_path)
        elif args.force:
            print(f"  ⚠ Destroy failed but --force set: cleaning local state for {env}")
            cleanup_terraform_artifacts(env_path)
        else:
            # Destroying core after a failed lab destroy deletes the environment
            # underneath the lab's state, so the next deploy fails with 401s.
            print(
                f"\n✗ Destroy failed at {env}. Stopping so later environments keep their state. "
                "Fix the error and rerun, or use --force to clean local state anyway."
            )
            sys.exit(1)

    if args.lab:
        print("\n✓ Destroy process completed! Core and any other lab are still deployed.")
        return

    _cleanup_mcp(root)
    unregister_rtce_server()
    print("✓ Removed the RTCE MCP server registration")
    _cleanup_pit_decisions(root)

    # With nothing deployed, the next deploy can be either kind again and asks which lab.
    creds_file = root / "credentials.env"
    saved = dotenv_values(str(creds_file)) if creds_file.exists() else {}
    # Core's destroy deleted the RTCE Global key, so the saved copy points at nothing.
    for key in (WORKSHOP, DEPLOY_LABS, ALWAYS_ON, CONTINUOUS, "CONFLUENT_RTCE_API_KEY", "CONFLUENT_RTCE_API_SECRET"):
        if saved.get(key):
            unset_key(str(creds_file), key)

    print("\n✓ Destroy process completed!")

    # Asked last so a failed destroy never costs the user the keys they need to rerun it.
    if not args.testing and creds_file.exists():
        answer = input("\nAlso delete saved credentials in credentials.env? (y/N): ").strip().lower()
        if answer == "y":
            creds_file.unlink()
            print("✓ Deleted credentials.env")
        else:
            print("Kept credentials.env")


if __name__ == "__main__":
    main()
