"""Deploying into an attendee's existing Confluent environment: choosing it and refusing what can't work."""

import json
import re
import subprocess
import sys
from pathlib import Path

from scripts import processes

ENV_KEY = "TF_VAR_existing_environment_id"
ENV_ID = re.compile(r"^env-[a-z0-9]+$")

_PROMPT = "Deploy into an existing environment? Paste its ID (env-xxxxx), or press Enter to create a new one: "
_HINT = "That doesn't look like an environment ID. It is `env-` followed by letters and digits."
_REFUSAL = (
    "Full demos can't deploy into an existing environment: Flink statements that run as a user "
    "stop after 4 hours, and the full demo needs them running around the clock. "
    "Deploy the workshop there, or deploy the full demo into a new environment."
)


def full_demo(*, with_labs: bool, video_2: bool, always_on: bool, flights: bool, workshop: bool) -> bool:
    return with_labs or video_2 or always_on or (flights and not workshop)


def _clean(value: str | None) -> str:
    return (value or "").strip().strip("\"'").strip()


def _valid_or_exit(env_id: str, source: str) -> str:
    if not ENV_ID.match(env_id):
        sys.exit(f"{source} '{env_id}' is not an environment ID. Expected `env-` followed by letters and digits.")
    return env_id


def _ask() -> str:
    print()  # a blank line before the question, like deploy's other prompts
    while True:
        try:
            answer = _clean(input(_PROMPT))
        except EOFError:
            return ""
        if not answer or ENV_ID.match(answer):
            return answer
        print(_HINT)


def choose(flag: str | None, creds_file: Path, creds: dict, *, interactive: bool, full: bool, save: bool = True) -> str:
    """Return the environment ID to deploy into, or "" to create a new environment.

    With save=False the caller saves the ID itself (with `save`) once its checks pass.
    """
    saved = _clean(creds.get(ENV_KEY))
    if _clean(flag):
        env_id = _valid_or_exit(_clean(flag), "--environment")
    elif saved:
        env_id = _valid_or_exit(saved, f"{ENV_KEY} in {creds_file.name}")
    elif interactive and not full:
        env_id = _ask()
    else:
        env_id = ""
    if env_id and full:
        if env_id == saved and not _clean(flag):
            sys.exit(
                f"{_REFUSAL} The environment comes from {ENV_KEY} in {creds_file.name}; "
                "delete that line (after `uv run destroy`) to go back to a new environment."
            )
        sys.exit(_REFUSAL)
    if save:
        _save_id(creds_file, creds, env_id)
    return env_id


def save(creds_file: Path, creds: dict, env_id: str) -> None:
    """Save the environment ID to credentials.env and creds; an unchanged saved ID isn't rewritten."""
    _save_id(creds_file, creds, env_id)


def _save_id(creds_file: Path, creds: dict, env_id: str) -> None:  # choose's `save` keyword hides `save`
    if env_id and env_id != _clean(creds.get(ENV_KEY)):
        from scripts import setup_rtce

        setup_rtce._save_to_env_file(creds_file, ENV_KEY, env_id)
        creds[ENV_KEY] = env_id


def check_flow_switch(env_id: str, core_outputs: dict) -> None:
    """Exit when core's state came from the other flow, or from another environment."""
    if not core_outputs:
        return
    deployed_existing = bool(core_outputs.get("existing_environment"))
    deployed_id = core_outputs.get("confluent_environment_id") or ""
    if deployed_existing != bool(env_id) or (env_id and deployed_id != env_id):
        where = f"environment {deployed_id}" if deployed_existing else "a new environment"
        sys.exit(f"This checkout is already deployed into {where}. Run `uv run destroy` first to switch environments.")


def _cli_json(*args: str) -> object:
    """Run a confluent command with JSON output; return the parsed JSON, or None on any failure."""
    try:
        result = subprocess.run(
            [processes.exe("confluent"), *args, "-o", "json"],
            capture_output=True,
            text=True,
            timeout=60,
            encoding="utf-8",
            errors="replace",
        )
        if result.returncode != 0:
            return None
        start = min((i for i in (result.stdout.find("["), result.stdout.find("{")) if i >= 0), default=0)
        return json.loads(result.stdout[start:])  # the CLI can print a notice before the JSON
    except (OSError, subprocess.SubprocessError, ValueError):
        return None


def current_user_id(env_id: str) -> str:
    """The signed-in user's `u-...` ID, or "". Needs --environment: an EnvironmentAdmin's bindings are scoped to it."""
    rows = _cli_json("iam", "rbac", "role-binding", "list", "--current-user", "--environment", env_id)
    for row in rows if isinstance(rows, list) else []:
        principal = row.get("principal") if isinstance(row, dict) else None
        if isinstance(principal, str) and principal.startswith("User:"):
            return principal.removeprefix("User:")
    return ""


def preflight(env_id: str) -> str:
    """The signed-in user's ID, which Terraform needs. An empty answer means the login can't see env_id."""
    user_id = current_user_id(env_id)
    if not user_id:
        sys.exit(
            f"Can't find {env_id} with your login. Check the ID, and that you have EnvironmentAdmin on it, "
            "or run `confluent login` again."
        )
    return user_id
