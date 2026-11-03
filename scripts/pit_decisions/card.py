"""The Agentic Pit Decisions credential card: the F1_* keys the race, Pit Wall, social feeds, and f1-sql read.

`uv run deploy` writes it from terraform/core and terraform/agentic-pit-decisions outputs, so the
lab's tools need no flags. `--creds <path>` or $F1_CREDS points them at another card.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

from dotenv import dotenv_values

from scripts.terraform import get_project_root

CARD = Path("tmp") / "agentic-pit-decisions-card.env"
CARD_ENV_VAR = "F1_CREDS"
DEFAULT_SECONDS_PER_LAP = 20
MIN_SECONDS_PER_LAP = 10


def card_path(root: Path | None = None) -> Path:
    return (root or get_project_root()) / CARD


def rtce_endpoint(region: str, org_id: str, env_id: str, cluster_id: str) -> str:
    if not (region and org_id and env_id and cluster_id):
        return ""
    return (
        f"https://mcp.{region}.aws.confluent.cloud/mcp/v1/context-engine"
        f"/organizations/{org_id}/environments/{env_id}/kafka-clusters/{cluster_id}"
    )


def card_fields(core: dict, rtce_key: str, rtce_secret: str, seconds_per_lap: int) -> dict[str, str]:
    """Flatten core's Terraform outputs into the card's fields (written as F1_<FIELD>)."""
    region = core["cloud_region"]
    org, env, cluster = (
        core["confluent_organization_id"],
        core["confluent_environment_id"],
        core["confluent_kafka_cluster_id"],
    )
    return {
        "prefix": core["resource_name"],
        "region": region,
        "organization_id": org,
        "environment_id": env,
        "cluster_id": cluster,
        "catalog": core["confluent_environment_display_name"],
        "database": core["confluent_kafka_cluster_display_name"],
        "compute_pool_id": core["confluent_flink_compute_pool_id"],
        "flink_rest_endpoint": core["confluent_flink_rest_endpoint"],
        "flink_api_key": core["app_manager_flink_api_key"],
        "flink_api_secret": core["app_manager_flink_api_secret"],
        "kafka_bootstrap": core["confluent_kafka_cluster_bootstrap_endpoint"],
        "kafka_api_key": core["app_manager_kafka_api_key"],
        "kafka_api_secret": core["app_manager_kafka_api_secret"],
        "schema_registry_url": core["confluent_schema_registry_rest_endpoint"],
        "sr_api_key": core["app_manager_schema_registry_api_key"],
        "sr_api_secret": core["app_manager_schema_registry_api_secret"],
        "rtce_mcp_endpoint": rtce_endpoint(region, org, env, cluster),
        "rtce_api_key": rtce_key,
        "rtce_api_secret": rtce_secret,
        "seconds_per_lap": str(seconds_per_lap),
    }


def write_card(root: Path, fields: dict[str, str]) -> Path:
    path = card_path(root)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(f"F1_{key.upper()}={value}\n" for key, value in fields.items()))
    path.chmod(0o600)
    return path


def delete_card(root: Path) -> None:
    card_path(root).unlink(missing_ok=True)


def load_card(explicit: str | None = None, root: Path | None = None) -> tuple[Path, dict[str, str]]:
    """Read the card from --creds, $F1_CREDS, or the one deploy wrote. Exits with a hint when it is missing."""
    path = Path(explicit or os.environ.get(CARD_ENV_VAR) or card_path(root))
    if not path.exists():
        sys.exit(
            f"No Agentic Pit Decisions credential card at {path}.\n"
            "  Run `uv run deploy` and choose the Agentic Pit Decisions lab, or pass --creds <path>."
        )
    return path, {key: value or "" for key, value in dotenv_values(path).items()}


def validate_seconds_per_lap(raw: str | int | None) -> tuple[int | None, str | None]:
    try:
        value = int(str(raw).strip())
    except (TypeError, ValueError):
        return None, f"seconds per lap must be a whole number, got {raw!r}"
    if value < MIN_SECONDS_PER_LAP:
        return None, (
            f"seconds per lap must be at least {MIN_SECONDS_PER_LAP}: below that, anomaly detection "
            "can't fill its 12 windows before the lap-24 anomaly"
        )
    return value, None
