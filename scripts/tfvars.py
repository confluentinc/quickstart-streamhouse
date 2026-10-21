"""
Terraform variables file (terraform.tfvars) management utilities.

Provides functions for:
- Writing terraform.tfvars files with automatic backup
- Generating formatted tfvars content for the core module
"""

import shutil
from pathlib import Path


def get_credential_value(creds: dict[str, str], key: str) -> str | None:
    """
    Get credential value, checking both TF_VAR_ prefixed and non-prefixed keys.

    Args:
        creds: Dictionary of credentials
        key: Key to look up (without TF_VAR_ prefix)

    Returns:
        Value if found, None otherwise
    """
    return creds.get(key) or creds.get(f"TF_VAR_{key}")


def write_tfvars_file(tfvars_path: Path, content: str) -> bool:
    """
    Write terraform.tfvars file with backup of existing file.

    Args:
        tfvars_path: Path to terraform.tfvars file
        content: Content to write

    Returns:
        True if successful, False otherwise
    """
    try:
        # Backup existing file
        if tfvars_path.exists():
            backup_path = tfvars_path.with_suffix(".tfvars.backup")
            shutil.copy2(tfvars_path, backup_path)

        # Ensure parent directory exists
        tfvars_path.parent.mkdir(parents=True, exist_ok=True)

        # Write new content
        with open(tfvars_path, "w") as f:
            f.write(content)

        return True
    except Exception as e:
        print(f"Error writing {tfvars_path}: {e}")
        return False


# Regions whose deployments use a Dedicated Kafka cluster, which the hotel webhook connector
# needs. Every other region gets Standard. TF_VAR_kafka_cluster_type overrides this.
DEDICATED_CLUSTER_REGIONS = frozenset({"us-west-2"})


def kafka_cluster_type(region: str, override: str | None = None) -> str:
    """The Kafka cluster type for a deployment: the override if set, else by region."""
    return override or ("dedicated" if region in DEDICATED_CLUSTER_REGIONS else "standard")


def generate_core_tfvars_content(
    region: str,
    api_key: str,
    api_secret: str,
    resource_prefix: str | None = None,
    deployment_name: str | None = None,
    environment_name: str | None = None,
    cluster_type: str | None = None,
    aws_bedrock_access_key: str | None = None,
    aws_bedrock_secret_key: str | None = None,
    aws_session_token: str | None = None,
    aws_tableflow_access_key: str | None = None,
    aws_tableflow_secret_key: str | None = None,
    aws_tableflow_session_token: str | None = None,
) -> str:
    """
    Generate terraform.tfvars content for the (AWS-only) core module.

    Bedrock and Tableflow AWS credentials are both optional: when omitted, the
    topic/serving/ops stack still deploys and the recovery streaming agent /
    Tableflow S3+Glue analytics path are skipped, respectively.

    Args:
        region: AWS region
        api_key: Confluent Cloud API key
        api_secret: Confluent Cloud API secret
        resource_prefix: Resource name prefix (defaults to the terraform default)
        deployment_name: Fixed base name such as RIVER-AIR-PROD, used instead of the
            prefix plus a random suffix (optional)
        environment_name: Base name for just the environment, cluster, and compute pool
            (optional, defaults to deployment_name)
        cluster_type: Kafka cluster type, standard or dedicated (optional, defaults to standard)
        aws_bedrock_access_key: AWS Bedrock access key (optional)
        aws_bedrock_secret_key: AWS Bedrock secret key (optional)
        aws_session_token: AWS session token for Bedrock (optional, for ASIA* temp creds)
        aws_tableflow_access_key: AWS access key for the Tableflow S3/Glue path (optional)
        aws_tableflow_secret_key: AWS secret key for the Tableflow S3/Glue path (optional)
        aws_tableflow_session_token: AWS session token for Tableflow (optional, for ASIA* temp creds)

    Returns:
        Formatted terraform.tfvars content
    """
    content = f"""# Core Infrastructure Configuration (AWS-only)
cloud_region = "{region}"
confluent_cloud_api_key = "{api_key}"
confluent_cloud_api_secret = "{api_secret}"
"""
    if resource_prefix:
        content += f'resource_prefix = "{resource_prefix}"\n'
    if deployment_name:
        content += f'deployment_name = "{deployment_name}"\n'
    if environment_name:
        content += f'environment_name = "{environment_name}"\n'
    if cluster_type:
        content += f'kafka_cluster_type = "{cluster_type}"\n'

    if aws_bedrock_access_key and aws_bedrock_secret_key:
        content += f'aws_bedrock_access_key = "{aws_bedrock_access_key}"\n'
        content += f'aws_bedrock_secret_key = "{aws_bedrock_secret_key}"\n'
        if aws_session_token:
            content += f'aws_session_token = "{aws_session_token}"\n'

    if aws_tableflow_access_key and aws_tableflow_secret_key:
        content += f'aws_tableflow_access_key = "{aws_tableflow_access_key}"\n'
        content += f'aws_tableflow_secret_key = "{aws_tableflow_secret_key}"\n'
        if aws_tableflow_session_token:
            content += f'aws_tableflow_session_token = "{aws_tableflow_session_token}"\n'

    return content


def write_tfvars_for_deployment(
    root: Path, region: str, creds: dict[str, str], resource_prefix: str = ""
) -> None:
    """
    Write terraform.tfvars for the core module (AWS-only, airline demo).

    airline-demo needs no tfvars of its own — it inherits everything from core via
    terraform_remote_state, and enable_analytics defaults to true (auto-skipped
    when core has no AWS Tableflow credentials).

    Args:
        root: Project root directory
        region: AWS region
        creds: Credentials dict (supports TF_VAR_-prefixed and bare keys)
        resource_prefix: Optional resource name prefix override
    """
    api_key = get_credential_value(creds, "confluent_cloud_api_key")
    api_secret = get_credential_value(creds, "confluent_cloud_api_secret")
    if not (api_key and api_secret):
        return

    core_tfvars_path = root / "terraform" / "core" / "terraform.tfvars"
    content = generate_core_tfvars_content(
        region,
        api_key,
        api_secret,
        resource_prefix=resource_prefix or None,
        deployment_name=get_credential_value(creds, "deployment_name"),
        environment_name=get_credential_value(creds, "environment_name"),
        cluster_type=kafka_cluster_type(region, get_credential_value(creds, "kafka_cluster_type")),
        aws_bedrock_access_key=get_credential_value(creds, "aws_bedrock_access_key"),
        aws_bedrock_secret_key=get_credential_value(creds, "aws_bedrock_secret_key"),
        aws_session_token=get_credential_value(creds, "aws_session_token"),
        aws_tableflow_access_key=get_credential_value(creds, "aws_tableflow_access_key"),
        aws_tableflow_secret_key=get_credential_value(creds, "aws_tableflow_secret_key"),
        aws_tableflow_session_token=get_credential_value(creds, "aws_tableflow_session_token"),
    )
    if write_tfvars_file(core_tfvars_path, content):
        print(f"✓ Wrote {core_tfvars_path}")
