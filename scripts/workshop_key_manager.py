#!/usr/bin/env python3
"""
Workshop Key Manager — create and manage scoped AWS credentials for workshops.

Lets a workshop organizer mint one AWS IAM user + access key pair, scoped to
exactly what this demo uses that key for:
  1. Bedrock model invocation, for the recovery streaming agent.
  2. Provisioning the Tableflow-to-S3/Glue path (Demo 3): an S3
     bucket, a cross-account IAM role, and its S3 + Glue Data Catalog
     policies (terraform/airline-demo/analytics.tf).

deploy.py writes this same key/secret into both TF_VAR_aws_bedrock_* and
TF_VAR_aws_tableflow_* (both features use one identity in a demo), so the
IAM policy here grants the union of what both paths need. The credentials
go into API-KEYS-AWS.md; paste the access key / secret into credentials.env
before `uv run deploy`.

This repo is AWS-only by design (RTCE + Flink Native Inference are AWS-only).

Usage:
    uv run api-keys create            # Create AWS workshop credentials -> API-KEYS-AWS.md
    uv run api-keys create --verbose
    uv run api-keys destroy           # Revoke them
    uv run api-keys destroy --keep-user   # Revoke keys but keep the IAM user for reuse
"""

import argparse
import json
import logging
import re
import sys
from datetime import datetime
from pathlib import Path

# AWS imports
try:
    import boto3
    from botocore.exceptions import ClientError

    BOTO3_AVAILABLE = True
except ImportError:
    BOTO3_AVAILABLE = False

from dotenv import dotenv_values, set_key

from .deploy import DEFAULT_PREFIX
from .logging_utils import setup_logging
from .terraform import get_project_root
from .ui import prompt_choice, prompt_with_default

# ============================================================================
# CONSTANTS
# ============================================================================

PROJECT_URL = "https://github.com/confluentinc/streamhouse-demo"

# AWS Constants
AWS_IAM_USERNAME = "workshop-demo-user"
AWS_POLICY_NAME = "StreamhouseDemoAccess"
AWS_CREDENTIALS_FILE = "API-KEYS-AWS.md"


# ============================================================================
# EXCEPTIONS
# ============================================================================


class MaxKeysReached(Exception):
    """Raised when an IAM user already has 2 access keys (AWS limit)."""

    pass


# ============================================================================
# COMMON UTILITIES
# ============================================================================


def get_tags(project_root: Path, owner_email: str) -> dict[str, str]:
    """Build resource tags matching Terraform pattern."""
    return {
        "Owner": owner_email,
        "Project": PROJECT_URL,
        "Environment": "workshop",
        "ManagedBy": "workshop-key-manager",
        "LocalPath": str(project_root),
    }


def get_owner_email(project_root: Path) -> str:
    """Get owner email from credentials.env or prompt user, saving the prompted value back."""
    creds_file = project_root / "credentials.env"

    # Try to load from credentials.env
    if creds_file.exists():
        creds = dotenv_values(creds_file)
        if creds.get("TF_VAR_owner_email"):
            return creds["TF_VAR_owner_email"].strip("'\"")

    # Prompt user and save back to credentials.env
    email = prompt_with_default(
        "Owner email for resource tagging (saved to credentials.env)", default=""
    )
    if email:
        set_key(str(creds_file), "TF_VAR_owner_email", email)
    return email


def get_resource_scope(project_root: Path) -> str:
    """The AWS name stem this deployment's resources share, as an IAM pattern.

    A fixed TF_VAR_deployment_name (RIVER-AIR-PROD) gives exact lowercase names such as
    river-air-prod-analytics. Otherwise names are TF_VAR_resource_prefix (or the deploy
    default) plus a random suffix terraform picks at apply, so the suffix is a wildcard.
    """
    creds_file = project_root / "credentials.env"
    creds = dotenv_values(creds_file) if creds_file.exists() else {}
    fixed = (creds.get("TF_VAR_deployment_name") or "").strip("'\"")
    if fixed:
        return fixed.lower()
    prefix = (creds.get("TF_VAR_resource_prefix") or "").strip("'\"") or DEFAULT_PREFIX
    return f"{prefix}-*"


def get_demo_policy(account_id: str, region: str, resource_scope: str) -> dict:
    """IAM policy covering exactly what this demo's one AWS key is used for:

    1. Bedrock model invocation (terraform/core/main.tf bedrock_connection) —
       used directly by Confluent's managed connection, not scoped further.
    2. Provisioning the Tableflow-to-S3/Glue path (Demo 3, optional):
       an S3 bucket, a cross-account IAM role, and its S3 + Glue policies
       (terraform/airline-demo/analytics.tf), scoped by the names they share
       (see get_resource_scope).
    """
    bucket_pattern = f"{resource_scope}-analytics"
    role_pattern = f"{resource_scope}-tableflow-glue"
    policy_pattern = f"{role_pattern}-*"  # -s3-access / -glue-access

    return {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "BedrockInvoke",
                "Effect": "Allow",
                "Action": [
                    "bedrock:InvokeModel",
                    "bedrock:InvokeModelWithResponseStream",
                ],
                "Resource": "*",
            },
            {
                "Sid": "CallerIdentity",
                "Effect": "Allow",
                "Action": "sts:GetCallerIdentity",
                "Resource": "*",
            },
            {
                "Sid": "TableflowBucket",
                "Effect": "Allow",
                "Action": [
                    "s3:CreateBucket",
                    "s3:DeleteBucket",
                    "s3:GetBucketLocation",
                    "s3:ListBucket",
                    "s3:ListBucketMultipartUploads",
                ],
                "Resource": f"arn:aws:s3:::{bucket_pattern}",
            },
            {
                "Sid": "TableflowBucketObjects",
                "Effect": "Allow",
                "Action": [
                    "s3:PutObject",
                    "s3:PutObjectTagging",
                    "s3:GetObject",
                    "s3:DeleteObject",
                    "s3:AbortMultipartUpload",
                    "s3:ListMultipartUploadParts",
                ],
                "Resource": f"arn:aws:s3:::{bucket_pattern}/*",
            },
            {
                "Sid": "TableflowIamRole",
                "Effect": "Allow",
                "Action": [
                    "iam:CreateRole",
                    "iam:GetRole",
                    "iam:DeleteRole",
                    "iam:AttachRolePolicy",
                    "iam:DetachRolePolicy",
                    "iam:ListAttachedRolePolicies",
                    "iam:ListRolePolicies",
                    "iam:TagRole",
                ],
                "Resource": f"arn:aws:iam::{account_id}:role/{role_pattern}",
            },
            {
                "Sid": "TableflowIamPolicy",
                "Effect": "Allow",
                "Action": [
                    "iam:CreatePolicy",
                    "iam:GetPolicy",
                    "iam:DeletePolicy",
                    "iam:GetPolicyVersion",
                    "iam:ListPolicyVersions",
                    "iam:CreatePolicyVersion",
                    "iam:DeletePolicyVersion",
                ],
                "Resource": f"arn:aws:iam::{account_id}:policy/{policy_pattern}",
            },
            {
                "Sid": "TableflowGlueCatalog",
                "Effect": "Allow",
                "Action": [
                    "glue:GetDatabase",
                    "glue:GetDatabases",
                    "glue:CreateDatabase",
                    "glue:UpdateDatabase",
                    "glue:DeleteDatabase",
                    "glue:GetTable",
                    "glue:GetTables",
                    "glue:CreateTable",
                    "glue:UpdateTable",
                    "glue:DeleteTable",
                    "glue:GetPartition",
                    "glue:GetPartitions",
                    "glue:BatchGetPartition",
                    "glue:CreatePartition",
                    "glue:BatchCreatePartition",
                    "glue:UpdatePartition",
                    "glue:DeletePartition",
                    "glue:BatchDeletePartition",
                ],
                "Resource": [
                    f"arn:aws:glue:{region}:{account_id}:catalog",
                    f"arn:aws:glue:{region}:{account_id}:database/*",
                    f"arn:aws:glue:{region}:{account_id}:table/*",
                ],
            },
        ],
    }


def get_aws_region(project_root: Path) -> str:
    """
    Get AWS region for workshop mode.

    AWS workshop mode always uses us-east-1.
    """
    return "us-east-1"


def create_or_get_iam_user(
    iam_client,
    tags: dict[str, str],
    logger: logging.Logger,
    username: str = AWS_IAM_USERNAME,
) -> bool:
    """Create IAM user if it doesn't exist, or get existing user."""
    try:
        # Check if user exists
        iam_client.get_user(UserName=username)
        logger.info(f"IAM user '{username}' already exists")
        return False
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchEntity":
            # User doesn't exist, create it
            logger.info(f"Creating IAM user '{username}'...")
            tag_list = [{"Key": k, "Value": v} for k, v in tags.items()]
            iam_client.create_user(UserName=username, Tags=tag_list)
            logger.info(f"✓ Created IAM user '{username}'")
            return True
        else:
            raise


def attach_demo_policy(
    iam_client,
    logger: logging.Logger,
    account_id: str,
    region: str,
    resource_scope: str,
    username: str = AWS_IAM_USERNAME,
) -> None:
    """Attach the inline demo policy (Bedrock invoke + Tableflow S3/Glue provisioning) to the IAM user."""
    logger.info(f"Attaching policy '{AWS_POLICY_NAME}'...")

    policy_doc = get_demo_policy(account_id, region, resource_scope)

    iam_client.put_user_policy(
        UserName=username,
        PolicyName=AWS_POLICY_NAME,
        PolicyDocument=json.dumps(policy_doc),
    )

    logger.info(f"✓ Attached inline policy '{AWS_POLICY_NAME}'")


def create_access_key(
    iam_client, logger: logging.Logger, username: str = AWS_IAM_USERNAME
) -> tuple[str, str]:
    """Create new access key for IAM user."""
    # Check existing keys
    response = iam_client.list_access_keys(UserName=username)
    existing_keys = response.get("AccessKeyMetadata", [])

    if len(existing_keys) >= 2:
        raise MaxKeysReached(
            f"User '{username}' already has 2 access keys (AWS limit of 2 per user)."
        )

    logger.info("Generating new access key...")

    response = iam_client.create_access_key(UserName=username)
    access_key = response["AccessKey"]

    access_key_id = access_key["AccessKeyId"]
    secret_access_key = access_key["SecretAccessKey"]

    logger.info(f"✓ Created access key: {access_key_id}")

    return access_key_id, secret_access_key


def test_bedrock_credentials(
    access_key_id: str, secret_access_key: str, region: str, logger: logging.Logger
) -> bool:
    """Test AWS Bedrock credentials."""
    from .test_bedrock_credentials import test_bedrock_credentials as test_func

    logger.info("Testing Bedrock access with Claude Sonnet 4.5...")

    # test_func returns (ok, error_type); callers here only need the bool.
    ok, _error_type = test_func(
        access_key_id=access_key_id,
        secret_access_key=secret_access_key,
        region=region,
        logger=logger,
    )
    return ok


def save_aws_credentials_file(
    project_root: Path,
    access_key_id: str,
    secret_access_key: str,
    region: str,
    resource_scope: str,
    tags: dict[str, str],
    logger: logging.Logger,
    username: str = AWS_IAM_USERNAME,
) -> None:
    """Save AWS credentials to markdown file with usage instructions."""
    creds_file = project_root / AWS_CREDENTIALS_FILE

    # Format tags for display (exclude LocalPath as it's not useful for workshop participants)
    tags_display = "\n".join(
        [f"**{key}:** `{value}`" for key, value in tags.items() if key != "LocalPath"]
    )

    content = f"""# Workshop Credentials (AWS)

## AWS Access Keys

Use these credentials when running `uv run deploy`. The same key powers both
the recovery streaming agent (Bedrock) and the Tableflow S3/Glue
analytics path (Demo 3) — both optional, both skipped if you leave this blank.

```
AWS Access Key ID:     {access_key_id}
AWS Secret Access Key: {secret_access_key}
```

## Usage Instructions

### For Workshop Participants

1. Clone the repository:
   ```bash
   git clone https://github.com/confluentinc/quickstart-streamhouse
   cd quickstart-streamhouse
   ```

2. Run deployment:
   ```bash
   uv run deploy
   ```

3. When prompted, enter the credentials above:
   - AWS Access Key: `{access_key_id}`
   - AWS Secret Key: `{secret_access_key}`

## Security Notes

- **Do NOT commit these credentials to Git**
- These keys are scoped to exactly this demo: Bedrock model invocation, plus
  creating/destroying the S3 bucket, IAM role, and Glue Data Catalog entries
  named `{resource_scope}-...` (the Tableflow-to-S3/Glue path)
- Keys should be revoked immediately after the workshop
- Each participant will use the same shared credentials

## After Workshop (For Organizers)

To revoke these credentials, run:

```bash
uv run api-keys destroy
```

Or delete the IAM user directly with AWS CLI:

```bash
# Delete IAM user directly in AWS (must remove all dependencies first)
aws iam delete-access-key --user-name {username} --access-key-id {access_key_id}
aws iam delete-user-policy --user-name {username} --policy-name {AWS_POLICY_NAME}
aws iam delete-user --user-name {username}
```

The api-keys destroy command will:
1. Delete the access key `{access_key_id}`
2. Ask if you want to delete the IAM user (for reuse in future workshops)
3. Clean up state files

---

## Resource Details

**IAM User:** `{username}`
**Region:** `{region}`
**Resource name scope:** `{resource_scope}-...`
**Policy:** `{AWS_POLICY_NAME}` (inline policy)
**Permissions:**
- `bedrock:InvokeModel`, `bedrock:InvokeModelWithResponseStream` (unscoped)
- `sts:GetCallerIdentity` (unscoped)
- S3 create/read/write/delete on bucket `{resource_scope}-analytics`
- IAM create/read/delete on role `{resource_scope}-tableflow-glue` and its policies
- Glue Data Catalog CRUD on this account's catalog/databases/tables

**Tags:**
{tags_display}

**Created:** {datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")} UTC
"""

    with open(creds_file, "w") as f:
        f.write(content)

    logger.info(f"✓ Saved credentials to {creds_file}")

    # Also update credentials.env if it exists. One key covers both Bedrock and
    # Tableflow (see module docstring), matching deploy.py's dual write.
    env_file = project_root / "credentials.env"
    if env_file.exists():
        for name in ("aws_bedrock_access_key", "aws_tableflow_access_key"):
            set_key(str(env_file), f"TF_VAR_{name}", access_key_id)
        for name in ("aws_bedrock_secret_key", "aws_tableflow_secret_key"):
            set_key(str(env_file), f"TF_VAR_{name}", secret_access_key)
        set_key(str(env_file), "TF_VAR_aws_iam_username", username)
        logger.info("✓ Updated credentials.env with new AWS credentials")


def parse_iam_username_from_md(project_root: Path) -> str | None:
    """Read IAM username from API-KEYS-AWS.md Resource Details section.

    Looks for the line:  **IAM User:** `<username>`
    Returns the username string, or None if not found.
    """
    creds_file = project_root / AWS_CREDENTIALS_FILE
    if not creds_file.exists():
        return None
    try:
        content = creds_file.read_text()
        match = re.search(r"^\*\*IAM User:\*\*\s+`([^`]+)`", content, re.MULTILINE)
        if match:
            return match.group(1)
    except OSError:
        pass
    return None


def cleanup_user_dependencies(
    iam_client, username: str, logger: logging.Logger
) -> tuple[bool, str | None]:
    """Comprehensively clean up all IAM user dependencies before deletion."""
    errors = []

    # 1. Remove from all groups
    try:
        response = iam_client.list_groups_for_user(UserName=username)
        groups = response.get("Groups", [])
        if groups:
            for group in groups:
                group_name = group["GroupName"]
                try:
                    iam_client.remove_user_from_group(
                        UserName=username, GroupName=group_name
                    )
                    logger.debug(f"  Removed from group '{group_name}'")
                except ClientError as e:
                    if e.response["Error"]["Code"] != "NoSuchEntity":
                        error_msg = f"Failed to remove from group '{group_name}': {e.response['Error']['Code']}"
                        errors.append(error_msg)
                        logger.error(f"  ✗ {error_msg}")
            if not errors:
                logger.info(f"✓ Removed user from {len(groups)} group(s)")
        else:
            logger.info("✓ No groups to remove")
    except ClientError as e:
        error_msg = f"Failed to list groups: {e.response['Error']['Code']}"
        errors.append(error_msg)
        logger.error(f"✗ {error_msg}")

    # 2. Delete all access keys
    try:
        response = iam_client.list_access_keys(UserName=username)
        keys = response.get("AccessKeyMetadata", [])
        if keys:
            for key in keys:
                key_id = key["AccessKeyId"]
                try:
                    iam_client.delete_access_key(UserName=username, AccessKeyId=key_id)
                    logger.debug(f"  Deleted access key {key_id}")
                except ClientError as e:
                    if e.response["Error"]["Code"] != "NoSuchEntity":
                        error_msg = f"Failed to delete access key {key_id}: {e.response['Error']['Code']}"
                        errors.append(error_msg)
                        logger.error(f"  ✗ {error_msg}")
            if not errors or len(errors) < len(keys):
                logger.info(f"✓ Deleted {len(keys)} access key(s)")
        else:
            logger.info("✓ No access keys to delete")
    except ClientError as e:
        error_msg = f"Failed to list access keys: {e.response['Error']['Code']}"
        errors.append(error_msg)
        logger.error(f"✗ {error_msg}")

    # 3. Detach all managed policies
    try:
        response = iam_client.list_attached_user_policies(UserName=username)
        policies = response.get("AttachedPolicies", [])
        if policies:
            for policy in policies:
                policy_arn = policy["PolicyArn"]
                try:
                    iam_client.detach_user_policy(
                        UserName=username, PolicyArn=policy_arn
                    )
                    logger.debug(f"  Detached policy {policy_arn}")
                except ClientError as e:
                    if e.response["Error"]["Code"] != "NoSuchEntity":
                        error_msg = f"Failed to detach policy {policy_arn}: {e.response['Error']['Code']}"
                        errors.append(error_msg)
                        logger.error(f"  ✗ {error_msg}")
            if not errors or len(errors) < len(policies):
                logger.info(f"✓ Detached {len(policies)} managed policy/policies")
        else:
            logger.info("✓ No managed policies to detach")
    except ClientError as e:
        error_msg = f"Failed to list managed policies: {e.response['Error']['Code']}"
        errors.append(error_msg)
        logger.error(f"✗ {error_msg}")

    # 4. Delete all inline policies
    try:
        response = iam_client.list_user_policies(UserName=username)
        policy_names = response.get("PolicyNames", [])
        if policy_names:
            for policy_name in policy_names:
                try:
                    iam_client.delete_user_policy(
                        UserName=username, PolicyName=policy_name
                    )
                    logger.debug(f"  Deleted inline policy '{policy_name}'")
                except ClientError as e:
                    if e.response["Error"]["Code"] != "NoSuchEntity":
                        error_msg = f"Failed to delete inline policy '{policy_name}': {e.response['Error']['Code']}"
                        errors.append(error_msg)
                        logger.error(f"  ✗ {error_msg}")
            if not errors or len(errors) < len(policy_names):
                logger.info(f"✓ Deleted {len(policy_names)} inline policy/policies")
        else:
            logger.info("✓ No inline policies to delete")
    except ClientError as e:
        error_msg = f"Failed to list inline policies: {e.response['Error']['Code']}"
        errors.append(error_msg)
        logger.error(f"✗ {error_msg}")

    # 5. Delete login profile
    try:
        iam_client.delete_login_profile(UserName=username)
        logger.info("✓ Deleted login profile")
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchEntity":
            logger.info("✓ No login profile to delete")
        else:
            error_msg = f"Failed to delete login profile: {e.response['Error']['Code']}"
            errors.append(error_msg)
            logger.error(f"✗ {error_msg}")

    # 6. Deactivate/delete MFA devices
    try:
        response = iam_client.list_mfa_devices(UserName=username)
        devices = response.get("MFADevices", [])
        if devices:
            for device in devices:
                serial = device["SerialNumber"]
                try:
                    iam_client.deactivate_mfa_device(
                        UserName=username, SerialNumber=serial
                    )
                    logger.debug(f"  Deactivated MFA device {serial}")

                    if ":mfa/" in serial:
                        try:
                            iam_client.delete_virtual_mfa_device(SerialNumber=serial)
                            logger.debug(f"  Deleted virtual MFA device {serial}")
                        except ClientError as e:
                            if e.response["Error"]["Code"] != "NoSuchEntity":
                                logger.warning(
                                    f"  Could not delete virtual MFA device: {e.response['Error']['Code']}"
                                )
                except ClientError as e:
                    if e.response["Error"]["Code"] != "NoSuchEntity":
                        error_msg = f"Failed to deactivate MFA device {serial}: {e.response['Error']['Code']}"
                        errors.append(error_msg)
                        logger.error(f"  ✗ {error_msg}")
            if not errors or len(errors) < len(devices):
                logger.info(f"✓ Deactivated {len(devices)} MFA device(s)")
        else:
            logger.info("✓ No MFA devices to deactivate")
    except ClientError as e:
        error_msg = f"Failed to list MFA devices: {e.response['Error']['Code']}"
        errors.append(error_msg)
        logger.error(f"✗ {error_msg}")

    # 7. Delete SSH public keys
    try:
        response = iam_client.list_ssh_public_keys(UserName=username)
        keys = response.get("SSHPublicKeys", [])
        if keys:
            for key in keys:
                key_id = key["SSHPublicKeyId"]
                try:
                    iam_client.delete_ssh_public_key(
                        UserName=username, SSHPublicKeyId=key_id
                    )
                    logger.debug(f"  Deleted SSH key {key_id}")
                except ClientError as e:
                    if e.response["Error"]["Code"] != "NoSuchEntity":
                        error_msg = f"Failed to delete SSH key {key_id}: {e.response['Error']['Code']}"
                        errors.append(error_msg)
                        logger.error(f"  ✗ {error_msg}")
            if not errors or len(errors) < len(keys):
                logger.info(f"✓ Deleted {len(keys)} SSH key(s)")
        else:
            logger.info("✓ No SSH keys to delete")
    except ClientError as e:
        error_msg = f"Failed to list SSH keys: {e.response['Error']['Code']}"
        errors.append(error_msg)
        logger.error(f"✗ {error_msg}")

    # 8. Delete signing certificates
    try:
        response = iam_client.list_signing_certificates(UserName=username)
        certs = response.get("Certificates", [])
        if certs:
            for cert in certs:
                cert_id = cert["CertificateId"]
                try:
                    iam_client.delete_signing_certificate(
                        UserName=username, CertificateId=cert_id
                    )
                    logger.debug(f"  Deleted signing certificate {cert_id}")
                except ClientError as e:
                    if e.response["Error"]["Code"] != "NoSuchEntity":
                        error_msg = f"Failed to delete certificate {cert_id}: {e.response['Error']['Code']}"
                        errors.append(error_msg)
                        logger.error(f"  ✗ {error_msg}")
            if not errors or len(errors) < len(certs):
                logger.info(f"✓ Deleted {len(certs)} signing certificate(s)")
        else:
            logger.info("✓ No signing certificates to delete")
    except ClientError as e:
        error_msg = (
            f"Failed to list signing certificates: {e.response['Error']['Code']}"
        )
        errors.append(error_msg)
        logger.error(f"✗ {error_msg}")

    # 9. Delete service-specific credentials
    try:
        response = iam_client.list_service_specific_credentials(UserName=username)
        creds = response.get("ServiceSpecificCredentials", [])
        if creds:
            for cred in creds:
                cred_id = cred["ServiceSpecificCredentialId"]
                service_name = cred["ServiceName"]
                try:
                    iam_client.delete_service_specific_credential(
                        UserName=username, ServiceSpecificCredentialId=cred_id
                    )
                    logger.debug(f"  Deleted {service_name} credential {cred_id}")
                except ClientError as e:
                    if e.response["Error"]["Code"] != "NoSuchEntity":
                        error_msg = f"Failed to delete {service_name} credential: {e.response['Error']['Code']}"
                        errors.append(error_msg)
                        logger.error(f"  ✗ {error_msg}")
            if not errors or len(errors) < len(creds):
                logger.info(f"✓ Deleted {len(creds)} service-specific credential(s)")
        else:
            logger.info("✓ No service-specific credentials to delete")
    except ClientError as e:
        error_msg = f"Failed to list service-specific credentials: {e.response['Error']['Code']}"
        errors.append(error_msg)
        logger.error(f"✗ {error_msg}")

    # 10. Delete permission boundary
    try:
        iam_client.delete_user_permissions_boundary(UserName=username)
        logger.info("✓ Deleted permission boundary")
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchEntity":
            logger.info("✓ No permission boundary to delete")
        else:
            error_msg = (
                f"Failed to delete permission boundary: {e.response['Error']['Code']}"
            )
            errors.append(error_msg)
            logger.error(f"✗ {error_msg}")

    # Return results
    if errors:
        error_details = "Encountered the following errors during cleanup:\n\n"
        for i, error in enumerate(errors, 1):
            error_details += f"{i}. {error}\n"
        error_details += f"\nUser '{username}' could not be fully cleaned up."
        return False, error_details
    else:
        return True, None


# ============================================================================
# COMMAND HANDLERS
# ============================================================================


def create_aws_command(args: argparse.Namespace, logger: logging.Logger) -> int:
    """Create AWS IAM user and access keys for workshop."""
    if not BOTO3_AVAILABLE:
        print("\n" + "=" * 70)
        print("ERROR: boto3 is not installed")
        print("=" * 70)
        print("\nboto3 is required for AWS API calls.")
        print("Please install it with:")
        print("\n  pip install boto3")
        print("\nOr add it to your project dependencies.")
        print("=" * 70)
        return 1

    try:
        # Get project root
        project_root = get_project_root()
        logger.debug(f"Project root: {project_root}")

        # Get owner email and region
        owner_email = get_owner_email(project_root)
        region = get_aws_region(project_root)
        resource_scope = get_resource_scope(project_root)

        # Build tags
        tags = get_tags(project_root, owner_email)

        # Prompt for IAM username (allows custom name, defaults to standard)
        iam_username = prompt_with_default(
            "IAM username for workshop user", default=AWS_IAM_USERNAME
        )

        # Create IAM client (uses default AWS credentials from environment/config)
        iam_client = boto3.client("iam")
        account_id = boto3.client("sts").get_caller_identity()["Account"]

        print("\n" + "=" * 70)
        print("CREATING AWS WORKSHOP CREDENTIALS")
        print("=" * 70)

        while True:
            # Create or get IAM user
            create_or_get_iam_user(iam_client, tags, logger, username=iam_username)

            # Attach policy
            attach_demo_policy(
                iam_client,
                logger,
                account_id=account_id,
                region=region,
                resource_scope=resource_scope,
                username=iam_username,
            )

            # Create access key (may raise MaxKeysReached)
            try:
                access_key_id, secret_access_key = create_access_key(
                    iam_client, logger, username=iam_username
                )
                break  # success — exit loop
            except MaxKeysReached as e:
                print(f"\n{e}")
                print("AWS allows a maximum of 2 access keys per IAM user.")
                choice = prompt_choice(
                    "How would you like to proceed?",
                    [
                        "Cancel — I'll delete an existing key manually then retry",
                        "Create a new IAM user with a different name",
                    ],
                )
                if "Cancel" in choice:
                    return 1
                # Prompt for a new username and loop back
                iam_username = prompt_with_default(
                    "New IAM username", default=f"{iam_username}-2"
                )
                print("\n" + "=" * 70)
                print("CREATING AWS WORKSHOP CREDENTIALS (new user)")
                print("=" * 70)

        # Test Bedrock access
        test_success = test_bedrock_credentials(
            access_key_id=access_key_id,
            secret_access_key=secret_access_key,
            region=region,
            logger=logger,
        )

        if not test_success:
            print("\n" + "=" * 70)
            print("⚠ WARNING: Bedrock access test did not complete successfully")
            print("=" * 70)
            print("\nPossible issues:")
            print("  1. AWS credentials haven't propagated yet (wait 30-60 seconds)")
            print("  2. Claude Sonnet 4.5 model not enabled in your AWS account")
            print("  3. Insufficient Bedrock permissions")
            print("\nTo verify manually:")
            print(f"  uv run test-bedrock --access-key {access_key_id} \\")
            print("    --secret-key <SECRET_KEY>")
            print("\nTo enable Claude models:")
            print("  AWS Console → Bedrock → Model Access → Request access")
            print("=" * 70)
            logger.warning("Bedrock test did not pass, but continuing anyway")
        else:
            logger.info(
                "✓ Bedrock access test passed - Claude Sonnet 4.5 is accessible"
            )

        # Save credentials
        save_aws_credentials_file(
            project_root,
            access_key_id,
            secret_access_key,
            region,
            resource_scope,
            tags,
            logger,
            username=iam_username,
        )

        print("=" * 70)
        print("✓ AWS WORKSHOP CREDENTIALS CREATED SUCCESSFULLY")
        print("=" * 70)
        print(f"\nCredentials saved to: {AWS_CREDENTIALS_FILE}")
        print("\nNext steps:")
        print(f"1. Review the credentials in {AWS_CREDENTIALS_FILE}")
        print("2. Share credentials with workshop participants")
        print("3. After workshop, run: uv run api-keys destroy aws")
        print("=" * 70 + "\n")

        return 0

    except ClientError as e:
        logger.error(f"AWS API error: {e}")
        print("\nPlease ensure you have:")
        print("1. Valid AWS credentials configured (aws configure)")
        print("2. IAM permissions to create users and policies")
        return 1
    except Exception as e:
        logger.error(f"Error creating workshop credentials: {e}")
        return 1


def destroy_aws_command(args: argparse.Namespace, logger: logging.Logger) -> int:
    """Destroy AWS workshop credentials and optionally delete IAM user."""
    if not BOTO3_AVAILABLE:
        print("\n" + "=" * 70)
        print("ERROR: boto3 is not installed")
        print("=" * 70)
        print("\nPlease install boto3 to use this command.")
        print("=" * 70)
        return 1

    try:
        # Get project root
        project_root = get_project_root()

        # Load access key and IAM username from credentials.env
        env_file = project_root / "credentials.env"
        env_creds = dotenv_values(str(env_file)) if env_file.exists() else {}
        access_key_id = env_creds.get("TF_VAR_aws_bedrock_access_key")
        iam_username = env_creds.get("TF_VAR_aws_iam_username", AWS_IAM_USERNAME)

        if not access_key_id:
            print("\n" + "=" * 70)
            print("WARNING: No AWS credentials found")
            print("=" * 70)
            print("\nNo AWS Bedrock access key found in credentials.env.")
            print("This usually means no credentials were created with this tool,")
            print("or they were already destroyed.")
            print("\nIf you want to manually delete workshop credentials:")
            print(f"1. AWS Console → IAM → Users → {iam_username}")
            print("2. Delete access keys")
            print("3. Optionally delete the user")
            print("=" * 70 + "\n")
            return 1

        # Create IAM client
        iam_client = boto3.client("iam")

        print("\n" + "=" * 70)
        print("DESTROYING AWS WORKSHOP CREDENTIALS")
        print("=" * 70)
        logger.info(f"Deleting access key {access_key_id}...")

        try:
            iam_client.delete_access_key(
                UserName=iam_username, AccessKeyId=access_key_id
            )
            logger.info(f"✓ Deleted access key {access_key_id}")
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchEntity":
                logger.warning(
                    f"Access key {access_key_id} not found (may already be deleted)"
                )
            else:
                raise

        # Ask about deleting user
        user_deleted = False
        if not args.keep_user:
            print("\nDelete IAM user entirely?")
            print(f"  User: {iam_username}")
            print("  (Saying 'No' allows you to reuse this user for future workshops)")

            delete_user = prompt_choice(
                "Delete IAM user?",
                ["No (keep user for future workshops)", "Yes (delete user completely)"],
            )

            if "Yes" in delete_user:
                # Comprehensive cleanup of all user dependencies
                logger.info("Cleaning up all IAM user dependencies...")
                success, error_details = cleanup_user_dependencies(
                    iam_client, iam_username, logger
                )
                if not success:
                    print("\n" + "=" * 70)
                    print("⚠ WARNING: Could not fully clean up IAM user")
                    print("=" * 70)
                    print(f"\n{error_details}")
                    print("\nTo manually delete the user:")
                    print(f"1. AWS Console → IAM → Users → {iam_username}")
                    print("2. Review and remove remaining dependencies")
                    print("3. Delete the user")
                    print("=" * 70 + "\n")
                    logger.warning(
                        f"User {iam_username} could not be deleted automatically"
                    )
                else:
                    # All dependencies cleaned up, now delete user
                    logger.info(f"Deleting IAM user {iam_username}...")
                    try:
                        iam_client.delete_user(UserName=iam_username)
                        logger.info(f"✓ Deleted IAM user {iam_username}")
                        user_deleted = True
                    except ClientError as e:
                        if e.response["Error"]["Code"] == "NoSuchEntity":
                            logger.warning(
                                f"User {iam_username} not found (may already be deleted)"
                            )
                            user_deleted = True
                        else:
                            print("\n" + "=" * 70)
                            print(
                                "⚠ WARNING: User cleanup succeeded but deletion failed"
                            )
                            print("=" * 70)
                            print(f"\nError: {e}")
                            print(f"User: {iam_username}")
                            print(
                                "\nThe user dependencies were cleaned up, but deletion failed."
                            )
                            print(
                                "Try running the command again, or delete manually via AWS Console."
                            )
                            print("=" * 70 + "\n")
                            logger.error(f"Failed to delete user after cleanup: {e}")
        else:
            logger.info(f"Keeping IAM user {iam_username} (--keep-user flag)")

        # Clear credentials from credentials.env; delete legacy state file if present
        if env_file.exists():
            from dotenv import unset_key

            unset_key(str(env_file), "TF_VAR_aws_bedrock_access_key")
            unset_key(str(env_file), "TF_VAR_aws_bedrock_secret_key")
            unset_key(str(env_file), "TF_VAR_aws_tableflow_access_key")
            unset_key(str(env_file), "TF_VAR_aws_tableflow_secret_key")
            unset_key(str(env_file), "TF_VAR_aws_iam_username")
        legacy_state = project_root / ".workshop-keys-state-aws.json"
        if legacy_state.exists():
            legacy_state.unlink()
            logger.info("✓ Removed legacy .workshop-keys-state-aws.json")
        creds_file = project_root / AWS_CREDENTIALS_FILE
        if creds_file.exists():
            creds_file.unlink()
            logger.info(f"✓ Deleted {AWS_CREDENTIALS_FILE}")

        print("=" * 70)
        print("✓ AWS WORKSHOP CREDENTIALS DESTROYED")
        print("=" * 70)
        print("\nDestroyed:")
        print(f"  - Access key: {access_key_id}")
        if user_deleted:
            print(f"  - IAM user: {iam_username}")
        elif not args.keep_user:
            print(
                f"  - IAM user: {iam_username} (cleanup attempted, may require manual deletion)"
            )
        print("  - Credentials cleared from credentials.env")
        print("=" * 70 + "\n")

        return 0

    except ClientError as e:
        logger.error(f"AWS API error: {e}")
        return 1
    except Exception as e:
        logger.error(f"Error destroying credentials: {e}")
        return 1




# ============================================================================
# ENTRY POINT
# ============================================================================


def main():
    """Create or revoke scoped AWS workshop credentials (AWS-only)."""
    parser = argparse.ArgumentParser(
        description="Create and manage scoped AWS workshop credentials (Bedrock + Tableflow S3/Glue)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s create              # Create AWS credentials -> API-KEYS-AWS.md
  %(prog)s create --verbose
  %(prog)s destroy             # Revoke the access keys
  %(prog)s destroy --keep-user # Revoke keys but keep the IAM user for reuse
        """,
    )

    subparsers = parser.add_subparsers(dest="command", help="Command to execute")

    create_parser = subparsers.add_parser(
        "create", help="Create AWS workshop credentials"
    )
    # `cloud` kept as an optional, AWS-only positional for backward compatibility
    # with `api-keys create aws`; hidden from help since AWS is the only option.
    create_parser.add_argument(
        "cloud", nargs="?", choices=["aws"], default="aws", help=argparse.SUPPRESS
    )
    create_parser.add_argument(
        "--verbose", action="store_true", help="Enable verbose logging"
    )

    destroy_parser = subparsers.add_parser(
        "destroy", help="Revoke AWS workshop credentials"
    )
    destroy_parser.add_argument(
        "cloud", nargs="?", choices=["aws"], default="aws", help=argparse.SUPPRESS
    )
    destroy_parser.add_argument(
        "--keep-user", action="store_true", help="Keep IAM user for reuse"
    )
    destroy_parser.add_argument(
        "--verbose", action="store_true", help="Enable verbose logging"
    )

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return 1

    logger = setup_logging(getattr(args, "verbose", False))

    if args.command == "create":
        return create_aws_command(args, logger)
    if args.command == "destroy":
        return destroy_aws_command(args, logger)

    parser.print_help()
    return 1


if __name__ == "__main__":
    sys.exit(main())
