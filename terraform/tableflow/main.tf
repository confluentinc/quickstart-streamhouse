# ===========================================================================
# tableflow — Demo 3's Tableflow-to-Iceberg path: a customer-owned S3 bucket, an AWS Glue Data Catalog,
# and the cross-account IAM role that wires them to Confluent (analytics.tf).
#
# A separate root from terraform/airline-demo so only it declares the AWS provider: `uv run deploy`
# applies it after airline-demo, and only for a full Flight Disruptions demo with Tableflow AWS
# creds (or always-on). A workshop deploy never initialises it, so attendees never download AWS.
# ===========================================================================

data "terraform_remote_state" "core" {
  backend = "local"
  config = {
    path = "../core/terraform.tfstate"
  }
}

locals {
  core = data.terraform_remote_state.core.outputs

  # Core's base name (RIVER-AIR-PROD, or prefix plus random suffix). try() keeps a core
  # state from before deployment_name existed working.
  name = try(local.core.resource_name, "${local.core.resource_prefix}-${local.core.random_id}")
  display_name = {
    for label in ["tableflow-api-key", "tableflow", "glue"] :
    label => try(local.core.resource_name_fixed, false) ? upper("${local.name}-${label}") : "${local.name}-${label}"
  }

  # Demo 3's S3/Glue path needs both the toggle AND AWS Tableflow creds in core.
  # Leave the creds empty and this cleanly skips instead of failing terraform
  # apply. aws_tableflow_credentials_present is a boolean, not a secret, but
  # Terraform marks it sensitive anyway since it's derived from
  # var.aws_tableflow_access_key — nonsensitive() is needed because
  # count/for_each can't take a sensitive value. try() treats a core state from
  # before this output existed as "no creds", so plan and destroy still work.
  # Deploy applies this root only outside the workshop, when the creds are present.
  analytics_enabled = var.enable_analytics && try(nonsensitive(local.core.aws_tableflow_credentials_present), false)

  # Always-on mode also syncs the offers to Iceberg, so the history includes recovery time.
  tableflow_topics = var.continuous ? distinct(concat(var.tableflow_topics, ["passenger_recommendations"])) : var.tableflow_topics
}

# --- Tableflow API key ------------------------------------------------------
# Enabling Tableflow (analytics.tf) requires a Tableflow-scoped API key owned by
# a principal that can enable Tableflow — app-manager holds EnvironmentAdmin.
# (Our RTCE Global key can't: it belongs to the DeveloperRead-only rtce-reader SA.)
resource "confluent_api_key" "tableflow" {
  count = local.analytics_enabled ? 1 : 0

  display_name = local.display_name["tableflow-api-key"]
  description  = "Tableflow API key owned by the app-manager service account"
  owner {
    id          = local.core.app_manager_service_account_id
    api_version = "iam/v2"
    kind        = "ServiceAccount"
  }

  managed_resource {
    id          = "tableflow"
    api_version = "tableflow/v1"
    kind        = "Tableflow"
  }

  lifecycle {
    precondition {
      condition     = !try(local.core.existing_environment, false)
      error_message = "Tableflow isn't available when deploying into an existing environment."
    }
  }
}
