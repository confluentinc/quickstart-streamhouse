terraform {
  required_version = ">= 1.0"
  required_providers {
    confluent = {
      source  = "confluentinc/confluent"
      version = "~> 2.38"
    }
    aws = {
      source  = "hashicorp/aws"
      version = ">= 5.17.0"
    }
  }
}

# Confluent provider is configured with the Cloud API key/secret from core state,
# so airline-demo never needs its own credentials in tfvars.
provider "confluent" {
  cloud_api_key    = data.terraform_remote_state.core.outputs.confluent_cloud_api_key
  cloud_api_secret = data.terraform_remote_state.core.outputs.confluent_cloud_api_secret
}

# Optional, same pattern as core's Bedrock connection: aws_tableflow_* creds are
# passed explicitly (never a tracked file) rather than relying on an ambient AWS
# CLI session. When they're empty, analytics.tf's resources are all
# count = 0 (see local.analytics_enabled in main.tf) and the skip_*
# flags below stop the provider from ever calling AWS (no STS
# GetCallerIdentity at init). skip_credentials_validation alone isn't enough —
# the AWS provider still falls through to the ambient SSO/IMDS credential
# chain if access_key/secret_key are empty, which fails loudly when that
# chain is ALSO empty/expired (as it should be allowed to be, since this
# whole path is meant to be optional). Obviously-fake placeholder credentials
# avoid that fallthrough entirely: they're never validated or used, since
# skip_credentials_validation is set and every AWS resource is count = 0.
provider "aws" {
  region = data.terraform_remote_state.core.outputs.cloud_region
  access_key = (
    local.analytics_enabled
    ? data.terraform_remote_state.core.outputs.aws_tableflow_access_key
    : "unused-analytics-disabled"
  )
  secret_key = (
    local.analytics_enabled
    ? data.terraform_remote_state.core.outputs.aws_tableflow_secret_key
    : "unused-analytics-disabled"
  )
  token = (
    local.analytics_enabled && data.terraform_remote_state.core.outputs.aws_tableflow_session_token != ""
    ? data.terraform_remote_state.core.outputs.aws_tableflow_session_token
    : null
  )

  skip_credentials_validation = !local.analytics_enabled
  skip_requesting_account_id  = !local.analytics_enabled
  skip_region_validation      = !local.analytics_enabled
}
