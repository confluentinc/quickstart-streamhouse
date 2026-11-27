terraform {
  required_version = ">= 1.0"
  required_providers {
    confluent = {
      source  = "confluentinc/confluent"
      version = "~> 2.73"
    }
  }
}

# Confluent provider is configured with the Cloud API key/secret from core state,
# so airline-demo never needs its own credentials in tfvars.
provider "confluent" {
  cloud_api_key    = data.terraform_remote_state.core.outputs.confluent_cloud_api_key
  cloud_api_secret = data.terraform_remote_state.core.outputs.confluent_cloud_api_secret
}

# No AWS provider: Tableflow's S3/Glue path lives in terraform/tableflow, so a workshop deploy never downloads it.
