# ---------------------------------------------------------------------------
# Core infrastructure variables — AWS-only.
#
# This demo is AWS-only by design: Real-Time Context Engine (RTCE) and Flink
# Native Inference / Bedrock are both AWS-only. There is deliberately no Azure
# path here.
# ---------------------------------------------------------------------------

variable "resource_prefix" {
  description = "Prefix applied to every named Confluent resource (env, cluster, compute pool, service accounts). A random suffix is appended so multiple deployments can coexist in one org."
  type        = string
  default     = "flight-recovery"

  validation {
    condition     = can(regex("^[a-z0-9-]{1,32}$", var.resource_prefix))
    error_message = "resource_prefix must be lowercase alphanumeric/hyphen, 1-32 chars."
  }
}

variable "deployment_name" {
  description = "Fixed base name for every named resource, for a recorded demo: RIVER-AIR-PROD gives RIVER-AIR-PROD-ENV, RIVER-AIR-PROD-CLUSTER, and so on. Leave empty (the default) to use resource_prefix plus a random suffix, so workshop deployments can coexist."
  type        = string
  default     = ""

  validation {
    condition     = can(regex("^([A-Za-z0-9-]{1,32})?$", var.deployment_name))
    error_message = "deployment_name must be letters, digits, or hyphens, up to 32 chars."
  }
}

variable "environment_name" {
  description = "Optional base name for just the environment, cluster, and compute pool (RIVER-AIR-PROD gives RIVER-AIR-PROD-ENV). Defaults to deployment_name. Service accounts and API keys keep deployment_name, because their names must be unique across the organization."
  type        = string
  default     = ""

  validation {
    condition     = can(regex("^([A-Za-z0-9-]{1,32})?$", var.environment_name))
    error_message = "environment_name must be letters, digits, or hyphens, up to 32 chars."
  }
}

variable "cloud_region" {
  description = "AWS region for deployment. Must be an RTCE-supported region (default us-east-1)."
  type        = string
  default     = "us-east-1"
}

variable "kafka_cluster_type" {
  description = "Kafka cluster type: standard (default) or dedicated (1 CKU). The hotel webhook connector needs dedicated. deploy picks it from the region; changing it on an existing cluster replaces the cluster."
  type        = string
  default     = "standard"

  validation {
    condition     = contains(["standard", "dedicated"], var.kafka_cluster_type)
    error_message = "kafka_cluster_type must be standard or dedicated."
  }
}

variable "confluent_cloud_api_key" {
  description = "Confluent Cloud API Key"
  type        = string
  sensitive   = true
}

variable "confluent_cloud_api_secret" {
  description = "Confluent Cloud API Secret"
  type        = string
  sensitive   = true
}

# AWS Bedrock credentials — used to create the managed Flink Bedrock connection
# that powers the recovery streaming agent (built in terraform/airline-demo).
variable "aws_bedrock_access_key" {
  description = "AWS Access Key ID for Bedrock"
  type        = string
  sensitive   = true
  default     = ""
}

variable "aws_bedrock_secret_key" {
  description = "AWS Secret Access Key for Bedrock"
  type        = string
  sensitive   = true
  default     = ""
}

variable "aws_session_token" {
  description = "AWS Session Token for temporary credentials (required when the access key starts with ASIA)"
  type        = string
  sensitive   = true
  default     = ""
}

# AWS credentials for the Tableflow S3/Glue provider integration
# (terraform/airline-demo/analytics.tf) — separate from the Bedrock
# creds above since this needs broader IAM/S3/Glue create permissions, not
# just bedrock:InvokeModel. Optional, same pattern as Bedrock: leave empty and
# airline-demo skips enable_analytics instead of failing.
variable "aws_tableflow_access_key" {
  description = "AWS Access Key ID for the Tableflow S3/Glue provider integration (optional)"
  type        = string
  sensitive   = true
  default     = ""
}

variable "aws_tableflow_secret_key" {
  description = "AWS Secret Access Key for the Tableflow S3/Glue provider integration"
  type        = string
  sensitive   = true
  default     = ""
}

variable "aws_tableflow_session_token" {
  description = "AWS Session Token for temporary Tableflow credentials (required when the access key starts with ASIA)"
  type        = string
  sensitive   = true
  default     = ""
}
