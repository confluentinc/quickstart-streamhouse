# ===========================================================================
# Core infrastructure for the Streamhouse airport-disruption demo (AWS-only).
#
# Creates: environment, Kafka cluster, Flink compute pool, the app-manager
# service account + API keys (Kafka / Schema Registry / Flink), the RTCE reader
# service account, and the managed Bedrock connection used by the recovery
# streaming agent (the agent/model itself lives in terraform/airline-demo).
#
# Scenario-specific Flink objects (tables, maintained-state INSERTs, the
# recovery model + agent) are NOT here — they live in terraform/airline-demo so
# the reusable infra stays cleanly separated from the airport schema.
# ===========================================================================

resource "random_id" "resource_suffix" {
  byte_length = 4
}

locals {
  cloud_provider = "AWS"
  region         = var.cloud_region
  prefix         = var.resource_prefix
  suffix         = random_id.resource_suffix.hex
  fixed_name     = var.deployment_name != ""
  name           = local.fixed_name ? upper(var.deployment_name) : "${var.resource_prefix}-${random_id.resource_suffix.hex}"

  # A fixed deployment name is written in capitals, labels included: RIVER-AIR-PROD-ENV.
  # environment_name renames just the environment, cluster, and compute pool: service account
  # names must be unique across the organization, and another deployment may already hold them.
  display_name = {
    for label in [
      "env", "cluster", "app-manager", "compute-pool", "app-manager-kafka-api-key",
      "app-manager-sr-api-key", "app-manager-flink-api-key", "rtce-reader",
      ] : label => (
      var.environment_name != "" && contains(["env", "cluster", "compute-pool"], label)
      ? upper("${var.environment_name}-${label}")
      : local.fixed_name ? upper("${local.name}-${label}") : "${local.name}-${label}"
    )
  }

  dedicated_cluster = var.kafka_cluster_type == "dedicated"

  # Bedrock inference-profile prefix follows the region family (us-*, eu-*, apac-*).
  model_prefix = length(regexall("^us-", var.cloud_region)) > 0 ? "us" : (length(regexall("^eu-", var.cloud_region)) > 0 ? "eu" : "apac")
}

resource "confluent_environment" "staging" {
  display_name = local.display_name["env"]

  stream_governance {
    package = "ADVANCED"
  }
}

data "confluent_schema_registry_cluster" "sr-cluster" {
  environment {
    id = confluent_environment.staging.id
  }

  depends_on = [
    confluent_kafka_cluster.standard
  ]
}

# Standard by default. kafka_cluster_type = "dedicated" (1 CKU) adds the hotel Webhooks source
# connector, which needs a Dedicated cluster; without it the generator publishes hotel rows
# straight to Kafka. The resource keeps its `standard` name so existing deployments don't move.
# Changing the type of an existing cluster replaces it.

resource "confluent_kafka_cluster" "standard" {
  display_name = local.display_name["cluster"]
  availability = "SINGLE_ZONE"
  cloud        = local.cloud_provider
  region       = local.region
  dynamic "standard" {
    for_each = local.dedicated_cluster ? [] : [1]
    content {}
  }
  dynamic "dedicated" {
    for_each = local.dedicated_cluster ? [1] : []
    content {
      cku = 1
    }
  }
  environment {
    id = confluent_environment.staging.id
  }
}

resource "confluent_service_account" "app-manager" {
  display_name = local.display_name["app-manager"]
  description  = "Service account to manage the ${local.name} Kafka cluster and Flink statements"
}

resource "confluent_role_binding" "app-manager-kafka-cluster-admin" {
  principal   = "User:${confluent_service_account.app-manager.id}"
  role_name   = "EnvironmentAdmin"
  crn_pattern = confluent_environment.staging.resource_name
}

resource "confluent_flink_compute_pool" "flinkpool-main" {
  display_name = local.display_name["compute-pool"]
  cloud        = local.cloud_provider
  region       = local.region
  max_cfu      = 20
  environment {
    id = confluent_environment.staging.id
  }
}

resource "confluent_api_key" "app-manager-kafka-api-key" {
  display_name = local.display_name["app-manager-kafka-api-key"]
  description  = "Kafka API Key owned by the app-manager service account"
  owner {
    id          = confluent_service_account.app-manager.id
    api_version = confluent_service_account.app-manager.api_version
    kind        = confluent_service_account.app-manager.kind
  }

  managed_resource {
    id          = confluent_kafka_cluster.standard.id
    api_version = confluent_kafka_cluster.standard.api_version
    kind        = confluent_kafka_cluster.standard.kind

    environment {
      id = confluent_environment.staging.id
    }
  }

  depends_on = [
    confluent_role_binding.app-manager-kafka-cluster-admin
  ]
}

resource "confluent_api_key" "app-manager-schema-registry-api-key" {
  display_name = local.display_name["app-manager-sr-api-key"]
  description  = "Schema Registry API Key owned by the app-manager service account"
  owner {
    id          = confluent_service_account.app-manager.id
    api_version = confluent_service_account.app-manager.api_version
    kind        = confluent_service_account.app-manager.kind
  }

  managed_resource {
    id          = data.confluent_schema_registry_cluster.sr-cluster.id
    api_version = data.confluent_schema_registry_cluster.sr-cluster.api_version
    kind        = data.confluent_schema_registry_cluster.sr-cluster.kind

    environment {
      id = confluent_environment.staging.id
    }
  }
  depends_on = [
    confluent_role_binding.app-manager-kafka-cluster-admin
  ]
}

data "confluent_flink_region" "demo_flink_region" {
  cloud  = local.cloud_provider
  region = local.region
}

resource "confluent_api_key" "app-manager-flink-api-key" {
  display_name = local.display_name["app-manager-flink-api-key"]
  description  = "Flink API Key owned by the app-manager service account"
  owner {
    id          = confluent_service_account.app-manager.id
    api_version = confluent_service_account.app-manager.api_version
    kind        = confluent_service_account.app-manager.kind
  }

  managed_resource {
    id          = data.confluent_flink_region.demo_flink_region.id
    api_version = data.confluent_flink_region.demo_flink_region.api_version
    kind        = data.confluent_flink_region.demo_flink_region.kind

    environment {
      id = confluent_environment.staging.id
    }
  }
}

data "confluent_organization" "main" {}

# ---------------------------------------------------------------------------
# RTCE MCP server — read-only service account + (CLI-created) Global API key.
# setup_rtce.py reads confluent_rtce_service_account_id (below) and creates the
# Global-scoped key the RTCE MCP endpoint requires (the TF provider can only mint
# Cloud/CRM-scoped keys, not Global ones).
# ---------------------------------------------------------------------------

resource "confluent_service_account" "rtce-reader" {
  display_name = local.display_name["rtce-reader"]
  description  = "Service account for the RTCE MCP server (DeveloperRead on topics + Schema Registry subjects)"
}

resource "confluent_role_binding" "rtce-reader-developer-read" {
  principal = "User:${confluent_service_account.rtce-reader.id}"
  role_name = "DeveloperRead"
  # DeveloperRead must target a resource type (topic=*), not the cluster root CRN.
  crn_pattern = "${confluent_kafka_cluster.standard.rbac_crn}/kafka=${confluent_kafka_cluster.standard.id}/topic=*"
}

resource "confluent_role_binding" "rtce-reader-schema-registry-read" {
  principal = "User:${confluent_service_account.rtce-reader.id}"
  role_name = "DeveloperRead"
  # DeveloperRead on subject=* is the Schema Registry read equivalent.
  crn_pattern = "${data.confluent_schema_registry_cluster.sr-cluster.resource_name}/subject=*"
}

# ---------------------------------------------------------------------------
# app-manager ACLs — read/write/create topics + read groups, so datagen and
# Flink can create and populate the airport topics.
# ---------------------------------------------------------------------------

resource "confluent_kafka_acl" "app-manager-read-on-topic" {
  kafka_cluster {
    id = confluent_kafka_cluster.standard.id
  }
  resource_type = "TOPIC"
  resource_name = "*"
  pattern_type  = "LITERAL"
  principal     = "User:${confluent_service_account.app-manager.id}"
  host          = "*"
  operation     = "READ"
  permission    = "ALLOW"
  rest_endpoint = confluent_kafka_cluster.standard.rest_endpoint
  credentials {
    key    = confluent_api_key.app-manager-kafka-api-key.id
    secret = confluent_api_key.app-manager-kafka-api-key.secret
  }
}

resource "confluent_kafka_acl" "app-manager-describe-on-cluster" {
  kafka_cluster {
    id = confluent_kafka_cluster.standard.id
  }
  resource_type = "CLUSTER"
  resource_name = "kafka-cluster"
  pattern_type  = "LITERAL"
  principal     = "User:${confluent_service_account.app-manager.id}"
  host          = "*"
  operation     = "DESCRIBE"
  permission    = "ALLOW"
  rest_endpoint = confluent_kafka_cluster.standard.rest_endpoint
  credentials {
    key    = confluent_api_key.app-manager-kafka-api-key.id
    secret = confluent_api_key.app-manager-kafka-api-key.secret
  }
}

resource "confluent_kafka_acl" "app-manager-write-on-topic" {
  kafka_cluster {
    id = confluent_kafka_cluster.standard.id
  }
  resource_type = "TOPIC"
  resource_name = "*"
  pattern_type  = "LITERAL"
  principal     = "User:${confluent_service_account.app-manager.id}"
  host          = "*"
  operation     = "WRITE"
  permission    = "ALLOW"
  rest_endpoint = confluent_kafka_cluster.standard.rest_endpoint
  credentials {
    key    = confluent_api_key.app-manager-kafka-api-key.id
    secret = confluent_api_key.app-manager-kafka-api-key.secret
  }
}

resource "confluent_kafka_acl" "app-manager-create-topic" {
  kafka_cluster {
    id = confluent_kafka_cluster.standard.id
  }
  resource_type = "TOPIC"
  resource_name = "*"
  pattern_type  = "LITERAL"
  principal     = "User:${confluent_service_account.app-manager.id}"
  host          = "*"
  operation     = "CREATE"
  permission    = "ALLOW"
  rest_endpoint = confluent_kafka_cluster.standard.rest_endpoint
  credentials {
    key    = confluent_api_key.app-manager-kafka-api-key.id
    secret = confluent_api_key.app-manager-kafka-api-key.secret
  }
}

resource "confluent_kafka_acl" "app-manager-read-on-group" {
  kafka_cluster {
    id = confluent_kafka_cluster.standard.id
  }
  resource_type = "GROUP"
  resource_name = "*"
  pattern_type  = "LITERAL"
  principal     = "User:${confluent_service_account.app-manager.id}"
  host          = "*"
  operation     = "READ"
  permission    = "ALLOW"
  rest_endpoint = confluent_kafka_cluster.standard.rest_endpoint
  credentials {
    key    = confluent_api_key.app-manager-kafka-api-key.id
    secret = confluent_api_key.app-manager-kafka-api-key.secret
  }
}

# ---------------------------------------------------------------------------
# Managed Bedrock connection (AWS-only) — powers the recovery streaming agent.
# The connection lives in core because it is reusable infra; the MODEL and AGENT
# that use it are scenario-specific and live in terraform/airline-demo.
# Endpoint uses a Claude Sonnet inference profile keyed off the region family.
# ---------------------------------------------------------------------------

resource "confluent_flink_connection" "bedrock_connection" {
  # Only created when Bedrock credentials are supplied. Without them, the topic /
  # serving / ops stack still deploys fully; the recovery streaming agent (in
  # terraform/airline-demo) is skipped until creds are added and this is re-applied.
  count = var.aws_bedrock_access_key != "" ? 1 : 0

  organization {
    id = data.confluent_organization.main.id
  }
  environment {
    id = confluent_environment.staging.id
  }
  compute_pool {
    id = confluent_flink_compute_pool.flinkpool-main.id
  }
  principal {
    id = confluent_service_account.app-manager.id
  }
  rest_endpoint = data.confluent_flink_region.demo_flink_region.rest_endpoint
  credentials {
    key    = confluent_api_key.app-manager-flink-api-key.id
    secret = confluent_api_key.app-manager-flink-api-key.secret
  }

  display_name      = "bedrock-connection"
  type              = "BEDROCK"
  endpoint          = "https://bedrock-runtime.${var.cloud_region}.amazonaws.com/model/${local.model_prefix}.anthropic.claude-sonnet-4-5-20250929-v1:0/invoke"
  aws_access_key    = var.aws_bedrock_access_key
  aws_secret_key    = var.aws_bedrock_secret_key
  aws_session_token = var.aws_session_token != "" ? var.aws_session_token : null

  depends_on = [
    confluent_api_key.app-manager-flink-api-key,
    confluent_role_binding.app-manager-kafka-cluster-admin
  ]

  lifecycle {
    create_before_destroy = false
  }
}
