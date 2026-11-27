# ===========================================================================
# Core infrastructure for the Streamhouse airport-disruption demo (AWS-only).
#
# Creates: environment, Kafka cluster, Flink compute pool, the app-manager
# service account + API keys (Kafka / Schema Registry / Flink), the RTCE
# reader service account, and its Global API key.
#
# existing_environment_id deploys into an environment the attendee already has,
# where they may be only EnvironmentAdmin: core then skips the environment, the
# service accounts, their role bindings and ACLs, and the attendee (user_id)
# owns every API key instead.
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
      "app-manager-sr-api-key", "app-manager-flink-api-key", "rtce-reader", "rtce-global-api-key",
      ] : label => (
      var.environment_name != "" && contains(["env", "cluster", "compute-pool"], label)
      ? upper("${var.environment_name}-${label}")
      : local.fixed_name ? upper("${local.name}-${label}") : "${local.name}-${label}"
    )
  }

  dedicated_cluster = var.kafka_cluster_type == "dedicated"

  # Existing environment: look it up instead of creating one; the user owns the keys.
  # one(...[*]) reads a counted resource without indexing it when its count is 0.
  existing         = var.existing_environment_id != ""
  env_id           = local.existing ? one(data.confluent_environment.existing[*].id) : one(confluent_environment.staging[*].id)
  env_display_name = local.existing ? one(data.confluent_environment.existing[*].display_name) : one(confluent_environment.staging[*].display_name)

  user_owner = {
    id          = var.user_id
    api_version = "iam/v2"
    kind        = "User"
  }
  # Owner of the Kafka, Schema Registry, and Flink keys.
  key_owner = local.existing ? local.user_owner : {
    id          = one(confluent_service_account.app-manager[*].id)
    api_version = one(confluent_service_account.app-manager[*].api_version)
    kind        = one(confluent_service_account.app-manager[*].kind)
  }
  # Owner of the RTCE Global key.
  rtce_key_owner = local.existing ? local.user_owner : {
    id          = one(confluent_service_account.rtce-reader[*].id)
    api_version = one(confluent_service_account.rtce-reader[*].api_version)
    kind        = one(confluent_service_account.rtce-reader[*].kind)
  }
}

data "confluent_environment" "existing" {
  count = local.existing ? 1 : 0
  id    = var.existing_environment_id

  # Not a variable validation: one that reads another variable needs Terraform 1.9.
  lifecycle {
    precondition {
      condition     = var.user_id != ""
      error_message = "user_id must be set when existing_environment_id is: the deploying user owns every API key in an existing environment."
    }
  }
}

resource "confluent_environment" "staging" {
  count = local.existing ? 0 : 1

  display_name = local.display_name["env"]

  stream_governance {
    package = "ADVANCED"
  }
}

data "confluent_schema_registry_cluster" "sr-cluster" {
  environment {
    id = local.env_id
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
    id = local.env_id
  }
}

resource "confluent_service_account" "app-manager" {
  count = local.existing ? 0 : 1

  display_name = local.display_name["app-manager"]
  description  = "Service account to manage the ${local.name} Kafka cluster and Flink statements"
}

resource "confluent_role_binding" "app-manager-kafka-cluster-admin" {
  count = local.existing ? 0 : 1

  principal   = "User:${confluent_service_account.app-manager[0].id}"
  role_name   = "EnvironmentAdmin"
  crn_pattern = confluent_environment.staging[0].resource_name
}

resource "confluent_flink_compute_pool" "flinkpool-main" {
  display_name = local.display_name["compute-pool"]
  cloud        = local.cloud_provider
  region       = local.region
  max_cfu      = 20
  environment {
    id = local.env_id
  }
}

resource "confluent_api_key" "app-manager-kafka-api-key" {
  display_name = local.display_name["app-manager-kafka-api-key"]
  description  = "Kafka API Key owned by the deploying user or the app-manager service account"
  owner {
    id          = local.key_owner.id
    api_version = local.key_owner.api_version
    kind        = local.key_owner.kind
  }

  managed_resource {
    id          = confluent_kafka_cluster.standard.id
    api_version = confluent_kafka_cluster.standard.api_version
    kind        = confluent_kafka_cluster.standard.kind

    environment {
      id = local.env_id
    }
  }

  depends_on = [
    confluent_role_binding.app-manager-kafka-cluster-admin
  ]
}

resource "confluent_api_key" "app-manager-schema-registry-api-key" {
  display_name = local.display_name["app-manager-sr-api-key"]
  description  = "Schema Registry API Key owned by the deploying user or the app-manager service account"
  owner {
    id          = local.key_owner.id
    api_version = local.key_owner.api_version
    kind        = local.key_owner.kind
  }

  managed_resource {
    id          = data.confluent_schema_registry_cluster.sr-cluster.id
    api_version = data.confluent_schema_registry_cluster.sr-cluster.api_version
    kind        = data.confluent_schema_registry_cluster.sr-cluster.kind

    environment {
      id = local.env_id
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
  description  = "Flink API Key owned by the deploying user or the app-manager service account"
  owner {
    id          = local.key_owner.id
    api_version = local.key_owner.api_version
    kind        = local.key_owner.kind
  }

  managed_resource {
    id          = data.confluent_flink_region.demo_flink_region.id
    api_version = data.confluent_flink_region.demo_flink_region.api_version
    kind        = data.confluent_flink_region.demo_flink_region.kind

    environment {
      id = local.env_id
    }
  }
}

data "confluent_organization" "main" {}

# ---------------------------------------------------------------------------
# RTCE MCP server — read-only service account + the Global API key the RTCE MCP
# endpoint requires. In an existing environment the deploying user owns the key
# instead. setup_rtce.py reads rtce_global_api_key / rtce_global_api_secret.
# ---------------------------------------------------------------------------

resource "confluent_service_account" "rtce-reader" {
  count = local.existing ? 0 : 1

  display_name = local.display_name["rtce-reader"]
  description  = "Service account for the RTCE MCP server (DeveloperRead on topics + Schema Registry subjects)"
}

resource "confluent_role_binding" "rtce-reader-developer-read" {
  count = local.existing ? 0 : 1

  principal = "User:${confluent_service_account.rtce-reader[0].id}"
  role_name = "DeveloperRead"
  # DeveloperRead must target a resource type (topic=*), not the cluster root CRN.
  crn_pattern = "${confluent_kafka_cluster.standard.rbac_crn}/kafka=${confluent_kafka_cluster.standard.id}/topic=*"
}

resource "confluent_role_binding" "rtce-reader-schema-registry-read" {
  count = local.existing ? 0 : 1

  principal = "User:${confluent_service_account.rtce-reader[0].id}"
  role_name = "DeveloperRead"
  # DeveloperRead on subject=* is the Schema Registry read equivalent.
  crn_pattern = "${data.confluent_schema_registry_cluster.sr-cluster.resource_name}/subject=*"
}

resource "confluent_api_key" "rtce-global" {
  display_name = local.display_name["rtce-global-api-key"]
  description  = "Global API key for the RTCE MCP server, owned by the deploying user or the rtce-reader service account"
  owner {
    id          = local.rtce_key_owner.id
    api_version = local.rtce_key_owner.api_version
    kind        = local.rtce_key_owner.kind
  }

  managed_resource {
    id          = "global"
    api_version = "global/v1"
    kind        = "Global"
  }

  depends_on = [
    confluent_role_binding.rtce-reader-developer-read,
    confluent_role_binding.rtce-reader-schema-registry-read,
  ]
}

# ---------------------------------------------------------------------------
# app-manager ACLs — read/write/create topics + read groups, so datagen and
# Flink can create and populate the airport topics. In an existing environment
# the user's EnvironmentAdmin role covers this, so there are no ACLs.
# ---------------------------------------------------------------------------

resource "confluent_kafka_acl" "app-manager-read-on-topic" {
  count = local.existing ? 0 : 1

  kafka_cluster {
    id = confluent_kafka_cluster.standard.id
  }
  resource_type = "TOPIC"
  resource_name = "*"
  pattern_type  = "LITERAL"
  principal     = "User:${confluent_service_account.app-manager[0].id}"
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
  count = local.existing ? 0 : 1

  kafka_cluster {
    id = confluent_kafka_cluster.standard.id
  }
  resource_type = "CLUSTER"
  resource_name = "kafka-cluster"
  pattern_type  = "LITERAL"
  principal     = "User:${confluent_service_account.app-manager[0].id}"
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
  count = local.existing ? 0 : 1

  kafka_cluster {
    id = confluent_kafka_cluster.standard.id
  }
  resource_type = "TOPIC"
  resource_name = "*"
  pattern_type  = "LITERAL"
  principal     = "User:${confluent_service_account.app-manager[0].id}"
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
  count = local.existing ? 0 : 1

  kafka_cluster {
    id = confluent_kafka_cluster.standard.id
  }
  resource_type = "TOPIC"
  resource_name = "*"
  pattern_type  = "LITERAL"
  principal     = "User:${confluent_service_account.app-manager[0].id}"
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
  count = local.existing ? 0 : 1

  kafka_cluster {
    id = confluent_kafka_cluster.standard.id
  }
  resource_type = "GROUP"
  resource_name = "*"
  pattern_type  = "LITERAL"
  principal     = "User:${confluent_service_account.app-manager[0].id}"
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
# These resources gained a count for existing environments. The moved blocks
# keep current deployments from planning to replace them.
# ---------------------------------------------------------------------------

moved {
  from = confluent_environment.staging
  to   = confluent_environment.staging[0]
}

moved {
  from = confluent_service_account.app-manager
  to   = confluent_service_account.app-manager[0]
}

moved {
  from = confluent_role_binding.app-manager-kafka-cluster-admin
  to   = confluent_role_binding.app-manager-kafka-cluster-admin[0]
}

moved {
  from = confluent_service_account.rtce-reader
  to   = confluent_service_account.rtce-reader[0]
}

moved {
  from = confluent_role_binding.rtce-reader-developer-read
  to   = confluent_role_binding.rtce-reader-developer-read[0]
}

moved {
  from = confluent_role_binding.rtce-reader-schema-registry-read
  to   = confluent_role_binding.rtce-reader-schema-registry-read[0]
}

moved {
  from = confluent_kafka_acl.app-manager-read-on-topic
  to   = confluent_kafka_acl.app-manager-read-on-topic[0]
}

moved {
  from = confluent_kafka_acl.app-manager-describe-on-cluster
  to   = confluent_kafka_acl.app-manager-describe-on-cluster[0]
}

moved {
  from = confluent_kafka_acl.app-manager-write-on-topic
  to   = confluent_kafka_acl.app-manager-write-on-topic[0]
}

moved {
  from = confluent_kafka_acl.app-manager-create-topic
  to   = confluent_kafka_acl.app-manager-create-topic[0]
}

moved {
  from = confluent_kafka_acl.app-manager-read-on-group
  to   = confluent_kafka_acl.app-manager-read-on-group[0]
}
