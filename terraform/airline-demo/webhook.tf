# ===========================================================================
# Hotel partner webhook: a fully managed Webhooks source connector (Dedicated clusters
# only, so deployed only when core created one) that writes each POSTed JSON object to hotel_availability_updates.
# The generator POSTs to it with the key and secret below, standing in for the partner.
# ===========================================================================

# The connector only accepts a Universal (Global) API key owned by the same service account
# that writes to Kafka; Kafka-, Schema Registry- and Cloud-scoped keys are rejected. Global keys
# need a Dedicated or Enterprise cluster, and an Organization Admin to create them.
resource "confluent_api_key" "hotel_webhook" {
  count        = local.webhook_enabled ? 1 : 0
  display_name = upper("${local.name}-hotel-webhook-api-key")
  description  = "Sends hotel availability to the hotel-partner-webhook connector"
  owner {
    id          = local.core.app_manager_service_account_id
    api_version = "iam/v2"
    kind        = "ServiceAccount"
  }

  managed_resource {
    id          = "global"
    api_version = "global/v1"
    kind        = "Global"

    environment {
      id = local.core.confluent_environment_id
    }
  }
}

resource "confluent_connector" "hotel_webhook" {
  count = local.webhook_enabled ? 1 : 0
  environment { id = local.core.confluent_environment_id }
  kafka_cluster { id = local.core.confluent_kafka_cluster_id }

  config_sensitive = {
    "webhook.api.secret" = confluent_api_key.hotel_webhook[0].secret
  }

  config_nonsensitive = {
    "connector.class"              = "WebhookSource"
    "name"                         = "hotel-partner-webhook"
    "kafka.auth.mode"              = "SERVICE_ACCOUNT"
    "kafka.service.account.id"     = local.core.app_manager_service_account_id
    "kafka.topic"                  = "hotel_availability_updates"
    "webhook.topic.routing.mode"   = "NONE"
    "webhook.auth.mode"            = "API_KEY_SECRET"
    "use.existing.webhook.api.key" = "true"
    "webhook.api.key"              = confluent_api_key.hotel_webhook[0].id
    "output.data.format"           = "AVRO"
    "schema.enforcement.mode"      = "EVOLVE"
    "tasks.max"                    = "2" # the connector rejects anything below 2
  }

  depends_on = [confluent_flink_statement.tables]
}
