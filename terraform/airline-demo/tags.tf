# ===========================================================================
# Stream Catalog tags on the demo's topics (shown in the Topics list in the UI).
#
# Flink creates each topic, so a tag binding waits for the statement that owns it.
# Tag definitions are created once; topic -> tags is the `topic_tags` map below.
# ===========================================================================

locals {
  # The provider has no color attribute; deploy applies these via the Catalog API.
  tag_definitions = {
    DATA_PRODUCT = {
      description = "Curated, consumer-ready data owned by River Air"
      color       = "GREEN_LIGHT"
    }
    PII = {
      description = "Contains passenger personal information (synthetic in this demo)"
      color       = "MAGENTA_LIGHT"
    }
    RAW_DATA = {
      description = "Unprocessed data as received from the source"
      color       = "NEUTRAL_LIGHT"
    }
    PARTNER_DATA = {
      description = "Supplied by an external partner"
      color       = "BLUE_LIGHT"
    }
  }

  topic_tags = {
    flight_status              = ["DATA_PRODUCT"]
    passenger_itineraries      = ["PII", "RAW_DATA"]
    hotel_inventory            = ["RAW_DATA", "PARTNER_DATA"]
    hotel_availability_updates = ["RAW_DATA", "PARTNER_DATA"]
    passenger_state            = ["PII", "DATA_PRODUCT"]
    flight_impact              = ["DATA_PRODUCT"]
    passenger_recommendations  = ["PII", "DATA_PRODUCT"]
  }

  # These two topics only exist when the recovery agent is enabled.
  agent_topic_tags = {
    passenger_state_changes = ["PII"]
    impacted_passengers     = ["PII"]
  }


  # The workshop's apply creates only local.tables; attendees create the other topics later.
  tagged_topics = {
    for topic, tags in merge(local.topic_tags, local.agent_enabled ? local.agent_topic_tags : {}) :
    topic => tags if !var.workshop || contains(keys(local.tables), topic)
  }

  topic_tag_bindings = {
    for pair in flatten([
      for topic, tags in local.tagged_topics : [for tag in tags : { topic = topic, tag = tag }]
    ]) : "${pair.topic}/${pair.tag}" => pair
  }
}

resource "confluent_tag" "demo" {
  for_each = local.tag_definitions

  schema_registry_cluster { id = local.core.confluent_schema_registry_id }
  rest_endpoint = local.core.confluent_schema_registry_rest_endpoint
  credentials {
    key    = local.core.app_manager_schema_registry_api_key
    secret = local.core.app_manager_schema_registry_api_secret
  }

  name        = each.key
  description = each.value.description
}

output "topic_tag_colors" {
  value       = { for name, tag in confluent_tag.demo : tag.name => local.tag_definitions[name].color }
  description = "Tag colors applied through the Catalog API after Terraform deploys"
}

resource "confluent_tag_binding" "topic" {
  for_each = local.topic_tag_bindings

  schema_registry_cluster { id = local.core.confluent_schema_registry_id }
  rest_endpoint = local.core.confluent_schema_registry_rest_endpoint
  credentials {
    key    = local.core.app_manager_schema_registry_api_key
    secret = local.core.app_manager_schema_registry_api_secret
  }

  tag_name    = confluent_tag.demo[each.value.tag].name
  entity_name = "${local.core.confluent_schema_registry_id}:${local.core.confluent_kafka_cluster_id}:${each.value.topic}"
  entity_type = "kafka_topic"

  depends_on = [confluent_flink_statement.tables, confluent_flink_statement.passenger_state, confluent_flink_statement.flight_impact, confluent_flink_statement.passenger_state_changes, confluent_flink_statement.impacted_passengers]
}
