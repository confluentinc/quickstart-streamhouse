output "source_topics" {
  value       = ["flight_status", "passenger_itineraries", "hotel_inventory", "flight_telemetry"]
  description = "Source topics datagen produces into"
}

output "served_topics" {
  value       = ["passenger_state", "flight_impact", "passenger_recommendations"]
  description = "Flink outputs and recovery offers (Lightning + RTCE read these)"
}

output "recovery_agent_enabled" {
  value       = local.agent_enabled || var.workshop
  description = "True when the recovery agent writes the live offers for RA417, RA183, and RA185, whether Terraform runs it or workshop attendees create it; the generator writes all the others"
}

# The webhook the hotel partner (the generator, here) sends availability to. Empty without a
# Dedicated cluster: the generator then publishes hotel rows to Kafka directly.
output "hotel_webhook_url" {
  value = local.webhook_enabled ? "https://${confluent_connector.hotel_webhook[0].id}.${local.core.cloud_region}.aws.public.confluent.cloud/webhook" : ""
}

output "hotel_webhook_api_key" {
  value = local.webhook_enabled ? confluent_api_key.hotel_webhook[0].id : ""
}

output "hotel_webhook_api_secret" {
  value     = local.webhook_enabled ? confluent_api_key.hotel_webhook[0].secret : ""
  sensitive = true
}

output "continuous" {
  value = var.continuous
}
