output "source_topics" {
  value       = ["flight_status", "passenger_itineraries", "hotel_inventory"]
  description = "Source topics datagen produces into"
}

output "served_topics" {
  value       = ["passenger_state", "flight_impact", "passenger_recommendations"]
  description = "Flink outputs and recovery offers (Lightning + RTCE read these)"
}

output "recovery_agent_enabled" {
  value       = local.agent_enabled || var.workshop
  description = "True when the recovery agent writes RA417's live offers, whether Terraform runs it or workshop attendees create it; the generator writes all the others"
}

output "tableflow_topics" {
  value       = local.analytics_enabled ? var.tableflow_topics : []
  description = "Topics exposed as Iceberg tables in the customer-owned S3 bucket via Tableflow, synced to AWS Glue (Demo 3)"
}

output "analytics_bucket" {
  value       = local.analytics_enabled ? aws_s3_bucket.analytics[0].bucket : ""
  description = "S3 bucket holding the Tableflow topics' Iceberg data/metadata (Demo 3)"
}

output "tableflow_role_name" {
  value       = local.analytics_enabled ? aws_iam_role.tableflow[0].name : ""
  description = "AWS IAM role Confluent Tableflow assumes to write to the analytics bucket. If Glue sync errors with AccessDenied, regenerate the Glue permission policy at Confluent Cloud Console > Environment > Tableflow > Catalog Integration > AWS Glue > Configure AWS Glue access, and reconcile it with aws_iam_policy.tableflow_glue."
}

output "glue_database" {
  value       = local.analytics_enabled ? local.glue_database : ""
  description = "AWS Glue database name the Iceberg tables sync into (defaults to the Kafka cluster ID)"
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
