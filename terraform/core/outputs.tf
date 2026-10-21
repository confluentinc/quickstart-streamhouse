output "confluent_environment_id" {
  value = confluent_environment.staging.id
}

output "confluent_kafka_cluster_id" {
  value = confluent_kafka_cluster.standard.id
}

output "confluent_kafka_cluster_bootstrap_endpoint" {
  value = confluent_kafka_cluster.standard.bootstrap_endpoint
}

output "confluent_kafka_cluster_rest_endpoint" {
  value = confluent_kafka_cluster.standard.rest_endpoint
}

output "confluent_schema_registry_id" {
  value = data.confluent_schema_registry_cluster.sr-cluster.id
}

output "confluent_schema_registry_rest_endpoint" {
  value = data.confluent_schema_registry_cluster.sr-cluster.rest_endpoint
}

output "confluent_flink_compute_pool_id" {
  value = confluent_flink_compute_pool.flinkpool-main.id
}

output "app_manager_service_account_id" {
  value = confluent_service_account.app-manager.id
}

output "app_manager_kafka_api_key" {
  value     = confluent_api_key.app-manager-kafka-api-key.id
  sensitive = true
}

output "app_manager_kafka_api_secret" {
  value     = confluent_api_key.app-manager-kafka-api-key.secret
  sensitive = true
}

output "app_manager_schema_registry_api_key" {
  value     = confluent_api_key.app-manager-schema-registry-api-key.id
  sensitive = true
}

output "app_manager_schema_registry_api_secret" {
  value     = confluent_api_key.app-manager-schema-registry-api-key.secret
  sensitive = true
}

output "app_manager_flink_api_key" {
  value     = confluent_api_key.app-manager-flink-api-key.id
  sensitive = true
}

output "app_manager_flink_api_secret" {
  value     = confluent_api_key.app-manager-flink-api-key.secret
  sensitive = true
}

output "confluent_organization_id" {
  value = data.confluent_organization.main.id
}

output "confluent_flink_rest_endpoint" {
  value = data.confluent_flink_region.demo_flink_region.rest_endpoint
}

output "confluent_cloud_api_key" {
  value     = var.confluent_cloud_api_key
  sensitive = true
}

output "confluent_cloud_api_secret" {
  value     = var.confluent_cloud_api_secret
  sensitive = true
}

output "cloud_provider" {
  value       = lower(local.cloud_provider)
  description = "The cloud provider used for deployment (always aws for this demo)"
}

output "llm_connection_name" {
  value       = try(confluent_flink_connection.bedrock_connection[0].display_name, "")
  description = "Name of the managed Bedrock connection used by the recovery streaming agent (empty when no Bedrock creds were supplied)"
}

output "bedrock_enabled" {
  value       = length(confluent_flink_connection.bedrock_connection) > 0
  description = "True when a Bedrock connection was created (Bedrock creds present) — gates the recovery streaming agent in airline-demo"
}

output "confluent_environment_display_name" {
  value       = confluent_environment.staging.display_name
  description = "The display name of the Confluent environment (Flink catalog name)"
}

output "confluent_kafka_cluster_display_name" {
  value       = confluent_kafka_cluster.standard.display_name
  description = "The display name of the Confluent Kafka cluster (Flink database name)"
}

output "cloud_region" {
  value       = var.cloud_region
  description = "The AWS region used for deployment"
}

output "resource_prefix" {
  value       = var.resource_prefix
  description = "The resource name prefix used for this deployment"
}

output "aws_tableflow_access_key" {
  value     = var.aws_tableflow_access_key
  sensitive = true
}

output "aws_tableflow_secret_key" {
  value     = var.aws_tableflow_secret_key
  sensitive = true
}

output "aws_tableflow_session_token" {
  value     = var.aws_tableflow_session_token
  sensitive = true
}

output "aws_tableflow_credentials_present" {
  value       = var.aws_tableflow_access_key != ""
  description = "True when AWS credentials for the Tableflow S3/Glue path were supplied — gates enable_analytics in airline-demo, mirroring bedrock_enabled"
  # Not actually sensitive (it's a boolean), but Terraform propagates the
  # sensitive marking from var.aws_tableflow_access_key since the value
  # is derived from it.
  sensitive = true
}

output "resource_name" {
  value       = local.name
  description = "Base name for every named resource: the fixed deployment_name in capitals, or resource_prefix plus the random suffix"
}

output "resource_name_fixed" {
  value       = local.fixed_name
  description = "True when deployment_name set the base name, so resource labels are written in capitals too"
}

output "random_id" {
  value       = random_id.resource_suffix.hex
  description = "Random ID suffix used for resource naming"
}

output "confluent_rtce_service_account_id" {
  value       = confluent_service_account.rtce-reader.id
  description = "Service account ID for the RTCE MCP server — setup_rtce.py uses it to auto-create a Global API key via the Confluent CLI"
}

output "dedicated_cluster" {
  value       = local.dedicated_cluster
  description = "True when the Kafka cluster is Dedicated, which the hotel webhook connector needs"
}
