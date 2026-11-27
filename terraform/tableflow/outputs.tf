output "tableflow_topics" {
  value       = local.analytics_enabled ? local.tableflow_topics : []
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
