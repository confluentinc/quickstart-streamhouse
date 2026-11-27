# Tableflow takes no credentials of its own: everything comes from core via terraform_remote_state,
# and the S3/Glue path also needs core's AWS Tableflow credentials.

variable "enable_analytics" {
  description = "Enable Tableflow with customer-owned S3 storage + an AWS Glue Data Catalog integration, for the Demo 3 Athena/Amazon Quick analytics scene. This toggle alone isn't enough — it also needs core's aws_tableflow_access_key/secret_key set (optional; auto-skipped when empty, see local.analytics_enabled). This creates a real S3 bucket and a cross-account IAM role with S3/IAM/Glue permissions."
  type        = bool
  default     = true
}

variable "tableflow_topics" {
  description = "Topics to expose as Iceberg tables in the customer-owned S3 bucket, synced to AWS Glue. passenger_recommendations is enabled on screen during Demo 3; add it here to pre-enable it."
  type        = list(string)
  default     = ["flight_impact"]
}

variable "continuous" {
  description = "Always-on mode: also sync passenger_recommendations, so the history includes recovery time. Deploy sets TF_VAR_continuous for both airline-demo and this root."
  type        = bool
  default     = false
}
