# airline-demo takes no credentials of its own — everything is inherited from core
# via terraform_remote_state. The only knobs are whether to build the Demo 3
# Tableflow-to-S3/Glue analytics path (analytics.tf), which additionally requires
# core to have AWS Tableflow credentials, which topics it syncs, and whether the
# recovery agent runs.

variable "enable_analytics" {
  description = "Enable Tableflow with customer-owned S3 storage + an AWS Glue Data Catalog integration, for the Demo 3 Athena/Amazon Quick analytics scene. This toggle alone isn't enough — it also needs core's aws_tableflow_access_key/secret_key set (optional, like aws_bedrock_access_key; auto-skipped when empty, see local.analytics_enabled). This creates a real S3 bucket and a cross-account IAM role with S3/IAM/Glue permissions."
  type        = bool
  default     = true
}

variable "tableflow_topics" {
  description = "Topics to expose as Iceberg tables in the customer-owned S3 bucket, synced to AWS Glue. passenger_recommendations is enabled on screen during Demo 3; add it here to pre-enable it."
  type        = list(string)
  default     = ["flight_impact"]
}

variable "workshop" {
  description = "Deploy the self-service workshop's starting point (docs/workshop.md): the three source feeds and passenger_recommendations only. Attendees create passenger_state, flight_impact, and the recovery agent in the SQL workspace, and the workshop leaves out Tableflow and the hotel webhook. `uv run deploy --workshop` sets it."
  type        = bool
  default     = false
}

variable "enable_recovery_agent" {
  description = "Run the recovery agent (sql/26-31), which writes RA417's live offers in place of the generator. Needs core's Bedrock connection and the 'rtce-connection' MCP connection; `uv run deploy` creates the connection after enabling RTCE, then sets this with a targeted apply."
  type        = bool
  default     = false
}
