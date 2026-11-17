variable "image_uri" {
  type        = string
  description = "The demo image in ECR with its tag, set by deploy"
}

variable "quick_principal_arns" {
  type        = list(string)
  default     = []
  description = "Quick user ARNs to share the Athena data source, datasets, and Topic with; deploy matches DEMO_USER_EMAILS. Empty skips Quick."
}

variable "rtce_api_key" {
  type        = string
  sensitive   = true
  description = "Global API key the flight board uses for Lightning Tables (CONFLUENT_RTCE_API_KEY)"
}

variable "rtce_api_secret" {
  type        = string
  sensitive   = true
  description = "Secret for rtce_api_key (CONFLUENT_RTCE_API_SECRET)"
}
