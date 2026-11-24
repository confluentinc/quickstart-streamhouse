variable "image_uri" {
  type        = string
  description = "The demo image in ECR with its tag, set by deploy"
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
