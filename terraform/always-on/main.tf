# ===========================================================================
# always-on — the River Air demo running continuously on AWS.
#
# One container image (the repo's Dockerfile) runs twice:
#   App Runner service  -> the flight board (airport-app, port 8000)
#   ECS Fargate service -> the generator   (airport-datagen --continuous)
# Both read their settings from one Secrets Manager secret built from core's
# outputs. The demo users are invited by hand and get their roles from the CLI,
# so this root creates no Confluent resources.
# ===========================================================================

data "terraform_remote_state" "core" {
  backend = "local"
  config = {
    path = "../core/terraform.tfstate"
  }
}

locals {
  core = data.terraform_remote_state.core.outputs
  name = lower(local.core.resource_name)

  # Names AWS caps at a length, each with its limit. The auto-scaling configuration's cap is 32,
  # so it gets the short "-board" suffix: core's default streamhouse-<8 hex> plus "-flight-board" is 33.
  limited_names = {
    flight_board_service       = { name = "${local.name}-flight-board", limit = 40 }
    flight_board_scaling       = { name = "${local.name}-board", limit = 32 }
    flight_board_access_role   = { name = "${local.name}-flight-board-access", limit = 64 }
    flight_board_instance_role = { name = "${local.name}-flight-board-instance", limit = 64 }
    generator_execution_role   = { name = "${local.name}-flight-generator-execution", limit = 64 }
  }
  names_too_long = [
    for item in values(local.limited_names) : "${item.name} (${length(item.name)} > ${item.limit})"
    if length(item.name) > item.limit
  ]

  # Every value the app and the generator read from their environment.
  settings = {
    # Terraform reports the bootstrap endpoint as SASL_SSL://host:port; the clients want host:port.
    STREAMHOUSE_BOOTSTRAP_SERVERS          = trimprefix(local.core.confluent_kafka_cluster_bootstrap_endpoint, "SASL_SSL://")
    STREAMHOUSE_KAFKA_API_KEY              = local.core.app_manager_kafka_api_key
    STREAMHOUSE_KAFKA_API_SECRET           = local.core.app_manager_kafka_api_secret
    STREAMHOUSE_SCHEMA_REGISTRY_URL        = local.core.confluent_schema_registry_rest_endpoint
    STREAMHOUSE_SCHEMA_REGISTRY_API_KEY    = local.core.app_manager_schema_registry_api_key
    STREAMHOUSE_SCHEMA_REGISTRY_API_SECRET = local.core.app_manager_schema_registry_api_secret
    STREAMHOUSE_ENVIRONMENT_ID             = local.core.confluent_environment_id
    STREAMHOUSE_CLUSTER_ID                 = local.core.confluent_kafka_cluster_id
    STREAMHOUSE_REGION                     = local.core.cloud_region
    CONFLUENT_RTCE_API_KEY                 = var.rtce_api_key
    CONFLUENT_RTCE_API_SECRET              = var.rtce_api_secret
    APP_PASSWORD                           = random_password.app.result
  }

  # NAME -> "<secret arn>:NAME::", the form App Runner and ECS use to read one JSON key.
  setting_refs = {
    for key in keys(local.settings) : key => "${aws_secretsmanager_secret.settings.arn}:${key}::"
  }
}

resource "random_password" "app" {
  length  = 24
  special = false
}

resource "aws_secretsmanager_secret" "settings" {
  name                    = "${local.name}-settings"
  description             = "Confluent Cloud settings and the flight board password for the always-on demo"
  recovery_window_in_days = 0
}

resource "aws_secretsmanager_secret_version" "settings" {
  secret_id     = aws_secretsmanager_secret.settings.id
  secret_string = jsonencode(local.settings)
}
