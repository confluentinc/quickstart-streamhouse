# ===========================================================================
# agentic-pit-decisions — the F1 Agentic Pit Decisions lab.
#
# Runs in core's environment, cluster, and Flink pool beside the Flight
# Disruptions lab (airline-demo). It creates only the lab's own objects, and
# none of their names overlap with airline-demo's:
#   car_telemetry, race_standings   live feeds `uv run f1-race` produces to
#   driver_race_history             past results, seeded by `uv run deploy`
#   llm_textgen_model               Claude hosted in Confluent, for the Step 3 agent
#
# The lab's own objects (car_state, pit_strategy_agent, pit_decisions) are built
# by the person running the lab from docs/agentic-pit-decisions/sql.
# RTCE is turned on per topic in the Console, as a lab step.
# ===========================================================================

data "terraform_remote_state" "core" {
  backend = "local"
  config = {
    path = "../core/terraform.tfstate"
  }
}

data "confluent_organization" "main" {}

data "confluent_flink_region" "lab" {
  cloud  = "AWS"
  region = data.terraform_remote_state.core.outputs.cloud_region
}

locals {
  core = data.terraform_remote_state.core.outputs

  objects = {
    car_telemetry       = "sql/01-source-car-telemetry.sql"
    race_standings      = "sql/02-source-race-standings.sql"
    driver_race_history = "sql/03-source-driver-race-history.sql"
    llm_textgen_model   = "sql/04-model-llm-textgen.sql"
  }
}

# No interdependencies, created in parallel. Each body is IF NOT EXISTS, so a
# re-apply after a partial failure is a no-op instead of "already exists".
resource "confluent_flink_statement" "objects" {
  for_each = local.objects

  organization { id = data.confluent_organization.main.id }
  environment { id = local.core.confluent_environment_id }
  compute_pool { id = local.core.confluent_flink_compute_pool_id }
  principal { id = local.core.app_manager_service_account_id }
  rest_endpoint = data.confluent_flink_region.lab.rest_endpoint
  credentials {
    key    = local.core.app_manager_flink_api_key
    secret = local.core.app_manager_flink_api_secret
  }

  statement_name = "pit-decisions-create-${replace(each.key, "_", "-")}"
  statement      = file("${path.module}/${each.value}")
  properties = {
    "sql.current-catalog"  = local.core.confluent_environment_display_name
    "sql.current-database" = local.core.confluent_kafka_cluster_display_name
  }
  lifecycle { ignore_changes = [properties] }
}
