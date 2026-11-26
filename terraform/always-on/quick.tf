# ===========================================================================
# Amazon Quick over the Tableflow history (scene 3).
#
# terraform/airline-demo syncs flight_impact and passenger_recommendations to
# Iceberg in S3 and the Glue database named after the Kafka cluster. This file
# adds what Quick needs to ask questions of them:
#   Athena workgroup (+ results bucket) <- Quick data source (own IAM role)
#   <- two direct-query datasets <- one Quick Topic (plain-English questions)
# shared with var.quick_principal_arns, the Quick users deploy matched to
# DEMO_USER_EMAILS. With none, or without Tableflow, it creates nothing.
#
# The Quick account subscription is shared by the whole AWS account, so this
# root never declares it: destroying it would delete every Quick user and asset.
# ===========================================================================

data "terraform_remote_state" "tableflow" {
  backend = "local"
  config = {
    path = "../tableflow/terraform.tfstate"
  }
}

data "aws_caller_identity" "current" {}

locals {
  analytics     = data.terraform_remote_state.tableflow.outputs
  quick_enabled = length(var.quick_principal_arns) > 0 && try(local.analytics.analytics_bucket, "") != ""
  account_id    = data.aws_caller_identity.current.account_id
  glue_arns = [
    "arn:aws:glue:${local.core.cloud_region}:${local.account_id}:catalog",
    "arn:aws:glue:${local.core.cloud_region}:${local.account_id}:database/${try(local.analytics.glue_database, "")}",
    "arn:aws:glue:${local.core.cloud_region}:${local.account_id}:table/${try(local.analytics.glue_database, "")}/*",
  ]

  # Owner permissions, as the Quick console grants them.
  data_source_actions = [
    "quicksight:DescribeDataSource", "quicksight:DescribeDataSourcePermissions", "quicksight:PassDataSource",
    "quicksight:UpdateDataSource", "quicksight:DeleteDataSource", "quicksight:UpdateDataSourcePermissions",
  ]
  data_set_actions = [
    "quicksight:DescribeDataSet", "quicksight:DescribeDataSetPermissions", "quicksight:PassDataSet",
    "quicksight:DescribeIngestion", "quicksight:ListIngestions", "quicksight:UpdateDataSet",
    "quicksight:DeleteDataSet", "quicksight:CreateIngestion", "quicksight:CancelIngestion",
    "quicksight:UpdateDataSetPermissions",
  ]
}

# ---------------------------------------------------------------------------
# Athena
# ---------------------------------------------------------------------------

resource "aws_s3_bucket" "athena_results" {
  count = local.quick_enabled ? 1 : 0

  bucket        = local.limited_names.athena_results_bucket.name
  force_destroy = true # query results only, so destroy empties it
}

resource "aws_athena_workgroup" "analytics" {
  count = local.quick_enabled ? 1 : 0

  name          = "${local.name}-analytics"
  force_destroy = true # a workgroup with saved queries or history otherwise fails to delete

  configuration {
    enforce_workgroup_configuration = true
    result_configuration {
      output_location = "s3://${aws_s3_bucket.athena_results[0].bucket}/"
    }
  }
}

# ---------------------------------------------------------------------------
# The role Quick uses for this data source only, so the account's Quick service role is never changed.
# ---------------------------------------------------------------------------

resource "aws_iam_role" "quick_athena" {
  count = local.quick_enabled ? 1 : 0

  name = local.limited_names.quick_athena_role.name
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "quicksight.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "quick_athena" {
  count = local.quick_enabled ? 1 : 0

  name = "query-the-tableflow-history"
  role = aws_iam_role.quick_athena[0].id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "athena:StartQueryExecution", "athena:GetQueryExecution", "athena:GetQueryResults",
          "athena:GetQueryResultsStream", "athena:StopQueryExecution", "athena:GetWorkGroup",
        ]
        Resource = aws_athena_workgroup.analytics[0].arn
      },
      {
        Effect = "Allow"
        Action = [
          "athena:ListWorkGroups", "athena:ListDataCatalogs", "athena:GetDataCatalog", "athena:ListDatabases",
          "athena:GetDatabase", "athena:ListTableMetadata", "athena:GetTableMetadata",
        ]
        Resource = "*"
      },
      {
        Effect = "Allow"
        Action = [
          "glue:GetDatabase", "glue:GetDatabases", "glue:GetTable", "glue:GetTables",
          "glue:GetPartition", "glue:GetPartitions", "glue:BatchGetPartition",
        ]
        Resource = local.glue_arns
      },
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:ListBucket", "s3:GetBucketLocation"]
        Resource = ["arn:aws:s3:::${local.analytics.analytics_bucket}", "arn:aws:s3:::${local.analytics.analytics_bucket}/*"]
      },
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject", "s3:PutObject", "s3:ListBucket", "s3:GetBucketLocation", "s3:AbortMultipartUpload",
          "s3:ListMultipartUploadParts", "s3:ListBucketMultipartUploads",
        ]
        Resource = [aws_s3_bucket.athena_results[0].arn, "${aws_s3_bucket.athena_results[0].arn}/*"]
      },
    ]
  })
}

# Quick tests the connection when it creates the data source, so wait for the new role to propagate
# (the same IAM delay that failed Tableflow on 2026-11-12).
resource "time_sleep" "quick_athena_role" {
  count = local.quick_enabled ? 1 : 0

  create_duration = "30s"
  depends_on      = [aws_iam_role_policy.quick_athena]
}

# ---------------------------------------------------------------------------
# Quick: the Athena data source and the two datasets
# ---------------------------------------------------------------------------

resource "aws_quicksight_data_source" "athena" {
  count = local.quick_enabled ? 1 : 0

  data_source_id = "${local.name}-athena"
  name           = "River Air history (Athena)"
  type           = "ATHENA"

  parameters {
    athena {
      work_group = aws_athena_workgroup.analytics[0].name
      role_arn   = aws_iam_role.quick_athena[0].arn
    }
  }

  dynamic "permission" {
    for_each = var.quick_principal_arns
    content {
      principal = permission.value
      actions   = local.data_source_actions
    }
  }

  depends_on = [time_sleep.quick_athena_role]
}

# Direct query, so every answer reads the latest Iceberg snapshot (no SPICE refreshes to schedule).
resource "aws_quicksight_data_set" "flight_impact" {
  count = local.quick_enabled ? 1 : 0

  data_set_id = "${local.name}-flight-impact"
  name        = "Flight impact"
  import_mode = "DIRECT_QUERY"

  physical_table_map {
    physical_table_map_id = "flight-impact"
    relational_table {
      data_source_arn = aws_quicksight_data_source.athena[0].arn
      catalog         = "AwsDataCatalog"
      schema          = local.analytics.glue_database
      name            = "flight_impact"
      input_columns {
        name = "key"
        type = "STRING"
      }
      input_columns {
        name = "origin"
        type = "STRING"
      }
      input_columns {
        name = "destination"
        type = "STRING"
      }
      input_columns {
        name = "scheduled_departure"
        type = "DATETIME"
      }
      input_columns {
        name = "scheduled_arrival"
        type = "DATETIME"
      }
      input_columns {
        name = "status"
        type = "STRING"
      }
      input_columns {
        name = "delay_minutes"
        type = "INTEGER"
      }
      input_columns {
        name = "affected_passengers"
        type = "INTEGER"
      }
    }
  }

  logical_table_map {
    logical_table_map_id = "flight-impact"
    alias                = "Flight impact"
    source {
      physical_table_id = "flight-impact"
    }
    data_transforms {
      create_columns_operation {
        columns {
          column_id   = "is-delayed"
          column_name = "is_delayed"
          expression  = "ifelse({delay_minutes} >= 15, 'Yes', 'No')" # delayed means 15 minutes or more
        }
      }
    }
  }

  dynamic "permissions" {
    for_each = var.quick_principal_arns
    content {
      principal = permissions.value
      actions   = local.data_set_actions
    }
  }
}

resource "aws_quicksight_data_set" "passenger_recommendations" {
  count = local.quick_enabled ? 1 : 0

  data_set_id = "${local.name}-passenger-recommendations"
  name        = "Passenger recommendations"
  import_mode = "DIRECT_QUERY"

  physical_table_map {
    physical_table_map_id = "passenger-recommendations"
    relational_table {
      data_source_arn = aws_quicksight_data_source.athena[0].arn
      catalog         = "AwsDataCatalog"
      schema          = local.analytics.glue_database
      name            = "passenger_recommendations"
      input_columns {
        name = "key"
        type = "STRING"
      }
      input_columns {
        name = "passenger_id"
        type = "STRING"
      }
      input_columns {
        name = "recommended_flight_id"
        type = "STRING"
      }
      input_columns {
        name = "hotel_name"
        type = "STRING"
      }
      input_columns {
        name = "status"
        type = "STRING"
      }
      input_columns {
        name = "hotel_cost"
        type = "DECIMAL"
      }
      input_columns {
        name = "impacted_at"
        type = "DATETIME"
      }
      input_columns {
        name = "recommended_at"
        type = "DATETIME"
      }
    }
  }

  logical_table_map {
    logical_table_map_id = "passenger-recommendations"
    alias                = "Passenger recommendations"
    source {
      physical_table_id = "passenger-recommendations"
    }
    data_transforms {
      create_columns_operation {
        columns {
          column_id   = "recovery-seconds"
          column_name = "recovery_seconds"
          # Recovery time excludes the customer's decision time: impacted to offer written.
          expression = "dateDiff({impacted_at}, {recommended_at}, 'SS')"
        }
      }
    }
  }

  dynamic "permissions" {
    for_each = var.quick_principal_arns
    content {
      principal = permissions.value
      actions   = local.data_set_actions
    }
  }
}

# ---------------------------------------------------------------------------
# Quick Topic: what lets people ask the history plain-English questions. The AWS provider has no Topic resource,
# so this drives the AWS CLI (with the operator's credentials, which deploy exports). Create deletes any
# leftover topic first, so a rerun after a failed apply doesn't stop on ResourceExistsException.
# ---------------------------------------------------------------------------

locals {
  topic_id = "${local.name}-flight-history"
  topic_actions = [
    "quicksight:DescribeTopic", "quicksight:DescribeTopicRefresh", "quicksight:ListTopicRefreshSchedules",
    "quicksight:DescribeTopicPermissions", "quicksight:DescribeTopicRefreshSchedule", "quicksight:DeleteTopic",
    "quicksight:UpdateTopic", "quicksight:CreateTopicRefreshSchedule", "quicksight:DeleteTopicRefreshSchedule",
    "quicksight:UpdateTopicRefreshSchedule", "quicksight:UpdateTopicPermissions",
  ]

  topic_json = local.quick_enabled ? jsonencode({
    AwsAccountId = local.account_id
    TopicId      = local.topic_id
    CustomInstructions = {
      CustomInstructionsString = join(" ", [
        "A flight is delayed when delay_minutes is 15 minutes or more (is_delayed = 'Yes').",
        "Impacted passengers is the sum of affected_passengers. Hotel cost is the sum of hotel_cost",
        "over passenger recommendations with status BOOKED. Recovery time is the average recovery_seconds",
        "over recommendations with status BOOKED, in seconds. Last month means the last 30 days.",
      ])
    }
    Topic = {
      Name                  = "River Air flight history"
      Description           = "Delays, impacted passengers, hotel cost, and recovery time from the River Air demo"
      UserExperienceVersion = "NEW_READER_EXPERIENCE"
      DataSets = [
        {
          DatasetArn         = aws_quicksight_data_set.flight_impact[0].arn
          DatasetName        = "Flight impact"
          DatasetDescription = "One row per flight: its schedule, delay, and how many passengers it put at risk"
          Columns = [
            { ColumnName = "key", ColumnFriendlyName = "Flight", ColumnDataRole = "DIMENSION", ColumnSynonyms = ["flight", "flight id"] },
            { ColumnName = "origin", ColumnFriendlyName = "Origin", ColumnDataRole = "DIMENSION", ColumnSynonyms = ["from", "departure airport"] },
            { ColumnName = "destination", ColumnFriendlyName = "Destination", ColumnDataRole = "DIMENSION" },
            { ColumnName = "scheduled_departure", ColumnFriendlyName = "Scheduled departure", ColumnDataRole = "DIMENSION", TimeGranularity = "DAY", ColumnSynonyms = ["date", "day", "departure time"] },
            { ColumnName = "scheduled_arrival", ColumnFriendlyName = "Scheduled arrival", ColumnDataRole = "DIMENSION", TimeGranularity = "DAY", ColumnSynonyms = ["arrival time", "landing time"] },
            { ColumnName = "status", ColumnFriendlyName = "Flight status", ColumnDataRole = "DIMENSION" },
            { ColumnName = "delay_minutes", ColumnFriendlyName = "Delay (minutes)", ColumnDataRole = "MEASURE", Aggregation = "AVERAGE", ColumnSynonyms = ["delay", "minutes late"] },
            { ColumnName = "affected_passengers", ColumnFriendlyName = "Impacted passengers", ColumnDataRole = "MEASURE", Aggregation = "SUM", ColumnSynonyms = ["impacted passengers", "passengers at risk", "affected passengers"] },
            { ColumnName = "is_delayed", ColumnFriendlyName = "Delayed", ColumnDataRole = "DIMENSION", ColumnSynonyms = ["delayed", "late"] },
          ]
        },
        {
          DatasetArn         = aws_quicksight_data_set.passenger_recommendations[0].arn
          DatasetName        = "Passenger recommendations"
          DatasetDescription = "One row per recovery offer to an impacted passenger, with its hotel cost and status"
          Columns = [
            { ColumnName = "key", ColumnFriendlyName = "Offer", ColumnDataRole = "DIMENSION" },
            { ColumnName = "passenger_id", ColumnFriendlyName = "Passenger", ColumnDataRole = "DIMENSION", ColumnSynonyms = ["passenger", "customer"] },
            { ColumnName = "recommended_flight_id", ColumnFriendlyName = "Rebooked flight", ColumnDataRole = "DIMENSION" },
            { ColumnName = "hotel_name", ColumnFriendlyName = "Hotel", ColumnDataRole = "DIMENSION" },
            { ColumnName = "status", ColumnFriendlyName = "Offer status", ColumnDataRole = "DIMENSION", ColumnSynonyms = ["booked", "offer status"] },
            { ColumnName = "hotel_cost", ColumnFriendlyName = "Hotel cost", ColumnDataRole = "MEASURE", Aggregation = "SUM", ColumnSynonyms = ["hotel spend", "hotel cost", "cost"] },
            { ColumnName = "impacted_at", ColumnFriendlyName = "Impacted at", ColumnDataRole = "DIMENSION", TimeGranularity = "SECOND" },
            { ColumnName = "recommended_at", ColumnFriendlyName = "Recommended at", ColumnDataRole = "DIMENSION", TimeGranularity = "SECOND", ColumnSynonyms = ["date", "day"] },
            { ColumnName = "recovery_seconds", ColumnFriendlyName = "Recovery time (seconds)", ColumnDataRole = "MEASURE", Aggregation = "AVERAGE", ColumnSynonyms = ["recovery time", "time to recover"] },
          ]
        },
      ]
    }
  }) : ""

  topic_permissions = jsonencode([for arn in var.quick_principal_arns : { Principal = arn, Actions = local.topic_actions }])
}

resource "terraform_data" "quick_topic" {
  count = local.quick_enabled ? 1 : 0

  triggers_replace = [sha256(local.topic_json), local.topic_permissions]
  input = {
    topic_id   = local.topic_id
    account_id = local.account_id
    region     = local.core.cloud_region
    # Depends on both datasets, so destroy deletes the topic before them.
    datasets = [aws_quicksight_data_set.flight_impact[0].arn, aws_quicksight_data_set.passenger_recommendations[0].arn]
  }

  provisioner "local-exec" {
    interpreter = ["/bin/bash", "-c"]
    command     = <<-EOT
      set -euo pipefail
      aws quicksight delete-topic --aws-account-id ${local.account_id} --topic-id ${local.topic_id} --region ${local.core.cloud_region} >/dev/null 2>&1 || true
      aws quicksight create-topic --region ${local.core.cloud_region} --cli-input-json "$TOPIC_JSON" >/dev/null
      aws quicksight update-topic-permissions --aws-account-id ${local.account_id} --topic-id ${local.topic_id} \
        --region ${local.core.cloud_region} --grant-permissions "$TOPIC_PERMISSIONS" >/dev/null
      echo "Created the Quick Topic ${local.topic_id}"
    EOT
    environment = {
      TOPIC_JSON        = local.topic_json
      TOPIC_PERMISSIONS = local.topic_permissions
    }
  }

  # Destroy runs before the datasets go, since this depends on them. A topic already deleted by hand is fine.
  provisioner "local-exec" {
    when    = destroy
    command = "aws quicksight delete-topic --aws-account-id ${self.input.account_id} --topic-id ${self.input.topic_id} --region ${self.input.region} >/dev/null 2>&1 || true"
  }
}

# ---------------------------------------------------------------------------
# The scene 3 queries, saved in the workgroup (Athena console > Saved queries).
# ---------------------------------------------------------------------------

resource "aws_athena_named_query" "history" {
  for_each = local.quick_enabled ? fileset("${path.module}/athena", "*.sql") : toset([])

  name      = trimsuffix(each.value, ".sql")
  workgroup = aws_athena_workgroup.analytics[0].id
  database  = local.analytics.glue_database
  query     = file("${path.module}/athena/${each.value}")
}
