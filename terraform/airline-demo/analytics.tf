# Demo 3: Tableflow syncs flight_impact (and passenger_recommendations, enabled
# on screen) to Iceberg in customer-owned S3 with an AWS Glue Data Catalog, so
# Amazon Athena/Quick can answer historical questions over them: which delays
# put the most connections at risk (flight_impact), and how long recovery took
# and what hotels cost (passenger_recommendations). S3 + Glue needs a real
# cross-account IAM trust relationship, which this file wires up.
#
# Bootstrap order (breaks the circular dependency between the IAM role's trust
# policy and the provider integration's computed iam_role_arn/external_id —
# see https://docs.confluent.io/cloud/current/connectors/provider-integration/index.html):
#   1. Compute the role's ARN from its name before it exists (aws_iam_role.tableflow).
#   2. Create confluent_provider_integration with that not-yet-existing ARN.
#   3. Create the real AWS IAM role, whose trust policy references the
#      provider integration's now-known iam_role_arn/external_id.

data "aws_caller_identity" "current" {
  count = local.analytics_enabled ? 1 : 0
}

locals {
  # S3 bucket names must be lowercase, so a fixed RIVER-AIR-PROD becomes river-air-prod-analytics.
  analytics_bucket_name = lower("${local.name}-analytics")
  tableflow_role_name   = lower("${local.name}-tableflow-glue")
  tableflow_role_arn = (
    local.analytics_enabled
    ? "arn:aws:iam::${data.aws_caller_identity.current[0].account_id}:role/${local.tableflow_role_name}"
    : ""
  )
  # Default Glue database name is the Kafka cluster ID (Confluent's convention).
  glue_database = local.core.confluent_kafka_cluster_id
}

resource "aws_s3_bucket" "analytics" {
  count  = local.analytics_enabled ? 1 : 0
  bucket = local.analytics_bucket_name
  # This stack creates the bucket for this deployment, and Tableflow fills it
  # with Iceberg files, so destroy empties it rather than failing on BucketNotEmpty.
  force_destroy = true
}

resource "confluent_provider_integration" "tableflow" {
  count = local.analytics_enabled ? 1 : 0

  display_name = local.display_name["tableflow"]
  environment {
    id = local.core.confluent_environment_id
  }
  aws {
    customer_role_arn = local.tableflow_role_arn
  }
}

resource "aws_iam_role" "tableflow" {
  count = local.analytics_enabled ? 1 : 0

  name        = local.tableflow_role_name
  description = "Cross-account role Confluent Tableflow assumes to write Iceberg data/metadata for the River Air analytics topics"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect    = "Allow"
        Principal = { AWS = confluent_provider_integration.tableflow[0].aws[0].iam_role_arn }
        Action    = "sts:AssumeRole"
        Condition = {
          StringEquals = { "sts:ExternalId" = confluent_provider_integration.tableflow[0].aws[0].external_id }
        }
      },
      {
        Effect    = "Allow"
        Principal = { AWS = confluent_provider_integration.tableflow[0].aws[0].iam_role_arn }
        Action    = "sts:TagSession"
      }
    ]
  })
}

# S3 permissions Tableflow needs to write Iceberg data + metadata.
# https://docs.confluent.io/cloud/current/topics/tableflow/get-started/quick-start-custom-storage-glue.html
resource "aws_iam_policy" "tableflow_s3" {
  count = local.analytics_enabled ? 1 : 0

  name        = "${local.tableflow_role_name}-s3-access"
  description = "S3 access Tableflow needs to write Iceberg data/metadata for the River Air analytics topics"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:GetBucketLocation", "s3:ListBucketMultipartUploads", "s3:ListBucket"]
        Resource = aws_s3_bucket.analytics[0].arn
      },
      {
        Effect   = "Allow"
        Action   = ["s3:PutObject", "s3:PutObjectTagging", "s3:GetObject", "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts"]
        Resource = "${aws_s3_bucket.analytics[0].arn}/*"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "tableflow_s3" {
  count = local.analytics_enabled ? 1 : 0

  role       = aws_iam_role.tableflow[0].name
  policy_arn = aws_iam_policy.tableflow_s3[0].arn
}

# Best-effort reconstruction of the standard AWS Glue Data Catalog CRUD actions
# Confluent's console normally generates for you at Environment > Tableflow >
# Catalog Integration > AWS Glue > "Configure AWS Glue access" (a region/account-
# scoped policy, not published as static example HCL by Confluent). Scoped to
# just this account/region/database, so a mismatch fails safe (AccessDenied on
# sync) rather than over-granting. If Glue sync errors after apply, regenerate
# the exact policy from that console flow and reconcile here.
resource "aws_iam_policy" "tableflow_glue" {
  count = local.analytics_enabled ? 1 : 0

  name        = "${local.tableflow_role_name}-glue-access"
  description = "AWS Glue Data Catalog access for the River Air Tableflow-to-Glue sync"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "glue:GetDatabase", "glue:GetDatabases", "glue:CreateDatabase", "glue:UpdateDatabase", "glue:DeleteDatabase",
          "glue:GetTable", "glue:GetTables", "glue:CreateTable", "glue:UpdateTable", "glue:DeleteTable",
          "glue:GetPartition", "glue:GetPartitions", "glue:BatchGetPartition",
          "glue:CreatePartition", "glue:BatchCreatePartition", "glue:UpdatePartition",
          "glue:DeletePartition", "glue:BatchDeletePartition",
        ]
        Resource = [
          "arn:aws:glue:${local.core.cloud_region}:${data.aws_caller_identity.current[0].account_id}:catalog",
          "arn:aws:glue:${local.core.cloud_region}:${data.aws_caller_identity.current[0].account_id}:database/${local.glue_database}",
          "arn:aws:glue:${local.core.cloud_region}:${data.aws_caller_identity.current[0].account_id}:table/${local.glue_database}/*",
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "tableflow_glue" {
  count = local.analytics_enabled ? 1 : 0

  role       = aws_iam_role.tableflow[0].name
  policy_arn = aws_iam_policy.tableflow_glue[0].arn
}

resource "confluent_tableflow_topic" "analytics" {
  for_each = local.analytics_enabled ? toset(var.tableflow_topics) : toset([])

  environment {
    id = local.core.confluent_environment_id
  }
  kafka_cluster {
    id = local.core.confluent_kafka_cluster_id
  }
  display_name  = each.value
  table_formats = ["ICEBERG"]
  byob_aws {
    bucket_name             = aws_s3_bucket.analytics[0].bucket
    provider_integration_id = confluent_provider_integration.tableflow[0].id
  }
  credentials {
    key    = confluent_api_key.tableflow[0].id
    secret = confluent_api_key.tableflow[0].secret
  }

  depends_on = [
    confluent_flink_statement.tables,
    confluent_flink_statement.passenger_state,
    confluent_flink_statement.flight_impact,
    aws_iam_role_policy_attachment.tableflow_s3,
  ]
}

resource "confluent_catalog_integration" "glue" {
  count = local.analytics_enabled ? 1 : 0

  environment {
    id = local.core.confluent_environment_id
  }
  kafka_cluster {
    id = local.core.confluent_kafka_cluster_id
  }
  display_name = local.display_name["glue"]
  aws_glue {
    provider_integration_id = confluent_provider_integration.tableflow[0].id
  }
  credentials {
    key    = confluent_api_key.tableflow[0].id
    secret = confluent_api_key.tableflow[0].secret
  }

  depends_on = [
    confluent_tableflow_topic.analytics,
    aws_iam_role_policy_attachment.tableflow_glue,
  ]
}
