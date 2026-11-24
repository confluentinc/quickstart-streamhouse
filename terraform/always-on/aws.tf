# ---------------------------------------------------------------------------
# Image registry
# ---------------------------------------------------------------------------

resource "aws_ecr_repository" "images" {
  name         = "${local.name}-images"
  force_delete = true
}

# ---------------------------------------------------------------------------
# Flight board: a Lambda function behind a public function URL. The Lambda Web Adapter in the image passes each
# request to airport-app on port 8000. The app's Basic auth (APP_PASSWORD) is the only gate.
# ---------------------------------------------------------------------------

# Created before the function, so Lambda writes to a log group with a retention instead of making its own.
resource "aws_cloudwatch_log_group" "flight_board" {
  name              = "/aws/lambda/${local.limited_names.flight_board_function.name}"
  retention_in_days = 14
}

# The function's role: write its logs and read the settings secret, nothing else.
resource "aws_iam_role" "flight_board" {
  name = local.limited_names.flight_board_role.name
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })

  # Checked here, the first flight board resource, so a long core name fails at plan with a clear message.
  lifecycle {
    precondition {
      condition     = length(local.names_too_long) == 0
      error_message = "These names are longer than AWS allows; shorten core's resource name: ${join(", ", local.names_too_long)}"
    }
  }
}

resource "aws_iam_role_policy_attachment" "flight_board_logs" {
  role       = aws_iam_role.flight_board.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy" "flight_board_settings" {
  name = "read-settings"
  role = aws_iam_role.flight_board.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = "secretsmanager:GetSecretValue"
      Resource = aws_secretsmanager_secret.settings.arn
    }]
  })
}

resource "aws_lambda_function" "flight_board" {
  function_name = local.limited_names.flight_board_function.name
  role          = aws_iam_role.flight_board.arn
  package_type  = "Image"
  image_uri     = var.image_uri
  architectures = ["x86_64"]
  memory_size   = 1024
  timeout       = 120  # App Runner's request timeout: /api/state pages Lightning in turn, 15 s a call
  publish       = true # every image or setting change publishes a new version, and the live alias moves to it

  # Lambda's filesystem is read-only outside /tmp, so run the app directly rather than through `uv run`.
  image_config {
    entry_point = ["/app/.venv/bin/airport-app"]
    command     = ["--host", "0.0.0.0", "--port", "8000"]
  }

  environment {
    variables = {
      STREAMHOUSE_SETTINGS_SECRET_ARN = aws_secretsmanager_secret.settings.arn # the app reads the secret at startup
      AWS_LWA_PORT                    = "8000"
      # TCP, not HTTP: once APP_PASSWORD is set every HTTP path answers 401.
      AWS_LWA_READINESS_CHECK_PROTOCOL = "tcp"
      HOME                             = "/tmp"
      # Lambda can't see a secret change, so a new secret version changes this and publishes a new function version.
      STREAMHOUSE_SETTINGS_VERSION = aws_secretsmanager_secret_version.settings.version_id
    }
  }

  depends_on = [
    aws_cloudwatch_log_group.flight_board,
    aws_iam_role_policy_attachment.flight_board_logs,
    aws_iam_role_policy.flight_board_settings,
  ]
}

resource "aws_lambda_alias" "live" {
  name             = "live"
  function_name    = aws_lambda_function.flight_board.function_name
  function_version = aws_lambda_function.flight_board.version
}

# One instance kept warm. Extra concurrent requests (one open board already makes a few in parallel) use on-demand
# instances, which can take a few seconds to start the first time; raise this to 2-3 if the board feels slow.
resource "aws_lambda_provisioned_concurrency_config" "flight_board" {
  function_name                     = aws_lambda_alias.live.function_name
  qualifier                         = aws_lambda_alias.live.name
  provisioned_concurrent_executions = 1
}

resource "aws_lambda_function_url" "flight_board" {
  function_name      = aws_lambda_alias.live.function_name
  qualifier          = aws_lambda_alias.live.name
  authorization_type = "NONE"
}

# A public function URL needs both statements: InvokeFunctionUrl, and InvokeFunction through the URL.
resource "aws_lambda_permission" "flight_board_url" {
  statement_id           = "public-function-url"
  action                 = "lambda:InvokeFunctionUrl"
  function_name          = aws_lambda_alias.live.function_name
  qualifier              = aws_lambda_alias.live.name
  principal              = "*"
  function_url_auth_type = "NONE"
}

resource "aws_lambda_permission" "flight_board_invoke" {
  statement_id             = "public-function-url-invoke"
  action                   = "lambda:InvokeFunction"
  function_name            = aws_lambda_alias.live.function_name
  qualifier                = aws_lambda_alias.live.name
  principal                = "*"
  invoked_via_function_url = true
}

# ---------------------------------------------------------------------------
# Generator: one Fargate task running airport-datagen --continuous forever.
# ---------------------------------------------------------------------------

resource "aws_ecs_cluster" "demo" {
  name = "${local.name}-cluster"
}

resource "aws_cloudwatch_log_group" "generator" {
  name              = "/ecs/${local.name}-flight-generator"
  retention_in_days = 14
}

# ECS uses this role to pull the image, read the settings secret, and write logs.
resource "aws_iam_role" "generator_execution" {
  name = local.limited_names.generator_execution_role.name
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ecs-tasks.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "generator_execution" {
  role       = aws_iam_role.generator_execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

resource "aws_iam_role_policy" "generator_settings" {
  name = "read-settings"
  role = aws_iam_role.generator_execution.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = "secretsmanager:GetSecretValue"
      Resource = aws_secretsmanager_secret.settings.arn
    }]
  })
}

resource "aws_ecs_task_definition" "generator" {
  family                   = "${local.name}-flight-generator"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = "512"
  memory                   = "1024"
  execution_role_arn       = aws_iam_role.generator_execution.arn

  runtime_platform {
    operating_system_family = "LINUX"
    cpu_architecture        = "X86_64"
  }

  container_definitions = jsonencode([{
    name      = "flight-generator"
    image     = var.image_uri
    essential = true
    command   = ["airport-datagen", "--continuous"]
    secrets   = [for key, ref in local.setting_refs : { name = key, valueFrom = ref }]
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = aws_cloudwatch_log_group.generator.name
        "awslogs-region"        = local.core.cloud_region
        "awslogs-stream-prefix" = "generator"
      }
    }
  }])
}

data "aws_vpc" "default" {
  default = true
}

data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
}

# Outbound only: the generator connects out to Confluent Cloud and listens on nothing.
resource "aws_security_group" "generator" {
  name        = "${local.name}-flight-generator"
  description = "Outbound only, for the flight generator"
  vpc_id      = data.aws_vpc.default.id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_ecs_service" "generator" {
  name            = "${local.name}-flight-generator"
  cluster         = aws_ecs_cluster.demo.id
  task_definition = aws_ecs_task_definition.generator.arn
  desired_count   = 1
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = data.aws_subnets.default.ids
    security_groups  = [aws_security_group.generator.id]
    assign_public_ip = true
  }

  depends_on = [
    aws_iam_role_policy_attachment.generator_execution,
    aws_iam_role_policy.generator_settings,
    aws_secretsmanager_secret_version.settings,
  ]
}
