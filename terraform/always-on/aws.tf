# ---------------------------------------------------------------------------
# Image registry
# ---------------------------------------------------------------------------

resource "aws_ecr_repository" "images" {
  name         = "${local.name}-images"
  force_delete = true
}

# ---------------------------------------------------------------------------
# Flight board: App Runner runs the image's default command (airport-app on port 8000).
# ---------------------------------------------------------------------------

# Lets App Runner pull the image from ECR.
resource "aws_iam_role" "flight_board_access" {
  name = local.limited_names.flight_board_access_role.name
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "build.apprunner.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "flight_board_access" {
  role       = aws_iam_role.flight_board_access.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSAppRunnerServicePolicyForECRAccess"
}

# The running service's role: it only reads the settings secret.
resource "aws_iam_role" "flight_board_instance" {
  name = local.limited_names.flight_board_instance_role.name
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "tasks.apprunner.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "flight_board_settings" {
  name = "read-settings"
  role = aws_iam_role.flight_board_instance.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = "secretsmanager:GetSecretValue"
      Resource = aws_secretsmanager_secret.settings.arn
    }]
  })
}

# Exactly one instance: the app keeps its view of the day in memory.
resource "aws_apprunner_auto_scaling_configuration_version" "flight_board" {
  auto_scaling_configuration_name = local.limited_names.flight_board_scaling.name
  min_size                        = 1
  max_size                        = 1

  # Checked here, the first App Runner resource, so a long core name fails at plan with a clear message.
  lifecycle {
    precondition {
      condition     = length(local.names_too_long) == 0
      error_message = "These names are longer than AWS allows; shorten core's resource name: ${join(", ", local.names_too_long)}"
    }
  }
}

resource "aws_apprunner_service" "flight_board" {
  service_name                   = local.limited_names.flight_board_service.name
  auto_scaling_configuration_arn = aws_apprunner_auto_scaling_configuration_version.flight_board.arn

  source_configuration {
    auto_deployments_enabled = false
    authentication_configuration {
      access_role_arn = aws_iam_role.flight_board_access.arn
    }
    image_repository {
      image_identifier      = var.image_uri
      image_repository_type = "ECR"
      image_configuration {
        port                        = "8000"
        runtime_environment_secrets = local.setting_refs
      }
    }
  }

  instance_configuration {
    instance_role_arn = aws_iam_role.flight_board_instance.arn
  }

  # TCP, not HTTP: once APP_PASSWORD is set every HTTP path answers 401.
  health_check_configuration {
    protocol = "TCP"
  }


  depends_on = [
    aws_iam_role_policy_attachment.flight_board_access,
    aws_iam_role_policy.flight_board_settings,
    aws_secretsmanager_secret_version.settings,
  ]
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
