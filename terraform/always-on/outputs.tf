output "app_url" {
  value       = "https://${aws_apprunner_service.flight_board.service_url}"
  description = "The flight board"
}

output "app_password" {
  value       = random_password.app.result
  sensitive   = true
  description = "The flight board password (any username)"
}

output "ecr_repository_url" {
  value       = aws_ecr_repository.images.repository_url
  description = "Where deploy pushes the demo image"
}

output "generator_log_group" {
  value       = aws_cloudwatch_log_group.generator.name
  description = "CloudWatch log group for the continuous generator"
}

# deploy compares this with the image it just pushed: an unchanged image means only a secret may have changed,
# which App Runner and ECS don't pick up without a redeploy.
output "image_uri" {
  value       = var.image_uri
  description = "The image both services run"
}

output "ecs_cluster_name" {
  value       = aws_ecs_cluster.demo.name
  description = "The ECS cluster that runs the generator"
}

output "generator_service_name" {
  value       = aws_ecs_service.generator.name
  description = "The generator's ECS service"
}

output "app_service_arn" {
  value       = aws_apprunner_service.flight_board.arn
  description = "The flight board's App Runner service"
}
