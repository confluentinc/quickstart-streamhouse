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
