output "lab_tables" {
  description = "The tables and model this root created."
  value       = keys(local.objects)
}
