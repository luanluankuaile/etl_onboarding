output "deployed_table_metadata" {
  description = "Absolute paths of deployed Raw and Persistent table contracts."
  value       = [for resource in local_file.table_metadata : resource.filename]
}

output "deployed_workflows" {
  description = "Absolute paths of deployed workflow definitions."
  value       = [for resource in local_file.workflow_metadata : resource.filename]
}