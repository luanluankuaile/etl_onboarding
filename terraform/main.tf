terraform {
  required_version = ">= 1.3.0"

  required_providers {
    local = {
      source  = "hashicorp/local"
      version = "~> 2.5"
    }
  }
}

locals {
  table_metadata_files = fileset("${path.module}/../metadata/tables", "**/*.yml")
  workflow_files = toset(concat(
    fileset("${path.module}/../metadata/workflows", "**/*.yml"),
    fileset("${path.module}/../metadata/workflows", "**/*.yaml"),
  ))
}

resource "local_file" "table_metadata" {
  for_each = toset(local.table_metadata_files)

  content              = file("${path.module}/../metadata/tables/${each.value}")
  filename             = "${var.metadata_directory}/tables/${each.value}"
  file_permission      = "0644"
  directory_permission = "0755"
}

resource "local_file" "workflow_metadata" {
  for_each = local.workflow_files

  content              = file("${path.module}/../metadata/workflows/${each.value}")
  filename             = "${var.metadata_directory}/workflows/${each.value}"
  file_permission      = "0644"
  directory_permission = "0755"
}