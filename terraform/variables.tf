variable "metadata_directory" {
  description = "Absolute directory where the ETL runtime reads deployed metadata."
  type        = string
  default     = "/opt/local_etl/metadata"
}