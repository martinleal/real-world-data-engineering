variable "project" {
  description = "Project name"
  type        = string
  default     = "air-quality-indicators"
}

variable "environment" {
  description = "Environment (e.g. dev, prod)"
  type        = string
  default     = "dev"
}

variable "mwaa_environment_name" {
  description = "Name of the MWAA environment (used to scope logs & metrics)"
  type        = string
}

variable "dags_bucket_arn" {
  description = "ARN of the DAGs bucket"
  type        = string
}

variable "logs_bucket_arn" {
  description = "ARN of the Logs bucket"
  type        = string
}

variable "data_bucket_arn" {
  description = "ARN of the Data bucket"
  type        = string
}

