variable "project" {
  description = "Project name used in bucket naming and tags"
  type        = string
  default     = "air-quality-indicators"
}

variable "environment" {
  description = "Environment identifier (e.g., dev, prod)"
  type        = string
  default     = "dev"
}

variable "use_random_suffix" {
  description = "Append a random suffix to buckets to ensure global uniqueness"
  type        = bool
  default     = true
}

variable "force_destroy" {
  description = "Allow bucket deletion even if non-empty (be careful in prod)"
  type        = bool
  default     = false
}

variable "tags" {
  description = "Extra tags to apply to all buckets"
  type        = map(string)
  default     = {}
}

# Lifecycle tuning (keep simple defaults)
variable "data_noncurrent_expiration_days" {
  description = "Delete noncurrent versions in data bucket after N days"
  type        = number
  default     = 1
}

variable "dags_noncurrent_expiration_days" {
  description = "Delete noncurrent versions in dags bucket after N days"
  type        = number
  default     = 90
}

variable "logs_expiration_days" {
  description = "Delete current log objects after N days (cost control)"
  type        = number
  default     = 90
}
