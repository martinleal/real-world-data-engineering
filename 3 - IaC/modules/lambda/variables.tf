variable "project" { type = string }
variable "environment" { type = string }
variable "function_name" { type = string }
variable "runtime" { type = string default = "python3.12" }
variable "handler" { type = string }
variable "source_path" { description = "Local path to lambda source code (folder)." type = string }
variable "data_bucket_name" { type = string }
variable "environment_variables" { type = map(string) default = {} }
variable "secrets_arns" { type = list(string) default = [] }
variable "memory_size" { type = number default = 256 }
variable "timeout" { type = number default = 60 }
variable "attach_s3_policy" { type = bool default = true }
variable "attach_secrets_policy" { type = bool default = true }
variable "attach_logs_policy" { type = bool default = true }
