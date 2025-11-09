output "data_bucket_name" {
  description = "S3 bucket name for data layer"
  value       = module.s3.data_bucket_name
}

output "dags_bucket_name" {
  description = "S3 bucket name storing Airflow DAGs"
  value       = module.s3.dags_bucket_name
}

output "logs_bucket_name" {
  description = "S3 bucket name for Airflow logs"
  value       = module.s3.logs_bucket_name
}

output "data_bucket_arn" {
  description = "ARN of data bucket"
  value       = module.s3.data_bucket_arn
}

output "dags_bucket_arn" {
  description = "ARN of DAGs bucket"
  value       = module.s3.dags_bucket_arn
}

output "logs_bucket_arn" {
  description = "ARN of logs bucket"
  value       = module.s3.logs_bucket_arn
}

output "mwaa_execution_role_arn" {
  description = "IAM role ARN for MWAA execution"
  value       = module.iam.mwaa_execution_role_arn
}

output "mwaa_execution_role_name" {
  description = "IAM role name for MWAA execution"
  value       = module.iam.mwaa_execution_role_name
}

output "mwaa_s3_policy_arn" {
  description = "ARN of custom IAM policy granting MWAA access to S3 buckets"
  value       = module.iam.mwaa_s3_policy_arn
}

output "pipeline_admin_role_arn" {
  description = "ARN of the pipeline-admin IAM role for CI/CD"
  value       = module.iam.pipeline_admin_role_arn
}

output "pipeline_admin_policy_arn" {
  description = "ARN of the pipeline admin policy"
  value       = module.iam.pipeline_admin_policy_arn
}