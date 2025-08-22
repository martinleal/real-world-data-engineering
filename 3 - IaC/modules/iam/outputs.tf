output "mwaa_execution_role_arn" {
  description = "IAM role ARN for MWAA execution"
  value       = aws_iam_role.mwaa_execution_role.arn
}

output "mwaa_execution_role_name" {
  description = "IAM role name for MWAA execution"
  value       = aws_iam_role.mwaa_execution_role.name
}

output "mwaa_s3_policy_arn" {
  description = "ARN of the custom IAM policy granting MWAA access to S3 buckets"
  value       = aws_iam_policy.mwaa_s3_access.arn
}

