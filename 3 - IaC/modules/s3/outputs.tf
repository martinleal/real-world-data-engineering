output "data_bucket_name" {
  value       = aws_s3_bucket.data.bucket
  description = "S3 bucket name for data"
}

output "dags_bucket_name" {
  value       = aws_s3_bucket.dags.bucket
  description = "S3 bucket name for Airflow DAGs"
}

output "logs_bucket_name" {
  value       = aws_s3_bucket.logs.bucket
  description = "S3 bucket name for Airflow logs"
}

output "data_bucket_arn" {
  value       = aws_s3_bucket.data.arn
  description = "ARN for data bucket"
}

output "dags_bucket_arn" {
  value       = aws_s3_bucket.dags.arn
  description = "ARN for DAGs bucket"
}

output "logs_bucket_arn" {
  value       = aws_s3_bucket.logs.arn
  description = "ARN for logs bucket"
}
