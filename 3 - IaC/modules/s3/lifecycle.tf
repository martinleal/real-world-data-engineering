# Data: keep noncurrent versions for rollback safety, then prune
resource "aws_s3_bucket_lifecycle_configuration" "data" {
  bucket = aws_s3_bucket.data.id

  rule {
    id     = "expire-data-noncurrent"
    status = "Enabled"

    filter {} # Matches all objects in the bucket
    
    noncurrent_version_expiration {
      noncurrent_days = var.data_noncurrent_expiration_days
    }
  }
}

# DAGs: changes are in Git; keep old versions briefly
resource "aws_s3_bucket_lifecycle_configuration" "dags" {
  bucket = aws_s3_bucket.dags.id

  rule {
    id     = "expire-dags-noncurrent"
    status = "Enabled"

    filter {} # Matches all objects in the bucket
    
    noncurrent_version_expiration {
      noncurrent_days = var.dags_noncurrent_expiration_days
    }
  }
}

# Logs: delete current objects after N days to control cost
resource "aws_s3_bucket_lifecycle_configuration" "logs" {
  bucket = aws_s3_bucket.logs.id

  rule {
    id     = "expire-logs"
    status = "Enabled"

    filter {} # Matches all objects in the bucket
    
    expiration {
      days = var.logs_expiration_days
    }
  }
}

