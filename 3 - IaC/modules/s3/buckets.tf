locals {
  # Only add the hyphen + suffix when random is enabled
  suffix = var.use_random_suffix && length(random_id.suffix) > 0 ? "-${random_id.suffix[0].hex}" : ""

  common_tags = merge({
    Project     = var.project
    Environment = var.environment
  }, var.tags)
}

# Conditionally create a random suffix for global uniqueness
resource "random_id" "suffix" {
  count       = var.use_random_suffix ? 1 : 0
  byte_length = 4
}

# Data bucket
resource "aws_s3_bucket" "data" {
  bucket        = "${var.project}-${var.environment}-data${local.suffix}"
  force_destroy = var.force_destroy
  tags          = merge(local.common_tags, { Name = "data" })
}

# DAGs bucket
resource "aws_s3_bucket" "dags" {
  bucket        = "${var.project}-${var.environment}-dags${local.suffix}"
  force_destroy = var.force_destroy
  tags          = merge(local.common_tags, { Name = "dags" })
}

# Logs bucket
resource "aws_s3_bucket" "logs" {
  bucket        = "${var.project}-${var.environment}-logs${local.suffix}"
  force_destroy = var.force_destroy
  tags          = merge(local.common_tags, { Name = "logs" })
}

# Public access blocks (hardened by default)
resource "aws_s3_bucket_public_access_block" "data" {
  bucket                  = aws_s3_bucket.data.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_public_access_block" "dags" {
  bucket                  = aws_s3_bucket.dags.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_public_access_block" "logs" {
  bucket                  = aws_s3_bucket.logs.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Enforce bucket-owner-only ownership (no ACLs)
resource "aws_s3_bucket_ownership_controls" "data" {
  bucket = aws_s3_bucket.data.id
  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

resource "aws_s3_bucket_ownership_controls" "dags" {
  bucket = aws_s3_bucket.dags.id
  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

resource "aws_s3_bucket_ownership_controls" "logs" {
  bucket = aws_s3_bucket.logs.id
  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}
