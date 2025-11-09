# ---- Existing: minimal S3 permissions (yours, kept) ----
resource "aws_iam_policy" "mwaa_s3_access" {
  name        = "${var.project}-${var.environment}-mwaa-s3-access"
  description = "Minimal S3 access for MWAA DAGs, logs (optional), and data"

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      # DAGs bucket: read-only
      {
        Effect = "Allow",
        Action = [
          "s3:GetObject",
          "s3:ListBucket",
          "s3:GetBucketLocation"
        ],
        Resource = [
          var.dags_bucket_arn,
          "${var.dags_bucket_arn}/*"
        ]
      },
      # Logs bucket (OPTIONAL for your own task artifacts, not required by MWAA)
      {
        Effect = "Allow",
        Action = [
          "s3:PutObject",
          "s3:ListBucket"
        ],
        Resource = [
          var.logs_bucket_arn,
          "${var.logs_bucket_arn}/*"
        ]
      },
      # Data bucket: rw (adjust if you want stricter)
      {
        Effect = "Allow",
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket",
          "s3:GetBucketLocation"
        ],
        Resource = [
          var.data_bucket_arn,
          "${var.data_bucket_arn}/*"
        ]
      }
    ]
  })
}

# ---- New: MWAA core permissions (CloudWatch Logs, SQS, metrics) ----
locals {
  account_id = data.aws_caller_identity.current.account_id
  region     = data.aws_region.current.id

  # CloudWatch log groups that MWAA uses: airflow-<env-name>-*
  mwaa_logs_arn = "arn:aws:logs:${local.region}:${local.account_id}:log-group:airflow-${var.mwaa_environment_name}-*"
  # SQS Celery queue created by MWAA in your account
  mwaa_sqs_arn  = "arn:aws:sqs:${local.region}:${local.account_id}:airflow-celery-*"
  # MWAA environment ARN for metrics
  mwaa_env_arn  = "arn:aws:airflow:${local.region}:${local.account_id}:environment/${var.mwaa_environment_name}"
}

resource "aws_iam_policy" "mwaa_core_access" {
  name        = "${var.project}-${var.environment}-mwaa-core-access"
  description = "Core permissions MWAA needs for logs, SQS and metrics"

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      # Publish health/metrics for this environment
      {
        Effect   = "Allow",
        Action   = ["airflow:PublishMetrics"],
        Resource = local.mwaa_env_arn
      },

      # CloudWatch Logs used by MWAA (task/scheduler/web/worker)
      {
        Effect = "Allow",
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents",
          "logs:GetLogEvents",
          "logs:GetLogRecord",
          "logs:GetLogGroupFields",
          "logs:GetQueryResults"
        ],
        Resource = local.mwaa_logs_arn
      },
      # Describe is only supported with "*"
      {
        Effect   = "Allow",
        Action   = ["logs:DescribeLogGroups"],
        Resource = "*"
      },

      # Celery executor SQS queue in your account
      {
        Effect = "Allow",
        Action = [
          "sqs:GetQueueAttributes",
          "sqs:GetQueueUrl",
          "sqs:ReceiveMessage",
          "sqs:SendMessage",
          "sqs:DeleteMessage",
          "sqs:ChangeMessageVisibility"
        ],
        Resource = local.mwaa_sqs_arn
      },

      # Needed if your account enforces S3 Public Access Block at account level
      {
        Effect   = "Allow",
        Action   = ["s3:GetAccountPublicAccessBlock"],
        Resource = "*"
      },

      # CloudWatch metrics for Airflow
      {
        Effect   = "Allow",
        Action   = ["cloudwatch:PutMetricData"],
        Resource = "*"
      }
    ]
  })
}

resource "aws_iam_policy" "pipeline_admin_policy" {
  name        = "${var.project}-${var.environment}-pipeline-admin"
  description = "Broad deploy permissions for CI/CD pipelines (use with caution)"

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Action = [
          "s3:ListBucket",
          "s3:GetBucketLocation",
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject"
        ],
        Resource = [
          var.dags_bucket_arn,
          "${var.dags_bucket_arn}/*",
          var.logs_bucket_arn,
          "${var.logs_bucket_arn}/*",
          var.data_bucket_arn,
          "${var.data_bucket_arn}/*"
        ]
      },

      {
        Effect = "Allow",
        Action = [
          "lambda:UpdateFunctionCode",
          "lambda:UpdateFunctionConfiguration",
          "lambda:CreateFunction",
          "lambda:DeleteFunction",
          "lambda:InvokeFunction"
        ],
        Resource = "*"
      },

      {
        Effect = "Allow",
        Action = [
          "airflow:CreateCliToken",
          "airflow:PublishMetrics"
        ],
        Resource = "*"
      },

      {
        Effect = "Allow",
        Action = [
          "iam:GetRole",
          "iam:PassRole",
          "sts:AssumeRole"
        ],
        Resource = "*"
      }
    ]
  })
}
