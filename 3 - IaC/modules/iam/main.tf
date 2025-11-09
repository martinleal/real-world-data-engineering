data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

resource "aws_iam_role" "mwaa_execution_role" {
  name = "${var.project}-${var.environment}-mwaa-exec-role"

  # Include BOTH principals per AWS docs
  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Principal = { Service = ["airflow.amazonaws.com","airflow-env.amazonaws.com"] },
        Action   = "sts:AssumeRole"
      }
    ]
  })

  tags = {
    Project     = var.project
    Environment = var.environment
  }
}

# Attach your existing S3 policy (scoped to your buckets)
resource "aws_iam_role_policy_attachment" "mwaa_s3_access" {
  role       = aws_iam_role.mwaa_execution_role.name
  policy_arn = aws_iam_policy.mwaa_s3_access.arn
}

# Attach MWAA core policy (CloudWatch Logs, SQS, metrics)
resource "aws_iam_role_policy_attachment" "mwaa_core_access" {
  role       = aws_iam_role.mwaa_execution_role.name
  policy_arn = aws_iam_policy.mwaa_core_access.arn
}

resource "aws_iam_role" "pipeline_admin_role" {
  name = "${var.project}-${var.environment}-pipeline-admin"

  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Principal = {
          Federated = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:oidc-provider/token.actions.githubusercontent.com"
        },
        Action = "sts:AssumeRoleWithWebIdentity",
        Condition = {
          StringEquals = {
            "token.actions.githubusercontent.com:aud" = "sts.amazonaws.com",
            # Allow any ref for now; tighten later if needed
            "token.actions.githubusercontent.com:sub" = "repo:martinleal/real-world-data-engineering:*"
          }
        }
      }
    ]
  })

  tags = {
    Project     = var.project
    Environment = var.environment
  }
}

resource "aws_iam_role_policy_attachment" "pipeline_admin_attach" {
  role       = aws_iam_role.pipeline_admin_role.name
  policy_arn = aws_iam_policy.pipeline_admin_policy.arn
}
