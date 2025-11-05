locals {
  tags = {
    Project     = var.project
    Environment = var.environment
  }
}

# Package lambda: simple zip using external data source (alternative: archive_file resource)
data "archive_file" "lambda_zip" {
  type        = "zip"
  source_dir  = var.source_path
  output_path = "${path.module}/build/${var.function_name}.zip"
}

resource "aws_iam_role" "lambda_role" {
  name = "${var.project}-${var.environment}-${var.function_name}-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Effect = "Allow",
      Principal = { Service = "lambda.amazonaws.com" },
      Action   = "sts:AssumeRole"
    }]
  })
  tags = local.tags
}

# Basic CloudWatch logs policy inline
resource "aws_iam_role_policy" "lambda_logs" {
  count = var.attach_logs_policy ? 1 : 0
  name  = "${var.project}-${var.environment}-${var.function_name}-logs"
  role  = aws_iam_role.lambda_role.id
  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Effect = "Allow",
      Action = [
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents"
      ],
      Resource = "arn:aws:logs:*:*:*"
    }]
  })
}

# Optional S3 access
resource "aws_iam_role_policy" "lambda_s3_rw" {
  count = var.attach_s3_policy ? 1 : 0
  name  = "${var.project}-${var.environment}-${var.function_name}-s3"
  role  = aws_iam_role.lambda_role.id
  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Effect = "Allow",
      Action = ["s3:PutObject","s3:GetObject","s3:ListBucket"],
      Resource = [
        "arn:aws:s3:::${var.data_bucket_name}",
        "arn:aws:s3:::${var.data_bucket_name}/*"
      ]
    }]
  })
}

# Optional Secrets Manager access
resource "aws_iam_role_policy" "lambda_secrets" {
  count = var.attach_secrets_policy && length(var.secrets_arns) > 0 ? 1 : 0
  name  = "${var.project}-${var.environment}-${var.function_name}-secrets"
  role  = aws_iam_role.lambda_role.id
  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Effect = "Allow",
      Action = ["secretsmanager:GetSecretValue"],
      Resource = var.secrets_arns
    }]
  })
}

resource "aws_lambda_function" "this" {
  function_name = "${var.project}-${var.environment}-${var.function_name}"
  role          = aws_iam_role.lambda_role.arn
  handler       = var.handler
  runtime       = var.runtime
  filename      = data.archive_file.lambda_zip.output_path
  source_code_hash = data.archive_file.lambda_zip.output_base64sha256
  memory_size   = var.memory_size
  timeout       = var.timeout

  environment {
    variables = var.environment_variables
  }
  tags = local.tags
}

# Permission so MWAA (Airflow tasks) can invoke the Lambda (any principal in the account)
resource "aws_lambda_permission" "allow_account_invoke" {
  statement_id  = "AllowAccountInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.this.function_name
  principal     = "*"  # Cambia a ARN específico de MWAA cuando lo tengas para mayor seguridad
}
