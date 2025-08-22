module "s3" {
  source  = "./modules/s3"
  project = "air-quality-indicators"
  environment = "dev" # or "prod"
  use_random_suffix = true

  # Set to true ONLY in sandboxes to allow easy teardown:
  force_destroy = true

  # Optional: tweak retention
  data_noncurrent_expiration_days = 1
  dags_noncurrent_expiration_days = 1
  logs_expiration_days            = 1
}

module "iam" {
  source = "./modules/iam"

  project     = "air-quality-indicators"
  environment = "dev"

  dags_bucket_arn = module.s3.dags_bucket_arn
  logs_bucket_arn = module.s3.logs_bucket_arn
  data_bucket_arn = module.s3.data_bucket_arn
  mwaa_environment_name = "air-quality-indicators-mwaa-dev"
}
