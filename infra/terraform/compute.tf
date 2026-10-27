locals {
  pipeline_env = {
    GRIDLAKE_CATALOG      = "glue"
    GRIDLAKE_WAREHOUSE    = local.warehouse
    GRIDLAKE_HOME         = "/tmp/gridlake"
    GRIDLAKE_KB_S3_URI    = "s3://${aws_s3_bucket.this["knowledge"].bucket}/kb/"
    GRIDLAKE_GRAPH_S3_URI = "s3://${aws_s3_bucket.this["knowledge"].bucket}/graph/graph.nt"
    EIA_API_KEY_SECRET    = var.eia_api_key_secret_name
    POWERTOOLS_LOG_LEVEL  = "INFO"
  }
}

resource "aws_ecr_repository" "pipeline" {
  name                 = local.name
  image_tag_mutability = "IMMUTABLE"
  image_scanning_configuration {
    scan_on_push = true
  }
  encryption_configuration {
    encryption_type = "KMS"
    kms_key         = aws_kms_key.data.arn
  }
}

# One image, two handlers: Step Functions tasks and the Kinesis consumer.
resource "aws_lambda_function" "step" {
  function_name = "${local.name}-step"
  role          = aws_iam_role.lambda.arn
  package_type  = "Image"
  image_uri     = coalesce(var.lambda_image_uri, "${aws_ecr_repository.pipeline.repository_url}:${var.artifact_version}")
  timeout       = 900
  memory_size   = 3008
  ephemeral_storage {
    size = 4096
  }
  image_config {
    command = ["gridlake.aws_lambda.step_handler"]
  }
  environment {
    variables = local.pipeline_env
  }
  tracing_config {
    mode = "Active"
  }
}

resource "aws_lambda_function" "stream" {
  function_name                  = "${local.name}-telemetry-consumer"
  role                           = aws_iam_role.lambda.arn
  package_type                   = "Image"
  image_uri                      = coalesce(var.lambda_image_uri, "${aws_ecr_repository.pipeline.repository_url}:${var.artifact_version}")
  timeout                        = 120
  memory_size                    = 1769
  reserved_concurrent_executions = var.stream_reserved_concurrency
  image_config {
    command = ["gridlake.aws_lambda.kinesis_handler"]
  }
  environment {
    variables = local.pipeline_env
  }
  tracing_config {
    mode = "Active"
  }
}

resource "aws_cloudwatch_log_group" "lambda" {
  for_each          = { step = aws_lambda_function.step.function_name, stream = aws_lambda_function.stream.function_name }
  name              = "/aws/lambda/${each.value}"
  retention_in_days = 30
}

# Scale-out path for backfills: Spark + Iceberg MERGE (spark/glue_silver_demand_hourly.py)
resource "aws_s3_object" "glue_scripts" {
  for_each = toset(["glue_silver_demand_hourly.py", "iceberg_maintenance.py"])
  bucket   = aws_s3_bucket.this["artifacts"].id
  key      = "glue/${each.key}"
  source   = "${path.module}/../../spark/${each.key}"
  etag     = filemd5("${path.module}/../../spark/${each.key}")
}

resource "aws_glue_job" "silver_demand_spark" {
  name              = "${local.name}-silver-demand-hourly"
  role_arn          = aws_iam_role.glue.arn
  glue_version      = "5.0"
  worker_type       = "G.1X"
  number_of_workers = 4
  execution_class   = "FLEX"
  timeout           = 60

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.this["artifacts"].bucket}/${aws_s3_object.glue_scripts["glue_silver_demand_hourly.py"].key}"
    python_version  = "3"
  }

  default_arguments = {
    "--datalake-formats"                 = "iceberg"
    "--enable-glue-datacatalog"          = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-metrics"                   = "true"
    "--enable-observability-metrics"     = "true"
    "--job-bookmark-option"              = "job-bookmark-disable" # Iceberg snapshots are the bookmark
    "--catalog"                          = "glue_catalog"
    "--warehouse"                        = local.warehouse
  }
}

resource "aws_glue_job" "maintenance" {
  name              = "${local.name}-iceberg-maintenance"
  role_arn          = aws_iam_role.glue.arn
  glue_version      = "5.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 120

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.this["artifacts"].bucket}/${aws_s3_object.glue_scripts["iceberg_maintenance.py"].key}"
  }

  default_arguments = {
    "--datalake-formats" = "iceberg"
    "--catalog"          = "glue_catalog"
    "--warehouse"        = local.warehouse
    "--tables"           = join(",", [for name, c in local.contracts : name if c.layer != "ops"])
    "--retain_days"      = "7"
  }
}

resource "aws_glue_trigger" "maintenance" {
  name     = "${local.name}-nightly-maintenance"
  type     = "SCHEDULED"
  schedule = "cron(30 3 * * ? *)"
  actions {
    job_name = aws_glue_job.maintenance.name
  }
}
