# Grid anomaly watcher (optional): an EventBridge schedule runs a Lambda once a day. It reads
# the public gold data products, flags unusual demand, interchange and fuel-mix changes,
# writes the brief to s3://<artifacts>/briefs/ and posts it to the grid-brief topic. It only
# reads gold; it changes nothing.

locals {
  watcher_count = var.enable_anomaly_watcher ? 1 : 0
}

resource "aws_sns_topic" "grid_brief" {
  count             = local.watcher_count
  name              = "${local.name}-grid-brief"
  kms_master_key_id = aws_kms_key.data.arn
}

resource "aws_sns_topic_subscription" "grid_brief_email" {
  for_each  = var.enable_anomaly_watcher ? toset(var.anomaly_watcher_emails) : toset([])
  topic_arn = aws_sns_topic.grid_brief[0].arn
  protocol  = "email"
  endpoint  = each.value
}

resource "aws_iam_role" "anomaly_watcher" {
  count              = local.watcher_count
  name               = "${local.name}-anomaly-watcher"
  assume_role_policy = data.aws_iam_policy_document.assume["lambda"].json
}

resource "aws_iam_role_policy_attachment" "anomaly_watcher_basic" {
  count      = local.watcher_count
  role       = aws_iam_role.anomaly_watcher[0].name
  policy_arn = "arn:${data.aws_partition.current.partition}:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

data "aws_iam_policy_document" "anomaly_watcher" {
  count = local.watcher_count
  statement {
    sid       = "ReadLakehouse"
    actions   = ["s3:GetObject", "s3:ListBucket", "s3:GetBucketLocation"]
    resources = [for k in ["warehouse", "knowledge"] : aws_s3_bucket.this[k].arn]
  }
  statement {
    sid       = "ReadLakehouseObjects"
    actions   = ["s3:GetObject"]
    resources = ["${aws_s3_bucket.this["warehouse"].arn}/*", "${aws_s3_bucket.this["knowledge"].arn}/graph/*"]
  }
  statement {
    sid       = "Briefs"
    actions   = ["s3:PutObject"]
    resources = ["${aws_s3_bucket.this["artifacts"].arn}/briefs/*"]
  }
  statement {
    sid     = "CatalogRead"
    actions = ["glue:GetDatabase", "glue:GetDatabases", "glue:GetTable", "glue:GetTables", "glue:GetPartitions", "lakeformation:GetDataAccess"]
    resources = [
      "arn:${data.aws_partition.current.partition}:glue:${var.region}:${local.account_id}:catalog",
      aws_glue_catalog_database.layer["gold"].arn,
      "arn:${data.aws_partition.current.partition}:glue:${var.region}:${local.account_id}:table/${aws_glue_catalog_database.layer["gold"].name}/*",
    ]
  }
  statement {
    sid       = "Brief"
    actions   = ["sns:Publish"]
    resources = [aws_sns_topic.grid_brief[0].arn]
  }
  statement {
    sid       = "Kms"
    actions   = ["kms:Decrypt", "kms:GenerateDataKey"]
    resources = [aws_kms_key.data.arn]
  }
  dynamic "statement" {
    for_each = var.anomaly_watcher_llm == "bedrock" ? [1] : []
    content {
      sid     = "Claude"
      actions = ["bedrock:InvokeModel", "bedrock:InvokeModelWithResponseStream"]
      resources = [
        "arn:${data.aws_partition.current.partition}:bedrock:*::foundation-model/*",
        "arn:${data.aws_partition.current.partition}:bedrock:${var.region}:${local.account_id}:inference-profile/*",
      ]
    }
  }
}

resource "aws_iam_role_policy" "anomaly_watcher" {
  count  = local.watcher_count
  role   = aws_iam_role.anomaly_watcher[0].id
  policy = data.aws_iam_policy_document.anomaly_watcher[0].json
}

# Read-only Lake Formation access to the public gold data products, nothing else: the brief
# goes to a wide audience, so the watcher cannot read what the brief must not contain.
resource "aws_lakeformation_permissions" "anomaly_watcher_gold" {
  count       = local.watcher_count
  depends_on  = [aws_lakeformation_resource_lf_tags.table]
  principal   = aws_iam_role.anomaly_watcher[0].arn
  permissions = ["SELECT", "DESCRIBE"]

  lf_tag_policy {
    resource_type = "TABLE"
    expression {
      key    = aws_lakeformation_lf_tag.classification.key
      values = ["public"]
    }
    expression {
      key    = aws_lakeformation_lf_tag.layer.key
      values = ["gold"]
    }
  }
}

resource "aws_lakeformation_permissions" "anomaly_watcher_database" {
  count       = local.watcher_count
  principal   = aws_iam_role.anomaly_watcher[0].arn
  permissions = ["DESCRIBE"]

  database {
    name = aws_glue_catalog_database.layer["gold"].name
  }

  depends_on = [aws_lakeformation_data_lake_settings.this]
}

resource "aws_lambda_function" "anomaly_watcher" {
  count         = local.watcher_count
  function_name = "${local.name}-anomaly-watcher"
  role          = aws_iam_role.anomaly_watcher[0].arn
  package_type  = "Image"
  image_uri     = coalesce(var.lambda_image_uri, "${aws_ecr_repository.pipeline.repository_url}:${var.artifact_version}")
  timeout       = 300
  memory_size   = 3008
  ephemeral_storage {
    size = 2048
  }
  image_config {
    command = ["gridlake.watcher.aws.handler"]
  }
  environment {
    variables = {
      GRIDLAKE_CATALOG           = local.pipeline_env.GRIDLAKE_CATALOG
      GRIDLAKE_WAREHOUSE         = local.pipeline_env.GRIDLAKE_WAREHOUSE
      GRIDLAKE_HOME              = "/tmp/gridlake"
      GRIDLAKE_GRAPH_S3_URI      = local.pipeline_env.GRIDLAKE_GRAPH_S3_URI
      GRIDLAKE_BRIEFS_S3_URI     = "s3://${aws_s3_bucket.this["artifacts"].bucket}/briefs/"
      GRIDLAKE_BRIEF_TOPIC_ARN   = aws_sns_topic.grid_brief[0].arn
      GRIDLAKE_AGENT_LLM         = var.anomaly_watcher_llm
      GRIDLAKE_AGENT_MODEL       = var.analyst_agent_model
      GRIDLAKE_WATCHER_CLEARANCE = "public"
    }
  }
  tracing_config {
    mode = "Active"
  }
}

resource "aws_cloudwatch_log_group" "anomaly_watcher" {
  count             = local.watcher_count
  name              = "/aws/lambda/${local.name}-anomaly-watcher"
  retention_in_days = 30
}

resource "aws_cloudwatch_event_rule" "daily_brief" {
  count               = local.watcher_count
  name                = "${local.name}-daily-grid-brief"
  description         = "Runs the grid anomaly watcher on the latest complete UTC day."
  schedule_expression = var.anomaly_watcher_schedule
}

resource "aws_cloudwatch_event_target" "anomaly_watcher" {
  count = local.watcher_count
  rule  = aws_cloudwatch_event_rule.daily_brief[0].name
  arn   = aws_lambda_function.anomaly_watcher[0].arn
  retry_policy {
    maximum_retry_attempts       = 2
    maximum_event_age_in_seconds = 3600
  }
}

resource "aws_lambda_permission" "anomaly_watcher" {
  count         = local.watcher_count
  statement_id  = "AllowEventBridge"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.anomaly_watcher[0].function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.daily_brief[0].arn
}
