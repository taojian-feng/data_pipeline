# Data reliability agent (optional): EventBridge sends every Step Functions execution status
# change to a Lambda that triages failed runs and quality spikes. It reads the execution
# history, quality results and the published lineage graph, writes drafted fixes and issues
# to s3://<artifacts>/incidents/ and summarises them on the alerts topic. It cannot change
# data, contracts or code: a person approves drafts with `gridlake incidents approve`.

locals {
  reliability_count = var.enable_reliability_agent ? 1 : 0
}

resource "aws_iam_role" "reliability_agent" {
  count              = local.reliability_count
  name               = "${local.name}-reliability-agent"
  assume_role_policy = data.aws_iam_policy_document.assume["lambda"].json
}

resource "aws_iam_role_policy_attachment" "reliability_agent_basic" {
  count      = local.reliability_count
  role       = aws_iam_role.reliability_agent[0].name
  policy_arn = "arn:${data.aws_partition.current.partition}:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

data "aws_iam_policy_document" "reliability_agent" {
  count = local.reliability_count
  statement {
    sid       = "RunLog"
    actions   = ["states:DescribeExecution", "states:GetExecutionHistory", "states:ListExecutions"]
    resources = [aws_sfn_state_machine.pipeline.arn, "arn:${data.aws_partition.current.partition}:states:${var.region}:${local.account_id}:execution:${aws_sfn_state_machine.pipeline.name}:*"]
  }
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
    sid       = "Drafts"
    actions   = ["s3:GetObject", "s3:PutObject", "s3:ListBucket"]
    resources = [aws_s3_bucket.this["artifacts"].arn, "${aws_s3_bucket.this["artifacts"].arn}/incidents/*"]
  }
  statement {
    sid     = "CatalogRead"
    actions = ["glue:GetDatabase", "glue:GetDatabases", "glue:GetTable", "glue:GetTables", "glue:GetPartitions", "lakeformation:GetDataAccess"]
    resources = concat(
      ["arn:${data.aws_partition.current.partition}:glue:${var.region}:${local.account_id}:catalog"],
      [for db in aws_glue_catalog_database.layer : db.arn],
      [for db in aws_glue_catalog_database.layer : "arn:${data.aws_partition.current.partition}:glue:${var.region}:${local.account_id}:table/${db.name}/*"],
    )
  }
  statement {
    sid       = "Alerts"
    actions   = ["sns:Publish"]
    resources = [aws_sns_topic.alerts.arn]
  }
  statement {
    sid       = "Kms"
    actions   = ["kms:Decrypt", "kms:GenerateDataKey"]
    resources = [aws_kms_key.data.arn]
  }
  dynamic "statement" {
    for_each = var.reliability_agent_llm == "bedrock" ? [1] : []
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

resource "aws_iam_role_policy" "reliability_agent" {
  count  = local.reliability_count
  role   = aws_iam_role.reliability_agent[0].id
  policy = data.aws_iam_policy_document.reliability_agent[0].json
}

# Read-only Lake Formation access: quality results and quarantine (ops), and the data
# layers up to the agent's clearance so it can compare rejected rows with accepted ones.
resource "aws_lakeformation_permissions" "reliability_agent_ops" {
  count       = local.reliability_count
  depends_on  = [aws_lakeformation_resource_lf_tags.table]
  principal   = aws_iam_role.reliability_agent[0].arn
  permissions = ["SELECT", "DESCRIBE"]

  lf_tag_policy {
    resource_type = "TABLE"
    expression {
      key    = aws_lakeformation_lf_tag.layer.key
      values = ["ops"]
    }
  }
}

resource "aws_lakeformation_permissions" "reliability_agent_data" {
  count       = local.reliability_count
  depends_on  = [aws_lakeformation_resource_lf_tags.table]
  principal   = aws_iam_role.reliability_agent[0].arn
  permissions = ["SELECT", "DESCRIBE"]

  lf_tag_policy {
    resource_type = "TABLE"
    expression {
      key    = aws_lakeformation_lf_tag.classification.key
      values = slice(["public", "internal", "confidential"], 0, index(["public", "internal", "confidential"], var.reliability_agent_clearance) + 1)
    }
    expression {
      key    = aws_lakeformation_lf_tag.layer.key
      values = ["bronze", "silver", "gold"]
    }
  }
}

resource "aws_lakeformation_permissions" "reliability_agent_database" {
  for_each    = var.enable_reliability_agent ? local.namespaces : toset([])
  principal   = aws_iam_role.reliability_agent[0].arn
  permissions = ["DESCRIBE"]

  database {
    name = aws_glue_catalog_database.layer[each.value].name
  }

  depends_on = [aws_lakeformation_data_lake_settings.this]
}

resource "aws_lambda_function" "reliability_agent" {
  count         = local.reliability_count
  function_name = "${local.name}-reliability-agent"
  role          = aws_iam_role.reliability_agent[0].arn
  package_type  = "Image"
  image_uri     = coalesce(var.lambda_image_uri, "${aws_ecr_repository.pipeline.repository_url}:${var.artifact_version}")
  timeout       = 300
  memory_size   = 3008
  ephemeral_storage {
    size = 2048
  }
  image_config {
    command = ["gridlake.reliability.aws.handler"]
  }
  environment {
    variables = {
      GRIDLAKE_CATALOG               = local.pipeline_env.GRIDLAKE_CATALOG
      GRIDLAKE_WAREHOUSE             = local.pipeline_env.GRIDLAKE_WAREHOUSE
      GRIDLAKE_HOME                  = "/tmp/gridlake"
      GRIDLAKE_GRAPH_S3_URI          = local.pipeline_env.GRIDLAKE_GRAPH_S3_URI
      GRIDLAKE_INCIDENTS_S3_URI      = "s3://${aws_s3_bucket.this["artifacts"].bucket}/incidents/"
      GRIDLAKE_ALERTS_TOPIC_ARN      = aws_sns_topic.alerts.arn
      GRIDLAKE_AGENT_LLM             = var.reliability_agent_llm
      GRIDLAKE_AGENT_MODEL           = var.analyst_agent_model
      GRIDLAKE_RELIABILITY_CLEARANCE = var.reliability_agent_clearance
      GRIDLAKE_RELIABILITY_MIN_RATE  = "0.02"
    }
  }
  tracing_config {
    mode = "Active"
  }
}

resource "aws_cloudwatch_log_group" "reliability_agent" {
  count             = local.reliability_count
  name              = "/aws/lambda/${local.name}-reliability-agent"
  retention_in_days = 30
}

resource "aws_cloudwatch_event_rule" "pipeline_finished" {
  count       = local.reliability_count
  name        = "${local.name}-pipeline-finished"
  description = "Every finished gridlake pipeline execution, for the data reliability agent."
  event_pattern = jsonencode({
    source        = ["aws.states"]
    "detail-type" = ["Step Functions Execution Status Change"]
    detail = {
      stateMachineArn = [aws_sfn_state_machine.pipeline.arn]
      status          = ["SUCCEEDED", "FAILED", "TIMED_OUT", "ABORTED"]
    }
  })
}

resource "aws_cloudwatch_event_target" "reliability_agent" {
  count = local.reliability_count
  rule  = aws_cloudwatch_event_rule.pipeline_finished[0].name
  arn   = aws_lambda_function.reliability_agent[0].arn
  retry_policy {
    maximum_retry_attempts       = 2
    maximum_event_age_in_seconds = 3600
  }
}

resource "aws_lambda_permission" "reliability_agent" {
  count         = local.reliability_count
  statement_id  = "AllowEventBridge"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.reliability_agent[0].function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.pipeline_finished[0].arn
}
