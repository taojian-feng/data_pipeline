# Grid analyst agent (optional): a Bedrock Agent whose action group is the same access-aware
# toolbox the local `gridlake ask` uses (src/gridlake/agent). Off by default.
#
# Access is enforced twice:
#   1. per caller: the calling app sets sessionAttributes.clearance on InvokeAgent from the
#      user's groups, and the tools refuse anything above it (the model cannot change it);
#   2. per agent: Lake Formation grants the tools' role tables up to
#      var.analyst_agent_max_clearance only, so Athena refuses the rest regardless.
# The knowledge base is queried by the tools with a classification filter. It is not
# associated with the agent directly, because that path has no per-caller filter.

locals {
  agent_count      = var.enable_analyst_agent ? 1 : 0
  agent_functions  = jsondecode(file("${path.module}/agent_functions.json"))
  agent_cleared_to = slice(["public", "internal", "confidential"], 0, index(["public", "internal", "confidential"], var.analyst_agent_max_clearance) + 1)
}

resource "aws_athena_workgroup" "analyst" {
  count         = local.agent_count
  name          = "${local.name}-analyst"
  force_destroy = true

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true
    bytes_scanned_cutoff_per_query     = 1073741824 # 1 GiB guardrail per agent query

    result_configuration {
      output_location = "s3://${aws_s3_bucket.this["artifacts"].bucket}/athena-results/analyst/"
      encryption_configuration {
        encryption_option = "SSE_KMS"
        kms_key_arn       = aws_kms_key.data.arn
      }
    }
  }
}

resource "aws_iam_role" "analyst_tools" {
  count              = local.agent_count
  name               = "${local.name}-analyst-tools"
  assume_role_policy = data.aws_iam_policy_document.assume["lambda"].json
}

resource "aws_iam_role_policy_attachment" "analyst_tools_basic" {
  count      = local.agent_count
  role       = aws_iam_role.analyst_tools[0].name
  policy_arn = "arn:${data.aws_partition.current.partition}:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

data "aws_iam_policy_document" "analyst_tools" {
  count = local.agent_count
  statement {
    sid       = "Athena"
    actions   = ["athena:StartQueryExecution", "athena:GetQueryExecution", "athena:GetQueryResults", "athena:StopQueryExecution"]
    resources = [aws_athena_workgroup.analyst[0].arn]
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
    sid       = "AthenaResults"
    actions   = ["s3:GetObject", "s3:PutObject", "s3:AbortMultipartUpload", "s3:GetBucketLocation", "s3:ListBucket"]
    resources = [aws_s3_bucket.this["artifacts"].arn, "${aws_s3_bucket.this["artifacts"].arn}/athena-results/analyst/*"]
  }
  statement {
    sid       = "PublishedGraph"
    actions   = ["s3:GetObject"]
    resources = ["${aws_s3_bucket.this["knowledge"].arn}/graph/*"]
  }
  statement {
    sid       = "Kms"
    actions   = ["kms:Decrypt", "kms:GenerateDataKey"]
    resources = [aws_kms_key.data.arn]
  }
  statement {
    sid       = "KnowledgeBaseRetrieve"
    actions   = ["bedrock:Retrieve"]
    resources = ["arn:${data.aws_partition.current.partition}:bedrock:${var.region}:${local.account_id}:knowledge-base/*"]
  }
}

resource "aws_iam_role_policy" "analyst_tools" {
  count  = local.agent_count
  role   = aws_iam_role.analyst_tools[0].id
  policy = data.aws_iam_policy_document.analyst_tools[0].json
}

# Same shape as the analyst grant in governance.tf, capped at the agent's max clearance.
resource "aws_lakeformation_permissions" "analyst_tools" {
  count       = local.agent_count
  depends_on  = [aws_lakeformation_resource_lf_tags.table]
  principal   = aws_iam_role.analyst_tools[0].arn
  permissions = ["SELECT", "DESCRIBE"]

  lf_tag_policy {
    resource_type = "TABLE"
    expression {
      key    = aws_lakeformation_lf_tag.classification.key
      values = local.agent_cleared_to
    }
    expression {
      key    = aws_lakeformation_lf_tag.layer.key
      values = ["silver", "gold"]
    }
  }
}

resource "aws_lambda_function" "analyst_tools" {
  count         = local.agent_count
  function_name = "${local.name}-analyst-tools"
  role          = aws_iam_role.analyst_tools[0].arn
  package_type  = "Image"
  image_uri     = coalesce(var.lambda_image_uri, "${aws_ecr_repository.pipeline.repository_url}:${var.artifact_version}")
  timeout       = 90
  memory_size   = 1769
  image_config {
    command = ["gridlake.agent.bedrock_action_group.handler"]
  }
  environment {
    variables = {
      GRIDLAKE_HOME             = "/tmp/gridlake"
      GRIDLAKE_AGENT_SQL        = "athena"
      GRIDLAKE_ATHENA_WORKGROUP = aws_athena_workgroup.analyst[0].name
      GRIDLAKE_GRAPH_S3_URI     = local.pipeline_env.GRIDLAKE_GRAPH_S3_URI
      GRIDLAKE_BEDROCK_KB_ID    = var.bedrock_knowledge_base_id
    }
  }
  tracing_config {
    mode = "Active"
  }
}

resource "aws_cloudwatch_log_group" "analyst_tools" {
  count             = local.agent_count
  name              = "/aws/lambda/${local.name}-analyst-tools"
  retention_in_days = 30
}

data "aws_iam_policy_document" "analyst_agent_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["bedrock.amazonaws.com"]
    }
    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [local.account_id]
    }
  }
}

resource "aws_iam_role" "analyst_agent" {
  count              = local.agent_count
  name               = "${local.name}-analyst-agent"
  assume_role_policy = data.aws_iam_policy_document.analyst_agent_assume.json
}

resource "aws_iam_role_policy" "analyst_agent" {
  count = local.agent_count
  role  = aws_iam_role.analyst_agent[0].id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = ["bedrock:InvokeModel", "bedrock:InvokeModelWithResponseStream", "bedrock:GetInferenceProfile"]
      Resource = [
        "arn:${data.aws_partition.current.partition}:bedrock:*::foundation-model/*",
        "arn:${data.aws_partition.current.partition}:bedrock:${var.region}:${local.account_id}:inference-profile/*",
      ]
    }]
  })
}

resource "aws_bedrockagent_agent" "analyst" {
  count                       = local.agent_count
  agent_name                  = "${local.name}-grid-analyst"
  description                 = "Answers grid questions with access-aware SQL, knowledge-graph and document tools, citing its tables."
  agent_resource_role_arn     = aws_iam_role.analyst_agent[0].arn
  foundation_model            = var.analyst_agent_model
  instruction                 = file("${path.module}/../../src/gridlake/agent/instructions.txt")
  idle_session_ttl_in_seconds = 900
  prepare_agent               = true
}

resource "aws_lambda_permission" "analyst_agent" {
  count         = local.agent_count
  statement_id  = "AllowBedrockAgent"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.analyst_tools[0].function_name
  principal     = "bedrock.amazonaws.com"
  source_arn    = aws_bedrockagent_agent.analyst[0].agent_arn
}

resource "aws_bedrockagent_agent_action_group" "tools" {
  count                      = local.agent_count
  depends_on                 = [aws_lambda_permission.analyst_agent]
  agent_id                   = aws_bedrockagent_agent.analyst[0].agent_id
  agent_version              = "DRAFT"
  action_group_name          = "gridlake-tools"
  description                = "Access-aware gridlake tools (generated from src/gridlake/agent/tools.py)."
  skip_resource_in_use_check = true

  action_group_executor {
    lambda = aws_lambda_function.analyst_tools[0].arn
  }

  function_schema {
    member_functions {
      dynamic "functions" {
        for_each = local.agent_functions
        content {
          name        = functions.value.name
          description = functions.value.description
          dynamic "parameters" {
            for_each = functions.value.parameters
            content {
              map_block_key = parameters.key
              type          = parameters.value.type
              description   = parameters.value.description
              required      = parameters.value.required
            }
          }
        }
      }
    }
  }
}
