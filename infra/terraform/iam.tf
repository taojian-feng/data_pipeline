# Least-privilege roles. Data access is additionally mediated by Lake Formation grants.

data "aws_iam_policy_document" "assume" {
  for_each = {
    glue          = "glue.amazonaws.com"
    lambda        = "lambda.amazonaws.com"
    states        = "states.amazonaws.com"
    scheduler     = "scheduler.amazonaws.com"
    lakeformation = "lakeformation.amazonaws.com"
  }
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = [each.value]
    }
  }
}

data "aws_iam_policy_document" "data_plane" {
  statement {
    sid       = "Warehouse"
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [for k in ["warehouse", "landing", "knowledge"] : aws_s3_bucket.this[k].arn]
  }
  statement {
    sid       = "WarehouseObjects"
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
    resources = [for k in ["warehouse", "landing", "knowledge"] : "${aws_s3_bucket.this[k].arn}/*"]
  }
  statement {
    sid       = "Artifacts"
    actions   = ["s3:GetObject"]
    resources = ["${aws_s3_bucket.this["artifacts"].arn}/*"]
  }
  statement {
    sid = "GlueCatalog"
    actions = [
      "glue:GetDatabase", "glue:GetDatabases", "glue:GetTable", "glue:GetTables", "glue:CreateTable",
      "glue:UpdateTable", "glue:GetPartitions", "lakeformation:GetDataAccess",
    ]
    resources = concat(
      ["arn:${data.aws_partition.current.partition}:glue:${var.region}:${local.account_id}:catalog"],
      [for db in aws_glue_catalog_database.layer : db.arn],
      [for db in aws_glue_catalog_database.layer : "arn:${data.aws_partition.current.partition}:glue:${var.region}:${local.account_id}:table/${db.name}/*"],
    )
  }
  statement {
    sid       = "Kms"
    actions   = ["kms:Decrypt", "kms:Encrypt", "kms:GenerateDataKey"]
    resources = [aws_kms_key.data.arn]
  }
  statement {
    sid       = "EiaSecret"
    actions   = ["secretsmanager:GetSecretValue"]
    resources = ["arn:${data.aws_partition.current.partition}:secretsmanager:${var.region}:${local.account_id}:secret:${var.eia_api_key_secret_name}-*"]
  }
}

resource "aws_iam_role" "glue" {
  name               = "${local.name}-glue"
  assume_role_policy = data.aws_iam_policy_document.assume["glue"].json
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue.name
  policy_arn = "arn:${data.aws_partition.current.partition}:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy" "glue_data" {
  role   = aws_iam_role.glue.id
  policy = data.aws_iam_policy_document.data_plane.json
}

resource "aws_iam_role" "lambda" {
  name               = "${local.name}-lambda"
  assume_role_policy = data.aws_iam_policy_document.assume["lambda"].json
}

resource "aws_iam_role_policy_attachment" "lambda_basic" {
  role       = aws_iam_role.lambda.name
  policy_arn = "arn:${data.aws_partition.current.partition}:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy" "lambda_data" {
  role   = aws_iam_role.lambda.id
  policy = data.aws_iam_policy_document.data_plane.json
}

data "aws_iam_policy_document" "lambda_stream" {
  statement {
    actions = [
      "kinesis:GetRecords", "kinesis:GetShardIterator", "kinesis:DescribeStream",
      "kinesis:DescribeStreamSummary", "kinesis:ListShards", "kinesis:ListStreams",
    ]
    resources = [aws_kinesis_stream.telemetry.arn]
  }
}

resource "aws_iam_role_policy" "lambda_stream" {
  role   = aws_iam_role.lambda.id
  policy = data.aws_iam_policy_document.lambda_stream.json
}

resource "aws_iam_role" "lakeformation_access" {
  name               = "${local.name}-lf-access"
  assume_role_policy = data.aws_iam_policy_document.assume["lakeformation"].json
}

resource "aws_iam_role_policy" "lakeformation_access" {
  role   = aws_iam_role.lakeformation_access.id
  policy = data.aws_iam_policy_document.data_plane.json
}

data "aws_iam_policy_document" "states" {
  statement {
    actions   = ["lambda:InvokeFunction"]
    resources = [aws_lambda_function.step.arn]
  }
  statement {
    actions   = ["glue:StartJobRun", "glue:GetJobRun", "glue:GetJobRuns", "glue:BatchStopJobRun"]
    resources = [aws_glue_job.silver_demand_spark.arn]
  }
  statement {
    actions   = ["sns:Publish"]
    resources = [aws_sns_topic.alerts.arn]
  }
  statement {
    actions   = ["bedrock:StartIngestionJob"]
    resources = ["arn:${data.aws_partition.current.partition}:bedrock:${var.region}:${local.account_id}:knowledge-base/*"]
  }
}

resource "aws_iam_role" "states" {
  name               = "${local.name}-states"
  assume_role_policy = data.aws_iam_policy_document.assume["states"].json
}

resource "aws_iam_role_policy" "states" {
  role   = aws_iam_role.states.id
  policy = data.aws_iam_policy_document.states.json
}

resource "aws_iam_role" "scheduler" {
  name               = "${local.name}-scheduler"
  assume_role_policy = data.aws_iam_policy_document.assume["scheduler"].json
}

resource "aws_iam_role_policy" "scheduler" {
  role = aws_iam_role.scheduler.id
  policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [{ Effect = "Allow", Action = "states:StartExecution", Resource = aws_sfn_state_machine.pipeline.arn }]
  })
}
