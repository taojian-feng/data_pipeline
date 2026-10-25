# Step Functions: ingest -> silver -> gold -> knowledge layer -> Bedrock KB sync.
# Every task retries with backoff; any failure alerts and stops before gold is published
# (quality gates raise inside the task, so a failed gate fails the execution).

locals {
  lambda_task = {
    Type     = "Task"
    Resource = "arn:${data.aws_partition.current.partition}:states:::lambda:invoke"
    Retry = [{
      ErrorEquals     = ["Lambda.ServiceException", "Lambda.TooManyRequestsException", "Lambda.SdkClientException"]
      IntervalSeconds = 5
      BackoffRate     = 2
      MaxAttempts     = 4
    }]
    Catch      = [{ ErrorEquals = ["States.ALL"], ResultPath = "$.error", Next = "NotifyFailure" }]
    ResultPath = null
  }

  step = { for s in ["seed", "ingest_eia", "silver", "gold", "knowledge"] : s => merge(local.lambda_task, {
    Parameters = {
      FunctionName = aws_lambda_function.step.arn
      Payload = {
        step         = s
        "since.$"    = "$$.Execution.StartTime"
        no_telemetry = true
      }
    }
  }) }
}

resource "aws_sfn_state_machine" "pipeline" {
  name     = "${local.name}-pipeline"
  role_arn = aws_iam_role.states.arn
  type     = "STANDARD"

  logging_configuration {
    log_destination        = "${aws_cloudwatch_log_group.sfn.arn}:*"
    include_execution_data = false
    level                  = "ERROR"
  }

  tracing_configuration {
    enabled = true
  }

  definition = jsonencode({
    Comment = "gridlake medallion pipeline"
    StartAt = "SeedDimensions"
    States = {
      SeedDimensions = merge(local.step["seed"], { Next = "IngestEIA" })
      IngestEIA      = merge(local.step["ingest_eia"], { Next = "Backfill?" })
      "Backfill?" = {
        Type    = "Choice"
        Choices = [{ Variable = "$.backfill", IsPresent = true, Next = "SilverDemandSpark" }]
        Default = "Silver"
      }
      SilverDemandSpark = {
        Type       = "Task"
        Resource   = "arn:${data.aws_partition.current.partition}:states:::glue:startJobRun.sync"
        Parameters = { JobName = aws_glue_job.silver_demand_spark.name }
        ResultPath = null
        Catch      = [{ ErrorEquals = ["States.ALL"], ResultPath = "$.error", Next = "NotifyFailure" }]
        Next       = "Silver"
      }
      Silver    = merge(local.step["silver"], { Next = "Gold" })
      Gold      = merge(local.step["gold"], { Next = "Knowledge" })
      Knowledge = merge(local.step["knowledge"], { Next = var.bedrock_knowledge_base_id == "" ? "Done" : "SyncKnowledgeBase" })
      SyncKnowledgeBase = {
        Type     = "Task"
        Resource = "arn:${data.aws_partition.current.partition}:states:::aws-sdk:bedrockagent:startIngestionJob"
        Parameters = {
          KnowledgeBaseId = var.bedrock_knowledge_base_id
          DataSourceId    = var.bedrock_data_source_id
        }
        ResultPath = null
        Catch      = [{ ErrorEquals = ["States.ALL"], ResultPath = "$.error", Next = "NotifyFailure" }]
        Next       = "Done"
      }
      NotifyFailure = {
        Type     = "Task"
        Resource = "arn:${data.aws_partition.current.partition}:states:::sns:publish"
        Parameters = {
          TopicArn    = aws_sns_topic.alerts.arn
          "Message.$" = "States.Format('gridlake pipeline failed: {}', $.error.Cause)"
        }
        Next = "Failed"
      }
      Failed = { Type = "Fail", Error = "PipelineFailed" }
      Done   = { Type = "Succeed" }
    }
  })
}

resource "aws_cloudwatch_log_group" "sfn" {
  name              = "/aws/vendedlogs/states/${local.name}-pipeline"
  retention_in_days = 30
}

resource "aws_scheduler_schedule" "pipeline" {
  name                = "${local.name}-pipeline"
  schedule_expression = var.schedule_expression
  flexible_time_window {
    mode = "OFF"
  }
  target {
    arn      = aws_sfn_state_machine.pipeline.arn
    role_arn = aws_iam_role.scheduler.arn
    input    = jsonencode({})
  }
}
