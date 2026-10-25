# Substation telemetry: devices -> Kinesis Data Streams -> Lambda consumer -> Iceberg bronze.
resource "aws_kinesis_stream" "telemetry" {
  name             = "${local.name}-grid-telemetry"
  retention_period = 168 # 7 days of replay for reprocessing
  encryption_type  = "KMS"
  kms_key_id       = aws_kms_key.data.arn

  stream_mode_details {
    stream_mode = "ON_DEMAND"
  }
}

resource "aws_sqs_queue" "telemetry_dlq" {
  name                      = "${local.name}-telemetry-dlq"
  message_retention_seconds = 1209600
  kms_master_key_id         = aws_kms_key.data.arn
}

resource "aws_iam_role_policy" "lambda_dlq" {
  role = aws_iam_role.lambda.id
  policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [{ Effect = "Allow", Action = "sqs:SendMessage", Resource = aws_sqs_queue.telemetry_dlq.arn }]
  })
}

resource "aws_lambda_event_source_mapping" "telemetry" {
  event_source_arn                   = aws_kinesis_stream.telemetry.arn
  function_name                      = aws_lambda_function.stream.arn
  starting_position                  = "TRIM_HORIZON"
  batch_size                         = 5000
  maximum_batching_window_in_seconds = 30
  parallelization_factor             = 1 # keep per-shard ordering for the watermark
  bisect_batch_on_function_error     = true
  maximum_retry_attempts             = 10
  maximum_record_age_in_seconds      = 86400

  destination_config {
    on_failure {
      destination_arn = aws_sqs_queue.telemetry_dlq.arn
    }
  }
}
