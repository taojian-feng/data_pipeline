output "warehouse" {
  value = local.warehouse
}

output "state_machine_arn" {
  value = aws_sfn_state_machine.pipeline.arn
}

output "telemetry_stream" {
  value = aws_kinesis_stream.telemetry.name
}

output "ecr_repository_url" {
  value = aws_ecr_repository.pipeline.repository_url
}

output "s3_tables_bucket_arn" {
  value = var.use_s3_tables ? aws_s3tables_table_bucket.products[0].arn : null
}

output "knowledge_base_source" {
  description = "S3 prefix to register as the Amazon Bedrock Knowledge Base data source."
  value       = "s3://${aws_s3_bucket.this["knowledge"].bucket}/kb/"
}

output "governed_tables" {
  value = sort(keys(local.contracts))
}
