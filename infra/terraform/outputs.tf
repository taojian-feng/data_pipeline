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

output "analyst_agent_id" {
  description = "Bedrock Agent id for the grid analyst (null unless enable_analyst_agent)."
  value       = var.enable_analyst_agent ? aws_bedrockagent_agent.analyst[0].agent_id : null
}

output "analyst_athena_workgroup" {
  value = var.enable_analyst_agent ? aws_athena_workgroup.analyst[0].name : null
}

output "reliability_incidents_uri" {
  description = "Where the data reliability agent writes drafts (set GRIDLAKE_INCIDENTS_S3_URI to this to review them)."
  value       = var.enable_reliability_agent ? "s3://${aws_s3_bucket.this["artifacts"].bucket}/incidents/" : null
}

output "grid_briefs_uri" {
  description = "Where the grid anomaly watcher writes daily briefs (null unless enable_anomaly_watcher)."
  value       = var.enable_anomaly_watcher ? "s3://${aws_s3_bucket.this["artifacts"].bucket}/briefs/" : null
}

output "grid_brief_topic_arn" {
  value = var.enable_anomaly_watcher ? aws_sns_topic.grid_brief[0].arn : null
}
