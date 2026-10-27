variable "region" {
  type    = string
  default = "us-east-1"
}

variable "environment" {
  type    = string
  default = "dev"
  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "environment must be dev, staging or prod."
  }
}

variable "name_prefix" {
  type    = string
  default = "gridlake"
}

variable "use_s3_tables" {
  description = "Also provision an Amazon S3 Tables bucket (managed Iceberg) for gold data products."
  type        = bool
  default     = true
}

variable "schedule_expression" {
  description = "EventBridge Scheduler expression for the batch pipeline."
  type        = string
  default     = "rate(1 hour)"
}

variable "eia_api_key_secret_name" {
  description = "Secrets Manager secret holding the EIA API key."
  type        = string
  default     = "gridlake/eia-api-key"
}

variable "artifact_version" {
  description = "Version of the gridlake wheel uploaded to the artifacts bucket by CI."
  type        = string
  default     = "0.1.0"
}

variable "analyst_principal_arns" {
  description = "IAM principals granted public + internal data (Lake Formation tag-based access)."
  type        = list(string)
  default     = []
}

variable "reliability_principal_arns" {
  description = "IAM principals additionally granted confidential grid telemetry."
  type        = list(string)
  default     = []
}

variable "lakeformation_admin_arns" {
  description = "Extra Lake Formation administrators (e.g. your console role) besides the deploy role."
  type        = list(string)
  default     = []
}

variable "stream_reserved_concurrency" {
  description = "Reserved concurrency for the telemetry consumer (-1 = unreserved; new accounts only have 10 in total)."
  type        = number
  default     = -1
}

variable "alarm_email" {
  type    = string
  default = ""
}

variable "lambda_image_uri" {
  description = "ECR image (built from Dockerfile.lambda by CI) for the pipeline Lambdas."
  type        = string
  default     = ""
}

variable "bedrock_knowledge_base_id" {
  description = "Existing Amazon Bedrock Knowledge Base to re-sync after each run (optional)."
  type        = string
  default     = ""
}

variable "bedrock_data_source_id" {
  type    = string
  default = ""
}

variable "enable_analyst_agent" {
  description = "Create the grid analyst Bedrock Agent, its tools Lambda and an Athena workgroup."
  type        = bool
  default     = false
}

variable "analyst_agent_model" {
  description = "Foundation model or inference profile id for the Bedrock Agent (must be enabled in the account)."
  type        = string
  default     = "anthropic.claude-opus-5-5"
}

variable "analyst_agent_max_clearance" {
  description = "Highest classification Lake Formation lets the agent's tools read. Per-caller clearance narrows it further."
  type        = string
  default     = "internal"
  validation {
    condition     = contains(["public", "internal", "confidential"], var.analyst_agent_max_clearance)
    error_message = "analyst_agent_max_clearance must be public, internal or confidential."
  }
}
