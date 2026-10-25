data "aws_caller_identity" "current" {}
data "aws_partition" "current" {}

locals {
  name       = "${var.name_prefix}-${var.environment}"
  account_id = data.aws_caller_identity.current.account_id

  # The same data contracts that drive the pipeline drive the catalog and governance.
  contract_files = fileset("${path.module}/../../contracts", "*.yaml")
  contracts      = { for f in local.contract_files : trimsuffix(f, ".yaml") => yamldecode(file("${path.module}/../../contracts/${f}")) }
  namespaces     = toset([for name, _ in local.contracts : split(".", name)[0]])

  glue_types = {
    string    = "string"
    double    = "double"
    long      = "bigint"
    int       = "int"
    boolean   = "boolean"
    date      = "date"
    timestamp = "timestamp"
  }

  warehouse = "s3://${aws_s3_bucket.this["warehouse"].bucket}/warehouse"
}

resource "aws_kms_key" "data" {
  description             = "${local.name} lakehouse encryption"
  enable_key_rotation     = true
  deletion_window_in_days = 30
}

resource "aws_kms_alias" "data" {
  name          = "alias/${local.name}-data"
  target_key_id = aws_kms_key.data.key_id
}
