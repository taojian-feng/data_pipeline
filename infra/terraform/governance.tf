# Lake Formation tag-based access control (LF-TBAC). Each table is tagged with the
# classification from its data contract; principals are granted tag expressions,
# so new tables are governed automatically, with no per-table grants.

# Lake Formation needs the IAM role behind the deploy session, not the STS session ARN.
data "aws_iam_session_context" "current" {
  arn = data.aws_caller_identity.current.arn
}

# NOTE: this resource owns the account's Lake Formation admin list for the region.
resource "aws_lakeformation_data_lake_settings" "this" {
  admins = distinct(concat([data.aws_iam_session_context.current.issuer_arn], var.lakeformation_admin_arns))
}

resource "aws_lakeformation_resource" "warehouse" {
  arn      = aws_s3_bucket.this["warehouse"].arn
  role_arn = aws_iam_role.lakeformation_access.arn
}

resource "aws_lakeformation_lf_tag" "classification" {
  key    = "classification"
  values = ["public", "internal", "confidential"]
}

resource "aws_lakeformation_lf_tag" "layer" {
  key    = "layer"
  values = ["bronze", "silver", "gold", "ops"]
}

resource "aws_lakeformation_resource_lf_tags" "table" {
  for_each = local.contracts

  table {
    database_name = aws_glue_catalog_table.contract[each.key].database_name
    name          = aws_glue_catalog_table.contract[each.key].name
  }

  lf_tag {
    key   = aws_lakeformation_lf_tag.classification.key
    value = each.value.classification
  }

  lf_tag {
    key   = aws_lakeformation_lf_tag.layer.key
    value = each.value.layer
  }
}

# Pipeline roles: read/write everything they produce.
resource "aws_lakeformation_permissions" "pipeline" {
  for_each    = { glue = aws_iam_role.glue.arn, lambda = aws_iam_role.lambda.arn }
  principal   = each.value
  permissions = ["SELECT", "INSERT", "DELETE", "DESCRIBE", "ALTER"]

  lf_tag_policy {
    resource_type = "TABLE"
    expression {
      key    = aws_lakeformation_lf_tag.layer.key
      values = aws_lakeformation_lf_tag.layer.values
    }
  }
}

# Analysts and BI: public + internal, gold and silver only.
resource "aws_lakeformation_permissions" "analyst" {
  for_each    = toset(var.analyst_principal_arns)
  principal   = each.value
  permissions = ["SELECT", "DESCRIBE"]

  lf_tag_policy {
    resource_type = "TABLE"
    expression {
      key    = aws_lakeformation_lf_tag.classification.key
      values = ["public", "internal"]
    }
    expression {
      key    = aws_lakeformation_lf_tag.layer.key
      values = ["silver", "gold"]
    }
  }
}

# Reliability engineering: adds confidential telemetry products.
resource "aws_lakeformation_permissions" "reliability" {
  for_each    = toset(var.reliability_principal_arns)
  principal   = each.value
  permissions = ["SELECT", "DESCRIBE"]

  lf_tag_policy {
    resource_type = "TABLE"
    expression {
      key    = aws_lakeformation_lf_tag.classification.key
      values = ["public", "internal", "confidential"]
    }
    expression {
      key    = aws_lakeformation_lf_tag.layer.key
      values = ["silver", "gold"]
    }
  }
}
