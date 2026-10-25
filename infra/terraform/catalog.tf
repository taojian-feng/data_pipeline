# Glue Data Catalog databases per layer, and one Iceberg table per data contract.
resource "aws_glue_catalog_database" "layer" {
  for_each     = local.namespaces
  name         = each.key
  description  = "gridlake ${each.key} layer"
  location_uri = "${local.warehouse}/${each.key}.db"
}

resource "aws_glue_catalog_table" "contract" {
  for_each      = local.contracts
  database_name = aws_glue_catalog_database.layer[split(".", each.key)[0]].name
  name          = split(".", each.key)[1]
  description   = trimspace(lookup(each.value, "description", ""))
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    "gridlake.owner"            = each.value.owner
    "gridlake.classification"   = each.value.classification
    "gridlake.contract-version" = tostring(each.value.version)
  }

  open_table_format_input {
    iceberg_input {
      metadata_operation = "CREATE"
      version            = "2"
    }
  }

  storage_descriptor {
    location = "${local.warehouse}/${split(".", each.key)[0]}.db/${split(".", each.key)[1]}"

    dynamic "columns" {
      for_each = each.value.columns
      content {
        name    = columns.value.name
        type    = local.glue_types[columns.value.type]
        comment = lookup(columns.value, "description", null)
      }
    }
  }

  # The pipeline evolves partition specs (metadata-only) from the same contract on first
  # load; Iceberg owns the schema from then on.
  lifecycle {
    ignore_changes = [storage_descriptor, parameters, open_table_format_input]
  }
}
