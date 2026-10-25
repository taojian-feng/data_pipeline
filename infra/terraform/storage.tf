# landing: raw API extracts (replayable), warehouse: Iceberg data + metadata,
# artifacts: job code + wheel, knowledge: Bedrock Knowledge Base data source.
resource "aws_s3_bucket" "this" {
  for_each = toset(["landing", "warehouse", "artifacts", "knowledge"])
  bucket   = "${local.name}-${each.key}-${local.account_id}"
}

resource "aws_s3_bucket_public_access_block" "this" {
  for_each                = aws_s3_bucket.this
  bucket                  = each.value.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "this" {
  for_each = aws_s3_bucket.this
  bucket   = each.value.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = aws_kms_key.data.arn
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_versioning" "this" {
  for_each = aws_s3_bucket.this
  bucket   = each.value.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "landing" {
  bucket = aws_s3_bucket.this["landing"].id
  rule {
    id     = "tier-raw-extracts"
    status = "Enabled"
    filter {}
    transition {
      days          = 30
      storage_class = "INTELLIGENT_TIERING"
    }
    noncurrent_version_expiration {
      noncurrent_days = 90
    }
  }
}

resource "aws_s3_bucket_policy" "tls_only" {
  for_each = aws_s3_bucket.this
  bucket   = each.value.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "DenyInsecureTransport"
      Effect    = "Deny"
      Principal = "*"
      Action    = "s3:*"
      Resource  = [each.value.arn, "${each.value.arn}/*"]
      Condition = { Bool = { "aws:SecureTransport" = "false" } }
    }]
  })
}

# Amazon S3 Tables: fully managed Iceberg (automatic compaction + snapshot expiry)
resource "aws_s3tables_table_bucket" "products" {
  count = var.use_s3_tables ? 1 : 0
  name  = "${local.name}-products"
}

resource "aws_s3tables_namespace" "gold" {
  count            = var.use_s3_tables ? 1 : 0
  namespace        = "gold"
  table_bucket_arn = aws_s3tables_table_bucket.products[0].arn
}
