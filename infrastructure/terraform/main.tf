resource "aws_s3_bucket" "med834" {
  bucket = var.bucket_name

  tags = {
    Project            = "MED834ENRL"
    Environment        = "dev"
    DataClassification = "synthetic"
    ManagedBy          = "Terraform"
  }
}

resource "aws_s3_bucket_ownership_controls" "med834" {
  bucket = aws_s3_bucket.med834.id

  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

resource "aws_s3_bucket_versioning" "med834" {
  bucket = aws_s3_bucket.med834.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "med834" {
  bucket = aws_s3_bucket.med834.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "med834" {
  bucket = aws_s3_bucket.med834.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

data "aws_iam_policy_document" "med834" {
  statement {
    sid    = "DenyInsecureTransport"
    effect = "Deny"

    principals {
      type        = "*"
      identifiers = ["*"]
    }

    actions = ["s3:*"]

    resources = [
      aws_s3_bucket.med834.arn,
      "${aws_s3_bucket.med834.arn}/*",
    ]

    condition {
      test     = "Bool"
      variable = "aws:SecureTransport"
      values   = ["false"]
    }
  }
}

resource "aws_s3_bucket_policy" "med834" {
  bucket = aws_s3_bucket.med834.id
  policy = data.aws_iam_policy_document.med834.json
}

locals {
  folders = [
    "landing/834/",
    "archive/834/",

    "lakehouse/bronze/edi_834_raw/",
    "lakehouse/bronze/edi_834_segments/",
    "lakehouse/bronze/ingestion_metadata/",

    "lakehouse/silver/member/",
    "lakehouse/silver/subscriber/",
    "lakehouse/silver/employer_group/",
    "lakehouse/silver/benefit_plan/",
    "lakehouse/silver/enrollment/",
    "lakehouse/silver/address/",
    "lakehouse/silver/transaction/",
    "lakehouse/silver/rejected_record/",

    "lakehouse/gold/dim_member/",
    "lakehouse/gold/dim_plan/",
    "lakehouse/gold/dim_group/",
    "lakehouse/gold/dim_date/",
    "lakehouse/gold/dim_transaction_source/",
    "lakehouse/gold/fact_enrollment/",

    "quarantine/834/invalid_format/",
    "quarantine/834/validation_failed/",
    "quarantine/834/processing_failed/",

    "audit/ingestion/",
    "audit/processing/",
    "audit/reconciliation/",
    "audit/errors/"
  ]
}

resource "aws_s3_object" "folders" {
  for_each = toset(local.folders)

  bucket = aws_s3_bucket.med834.id
  key    = each.value
}
