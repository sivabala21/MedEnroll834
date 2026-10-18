# 07. AWS / S3 Setup

This chapter covers the **enterprise-target** AWS S3 setup (the current POC
runs on S3-compatible blob storage instead — see
[Chapter 4, section 6](04-modern-architecture.md#6-poc-vs-enterprise-storage)).
The Terraform and IAM policy below are real, committed infrastructure code
already in the repo (`04_infrastructure/01_terraform/`), even though the
runtime pipeline currently points at blob storage.

## Terraform-managed S3 bucket

> Source: [`04_infrastructure/01_terraform/main.tf`](https://github.com/yourusername/medenroll834/blob/main/04_infrastructure/01_terraform/main.tf)

```hcl
resource "aws_s3_bucket" "med834" {
  bucket = var.bucket_name
  tags = {
    Project            = "MED834ENRL"
    Environment        = "dev"
    DataClassification = "synthetic"
    ManagedBy          = "Terraform"
  }
}

resource "aws_s3_bucket_versioning" "med834" {
  bucket = aws_s3_bucket.med834.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "med834" {
  bucket = aws_s3_bucket.med834.id
  rule {
    apply_server_side_encryption_by_default { sse_algorithm = "AES256" }
  }
}

resource "aws_s3_bucket_public_access_block" "med834" {
  bucket                  = aws_s3_bucket.med834.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}
```

A bucket policy denies any non-TLS request, and ownership controls disable
ACL-based access entirely — standard hardening that applies automatically
once a deployment is on a standard AWS account; nothing further to
configure beyond attaching the Terraform-managed policy.


## Folder layout provisioned as code

Terraform pre-creates every prefix the pipeline expects, as zero-byte
placeholder objects (`aws_s3_object.folders`, `for_each` over a list):

```text
landing/834/
archive/834/
lakehouse/bronze/{edi_834_raw,edi_834_segments,ingestion_metadata}/
lakehouse/silver/{member,subscriber,employer_group,benefit_plan,enrollment,address,transaction,rejected_record}/
lakehouse/gold/{dim_member,dim_plan,dim_group,dim_date,dim_transaction_source,fact_enrollment}/
quarantine/834/{invalid_format,validation_failed,processing_failed}/
audit/{ingestion,processing,reconciliation,errors}/
```

## Least-privilege IAM policy

> Source: [`04_infrastructure/01_terraform/s3_pipeline_least_privilege_policy.json`](https://github.com/yourusername/medenroll834/blob/main/04_infrastructure/01_terraform/s3_pipeline_least_privilege_policy.json)

```json
{
  "Version": "2012-10-17",
  "Statement": [
    { "Sid": "ListTheBucket", "Effect": "Allow",
      "Action": "s3:ListBucket",
      "Resource": "arn:aws:s3:::med834enrl-s3" },
    { "Sid": "ReadWriteObjects", "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
      "Resource": "arn:aws:s3:::med834enrl-s3/*" }
  ]
}
```

Only `ListBucket` on the bucket ARN itself, and `Get`/`Put`/`Delete` scoped
to objects under it — no `s3:*`, no cross-bucket access.

## Applying the Terraform

```bash
cd 04_infrastructure/01_terraform
terraform init
terraform plan -var="aws_region=us-east-1" -var="bucket_name=med834enrl-s3"
terraform apply
```

## The S3 client interface (enterprise target)

> Source: [`src/medenroll834/s3/s3_utils.py`](https://github.com/yourusername/medenroll834/blob/main/src/medenroll834/s3/s3_utils.py)

Credentials resolve through boto3's default chain (env vars, shared config,
or IAM role) — in Airflow these come from the AWS connection exported as
environment variables (`AWS_REGION`, `AWS_ACCESS_KEY_ID`,
`AWS_SECRET_ACCESS_KEY` in `docker-compose.yaml`).

```python
def upload_834(local_path: str, bucket: str, s3_prefix: str = "landing/834/") -> str:
    """Upload one .834 file under the given prefix, preserving its filename."""
    name = Path(local_path).name
    key = f"{s3_prefix.rstrip('/')}/{name}"
    return upload_file(local_path, bucket, key)

def archive_834(bucket: str, key: str, archive_prefix: str = "archive/834/") -> str:
    """Move a processed object from landing to archive (copy + delete)."""
    ...
```

This is the exact function signature shape mirrored by the POC's
`src/medenroll834/storage/blob_storage.py` (`upload_834`, `archive_834`,
`list_landing_files`, `put_text`), which is why migrating the Airflow DAG's
`upload_to_blob`/`archive_blob` tasks to S3 is a drop-in swap — see the
"Enterprise migration note" in [Chapter 33](33-end-to-end-execution.md).

## AWS Cloud Shell backout

A standalone backout script exists for environments where S3 was
provisioned outside Terraform (e.g. via AWS CloudShell):

```bash
bash 04_infrastructure/03_aws_cloud_shell/aws_s3_backout.sh
```

Removes all folder placeholders and objects from the `med834enrl-s3` bucket.
