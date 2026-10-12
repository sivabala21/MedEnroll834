# 29. Security

## Credential handling

### Short-lived, minted-on-demand blob storage credentials

> Source: [`src/medenroll834/storage/blob_storage.py`](https://github.com/yourusername/medenroll834/blob/main/src/medenroll834/storage/blob_storage.py)

```python
def _client_and_bucket():
    """Mint fresh short-lived creds and return (s3_client, bucket_name)."""
    token = os.environ["BLOB_STORAGE_TOKEN"]
    creds = requests.post(
        CREDENTIALS_URL, headers={"Authorization": f"Bearer {token}"}, timeout=30
    ).json()
    s3 = boto3.client("s3", endpoint_url=creds["endpoint"], region_name="auto",
        aws_access_key_id=creds["accessKeyId"],
        aws_secret_access_key=creds["secretAccessKey"],
        aws_session_token=creds["sessionToken"])
    return s3, creds["bucket"]
```

Every call mints a **fresh set of S3-compatible credentials, valid ~10
minutes**, from a single long-lived token — there is no long-lived AWS
access key sitting in memory or logs for an attacker to exfiltrate and
reuse later. The POC's security model is explicitly: "one secret
(`BLOB_STORAGE_TOKEN`) that can only mint short windows of access," not
"permanent keys."

### AWS enterprise-target credentials

```python
def _client():
    return boto3.client("s3", region_name=os.environ.get("AWS_REGION", "us-east-1"))
```

Resolved through boto3's default credential chain — environment variables,
shared config, or (in a real deployment) an IAM role — never hardcoded.

### Secrets live only in `.env` (git-ignored)

```text
# .gitignore
.env
```

All secrets (`BLOB_STORAGE_TOKEN`, `OLTP_DATABASE_URL`, `DATABRICKS_TOKEN`,
AWS keys) live in `.env`, which is git-ignored; `.env.example` ships as the
template with **no real values**. `docker-compose.yaml` interpolates these
as `${VAR}` references rather than inlining them.

## IAM least privilege (S3)

> Source: [`04_infrastructure/01_terraform/s3_pipeline_least_privilege_policy.json`](https://github.com/yourusername/medenroll834/blob/main/04_infrastructure/01_terraform/s3_pipeline_least_privilege_policy.json)

```json
{
  "Statement": [
    { "Sid": "ListTheBucket", "Effect": "Allow", "Action": "s3:ListBucket",
      "Resource": "arn:aws:s3:::med834enrl-s3" },
    { "Sid": "ReadWriteObjects", "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
      "Resource": "arn:aws:s3:::med834enrl-s3/*" }
  ]
}
```

No `s3:*`, no cross-bucket access, no bucket-policy modification rights.

## S3 bucket hardening (Terraform)

```hcl
resource "aws_s3_bucket_public_access_block" "med834" {
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "med834" {
  rule { apply_server_side_encryption_by_default { sse_algorithm = "AES256" } }
}
```

Plus a bucket policy that **denies any request over plain HTTP**
(`aws:SecureTransport == false`), and `BucketOwnerEnforced` ownership
controls that disable ACL-based access entirely.

## Database connection security

`OLTP_DATABASE_URL` requires `?sslmode=require` for the managed Postgres
provider used in this POC — enforced by convention/documentation (see the
[PostgreSQL Setup](08-postgresql-setup.md) chapter) rather than
programmatically, since `psycopg2.connect()` accepts the sslmode as part
of the connection string itself.

## Transactional integrity as a security property

Both the OLTP schema deployment and the OLTP data load wrap all statements
in a single transaction with rollback on any exception (see
[Chapter 23](23-error-handling-retry.md)) — this prevents a failed deploy
or load from leaving the database in a state where constraints/FKs are
partially applied, which could otherwise be an integrity/availability risk.

## PHI/PII posture

This project processes **zero production member data** — every 834 file
is synthetically generated (see [Chapter 10](10-synthetic-data-generation.md)).
That said, the schema and pipeline are structured as if the data were real
PHI/PII, per [`SECURITY.md`](https://github.com/yourusername/medenroll834/blob/main/SECURITY.md):

- Compliance with HIPAA regulations is a stated design goal for any
  production deployment of this architecture.
- Data encryption at rest (S3 SSE, Delta/Parquet on encrypted storage) and
  in transit (`sslmode=require`, HTTPS-only S3 policy) are both already
  enforced at the infrastructure level documented above.
- Access logging/auditing is intentionally left as a
  [future improvement](38-future-improvements.md) — out of scope for this
  POC's synthetic-data threat model.

## CI security scanning

`.github/workflows/ci.yml` runs a dedicated `security` job on every push/PR:

```yaml
security:
  steps:
    - run: pip install bandit safety
    - name: Run Bandit
      run: bandit -r src/ -f json -o bandit-report.json
    - name: Run Safety
      run: safety check --json > safety-report.json
```

`bandit` statically scans `src/` for common Python security anti-patterns
(e.g. `eval`, hardcoded secrets, insecure deserialization); `safety` checks
installed dependencies against the PyUp/OSV known-vulnerability database.
See [Chapter 31](31-ci-cd.md) for the full CI pipeline.
