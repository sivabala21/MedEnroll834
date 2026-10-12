"""Verify the S3-only IAM keys can access the pipeline bucket."""
import os
import sys

import boto3
from botocore.exceptions import ClientError

BUCKET = os.environ.get("S3_BUCKET", "med834enrl-s3")


def main():
    s3 = boto3.client(
        "s3",
        region_name=os.environ.get("AWS_REGION", "us-east-1"),
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    )
    # Who am I (STS is allowed for any valid key)
    try:
        ident = boto3.client(
            "sts",
            aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
            aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        ).get_caller_identity()
        print("Identity ARN:", ident["Arn"])
        print("Account:", ident["Account"])
    except ClientError as e:
        print("STS check failed (not fatal):", e)

    # List the bucket (ListBucket permission)
    try:
        resp = s3.list_objects_v2(Bucket=BUCKET, MaxKeys=5)
        print(f"ListBucket OK on {BUCKET}. KeyCount={resp.get('KeyCount', 0)}")
    except ClientError as e:
        print(f"ListBucket FAILED on {BUCKET}: {e.response['Error']['Code']}")
        return 1

    # Round-trip a tiny test object (PutObject + GetObject + DeleteObject)
    key = "audit/connectivity_check.txt"
    try:
        s3.put_object(Bucket=BUCKET, Key=key, Body=b"med834 connectivity check")
        s3.get_object(Bucket=BUCKET, Key=key)
        s3.delete_object(Bucket=BUCKET, Key=key)
        print("Put/Get/Delete OK on", key)
    except ClientError as e:
        print(f"Object round-trip FAILED: {e.response['Error']['Code']}")
        return 1

    print("\nALL S3 CHECKS PASSED - credentials work for the pipeline.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
