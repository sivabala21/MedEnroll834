"""
src/medenroll834/s3/s3_utils.py
Thin S3 helpers for the 834 landing / archive / audit layout.

Credentials are resolved by boto3's default chain (env vars, shared config,
or IAM role). In Airflow these come from the AWS connection exported as env.
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import List

import boto3

from src.medenroll834.common.logging import setup_logging

_LOGGER = setup_logging("s3_utils")


def _client():
    return boto3.client("s3", region_name=os.environ.get("AWS_REGION", "us-east-1"))


def upload_file(local_path: str, bucket: str, key: str) -> str:
    """Upload a single file to s3://bucket/key and return the s3 URI."""
    try:
        _client().upload_file(local_path, bucket, key)
    except Exception:
        _LOGGER.exception("S3 | upload failed local_path=%s bucket=%s key=%s", local_path, bucket, key)
        raise
    _LOGGER.info("S3 | uploaded %s -> s3://%s/%s", local_path, bucket, key)
    return f"s3://{bucket}/{key}"


def upload_834(local_path: str, bucket: str, s3_prefix: str = "landing/834/") -> str:
    """Upload one .834 file under the given prefix, preserving its filename."""
    name = Path(local_path).name
    key = f"{s3_prefix.rstrip('/')}/{name}"
    return upload_file(local_path, bucket, key)


def list_objects(bucket: str, prefix: str) -> List[str]:
    """List object keys under a prefix (excludes folder placeholder keys)."""
    paginator = _client().get_paginator("list_objects_v2")
    keys: List[str] = []
    try:
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                if not obj["Key"].endswith("/"):
                    keys.append(obj["Key"])
    except Exception:
        _LOGGER.exception("S3 | list_objects failed bucket=%s prefix=%s", bucket, prefix)
        raise
    _LOGGER.debug("S3 | listed %d object(s) under s3://%s/%s", len(keys), bucket, prefix)
    return keys


def list_landing_files(bucket: str, s3_prefix: str = "landing/834/") -> List[str]:
    return [k for k in list_objects(bucket, s3_prefix) if k.endswith(".834")]


def archive_834(bucket: str, key: str, archive_prefix: str = "archive/834/") -> str:
    """Move a processed object from landing to archive (copy + delete)."""
    name = Path(key).name
    dest = f"{archive_prefix.rstrip('/')}/{name}"
    s3 = _client()
    try:
        s3.copy_object(Bucket=bucket, CopySource={"Bucket": bucket, "Key": key}, Key=dest)
        s3.delete_object(Bucket=bucket, Key=key)
    except Exception:
        _LOGGER.exception("S3 | archive failed bucket=%s key=%s -> %s", bucket, key, dest)
        raise
    _LOGGER.info("S3 | archived s3://%s/%s -> s3://%s/%s", bucket, key, bucket, dest)
    return dest


def put_text(bucket: str, key: str, text: str, content_type: str = "text/plain") -> str:
    """Write a string object (used for audit logs and reports)."""
    try:
        _client().put_object(
            Bucket=bucket, Key=key, Body=text.encode("utf-8"), ContentType=content_type
        )
    except Exception:
        _LOGGER.exception("S3 | put_text failed bucket=%s key=%s", bucket, key)
        raise
    _LOGGER.info("S3 | wrote text object s3://%s/%s (%d bytes)", bucket, key, len(text))
    return f"s3://{bucket}/{key}"
