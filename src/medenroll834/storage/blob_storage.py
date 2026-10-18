"""
src/medenroll834/storage/blob_storage.py
S3-compatible blob storage helpers for the POC landing/archive/report layer.
Mints short-lived S3 credentials (valid ~10 min) from the blob storage token
on each call, so there is nothing long-lived to leak.

Env:
  BLOB_STORAGE_TOKEN - the bucket token (from the blob storage provider console)

Layout: landing/834/, archive/834/, audit/reports/. In the enterprise target
architecture this same interface is backed by an AWS S3 bucket instead
(see src/medenroll834/s3/s3_utils.py).
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import List

import boto3
import requests

from src.medenroll834.common.logging import setup_logging

_LOGGER = setup_logging("blob_storage")

CREDENTIALS_URL = "https://blob.upstash.io/v1/credentials"


def _client_and_bucket():
    """Mint fresh short-lived creds and return (s3_client, bucket_name)."""
    token = os.environ["BLOB_STORAGE_TOKEN"]
    try:
        resp = requests.post(
            CREDENTIALS_URL, headers={"Authorization": f"Bearer {token}"}, timeout=30
        )
        resp.raise_for_status()
        creds = resp.json()
    except Exception:
        # Never log the token itself -- only that minting failed.
        _LOGGER.exception("BLOB | failed to mint short-lived credentials from %s", CREDENTIALS_URL)
        raise
    s3 = boto3.client(
        "s3",
        endpoint_url=creds["endpoint"],
        region_name="auto",
        aws_access_key_id=creds["accessKeyId"],
        aws_secret_access_key=creds["secretAccessKey"],
        aws_session_token=creds["sessionToken"],
    )
    return s3, creds["bucket"]


def upload_file(local_path: str, key: str) -> str:
    s3, bucket = _client_and_bucket()
    try:
        s3.upload_file(local_path, bucket, key)
    except Exception:
        _LOGGER.exception("BLOB | upload failed local_path=%s key=%s", local_path, key)
        raise
    _LOGGER.info("BLOB | uploaded %s -> blob://%s/%s", local_path, bucket, key)
    return f"blob://{bucket}/{key}"


def upload_834(local_path: str, s3_prefix: str = "landing/834/") -> str:
    name = Path(local_path).name
    key = f"{s3_prefix.rstrip('/')}/{name}"
    return upload_file(local_path, key)


def list_objects(prefix: str) -> List[str]:
    s3, bucket = _client_and_bucket()
    paginator = s3.get_paginator("list_objects_v2")
    keys: List[str] = []
    try:
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                if not obj["Key"].endswith("/"):
                    keys.append(obj["Key"])
    except Exception:
        _LOGGER.exception("BLOB | list_objects failed prefix=%s", prefix)
        raise
    _LOGGER.debug("BLOB | listed %d object(s) under blob://%s/%s", len(keys), bucket, prefix)
    return keys


def list_landing_files(s3_prefix: str = "landing/834/") -> List[str]:
    return [k for k in list_objects(s3_prefix) if k.endswith(".834")]


def download_file(key: str, local_path: str) -> str:
    s3, bucket = _client_and_bucket()
    Path(local_path).parent.mkdir(parents=True, exist_ok=True)
    try:
        s3.download_file(bucket, key, local_path)
    except Exception:
        _LOGGER.exception("BLOB | download failed key=%s -> %s", key, local_path)
        raise
    _LOGGER.debug("BLOB | downloaded blob://%s/%s -> %s", bucket, key, local_path)
    return local_path


def archive_834(key: str, archive_prefix: str = "archive/834/") -> str:
    s3, bucket = _client_and_bucket()
    name = Path(key).name
    dest = f"{archive_prefix.rstrip('/')}/{name}"
    try:
        s3.copy_object(Bucket=bucket, CopySource={"Bucket": bucket, "Key": key}, Key=dest)
        s3.delete_object(Bucket=bucket, Key=key)
    except Exception:
        _LOGGER.exception("BLOB | archive failed key=%s -> %s", key, dest)
        raise
    _LOGGER.info("BLOB | archived blob://%s/%s -> blob://%s/%s", bucket, key, bucket, dest)
    return dest


def put_text(key: str, text: str, content_type: str = "text/plain") -> str:
    s3, bucket = _client_and_bucket()
    try:
        s3.put_object(Bucket=bucket, Key=key, Body=text.encode("utf-8"), ContentType=content_type)
    except Exception:
        _LOGGER.exception("BLOB | put_text failed key=%s", key)
        raise
    _LOGGER.info("BLOB | wrote text object blob://%s/%s (%d bytes)", bucket, key, len(text))
    return f"blob://{bucket}/{key}"
