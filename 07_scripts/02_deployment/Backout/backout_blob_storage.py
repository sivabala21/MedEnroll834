#!/usr/bin/env python3
"""
Backout (rollback) the MedEnroll834 blob storage content.

Removes all objects under landing/834/, archive/834/, failed/834/, and
audit/reports/ - the prefixes written by upload_to_blob / archive_blob /
quarantine_failed_files / generate_report in
05_orchestration/airflow/01_dags/med834_e2e_pipeline.py.

The bucket itself is managed by the blob storage provider (not created/
dropped by this repo), so this only clears its contents.

Usage:
    python 07_scripts/02_deployment/backout_blob_storage.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.medenroll834.storage import blob_storage

PREFIXES = ["landing/834/", "archive/834/", "failed/834/", "audit/reports/"]


def main() -> int:
    total = 0
    for prefix in PREFIXES:
        keys = blob_storage.list_objects(prefix)
        if keys:
            s3, bucket = blob_storage._client_and_bucket()
            for key in keys:
                s3.delete_object(Bucket=bucket, Key=key)
                total += 1
        print(f"  OK: removed {len(keys)} object(s) under {prefix}")

    print(f"\nAll MedEnroll834 objects removed from blob storage ({total} total).")
    print("Re-run the pipeline DAG to repopulate.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

