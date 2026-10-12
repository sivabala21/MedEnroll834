"""Test blob storage: mint short-lived S3 creds and do a round-trip."""
import os
import boto3
import requests

TOKEN = os.environ["BLOB_STORAGE_TOKEN"]

creds = requests.post(
    "https://blob.upstash.io/v1/credentials",
    headers={"Authorization": f"Bearer {TOKEN}"},
).json()
print("endpoint:", creds.get("endpoint"))
print("bucket:", creds.get("bucket"))
print("expiresAt:", creds.get("expiresAt"))

s3 = boto3.client(
    "s3",
    endpoint_url=creds["endpoint"],
    region_name="auto",
    aws_access_key_id=creds["accessKeyId"],
    aws_secret_access_key=creds["secretAccessKey"],
    aws_session_token=creds["sessionToken"],
)

# write a tiny test object
s3.put_object(Bucket=creds["bucket"], Key="connectivity_check.txt",
              Body=b"blob storage connectivity ok")
listing = s3.list_objects_v2(Bucket=creds["bucket"])
print("objects:", [item["Key"] for item in listing.get("Contents", [])])
s3.delete_object(Bucket=creds["bucket"], Key="connectivity_check.txt")
print("BLOB_STORAGE_OK")

