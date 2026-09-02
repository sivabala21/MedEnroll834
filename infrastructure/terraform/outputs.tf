output "s3_bucket_name" {
  description = "MED834 S3 bucket name"
  value       = aws_s3_bucket.med834.bucket
}

output "s3_bucket_arn" {
  description = "MED834 S3 bucket ARN"
  value       = aws_s3_bucket.med834.arn
}

output "s3_bucket_region" {
  description = "AWS region hosting the MED834 S3 bucket"
  value       = aws_s3_bucket.med834.region
}
