variable "aws_region" {
  description = "AWS region"
  type        = string
}

variable "bucket_name" {
  description = "S3 bucket for the MED834 enrollment pipeline"
  type        = string
  default     = "med834enrl-s3"
}