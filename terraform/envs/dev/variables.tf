variable "aws_region" {
  type    = string
  default = "us-east-1"
}

variable "project" {
  type    = string
  default = "market-risk-platform"
}

variable "environment" {
  type    = string
  default = "dev"
}

variable "bucket_name" {
  type        = string
  description = "Main S3 bucket name — must be globally unique"
}

variable "alert_email" {
  type        = string
  description = "Email for pipeline alerts"
}