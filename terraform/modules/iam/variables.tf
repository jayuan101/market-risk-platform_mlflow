variable "bucket_arn" {
  description = "Main project S3 bucket ARN"
  type        = string
}

variable "project" {
  type    = string
  default = "market-risk-platform"
}

variable "environment" {
  type    = string
  default = "dev"
}