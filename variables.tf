variable "lambda_role_arn" {
  type        = string
  description = "IAM role ARN for Lambda execution"
}

variable "project" {
  type    = string
  default = "market-risk-platform"
}

variable "environment" {
  type    = string
  default = "dev"
}