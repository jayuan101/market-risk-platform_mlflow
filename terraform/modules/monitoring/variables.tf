variable "project" {
  type    = string
  default = "market-risk-platform"
}

variable "environment" {
  type    = string
  default = "dev"
}

variable "alert_email" {
  description = "Email to receive pipeline alerts"
  type        = string
}