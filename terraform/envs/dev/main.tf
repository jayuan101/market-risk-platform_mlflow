terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  required_version = ">= 1.5.0"

  # Local state for now — safe for dev learning
  backend "local" {}
}

provider "aws" {
  region  = var.aws_region
  profile = "mrisk-dev"
}

module "s3" {
  source      = "../../modules/s3"
  bucket_name = var.bucket_name
  environment = var.environment
  project     = var.project
}

module "iam" {
  source      = "../../modules/iam"
  bucket_arn  = module.s3.bucket_arn
  environment = var.environment
  project     = var.project
}

module "monitoring" {
  source      = "../../modules/monitoring"
  environment = var.environment
  project     = var.project
  alert_email = var.alert_email
}