output "bucket_name" {
  value = module.s3.bucket_id
}

output "bucket_arn" {
  value = module.s3.bucket_arn
}

output "lambda_exec_role_arn" {
  value = module.iam.lambda_exec_role_arn
}

output "sfn_exec_role_arn" {
  value = module.iam.sfn_exec_role_arn
}

output "sns_topic_arn" {
  value = module.monitoring.sns_topic_arn
}

output "pipeline_log_group" {
  value = module.monitoring.pipeline_log_group
}

output "lambda_function_arn" {
  value = module.lambda.function_arn
}

output "lambda_function_name" {
  value = module.lambda.function_name
}