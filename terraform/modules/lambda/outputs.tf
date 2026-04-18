output "function_arn" {
  value = aws_lambda_function.pipeline.arn
}

output "function_name" {
  value = aws_lambda_function.pipeline.function_name
}
