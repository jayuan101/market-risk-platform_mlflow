output "lambda_exec_role_arn" {
  value = aws_iam_role.lambda_exec.arn
}

output "sfn_exec_role_arn" {
  value = aws_iam_role.sfn_exec.arn
}