output "sns_topic_arn" {
  value = aws_sns_topic.pipeline_alerts.arn
}

output "pipeline_log_group" {
  value = aws_cloudwatch_log_group.pipeline_logs.name
}