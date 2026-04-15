resource "aws_sns_topic" "pipeline_alerts" {
  name = "mrisk-pipeline-alerts"

  tags = {
    Project     = var.project
    Environment = var.environment
  }
}

resource "aws_sns_topic_subscription" "email_alert" {
  topic_arn = aws_sns_topic.pipeline_alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}

resource "aws_cloudwatch_log_group" "pipeline_logs" {
  name              = "/mrisk/pipeline"
  retention_in_days = 7

  tags = {
    Project     = var.project
    Environment = var.environment
  }
}

resource "aws_cloudwatch_log_group" "lambda_logs" {
  name              = "/mrisk/lambda"
  retention_in_days = 7

  tags = {
    Project     = var.project
    Environment = var.environment
  }
}