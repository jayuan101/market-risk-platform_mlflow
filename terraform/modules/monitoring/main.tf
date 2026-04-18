resource "aws_sns_topic" "pipeline_alerts" {
  name = "${var.project}-${var.environment}-alerts"

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
  name              = "/aws/lambda/mrisk-pipeline-lambda"
  retention_in_days = 14

  tags = {
    Project     = var.project
    Environment = var.environment
  }
}
