resource = "aws_lambda_function" "my_lambda_function" {
  function_name = "mrisk_pipeline-lambda"
  role          = var.lambda_role_arn
  handler       = "pipeline_handler.lambda_handler"
  runtime       = "python3.11"
  timeout       = 900

  filename         = "${path.module}/mrisk_pipeline.zip"}
  source_code_hash = filebase64sha256("${path.module}/mrisk_pipeline.zip")


    environment {
        variables = {
        AWS_DEFAULT_REGION = "us-east-1"
        PROJECT_NAME = var.project
        ENVIRONMENT = var.environment
        }
    }




    tags = {
        Project     = var.project
        Environment = var.environment
    }