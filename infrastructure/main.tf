provider "aws" {
  region = var.aws_region
}

# --------------------------------------------------------------------------------------------------
# S3 Buckets for Medallion Architecture
# --------------------------------------------------------------------------------------------------

resource "aws_s3_bucket" "bronze" {
  bucket = "${var.project_name}-bronze-${var.environment}"
}

resource "aws_s3_bucket" "silver" {
  bucket = "${var.project_name}-silver-${var.environment}"
}

resource "aws_s3_bucket" "gold" {
  bucket = "${var.project_name}-gold-${var.environment}"
}

resource "aws_s3_bucket" "glue_scripts" {
  bucket = "${var.project_name}-glue-scripts-${var.environment}"
}

resource "aws_s3_object" "etl_script" {
  bucket = aws_s3_bucket.glue_scripts.id
  key    = "etl.py"
  source = "../glue_jobs/etl.py"
  etag   = filemd5("../glue_jobs/etl.py")
}

# --------------------------------------------------------------------------------------------------
# IAM Roles
# --------------------------------------------------------------------------------------------------

data "aws_iam_policy_document" "glue_assume_role" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "glue_role" {
  name               = "${var.project_name}-glue-role-${var.environment}"
  assume_role_policy = data.aws_iam_policy_document.glue_assume_role.json
}

resource "aws_iam_role_policy_attachment" "glue_s3_access" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3FullAccess" # For simplicity, but should be scoped down
}

resource "aws_iam_role_policy_attachment" "glue_managed_policy" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_secretsmanager_secret" "db_credentials" {
  name        = "${var.project_name}-db-credentials-${var.environment}"
  description = "Placeholder for database credentials"
}

data "aws_iam_policy_document" "glue_extra_permissions" {
  statement {
    actions = [
      "secretsmanager:GetSecretValue"
    ]
    resources = [aws_secretsmanager_secret.db_credentials.arn]
  }
  statement {
    actions   = ["cloudwatch:PutMetricData"]
    resources = ["*"]
  }
}

resource "aws_iam_policy" "glue_extra_policy" {
  name   = "${var.project_name}-glue-extra-policy-${var.environment}"
  policy = data.aws_iam_policy_document.glue_extra_permissions.json
}

resource "aws_iam_role_policy_attachment" "glue_extra_attachment" {
  role       = aws_iam_role.glue_role.name
  policy_arn = aws_iam_policy.glue_extra_policy.arn
}

data "aws_iam_policy_document" "lambda_assume_role" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "lambda_role" {
  name               = "${var.project_name}-lambda-role-${var.environment}"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json
}

resource "aws_iam_role_policy_attachment" "lambda_glue_access" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = "arn:aws:iam::aws:policy/AWSGlueConsoleFullAccess" # For simplicity
}

# --------------------------------------------------------------------------------------------------
# AWS Glue
# --------------------------------------------------------------------------------------------------

resource "aws_glue_job" "fitness_etl" {
  name     = "${var.project_name}-etl-job-${var.environment}"
  role_arn = aws_iam_role.glue_role.arn
  command {
    script_location = "s3://${aws_s3_bucket.glue_scripts.bucket}/${aws_s3_object.etl_script.key}"
    python_version  = "3"
  }
  default_arguments = {
    "--SECRET_NAME" = aws_secretsmanager_secret.db_credentials.name
    "--AWS_REGION"  = var.aws_region
  }
  glue_version = "4.0"
}

# --------------------------------------------------------------------------------------------------
# AWS Lambda
# --------------------------------------------------------------------------------------------------

resource "aws_lambda_function" "start_glue_job" {
  filename         = "lambda_src.zip"
  function_name    = "${var.project_name}-start-glue-job-${var.environment}"
  role             = aws_iam_role.lambda_role.arn
  handler          = "main.handler"
  runtime          = "python3.9"
  source_code_hash = data.archive_file.lambda_zip.output_base64sha256

  environment {
    variables = {
      GLUE_JOB_NAME  = aws_glue_job.fitness_etl.name
      SILVER_S3_PATH = "s3://${aws_s3_bucket.silver.bucket}/"
    }
  }
}

data "archive_file" "lambda_zip" {
  type        = "zip"
  source_dir  = "${path.module}/lambda_src/start_glue_job"
  output_path = "${path.module}/lambda_src.zip"
}

# --------------------------------------------------------------------------------------------------
# S3 Event Trigger for Lambda
# --------------------------------------------------------------------------------------------------

resource "aws_lambda_permission" "allow_s3" {
  statement_id  = "AllowExecutionFromS3"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.start_glue_job.function_name
  principal     = "s3.amazonaws.com"
  source_arn    = aws_s3_bucket.bronze.arn
}

resource "aws_s3_bucket_notification" "bronze_notification" {
  bucket = aws_s3_bucket.bronze.id

  lambda_function {
    lambda_function_arn = aws_lambda_function.start_glue_job.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "uploads/" # Optional: only trigger for objects in a specific prefix
    filter_suffix       = ".csv"     # Optional: only trigger for .csv files
  }

  depends_on = [aws_lambda_permission.allow_s3]
}


# --------------------------------------------------------------------------------------------------
# SNS and CloudWatch Alarms for Monitoring
# --------------------------------------------------------------------------------------------------

resource "aws_sns_topic" "pipeline_notifications" {
  name = "${var.project_name}-pipeline-notifications-${var.environment}"
}

resource "aws_cloudwatch_metric_alarm" "glue_failure_alarm" {
  alarm_name          = "${var.project_name}-glue-failure-alarm"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = "1"
  metric_name         = "glue.driver.aggregate.failedStages"
  namespace           = "AWS/Glue"
  period              = "300"
  statistic           = "Sum"
  threshold           = "1"
  alarm_description   = "Alarm when the Glue ETL job fails"
  dimensions = {
    JobName = aws_glue_job.fitness_etl.name
    Type    = "gauge"
  }
  alarm_actions = [aws_sns_topic.pipeline_notifications.arn]
  ok_actions    = [aws_sns_topic.pipeline_notifications.arn]
}
