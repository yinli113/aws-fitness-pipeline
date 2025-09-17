output "bronze_bucket_name" {
  description = "The name of the Bronze S3 bucket."
  value       = aws_s3_bucket.bronze.bucket
}

output "silver_bucket_name" {
  description = "The name of the Silver S3 bucket."
  value       = aws_s3_bucket.silver.bucket
}

output "gold_bucket_name" {
  description = "The name of the Gold S3 bucket."
  value       = aws_s3_bucket.gold.bucket
}

output "glue_job_name" {
  description = "The name of the AWS Glue job."
  value       = aws_glue_job.fitness_etl.name
}

output "lambda_function_name" {
  description = "The name of the Lambda function."
  value       = aws_lambda_function.start_glue_job.function_name
}

output "sns_topic_arn" {
  description = "The ARN of the SNS topic for notifications."
  value       = aws_sns_topic.pipeline_notifications.arn
}
