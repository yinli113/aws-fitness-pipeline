import os
import boto3
import logging
import urllib.parse

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
glue_client = boto3.client("glue")

def basic_schema_check(bucket, key):
    """
    Placeholder for a basic schema check.
    For example, check file extension, name, or size.
    A more advanced check could involve reading the first few lines.
    """
    logger.info(f"Performing basic schema check for s3://{bucket}/{key}")
    if not key.endswith('.csv'):
        logger.warning("File is not a .csv, skipping trigger.")
        return False
    # Add more checks here if needed
    logger.info("Schema check passed.")
    return True

def handler(event, context):
    """
    Lambda function handler triggered by S3 PUT events.
    It performs a basic schema check and starts the AWS Glue ETL job.
    """
    # Get environment variables
    job_name = os.environ.get("GLUE_JOB_NAME")
    silver_s3_path = os.environ.get("SILVER_S3_PATH")

    if not job_name or not silver_s3_path:
        error_msg = "GLUE_JOB_NAME or SILVER_S3_PATH environment variables not set."
        logger.error(error_msg)
        raise ValueError(error_msg)

    # Get bucket and key from the S3 event
    try:
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
        bronze_s3_path = f"s3://{bucket}/{key}"
    except (KeyError, IndexError) as e:
        logger.error(f"Could not extract bucket/key from S3 event: {event}")
        raise e

    # Perform a basic schema check before triggering the job
    if not basic_schema_check(bucket, key):
        return { "statusCode": 200, "body": "Schema check failed. Glue job not triggered." }

    try:
        logger.info(f"Starting Glue job: {job_name}")
        response = glue_client.start_job_run(
            JobName=job_name,
            Arguments={
                "--BRONZE_S3_PATH": bronze_s3_path,
                "--SILVER_S3_PATH": silver_s3_path,
            }
        )
        job_run_id = response["JobRunId"]
        logger.info(f"Successfully started Glue job with run ID: {job_run_id}")
        return {
            "statusCode": 200,
            "body": f"Successfully started Glue job {job_name} with run ID {job_run_id}"
        }
    except Exception as e:
        logger.error(f"Error starting Glue job {job_name}: {e}")
        raise e
