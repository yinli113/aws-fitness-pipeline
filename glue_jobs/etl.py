import sys
import time
import json
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.window import Window
from pyspark.sql.functions import avg, lag, col

def get_secret(secret_name, region_name):
    """Retrieves a secret from AWS Secrets Manager."""
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name=region_name)
    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
        if 'SecretString' in get_secret_value_response:
            return json.loads(get_secret_value_response['SecretString'])
        else:
            # Handle binary secret if needed
            return get_secret_value_response['SecretBinary']
    except Exception as e:
        print(f"Failed to retrieve secret '{secret_name}'. Error: {e}")
        raise e

def put_cloudwatch_metric(namespace, metric_name, value, unit='None'):
    """Puts a custom metric to AWS CloudWatch."""
    try:
        cloudwatch = boto3.client('cloudwatch')
        cloudwatch.put_metric_data(
            Namespace=namespace,
            MetricData=[{'MetricName': metric_name, 'Value': value, 'Unit': unit}]
        )
        print(f"Successfully put metric {metric_name} with value {value} to namespace {namespace}")
    except Exception as e:
        print(f"Failed to put CloudWatch metric '{metric_name}'. Error: {e}")

# --- Start of Job ---
start_time = time.time()
error_count = 0
args = {}

try:
    # Get job arguments
    args = getResolvedOptions(sys.argv, [
        "JOB_NAME",
        "BRONZE_S3_PATH",
        "SILVER_S3_PATH",
        "SECRET_NAME",
        "AWS_REGION"
    ])

    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args["JOB_NAME"], args)

    # --- 1. Retrieve Credentials (Example) ---
    # secret_name = args["SECRET_NAME"]
    # region_name = args["AWS_REGION"]
    # credentials = get_secret(secret_name, region_name)
    # print("Successfully retrieved credentials from Secrets Manager.")
    # Example usage:
    # db_user = credentials.get('username')
    # db_password = credentials.get('password')

    # Define source and destination paths from arguments
    bronze_s3_path = args["BRONZE_S3_PATH"]
    silver_s3_path = args["SILVER_S3_PATH"]

    # Read data from the Bronze layer
    datasource = glueContext.create_dynamic_frame.from_options(
        connection_type="s3",
        connection_options={"paths": [bronze_s3_path]},
        format="csv",
        format_options={"withHeader": True, "separator": ","},
        transformation_ctx="datasource",
    )

    df = datasource.toDF()

    # --- Data Cleaning and Feature Engineering ---
    df_cleaned = df # Placeholder
    df_featured = df_cleaned # Placeholder

    dynamic_frame_final = DynamicFrame.fromDF(df_featured, glueContext, "dynamic_frame_final")

    # Write the transformed data to the Silver layer
    glueContext.write_dynamic_frame.from_options(
        frame=dynamic_frame_final,
        connection_type="s3",
        connection_options={"path": silver_s3_path},
        format="parquet",
        transformation_ctx="datasink",
    )

except Exception as e:
    print(f"An error occurred during the Glue job: {e}")
    error_count = 1
    raise e # Re-raise the exception to fail the job

finally:
    # --- Publish CloudWatch Metrics ---
    duration = time.time() - start_time
    job_name = args.get("JOB_NAME", "aws-fitness-pipeline-etl-job")
    metric_namespace = f"Glue/{job_name}"

    put_cloudwatch_metric(metric_namespace, "JobDuration", duration, "Seconds")
    put_cloudwatch_metric(metric_namespace, "ErrorCount", error_count, "Count")

    if 'job' in locals():
        job.commit()
