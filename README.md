# AWS Fitness Pipeline

This project provides a skeleton for a serverless data pipeline on AWS, designed to process fitness data. It follows the Medallion architecture (Bronze, Silver, Gold layers) for data processing and is defined using Terraform for Infrastructure as Code (IaC).

## Architecture

The pipeline uses a combination of AWS services to create an automated and scalable ETL (Extract, Transform, Load) workflow:

-   **S3 (Simple Storage Service)**: Used for data storage, with separate buckets for each layer of the Medallion architecture.
    -   `Bronze Bucket`: Stores raw, unprocessed data as it arrives.
    -   `Silver Bucket`: Stores cleaned, transformed, and enriched data, often in a more efficient format like Parquet.
    -   `Gold Bucket`: Stores aggregated data ready for analytics, reporting, or machine learning.
-   **Lambda**: A serverless compute service that triggers the Glue ETL job.
-   **Glue**: A fully managed ETL service that runs the PySpark script to transform data from the Bronze to the Silver layer.
-   **EventBridge (CloudWatch Events)**: Acts as a scheduler, triggering the Lambda function on a defined schedule (e.g., daily).
-   **SNS (Simple Notification Service)**: Used for sending notifications about the pipeline's status (e.g., on failure).
-   **CloudWatch**: Monitors the health and performance of the pipeline, with alarms configured to trigger SNS notifications.
-   **IAM (Identity and Access Management)**: Manages permissions for the different AWS services to interact with each other securely.

### Workflow

1.  Raw data is landed in the **Bronze S3 bucket**.
2.  An **EventBridge** rule triggers the **Lambda function** on a set schedule.
3.  The Lambda function starts the **AWS Glue job**.
4.  The Glue job executes the `etl.py` PySpark script, which reads data from the Bronze bucket, performs transformations, and writes the processed data to the **Silver S3 bucket** in Parquet format.
5.  If the Glue job fails, a **CloudWatch Alarm** is triggered, which sends a notification to an **SNS topic**.
6.  (Future step) Another Glue job could be created to process data from Silver to Gold for business-level aggregations.

## Project Structure

```
.
├── README.md
├── glue_jobs
│   └── etl.py          # PySpark script for the Glue ETL job
└── infrastructure
    ├── main.tf         # Main Terraform configuration
    ├── outputs.tf      # Terraform outputs
    ├── variables.tf    # Terraform variables
    └── lambda_src
        └── start_glue_job
            └── main.py # Python script for the Lambda function
```

## How to Deploy

### Prerequisites

-   [Terraform](https://learn.hashicorp.com/tutorials/terraform/install-cli) installed
-   [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-install.html) installed and configured with your credentials

### Deployment Steps

1.  **Navigate to the infrastructure directory:**
    ```bash
    cd infrastructure
    ```

2.  **Initialize Terraform:**
    This will download the necessary providers.
    ```bash
    terraform init
    ```

3.  **Review the deployment plan:**
    This will show you what resources Terraform will create.
    ```bash
    terraform plan
    ```

4.  **Apply the configuration:**
    This will create the AWS resources.
    ```bash
    terraform apply
    ```
    Enter `yes` when prompted to confirm.

### Cleanup

To tear down all the created resources, run the following command from the `infrastructure` directory:

```bash
terraform destroy
```
Enter `yes` when prompted to confirm.
