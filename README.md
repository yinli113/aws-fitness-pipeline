# AWS Serverless Fitness Data Pipeline

> **Impact Statement:** This scalable ETL pipeline processes 365+ days of fitness tracking data using a serverless AWS architecture, transforming raw logs into Gold-standard analytics tables for health trend monitoring and ML prediction.

![Architecture Status](https://img.shields.io/badge/Status-Production-success?style=flat-square)
![AWS](https://img.shields.io/badge/Cloud-AWS-232F3E?style=flat-square&logo=amazon-aws)
![Python](https://img.shields.io/badge/Code-Python_3.9+-3776AB?style=flat-square&logo=python)

## Overview

A robust data engineering solution that ingests, cleans, and models high-volume fitness tracker data. Designed with a **hybrid execution engine**, it supports rapid local development (using file systems) and seamless deployment to AWS (using S3 and Glue).

The pipeline implements a **Medallion Architecture** to ensure data quality:
- **Bronze**: Raw ingestion from CSV sources.
- **Silver**: Cleaned, standardized Parquet files.
- **Gold**: Star-schema dimensional models (`fact_daily`, `dim_user`, etc.) ready for BI tools like Amazon QuickSight.

## Architecture

```mermaid
graph LR
    Source[Raw Data\n(CSV)] -->|Ingest| Bronze[(Bronze Layer\nS3/Local)]
    Bronze -->|Cleanse| Silver[(Silver Layer\nParquet)]
    Silver -->|Transform| Gold[(Gold Layer\nStar Schema)]
    
    subgraph "Gold Domain Models"
        Gold --> Fact[Fact Daily]
        Gold --> DimU[Dim User]
        Gold --> DimA[Dim Activity]
    end
    
    Gold -->|Training Data| ML[(ML Predictions)]
```

## Key Features

- **Dual-Mode Execution**: Toggle between `local` and `s3` storage modes via environment variables (`FITNESS_STORAGE_MODE`).
- **Data Quality Checks**: Automated validation of schema consistency and value ranges during Bronze->Silver transformation.
- **Dimensional Modeling**: Transforms flat files into optimal Star Schema for high-performance analytics.
- **ML Integration**: Dedicated pipeline branch to generate feature stores for machine learning models.

## Repository Structure

```
├── src/                # ETL source code
├── docs/               # Architecture decision records
├── pipeline_config.py  # Configuration for Local/AWS paths
├── requirements.txt    # Python dependencies
└── README.md           # This file
```

## Setup & Usage

### 1. Local Development
Run the pipeline locally to test logic without incurring cloud costs.

```bash
# Set environment to local
export FITNESS_STORAGE_MODE="local"
export FITNESS_LOCAL_ROOT="./data"

# Run the pipeline
python src/main.py
```

### 2. AWS Deployment
Deploy to AWS Glue or Lambda for serverless execution.

```bash
# Set environment to AWS
export FITNESS_STORAGE_MODE="s3"
export FITNESS_S3_BUCKET="your-production-bucket"

# Run (or trigger via EventBridge)
python src/main.py
```

## Configuration

The pipeline behavior is controlled via `pipeline_config.py` using 12-factor app principles:

| Variable | Description | Default |
|----------|-------------|---------|
| `FITNESS_STORAGE_MODE` | `local` or `s3` | `s3` |
| `FITNESS_S3_BUCKET` | Target AWS Bucket | `fitness-aws-bucket` |
| `FITNESS_RAW_PATH` | Override input path | `s3://.../raw.csv` |

## License

This project is open-source and available under the MIT License.
