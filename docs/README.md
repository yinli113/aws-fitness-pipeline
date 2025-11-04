
- Local PySpark ETL for fitness tracking data builds bronze/silver/gold layers plus optional ML experiments.
- Data quality is questionable (large gaps, inconsistent weight/stress signals), so downstream analysis should validate assumptions before using.

## Project Overview

This project ingests a yearly health-fitness CSV exported from a Numbers workbook and refactors it into a standard analytics engineering shape. The focus is on enabling data analysts and data scientists to reason about the data quality, wrangle features, and prepare reporting models while acknowledging the limitations of the raw source.

### Key Goals
- Provide repeatable PySpark pipelines for bronze (raw landing), silver (cleaned + enriched), and gold (star-schema) layers.
- Document the transformations and limitations so analysts can decide how to treat suspicious metrics.
- Supply a light ML experimentation module that demonstrates stress-trend classification and weight-change regression, while noting poor predictive signal due to data issues.

## ETL Layers

### Bronze (`src/bronze.py`)
- **Input**: CSV at `pipeline_config.DEFAULT_RAW_PATH` (update via env or config if needed).
- **Actions**: Reads with explicit schema, basic validation, stamps ingest timestamp.
- **Output**: Parquet in `data/bronze/`.

### Silver (`src/silver.py`)
- **Input**: Bronze Parquet.
- **Cleaning Rules**: Negative-to-zero fixes, range filters (age, steps, heart rate, etc.), drop zero-step days, drop duplicates.
- **Feature Engineering**:
  - Activity level buckets + one-hot flags.
  - Sleep quality buckets using analyst-defined bins.
  - BMI category.
  - Date parts + weekend flag.
  - Rolling 30-day averages for steps/calories/exercise/stress.
  - 30-day weight and stress deltas (clamped to ±5 kg).
- **Output**: Parquet in `data/silver/`.

### Gold (`src/gold.py`)
- **Input**: Silver Parquet.
- **Dimensions**:
  - `dim_user`: avg anthropometrics per user.
  - `dim_date`: date attributes.
  - `dim_activity`: grain per user per day with activity + sleep descriptors.
- **Fact Table (`fact_daily`)**: Combines measures (steps, exercise_minutes, calories) with long-term features (ra30_*), weight/stress change, and flags for BI use.
- **Output**: Parquet tables in `data/gold/` (dim_*, fact_daily).

## ML Experiments (`src/ml.py`)
- Reads gold fact table to build:
  - **Stress change classifier** (`more_stress`, `no_changes`, `less_stress`) based on 30-day stress delta. Accuracy ~0.60; dominated by prior stress.
  - **Sleep quality classifier** (target `sleep_quality`). Accuracy ~0.65; limited variation.
  - **Weight-change regressor** (target 30-day weight delta). RMSE ~4.5 kg due to noisy/no predictive features.
- Outputs predictions into `data/gold/fact_predictions.parquet`. Treat results as illustrative; not production ready.

## Data Quality Observations
- Weight measurements jump unrealistically; 7-day & 30-day deltas often clamp at ±5 kg.
- Stress levels lack correlating behavioral drivers (activity, sleep), so predictive models have weak signal.
- Source likely derived/aggregated manually (Numbers sheet) without sensor-level integrity.

**Recommendation**: use the pipelines to audit data, generate diagnostics, and highlight remediation needs before any business-critical analysis.

### Dataset Provenance & Reliability
- Raw dataset: [Kaggle “Fitness Tracking” synthetic lifelog](https://www.kaggle.com/datasets/waqasishtiaq/fitness).
- Values are statistically generated, not captured from real devices; they provide structure but not trustworthy correlations.
- Treat outputs as prototypes for pipeline/app design rather than production-grade insights.

## Running the Pipelines
```bash
# Activate virtual env beforehand (.venv)
python -m src.bronze
python -m src.silver
python -m src.gold
python -m src.ml  # optional, writes predictions & logs metrics
```

Tests live under `src/tests/` (pytest).

### AWS Glue Deployment
- Package the repo root (include `src/` and `pipeline_config.py`) and upload to `s3://fitness-aws-bucket/Code/src.zip`.
- In Glue notebooks/jobs, download & extract once per session:
  ```python
  import boto3, zipfile, sys

  s3 = boto3.client("s3", region_name="ap-southeast-2")
  s3.download_file("fitness-aws-bucket", "Code/src.zip", "/tmp/src.zip")
  with zipfile.ZipFile("/tmp/src.zip", "r") as zf:
      zf.extractall("/tmp")
  sys.path.insert(0, "/tmp")
  ```
- Set storage mode for S3 execution before running stages:
  ```python
  import os
  os.environ["FITNESS_STORAGE_MODE"] = "s3"
  os.environ["FITNESS_S3_BUCKET"] = "fitness-aws-bucket"
  ```
- Execute the stages sequentially with the Glue Spark session:
  ```python
  from src.bronze import BronzePipeline
  from src.silver import SilverPipeline
  from src.gold import GoldPipeline

  spark = glueContext.spark_session
  BronzePipeline(spark).run()
  SilverPipeline(spark).run()
  GoldPipeline(spark).run()
  # from src.ml import MLPipeline; MLPipeline(spark).run()
  ```

### Automation & Notifications
- **AWS Lambda (`activate-s3`)**: receives S3 PUT events for raw CSV uploads.
- **Amazon EventBridge (`s3-updated`)**: routes S3 update events to the Lambda + downstream targets.
- **Amazon SNS (`s3-glue-notification`)**: email topic for data-update alerts.
- **Amazon CloudWatch**: monitors the Glue job logs and captures EventBridge rule output.
- With this wiring, any new object landing in `fitness-aws-bucket` triggers the workflow and notifies analysts.

## Product Vision (Future App)
- Goal: personal wellness coach that predicts weight change, sleep quality, and stress evolution from daily activity logs.
- User flow: capture profile (age, gender, BMI, targets), simulate a 30-day plan, adapt recommendations as new activity data arrives.
- AWS architecture (S3 → Lambda → Glue → EventBridge → SNS) mirrors a production separation of dev/prod and scales to richer tables (e.g., nutrition intake).
- Next data milestone: onboard real sensor streams to replace the synthetic Kaggle lifelog and improve model fidelity.

## Removing Unused Code
- Legacy notebook scripts have been deleted.
- Only the current ETL + ML modules remain in `src/`.
- Keep `data_cleaning_requirement.md` as reference for analyst-defined rules.

## Next Steps (for Analysts/Data Scientists)
- Validate raw measurement reliability; consider sourcing higher fidelity weight/stress data.
- Explore simpler aggregates (monthly averages) instead of noisy day-level modeling.
- If continuing ML, collect richer features (nutrition, sleep hygiene, context) to improve signal.

