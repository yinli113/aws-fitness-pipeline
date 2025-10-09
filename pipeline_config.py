"""Shared configuration utilities for the fitness pipeline (local & AWS)."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Union

StoragePath = Union[str, os.PathLike[str]]

STORAGE_MODE = os.getenv("FITNESS_STORAGE_MODE", "s3").lower()
IS_LOCAL_STORAGE = STORAGE_MODE == "local"


def is_remote_path(path: StoragePath) -> bool:
    """Return True when the given path points to an object store URI."""

    return str(path).startswith(("s3://", "s3a://"))


if IS_LOCAL_STORAGE:
    PROJECT_ROOT = Path(__file__).resolve().parent
    DATA_DIR = Path(os.getenv("FITNESS_LOCAL_ROOT", PROJECT_ROOT / "data"))
    BRONZE_DIR = DATA_DIR / "bronze"
    SILVER_DIR = DATA_DIR / "silver"
    GOLD_DIR = DATA_DIR / "gold"
    ML_DIR = DATA_DIR / "ml"

    for directory in (DATA_DIR, BRONZE_DIR, SILVER_DIR, GOLD_DIR, ML_DIR):
        directory.mkdir(parents=True, exist_ok=True)

    DEFAULT_RAW_PATH = str(
        Path(
            os.getenv(
                "FITNESS_RAW_PATH",
                PROJECT_ROOT.parent / "health_fitness_tracking_365days.csv",
            )
        )
    )

    BRONZE_OUTPUT = str(BRONZE_DIR / "fitness_bronze.parquet")
    SILVER_OUTPUT = str(SILVER_DIR / "fitness_silver.parquet")
    DIM_USER_OUTPUT = str(GOLD_DIR / "dim_user.parquet")
    DIM_DATE_OUTPUT = str(GOLD_DIR / "dim_date.parquet")
    DIM_ACTIVITY_OUTPUT = str(GOLD_DIR / "dim_activity.parquet")
    FACT_DAILY_OUTPUT = str(GOLD_DIR / "fact_daily.parquet")
    PREDICTIONS_OUTPUT = str(ML_DIR / "fact_predictions.parquet")
else:
    S3_PROTOCOL = os.getenv("FITNESS_S3_PROTOCOL", "s3").rstrip(":")
    S3_BUCKET = os.getenv("FITNESS_S3_BUCKET", "fitness-aws-bucket").strip()

    def _s3_uri(prefix: str, obj: str | None = None) -> str:
        prefix_clean = prefix.strip("/")
        parts: list[str] = []
        if prefix_clean:
            parts.append(prefix_clean)
        if obj:
            parts.append(obj.strip("/"))
        key = "/".join(parts)
        base = f"{S3_PROTOCOL}://{S3_BUCKET}"
        return f"{base}/{key}" if key else base

    raw_prefix = os.getenv("FITNESS_RAW_PREFIX", "Raw Data Bucket")
    raw_object = os.getenv("FITNESS_RAW_OBJECT", "health_fitness_tracking_365days.csv")
    bronze_prefix = os.getenv("FITNESS_BRONZE_PREFIX", "Bronze Bucket")
    silver_prefix = os.getenv("FITNESS_SILVER_PREFIX", "Silver Bucket")
    gold_prefix = os.getenv("FITNESS_GOLD_PREFIX", "Gold Bucket")
    ml_prefix = os.getenv("FITNESS_ML_PREFIX", "ML Bucket")

    DEFAULT_RAW_PATH = os.getenv("FITNESS_RAW_PATH", _s3_uri(raw_prefix, raw_object))
    BRONZE_OUTPUT = _s3_uri(bronze_prefix, "fitness_bronze.parquet")
    SILVER_OUTPUT = _s3_uri(silver_prefix, "fitness_silver.parquet")
    DIM_USER_OUTPUT = _s3_uri(gold_prefix, "dim_user.parquet")
    DIM_DATE_OUTPUT = _s3_uri(gold_prefix, "dim_date.parquet")
    DIM_ACTIVITY_OUTPUT = _s3_uri(gold_prefix, "dim_activity.parquet")
    FACT_DAILY_OUTPUT = _s3_uri(gold_prefix, "fact_daily.parquet")
    PREDICTIONS_OUTPUT = _s3_uri(ml_prefix, "fact_predictions.parquet")

