from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.bronze import BronzePipeline
from src.silver import SilverPipeline
from src.gold import GoldPipeline


def _prepare_silver(tmp_path: Path, spark_session) -> Path:
    csv_path = tmp_path / "raw.csv"
    data = {
        "user_id": [1, 1, 2, 2],
        "age": [25, 25, 31, 31],
        "gender": ["M", "M", "F", "F"],
        "date": ["2024-09-06", "2024-09-07", "2024-09-06", "2024-09-07"],
        "steps": [10000, 12000, 8000, 9000],
        "heart_rate_avg": [70.0, 72.0, 65.0, 67.0],
        "sleep_hours": [7.5, 7.0, 8.0, 8.5],
        "calories_burned": [2500, 2600, 2000, 2100],
        "exercise_minutes": [60, 65, 45, 50],
        "stress_level": [5, 6, 3, 4],
        "weight_kg": [70.0, 70.5, 55.0, 55.2],
        "bmi": [22.0, 22.1, 20.0, 20.1],
    }
    pd.DataFrame(data).to_csv(csv_path, index=False)
    bronze_path = tmp_path / "bronze.parquet"
    silver_path = tmp_path / "silver.parquet"
    BronzePipeline(spark=spark_session, raw_path=str(csv_path), output_path=str(bronze_path)).run()
    SilverPipeline(spark=spark_session, bronze_path=str(bronze_path), output_path=str(silver_path)).run()
    return silver_path


def test_gold_pipeline_builds_dimensions(tmp_path: Path, spark_session):
    silver_path = _prepare_silver(tmp_path, spark_session)
    pipeline = GoldPipeline(spark=spark_session, silver_path=str(silver_path))
    dim_user, dim_date, dim_activity, fact_daily = pipeline.run()

    assert dim_user.count() > 0
    assert dim_date.count() > 0
    assert fact_daily.count() > 0
    assert "activity_level" in dim_activity.columns

