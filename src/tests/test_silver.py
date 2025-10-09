from __future__ import annotations

from pathlib import Path

import pandas as pd

from pipeline_config import SILVER_OUTPUT
from src.bronze import BronzePipeline
from src.silver import SilverPipeline


def _make_bronze(tmp_path: Path, spark_session) -> Path:
    csv_path = tmp_path / "raw.csv"
    data = {
        "user_id": [1, 1, 2],
        "age": [25, 25, 31],
        "gender": ["M", "M", "F"],
        "date": ["2024-09-06", "2024-09-07", "2024-09-06"],
        "steps": [10000, 50000, 0],
        "heart_rate_avg": [70.0, 75.0, 80.0],
        "sleep_hours": [7.5, 8.0, 5.0],
        "calories_burned": [2500.0, 2600.0, 2200.0],
        "exercise_minutes": [60.0, 65.0, 30.0],
        "stress_level": [5, 6, 2],
        "weight_kg": [70.0, 70.5, 55.0],
        "bmi": [22.0, 22.1, 20.0],
    }
    pd.DataFrame(data).to_csv(csv_path, index=False)
    bronze_path = tmp_path / "bronze.parquet"
    BronzePipeline(spark=spark_session, raw_path=str(csv_path), output_path=str(bronze_path)).run()
    return bronze_path


def test_silver_pipeline_cleans_and_enriches(tmp_path: Path, spark_session):
    bronze_path = _make_bronze(tmp_path, spark_session)
    output_path = tmp_path / "silver.parquet"

    pipeline = SilverPipeline(spark=spark_session, bronze_path=str(bronze_path), output_path=str(output_path))
    df = pipeline.run()

    assert output_path.exists()
    assert "activity_level" in df.columns
    assert "sleep_quality" in df.columns
    assert df.filter(df.steps == 0).count() == 0

