from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from src.bronze import BronzePipeline


def _write_sample_csv(path: Path) -> None:
    data = {
        "user_id": [1, 2],
        "age": [25, 30],
        "gender": ["M", "F"],
        "date": ["2024-09-06", "2024-09-07"],
        "steps": [10000, 8000],
        "heart_rate_avg": [70.0, 75.0],
        "sleep_hours": [7.5, 8.0],
        "calories_burned": [2500.0, 2000.0],
        "exercise_minutes": [60.0, 45.0],
        "stress_level": [5, 4],
        "weight_kg": [70.0, 60.0],
        "bmi": [22.0, 21.0],
    }
    pd.DataFrame(data).to_csv(path, index=False)


def test_bronze_pipeline_writes_parquet(tmp_path: Path, spark_session):
    csv_path = tmp_path / "raw.csv"
    _write_sample_csv(csv_path)
    output_path = tmp_path / "bronze.parquet"

    pipeline = BronzePipeline(spark=spark_session, raw_path=str(csv_path), output_path=str(output_path))
    df = pipeline.run()

    assert output_path.exists(), "Bronze output parquet should exist"
    assert df.count() == 2
    assert "ingest_ts" in df.columns


def test_bronze_pipeline_missing_file_raises(spark_session):
    pipeline = BronzePipeline(spark=spark_session, raw_path="/nonexistent.csv", output_path="/tmp/out")
    with pytest.raises(FileNotFoundError):
        pipeline.read_raw()

