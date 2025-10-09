from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.bronze import BronzePipeline
from src.silver import SilverPipeline
from src.gold import GoldPipeline
from src.ml import MLPipeline


def _prepare_fact(tmp_path: Path, spark_session) -> Path:
    csv_path = tmp_path / "raw.csv"
    records = []
    for user_id in (1, 2):
        for day in range(6, 13):
            records.append(
                {
                    "user_id": user_id,
                    "age": 25 + user_id,
                    "gender": "M" if user_id == 1 else "F",
                    "date": f"2024-09-{day:02d}",
                    "steps": 8000 + user_id * 500 + day * 10,
                    "heart_rate_avg": 70 + day / 10,
                    "sleep_hours": 7 + (day % 3) * 0.5,
                    "calories_burned": 2300 + day * 50,
                    "exercise_minutes": 45 + day,
                    "stress_level": 4 + (day % 3),
                    "weight_kg": 70 + user_id,
                    "bmi": 22 + user_id,
                }
            )
    pd.DataFrame(records).to_csv(csv_path, index=False)

    bronze_path = tmp_path / "bronze.parquet"
    silver_path = tmp_path / "silver.parquet"
    fact_path = tmp_path / "fact.parquet"

    BronzePipeline(spark=spark_session, raw_path=str(csv_path), output_path=str(bronze_path)).run()
    SilverPipeline(spark=spark_session, bronze_path=str(bronze_path), output_path=str(silver_path)).run()
    GoldPipeline(
        spark=spark_session,
        silver_path=str(silver_path),
        fact_daily_path=str(fact_path),
    ).run()
    return fact_path


def test_ml_pipeline_generates_predictions(tmp_path: Path, spark_session):
    fact_path = _prepare_fact(tmp_path, spark_session)
    output_path = tmp_path / "predictions.parquet"

    pipeline = MLPipeline(
        spark=spark_session,
        fact_daily_path=str(fact_path),
        output_path=str(output_path),
    )
    predictions = pipeline.run()

    assert output_path.exists()
    assert predictions.count() > 0
    assert "predicted_stress_change" in predictions.columns

