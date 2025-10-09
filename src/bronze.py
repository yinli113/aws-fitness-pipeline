"""Bronze ingestion pipeline for PySpark environments (local or AWS)."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F, types as T

from pipeline_config import BRONZE_OUTPUT, DEFAULT_RAW_PATH, is_remote_path

LOGGER = logging.getLogger(__name__)


BRONZE_SCHEMA = T.StructType(
    [
        T.StructField("user_id", T.IntegerType(), True),
        T.StructField("age", T.IntegerType(), True),
        T.StructField("gender", T.StringType(), True),
        T.StructField("date", T.StringType(), True),
        T.StructField("steps", T.IntegerType(), True),
        T.StructField("heart_rate_avg", T.DoubleType(), True),
        T.StructField("sleep_hours", T.DoubleType(), True),
        T.StructField("calories_burned", T.DoubleType(), True),
        T.StructField("exercise_minutes", T.DoubleType(), True),
        T.StructField("stress_level", T.IntegerType(), True),
        T.StructField("weight_kg", T.DoubleType(), True),
        T.StructField("bmi", T.DoubleType(), True),
    ]
)


@dataclass
class BronzePipeline:
    """Load and validate the raw CSV into a bronze Parquet dataset."""

    spark: SparkSession
    raw_path: str | Path | None = None
    output_path: str | Path = BRONZE_OUTPUT

    def __post_init__(self) -> None:
        self.raw_path = self.raw_path or DEFAULT_RAW_PATH
        self.output_path = self.output_path or BRONZE_OUTPUT
        if is_remote_path(self.raw_path):
            self.raw_path = str(self.raw_path)
        else:
            self.raw_path = Path(self.raw_path)
        if is_remote_path(self.output_path):
            self.output_path = str(self.output_path)
        else:
            self.output_path = Path(self.output_path)

    def _ensure_source_exists(self) -> None:
        if isinstance(self.raw_path, Path):
            if not self.raw_path.exists():
                raise FileNotFoundError(
                    f"Raw data file not found: {self.raw_path}. "
                    "Check the path or set the FITNESS_RAW_PATH environment variable."
                )
        else:
            LOGGER.info("Skipping existence check for remote source %s", self.raw_path)

    def read_raw(self) -> DataFrame:
        self._ensure_source_exists()
        LOGGER.info("Reading raw CSV from %s", self.raw_path)
        raw_uri = str(self.raw_path)
        return (
            self.spark.read.option("header", True)
            .option("multiLine", True)
            .option("escape", '"')
            .schema(BRONZE_SCHEMA)
            .csv(raw_uri)
        )

    def validate_schema(self, df: DataFrame) -> None:
        expected_cols = [f.name for f in BRONZE_SCHEMA]
        actual_cols = df.columns
        missing = [c for c in expected_cols if c not in actual_cols]
        extra = [c for c in actual_cols if c not in expected_cols]
        if missing or extra:
            raise ValueError(f"Schema mismatch. Missing: {missing} | Extra: {extra}")

    def transform(self, df: DataFrame) -> DataFrame:
        bronze_df = df.withColumn("ingest_ts", F.current_timestamp())
        row_count = bronze_df.count()
        if row_count == 0:
            raise ValueError("No data found in the source file")
        LOGGER.info("Bronze dataset contains %s rows", row_count)
        for column in bronze_df.columns:
            null_count = bronze_df.filter(F.col(column).isNull()).count()
            if null_count == row_count:
                LOGGER.warning("Column %s is completely null", column)
        return bronze_df

    def write(self, df: DataFrame) -> None:
        output_uri = str(self.output_path)
        if isinstance(self.output_path, Path):
            self.output_path.parent.mkdir(parents=True, exist_ok=True)
        LOGGER.info("Writing bronze dataset to %s", output_uri)
        df.write.mode("overwrite").parquet(output_uri)

    def run(self) -> DataFrame:
        raw_df = self.read_raw()
        self.validate_schema(raw_df)
        bronze_df = self.transform(raw_df)
        self.write(bronze_df)
        return bronze_df.cache()


def create_spark(app_name: str = "fitness-bronze") -> SparkSession:
    return SparkSession.builder.master("local[*]").appName(app_name).getOrCreate()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    spark = create_spark()
    try:
        pipeline = BronzePipeline(spark=spark)
        bronze_df = pipeline.run()
        bronze_df.limit(5).show()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

