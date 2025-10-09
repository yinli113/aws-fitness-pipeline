"""Silver transformation pipeline with data cleaning and feature engineering."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Tuple

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from pipeline_config import BRONZE_OUTPUT, SILVER_OUTPUT, is_remote_path

LOGGER = logging.getLogger(__name__)


OUTLIER_RULES: Dict[str, Tuple[float, float]] = {
    "age": (18, 100),
    "steps": (0, 50000),
    "heart_rate_avg": (30, 200),
    "sleep_hours": (2, 16),
    "calories_burned": (500, 8000),
    "exercise_minutes": (0, 600),
    "stress_level": (1, 10),
    "weight_kg": (30, 200),
    "bmi": (10, 60),
}


ACTIVITY_LEVEL_BINS: List[Tuple[int, int, str]] = [
    (0, 4999, "sedentary"),
    (5000, 9999, "light"),
    (10000, 14999, "moderate"),
    (15000, 999999, "active"),
]

SLEEP_QUALITY_BINS: List[Tuple[float, float, str]] = [
    (0, 6, "poor"),
    (6, 8, "fair"),
    (8, 10, "good"),
    (10, 24, "excellent"),
]


@dataclass
class SilverPipeline:
    """Clean bronze data and create enriched silver dataset."""

    spark: SparkSession
    bronze_path: str | Path = BRONZE_OUTPUT
    output_path: str | Path = SILVER_OUTPUT

    def __post_init__(self) -> None:
        self.bronze_path = self.bronze_path or BRONZE_OUTPUT
        self.output_path = self.output_path or SILVER_OUTPUT

        if is_remote_path(self.bronze_path):
            self.bronze_path = str(self.bronze_path)
        else:
            self.bronze_path = Path(self.bronze_path)

        if is_remote_path(self.output_path):
            self.output_path = str(self.output_path)
        else:
            self.output_path = Path(self.output_path)

    def read_bronze(self) -> DataFrame:
        if isinstance(self.bronze_path, Path):
            if not self.bronze_path.exists():
                raise FileNotFoundError(
                    f"Bronze dataset not found at {self.bronze_path}. Run the bronze pipeline first."
                )
        else:
            LOGGER.info("Skipping existence check for remote bronze path %s", self.bronze_path)
        LOGGER.info("Reading bronze dataset from %s", self.bronze_path)
        return self.spark.read.parquet(str(self.bronze_path))

    def clean_data(self, df: DataFrame) -> DataFrame:
        LOGGER.info("Cleaning data according to requirements doc")
        numeric_cols = [
            "steps",
            "heart_rate_avg",
            "sleep_hours",
            "calories_burned",
            "exercise_minutes",
            "weight_kg",
            "bmi",
        ]
        cleaned = df
        for column in numeric_cols:
            cleaned = cleaned.withColumn(column, F.when(F.col(column) < 0, 0).otherwise(F.col(column)))
        for column, (min_val, max_val) in OUTLIER_RULES.items():
            cleaned = cleaned.filter((F.col(column) >= min_val) & (F.col(column) <= max_val))
        cleaned = cleaned.filter(F.col("steps") > 0)
        cleaned = cleaned.dropDuplicates()
        cleaned = cleaned.withColumn("date", F.to_date("date", "yyyy-MM-dd"))
        cleaned = cleaned.na.drop(subset=["user_id", "date"])
        return cleaned

    def feature_engineering(self, df: DataFrame) -> DataFrame:
        LOGGER.info("Adding derived features")

        for bin_min, bin_max, label in ACTIVITY_LEVEL_BINS:
            df = df.withColumn(
                f"activity_level_{label}",
                F.when((F.col("steps") >= bin_min) & (F.col("steps") <= bin_max), 1).otherwise(0),
            )

        df = df.withColumn(
            "activity_level",
            F.when(F.col("steps") < 5000, "sedentary")
            .when(F.col("steps") < 10000, "light")
            .when(F.col("steps") < 15000, "moderate")
            .otherwise("active"),
        )

        def sleep_quality_case(column: str) -> F.Column:
            case_col = F.lit("poor")
            for min_val, max_val, label in SLEEP_QUALITY_BINS:
                case_col = F.when((F.col(column) >= min_val) & (F.col(column) < max_val), label).otherwise(case_col)
            return case_col

        df = df.withColumn("sleep_quality", sleep_quality_case("sleep_hours"))
        df = df.withColumn(
            "bmi_category",
            F.when(F.col("bmi") < 18.5, "underweight")
            .when(F.col("bmi") < 25, "normal")
            .when(F.col("bmi") < 30, "overweight")
            .otherwise("obese"),
        )

        df = df.withColumn("year", F.year("date"))
        df = df.withColumn("month", F.month("date"))
        df = df.withColumn("day", F.dayofmonth("date"))
        df = df.withColumn("weekday", F.date_format("date", "EEEE"))
        df = df.withColumn("weekday_num", ((F.dayofweek("date") + 5) % 7).cast("int"))
        df = df.withColumn("is_weekend", F.col("weekday").isin("Saturday", "Sunday").cast("int"))

        window = Window.partitionBy("user_id").orderBy("date")
        df = df.withColumn("weight_prev", F.lag("weight_kg").over(window))
        df = df.withColumn("weight_change_30d", F.col("weight_kg") - F.lag("weight_kg", 30).over(window))
        df = df.withColumn("ra30_steps", F.avg("steps").over(window.rowsBetween(-29, 0)))
        df = df.withColumn("ra30_calories", F.avg("calories_burned").over(window.rowsBetween(-29, 0)))
        df = df.withColumn("ra30_ex_minutes", F.avg("exercise_minutes").over(window.rowsBetween(-29, 0)))
        df = df.withColumn("lag1_stress", F.lag("stress_level").over(window))
        df = df.withColumn("lag1_sleep_hours", F.lag("sleep_hours").over(window))
        df = df.withColumn("ra30_stress", F.avg("stress_level").over(window.rowsBetween(-29, 0)))
        df = df.withColumn("stress_change_30d", F.col("stress_level") - F.lag("stress_level", 30).over(window))

        df = df.withColumn(
            "height_m",
            F.when(F.col("bmi") > 0, F.sqrt(F.col("weight_kg") / F.col("bmi"))).otherwise(None),
        )
        df = df.withColumn("height_cm", F.col("height_m") * 100)
        df = df.withColumn(
            "bmr",
            10 * F.col("weight_kg")
            + 6.25 * F.col("height_cm")
            - 5 * F.col("age")
            + F.when(F.col("gender") == "M", 5).otherwise(-161),
        )
        df = df.withColumn("caloric_balance", F.col("calories_burned") - F.col("bmr"))
        return df

    def write(self, df: DataFrame) -> None:
        output_uri = str(self.output_path)
        if isinstance(self.output_path, Path):
            self.output_path.parent.mkdir(parents=True, exist_ok=True)
        LOGGER.info("Writing silver dataset to %s", output_uri)
        df.write.mode("overwrite").parquet(output_uri)

    def run(self) -> DataFrame:
        bronze_df = self.read_bronze()
        cleaned = self.clean_data(bronze_df)
        enriched = self.feature_engineering(cleaned)
        self.write(enriched)
        return enriched.cache()


def create_spark(app_name: str = "fitness-silver") -> SparkSession:
    return SparkSession.builder.master("local[*]").appName(app_name).getOrCreate()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    spark = create_spark()
    try:
        pipeline = SilverPipeline(spark=spark)
        silver_df = pipeline.run()
        silver_df.limit(5).show()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

