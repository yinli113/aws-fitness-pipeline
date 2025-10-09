"""Gold star-schema creation for PySpark environments (local or AWS)."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from pipeline_config import (
    DIM_ACTIVITY_OUTPUT,
    DIM_DATE_OUTPUT,
    DIM_USER_OUTPUT,
    FACT_DAILY_OUTPUT,
    SILVER_OUTPUT,
    is_remote_path,
)

LOGGER = logging.getLogger(__name__)


@dataclass
class GoldPipeline:
    """Build dimension and fact tables from the silver dataset."""

    spark: SparkSession
    silver_path: str | Path = SILVER_OUTPUT
    dim_user_path: str | Path = DIM_USER_OUTPUT
    dim_date_path: str | Path = DIM_DATE_OUTPUT
    dim_activity_path: str | Path = DIM_ACTIVITY_OUTPUT
    fact_daily_path: str | Path = FACT_DAILY_OUTPUT

    def __post_init__(self) -> None:
        self.silver_path = self.silver_path or SILVER_OUTPUT
        self.dim_user_path = self.dim_user_path or DIM_USER_OUTPUT
        self.dim_date_path = self.dim_date_path or DIM_DATE_OUTPUT
        self.dim_activity_path = self.dim_activity_path or DIM_ACTIVITY_OUTPUT
        self.fact_daily_path = self.fact_daily_path or FACT_DAILY_OUTPUT

        self.silver_path = str(self.silver_path) if is_remote_path(self.silver_path) else Path(self.silver_path)
        self.dim_user_path = str(self.dim_user_path) if is_remote_path(self.dim_user_path) else Path(self.dim_user_path)
        self.dim_date_path = str(self.dim_date_path) if is_remote_path(self.dim_date_path) else Path(self.dim_date_path)
        self.dim_activity_path = str(self.dim_activity_path) if is_remote_path(self.dim_activity_path) else Path(self.dim_activity_path)
        self.fact_daily_path = str(self.fact_daily_path) if is_remote_path(self.fact_daily_path) else Path(self.fact_daily_path)

    def read_silver(self) -> DataFrame:
        if isinstance(self.silver_path, Path):
            if not self.silver_path.exists():
                raise FileNotFoundError(
                    f"Silver dataset not found at {self.silver_path}. Run the silver pipeline first."
                )
        else:
            LOGGER.info("Skipping existence check for remote silver path %s", self.silver_path)
        LOGGER.info("Loading silver dataset from %s", self.silver_path)
        return self.spark.read.parquet(str(self.silver_path))

    def build_dim_user(self, df: DataFrame) -> DataFrame:
        dim_user = (
            df.groupBy("user_id", "age", "gender")
            .agg(
                F.round(F.avg("height_cm"), 2).alias("avg_height_cm"),
                F.round(F.avg("weight_kg"), 2).alias("avg_weight_kg"),
                F.round(F.avg("bmi"), 2).alias("avg_bmi"),
                F.round(F.avg("bmr"), 2).alias("avg_bmr"),
            )
            .orderBy("user_id")
        )
        LOGGER.info("dim_user contains %s rows", dim_user.count())
        return dim_user

    def build_dim_date(self, df: DataFrame) -> DataFrame:
        dim_date = (
            df.select(
                F.date_format("date", "yyyyMMdd").cast("int").alias("date_key"),
                "date",
                "day",
                "month",
                "year",
                F.date_format("date", "EEEE").alias("weekday"),
            )
            .dropDuplicates(["date_key"])
            .orderBy("date")
        )
        LOGGER.info("dim_date contains %s rows", dim_date.count())
        return dim_date

    def build_dim_activity(self, df: DataFrame) -> DataFrame:
        dim_activity = df.select(
            "user_id",
            "date",
            "steps",
            "exercise_minutes",
            "calories_burned",
            "activity_level",
            "sleep_quality",
        ).orderBy("user_id", "date")
        LOGGER.info("dim_activity contains %s rows", dim_activity.count())
        return dim_activity

    def build_fact_daily(self, df: DataFrame) -> DataFrame:
        fact_daily = (
            df.select(
                "user_id",
                F.date_format("date", "yyyyMMdd").cast("int").alias("date_key"),
                "age",
                "gender",
                "sleep_hours",
                "heart_rate_avg",
                "stress_level",
                "steps",
                "exercise_minutes",
                "calories_burned",
                F.col("weight_kg").alias("weight_kg"),
                F.col("bmi").alias("bmi"),
                "height_cm",
                "bmr",
                F.round("ra30_steps", 2).alias("ra30_steps"),
                F.round("ra30_calories", 2).alias("ra30_calories"),
                F.round("ra30_ex_minutes", 2).alias("ra30_ex_minutes"),
                F.round("caloric_balance", 2).alias("caloric_balance"),
                F.round("weight_change_30d", 2).alias("weight_change_30d"),
                F.col("lag1_stress").alias("yesterday_stress"),
                F.col("lag1_sleep_hours").alias("yesterday_sleep"),
                "month",
                "year",
                "day",
                F.date_format("date", "EEEE").alias("weekday"),
                F.col("weekday_num").cast("int").alias("weekday_num"),
                F.col("is_weekend").cast("int").alias("is_weekend"),
                "activity_level",
                "sleep_quality",
                "bmi_category",
                F.round("ra30_stress", 2).alias("ra30_stress"),
                F.round("stress_change_30d", 2).alias("stress_change_30d"),
            )
            .filter(F.col("user_id").isNotNull())
        )
        LOGGER.info("fact_daily contains %s rows", fact_daily.count())
        return fact_daily

    def write(self, dataframe: DataFrame, path: str | Path) -> None:
        output_uri = str(path)
        if isinstance(path, Path):
            path.parent.mkdir(parents=True, exist_ok=True)
        LOGGER.info("Writing dataset to %s", output_uri)
        dataframe.write.mode("overwrite").parquet(output_uri)

    def run(self) -> Tuple[DataFrame, DataFrame, DataFrame, DataFrame]:
        silver_df = self.read_silver()
        dim_user = self.build_dim_user(silver_df)
        dim_date = self.build_dim_date(silver_df)
        dim_activity = self.build_dim_activity(silver_df)
        fact_daily = self.build_fact_daily(silver_df)

        self.write(dim_user, self.dim_user_path)
        self.write(dim_date, self.dim_date_path)
        self.write(dim_activity, self.dim_activity_path)
        self.write(fact_daily, self.fact_daily_path)

        return dim_user.cache(), dim_date.cache(), dim_activity.cache(), fact_daily.cache()


def create_spark(app_name: str = "fitness-gold") -> SparkSession:
    return SparkSession.builder.master("local[*]").appName(app_name).getOrCreate()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    spark = create_spark()
    try:
        pipeline = GoldPipeline(spark=spark)
        dim_user, dim_date, dim_activity, fact_daily = pipeline.run()
        dim_user.limit(5).show()
        fact_daily.limit(5).show()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

