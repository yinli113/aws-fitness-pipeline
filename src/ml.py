"""Machine learning training and prediction pipeline for the gold dataset."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator, RegressionEvaluator
from pyspark.ml.feature import IndexToString, StringIndexer, VectorAssembler
from pyspark.ml.regression import RandomForestRegressor

from pipeline_config import FACT_DAILY_OUTPUT, PREDICTIONS_OUTPUT, is_remote_path

LOGGER = logging.getLogger(__name__)


@dataclass
class MLPipeline:
    """Train ML models using the fact dataset and persist predictions."""

    spark: SparkSession
    fact_daily_path: str | Path = FACT_DAILY_OUTPUT
    output_path: str | Path = PREDICTIONS_OUTPUT

    def __post_init__(self) -> None:
        self.fact_daily_path = self.fact_daily_path or FACT_DAILY_OUTPUT
        self.output_path = self.output_path or PREDICTIONS_OUTPUT

        self.fact_daily_path = str(self.fact_daily_path) if is_remote_path(self.fact_daily_path) else Path(self.fact_daily_path)
        self.output_path = str(self.output_path) if is_remote_path(self.output_path) else Path(self.output_path)

    def read_fact(self) -> DataFrame:
        if isinstance(self.fact_daily_path, Path):
            if not self.fact_daily_path.exists():
                raise FileNotFoundError(
                    f"Fact dataset not found at {self.fact_daily_path}. Run the gold pipeline first."
                )
        else:
            LOGGER.info("Skipping existence check for remote fact path %s", self.fact_daily_path)
        LOGGER.info("Loading fact dataset from %s", self.fact_daily_path)
        return self.spark.read.parquet(str(self.fact_daily_path))

    def prepare_dataset(self, df: DataFrame) -> DataFrame:
        LOGGER.info("Preparing ML dataset")
        base_columns = [
            "user_id",
            "date_key",
            "age",
            "gender",
            "steps",
            "exercise_minutes",
            "calories_burned",
            "heart_rate_avg",
            "stress_level",
            "sleep_hours",
            "sleep_quality",
            "activity_level",
            "ra30_steps",
            "ra30_calories",
            "ra30_ex_minutes",
            "ra30_stress",
            "yesterday_stress",
            "caloric_balance",
            "weight_change_30d",
            "stress_change_30d",
            "weight_kg",
            "yesterday_sleep",
            "month",
            "year",
            "day",
            "weekday",
            "weekday_num",
            "is_weekend",
        ]

        df_selected = df.select(*base_columns)

        # Drop rows missing categorical or target labels
        df_selected = df_selected.na.drop(subset=["sleep_quality", "activity_level", "sleep_hours"])

        fill_zero_cols = [
            "ra30_steps",
            "ra30_calories",
            "ra30_ex_minutes",
            "ra30_stress",
            "yesterday_stress",
            "caloric_balance",
            "weight_change_30d",
            "stress_change_30d",
            "yesterday_sleep",
        ]
        df_selected = df_selected.fillna(0, subset=fill_zero_cols)

        LOGGER.info("ML dataset prepared with %s rows", df_selected.count())
        return df_selected.cache()

    def _fit_stress_classifier(self, df_ml: DataFrame) -> Tuple[Pipeline, DataFrame]:
        stress_labeled = df_ml.withColumn(
            "stress_change_label",
            F.when(F.col("stress_change_30d") >= 3, "more_stress")
            .when(F.col("stress_change_30d") <= -3, "less_stress")
            .otherwise("no_changes"),
        )

        label_indexer = StringIndexer(
            inputCol="stress_change_label",
            outputCol="stress_label_idx",
            handleInvalid="skip",
        ).fit(stress_labeled)

        feature_cols = [
            "steps",
            "ra30_steps",
            "ra30_ex_minutes",
            "ra30_stress",
            "exercise_minutes",
            "calories_burned",
            "stress_level",
            "yesterday_stress",
            "caloric_balance",
            "weight_change_30d",
            "age",
            "heart_rate_avg",
        ]

        indexed_df = label_indexer.transform(stress_labeled)

        gender_indexer = StringIndexer(inputCol="gender", outputCol="gender_idx", handleInvalid="keep")
        activity_indexer = StringIndexer(
            inputCol="activity_level", outputCol="activity_level_idx", handleInvalid="keep"
        )

        assembler = VectorAssembler(
            inputCols=[
                "steps",
                "ra30_steps",
                "ra30_ex_minutes",
                "ra30_stress",
                "exercise_minutes",
                "calories_burned",
                "stress_level",
                "yesterday_stress",
                "caloric_balance",
                "weight_change_30d",
                "age",
                "heart_rate_avg",
                "gender_idx",
                "activity_level_idx",
            ],
            outputCol="features",
        )

        classifier = RandomForestClassifier(
            labelCol="stress_label_idx",
            featuresCol="features",
            maxDepth=6,
            numTrees=80,
            seed=42,
        )

        label_converter = IndexToString(
            inputCol="prediction", outputCol="predicted_stress_change", labels=label_indexer.labels
        )

        pipeline = Pipeline(
            stages=[gender_indexer, activity_indexer, assembler, classifier, label_converter]
        )

        return pipeline, indexed_df

    def _build_sleep_model(self, df_ml: DataFrame) -> Tuple[Pipeline, DataFrame]:
        sleep_df = df_ml

        sleep_indexer = StringIndexer(
            inputCol="sleep_quality", outputCol="sleep_quality_idx", handleInvalid="skip"
        ).fit(sleep_df)

        gender_indexer = StringIndexer(inputCol="gender", outputCol="gender_idx", handleInvalid="keep")
        activity_indexer = StringIndexer(
            inputCol="activity_level", outputCol="activity_level_idx", handleInvalid="keep"
        )

        assembler = VectorAssembler(
            inputCols=[
                "steps",
                "exercise_minutes",
                "calories_burned",
                "ra30_steps",
                "ra30_calories",
                "ra30_ex_minutes",
                "ra30_stress",
                "stress_level",
                "yesterday_stress",
                "age",
                "heart_rate_avg",
                "caloric_balance",
                "weight_change_30d",
                "yesterday_sleep",
                "gender_idx",
                "activity_level_idx",
            ],
            outputCol="features",
        )

        classifier = RandomForestClassifier(
            labelCol="sleep_quality_idx",
            featuresCol="features",
            numTrees=60,
            maxDepth=8,
            seed=42,
        )

        label_converter = IndexToString(
            inputCol="prediction", outputCol="predicted_sleep_quality", labels=sleep_indexer.labels
        )

        pipeline = Pipeline(
            stages=[sleep_indexer, gender_indexer, activity_indexer, assembler, classifier, label_converter]
        )

        return pipeline, sleep_df

    def _build_weight_model(self, df_ml: DataFrame) -> Tuple[Pipeline, DataFrame]:
        weight_df = df_ml.withColumn(
            "weight_change_target",
            F.col("weight_change_30d"),
        )

        gender_indexer = StringIndexer(inputCol="gender", outputCol="gender_idx", handleInvalid="keep")

        assembler = VectorAssembler(
            inputCols=[
                "age",
                "steps",
                "stress_level",
                "heart_rate_avg",
                "calories_burned",
                "exercise_minutes",
                "gender_idx",
            ],
            outputCol="features",
        )

        regressor = RandomForestRegressor(
            labelCol="weight_change_target",
            featuresCol="features",
            numTrees=80,
            maxDepth=8,
            seed=42,
        )

        pipeline = Pipeline(stages=[gender_indexer, assembler, regressor])

        return pipeline, weight_df

    def write(self, df: DataFrame) -> None:
        output_uri = str(self.output_path)
        if isinstance(self.output_path, Path):
            self.output_path.parent.mkdir(parents=True, exist_ok=True)
        LOGGER.info("Writing ML predictions to %s", output_uri)
        df.write.mode("overwrite").parquet(output_uri)

    def run(self) -> DataFrame:
        fact_df = self.read_fact()
        df_ml = self.prepare_dataset(fact_df)

        # Stress change classifier
        stress_pipeline, stress_df = self._fit_stress_classifier(df_ml)
        stress_train, stress_test = stress_df.randomSplit([0.8, 0.2], seed=42)
        stress_model = stress_pipeline.fit(stress_train)
        stress_test_pred = stress_model.transform(stress_test)

        stress_acc = MulticlassClassificationEvaluator(
            labelCol="stress_label_idx", predictionCol="prediction", metricName="accuracy"
        ).evaluate(stress_test_pred)
        stress_f1 = MulticlassClassificationEvaluator(
            labelCol="stress_label_idx", predictionCol="prediction", metricName="f1"
        ).evaluate(stress_test_pred)
        LOGGER.info("Stress change classifier - accuracy: %.3f, f1: %.3f", stress_acc, stress_f1)

        stress_predictions = stress_model.transform(stress_df).select(
            "user_id",
            "date_key",
            F.col("predicted_stress_change"),
            F.col("stress_change_label").alias("actual_stress_change"),
        )

        # Sleep quality model
        sleep_pipeline, sleep_df = self._build_sleep_model(df_ml)
        sleep_train, sleep_test = sleep_df.randomSplit([0.8, 0.2], seed=42)
        sleep_model = sleep_pipeline.fit(sleep_train)
        sleep_test_pred = sleep_model.transform(sleep_test)

        sleep_acc = MulticlassClassificationEvaluator(
            labelCol="sleep_quality_idx", predictionCol="prediction", metricName="accuracy"
        ).evaluate(sleep_test_pred)
        sleep_f1 = MulticlassClassificationEvaluator(
            labelCol="sleep_quality_idx", predictionCol="prediction", metricName="f1"
        ).evaluate(sleep_test_pred)
        LOGGER.info("Sleep quality model - accuracy: %.3f, f1: %.3f", sleep_acc, sleep_f1)

        sleep_predictions = sleep_model.transform(sleep_df).select(
            "user_id",
            "date_key",
            F.col("predicted_sleep_quality"),
            F.col("sleep_quality").alias("actual_sleep_quality"),
        )

        # Weight change regression
        weight_pipeline, weight_df = self._build_weight_model(df_ml)
        weight_train, weight_test = weight_df.randomSplit([0.8, 0.2], seed=42)
        weight_model = weight_pipeline.fit(weight_train)
        weight_test_pred = weight_model.transform(weight_test)

        weight_rmse = RegressionEvaluator(
            labelCol="weight_change_target", predictionCol="prediction", metricName="rmse"
        ).evaluate(weight_test_pred)
        weight_mae = RegressionEvaluator(
            labelCol="weight_change_target", predictionCol="prediction", metricName="mae"
        ).evaluate(weight_test_pred)
        LOGGER.info("Weight change regression - RMSE: %.3f, MAE: %.3f", weight_rmse, weight_mae)

        weight_predictions = weight_model.transform(weight_df).select(
            "user_id",
            "date_key",
            F.round(F.col("prediction"), 3).alias("predicted_weight_change_30d"),
            F.round(F.col("weight_change_target"), 3).alias("actual_weight_change_30d"),
        )

        predictions = (
            stress_predictions
            .join(sleep_predictions, ["user_id", "date_key"], "outer")
            .join(weight_predictions, ["user_id", "date_key"], "outer")
        )

        self.write(predictions)
        return predictions.cache()


def create_spark(app_name: str = "fitness-ml") -> SparkSession:
    return SparkSession.builder.master("local[*]").appName(app_name).getOrCreate()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    spark = create_spark()
    try:
        pipeline = MLPipeline(spark=spark)
        predictions = pipeline.run()
        predictions.limit(5).show()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

