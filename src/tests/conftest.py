"""Pytest fixtures for Spark testing."""

from __future__ import annotations

import os
import shutil
import sys
from pathlib import Path
from typing import Iterator

import pytest
from pyspark.sql import SparkSession


PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"
for candidate in (PROJECT_ROOT, SRC_ROOT):
    if str(candidate) not in sys.path:
        sys.path.insert(0, str(candidate))

if os.getenv("FITNESS_STORAGE_MODE", "s3").lower() != "local":
    os.environ.setdefault("FITNESS_STORAGE_MODE", "local")


@pytest.fixture(scope="session")
def spark_session() -> Iterator[SparkSession]:
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("fitness-pipeline-tests")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    try:
        yield spark
    finally:
        spark.stop()


@pytest.fixture
def temp_dir(tmp_path: Path) -> Iterator[Path]:
    yield tmp_path
    shutil.rmtree(tmp_path, ignore_errors=True)

