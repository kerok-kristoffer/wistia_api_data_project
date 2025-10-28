import os
import platform
import sys

import pytest
from pyspark.sql import SparkSession
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


@pytest.fixture(scope="session")
def spark():
    # Skip all tests marked "spark" on Windows unless user opted in
    if platform.system() == "Windows" and not os.environ.get("SPARK_ON_WINDOWS"):
        pytest.skip(
            "Skipping Spark tests on Windows (set SPARK_ON_WINDOWS=1 to force).",
            allow_module_level=True,
        )

    # Use the project venv for both driver and worker
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    spark = (
        SparkSession.builder.master("local[1]")
        .appName("tests")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.execution.pyspark.udf.faulthandler.enabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()


@pytest.fixture(autouse=True)
def _aws_dummy_env(monkeypatch):
    # Safe defaults for tests; real creds come from contract tests only
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
