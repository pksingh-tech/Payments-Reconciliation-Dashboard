import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
LOCAL_HADOOP_HOME = PROJECT_ROOT / ".hadoop"

if os.name == "nt":
    os.environ.setdefault(
        "HADOOP_HOME",
        str(LOCAL_HADOOP_HOME)
    )
    os.environ.setdefault(
        "hadoop.home.dir",
        str(LOCAL_HADOOP_HOME)
    )
    # Add HADOOP_HOME/bin to Windows PATH so JVM can load hadoop.dll
    hadoop_bin = str(LOCAL_HADOOP_HOME / "bin")
    if hadoop_bin not in os.environ.get("PATH", ""):
        os.environ["PATH"] = hadoop_bin + os.pathsep + os.environ.get("PATH", "")

from pyspark.sql import SparkSession


def create_spark_session():

    # Workaround for Spark/Windows native-hadoop library lookup issues
    os.environ.setdefault("HADOOP_HOME", str(LOCAL_HADOOP_HOME))
    os.environ.setdefault("HADOOP_OPTS", "-Djava.library.path=")

    # Ensure Spark Python workers use the venv python (critical on Windows)
    python_exe = sys.executable
    os.environ.setdefault("PYSPARK_PYTHON", python_exe)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", python_exe)

    spark_event_log_dir = PROJECT_ROOT / "logs" / "spark-events"
    spark_event_log_dir.mkdir(
        parents=True,
        exist_ok=True
    )

    spark = (

        SparkSession.builder
        .appName("Payments-Reconciliation")
        .master("local[*]")
        .config(
            "spark.jars.packages",
            "org.postgresql:postgresql:42.7.4"
        )
        .config(
            "spark.sql.shuffle.partitions",
            "8"
        )
        .config(
            "spark.sql.execution.arrow.pyspark.enabled",
            "false"
        )
        .config(
            "spark.driver.memory",
            "2g"
        )
        .config(
            "spark.executor.memory",
            "2g"
        )
        .config(
            "spark.eventLog.enabled",
            "true"
        )
        .config(
            "spark.eventLog.dir",
            spark_event_log_dir.resolve().as_uri()
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark
