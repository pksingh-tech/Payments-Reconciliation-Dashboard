import os
from pathlib import Path

from utils.config_reader import load_config
from utils.spark_session import create_spark_session

from ingestion.bronze_load import BronzeLoader
from transformation.silver_transform import SilverTransformer
from transformation.gold_transform import GoldTransformer

from audit.audit_logger import (
    log_pipeline_start,
    log_pipeline_end
)

from audit.metadata_manager import MetadataManager


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def keep_spark_ui_open(spark):

    keep_open = os.getenv(
        "KEEP_SPARK_UI_OPEN",
        "true"
    ).lower()

    if keep_open in {
            "0",
            "false",
            "no",
            "n"
    }:
        return

    ui_url = spark.sparkContext.uiWebUrl or "http://localhost:4040"

    print("\nSpark UI is still running:")
    print(ui_url)
    print(
        "Press Enter here when you are done viewing the Spark UI."
    )

    try:
        input()
    except EOFError:
        pass


def create_directories():

    config = load_config(
        PROJECT_ROOT / "configs" / "app_config.yaml"
    )

    bronze_path = Path(
        config.get("storage", {}).get("bronze_path", "data/bronze")
    )
    silver_path = Path(
        config.get("storage", {}).get("silver_path", "data/silver")
    )
    gold_path = Path(
        config.get("storage", {}).get("gold_path", "data/gold")
    )

    if not bronze_path.is_absolute():
        bronze_path = PROJECT_ROOT / bronze_path

    if not silver_path.is_absolute():
        silver_path = PROJECT_ROOT / silver_path

    if not gold_path.is_absolute():
        gold_path = PROJECT_ROOT / gold_path

    directories = [
        PROJECT_ROOT / "logs",
        PROJECT_ROOT / "reports",
        PROJECT_ROOT / "data",
        bronze_path,
        silver_path,
        gold_path,
    ]

    for directory in directories:
        os.makedirs(directory, exist_ok=True)


def main():

    # 1. Setup
    create_directories()
    log_pipeline_start()

    # 2. Spark Session (ONLY ENGINE USED)
    spark = create_spark_session()

    # 3. Bronze Layer (PySpark DataFrames)
    bronze_loader = BronzeLoader(spark)

    transactions_df, refunds_df, chargebacks_df = bronze_loader.execute()

    print("\nBronze Layer Completed Successfully\n")

    # 4. Silver Layer (PySpark Transformations)
    silver_transformer = SilverTransformer(spark)

    base_df = silver_transformer.execute()

    print("\nSilver Layer Completed Successfully\n")

    # ONLY SAFE ACTION (Spark action, not Pandas)
    print(f"Base Reconciliation Rows: {base_df.count()}")

    # 5. Gold Layer - Fact & Dimension tables (for Snowflake + Power BI)
    gold_transformer = GoldTransformer(spark)
    gold_tables = gold_transformer.execute()

    print("\nGold Layer Completed Successfully\n")
    print(f"  fact_transaction rows: {gold_tables['fact_transaction'].count()}")
    print(f"  fact_refund rows:      {gold_tables['fact_refund'].count()}")
    print(f"  fact_chargeback rows:  {gold_tables['fact_chargeback'].count()}")

    # 6. Metrics (PySpark-only)
    metadata_manager = MetadataManager()


    total_records = sum(
        int(row["records_inserted"])
        for row in bronze_loader.bronze_audit_rows
        if row["status"] == "SUCCESS"
    )

    metadata_manager.save_metrics(
        total_records=total_records,
        status="SUCCESS"
    )

    # 7. End pipeline
    log_pipeline_end()

    print("\nPIPELINE COMPLETED SUCCESSFULLY")
    print(f"Total Bronze Records: {total_records}")
    print(
        "Bronze Audit Last Run: "
        f"{bronze_loader.audit_last_run_path}"
    )
    keep_spark_ui_open(spark)


if __name__ == "__main__":
    main()
