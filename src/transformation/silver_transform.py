from pathlib import Path

from transformation.deduplication import Deduplicator
from transformation.standardization import Standardizer
from reconciliation.base_reconciliation import BaseReconciliation
from transformation.data_quality import DataQualityValidator
from utils.config_reader import load_config


PROJECT_ROOT = Path(__file__).resolve().parents[2]


class SilverTransformer:

    def __init__(self, spark):

        self.spark = spark

        self.config = load_config(
            PROJECT_ROOT / "configs" / "app_config.yaml"
        )

        self.bronze_path = self._project_path(
            self.config.get(
                "storage",
                {}
            ).get(
                "bronze_path",
                "data/bronze"
            )
        )

        self.silver_path = self._project_path(
            self.config.get(
                "storage",
                {}
            ).get(
                "silver_path",
                "data/silver"
            )
        )

        self.standardizer = Standardizer()
        self.deduplicator = Deduplicator()
        self.recon = BaseReconciliation()
        self.validator = DataQualityValidator()

    def _project_path(
            self,
            path
    ):

        resolved_path = Path(path)

        if resolved_path.is_absolute():
            return resolved_path

        return PROJECT_ROOT / resolved_path

    def _spark_path(
            self,
            path
    ):

        return Path(path).resolve().as_uri().replace("%3D", "=")

    def read_bronze(
            self,
            name
    ):
        from pyspark.sql.functions import col
        import os

        bronze_dir = self.bronze_path / name
        spark_dir = self._spark_path(bronze_dir)

        # Collect all parquet paths: flat files + partitioned subdirs
        parts = []
        for entry in os.scandir(str(bronze_dir)):
            if entry.is_file() and entry.name.endswith(".parquet"):
                parts.append(self._spark_path(entry.path))
            elif entry.is_dir() and not entry.name.startswith("_") and not entry.name.startswith("."):
                parts.append(self._spark_path(entry.path))

        if not parts:
            # Fallback: read whole directory
            return (
                self.spark.read
                .option("recursiveFileLookup", "true")
                .parquet(spark_dir)
            )

        # Read each part separately and union — avoids TIMESTAMP vs TIMESTAMP_NTZ merge error
        dfs = []
        for part in parts:
            df = self.spark.read.option("recursiveFileLookup", "true").parquet(part)
            # Normalise load_timestamp to TIMESTAMP so all parts have same type
            if "load_timestamp" in df.columns:
                df = df.withColumn("load_timestamp", col("load_timestamp").cast("timestamp"))
            dfs.append(df)

        result = dfs[0]
        for df in dfs[1:]:
            result = result.unionByName(df, allowMissingColumns=True)

        return result



    def write_silver(
            self,
            df,
            name
    ):

        out_path = self.silver_path / name

        (
            df.write
            .mode("overwrite")
            .option("compression", "snappy")
            .parquet(
                self._spark_path(out_path)
            )
        )

        return df

    def execute(self):

        transactions = self.read_bronze("transactions")
        refunds = self.read_bronze("refunds")
        chargebacks = self.read_bronze("chargebacks")

        transactions = (
            self.standardizer
            .standardize_transactions(transactions)
        )
        refunds = (
            self.standardizer
            .standardize_refunds(refunds)
        )
        chargebacks = (
            self.standardizer
            .standardize_chargebacks(chargebacks)
        )

        transactions = (
            self.deduplicator
            .remove_duplicates(
                transactions,
                "transaction_id"
            )
        )
        refunds = (
            self.deduplicator
            .remove_duplicates(
                refunds,
                "refund_id"
            )
        )
        chargebacks = (
            self.deduplicator
            .remove_duplicates(
                chargebacks,
                "chargeback_id"
            )
        )

        self.write_silver(
            transactions,
            "transactions"
        )
        self.write_silver(
            refunds,
            "refunds"
        )
        self.write_silver(
            chargebacks,
            "chargebacks"
        )

        base_df = self.recon.build_base(
            transactions,
            refunds,
            chargebacks
        )

        self.write_silver(
            base_df,
            "reconciliation_base"
        )

        # Run Data Quality Validations
        self.validator.validate(
            transactions,
            refunds,
            chargebacks,
            base_df
        )

        return base_df
