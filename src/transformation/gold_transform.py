import shutil
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast, coalesce, lit
from pyspark.sql.window import Window

from utils.config_reader import load_config


PROJECT_ROOT = Path(__file__).resolve().parents[2]


class GoldTransformer:

    def __init__(self, spark):
        self.spark = spark
        self.config = load_config(
            PROJECT_ROOT / "configs" / "app_config.yaml"
        )

        self.silver_path = self._project_path(
            self.config.get("storage", {}).get("silver_path", "data/silver")
        )

        self.gold_path = self._project_path(
            self.config.get("storage", {}).get("gold_path", "data/gold")
        )

    def _project_path(self, path):
        resolved = Path(path)
        if resolved.is_absolute():
            return resolved
        return PROJECT_ROOT / resolved

    def _spark_path(self, path):
        return Path(path).resolve().as_uri().replace("%3D", "=")

    def _write_gold(self, df, name):
        """Write a single flat parquet file under data/gold/ (Snowflake loader compatible)."""
        self.gold_path.mkdir(parents=True, exist_ok=True)
        out_path = self.gold_path / f"{name}.parquet"
        tmp_dir = self.gold_path / f"_tmp_{name}"

        if tmp_dir.exists():
            shutil.rmtree(tmp_dir)

        (
            df.coalesce(1)
            .write
            .mode("overwrite")
            .option("compression", "snappy")
            .parquet(str(tmp_dir.resolve()))
        )

        part_files = sorted(tmp_dir.glob("part-*.parquet"))
        if not part_files:
            raise RuntimeError(f"No parquet part file produced for {name}")

        if out_path.exists():
            out_path.unlink()

        shutil.move(str(part_files[0]), str(out_path))
        shutil.rmtree(tmp_dir)

        row_count = df.count()
        print(f"[GOLD] Wrote {name} via Spark ({row_count} rows)")
        return df

    def _read_silver(self, name):
        path = self._spark_path(self.silver_path / name)
        return (
            self.spark.read
            .option("recursiveFileLookup", "true")
            .parquet(path)
        )

    def _build_dim_date(self, date_bounds_df):
        """Generate a complete date dimension table using Spark SQL only."""
        return (
            date_bounds_df
            .withColumn("full_date", F.explode(F.sequence(F.col("min_d"), F.col("max_d"))))
            .select(
                F.date_format("full_date", "yyyyMMdd").cast("int").alias("date_key"),
                F.col("full_date"),
                F.year("full_date").alias("year"),
                F.quarter("full_date").alias("quarter"),
                F.concat(lit("Q"), F.quarter("full_date").cast("string")).alias("quarter_name"),
                F.month("full_date").alias("month"),
                F.date_format("full_date", "MMMM").alias("month_name"),
                F.date_format("full_date", "MMM").alias("month_abbr"),
                F.weekofyear("full_date").alias("week_of_year"),
                (F.pmod(F.dayofweek("full_date") + lit(5), lit(7)) + lit(1)).alias("day_of_week"),
                F.date_format("full_date", "EEEE").alias("day_name"),
                F.dayofmonth("full_date").alias("day_of_month"),
                F.when(F.dayofweek("full_date").isin(1, 7), lit(1)).otherwise(lit(0)).alias("is_weekend"),
                F.date_format("full_date", "yyyy-MM").alias("year_month"),
            )
        )

    def _date_column(self, df, preferred, fallback):
        return preferred if preferred in df.columns else fallback

    def execute(self):
        """Build gold layer using PySpark."""
        print("[GOLD] Building gold layer using PySpark...")
        self.gold_path.mkdir(parents=True, exist_ok=True)

        tx = self._read_silver("transactions")
        rf = self._read_silver("refunds")
        cb = self._read_silver("chargebacks")

        tx_date_col = self._date_column(tx, "transaction_date", "date")
        rf_date_col = self._date_column(rf, "refund_date", "date")
        cb_date_col = self._date_column(cb, "chargeback_date", "date")

        tx = tx.withColumn("transaction_date", F.to_date(F.col(tx_date_col)))
        rf = rf.withColumn("refund_date", F.to_date(F.col(rf_date_col)))
        cb = cb.withColumn("chargeback_date", F.to_date(F.col(cb_date_col)))

        dim_merchant = (
            tx.select("merchant_id")
            .filter(F.col("merchant_id").isNotNull())
            .distinct()
            .orderBy("merchant_id")
            .withColumn("merchant_key", F.row_number().over(Window.orderBy("merchant_id")))
            .withColumn("merchant_name", F.col("merchant_id"))
            .withColumn("merchant_category", lit("Unknown"))
            .withColumn("status", lit("Active"))
        )

        dim_currency = (
            tx.select("currency")
            .filter(F.col("currency").isNotNull())
            .distinct()
            .orderBy("currency")
            .withColumn("currency_key", F.row_number().over(Window.orderBy("currency")))
            .withColumn("currency_code", F.col("currency"))
            .withColumn(
                "currency_name",
                F.when(F.col("currency") == "USD", lit("US Dollar")).otherwise(F.col("currency")),
            )
            .withColumn("decimal_places", lit(2))
        )

        dim_status = (
            tx.select("status")
            .filter(F.col("status").isNotNull())
            .distinct()
            .orderBy("status")
            .withColumn("status_key", F.row_number().over(Window.orderBy("status")))
            .withColumn("status_code", F.col("status"))
            .withColumn("status_name", F.initcap(F.col("status")))
            .withColumn(
                "status_category",
                F.when(F.col("status").isin("settled", "completed"), lit("SUCCESS"))
                .when(F.col("status").isin("failed", "voided"), lit("FAILURE"))
                .otherwise(lit("IN_PROGRESS")),
            )
        )

        dim_refund_reason = (
            rf.select("reason")
            .filter(F.col("reason").isNotNull())
            .distinct()
            .orderBy("reason")
            .withColumn("refund_reason_key", F.row_number().over(Window.orderBy("reason")))
            .withColumn("reason_code", F.col("reason"))
            .withColumn(
                "reason_name",
                F.regexp_replace(F.initcap(F.regexp_replace(F.col("reason"), "_", " ")), "  ", " "),
            )
            .withColumn(
                "reason_category",
                F.when(F.lower(F.col("reason")).contains("fraud"), lit("HIGH_RISK"))
                .when(F.col("reason").isin("defective_item", "item_not_received"), lit("PRODUCT"))
                .when(F.col("reason").isin("customer_request", "goodwill"), lit("CUSTOMER"))
                .otherwise(lit("OPERATIONAL")),
            )
        )

        dim_cb_reason = (
            cb.select("reason_code")
            .filter(F.col("reason_code").isNotNull())
            .distinct()
            .orderBy("reason_code")
            .withColumn("chargeback_reason_key", F.row_number().over(Window.orderBy("reason_code")))
            .withColumn("reason_name", F.col("reason_code"))
            .withColumn("source_system", lit("CARD_NETWORK"))
        )

        date_bounds = (
            tx.select(F.col("transaction_date").alias("d"))
            .union(rf.select(F.col("refund_date").alias("d")))
            .union(cb.select(F.col("chargeback_date").alias("d")))
            .filter(F.col("d").isNotNull())
            .agg(
                F.min("d").alias("min_d"),
                F.max("d").alias("max_d"),
            )
        )
        dim_date = self._build_dim_date(date_bounds)

        rf_agg = rf.groupBy("transaction_id").agg(
            F.sum("refund_amount").alias("refund_amount"),
            F.count("refund_id").alias("refund_count"),
        )
        cb_agg = cb.groupBy("transaction_id").agg(
            F.sum("chargeback_amount").alias("chargeback_amount"),
            F.count("chargeback_id").alias("chargeback_count"),
        )

        null_amount_count = tx.filter(F.col("amount").isNull()).count()
        if null_amount_count > 0:
            print(
                f"[GOLD] {null_amount_count} transaction(s) with null amount; "
                "gross_amount set to 0.0 and risk_category=INVALID_AMOUNT"
            )

        f_tx = (
            tx.alias("tx")
            .join(rf_agg.alias("rf"), "transaction_id", "left")
            .join(cb_agg.alias("cb"), "transaction_id", "left")
            .withColumn("gross_amount", coalesce(F.col("amount"), lit(0.0)))
            .withColumn("refund_amount", coalesce(F.col("refund_amount"), lit(0.0)))
            .withColumn("chargeback_amount", coalesce(F.col("chargeback_amount"), lit(0.0)))
            .withColumn("refund_count", coalesce(F.col("refund_count"), lit(0)).cast("int"))
            .withColumn("chargeback_count", coalesce(F.col("chargeback_count"), lit(0)).cast("int"))
            .withColumn(
                "net_amount",
                F.col("gross_amount") - F.col("refund_amount") - F.col("chargeback_amount"),
            )
            .withColumn("has_refund", (F.col("refund_amount") > 0).cast("int"))
            .withColumn("has_chargeback", (F.col("chargeback_amount") > 0).cast("int"))
            .withColumn(
                "risk_category",
                F.when(F.col("amount").isNull(), lit("INVALID_AMOUNT"))
                .when(F.col("chargeback_amount") > 0, lit("CHARGEBACK"))
                .when(F.col("refund_amount") > F.col("gross_amount") * 0.5, lit("HIGH_REFUND"))
                .when(F.col("status").isin("failed", "voided"), lit("FAILED"))
                .otherwise(lit("NORMAL")),
            )
            .join(
                broadcast(dim_date.select("full_date", "date_key")),
                F.col("transaction_date") == F.col("full_date"),
                "left",
            )
            .join(
                broadcast(dim_merchant.select("merchant_id", "merchant_key")),
                "merchant_id",
                "left",
            )
            .join(
                broadcast(dim_currency.select("currency_code", "currency_key")),
                F.col("currency") == F.col("currency_code"),
                "left",
            )
            .join(
                broadcast(dim_status.select("status_code", "status_key")),
                F.col("status") == F.col("status_code"),
                "left",
            )
        )

        fact_transaction = (
            f_tx.withColumn("transaction_key", F.row_number().over(Window.orderBy("transaction_id")))
            .select(
                "transaction_key",
                "transaction_id",
                "date_key",
                "merchant_key",
                "currency_key",
                "status_key",
                "transaction_date",
                "gross_amount",
                "refund_amount",
                "chargeback_amount",
                "net_amount",
                "refund_count",
                "chargeback_count",
                "has_refund",
                "has_chargeback",
                "risk_category",
                "source_system",
            )
        )

        rf_with_currency = rf
        if "currency" not in rf.columns:
            rf_with_currency = rf.withColumn("currency", lit(None).cast("string"))

        fact_refund = (
            rf_with_currency.alias("rf")
            .join(
                broadcast(dim_date.select("full_date", "date_key")),
                F.col("refund_date") == F.col("full_date"),
                "left",
            )
            .withColumnRenamed("date_key", "refund_date_key")
            .join(
                broadcast(dim_merchant.select("merchant_id", "merchant_key")),
                "merchant_id",
                "left",
            )
            .join(
                broadcast(dim_currency.select("currency_code", "currency_key")),
                F.col("currency") == F.col("currency_code"),
                "left",
            )
            .withColumn("currency_key", coalesce(F.col("currency_key"), lit(1)))
            .join(
                broadcast(dim_refund_reason.select("reason_code", "refund_reason_key")),
                F.col("reason") == F.col("reason_code"),
                "left",
            )
            .withColumn("refund_key", F.row_number().over(Window.orderBy("refund_id")))
            .select(
                "refund_key",
                "refund_id",
                "transaction_id",
                "refund_date_key",
                "merchant_key",
                "currency_key",
                "refund_reason_key",
                "refund_date",
                "refund_amount",
                "reason",
                "source_system",
            )
        )

        cb_with_currency = cb
        if "currency" not in cb.columns:
            cb_with_currency = cb.withColumn("currency", lit(None).cast("string"))

        fact_chargeback = (
            cb_with_currency.alias("cb")
            .join(
                broadcast(dim_date.select("full_date", "date_key")),
                F.col("chargeback_date") == F.col("full_date"),
                "left",
            )
            .withColumnRenamed("date_key", "chargeback_date_key")
            .join(
                broadcast(dim_merchant.select("merchant_id", "merchant_key")),
                "merchant_id",
                "left",
            )
            .join(
                broadcast(dim_currency.select("currency_code", "currency_key")),
                F.col("currency") == F.col("currency_code"),
                "left",
            )
            .withColumn("currency_key", coalesce(F.col("currency_key"), lit(1)))
            .join(
                broadcast(dim_cb_reason.select("reason_code", "chargeback_reason_key")),
                "reason_code",
                "left",
            )
            .withColumn("chargeback_key", F.row_number().over(Window.orderBy("chargeback_id")))
            .select(
                "chargeback_key",
                "chargeback_id",
                "transaction_id",
                "chargeback_date_key",
                "merchant_key",
                "currency_key",
                "chargeback_reason_key",
                "chargeback_date",
                "chargeback_amount",
                "reason_code",
                "source_system",
            )
        )

        self._write_gold(dim_date, "dim_date")
        self._write_gold(dim_merchant, "dim_merchant")
        self._write_gold(dim_currency, "dim_currency")
        self._write_gold(dim_status, "dim_transaction_status")
        self._write_gold(dim_refund_reason, "dim_refund_reason")
        self._write_gold(dim_cb_reason, "dim_chargeback_reason")
        self._write_gold(fact_transaction, "fact_transaction")
        self._write_gold(fact_refund, "fact_refund")
        self._write_gold(fact_chargeback, "fact_chargeback")

        print("\n[GOLD] Gold layer completed successfully (PySpark)")
        print("  Dims : dim_date, dim_merchant, dim_currency, dim_transaction_status, dim_refund_reason, dim_chargeback_reason")
        print("  Facts: fact_transaction, fact_refund, fact_chargeback")
        print(f"  fact_transaction rows: {fact_transaction.count()}")
        print(f"  fact_refund rows:      {fact_refund.count()}")
        print(f"  fact_chargeback rows:  {fact_chargeback.count()}")

        return {
            "dim_date": dim_date,
            "dim_merchant": dim_merchant,
            "dim_currency": dim_currency,
            "dim_status": dim_status,
            "dim_refund_reason": dim_refund_reason,
            "dim_cb_reason": dim_cb_reason,
            "fact_transaction": fact_transaction,
            "fact_refund": fact_refund,
            "fact_chargeback": fact_chargeback,
        }


if __name__ == "__main__":
    from utils.spark_session import create_spark_session
    spark = create_spark_session()
    gt = GoldTransformer(spark)
    gt.execute()
    spark.stop()