# pyrefly: ignore [missing-import]
from pyspark.sql.functions import col, to_date


class Standardizer:

    def standardize_transactions(self, df):

        # Bronze transaction schema uses `date` (see README/parquet columns), not `transaction_date`.
        date_col = "transaction_date" if "transaction_date" in df.columns else "date"

        return (
            df
            .withColumn("transaction_date", to_date(col(date_col)))
            .withColumn("amount", col("amount").cast("double"))
            .withColumn("transaction_id", col("transaction_id").cast("string"))
        )


    def standardize_refunds(self, df):

        # Bronze refunds schema uses `date` (not `refund_date`).
        # Even if `refund_date` doesn't exist, keep pipeline stable.
        if "refund_date" in df.columns:
            date_expr = to_date(col("refund_date"))
        else:
            date_expr = to_date(col("date"))

        return (
            df
            .withColumn("refund_date", date_expr)
            .withColumn("refund_amount", col("refund_amount").cast("double"))
            .withColumn("transaction_id", col("transaction_id").cast("string"))
        )





    def standardize_chargebacks(self, df):

        # Bronze chargebacks schema uses `date` (not `chargeback_date`).
        if "chargeback_date" in df.columns:
            date_expr = to_date(col("chargeback_date"))
        else:
            date_expr = to_date(col("date"))

        return (
            df
            .withColumn("chargeback_date", date_expr)
            .withColumn("chargeback_amount", col("chargeback_amount").cast("double"))
            .withColumn("transaction_id", col("transaction_id").cast("string"))
        )


