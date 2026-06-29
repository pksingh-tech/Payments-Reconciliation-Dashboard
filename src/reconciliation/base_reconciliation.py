from pyspark.sql.functions import col
from pyspark.sql.functions import max as spark_max
from pyspark.sql.functions import sum as spark_sum


class BaseReconciliation:

    def build_base(self, transactions, refunds, chargebacks):

        txn = transactions.select(
            col("transaction_id"),
            col("amount").alias("txn_amount"),
            col("transaction_date")
        )

        ref = (
            refunds
            .groupBy("transaction_id")
            .agg(
                spark_sum("refund_amount").alias("refund_amount"),
                spark_max("refund_date").alias("refund_date")
            )
        )

        cb = (
            chargebacks
            .groupBy("transaction_id")
            .agg(
                spark_sum("chargeback_amount").alias("chargeback_amount"),
                spark_max("chargeback_date").alias("chargeback_date")
            )
        )

        base = txn \
            .join(ref, "transaction_id", "left") \
            .join(cb, "transaction_id", "left")

        return base
