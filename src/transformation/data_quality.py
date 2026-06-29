import json
import os
from datetime import datetime
from pathlib import Path
from pyspark.sql.functions import col, coalesce, lit

PROJECT_ROOT = Path(__file__).resolve().parents[2]


class DataQualityValidator:

    def __init__(self):

        self.report_dir = PROJECT_ROOT / "reports" / "data_quality"
        self.report_dir.mkdir(
            parents=True,
            exist_ok=True
        )

    def validate(
            self,
            transactions,
            refunds,
            chargebacks,
            base
    ):

        checks = []

        # 1. Uniqueness Checks
        # Transactions Uniqueness
        txn_count = transactions.count()
        txn_distinct = (
            transactions
            .select("transaction_id")
            .distinct()
            .count()
        )
        txn_dup_count = txn_count - txn_distinct
        checks.append(
            {
                "name": "transactions_uniqueness",
                "description": "Check if transaction_id is unique in silver transactions",
                "metric_value": txn_dup_count,
                "status": "PASS" if txn_dup_count == 0 else "FAIL"
            }
        )

        # Refunds Uniqueness
        ref_count = refunds.count()
        ref_distinct = (
            refunds
            .select("refund_id")
            .distinct()
            .count()
        )
        ref_dup_count = ref_count - ref_distinct
        checks.append(
            {
                "name": "refunds_uniqueness",
                "description": "Check if refund_id is unique in silver refunds",
                "metric_value": ref_dup_count,
                "status": "PASS" if ref_dup_count == 0 else "FAIL"
            }
        )

        # Chargebacks Uniqueness
        cb_count = chargebacks.count()
        cb_distinct = (
            chargebacks
            .select("chargeback_id")
            .distinct()
            .count()
        )
        cb_dup_count = cb_count - cb_distinct
        checks.append(
            {
                "name": "chargebacks_uniqueness",
                "description": "Check if chargeback_id is unique in silver chargebacks",
                "metric_value": cb_dup_count,
                "status": "PASS" if cb_dup_count == 0 else "FAIL"
            }
        )

        # 2. Referential Integrity Checks
        # Refunds Referential Integrity
        orphaned_refunds = (
            refunds
            .join(
                transactions,
                "transaction_id",
                "left_anti"
            )
            .count()
        )
        checks.append(
            {
                "name": "refunds_referential_integrity",
                "description": "Check if every refund's transaction_id exists in transactions",
                "metric_value": orphaned_refunds,
                "status": "PASS" if orphaned_refunds == 0 else "FAIL"
            }
        )

        # Chargebacks Referential Integrity
        orphaned_cbs = (
            chargebacks
            .join(
                transactions,
                "transaction_id",
                "left_anti"
            )
            .count()
        )
        checks.append(
            {
                "name": "chargebacks_referential_integrity",
                "description": "Check if every chargeback's transaction_id exists in transactions",
                "metric_value": orphaned_cbs,
                "status": "PASS" if orphaned_cbs == 0 else "FAIL"
            }
        )

        # 3. Business Logic Rules
        # Refund Amount <= Transaction Amount
        invalid_ref_amt = (
            refunds
            .join(
                transactions,
                "transaction_id",
                "inner"
            )
            .filter(col("refund_amount") > col("amount"))
            .count()
        )
        checks.append(
            {
                "name": "refund_amount_rule",
                "description": "Check if refund_amount is less than or equal to transaction amount",
                "metric_value": invalid_ref_amt,
                "status": "PASS" if invalid_ref_amt == 0 else "FAIL"
            }
        )

        # Chargeback Amount <= Transaction Amount
        invalid_cb_amt = (
            chargebacks
            .join(
                transactions,
                "transaction_id",
                "inner"
            )
            .filter(col("chargeback_amount") > col("amount"))
            .count()
        )
        checks.append(
            {
                "name": "chargeback_amount_rule",
                "description": "Check if chargeback_amount is less than or equal to transaction amount",
                "metric_value": invalid_cb_amt,
                "status": "PASS" if invalid_cb_amt == 0 else "FAIL"
            }
        )

        # Refund Date >= Transaction Date
        invalid_ref_date = (
            refunds
            .join(
                transactions,
                "transaction_id",
                "inner"
            )
            .filter(col("refund_date") < col("transaction_date"))
            .count()
        )
        checks.append(
            {
                "name": "refund_date_rule",
                "description": "Check if refund_date is greater than or equal to transaction_date",
                "metric_value": invalid_ref_date,
                "status": "PASS" if invalid_ref_date == 0 else "FAIL"
            }
        )

        # Chargeback Date >= Transaction Date
        invalid_cb_date = (
            chargebacks
            .join(
                transactions,
                "transaction_id",
                "inner"
            )
            .filter(col("chargeback_date") < col("transaction_date"))
            .count()
        )
        checks.append(
            {
                "name": "chargeback_date_rule",
                "description": "Check if chargeback_date is greater than or equal to transaction_date",
                "metric_value": invalid_cb_date,
                "status": "PASS" if invalid_cb_date == 0 else "FAIL"
            }
        )

        # Null / non-positive amount checks
        null_txn_amt = transactions.filter(col("amount").isNull()).count()
        checks.append(
            {
                "name": "transaction_amount_not_null",
                "description": "Check if transaction amount is present (not null)",
                "metric_value": null_txn_amt,
                "status": "PASS" if null_txn_amt == 0 else "FAIL",
            }
        )

        non_pos_txn = (
            transactions
            .filter(col("amount").isNull() | (col("amount") <= 0))
            .count()
        )
        non_pos_ref = (
            refunds
            .filter(col("refund_amount") <= 0)
            .count()
        )
        non_pos_cb = (
            chargebacks
            .filter(col("chargeback_amount") <= 0)
            .count()
        )
        total_non_pos = non_pos_txn + non_pos_ref + non_pos_cb
        checks.append(
            {
                "name": "positive_amounts_rule",
                "description": "Check if all amounts (transactions, refunds, chargebacks) are positive",
                "metric_value": total_non_pos,
                "status": "PASS" if total_non_pos == 0 else "FAIL"
            }
        )

        # 4. Reconciliation Checks
        # Reconciled Total Amounts Rule (Refund + Chargeback <= Transaction Amount)
        invalid_recon_amt = (
            base
            .filter(
                (
                    coalesce(col("refund_amount"), lit(0.0))
                    + coalesce(col("chargeback_amount"), lit(0.0))
                )
                > col("txn_amount")
            )
            .count()
        )
        checks.append(
            {
                "name": "reconciled_total_amounts_rule",
                "description": "Check if sum of refunds and chargebacks is <= transaction amount",
                "metric_value": invalid_recon_amt,
                "status": "PASS" if invalid_recon_amt == 0 else "FAIL"
            }
        )

        # Reconciliation Row Count Integrity
        recon_rows = base.count()
        txn_rows = transactions.count()
        row_diff = abs(recon_rows - txn_rows)
        checks.append(
            {
                "name": "reconciliation_row_count_integrity",
                "description": "Check if base reconciliation table row count matches transactions count",
                "metric_value": row_diff,
                "status": "PASS" if row_diff == 0 else "FAIL"
            }
        )

        # Generate report metrics
        total_checks = len(checks)
        failed_checks = sum(1 for c in checks if c["status"] == "FAIL")
        passed_checks = total_checks - failed_checks
        overall_status = "FAIL" if failed_checks > 0 else "PASS"

        report = {
            "timestamp": datetime.now().isoformat(),
            "status": overall_status,
            "summary": {
                "total_checks": total_checks,
                "passed": passed_checks,
                "failed": failed_checks
            },
            "checks": checks
        }

        # Save to JSON
        report_file = self.report_dir / "dq_report.json"
        with open(report_file, "w") as f:
            json.dump(report, f, indent=4)

        # Save to human-readable TXT
        txt_report_file = self.report_dir / "dq_report.txt"
        with open(txt_report_file, "w") as f:
            f.write("=" * 80 + "\n")
            f.write("                   DATA QUALITY REPORT\n")
            f.write("=" * 80 + "\n")
            f.write(f"Timestamp      : {report['timestamp']}\n")
            f.write(f"Overall Status : {overall_status}\n")
            f.write(f"Total Checks   : {total_checks}\n")
            f.write(f"Passed         : {passed_checks}\n")
            f.write(f"Failed         : {failed_checks}\n")
            f.write("-" * 80 + "\n")
            f.write("\n PASSED CHECKS\n")
            f.write("-" * 80 + "\n")
            for check in checks:
                if check["status"] == "PASS":
                    f.write(
                        f"  [PASS]  {check['name']}\n"
                        f"          {check['description']}\n"
                        f"          Failing rows: {check['metric_value']}\n\n"
                    )
            f.write("-" * 80 + "\n")
            f.write("\n FAILED CHECKS\n")
            f.write("-" * 80 + "\n")
            if failed_checks == 0:
                f.write("  No failed checks.\n\n")
            else:
                for check in checks:
                    if check["status"] == "FAIL":
                        f.write(
                            f"  [FAIL]  {check['name']}\n"
                            f"          {check['description']}\n"
                            f"          Failing rows: {check['metric_value']}\n\n"
                        )
            f.write("=" * 80 + "\n")

        print(f"\n[DQ] Report saved -> {report_file}")
        print(f"[DQ] Report saved -> {txt_report_file}")

        # Console logs
        print("\n" + "="*80)
        print("                      DATA QUALITY REPORT SUMMARY")
        print("="*80)
        print(f"Overall Status: {overall_status}")
        print(f"Passed Checks : {passed_checks}/{total_checks}")
        print(f"Failed Checks : {failed_checks}/{total_checks}")
        print("-"*80)
        for check in checks:
            marker = "[PASS]" if check["status"] == "PASS" else "[FAIL]"
            print(
                f"{marker} {check['name']}: "
                f"{check['metric_value']} failing rows - {check['description']}"
            )
        print("="*80 + "\n")

        return report
