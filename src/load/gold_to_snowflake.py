"""
Gold layer -> Snowflake loader.
Uses snowflake-connector-python + pandas.

Usage (after setting env / .env):
  python -m src.load.gold_to_snowflake
  python -m src.load.gold_to_snowflake --check-env

Required env vars (or use .env):
  SNOWFLAKE_ACCOUNT
  SNOWFLAKE_USER
  SNOWFLAKE_PASSWORD
  SNOWFLAKE_WAREHOUSE
  SNOWFLAKE_DATABASE   (defaults to PAYMENTS_DB)
  SNOWFLAKE_SCHEMA     (defaults to GOLD)
  SNOWFLAKE_ROLE       (optional)
"""

import argparse
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
import pandas as pd
from snowflake.connector.pandas_tools import write_pandas
import snowflake.connector

PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(PROJECT_ROOT / ".env")

GOLD_PATH = PROJECT_ROOT / "data" / "gold"
ENV_FILE = PROJECT_ROOT / ".env"
ENV_EXAMPLE = PROJECT_ROOT / ".env.example"

REQUIRED_ENV_VARS = (
    "SNOWFLAKE_ACCOUNT",
    "SNOWFLAKE_USER",
    "SNOWFLAKE_PASSWORD",
    "SNOWFLAKE_WAREHOUSE",
)

TABLES = [
    "dim_date",
    "dim_merchant",
    "dim_currency",
    "dim_transaction_status",
    "dim_refund_reason",
    "dim_chargeback_reason",
    "fact_transaction",
    "fact_refund",
    "fact_chargeback",
]

# Parquet -> Snowflake column alignment (sql/snowflake_gold_ddl.sql)
TABLE_COLUMNS: dict[str, list[str]] = {
    "dim_date": [
        "date_key", "full_date", "year", "quarter", "quarter_name", "month",
        "month_name", "month_abbr", "week_of_year", "day_of_week", "day_name",
        "day_of_month", "is_weekend", "year_month",
    ],
    "dim_merchant": [
        "merchant_key", "merchant_id", "merchant_name", "merchant_category", "status",
    ],
    "dim_currency": [
        "currency_key", "currency_code", "currency_name", "decimal_places",
    ],
    "dim_transaction_status": [
        "status_key", "status_code", "status_name", "status_category",
    ],
    "dim_refund_reason": [
        "refund_reason_key", "reason_code", "reason_name", "reason_category",
    ],
    "dim_chargeback_reason": [
        "chargeback_reason_key", "reason_code", "reason_name", "source_system",
    ],
    "fact_transaction": [
        "transaction_key", "transaction_id", "date_key", "merchant_key",
        "currency_key", "status_key", "transaction_date", "gross_amount",
        "refund_amount", "chargeback_amount", "net_amount", "refund_count",
        "chargeback_count", "has_refund", "has_chargeback", "risk_category",
        "source_system",
    ],
    "fact_refund": [
        "refund_key", "refund_id", "transaction_id", "refund_date_key",
        "merchant_key", "currency_key", "refund_reason_key", "refund_date",
        "refund_amount", "refund_reason_raw", "source_system",
    ],
    "fact_chargeback": [
        "chargeback_key", "chargeback_id", "transaction_id", "chargeback_date_key",
        "merchant_key", "currency_key", "chargeback_reason_key", "chargeback_date",
        "chargeback_amount", "reason_code", "source_system",
    ],
}

COLUMN_RENAMES: dict[str, dict[str, str]] = {
    "fact_refund": {"reason": "refund_reason_raw"},
}

# Existing clustered fact tables hit a write_pandas TRUNCATE+COPY bug on some
# Snowflake accounts; stage via auto-created temp table instead.
FACT_TABLES = {"fact_transaction", "fact_refund", "fact_chargeback"}

# Snowflake NOT NULL columns that may be null in parquet — fill before load.
NOT_NULL_DEFAULTS: dict[str, dict[str, object]] = {
    "fact_transaction": {"gross_amount": 0.0},
    "fact_refund": {"refund_amount": 0.0},
    "fact_chargeback": {"chargeback_amount": 0.0},
}


def _env(name: str, default: str | None = None) -> str | None:
    value = os.getenv(name)
    if value is None or not value.strip():
        return default
    return value.strip()


def get_snowflake_config() -> dict[str, str | None]:
    return {
        "account": _env("SNOWFLAKE_ACCOUNT"),
        "user": _env("SNOWFLAKE_USER"),
        "password": _env("SNOWFLAKE_PASSWORD"),
        "warehouse": _env("SNOWFLAKE_WAREHOUSE"),
        "database": _env("SNOWFLAKE_DATABASE", "PAYMENTS_DB"),
        "schema": _env("SNOWFLAKE_SCHEMA", "GOLD"),
        "role": _env("SNOWFLAKE_ROLE"),
    }


def validate_snowflake_env() -> list[str]:
    missing = []
    for var in REQUIRED_ENV_VARS:
        if not _env(var):
            missing.append(var)
    return missing


def print_env_help(missing: list[str]) -> None:
    print("\nSnowflake credentials are not configured.")
    print(f"  Env file: {ENV_FILE}")
    if missing:
        print(f"  Missing: {', '.join(missing)}")
    print("\nFill in your Snowflake trial/workspace values in .env:")
    print("  SNOWFLAKE_ACCOUNT=<account_locator>.<region>   # e.g. xy12345.us-east-1")
    print("  SNOWFLAKE_USER=<your_username>")
    print("  SNOWFLAKE_PASSWORD=<your_password>")
    print("  SNOWFLAKE_WAREHOUSE=COMPUTE_WH")
    print("  SNOWFLAKE_DATABASE=PAYMENTS_DB")
    print("  SNOWFLAKE_SCHEMA=GOLD")
    print("\nThen create tables in Snowflake:")
    print("  1. Run sql/snowflake_gold_ddl.sql in a Snowflake worksheet")
    print("  2. Re-run: python src/load/gold_to_snowflake.py")
    if ENV_EXAMPLE.exists():
        print(f"\nSee {ENV_EXAMPLE} for a full template.")


def get_connection():
    missing = validate_snowflake_env()
    if missing:
        print_env_help(missing)
        raise RuntimeError(
            "Missing required Snowflake env vars: "
            + ", ".join(missing)
        )

    cfg = get_snowflake_config()
    connect_kwargs = {
        "account": cfg["account"],
        "user": cfg["user"],
        "password": cfg["password"],
        "warehouse": cfg["warehouse"],
        "database": cfg["database"],
        "schema": cfg["schema"],
    }
    if cfg["role"]:
        connect_kwargs["role"] = cfg["role"]

    conn = snowflake.connector.connect(**connect_kwargs)
    return conn

INT_COLUMNS = {
    "dim_date": {
        "date_key", "year", "quarter", "month", "week_of_year", "day_of_week",
        "day_of_month", "is_weekend",
    },
    "dim_merchant": {"merchant_key"},
    "dim_currency": {"currency_key", "decimal_places"},
    "dim_transaction_status": {"status_key"},
    "dim_refund_reason": {"refund_reason_key"},
    "dim_chargeback_reason": {"chargeback_reason_key"},
    "fact_transaction": {
        "transaction_key", "date_key", "merchant_key", "currency_key", "status_key",
        "refund_count", "chargeback_count", "has_refund", "has_chargeback",
    },
    "fact_refund": {
        "refund_key", "refund_date_key", "merchant_key", "currency_key", "refund_reason_key",
    },
    "fact_chargeback": {
        "chargeback_key", "chargeback_date_key", "merchant_key", "currency_key",
        "chargeback_reason_key",
    },
}


def prepare_dataframe(df: pd.DataFrame, table_name: str) -> pd.DataFrame:
    df = df.rename(columns=COLUMN_RENAMES.get(table_name, {}))
    expected = TABLE_COLUMNS[table_name]
    missing = [col for col in expected if col not in df.columns]
    if missing:
        raise ValueError(
            f"{table_name} parquet is missing columns required by Snowflake DDL: {missing}"
        )
    df = df[expected].copy()

    for col, default in NOT_NULL_DEFAULTS.get(table_name, {}).items():
        if col in df.columns and df[col].isna().any():
            null_count = int(df[col].isna().sum())
            print(f"  [WARN] {table_name}.{col}: filling {null_count} null(s) with {default}")
            df[col] = df[col].fillna(default)

    for col in INT_COLUMNS.get(table_name, set()):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    df.columns = [c.upper() for c in df.columns]
    return df


def _load_via_staging(conn, table_name: str, df: pd.DataFrame) -> int:
    """Load fact tables through a temp staging table (avoids TRUNCATE+COPY bug)."""
    target = table_name.upper()
    stage = f"_{target}_LOAD"
    columns = ", ".join(df.columns)

    write_pandas(
        conn,
        df,
        stage,
        overwrite=True,
        auto_create_table=True,
        quote_identifiers=False,
    )

    cur = conn.cursor()
    try:
        cur.execute(f"TRUNCATE TABLE {target}")
        cur.execute(
            f"INSERT INTO {target} ({columns}) SELECT {columns} FROM {stage}"
        )
    finally:
        cur.execute(f"DROP TABLE IF EXISTS {stage}")

    nrows = len(df)
    print(f"  [OK] {table_name}: {nrows} rows loaded (staging)")
    return nrows


def load_table(conn, table_name: str):
    parquet_file = GOLD_PATH / f"{table_name}.parquet"
    if not parquet_file.exists():
        print(f"  [SKIP] {parquet_file} not found")
        return 0

    df = prepare_dataframe(pd.read_parquet(parquet_file), table_name)

    if table_name in FACT_TABLES:
        return _load_via_staging(conn, table_name, df)

    _, nchunks, nrows, _ = write_pandas(
        conn,
        df,
        table_name.upper(),
        overwrite=True,
        quote_identifiers=False,
    )
    print(f"  [OK] {table_name}: {nrows} rows loaded ({nchunks} chunks)")
    return nrows

def check_env() -> int:
    print("=== Snowflake env check ===")
    cfg = get_snowflake_config()
    missing = validate_snowflake_env()

    print(f"  .env path: {ENV_FILE}")
    print(f"  SNOWFLAKE_ACCOUNT:   {cfg['account'] or '(not set)'}")
    print(f"  SNOWFLAKE_USER:      {cfg['user'] or '(not set)'}")
    print(f"  SNOWFLAKE_PASSWORD:  {'*' * 8 if cfg['password'] else '(not set)'}")
    print(f"  SNOWFLAKE_WAREHOUSE: {cfg['warehouse'] or '(not set)'}")
    print(f"  SNOWFLAKE_DATABASE:  {cfg['database']}")
    print(f"  SNOWFLAKE_SCHEMA:    {cfg['schema']}")
    print(f"  SNOWFLAKE_ROLE:      {cfg['role'] or '(optional, not set)'}")

    gold_files = [t for t in TABLES if (GOLD_PATH / f"{t}.parquet").exists()]
    print(f"\n  Gold parquet files ready: {len(gold_files)}/{len(TABLES)}")

    if missing:
        print_env_help(missing)
        return 1

    print("\n  Env looks ready. Run without --check-env to load.")
    return 0


def main():
    parser = argparse.ArgumentParser(description="Load gold parquet files into Snowflake")
    parser.add_argument(
        "--check-env",
        action="store_true",
        help="Validate .env and gold files without connecting to Snowflake",
    )
    args = parser.parse_args()

    if args.check_env:
        sys.exit(check_env())

    print("=== GOLD -> Snowflake Loader ===")
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA()")
        print("Connected to:", cur.fetchone())

        total = 0
        for tbl in TABLES:
            total += load_table(conn, tbl)

        print(f"\n=== Done. Total rows across gold tables: {total} ===")
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
