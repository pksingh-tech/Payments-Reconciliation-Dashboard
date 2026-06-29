# Gold Layer + Snowflake + Power BI Guide

## Gold Layer Tables (Star Schema)

**Dimensions (6)**
- `dim_date` (recommended to mark as date table in PBI)
- `dim_merchant`
- `dim_currency`
- `dim_transaction_status`
- `dim_refund_reason`
- `dim_chargeback_reason`

**Facts (3)**
- `fact_transaction` (main – transaction grain)
- `fact_refund`
- `fact_chargeback`

Files are produced under `data/gold/*.parquet` by the gold layer (PySpark or pandas fallback).

## Snowflake Setup

1. Run the DDL:
   ```sql
   -- Open sql/snowflake_gold_ddl.sql
   -- Execute in Snowflake worksheet (after creating DB + SCHEMA)
   ```

2. Load data (two easy options):

   **Option A: Python loader (recommended for dev)**
   ```bash
   # Add these to .env or export
   SNOWFLAKE_ACCOUNT=xxx
   SNOWFLAKE_USER=xxx
   SNOWFLAKE_PASSWORD=xxx
   SNOWFLAKE_WAREHOUSE=COMPUTE_WH
   SNOWFLAKE_DATABASE=PAYMENTS_DB
   SNOWFLAKE_SCHEMA=GOLD

   python -m src.load.gold_to_snowflake
   ```

   **Option B: Manual COPY (from stage)**
   ```sql
   PUT file://<local-gold-path>/fact_transaction.parquet @my_stage AUTO_COMPRESS=FALSE;
   COPY INTO fact_transaction FROM @my_stage/fact_transaction.parquet
     FILE_FORMAT = (TYPE=PARQUET) ON_ERROR='CONTINUE';
   ```

## Power BI Dashboard Structure (Recommended)

### 1. Executive Summary (Page 1)
- **Cards / KPIs**:
  - Total Revenue (SUM gross_amount)
  - Total Refunds
  - Total Chargebacks
  - Net Revenue
- **Line / Area chart**: Net Revenue + Revenue trend by date (use dim_date)
- Slicers: Date range, Merchant, Status, Risk Category

### 2. Merchant Performance (Page 2)
- Bar chart: Revenue + Net Revenue by Merchant
- Table or matrix: Merchant | Revenue | Refunds | Chargebacks | Refund % | Chargeback %
- Use `VW_MERCHANT_RISK` or create DAX measures:
  ```dax
  Refund Rate % = DIVIDE( SUM(fact_transaction[HAS_REFUND]), COUNTROWS(fact_transaction) )
  Chargeback Rate % = DIVIDE( SUM(fact_transaction[HAS_CHARGEBACK]), COUNTROWS(fact_transaction) )
  ```

### 3. Risk Monitoring (Page 3)
- Stacked bar or heatmap: Risk Category by Merchant
- Donut: Chargeback reason breakdown (join fact_chargeback -> dim_chargeback_reason)
- Trend line: Chargebacks over time
- Top risk merchants (high chargeback rate)

### 4. Operational Metrics (Page 4)
- Volume (count of txns) trend
- Average transaction amount
- Status breakdown (settled / failed / etc.)
- Refund vs Chargeback counts over time

## Relationships in Power BI (Model view)
- fact_transaction[DATE_KEY]      --> dim_date[DATE_KEY]
- fact_transaction[MERCHANT_KEY]   --> dim_merchant[MERCHANT_KEY]
- fact_transaction[CURRENCY_KEY]   --> dim_currency[CURRENCY_KEY]
- fact_transaction[STATUS_KEY]     --> dim_transaction_status[STATUS_KEY]

Same pattern for fact_refund / fact_chargeback.

## Key DAX Measures (add to a measures table)
```dax
Revenue          = SUM(fact_transaction[GROSS_AMOUNT])
Refunds          = SUM(fact_transaction[REFUND_AMOUNT])
Chargebacks      = SUM(fact_transaction[CHARGEBACK_AMOUNT])
Net Revenue      = SUM(fact_transaction[NET_AMOUNT])

Total Transactions = COUNTROWS(fact_transaction)
Refund Rate %    = DIVIDE( SUM(fact_transaction[HAS_REFUND]), [Total Transactions] )
Chargeback Rate % = DIVIDE( SUM(fact_transaction[HAS_CHARGEBACK]), [Total Transactions] )
```

## Tips
- Use dim_date as the official Power BI date table (Modeling tab).
- For large data: enable incremental refresh on fact tables using DATE_KEY.
- Add row-level security on merchant if needed.
- Cluster keys in Snowflake (already in DDL) give good pruning for date + merchant filters.

## Regenerating Gold Layer
```bash
python src/main.py          # runs bronze -> silver -> gold (if full pipeline works)
# or directly gold:
python -c "
import sys; sys.path.insert(0,'src')
from utils.spark_session import create_spark_session
from transformation.gold_transform import GoldTransformer
s = create_spark_session()
GoldTransformer(s).execute()
s.stop()
"
```

Gold files ready in `data/gold/`.
