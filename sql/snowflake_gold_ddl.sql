-- =====================================================================
-- SNOWFLAKE DDL: GOLD LAYER - STAR SCHEMA for Payments Reconciliation
-- For Power BI Dashboards: Executive Summary, Merchant Performance,
-- Risk Monitoring, Operational Metrics
--
-- Target: Create a dedicated database/schema e.g.
--   CREATE DATABASE IF NOT EXISTS PAYMENTS_DB;
--   CREATE SCHEMA IF NOT EXISTS PAYMENTS_DB.GOLD;
--   USE SCHEMA PAYMENTS_DB.GOLD;
--
-- Data loading options after tables created:
--   1. Snowflake Connector (Python) + pandas / write_pandas
--   2. COPY INTO from internal stage or S3 stage (parquet -> table)
--   3. Snowpipe for continuous
--
-- Recommended: Use a dedicated warehouse for load + separate for PBI queries.
-- =====================================================================

-- Use your target context
-- USE ROLE ACCOUNTADMIN;   -- or appropriate role
-- USE WAREHOUSE COMPUTE_WH;
-- CREATE DATABASE IF NOT EXISTS PAYMENTS_DB;
-- CREATE SCHEMA IF NOT EXISTS PAYMENTS_DB.GOLD;
-- USE SCHEMA PAYMENTS_DB.GOLD;

-- ---------------------------------------------------------------------
-- DIMENSION TABLES
-- ---------------------------------------------------------------------

CREATE OR REPLACE TABLE DIM_DATE (
    DATE_KEY         INTEGER     NOT NULL PRIMARY KEY,
    FULL_DATE        DATE        NOT NULL,
    YEAR             INTEGER     NOT NULL,
    QUARTER          INTEGER     NOT NULL,
    QUARTER_NAME     VARCHAR(2)  NOT NULL,
    MONTH            INTEGER     NOT NULL,
    MONTH_NAME       VARCHAR(20) NOT NULL,
    MONTH_ABBR       VARCHAR(3)  NOT NULL,
    WEEK_OF_YEAR     INTEGER     NOT NULL,
    DAY_OF_WEEK      INTEGER     NOT NULL,   -- 1=Mon ... 7=Sun
    DAY_NAME         VARCHAR(10) NOT NULL,
    DAY_OF_MONTH     INTEGER     NOT NULL,
    IS_WEEKEND       INTEGER     NOT NULL,   -- 0/1
    YEAR_MONTH       VARCHAR(7)  NOT NULL
)
COMMENT = 'Date dimension for time intelligence in Power BI (use as date table)';

-- Clustering rarely needed on tiny dim. Add if very large calendar.

CREATE OR REPLACE TABLE DIM_MERCHANT (
    MERCHANT_KEY        INTEGER       NOT NULL PRIMARY KEY,
    MERCHANT_ID         VARCHAR(64)   NOT NULL,
    MERCHANT_NAME       VARCHAR(255)  NOT NULL,
    MERCHANT_CATEGORY   VARCHAR(64),
    STATUS              VARCHAR(32)   DEFAULT 'Active',
    EFFECTIVE_FROM      TIMESTAMP_NTZ,
    EFFECTIVE_TO        TIMESTAMP_NTZ
)
COMMENT = 'Merchant dimension. Extend with risk_tier, onboarding_date, etc.';

CREATE OR REPLACE TABLE DIM_CURRENCY (
    CURRENCY_KEY     INTEGER     NOT NULL PRIMARY KEY,
    CURRENCY_CODE    VARCHAR(3)  NOT NULL,
    CURRENCY_NAME    VARCHAR(64),
    DECIMAL_PLACES   INTEGER     DEFAULT 2
)
COMMENT = 'Currency dimension (currently USD only)';

CREATE OR REPLACE TABLE DIM_TRANSACTION_STATUS (
    STATUS_KEY        INTEGER      NOT NULL PRIMARY KEY,
    STATUS_CODE       VARCHAR(32)  NOT NULL,
    STATUS_NAME       VARCHAR(64),
    STATUS_CATEGORY   VARCHAR(32)   -- SUCCESS | FAILURE | IN_PROGRESS
)
COMMENT = 'Transaction status dimension';

CREATE OR REPLACE TABLE DIM_REFUND_REASON (
    REFUND_REASON_KEY  INTEGER     NOT NULL PRIMARY KEY,
    REASON_CODE        VARCHAR(64) NOT NULL,
    REASON_NAME        VARCHAR(128),
    REASON_CATEGORY    VARCHAR(32)  -- HIGH_RISK, PRODUCT, CUSTOMER, OPERATIONAL
)
COMMENT = 'Refund reason dimension for root cause and risk analysis';

CREATE OR REPLACE TABLE DIM_CHARGEBACK_REASON (
    CHARGEBACK_REASON_KEY INTEGER     NOT NULL PRIMARY KEY,
    REASON_CODE           VARCHAR(16) NOT NULL,
    REASON_NAME           VARCHAR(128),
    SOURCE_SYSTEM         VARCHAR(32)
)
COMMENT = 'Chargeback reason code dimension (e.g. Visa/MC codes)';

-- ---------------------------------------------------------------------
-- FACT TABLES
-- ---------------------------------------------------------------------

-- Core grain: 1 row per original transaction
CREATE OR REPLACE TABLE FACT_TRANSACTION (
    TRANSACTION_KEY     INTEGER        NOT NULL PRIMARY KEY,
    TRANSACTION_ID      VARCHAR(64)    NOT NULL,
    DATE_KEY            INTEGER        NOT NULL REFERENCES DIM_DATE(DATE_KEY),
    MERCHANT_KEY        INTEGER        REFERENCES DIM_MERCHANT(MERCHANT_KEY),
    CURRENCY_KEY        INTEGER        REFERENCES DIM_CURRENCY(CURRENCY_KEY),
    STATUS_KEY          INTEGER        REFERENCES DIM_TRANSACTION_STATUS(STATUS_KEY),

    TRANSACTION_DATE    DATE,

    -- Measures (use NUMBER for money)
    GROSS_AMOUNT        NUMBER(18,2)   NOT NULL,
    REFUND_AMOUNT       NUMBER(18,2)   DEFAULT 0,
    CHARGEBACK_AMOUNT   NUMBER(18,2)   DEFAULT 0,
    NET_AMOUNT          NUMBER(18,2),                     -- gross - refund - cb

    -- Flags & counts (great for Power BI DAX)
    REFUND_COUNT        INTEGER        DEFAULT 0,
    CHARGEBACK_COUNT    INTEGER        DEFAULT 0,
    HAS_REFUND          INTEGER        DEFAULT 0,         -- 0/1 flag
    HAS_CHARGEBACK      INTEGER        DEFAULT 0,         -- 0/1 flag

    RISK_CATEGORY       VARCHAR(32),                       -- NORMAL, HIGH_REFUND, CHARGEBACK, FAILED

    SOURCE_SYSTEM       VARCHAR(32),
    -- Optional audit columns
    SOURCE_LOAD_TIMESTAMP TIMESTAMP_NTZ,
    GOLD_LOAD_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (DATE_KEY, MERCHANT_KEY)
COMMENT = 'Primary fact table at transaction grain. Use for Executive + Merchant Performance + Risk + Ops.';

-- Fact at refund grain (supports detailed refund analysis)
CREATE OR REPLACE TABLE FACT_REFUND (
    REFUND_KEY          INTEGER        NOT NULL PRIMARY KEY,
    REFUND_ID           VARCHAR(64)    NOT NULL,
    TRANSACTION_ID      VARCHAR(64)    NOT NULL,
    REFUND_DATE_KEY     INTEGER        NOT NULL REFERENCES DIM_DATE(DATE_KEY),
    MERCHANT_KEY        INTEGER        REFERENCES DIM_MERCHANT(MERCHANT_KEY),
    CURRENCY_KEY        INTEGER        REFERENCES DIM_CURRENCY(CURRENCY_KEY),
    REFUND_REASON_KEY   INTEGER        REFERENCES DIM_REFUND_REASON(REFUND_REASON_KEY),

    REFUND_DATE         DATE,
    REFUND_AMOUNT       NUMBER(18,2)   NOT NULL,
    REFUND_REASON_RAW   VARCHAR(128),

    SOURCE_SYSTEM       VARCHAR(32),
    LOAD_TIMESTAMP      TIMESTAMP_NTZ
)
CLUSTER BY (REFUND_DATE_KEY, MERCHANT_KEY)
COMMENT = 'Fact table for every individual refund. Use for refund reason breakdowns.';

-- Fact at chargeback grain
CREATE OR REPLACE TABLE FACT_CHARGEBACK (
    CHARGEBACK_KEY          INTEGER        NOT NULL PRIMARY KEY,
    CHARGEBACK_ID           VARCHAR(64)    NOT NULL,
    TRANSACTION_ID          VARCHAR(64)    NOT NULL,
    CHARGEBACK_DATE_KEY     INTEGER        NOT NULL REFERENCES DIM_DATE(DATE_KEY),
    MERCHANT_KEY            INTEGER        REFERENCES DIM_MERCHANT(MERCHANT_KEY),
    CURRENCY_KEY            INTEGER        REFERENCES DIM_CURRENCY(CURRENCY_KEY),
    CHARGEBACK_REASON_KEY   INTEGER        REFERENCES DIM_CHARGEBACK_REASON(CHARGEBACK_REASON_KEY),

    CHARGEBACK_DATE         DATE,
    CHARGEBACK_AMOUNT       NUMBER(18,2)   NOT NULL,
    REASON_CODE             VARCHAR(16),

    SOURCE_SYSTEM           VARCHAR(32),
    LOAD_TIMESTAMP          TIMESTAMP_NTZ
)
CLUSTER BY (CHARGEBACK_DATE_KEY, MERCHANT_KEY)
COMMENT = 'Fact table for every individual chargeback. Use for risk monitoring.';

-- ---------------------------------------------------------------------
-- OPTIONAL: Helpful views for Power BI (or use in dataset directly)
-- ---------------------------------------------------------------------

CREATE OR REPLACE VIEW VW_EXECUTIVE_SUMMARY AS
SELECT
    d.FULL_DATE,
    d.YEAR,
    d.MONTH,
    d.YEAR_MONTH,
    m.MERCHANT_ID,
    SUM(f.GROSS_AMOUNT)        AS REVENUE,
    SUM(f.REFUND_AMOUNT)       AS REFUNDS,
    SUM(f.CHARGEBACK_AMOUNT)   AS CHARGEBACKS,
    SUM(f.NET_AMOUNT)          AS NET_REVENUE,
    COUNT(*)                   AS TXN_COUNT,
    SUM(f.HAS_REFUND)          AS TXN_WITH_REFUND,
    SUM(f.HAS_CHARGEBACK)      AS TXN_WITH_CHARGEBACK
FROM FACT_TRANSACTION f
JOIN DIM_DATE d           ON f.DATE_KEY = d.DATE_KEY
JOIN DIM_MERCHANT m       ON f.MERCHANT_KEY = m.MERCHANT_KEY
GROUP BY 1,2,3,4,5;

CREATE OR REPLACE VIEW VW_MERCHANT_RISK AS
SELECT
    m.MERCHANT_ID,
    COUNT(*)                                                  AS TOTAL_TXNS,
    SUM(f.GROSS_AMOUNT)                                       AS TOTAL_REVENUE,
    SUM(f.REFUND_AMOUNT)                                      AS TOTAL_REFUNDS,
    SUM(f.CHARGEBACK_AMOUNT)                                  AS TOTAL_CHARGEBACKS,
    SUM(f.NET_AMOUNT)                                         AS NET_REVENUE,
    ROUND(100.0 * SUM(f.HAS_REFUND) / NULLIF(COUNT(*),0), 2)   AS REFUND_RATE_PCT,
    ROUND(100.0 * SUM(f.HAS_CHARGEBACK) / NULLIF(COUNT(*),0), 2) AS CHARGEBACK_RATE_PCT,
    SUM(CASE WHEN f.RISK_CATEGORY = 'CHARGEBACK' THEN 1 ELSE 0 END) AS CHARGEBACK_TXNS
FROM FACT_TRANSACTION f
JOIN DIM_MERCHANT m ON f.MERCHANT_KEY = m.MERCHANT_KEY
GROUP BY m.MERCHANT_ID;

-- =====================================================================
-- END OF DDL
-- Recommended next steps after load:
--   1. Verify row counts match gold layer.
--   2. Mark date table in Power BI (Modeling > Mark as date table).
--   3. Create relationships: FACT -> DIMs using *_KEY columns.
--   4. Build measures in PBI:
--        Revenue = SUM(FACT_TRANSACTION[GROSS_AMOUNT])
--        Net Revenue = SUM(FACT_TRANSACTION[NET_AMOUNT])
--        Refund Rate % = DIVIDE( SUM(HAS_REFUND), COUNTROWS(...) )
-- =====================================================================