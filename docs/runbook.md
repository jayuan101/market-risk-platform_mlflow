## Stage 9 – Curated analytics / Silver layer (Glue + Athena)

Commands (run in order for a clean environment):
  .\run.ps1 -task curated           # build Parquet, upload to s3://$RAW_BUCKET/silver/curated/
  .\run.ps1 -task register-curated  # register Glue tables + MSCK REPAIR (mrisk_curated_db)

What it does:
  - Reads DQ-passed CSVs from datasets/dq_output/
  - Joins credit_exposure + limit_thresholds on (instrument_id, desk_id, region)
  - Computes breach_flag and breach_pct
  - Writes partitioned Parquet locally and to S3 silver prefix
  - Registers Glue DB: mrisk_curated_db
  - Creates external Parquet tables:
      mrisk_curated_market_prices   (partitioned by business_date)
      mrisk_curated_exposure_limits (partitioned by business_date, region)
  - Runs MSCK REPAIR TABLE so Athena can see the partitions

IMPORTANT – Athena data source:
  In the Athena Query Editor set "Data source" to "AwsDataCatalog" (standard Glue).
  Do NOT use the S3 Tables / Iceberg catalog (awsdatacatalog$iceberg-aws).

Athena validation:
  USE mrisk_curated_db;
  SELECT * FROM mrisk_curated_exposure_limits LIMIT 10;

Analytics queries (cross-domain risk views):

-- Exposure vs limits by desk/region
SELECT business_date, desk_id, region,
       COUNT(*) AS positions,
       SUM(exposure_amount) AS total_exposure,
       SUM(limit_amount) AS total_limit,
       SUM(CASE WHEN breach_flag THEN 1 ELSE 0 END) AS breach_count
FROM mrisk_curated_exposure_limits
GROUP BY business_date, desk_id, region
ORDER BY business_date, desk_id, region;

-- Top 10 worst breaches by breach_pct
SELECT business_date, instrument_id, desk_id, region,
       exposure_amount, limit_amount, breach_pct
FROM mrisk_curated_exposure_limits
WHERE breach_flag = true
ORDER BY breach_pct DESC
LIMIT 10;

-- Daily breach trend
SELECT business_date,
       COUNT(*) AS total_positions,
       SUM(CASE WHEN breach_flag THEN 1 ELSE 0 END) AS breaches
FROM mrisk_curated_exposure_limits
GROUP BY business_date
ORDER BY business_date;

-- Region-level risk concentration
SELECT region,
       SUM(exposure_amount) AS total_exposure,
       SUM(limit_amount)    AS total_limit,
       SUM(CASE WHEN breach_flag THEN 1 ELSE 0 END) AS breach_count
FROM mrisk_curated_exposure_limits
GROUP BY region
ORDER BY total_exposure DESC;

---

## Stage 10 – Gold / serving layer (Glue + Athena)

Commands:
  .\run.ps1 -task gold              # build gold Parquet from curated, upload to s3://$RAW_BUCKET/gold/
  .\run.ps1 -task register-gold     # register Glue gold tables + MSCK REPAIR (mrisk_gold_db)

What it does:
  - Reads curated/exposure_limits Parquet from datasets/curated/
  - Produces two pre-aggregated gold tables:
      breach_summary  – one row per (business_date, desk_id, region) with counts, rates
      top_breaches    – top 20 breach_pct rows per business_date
  - Uploads to s3://$RAW_BUCKET/gold/<table>/...
  - Registers Glue DB: mrisk_gold_db
  - Creates external Parquet tables:
      mrisk_gold_breach_summary  (partitioned by business_date, region)
      mrisk_gold_top_breaches    (partitioned by business_date)
  - Runs MSCK REPAIR TABLE on both

Bronze / Silver / Gold model summary:
  Bronze  = raw S3 + mrisk_raw_db
  Silver  = curated Parquet + mrisk_curated_db  (business logic, breach flags)
  Gold    = serving-layer Parquet + mrisk_gold_db  (pre-aggregated for dashboards)

Athena validation:
  USE mrisk_gold_db;
  SELECT * FROM mrisk_gold_breach_summary LIMIT 10;
  SELECT * FROM mrisk_gold_top_breaches   LIMIT 10;

---

## Stage 6 – Glue Catalog (raw)

Command:
  .\run.ps1 -task register-raw

What it does:
  - Ensures Glue database: mrisk_raw_db
  - Creates/updates 4 external tables pointed at S3 raw prefixes:
      mrisk_raw_market_prices
      mrisk_raw_credit_exposure
      mrisk_raw_limit_thresholds
      mrisk_raw_reg_reference
  - Sets partition keys (business_date, region as defined)

Validation:
  - aws glue get-database --name mrisk_raw_db
  - aws glue get-tables --database-name mrisk_raw_db
  - Athena:
      USE mrisk_raw_db;
      SELECT * FROM mrisk_raw_market_prices LIMIT 10;
