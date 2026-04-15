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
