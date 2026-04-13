## market_prices
Grain: one row per instrument per business date
Columns:
  business_date     DATE        partition key
  instrument_id     STRING      not null
  price             DOUBLE      not null, > 0
  currency          STRING      not null
  source_system     STRING      not null
  ingest_timestamp  TIMESTAMP   audit

## credit_exposure
Grain: one row per instrument per desk per business date
Columns:
  business_date     DATE        partition key
  instrument_id     STRING      not null
  desk_id           STRING      not null
  region            STRING      partition key
  exposure_amount   DOUBLE      not null, >= 0
  currency          STRING      not null
  ingest_timestamp  TIMESTAMP   audit

## limit_thresholds
Grain: one row per instrument per desk, effective date driven
Columns:
  instrument_id     STRING      not null
  desk_id           STRING      not null
  region            STRING      not null
  limit_amount      DOUBLE      not null, > 0
  effective_date    DATE        not null
  currency          STRING      not null

## exposure_limits (curated join output)
Grain: one row per instrument per desk per business date
Columns:
  business_date     DATE
  instrument_id     STRING
  desk_id           STRING
  region            STRING
  exposure_amount   DOUBLE
  limit_amount      DOUBLE
  breach_flag       BOOLEAN     ← exposure_amount > limit_amount
  breach_pct        DOUBLE      ← exposure_amount / limit_amount * 100
  currency          STRING
  load_timestamp    TIMESTAMP   audit
  pipeline_run_id   STRING      audit