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

## Intentional Bad Records (for DQ testing)

### market_prices
- Row 51: business_date = "NOT-A-DATE"        → fails date format check
- Row 52: price = -99.99                       → fails price > 0 check
- Row 53: instrument_id = null                 → fails null check
- Row 54: price = null                         → fails null check

### credit_exposure
- Row 201: exposure_amount = null              → fails null check
- Row 202: exposure_amount = -500000           → fails >= 0 check
- Row 203: exposure_amount = 999999999         → fails anomaly threshold
- Row 204: duplicate JPM + DESK-EQUITY on 2024-01-15 → fails uniqueness check

### limit_thresholds
- Row 41: limit_amount = null                  → fails null check
- Row 42: limit_amount = 0                     → fails > 0 check
- Row 43: instrument_id = null                 → fails null check

### reg_reference
- Row 11: instrument_id = null                 → fails null check