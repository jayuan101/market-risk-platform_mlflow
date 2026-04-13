## Hard Fail Rules (pipeline stops, records go to rejected/)
  - instrument_id is null
  - exposure_amount is null or negative
  - limit_amount is null or zero
  - business_date is null or unparseable
  - duplicate instrument_id + desk_id + business_date in exposure

## Warning Rules (records flagged but pipeline continues)
  - price <= 0 in market_prices
  - exposure_amount > 10x the 30-day average (anomaly flag)
  - currency not in approved list

## File-level Rules
  - Row count must be > 0
  - File checksum must match manifest if manifest present
  - Column count must match expected schema