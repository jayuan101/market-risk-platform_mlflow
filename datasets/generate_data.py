import pandas as pd
import numpy as np
import os
from datetime import date, timedelta

np.random.seed(42)
OUTPUT_DIR = os.path.dirname(os.path.abspath(__file__))

# ── Reference data ──────────────────────────────────────────────
INSTRUMENTS = [
    "AAPL", "MSFT", "JPM", "GS", "BAC",
    "C",    "WFC",  "MS",  "BLK", "SPY"
]
DESKS       = ["DESK-EQUITY", "DESK-CREDIT", "DESK-RATES", "DESK-FX"]
REGIONS     = ["NA", "EMEA", "APAC"]
CURRENCIES  = ["USD", "EUR", "GBP"]
SOURCES     = ["BLOOMBERG", "REUTERS", "INTERNAL"]
ASSET_CLASS = ["EQUITY", "FIXED_INCOME", "FX", "COMMODITY"]
REG_CAT     = ["TIER1", "TIER2", "EXEMPT"]

BUSINESS_DATES = [date(2024, 1, 15) + timedelta(days=i) for i in range(5)]

# ── 1. market_prices ────────────────────────────────────────────
rows = []
for bd in BUSINESS_DATES:
    for inst in INSTRUMENTS:
        rows.append({
            "business_date":     bd.isoformat(),
            "instrument_id":     inst,
            "price":             round(np.random.uniform(50, 500), 4),
            "currency":          np.random.choice(CURRENCIES),
            "source_system":     np.random.choice(SOURCES),
            "ingest_timestamp":  f"{bd.isoformat()}T09:00:00Z"
        })

# Bad records — intentional
rows.append({"business_date": "NOT-A-DATE",  "instrument_id": "AAPL", "price": 150.00, "currency": "USD", "source_system": "BLOOMBERG", "ingest_timestamp": "2024-01-15T09:00:00Z"})  # bad date
rows.append({"business_date": "2024-01-15",  "instrument_id": "MSFT", "price": -99.99, "currency": "USD", "source_system": "REUTERS",   "ingest_timestamp": "2024-01-15T09:00:00Z"})   # negative price
rows.append({"business_date": "2024-01-15",  "instrument_id": None,   "price": 200.00, "currency": "EUR", "source_system": "INTERNAL",  "ingest_timestamp": "2024-01-15T09:00:00Z"})   # null instrument_id
rows.append({"business_date": "2024-01-15",  "instrument_id": "SPY",  "price": None,   "currency": "USD", "source_system": "BLOOMBERG", "ingest_timestamp": "2024-01-15T09:00:00Z"})   # null price

df_prices = pd.DataFrame(rows)
df_prices.to_csv(f"{OUTPUT_DIR}/market_prices.csv", index=False)
print(f"market_prices:      {len(df_prices)} rows")

# ── 2. credit_exposure ──────────────────────────────────────────
rows = []
for bd in BUSINESS_DATES:
    for inst in INSTRUMENTS:
        for desk in DESKS:
            region = np.random.choice(REGIONS)
            rows.append({
                "business_date":     bd.isoformat(),
                "instrument_id":     inst,
                "desk_id":           desk,
                "region":            region,
                "exposure_amount":   round(np.random.uniform(100_000, 5_000_000), 2),
                "currency":          "USD",
                "ingest_timestamp":  f"{bd.isoformat()}T09:00:00Z"
            })

# Bad records — intentional
rows.append({"business_date": "2024-01-15", "instrument_id": "JPM",  "desk_id": "DESK-EQUITY", "region": "NA",   "exposure_amount": None,        "currency": "USD", "ingest_timestamp": "2024-01-15T09:00:00Z"})   # null exposure
rows.append({"business_date": "2024-01-15", "instrument_id": "GS",   "desk_id": "DESK-CREDIT", "region": "EMEA", "exposure_amount": -500_000,    "currency": "USD", "ingest_timestamp": "2024-01-15T09:00:00Z"})   # negative exposure
rows.append({"business_date": "2024-01-15", "instrument_id": "BAC",  "desk_id": "DESK-RATES",  "region": "APAC", "exposure_amount": 999_999_999, "currency": "USD", "ingest_timestamp": "2024-01-15T09:00:00Z"})   # absurdly high
rows.append({"business_date": "2024-01-15", "instrument_id": "JPM",  "desk_id": "DESK-EQUITY", "region": "NA",   "exposure_amount": 1_200_000,   "currency": "USD", "ingest_timestamp": "2024-01-15T09:00:00Z"})   # duplicate key

df_exposure = pd.DataFrame(rows)
df_exposure.to_csv(f"{OUTPUT_DIR}/credit_exposure.csv", index=False)
print(f"credit_exposure:    {len(df_exposure)} rows")

# ── 3. limit_thresholds ─────────────────────────────────────────
rows = []
for inst in INSTRUMENTS:
    for desk in DESKS:
        region = np.random.choice(REGIONS)
        rows.append({
            "instrument_id":  inst,
            "desk_id":        desk,
            "region":         region,
            "limit_amount":   round(np.random.uniform(1_000_000, 8_000_000), 2),
            "effective_date": "2024-01-01",
            "currency":       "USD"
        })

# Bad records — intentional
rows.append({"instrument_id": "AAPL", "desk_id": "DESK-EQUITY", "region": "NA",   "limit_amount": None,  "effective_date": "2024-01-01", "currency": "USD"})  # null limit
rows.append({"instrument_id": "MSFT", "desk_id": "DESK-CREDIT", "region": "EMEA", "limit_amount": 0,     "effective_date": "2024-01-01", "currency": "USD"})  # zero limit (invalid)
rows.append({"instrument_id": None,   "desk_id": "DESK-RATES",  "region": "APAC", "limit_amount": 500_000, "effective_date": "2024-01-01", "currency": "USD"})  # null instrument

df_limits = pd.DataFrame(rows)
df_limits.to_csv(f"{OUTPUT_DIR}/limit_thresholds.csv", index=False)
print(f"limit_thresholds:   {len(df_limits)} rows")

# ── 4. reg_reference ────────────────────────────────────────────
rows = []
for inst in INSTRUMENTS:
    rows.append({
        "instrument_id":    inst,
        "asset_class":      np.random.choice(ASSET_CLASS),
        "reg_category":     np.random.choice(REG_CAT),
        "reporting_flag":   np.random.choice([True, False]),
        "last_updated":     "2024-01-01"
    })

# Bad record — intentional
rows.append({"instrument_id": None, "asset_class": "EQUITY", "reg_category": "TIER1", "reporting_flag": True, "last_updated": "2024-01-01"})  # null instrument

df_reg = pd.DataFrame(rows)
df_reg.to_csv(f"{OUTPUT_DIR}/reg_reference.csv", index=False)
print(f"reg_reference:      {len(df_reg)} rows")

print("\nAll datasets generated successfully.")
