import pandas as pd
import os

BASE = os.path.dirname(os.path.abspath(__file__))

# market_prices — drop bad rows
df = pd.read_csv(f"{BASE}/market_prices.csv")
df_clean = df[
    df["instrument_id"].notna() &
    df["price"].notna() &
    (df["price"] > 0) &
    (df["business_date"].str.match(r"^\d{4}-\d{2}-\d{2}$", na=False))
]
df_clean.to_csv(f"{BASE}/clean/market_prices_clean.csv", index=False)
print(f"market_prices clean: {len(df_clean)} rows (dropped {len(df) - len(df_clean)})")

# credit_exposure — drop bad rows + deduplicate
df = pd.read_csv(f"{BASE}/credit_exposure.csv")
df_clean = df[
    df["instrument_id"].notna() &
    df["exposure_amount"].notna() &
    (df["exposure_amount"] >= 0) &
    (df["exposure_amount"] < 100_000_000)
].drop_duplicates(subset=["business_date", "instrument_id", "desk_id"])
df_clean.to_csv(f"{BASE}/clean/credit_exposure_clean.csv", index=False)
print(f"credit_exposure clean: {len(df_clean)} rows (dropped {len(df) - len(df_clean)})")

# limit_thresholds — drop bad rows
df = pd.read_csv(f"{BASE}/limit_thresholds.csv")
df_clean = df[
    df["instrument_id"].notna() &
    df["limit_amount"].notna() &
    (df["limit_amount"] > 0)
]
df_clean.to_csv(f"{BASE}/clean/limit_thresholds_clean.csv", index=False)
print(f"limit_thresholds clean: {len(df_clean)} rows (dropped {len(df) - len(df_clean)})")

# reg_reference — drop bad rows
df = pd.read_csv(f"{BASE}/reg_reference.csv")
df_clean = df[df["instrument_id"].notna()]
df_clean.to_csv(f"{BASE}/clean/reg_reference_clean.csv", index=False)
print(f"reg_reference clean: {len(df_clean)} rows (dropped {len(df) - len(df_clean)})")

print("\nClean datasets created successfully.")
