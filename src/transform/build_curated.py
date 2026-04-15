import os
from datetime import datetime

import pandas as pd

from utils.logger import logger
from utils.config import config
import boto3

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
DQ_OUTPUT_DIR = os.path.join(BASE_DIR, "datasets", "dq_output")
CURATED_LOCAL_DIR = os.path.join(BASE_DIR, "datasets", "curated")

SESSION = boto3.Session(profile_name=config.AWS_PROFILE, region_name=config.AWS_REGION)
s3 = SESSION.client("s3")


def ensure_dirs():
    os.makedirs(CURATED_LOCAL_DIR, exist_ok=True)


def load_latest_passed(domain: str) -> pd.DataFrame:
    """
    Find the latest *_passed_*.csv for a domain and load it.
    """
    pattern = f"{domain}_passed_"
    files = [
        f
        for f in os.listdir(DQ_OUTPUT_DIR)
        if f.startswith(pattern) and f.endswith(".csv")
    ]
    if not files:
        raise FileNotFoundError(f"No passed file found for domain={domain} in {DQ_OUTPUT_DIR}")

    files.sort()
    latest = files[-1]
    path = os.path.join(DQ_OUTPUT_DIR, latest)
    logger.info(f"Loading latest passed file for {domain}: {path}")
    return pd.read_csv(path)


def build_curated_market_prices() -> pd.DataFrame:
    df = load_latest_passed("market_prices")

    # Normalize types
    df["business_date"] = pd.to_datetime(df["business_date"], errors="coerce")
    df["price"] = pd.to_numeric(df["price"], errors="coerce")

    # Drop rows that became NaT or NaN after type coercion
    df = df.dropna(subset=["business_date", "price", "instrument_id"])

    # Add audit columns
    run_ts = datetime.utcnow().isoformat() + "Z"
    df["load_timestamp"] = run_ts
    df["pipeline_run_id"] = f"CURATED-{datetime.utcnow().strftime('%Y%m%dT%H%M%S')}"

    return df


def build_curated_exposure_limits() -> pd.DataFrame:
    df_exp = load_latest_passed("credit_exposure")
    df_lim = load_latest_passed("limit_thresholds")

    df_exp["business_date"] = pd.to_datetime(df_exp["business_date"], errors="coerce")
    df_exp["exposure_amount"] = pd.to_numeric(df_exp["exposure_amount"], errors="coerce")
    df_lim["limit_amount"] = pd.to_numeric(df_lim["limit_amount"], errors="coerce")

    # Drop bads that slipped
    df_exp = df_exp.dropna(
        subset=["business_date", "instrument_id", "desk_id", "exposure_amount"]
    )
    df_lim = df_lim.dropna(
        subset=["instrument_id", "desk_id", "limit_amount"]
    )

    # Join on instrument_id + desk_id + region (if region exists in both)
    join_keys = ["instrument_id", "desk_id", "region"]
    for col in join_keys:
        if col not in df_exp.columns or col not in df_lim.columns:
            raise KeyError(f"Missing join column {col} in exposure or limits")

    df = df_exp.merge(
        df_lim,
        on=["instrument_id", "desk_id", "region"],
        how="left",
        suffixes=("_exp", "_lim"),
    )

    # Compute breach flag and pct
    df["breach_flag"] = df["exposure_amount"] > df["limit_amount"]
    df["breach_pct"] = (df["exposure_amount"] / df["limit_amount"]) * 100

    # Audit fields
    run_ts = datetime.utcnow().isoformat() + "Z"
    df["load_timestamp"] = run_ts
    df["pipeline_run_id"] = f"CURATED-{datetime.utcnow().strftime('%Y%m%dT%H%M%S')}"

    # Reorder columns into a clean curated view
    cols = [
        "business_date",
        "instrument_id",
        "desk_id",
        "region",
        "exposure_amount",
        "limit_amount",
        "breach_flag",
        "breach_pct",
        "currency_exp",
        "currency_lim",
        "load_timestamp",
        "pipeline_run_id",
    ]
    # Keep only columns that exist
    cols = [c for c in cols if c in df.columns]
    df = df[cols]

    return df


def write_parquet_partitioned(df: pd.DataFrame, domain: str, partition_cols: list[str]) -> None:
    """
    Write DataFrame as partitioned Parquet under datasets/curated/<domain>/
    and upload to S3 curated prefix.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    domain_dir = os.path.join(CURATED_LOCAL_DIR, domain)
    os.makedirs(domain_dir, exist_ok=True)

    # Ensure partition columns exist and are string or datetime as needed
    for col in partition_cols:
        if col not in df.columns:
            raise KeyError(f"Partition column {col} missing for {domain}")

    # Use business_date as string YYYY-MM-DD for folder
    if "business_date" in partition_cols:
        df["business_date"] = pd.to_datetime(df["business_date"]).dt.strftime("%Y-%m-%d")

    table = pa.Table.from_pandas(df)
    pq.write_to_dataset(
        table,
        root_path=domain_dir,
        partition_cols=partition_cols,
    )  # [web:67][web:77][web:71][web:69][web:74]

    logger.info(f"Wrote local parquet dataset for {domain} at {domain_dir}")

    # Upload to S3 curated prefix
    bucket = config.RAW_BUCKET  # same bucket, different prefix
    curated_prefix = getattr(config, "CURATED_PREFIX", "silver/curated")

    for root, _, files in os.walk(domain_dir):
        for f in files:
            if not f.endswith(".parquet"):
                continue
            local_path = os.path.join(root, f)
            rel_path = os.path.relpath(local_path, domain_dir).replace("\\", "/")
            s3_key = f"{curated_prefix}/{domain}/{rel_path}"
            logger.info(f"Uploading curated parquet {local_path} -> s3://{bucket}/{s3_key}")
            s3.upload_file(local_path, bucket, s3_key)


def main():
    ensure_dirs()

    logger.info("Building curated market prices...")
    df_prices = build_curated_market_prices()
    write_parquet_partitioned(df_prices, domain="market_prices", partition_cols=["business_date"])

    logger.info("Building curated exposure_limits...")
    df_exp_lim = build_curated_exposure_limits()
    write_parquet_partitioned(df_exp_lim, domain="exposure_limits", partition_cols=["business_date", "region"])

    logger.info("Curated datasets built and uploaded.")


if __name__ == "__main__":
    main()