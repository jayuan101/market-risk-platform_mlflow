"""
Gold layer transform.

Reads curated Parquet (silver) and produces two gold tables:
  - breach_summary  : one row per (business_date, desk_id, region)
  - top_breaches    : top 20 breach_pct rows per business_date

Output:  datasets/gold/<table>/...  and  s3://<bucket>/gold/<table>/...
"""
import os
from datetime import datetime

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from utils.config import config
from utils.logger import logger

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
CURATED_LOCAL_DIR = os.path.join(BASE_DIR, "datasets", "curated")
GOLD_LOCAL_DIR = os.path.join(BASE_DIR, "datasets", "gold")

_session = boto3.Session(profile_name=config.AWS_PROFILE, region_name=config.AWS_REGION)
_s3 = _session.client("s3")

_RUN_ID = f"GOLD-{datetime.utcnow().strftime('%Y%m%dT%H%M%S')}"
_RUN_TS = datetime.utcnow().isoformat() + "Z"


def _load_curated_exposure_limits() -> pd.DataFrame:
    domain_dir = os.path.join(CURATED_LOCAL_DIR, "exposure_limits")
    if not os.path.exists(domain_dir):
        raise FileNotFoundError(
            f"Curated exposure_limits not found at {domain_dir}. "
            "Run: .\\run.ps1 -task curated"
        )
    logger.info(f"Loading curated exposure_limits from {domain_dir}")
    df = pq.read_table(domain_dir).to_pandas()
    # Drop rows where region is null (__HIVE_DEFAULT_PARTITION__)
    df = df[df["region"].notna() & (df["region"] != "")]
    return df


def _build_breach_summary(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["business_date"] = pd.to_datetime(df["business_date"]).dt.strftime("%Y-%m-%d")

    agg = (
        df.groupby(["business_date", "desk_id", "region"], as_index=False)
        .agg(
            total_positions=("instrument_id", "count"),
            total_exposure=("exposure_amount", "sum"),
            total_limit=("limit_amount", "sum"),
            breach_count=("breach_flag", "sum"),
        )
    )
    agg["breach_rate"] = (agg["breach_count"] / agg["total_positions"]).round(4)
    agg["load_timestamp"] = _RUN_TS
    agg["pipeline_run_id"] = _RUN_ID
    return agg


def _build_top_breaches(df: pd.DataFrame, top_n: int = 20) -> pd.DataFrame:
    df = df.copy()
    df["business_date"] = pd.to_datetime(df["business_date"]).dt.strftime("%Y-%m-%d")
    df = df[df["breach_flag"] == True]

    df["rank_within_day"] = (
        df.groupby("business_date")["breach_pct"]
        .rank(method="dense", ascending=False)
    )
    df = df[df["rank_within_day"] <= top_n].copy()
    df["load_timestamp"] = _RUN_TS
    df["pipeline_run_id"] = _RUN_ID
    return df


def _write_gold(df: pd.DataFrame, table: str, partition_cols: list[str]) -> None:
    out_dir = os.path.join(GOLD_LOCAL_DIR, table)
    os.makedirs(out_dir, exist_ok=True)

    for col in partition_cols:
        if col not in df.columns:
            raise KeyError(f"Partition column '{col}' missing in gold table '{table}'")

    if "business_date" in partition_cols:
        df = df.copy()
        df["business_date"] = pd.to_datetime(df["business_date"]).dt.strftime("%Y-%m-%d")

    pq.write_to_dataset(
        pa.Table.from_pandas(df),
        root_path=out_dir,
        partition_cols=partition_cols,
    )
    logger.info(f"Wrote gold parquet locally: {out_dir}")

    bucket = config.RAW_BUCKET
    gold_prefix = getattr(config, "GOLD_PREFIX", "gold")

    for root, _, files in os.walk(out_dir):
        for fname in files:
            if not fname.endswith(".parquet"):
                continue
            local_path = os.path.join(root, fname)
            rel_path = os.path.relpath(local_path, out_dir).replace("\\", "/")
            s3_key = f"{gold_prefix}/{table}/{rel_path}"
            logger.info(f"Uploading {local_path} -> s3://{bucket}/{s3_key}")
            _s3.upload_file(local_path, bucket, s3_key)


def main() -> None:
    os.makedirs(GOLD_LOCAL_DIR, exist_ok=True)

    df = _load_curated_exposure_limits()

    logger.info("Building gold breach_summary...")
    _write_gold(
        _build_breach_summary(df),
        table="breach_summary",
        partition_cols=["business_date", "region"],
    )

    logger.info("Building gold top_breaches...")
    _write_gold(
        _build_top_breaches(df, top_n=20),
        table="top_breaches",
        partition_cols=["business_date"],
    )

    logger.info("Gold layer built and uploaded.")


if __name__ == "__main__":
    main()
