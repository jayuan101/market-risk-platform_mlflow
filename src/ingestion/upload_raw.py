import os
import json
from datetime import date
import boto3
from botocore.exceptions import ClientError

from utils.config import config
from utils.logger import logger

SESSION = boto3.Session(profile_name=config.AWS_PROFILE, region_name=config.AWS_REGION)
s3 = SESSION.client("s3")

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
DATA_DIR = os.path.join(BASE_DIR, "datasets")
CLEAN_DIR = os.path.join(DATA_DIR, "clean")
MANIFEST_PATH = os.path.join(DATA_DIR, "manifest.json")


def _build_s3_key(domain: str, file_name: str, business_date: str) -> str:
    return f"{config.RAW_PREFIX}/{domain}/business_date={business_date}/{file_name}"


def upload_file(local_path: str, bucket: str, key: str, extra_metadata: dict | None = None) -> None:
    extra_args = {
        "Metadata": {
            "project": config.PROJECT_NAME,
            "environment": "dev",
        }
    }
    if extra_metadata:
        extra_args["Metadata"].update(extra_metadata)

    logger.info(f"Uploading {local_path} -> s3://{bucket}/{key}")

    try:
        s3.upload_file(
            Filename=local_path,
            Bucket=bucket,
            Key=key,
            ExtraArgs=extra_args,
        )
    except ClientError as e:
        logger.error(f"Failed to upload {local_path} -> s3://{bucket}/{key}: {e}")
        raise


def load_manifest() -> list[dict]:
    with open(MANIFEST_PATH, "r") as f:
        return json.load(f)


def main(business_date: str | None = None) -> None:
    if business_date is None:
        business_date = date(2024, 1, 15).isoformat()

    bucket = config.RAW_BUCKET
    if not bucket:
        raise RuntimeError("RAW_BUCKET not set in .env")

    logger.info(f"Starting raw ingestion for business_date={business_date}, bucket={bucket}")

    manifest = load_manifest()
    logger.info(f"Loaded manifest with {len(manifest)} entries from {MANIFEST_PATH}")

    domain_map = {
        "market_prices_clean.csv":     "market_prices",
        "credit_exposure_clean.csv":   "credit_exposure",
        "limit_thresholds_clean.csv":  "limit_thresholds",
        "reg_reference_clean.csv":     "reg_reference",
    }

    for m in manifest:
        fname = m["file_name"]
        if fname not in domain_map:
            logger.warning(f"Skipping manifest entry {fname} (not in domain_map)")
            continue

        domain = domain_map[fname]
        local_path = os.path.join(CLEAN_DIR, fname)
        if not os.path.exists(local_path):
            logger.error(f"Local file for manifest entry not found: {local_path}")
            continue

        key = _build_s3_key(domain, fname, business_date)
        extra_metadata = {
            "batch_id": m["batch_id"],
            "source_system": m["source_system"],
            "row_count": str(m["row_count"]),
            "checksum_md5": m["checksum_md5"],
            "business_date": business_date,
            "domain": domain,
        }

        upload_file(local_path, bucket, key, extra_metadata)

    logger.info("Raw ingestion completed successfully")


if __name__ == "__main__":
    main()