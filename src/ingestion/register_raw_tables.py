import os

from utils.config import config
from utils.logger import logger
from utils.glue_helper import ensure_database, create_or_update_table


def get_s3_location(domain: str) -> str:
    """
    Returns the S3 location for the raw zone domain, without partition suffix.
    Example: s3://bucket/raw/market_prices/
    """
    return f"s3://{config.RAW_BUCKET}/{config.RAW_PREFIX}/{domain}/"


def main() -> None:
    raw_db = config.GLUE_RAW_DB
    if not raw_db:
        raise RuntimeError("GLUE_RAW_DB not set in .env")

    logger.info(f"Ensuring Glue raw database: {raw_db}")
    ensure_database(raw_db, description="Raw zone for market-risk demo")

    # ── market_prices ────────────────────────────────────────────
    prices_columns = [
        {"Name": "business_date", "Type": "string"},
        {"Name": "instrument_id", "Type": "string"},
        {"Name": "price", "Type": "double"},
        {"Name": "currency", "Type": "string"},
        {"Name": "source_system", "Type": "string"},
        {"Name": "ingest_timestamp", "Type": "string"},
    ]
    prices_partitions = [
        {"Name": "business_date", "Type": "string"},
    ]
    create_or_update_table(
        db_name=raw_db,
        table_name="mrisk_raw_market_prices",
        location=get_s3_location("market_prices"),
        columns=prices_columns,
        partition_keys=prices_partitions,
        table_description="Raw market prices with business_date partition",
    )

    # ── credit_exposure ──────────────────────────────────────────
    exposure_columns = [
        {"Name": "business_date", "Type": "string"},
        {"Name": "instrument_id", "Type": "string"},
        {"Name": "desk_id", "Type": "string"},
        {"Name": "region", "Type": "string"},
        {"Name": "exposure_amount", "Type": "double"},
        {"Name": "currency", "Type": "string"},
        {"Name": "ingest_timestamp", "Type": "string"},
    ]
    exposure_partitions = [
        {"Name": "business_date", "Type": "string"},
        {"Name": "region", "Type": "string"},
    ]
    create_or_update_table(
        db_name=raw_db,
        table_name="mrisk_raw_credit_exposure",
        location=get_s3_location("credit_exposure"),
        columns=exposure_columns,
        partition_keys=exposure_partitions,
        table_description="Raw credit exposure, partitioned by business_date and region",
    )

    # ── limit_thresholds ─────────────────────────────────────────
    limit_columns = [
        {"Name": "instrument_id", "Type": "string"},
        {"Name": "desk_id", "Type": "string"},
        {"Name": "region", "Type": "string"},
        {"Name": "limit_amount", "Type": "double"},
        {"Name": "effective_date", "Type": "string"},
        {"Name": "currency", "Type": "string"},
    ]
    limit_partitions: list[dict] = []
    create_or_update_table(
        db_name=raw_db,
        table_name="mrisk_raw_limit_thresholds",
        location=get_s3_location("limit_thresholds"),
        columns=limit_columns,
        partition_keys=limit_partitions,
        table_description="Raw limit thresholds reference table",
    )

    # ── reg_reference ────────────────────────────────────────────
    reg_columns = [
        {"Name": "instrument_id", "Type": "string"},
        {"Name": "asset_class", "Type": "string"},
        {"Name": "reg_category", "Type": "string"},
        {"Name": "reporting_flag", "Type": "boolean"},
        {"Name": "last_updated", "Type": "string"},
    ]
    reg_partitions: list[dict] = []
    create_or_update_table(
        db_name=raw_db,
        table_name="mrisk_raw_reg_reference",
        location=get_s3_location("reg_reference"),
        columns=reg_columns,
        partition_keys=reg_partitions,
        table_description="Raw regulatory reference attributes",
    )

    logger.info("Raw Glue tables registered/updated successfully.")


if __name__ == "__main__":
    main()