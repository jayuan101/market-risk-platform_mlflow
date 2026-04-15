from utils.config import config
from utils.logger import logger
from utils.glue_helper import ensure_database, create_or_update_table, repair_table_partitions


def curated_location(domain: str) -> str:
    """
    s3://bucket/curated/<domain>/
    """
    bucket = config.RAW_BUCKET  # same bucket as raw, different prefix
    curated_prefix = getattr(config, "CURATED_PREFIX", "silver/curated")
    return f"s3://{bucket}/{curated_prefix}/{domain}/"


def main():
    curated_db = config.GLUE_CURATED_DB
    if not curated_db:
        raise RuntimeError("GLUE_CURATED_DB not set in .env")

    logger.info(f"Ensuring Glue curated database: {curated_db}")
    ensure_database(curated_db, description="Curated zone for market-risk demo")

    # ── mrisk_curated_market_prices ─────────────────────────────
    prices_columns = [
        {"Name": "business_date", "Type": "string"},
        {"Name": "instrument_id", "Type": "string"},
        {"Name": "price", "Type": "double"},
        {"Name": "currency", "Type": "string"},
        {"Name": "source_system", "Type": "string"},
        {"Name": "ingest_timestamp", "Type": "string"},
        {"Name": "load_timestamp", "Type": "string"},
        {"Name": "pipeline_run_id", "Type": "string"},
    ]
    prices_partitions = [
        {"Name": "business_date", "Type": "string"},
    ]

    create_or_update_table(
        db_name=curated_db,
        table_name="mrisk_curated_market_prices",
        location=curated_location("market_prices"),
        columns=prices_columns,
        partition_keys=prices_partitions,
        input_format="org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
        output_format="org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
        serde_lib="org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
        classification="parquet",
        table_description="Curated market prices (Parquet, partitioned by business_date)",
    )
    repair_table_partitions(curated_db, "mrisk_curated_market_prices")

    # ── mrisk_curated_exposure_limits ───────────────────────────
    exp_lim_columns = [
        {"Name": "business_date", "Type": "string"},
        {"Name": "instrument_id", "Type": "string"},
        {"Name": "desk_id", "Type": "string"},
        {"Name": "region", "Type": "string"},
        {"Name": "exposure_amount", "Type": "double"},
        {"Name": "limit_amount", "Type": "double"},
        {"Name": "breach_flag", "Type": "boolean"},
        {"Name": "breach_pct", "Type": "double"},
        {"Name": "currency_exp", "Type": "string"},
        {"Name": "currency_lim", "Type": "string"},
        {"Name": "load_timestamp", "Type": "string"},
        {"Name": "pipeline_run_id", "Type": "string"},
    ]
    exp_lim_partitions = [
        {"Name": "business_date", "Type": "string"},
        {"Name": "region", "Type": "string"},
    ]

    create_or_update_table(
        db_name=curated_db,
        table_name="mrisk_curated_exposure_limits",
        location=curated_location("exposure_limits"),
        columns=exp_lim_columns,
        partition_keys=exp_lim_partitions,
        input_format="org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
        output_format="org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
        serde_lib="org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
        classification="parquet",
        table_description="Curated exposure vs limit with breach flags (Parquet, partitioned by business_date and region)",
    )
    repair_table_partitions(curated_db, "mrisk_curated_exposure_limits")

    logger.info("Curated Glue tables registered/updated successfully.")


if __name__ == "__main__":
    main()