"""
Register gold Glue tables and repair partitions so Athena can query them.
"""
from utils.config import config
from utils.logger import logger
from utils.glue_helper import ensure_database, create_or_update_table, repair_table_partitions

_PARQUET_FORMATS = dict(
    input_format="org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
    output_format="org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
    serde_lib="org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
    classification="parquet",
)


def _gold_location(table: str) -> str:
    bucket = config.RAW_BUCKET
    gold_prefix = getattr(config, "GOLD_PREFIX", "gold")
    return f"s3://{bucket}/{gold_prefix}/{table}/"


def main() -> None:
    gold_db = config.GLUE_GOLD_DB
    logger.info(f"Ensuring Glue gold database: {gold_db}")
    ensure_database(gold_db, description="Gold/serving zone for market-risk demo")

    # ── mrisk_gold_breach_summary ─────────────────────────────────────────
    create_or_update_table(
        db_name=gold_db,
        table_name="mrisk_gold_breach_summary",
        location=_gold_location("breach_summary"),
        columns=[
            {"Name": "desk_id",          "Type": "string"},
            {"Name": "total_positions",  "Type": "bigint"},
            {"Name": "total_exposure",   "Type": "double"},
            {"Name": "total_limit",      "Type": "double"},
            {"Name": "breach_count",     "Type": "bigint"},
            {"Name": "breach_rate",      "Type": "double"},
            {"Name": "load_timestamp",   "Type": "string"},
            {"Name": "pipeline_run_id",  "Type": "string"},
        ],
        partition_keys=[
            {"Name": "business_date", "Type": "string"},
            {"Name": "region",        "Type": "string"},
        ],
        table_description="Gold breach summary by date/desk/region (Parquet)",
        **_PARQUET_FORMATS,
    )
    repair_table_partitions(gold_db, "mrisk_gold_breach_summary")

    # ── mrisk_gold_top_breaches ───────────────────────────────────────────
    create_or_update_table(
        db_name=gold_db,
        table_name="mrisk_gold_top_breaches",
        location=_gold_location("top_breaches"),
        columns=[
            {"Name": "instrument_id",   "Type": "string"},
            {"Name": "desk_id",         "Type": "string"},
            {"Name": "region",          "Type": "string"},
            {"Name": "exposure_amount", "Type": "double"},
            {"Name": "limit_amount",    "Type": "double"},
            {"Name": "breach_flag",     "Type": "boolean"},
            {"Name": "breach_pct",      "Type": "double"},
            {"Name": "rank_within_day", "Type": "double"},
            {"Name": "currency_exp",    "Type": "string"},
            {"Name": "currency_lim",    "Type": "string"},
            {"Name": "load_timestamp",  "Type": "string"},
            {"Name": "pipeline_run_id", "Type": "string"},
        ],
        partition_keys=[
            {"Name": "business_date", "Type": "string"},
        ],
        table_description="Gold top 20 breaches per day (Parquet)",
        **_PARQUET_FORMATS,
    )
    repair_table_partitions(gold_db, "mrisk_gold_top_breaches")

    logger.info("Gold Glue tables registered/updated successfully.")


if __name__ == "__main__":
    main()
