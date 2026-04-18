from utils.config import config
from utils.logger import logger
from utils.glue_helper import ensure_database, create_or_update_table, repair_table_partitions


def audit_location() -> str:
    return f"s3://{config.RAW_BUCKET}/{config.AUDIT_PREFIX}/pipeline_runs/"


def main():
    audit_db = config.GLUE_AUDIT_DB
    if not audit_db:
        raise RuntimeError("GLUE_AUDIT_DB not set in .env")

    logger.info(f"Ensuring Glue audit database: {audit_db}")
    ensure_database(audit_db, description="Audit DB for pipeline runs")

    # run_id is a partition key — excluded from columns per Hive convention
    columns = [
        {"Name": "overall_status", "Type": "string"},
        {"Name": "started_at", "Type": "string"},
        {"Name": "finished_at", "Type": "string"},
        {"Name": "steps", "Type": "array<struct<step:string,status:string,start_time:string,end_time:string,duration_seconds:double,error:string>>"},
    ]

    partition_keys = [
        {"Name": "run_id", "Type": "string"},
    ]

    create_or_update_table(
        db_name=audit_db,
        table_name="mrisk_audit_pipeline_runs",
        location=audit_location(),
        columns=columns,
        partition_keys=partition_keys,
        input_format="org.apache.hadoop.mapred.TextInputFormat",
        output_format="org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
        serde_lib="org.openx.data.jsonserde.JsonSerDe",
        serde_params={"ignore.malformed.json": "true"},
        classification="json",
        table_description="Audit records for pipeline runs, one JSON per run",
    )

    logger.info("Repairing partitions so existing S3 run_id prefixes are visible in Athena...")
    repair_table_partitions(audit_db, "mrisk_audit_pipeline_runs")

    logger.info("Audit Glue table registered/updated successfully.")


if __name__ == "__main__":
    main()
