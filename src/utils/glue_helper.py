import time

import boto3
from botocore.exceptions import ClientError

from utils.config import config
from utils.logger import logger

_session = boto3.Session(
    profile_name=config.AWS_PROFILE,
    region_name=config.AWS_REGION,
)
_glue = _session.client("glue")
_athena = _session.client("athena")

# Default serde params per classification
_SERDE_DEFAULTS: dict[str, dict] = {
    "csv": {
        "field.delim": ",",
        "escape.delim": "\\",
        "serialization.format": ",",
    },
    "parquet": {
        "serialization.format": "1",
    },
}


def ensure_database(db_name: str, description: str = "") -> None:
    try:
        _glue.get_database(Name=db_name)
        logger.info(f"Glue database already exists: {db_name}")
    except ClientError as e:
        if e.response["Error"]["Code"] == "EntityNotFoundException":
            logger.info(f"Creating Glue database: {db_name}")
            _glue.create_database(
                DatabaseInput={
                    "Name": db_name,
                    "Description": description,
                }
            )
        else:
            logger.error(f"Error checking/creating database {db_name}: {e}")
            raise


def create_or_update_table(
    db_name: str,
    table_name: str,
    location: str,
    columns: list[dict],
    partition_keys: list[dict] | None = None,
    input_format: str = "org.apache.hadoop.mapred.TextInputFormat",
    output_format: str = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
    serde_lib: str = "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
    serde_params: dict | None = None,
    classification: str = "csv",
    table_description: str = "",
) -> None:
    if partition_keys is None:
        partition_keys = []

    # Use caller-supplied serde_params, else pick sensible defaults per format
    if serde_params is None:
        serde_params = _SERDE_DEFAULTS.get(classification, _SERDE_DEFAULTS["csv"])

    table_input = {
        "Name": table_name,
        "Description": table_description,
        "TableType": "EXTERNAL_TABLE",
        "StorageDescriptor": {
            "Columns": columns,
            "Location": location,
            "InputFormat": input_format,
            "OutputFormat": output_format,
            "SerdeInfo": {
                "SerializationLibrary": serde_lib,
                "Parameters": serde_params,
            },
            "Compressed": False,
            "NumberOfBuckets": -1,
        },
        "PartitionKeys": partition_keys,
        "Parameters": {
            "classification": classification,
            "EXTERNAL": "TRUE",
        },
    }

    try:
        _glue.get_table(DatabaseName=db_name, Name=table_name)
        logger.info(f"Updating Glue table: {db_name}.{table_name}")
        _glue.update_table(
            DatabaseName=db_name,
            TableInput=table_input,
        )
    except ClientError as e:
        if e.response["Error"]["Code"] == "EntityNotFoundException":
            logger.info(f"Creating Glue table: {db_name}.{table_name}")
            _glue.create_table(
                DatabaseName=db_name,
                TableInput=table_input,
            )
        else:
            logger.error(f"Error creating/updating table {db_name}.{table_name}: {e}")
            raise


def repair_table_partitions(db_name: str, table_name: str) -> None:
    """
    Run MSCK REPAIR TABLE via Athena so newly uploaded S3 partitions become visible.
    Blocks until the query completes or raises on failure.
    """
    output_location = config.ATHENA_OUTPUT
    query = f"MSCK REPAIR TABLE `{db_name}`.`{table_name}`"
    logger.info(f"Running partition repair: {query}")

    resp = _athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": db_name, "Catalog": "AwsDataCatalog"},
        ResultConfiguration={"OutputLocation": output_location},
    )
    execution_id = resp["QueryExecutionId"]

    for _ in range(60):  # wait up to ~5 minutes
        status = _athena.get_query_execution(QueryExecutionId=execution_id)
        state = status["QueryExecution"]["Status"]["State"]
        if state == "SUCCEEDED":
            logger.info(f"Partition repair succeeded for {db_name}.{table_name}")
            return
        if state in ("FAILED", "CANCELLED"):
            reason = status["QueryExecution"]["Status"].get("StateChangeReason", "")
            raise RuntimeError(
                f"MSCK REPAIR TABLE failed for {db_name}.{table_name}: {reason}"
            )
        time.sleep(5)

    raise TimeoutError(f"MSCK REPAIR TABLE timed out for {db_name}.{table_name}")
