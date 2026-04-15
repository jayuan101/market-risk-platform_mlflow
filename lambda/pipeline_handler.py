import json
import os
from utils.logger import logger
from ingestion import upload_raw
from quality import run_quality_local
from transform import build_curated, build_gold


def lambda_handler(event, context):
    """
    Event expects: {"stage": "ingest_raw" | "dq" | "curated" | "gold"}
    """
    stage = event.get("stage")
    logger.info(f"Pipeline Lambda invoked for stage={stage}")

    try:
        if stage == "ingest_raw":
            upload_raw.main()
        elif stage == "dq":
            run_quality_local.main()
        elif stage == "curated":
            build_curated.main()
        elif stage == "gold":
            build_gold.main()
        else:
            raise ValueError(f"Unsupported stage: {stage}")

        result = {"status": "OK", "stage": stage}
        logger.info(f"Stage {stage} completed successfully")
    except Exception as e:
        logger.exception(f"Stage {stage} failed: {e}")
        result = {"status": "FAILED", "stage": stage, "error": str(e)}
        # Let Step Functions see it as a failure
        raise

    return {
        "statusCode": 200,
        "body": json.dumps(result),
    }