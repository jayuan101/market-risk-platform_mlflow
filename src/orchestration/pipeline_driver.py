import os
from datetime import datetime
import subprocess
import sys
import json

import mlflow

from utils.config import config
from utils.logger import logger

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
AUDIT_DIR = os.path.join(BASE_DIR, "datasets", "audit")
os.makedirs(AUDIT_DIR, exist_ok=True)

_session = config.get_boto3_session()
_s3 = _session.client("s3")
_cloudwatch = _session.client("cloudwatch")


def run_step(name: str, cmd: list[str]) -> dict:
    logger.info(f"=== START STEP: {name} ===")
    start = datetime.utcnow()
    try:
        result = subprocess.run(
            cmd,
            cwd=BASE_DIR,
            capture_output=True,
            text=True,
            check=True,
        )
        end = datetime.utcnow()
        logger.info(f"=== END STEP: {name} (OK) ===")
        logger.info(result.stdout)
        status = "SUCCESS"
        error = None
    except subprocess.CalledProcessError as e:
        end = datetime.utcnow()
        logger.error(f"=== END STEP: {name} (FAILED) ===")
        logger.error(e.stdout)
        logger.error(e.stderr)
        status = "FAILED"
        error = e.stderr or str(e)

    return {
        "step": name,
        "status": status,
        "start_time": start.isoformat() + "Z",
        "end_time": end.isoformat() + "Z",
        "duration_seconds": (end - start).total_seconds(),
        "error": error,
    }


def main():
    run_id = f"RUN-{datetime.utcnow().strftime('%Y%m%dT%H%M%S')}"
    logger.info(f"Starting full pipeline run_id={run_id}")

    mlflow.set_tracking_uri(config.MLFLOW_TRACKING_URI)
    mlflow.set_experiment("mrisk-pipeline")

    with mlflow.start_run(run_name=run_id):
        steps = []
        steps.append(run_step("ingest_raw", [sys.executable, "src/ingestion/upload_raw.py"]))
        steps.append(run_step("dq", [sys.executable, "src/quality/run_quality_local.py"]))
        steps.append(run_step("curated", [sys.executable, "src/transform/build_curated.py"]))
        steps.append(run_step("gold", [sys.executable, "src/transform/build_gold.py"]))

        overall_status = "SUCCESS" if all(s["status"] == "SUCCESS" for s in steps) else "FAILED"

        run_record = {
            "run_id": run_id,
            "overall_status": overall_status,
            "started_at": steps[0]["start_time"],
            "finished_at": steps[-1]["end_time"],
            "steps": steps,
        }

        # MLflow: tags + per-step duration metrics
        mlflow.set_tag("overall_status", overall_status)
        mlflow.set_tag("pipeline_run_id", run_id)
        for step in steps:
            mlflow.log_metric(f"{step['step']}_duration_s", step["duration_seconds"])
            mlflow.log_metric(f"{step['step']}_success", 1.0 if step["status"] == "SUCCESS" else 0.0)

        audit_path = os.path.join(AUDIT_DIR, f"pipeline_run_{run_id}.json")
        with open(audit_path, "w") as f:
            json.dump(run_record, f, indent=2)

        mlflow.log_artifact(audit_path, artifact_path="audit")

        logger.info(f"Pipeline run {run_id} completed with status={overall_status}")
        logger.info(f"Local audit record written to {audit_path}")

        # Upload to S3 audit prefix (Hive-partitioned by run_id for Athena compatibility)
        s3_key = f"{config.AUDIT_PREFIX}/pipeline_runs/run_id={run_id}/pipeline_run.json"
        logger.info(f"Uploading audit record to s3://{config.RAW_BUCKET}/{s3_key}")
        _s3.upload_file(audit_path, config.RAW_BUCKET, s3_key)

        # Publish custom CloudWatch metric so alarms can fire on pipeline failures
        metric_value = 1.0 if overall_status == "SUCCESS" else 0.0
        _cloudwatch.put_metric_data(
            Namespace="MriskPlatform",
            MetricData=[{
                "MetricName": "PipelineRunStatus",
                "Dimensions": [
                    {"Name": "Environment", "Value": os.getenv("ENVIRONMENT", "dev")},
                    {"Name": "Pipeline", "Value": "mrisk"},
                ],
                "Timestamp": datetime.utcnow(),
                "Value": metric_value,
                "Unit": "Count",
            }],
        )
        logger.info(f"CloudWatch metric published: PipelineRunStatus={metric_value}")

    if overall_status != "SUCCESS":
        sys.exit(1)


if __name__ == "__main__":
    main()
