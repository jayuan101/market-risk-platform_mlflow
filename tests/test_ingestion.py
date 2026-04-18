import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from utils.config import config


def test_config_defaults():
    assert config.RAW_PREFIX == "raw"
    assert config.AUDIT_PREFIX == "audit"
    assert config.GLUE_RAW_DB == "mrisk_raw_db"
    assert config.GLUE_AUDIT_DB == "mrisk_audit_db"


def test_s3_derived_paths():
    if not config.RAW_BUCKET:
        return  # skip if no bucket configured
    assert config.AUDIT_PATH.startswith("s3://")
    assert config.AUDIT_PATH.endswith("/")
    assert "audit" in config.AUDIT_PATH


def test_s3_key_construction():
    run_id = "RUN-20240115T120000"
    audit_prefix = "audit"
    key = f"{audit_prefix}/pipeline_runs/run_id={run_id}/pipeline_run.json"
    assert key == "audit/pipeline_runs/run_id=RUN-20240115T120000/pipeline_run.json"
    assert key.startswith("audit/pipeline_runs/run_id=RUN-")
    assert key.endswith("/pipeline_run.json")
