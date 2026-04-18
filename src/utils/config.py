import os
import boto3
from dotenv import load_dotenv

load_dotenv()

class Config:
    # AWS Identity
    AWS_PROFILE: str = os.getenv("AWS_PROFILE", "mrisk-dev")
    AWS_REGION: str  = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
    PROJECT_NAME: str = os.getenv("PROJECT_NAME", "market-risk-platform")

    # S3 Buckets & Prefixes
    RAW_BUCKET: str           = os.getenv("RAW_BUCKET")
    RAW_PREFIX: str           = os.getenv("RAW_PREFIX", "raw")
    BRONZE_PREFIX: str        = os.getenv("BRONZE_PREFIX", "bronze")
    SILVER_PREFIX: str        = os.getenv("SILVER_PREFIX", "silver")
    GOLD_PREFIX: str          = os.getenv("GOLD_PREFIX", "gold")
    REJECTED_PREFIX: str      = os.getenv("REJECTED_PREFIX", "rejected")
    AUDIT_PREFIX: str         = os.getenv("AUDIT_PREFIX", "audit")
    ATHENA_RESULTS_PREFIX: str = os.getenv("ATHENA_RESULTS_PREFIX", "athena-result")

    # Glue Catalog Databases
    GLUE_RAW_DB: str    = os.getenv("GLUE_RAW_DB",    "mrisk_raw_db")
    CURATED_PREFIX: str  = os.getenv("CURATED_PREFIX", "silver/curated")
    GLUE_CURATED_DB: str = os.getenv("GLUE_CURATED_DB", "mrisk_curated_db")
    GLUE_BRONZE_DB: str = os.getenv("GLUE_BRONZE_DB", "mrisk_bronze_db")
    GLUE_SILVER_DB: str = os.getenv("GLUE_SILVER_DB", "mrisk_silver_db")
    GLUE_GOLD_DB: str   = os.getenv("GLUE_GOLD_DB",   "mrisk_gold_db")
    GLUE_AUDIT_DB: str  = os.getenv("GLUE_AUDIT_DB",  "mrisk_audit_db")

    # MLflow
    MLFLOW_TRACKING_URI: str = os.getenv("MLFLOW_TRACKING_URI", "mlruns")

    # Derived S3 paths — computed once, reused everywhere
    @property
    def BRONZE_PATH(self) -> str:
        return f"s3://{self.RAW_BUCKET}/{self.BRONZE_PREFIX}/"

    @property
    def SILVER_PATH(self) -> str:
        return f"s3://{self.RAW_BUCKET}/{self.SILVER_PREFIX}/"

    @property
    def GOLD_PATH(self) -> str:
        return f"s3://{self.RAW_BUCKET}/{self.GOLD_PREFIX}/"

    @property
    def REJECTED_PATH(self) -> str:
        return f"s3://{self.RAW_BUCKET}/{self.REJECTED_PREFIX}/"

    @property
    def AUDIT_PATH(self) -> str:
        return f"s3://{self.RAW_BUCKET}/{self.AUDIT_PREFIX}/"

    @property
    def ATHENA_OUTPUT(self) -> str:
        return f"s3://{self.RAW_BUCKET}/{self.ATHENA_RESULTS_PREFIX}/"

    def validate(self) -> None:
        """Fail fast at startup if critical config is missing."""
        required = {
            "RAW_BUCKET": self.RAW_BUCKET,
        }
        missing = [k for k, v in required.items() if not v]
        if missing:
            raise EnvironmentError(f"Missing required env vars: {missing}")

    def get_boto3_session(self) -> boto3.Session:
        return boto3.Session(
            profile_name=self.AWS_PROFILE,
            region_name=self.AWS_REGION
        )

config = Config()
