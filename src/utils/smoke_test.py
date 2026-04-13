from config import config
from logger import logger

logger.info(f"Project: {config.PROJECT_NAME}")
logger.info(f"Region: {config.AWS_REGION}")
logger.info(f"Raw bucket: {config.RAW_BUCKET}")
logger.info("Smoke test passed")