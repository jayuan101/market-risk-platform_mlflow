import os
from datetime import datetime
import pandas as pd

from utils.logger import logger
from quality.rules import (
    validate_market_prices,
    validate_credit_exposure,
    validate_limit_thresholds,
    validate_reg_reference,
)

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
DATA_DIR = os.path.join(BASE_DIR, "datasets")
CLEAN_DIR = os.path.join(DATA_DIR, "clean")
DQ_OUTPUT_DIR = os.path.join(DATA_DIR, "dq_output")


def ensure_dirs():
    os.makedirs(DQ_OUTPUT_DIR, exist_ok=True)


def run_for_domain(
    csv_name: str,
    validator,
    domain: str,
):
    input_path = os.path.join(CLEAN_DIR, csv_name)
    logger.info(f"Running DQ for domain={domain}, file={input_path}")

    df = pd.read_csv(input_path)
    total = len(df)

    df_valid, df_invalid = validator(df)
    passed = len(df_valid)
    failed = len(df_invalid)

    logger.info(f"{domain}: total={total}, passed={passed}, failed={failed}")

    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")

    if failed > 0:
        rejected_path = os.path.join(DQ_OUTPUT_DIR, f"{domain}_rejected_{ts}.csv")
        df_invalid.to_csv(rejected_path, index=False)
        logger.info(f"{domain}: wrote rejected rows to {rejected_path}")

    passed_path = os.path.join(DQ_OUTPUT_DIR, f"{domain}_passed_{ts}.csv")
    df_valid.to_csv(passed_path, index=False)
    logger.info(f"{domain}: wrote passed rows to {passed_path}")

    return {
        "domain": domain,
        "total": total,
        "passed": passed,
        "failed": failed,
        "rejected_path": rejected_path if failed > 0 else None,
        "passed_path": passed_path,
    }


def main():
    ensure_dirs()
    summary = []

    summary.append(
        run_for_domain(
            csv_name="market_prices_clean.csv",
            validator=validate_market_prices,
            domain="market_prices",
        )
    )
    summary.append(
        run_for_domain(
            csv_name="credit_exposure_clean.csv",
            validator=validate_credit_exposure,
            domain="credit_exposure",
        )
    )
    summary.append(
        run_for_domain(
            csv_name="limit_thresholds_clean.csv",
            validator=validate_limit_thresholds,
            domain="limit_thresholds",
        )
    )
    summary.append(
        run_for_domain(
            csv_name="reg_reference_clean.csv",
            validator=validate_reg_reference,
            domain="reg_reference",
        )
    )

    summary_df = pd.DataFrame(summary)
    summary_path = os.path.join(DQ_OUTPUT_DIR, f"dq_summary_{datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}.csv")
    summary_df.to_csv(summary_path, index=False)
    logger.info(f"Wrote DQ summary to {summary_path}")


if __name__ == "__main__":
    main()