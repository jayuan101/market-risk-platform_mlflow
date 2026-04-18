"""
Historical Value-at-Risk (VaR) model for market prices.

Loads curated market prices, computes daily log returns per instrument,
then calculates Historical VaR at configurable confidence levels.
Results are tracked in MLflow: params, metrics, and a CSV artifact.

Run:
    .\run.ps1 -task var-model
"""
import os
from datetime import datetime

import mlflow
import pandas as pd
import numpy as np
import pyarrow.parquet as pq

from utils.config import config
from utils.logger import logger

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
CURATED_LOCAL_DIR = os.path.join(BASE_DIR, "datasets", "curated")
OUTPUT_DIR = os.path.join(BASE_DIR, "datasets", "var_output")


def load_curated_prices() -> pd.DataFrame:
    prices_dir = os.path.join(CURATED_LOCAL_DIR, "market_prices")
    if not os.path.exists(prices_dir):
        raise FileNotFoundError(
            f"Curated market prices not found at {prices_dir}. "
            "Run: .\\run.ps1 -task pipeline"
        )
    df = pq.read_table(prices_dir).to_pandas()
    df["business_date"] = pd.to_datetime(df["business_date"], errors="coerce")
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    return df.dropna(subset=["business_date", "price", "instrument_id"])


def compute_historical_var(
    df: pd.DataFrame,
    confidence_levels: list[float],
    lookback_days: int,
) -> pd.DataFrame:
    df = df.sort_values(["instrument_id", "business_date"])

    # Compute daily log returns per instrument
    df["log_return"] = df.groupby("instrument_id")["price"].transform(
        lambda x: np.log(x / x.shift(1))
    )
    df = df.dropna(subset=["log_return"])

    # Trim to lookback window
    cutoff = df["business_date"].max() - pd.Timedelta(days=lookback_days)
    df = df[df["business_date"] >= cutoff]

    records = []
    for instrument_id, grp in df.groupby("instrument_id"):
        returns = grp["log_return"].values
        if len(returns) < 5:
            continue
        row = {
            "instrument_id": instrument_id,
            "n_observations": len(returns),
            "mean_return": float(np.mean(returns)),
            "std_return": float(np.std(returns)),
        }
        for cl in confidence_levels:
            var_key = f"var_{int(cl * 100)}"
            # VaR = negative of the (1-cl) percentile of returns
            row[var_key] = float(-np.percentile(returns, (1 - cl) * 100))
        records.append(row)

    return pd.DataFrame(records)


def main(confidence_levels: list[float] | None = None, lookback_days: int = 252):
    if confidence_levels is None:
        confidence_levels = [0.95, 0.99]

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    mlflow.set_tracking_uri(config.MLFLOW_TRACKING_URI)
    mlflow.set_experiment("mrisk-var-model")

    run_ts = datetime.utcnow().strftime("%Y%m%dT%H%M%S")

    with mlflow.start_run(run_name=f"var-{run_ts}"):
        mlflow.log_param("confidence_levels", confidence_levels)
        mlflow.log_param("lookback_days", lookback_days)
        mlflow.log_param("method", "historical")

        logger.info("Loading curated market prices...")
        df = load_curated_prices()
        mlflow.log_metric("total_price_records", len(df))
        mlflow.log_metric("unique_instruments", df["instrument_id"].nunique())

        logger.info(f"Computing Historical VaR (lookback={lookback_days}d, CL={confidence_levels})...")
        var_df = compute_historical_var(df, confidence_levels, lookback_days)

        # Log aggregate metrics
        for cl in confidence_levels:
            var_key = f"var_{int(cl * 100)}"
            if var_key in var_df.columns:
                mlflow.log_metric(f"mean_{var_key}", float(var_df[var_key].mean()))
                mlflow.log_metric(f"max_{var_key}", float(var_df[var_key].max()))

        # Save and log CSV artifact
        out_path = os.path.join(OUTPUT_DIR, f"var_results_{run_ts}.csv")
        var_df.to_csv(out_path, index=False)
        mlflow.log_artifact(out_path, artifact_path="var_results")

        logger.info(f"VaR results written to {out_path}")
        logger.info(f"\n{var_df.to_string(index=False)}")

    return var_df


if __name__ == "__main__":
    main()
