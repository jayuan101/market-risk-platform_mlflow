from typing import Any, Dict, List, Tuple
import pandas as pd


def _fail(msg: str) -> Tuple[bool, str]:
    return False, msg


def validate_market_prices(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Returns (df_valid, df_invalid) with a 'dq_reason' column on invalid.
    """
    df = df.copy()
    failures: List[Dict[str, Any]] = []

    for idx, row in df.iterrows():
        reasons = []

        # Required fields
        if pd.isna(row["instrument_id"]):
            reasons.append("instrument_id_null")

        if pd.isna(row["price"]):
            reasons.append("price_null")

        # Format / range checks
        if not isinstance(row["business_date"], str) or len(row["business_date"]) != 10:
            # simple sanity check, detailed parsing later
            reasons.append("business_date_invalid_format")

        try:
            price = float(row["price"])
            if price <= 0:
                reasons.append("price_non_positive")
        except Exception:
            reasons.append("price_not_numeric")

        if reasons:
            failures.append({"index": idx, "dq_reason": ",".join(reasons)})

    if failures:
        fail_df = pd.DataFrame(failures).set_index("index")
        df_invalid = df.join(fail_df, how="inner")
        df_valid = df[~df.index.isin(df_invalid.index)]
    else:
        df_invalid = df.iloc[0:0].copy()
        df_invalid["dq_reason"] = []
        df_valid = df

    return df_valid, df_invalid


def validate_credit_exposure(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    df = df.copy()
    failures: List[Dict[str, Any]] = []

    # Check duplicate keys: business_date + instrument_id + desk_id
    key_cols = ["business_date", "instrument_id", "desk_id"]
    duplicated = df.duplicated(subset=key_cols, keep=False)

    for idx, row in df.iterrows():
        reasons = []

        if pd.isna(row["instrument_id"]):
            reasons.append("instrument_id_null")

        if pd.isna(row["exposure_amount"]):
            reasons.append("exposure_null")
        else:
            try:
                exp = float(row["exposure_amount"])
                if exp < 0:
                    reasons.append("exposure_negative")
                if exp >= 100_000_000:
                    reasons.append("exposure_suspiciously_high")
            except Exception:
                reasons.append("exposure_not_numeric")

        if duplicated.iloc[idx]:
            reasons.append("duplicate_business_key")

        if reasons:
            failures.append({"index": idx, "dq_reason": ",".join(reasons)})

    if failures:
        fail_df = pd.DataFrame(failures).set_index("index")
        df_invalid = df.join(fail_df, how="inner")
        df_valid = df[~df.index.isin(df_invalid.index)]
    else:
        df_invalid = df.iloc[0:0].copy()
        df_invalid["dq_reason"] = []
        df_valid = df

    return df_valid, df_invalid


def validate_limit_thresholds(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    df = df.copy()
    failures: List[Dict[str, Any]] = []

    for idx, row in df.iterrows():
        reasons = []

        if pd.isna(row["instrument_id"]):
            reasons.append("instrument_id_null")

        if pd.isna(row["limit_amount"]):
            reasons.append("limit_null")
        else:
            try:
                limit = float(row["limit_amount"])
                if limit <= 0:
                    reasons.append("limit_non_positive")
            except Exception:
                reasons.append("limit_not_numeric")

        if reasons:
            failures.append({"index": idx, "dq_reason": ",".join(reasons)})

    if failures:
        fail_df = pd.DataFrame(failures).set_index("index")
        df_invalid = df.join(fail_df, how="inner")
        df_valid = df[~df.index.isin(df_invalid.index)]
    else:
        df_invalid = df.iloc[0:0].copy()
        df_invalid["dq_reason"] = []
        df_valid = df

    return df_valid, df_invalid


def validate_reg_reference(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    df = df.copy()
    failures: List[Dict[str, Any]] = []

    for idx, row in df.iterrows():
        reasons = []

        if pd.isna(row["instrument_id"]):
            reasons.append("instrument_id_null")

        if reasons:
            failures.append({"index": idx, "dq_reason": ",".join(reasons)})

    if failures:
        fail_df = pd.DataFrame(failures).set_index("index")
        df_invalid = df.join(fail_df, how="inner")
        df_valid = df[~df.index.isin(df_invalid.index)]
    else:
        df_invalid = df.iloc[0:0].copy()
        df_invalid["dq_reason"] = []
        df_valid = df

    return df_valid, df_invalid