import os
import sys

import pandas as pd
import numpy as np

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from transform.build_gold import _build_breach_summary, _build_top_breaches


def _exposure_df():
    return pd.DataFrame([
        {
            "business_date": "2024-01-15",
            "instrument_id": "AAPL",
            "desk_id": "DESK-A",
            "region": "NA",
            "exposure_amount": 1_500_000.0,
            "limit_amount": 1_000_000.0,
            "breach_flag": True,
            "breach_pct": 150.0,
        },
        {
            "business_date": "2024-01-15",
            "instrument_id": "JPM",
            "desk_id": "DESK-A",
            "region": "NA",
            "exposure_amount": 500_000.0,
            "limit_amount": 1_000_000.0,
            "breach_flag": False,
            "breach_pct": 50.0,
        },
        {
            "business_date": "2024-01-15",
            "instrument_id": "GS",
            "desk_id": "DESK-B",
            "region": "EMEA",
            "exposure_amount": 2_000_000.0,
            "limit_amount": 1_500_000.0,
            "breach_flag": True,
            "breach_pct": 133.3,
        },
    ])


def test_breach_summary_counts():
    df = _exposure_df()
    summary = _build_breach_summary(df)
    assert set(summary.columns) >= {"breach_count", "breach_rate", "total_positions"}
    total_breaches = summary["breach_count"].sum()
    assert total_breaches == 2


def test_breach_summary_rate():
    df = _exposure_df()
    summary = _build_breach_summary(df)
    na_row = summary[summary["region"] == "NA"].iloc[0]
    assert na_row["total_positions"] == 2
    assert na_row["breach_count"] == 1
    assert abs(na_row["breach_rate"] - 0.5) < 0.01


def test_top_breaches_only_breach_rows():
    df = _exposure_df()
    top = _build_top_breaches(df, top_n=10)
    assert len(top) == 2
    assert all(top["breach_flag"] == True)


def test_top_breaches_top_n_limit():
    rows = []
    for i in range(25):
        rows.append({
            "business_date": "2024-01-15",
            "instrument_id": f"INST-{i:03d}",
            "desk_id": "DESK-A",
            "region": "NA",
            "exposure_amount": float(1_000_000 + i * 10_000),
            "limit_amount": 1_000_000.0,
            "breach_flag": True,
            "breach_pct": float(100 + i),
        })
    df = pd.DataFrame(rows)
    top = _build_top_breaches(df, top_n=5)
    assert len(top) <= 5
