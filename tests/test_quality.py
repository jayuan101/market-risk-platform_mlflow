import os
import sys

import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from quality.rules import (
    validate_market_prices,
    validate_credit_exposure,
    validate_limit_thresholds,
    validate_reg_reference,
)


def _valid_prices_row(**overrides):
    base = {
        "business_date": "2024-01-15",
        "instrument_id": "AAPL",
        "price": 150.0,
        "currency": "USD",
        "source_system": "BLOOMBERG",
        "ingest_timestamp": "2024-01-15T09:00:00Z",
    }
    base.update(overrides)
    return base


def test_valid_market_prices_passes():
    df = pd.DataFrame([_valid_prices_row()])
    valid, invalid = validate_market_prices(df)
    assert len(valid) == 1
    assert len(invalid) == 0


def test_null_instrument_id_fails():
    df = pd.DataFrame([_valid_prices_row(instrument_id=None)])
    valid, invalid = validate_market_prices(df)
    assert len(invalid) == 1
    assert "instrument_id_null" in invalid.iloc[0]["dq_reason"]


def test_valid_reg_reference_passes():
    df = pd.DataFrame([{
        "instrument_id": "AAPL",
        "asset_class": "EQUITY",
        "reg_category": "TIER1",
        "reporting_flag": True,
        "last_updated": "2024-01-01",
    }])
    valid, invalid = validate_reg_reference(df)
    assert len(valid) == 1
    assert len(invalid) == 0


def test_null_reg_reference_instrument_fails():
    df = pd.DataFrame([{
        "instrument_id": None,
        "asset_class": "EQUITY",
        "reg_category": "TIER1",
        "reporting_flag": True,
        "last_updated": "2024-01-01",
    }])
    valid, invalid = validate_reg_reference(df)
    assert len(invalid) == 1


def test_exposure_over_limit_passes_dq():
    """Breach detection is a business rule in transform, not a DQ failure."""
    df = pd.DataFrame([{
        "business_date": "2024-01-15",
        "instrument_id": "JPM",
        "desk_id": "DESK-A",
        "region": "NA",
        "exposure_amount": 50_000_000.0,
        "currency": "USD",
        "ingest_timestamp": "2024-01-15T09:00:00Z",
    }])
    valid, invalid = validate_credit_exposure(df)
    assert len(valid) == 1
    assert len(invalid) == 0


def test_valid_limit_thresholds_passes():
    df = pd.DataFrame([{
        "instrument_id": "AAPL",
        "desk_id": "DESK-A",
        "region": "NA",
        "limit_amount": 1_000_000.0,
        "effective_date": "2024-01-01",
        "currency": "USD",
    }])
    valid, invalid = validate_limit_thresholds(df)
    assert len(valid) == 1
    assert len(invalid) == 0
