import pandas as pd

from quality.rules import (
    validate_market_prices,
    validate_credit_exposure,
    validate_limit_thresholds,
    validate_reg_reference,
)


def test_market_prices_negative_price_fails():
    df = pd.DataFrame(
        [
            {
                "business_date": "2024-01-15",
                "instrument_id": "AAPL",
                "price": -10,
                "currency": "USD",
                "source_system": "BLOOMBERG",
                "ingest_timestamp": "2024-01-15T09:00:00Z",
            }
        ]
    )
    valid, invalid = validate_market_prices(df)
    assert len(valid) == 0
    assert len(invalid) == 1
    assert "price_non_positive" in invalid.iloc[0]["dq_reason"]


def test_credit_exposure_duplicate_key_fails():
    df = pd.DataFrame(
        [
            {
                "business_date": "2024-01-15",
                "instrument_id": "JPM",
                "desk_id": "DESK-EQUITY",
                "region": "NA",
                "exposure_amount": 1000000,
                "currency": "USD",
                "ingest_timestamp": "2024-01-15T09:00:00Z",
            },
            {
                "business_date": "2024-01-15",
                "instrument_id": "JPM",
                "desk_id": "DESK-EQUITY",
                "region": "NA",
                "exposure_amount": 2000000,
                "currency": "USD",
                "ingest_timestamp": "2024-01-15T09:00:00Z",
            },
        ]
    )
    valid, invalid = validate_credit_exposure(df)
    assert len(valid) == 0
    assert len(invalid) == 2
    assert "duplicate_business_key" in invalid.iloc[0]["dq_reason"]


def test_limit_thresholds_zero_limit_fails():
    df = pd.DataFrame(
        [
            {
                "instrument_id": "AAPL",
                "desk_id": "DESK-EQUITY",
                "region": "NA",
                "limit_amount": 0,
                "effective_date": "2024-01-01",
                "currency": "USD",
            }
        ]
    )
    valid, invalid = validate_limit_thresholds(df)
    assert len(valid) == 0
    assert len(invalid) == 1
    assert "limit_non_positive" in invalid.iloc[0]["dq_reason"]