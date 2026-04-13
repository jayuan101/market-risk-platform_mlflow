# Data Dictionary — MR Risk Platform

**Version:** 1.0.0
**Last Updated:** 2026-04-13
**Owner:** Market Risk Data Engineering
**S3 Location:** s3://mr-risk-platform/gold/
**Glue Database:** mr_risk_gold

---

## Table of Contents
- [market_prices](#market_prices)
- [exposure](#exposure)
- [limits](#limits)
- [reg_ref](#reg_ref)
- [Relationships](#relationships)
- [Partition Strategy](#partition-strategy)
- [Naming Conventions](#naming-conventions)

---

## market_prices

**Description:** End-of-day market prices for all financial instruments sourced from upstream pricing systems. One row per instrument per business date per source system.

**S3 Path:** `s3://mr-risk-platform/gold/market_risk/business_date=*/region=*/asset_class=*/`
**Source File:** `market_prices.csv`
**Update Frequency:** Daily (EOD batch, T+0 by 18:00 EST)
**Primary Key:** `(business_date, instrument_id, source_system)`

| Column | Data Type | Nullable | Format / Example | Description |
|---|---|---|---|---|
| `business_date` | `DATE` | NOT NULL | `2026-04-13` | The official trading/business date for the price record. Not the same as ingestion date. Used as partition key. |
| `instrument_id` | `STRING` | NOT NULL | `EQ-AAPL-US` | Unique identifier for a financial instrument. Follows `{asset_class_prefix}-{ticker}-{exchange_code}` convention. Foreign key → `reg_ref.instrument_id`. |
| `price` | `DOUBLE` | NOT NULL | `189.45` | Mid-market close price in the currency specified in the `currency` column. Must be > 0. |
| `currency` | `STRING(3)` | NOT NULL | `USD` | ISO 4217 3-letter currency code. Denotes the pricing currency for the instrument. |
| `source_system` | `STRING` | NOT NULL | `BLOOMBERG`, `REFINITIV`, `INTERNAL` | Originating upstream pricing system. Used for lineage tracking and source conflict resolution. |

**Business Rules:**
- If `source_system = INTERNAL`, price is derived from internal quant model — flag for validation.
- `price` must never be NULL or zero; failed rows land in `s3://mr-risk-platform/quarantine/market_prices/`.
- Duplicate `(business_date, instrument_id, source_system)` is rejected at ingestion.

---

## exposure

**Description:** Daily net financial exposure per instrument per desk per region. Represents the risk-weighted position size used for limit utilization calculations.

**S3 Path:** `s3://mr-risk-platform/gold/market_risk/business_date=*/region=*/`
**Source File:** `exposure.csv`
**Update Frequency:** Daily (EOD batch, T+0 by 19:00 EST)
**Primary Key:** `(business_date, instrument_id, desk_id, region)`

| Column | Data Type | Nullable | Format / Example | Description |
|---|---|---|---|---|
| `business_date` | `DATE` | NOT NULL | `2026-04-13` | Business date for the exposure snapshot. Aligns with `market_prices.business_date`. Partition key. |
| `instrument_id` | `STRING` | NOT NULL | `EQ-AAPL-US` | Instrument identifier. Foreign key → `reg_ref.instrument_id` and `market_prices.instrument_id`. |
| `desk_id` | `STRING` | NOT NULL | `DESK-EQ-NY-01` | Unique trading desk identifier. Foreign key → `limits.desk_id`. Format: `DESK-{asset_class}-{location}-{seq}`. |
| `exposure_amount` | `DOUBLE` | NOT NULL | `4500000.00` | Net exposure in USD (normalized). Positive = long, negative = short. |
| `region` | `STRING` | NOT NULL | `US`, `EMEA`, `APAC`, `LATAM` | Geographic region of the desk holding the position. Partition key. Controlled vocabulary. |

**Business Rules:**
- `exposure_amount` is always normalized to USD at time of ingestion using `market_prices.price` × FX rate.
- `region` must be one of: `US`, `EMEA`, `APAC`, `LATAM` — any other value fails schema validation.
- Negative exposure is valid (short positions) — do not filter out.

---

## limits

**Description:** Approved risk limits per desk per region as set by the Risk Management Committee. Defines the maximum allowable exposure per desk. Effective-dated to support limit amendments without history loss.

**S3 Path:** `s3://mr-risk-platform/gold/regulatory_reports/`
**Source File:** `limits.csv`
**Update Frequency:** Event-driven (updated on limit approval, typically weekly or on breach)
**Primary Key:** `(desk_id, region, effective_date)`

| Column | Data Type | Nullable | Format / Example | Description |
|---|---|---|---|---|
| `desk_id` | `STRING` | NOT NULL | `DESK-EQ-NY-01` | Trading desk identifier. Foreign key → `exposure.desk_id`. |
| `region` | `STRING` | NOT NULL | `US` | Geographic region the limit applies to. Must match `exposure.region` controlled vocabulary. |
| `limit_amount` | `DOUBLE` | NOT NULL | `10000000.00` | Maximum allowable net exposure in USD for this desk/region combination. Must be > 0. |
| `currency` | `STRING(3)` | NOT NULL | `USD` | Currency denomination of `limit_amount`. All limits normalized to USD at load time. |
| `effective_date` | `DATE` | NOT NULL | `2026-01-01` | Date the limit became active. Latest record per `(desk_id, region)` is the current active limit. |

**Business Rules:**
- To get the **current active limit**: filter `WHERE effective_date = MAX(effective_date)` per `(desk_id, region)`.
- Limit breaches are flagged when `exposure.exposure_amount > limits.limit_amount` for matching `(desk_id, region, business_date)`.
- Do NOT delete superseded limits — they are retained for regulatory audit trail.

---

## reg_ref

**Description:** Regulatory reference / classification table mapping each instrument to its asset class and regulatory reporting category. Slowly changing — updated when new instruments are onboarded or regulatory classifications change.

**S3 Path:** `s3://mr-risk-platform/gold/regulatory_reports/`
**Source File:** `reg_ref.csv`
**Update Frequency:** On-demand (instrument onboarding or regulatory reclassification)
**Primary Key:** `instrument_id`

| Column | Data Type | Nullable | Format / Example | Description |
|---|---|---|---|---|
| `instrument_id` | `STRING` | NOT NULL | `EQ-AAPL-US` | Unique instrument identifier. Primary key. Referenced by `market_prices` and `exposure`. |
| `asset_class` | `STRING` | NOT NULL | `EQUITY`, `FIXED_INCOME`, `FX`, `COMMODITY` | Top-level asset classification. Used as partition key in `market_prices` and `exposure` gold layer. Controlled vocabulary. |
| `reg_category` | `STRING` | NOT NULL | `FRTB_SA`, `FRTB_IMA`, `BASEL_III`, `EXEMPT` | Regulatory reporting bucket determining which capital calculation methodology applies. |

**Business Rules:**
- `asset_class` must be one of: `EQUITY`, `FIXED_INCOME`, `FX`, `COMMODITY`.
- `reg_category` must be one of: `FRTB_SA`, `FRTB_IMA`, `BASEL_III`, `EXEMPT`.
- This is a **reference/dimension table** — join to `market_prices` and `exposure` on `instrument_id`.
- Treat as SCD Type 2 if regulatory reclassification occurs — preserve history.

---

## Relationships
market_prices.instrument_id ──→ reg_ref.instrument_id
exposure.instrument_id ──→ reg_ref.instrument_id
exposure.desk_id ──→ limits.desk_id
exposure.region ──→ limits.region
market_prices.business_date == exposure.business_date


**Core Join Pattern — Limit Utilization:**
```sql
SELECT
    e.business_date,
    e.desk_id,
    e.region,
    r.asset_class,
    r.reg_category,
    e.exposure_amount,
    l.limit_amount,
    ROUND((e.exposure_amount / l.limit_amount) * 100, 2) AS utilization_pct,
    CASE WHEN e.exposure_amount > l.limit_amount THEN 'BREACH' ELSE 'OK' END AS limit_status
FROM exposure e
JOIN limits l
    ON e.desk_id = l.desk_id
    AND e.region = l.region
    AND l.effective_date = (
        SELECT MAX(effective_date)
        FROM limits
        WHERE desk_id = e.desk_id AND region = e.region
    )
JOIN reg_ref r
    ON e.instrument_id = r.instrument_id
WHERE e.business_date = '2026-04-13'
```

---

## Partition Strategy

| Table | Partition Keys (in order) | Rationale |
|---|---|---|
| `market_prices` | `business_date`, `region`, `asset_class` | Date-range queries dominate; region/asset_class eliminate large data slices |
| `exposure` | `business_date`, `region` | Desk-level queries always scoped to date + region |
| `limits` | None (small, static) | Full scan is trivial; filter in query on `effective_date` |
| `reg_ref` | None (reference table) | ~10K rows max; broadcast join in Spark |

---

## Naming Conventions

- **Dates:** Always `YYYY-MM-DD` (ISO 8601) — no epoch, no `YYYYMMDD` integers
- **IDs:** Composite human-readable keys (`DESK-EQ-NY-01`) preferred over surrogate integers for auditability
- **Currency:** ISO 4217 3-letter code always (`USD`, `EUR`, `GBP`) — never symbols
- **Amounts:** Always `DOUBLE` in USD normalized — raw currency stored in `currency` column alongside
- **Enums:** All controlled vocabulary fields documented above — schema validation rejects any unlisted value
- **Nullability:** No nullable fields in gold layer — NULL values fail upstream Great Expectations checks and route to quarantine

---

## Changelog

| Version | Date | Author | Change |
|---|---|---|---|
| 1.0.0 | 2026-04-13 | MR Risk DE Team | Initial schema definitions for all four source datasets |