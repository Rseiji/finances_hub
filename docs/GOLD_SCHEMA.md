# Gold Schema

This document describes gold-layer historical tables used for analytics and dashboards. Each table stores daily price history for a specific asset category, with a consistent pattern so new categories can be added later.

## Table: gold.fiat_fx_rates_daily
Daily fiat FX rates, quoted as base/quote currency pairs (e.g., USDBRL).

Columns:
- asset: Pair code such as USDBRL or JPYBRL.
- base_currency: Base currency code (e.g., USD).
- quote_currency: Quote currency code (e.g., BRL).
- currency: Quote currency code (e.g., BRL).
- rate: FX rate for one unit of base in quote.
- rate_date: Date for the observation (daily grain).
- rate_ts: Timestamp for the observation in UTC (daily series uses midnight).
- source: Source system or provider name.
- silver_request_id: Lineage pointer to the silver request/event that produced this row.
- ingested_at: When the gold row was written (UTC).

Keys and constraints:
- Unique key: (asset, currency, rate_date)

## Table: gold.crypto_prices_daily
Daily crypto prices, stored with high-precision numeric values.

Columns:
- asset: Internal asset label (e.g., BTC, ETH).
- currency: Quoted currency for the price (e.g., usd).
- price: Daily price with high precision (NUMERIC(38, 18)).
- price_date: Date for the observation (daily grain).
- price_ts: Timestamp for the observation in UTC (daily series uses midnight).
- source: Source system or provider name.
- silver_request_id: Lineage pointer to the silver request/event that produced this row.
- ingested_at: When the gold row was written (UTC).

Keys and constraints:
- Unique key: (asset, currency, price_date)

## Table: gold.br_stocks_daily
Daily prices for Brazilian equities.

Columns:
- asset: Internal asset label.
- currency: Quoted currency for the price (e.g., brl).
- price: Daily price (NUMERIC(18, 6)).
- price_date: Date for the observation (daily grain).
- price_ts: Timestamp for the observation in UTC (daily series uses midnight).
- source: Source system or provider name.
- silver_request_id: Lineage pointer to the silver request/event that produced this row.
- ingested_at: When the gold row was written (UTC).

Keys and constraints:
- Unique key: (asset, currency, price_date)

## Table: gold.indices_daily
Daily prices for index series (e.g., S&P 500, IBOV).

Columns:
- asset: Index identifier (e.g., SP500, IBOV).
- currency: Quoted currency for the index.
- price: Daily index value (NUMERIC(18, 6)).
- price_date: Date for the observation (daily grain).
- price_ts: Timestamp for the observation in UTC (daily series uses midnight).
- source: Source system or provider name.
- silver_request_id: Lineage pointer to the silver request/event that produced this row.
- ingested_at: When the gold row was written (UTC).

Keys and constraints:
- Unique key: (asset, currency, price_date)

## Table: gold.etfs_daily
Daily prices for ETFs (e.g., IVVB11).

Columns:
- asset: ETF symbol (e.g., IVVB11).
- currency: Quoted currency for the price.
- price: Daily price (NUMERIC(18, 6)).
- price_date: Date for the observation (daily grain).
- price_ts: Timestamp for the observation in UTC (daily series uses midnight).
- source: Source system or provider name.
- silver_request_id: Lineage pointer to the silver request/event that produced this row.
- ingested_at: When the gold row was written (UTC).

Keys and constraints:
- Unique key: (asset, currency, price_date)

## Topology Recommendation
A category-specific, multi-star topology works best here:
- Each asset category is its own star schema (fact table + shared/conformed dimensions).
- Keep one gold fact table per asset category (FX, crypto, equities, indices, ETFs).
- Use consistent column names and types across tables to simplify downstream dashboards.
- Add conformed dimensions later (e.g., dim_asset, dim_currency, dim_market) to support cross-category joins without changing the fact tables.
- New asset categories can be added by creating another daily fact table following the same column pattern.
