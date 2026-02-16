# Silver Schema

This document describes the silver schema tables and columns used to normalize bronze ingestion payloads into query-friendly datasets.

## Table: silver.yfinance_prices
Normalized price points from the YFinance API.

Columns:
- request_id: Bronze ingestion event UID that produced this row.
- symbol: Market symbol queried from YFinance (e.g., ^GSPC, ^BVSP).
- asset: Internal asset label used by ingestion (often same as symbol).
- currency: Quoted currency for the price (e.g., usd, brl).
- price: Price value as numeric.
- price_ts: Timestamp of the price observation in UTC (daily series uses midnight).
- price_date: Date portion of price_ts, used for daily grouping.
- fetched_at: When the bronze event was fetched from the API (UTC).
- ingested_at: When the silver row was written (UTC).

Keys and constraints:
- Unique key: (symbol, price_date)

## Table: silver.binance_prices
Normalized crypto price points from Binance daily klines.

Columns:
- request_id: Bronze ingestion event UID that produced this row.
- symbol: Binance trading pair (e.g., BTCUSDT).
- asset: Internal asset label (e.g., BTC).
- currency: Quoted currency for the price (e.g., usdt).
- price: Close price value as numeric.
- price_ts: Timestamp of the price observation in UTC.
- price_date: Date portion of price_ts, used for daily grouping.
- fetched_at: When the bronze event was fetched from the API (UTC).
- ingested_at: When the silver row was written (UTC).

Keys and constraints:
- Unique key: (symbol, currency, price_date)
