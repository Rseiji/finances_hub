# Project Guide

This document is the source of truth for building Finances Hub. It defines scope, architecture, data flow, and implementation guidance for agent-driven development.

## 1. Problem Statement
Build a local, free-to-run data platform to ingest stock and crypto data (prices, trades, wallets), normalize it, store it in a raw layer, transform it to curated datasets, and expose analytics for tax reporting and performance metrics.

## 2. Objectives
- Provide ETL pipelines for:
  - Stock price time series
  - Crypto price time series
  - Trade activity (buy/sell)
  - Wallet and holdings snapshots
- Store data in a layered model:
  - Raw: as-received data
  - Staged: normalized types and schemas
  - Curated: analytics-ready views and aggregates
- Enable orchestration and scheduling on a local machine
- Provide APIs or exports for reporting and dashboards

## 2.1 Terminology
- **Asset**: A tradeable instrument (stock, ETF, crypto, token).
- **Account**: A brokerage, exchange, or wallet boundary.
- **Position**: Holdings for an asset in an account at a point in time.
- **Trade**: A buy/sell execution with quantity, price, fees, and timestamp.
- **Lot**: A specific purchase batch used for tax reporting.
- **Price**: A point-in-time valuation for an asset in a currency.

## 3. Guiding Principles
- Local-first and free (no paid API dependencies).
- Deterministic, reproducible pipelines.
- Schema-first and documented.
- Composable components to allow incremental expansion.
- Clear separation of ingestion, processing, and analytics.

## 4. Architecture Overview
### 4.1 Layers
1. **Ingestion**: Fetch data from public APIs or data providers.
2. **Storage (Raw)**: Store exact payloads with metadata (source, timestamp, request parameters).
3. **Processing**: Normalize, deduplicate, and validate.
4. **Storage (Curated)**: Analytical tables, aggregates, and materialized views.
5. **Analytics/Reporting**: Metrics, tax summaries, ROI, dashboards.

### 4.2 Components
- **Ingestion Services**: Source-specific connectors.
- **Orchestrator**: Schedule pipelines and handle dependencies.
- **Storage Layer**: Database with raw/staged/curated schemas.
- **API Layer**: Simple endpoints to serve data to dashboards.

### 4.3 Architecture Diagram
```mermaid
flowchart LR
  A[Data Sources\nStocks • Crypto • Trades] --> B[Ingestion Connectors]
  B --> C[(Raw Schema\nPostgres)]
  C --> D[Processing & Validation]
  D --> E[(Staged Schema\nPostgres)]
  E --> F[Curated Models & Aggregates]
  F --> G[(Curated Schema\nPostgres)]
  G --> H[Analytics & Reports\nDashboards • Tax]
  H --> I[API / Exports]
  O[Orchestrator & Scheduler] -.-> B
  O -.-> D
  O -.-> F
```

## 5. Data Sources (Initial Targets)
- Stocks: public endpoints (e.g., free market data providers).
- Crypto: public endpoints (e.g., exchange APIs or aggregators).
- Trades/Transactions: CSV imports or exchange APIs.

All sources must be free or allow free tiers. See docs/DATA_SOURCES.md for provider selection.

## 6. Data Model (High-Level)
### 6.1 Raw Tables
- raw_prices_stock
- raw_prices_crypto
- raw_trades
- raw_wallets

### 6.2 Staged Tables
- stg_prices (normalized symbol, timestamp, price, currency)
- stg_trades (normalized trade fields)
- stg_wallets (normalized balances)

### 6.3 Curated Tables
- fact_returns
- fact_positions
- fact_pnl
- dim_assets
- dim_accounts

## 6.3.1 Schema Contract (Required)
All staged and curated tables must be defined with explicit column names, types, and keys in a single schema contract document. This contract must be updated before any pipeline implementation to ensure consistent field naming and reduce agent ambiguity.

## 6.4 Table Update Strategy
- **Raw tables**: **Append-only**. Store every payload with ingestion metadata (source, fetched_at, request_params). Never overwrite raw records.
- **Staged tables**: **Upsert** on natural keys (e.g., asset + timestamp + source). Use deterministic deduplication rules.
- **Curated tables**: **Upsert or overwrite by partition** (e.g., by date). For daily aggregates, overwrite the affected date range during backfills.
- **Dimensions**: **Upsert** on stable business keys (e.g., asset_symbol, account_id).

## 6.5 Data Quality Restrictions
- **Timestamps**: Must be UTC, not null, and aligned to expected granularity (daily/hourly). No future timestamps beyond ingestion time + 5 minutes.
- **Prices**: Must be numeric and > 0. Reject zero/negative values.
- **Volumes**: Must be numeric and >= 0. Reject negative values.
- **Symbols**: Must be normalized (uppercase, no whitespace). Unknown symbols go to a quarantine table.
- **Duplicates**: No duplicate records for the same natural key (symbol + timestamp + source).
- **Currency**: Must be valid ISO 4217 codes when present.
- **Outliers**: Flag values that deviate > 50% from a 24h rolling median for review (do not drop automatically).
- **Quarantine**: Invalid records are stored in a quarantine table with error reasons for audit and reprocessing.

## 6.6 Raw DDL Generalization Principles
To support new sources without reworking storage, raw ingestion must follow a **generic envelope** pattern:
- Each raw table stores: `source`, `endpoint`, `request_params`, `request_id`, `fetched_at`, `run_id`, and the **provider payload** (JSON).
- Raw tables can be source-specific (e.g., `raw_prices_stock`) but must keep the same metadata columns to allow uniform replay and auditing.
- New sources must map into the existing raw categories (prices, trades, wallets). Only add new raw tables if the data does not fit these categories.
- All raw tables must be append-only and never updated in place.

### 6.7 Ingestion Injection Usage Example
Ingestion functions should return envelopes only. Downstream code injects those envelopes into storage.

Example:
```python
import os

from ingestion.coingecko import fetch_bitcoin_daily_prices
from ingestion.persist import inject_envelopes

os.environ["FINANCES_HUB_PG_DSN"] = "postgresql://my_user_here:my_pwd_here@localhost:5432/finances_hub"
os.environ["FINANCES_HUB_SINK"] = "postgres"

envelopes = fetch_bitcoin_daily_prices(days=7, vs_currency="usd")
inject_envelopes(envelopes, category="coingecko")
```

## 7. Orchestration
- Daily jobs for prices.
- Event-driven jobs for trades or manual import.
- Retry strategy and logging for failed ingestions.
- **Backfill requirement**: Each pipeline run must detect missed dates and backfill historical data so that delayed or missed schedules do not create gaps. The ETL should fetch from the last successful date through the current date.

## 7.1 Data Pipeline Pattern
This project follows the **Medallion Architecture** (Bronze/Silver/Gold):
- **Bronze**: Raw schema (as-received payloads).
- **Silver**: Staged schema (normalized, validated).
- **Gold**: Curated schema (analytics-ready facts/dimensions).

## 7.2 Analytics Transformations (Planned)
The analytics layer must support multiple aggregation types and metrics. Initial transformation targets:
- **Portfolio valuation**: daily portfolio value by account, asset class, and currency.
- **PnL**: realized and unrealized profit/loss by asset, account, and date.
- **Returns**: time-weighted and money-weighted returns by portfolio and asset.
- **Cost basis**: FIFO/LIFO/average cost methods (configurable per account).
- **Cash flow**: deposits/withdrawals, dividends, fees, and transfers.
- **Tax lots**: lot-level tracking for sales with gain/loss classification.
- **Performance attribution**: allocation vs selection effects (optional later phase).
- **Benchmarks**: compare to market indices or crypto benchmarks (optional later phase).

These transformations must be implemented as curated facts and dimensions, with daily/weekly/monthly aggregates to enable dashboards and reports.

## 8. Security & Privacy
- Local storage by default.
- No keys committed to source control.
- Use environment variables for credentials.

## 9. Milestones
1. Skeleton project structure with docs and config.
2. Implement one stock price ingestion.
3. Implement one crypto price ingestion.
4. Raw storage + staging transforms.
5. Curated analytics and tax reports.
6. API layer for dashboards.

## 9.1 MVP Scope Clarification
- Define Medallion schemas and generic raw envelope.
- Ingest **1–2 data sources** (one stock, one crypto) with backfill and data quality rules.
- Produce **MVP curated metrics**: daily portfolio value, basic PnL, and simple returns.
- Connect Metabase to curated tables for basic dashboards.

## 10. Agent Instructions
All agent policies and checklists are maintained in [docs/AGENTS.md](docs/AGENTS.md). Consult it before implementing any changes.

## 11. Analytics Tooling
Primary analytics tooling will be **Metabase** connected to Postgres for dashboards and reports. Exports (CSV/Parquet) should also be supported for tax workflows.

## 12. Next Steps
- Confirm data sources in docs/DATA_SOURCES.md.
- Finalize Postgres schema contract and DDL.
- Implement ingestion connectors.
