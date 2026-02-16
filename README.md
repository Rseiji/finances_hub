# Finances Hub

Finances Hub is a personal data platform for ingesting financial data (stocks and cryptocurrencies), processing it, and producing analytics such as tax reports and dashboards. This repository is designed to be fully automated with agent-driven development.

## Goals
- Ingest time-series data for stocks and cryptocurrencies from free data sources.
- Collect trades (buy/sell), holdings, and wallet data.
- Normalize and store raw data in a raw database layer.
- Process and transform data into analytics-ready models.
- Provide metrics for profits, ROI, and tax reporting.
- Run locally as lightweight services, free to operate.

## Scope (Initial)
- ETL pipelines for prices and trades.
- API integrations for selected providers.
- Orchestration for scheduled ingestion and processing.
- A structured data model for raw and curated data.
- Basic reporting endpoints and/or exports.

## Non-Goals (Initial)
- Live trading execution.
- Paid API usage or vendor lock-in.
- Cloud-only infrastructure requirements.

## Technology Choices (Initial)
### Why Postgres?
- **Local-first and free**: Runs on a laptop without cloud dependencies and has a mature open-source ecosystem.
- **Strong raw storage fit**: JSONB columns store provider payloads without premature schema decisions, while still allowing indexing and querying.
- **Reliability**: ACID guarantees make it safe for append-only ingestion and replayable pipelines.
- **Analytics-friendly**: Works well with BI tools (e.g., Metabase) and supports SQL for curated models.
- **Extensible**: Easy to add schemas (bronze/silver/gold) and evolve tables over time.

### Why not a simpler database like DuckDB as the go-to solution?
- **Embedded by design**: Great for local analytics, but less suited to long-running services and multi-client concurrency.
- **Cloud path**: Postgres maps directly to managed cloud databases when you outgrow local setups.
- **Operational fit**: Ingestion pipelines benefit from a server database that can accept writes while queries run.

## Version Control Policy
- Single-person project using only the `master` branch with direct commits (no feature branches).
- Revisit this policy if collaborators join or scope expands.
- This may change in the future according to my needs.

## Running the Project
### Data Ingestion
1. Ensure Postgres is running and the bronze schema is applied.
2. Set the database connection string:
  - `FINANCES_HUB_PG_DSN=postgresql://<user>:<password>@localhost:5432/finances_hub`
3. Run ingestion jobs or orchestration as needed.

#### Bronze Ingestion (Default Jobs)
Run the default bronze layer ingestion (binance crypto + yfinance SP500/IBOV):

```bash
source .venv/bin/activate
export FINANCES_HUB_PG_DSN=postgresql://<user>:<password>@localhost:5432/finances_hub
export FINANCES_HUB_SINK=postgres
PYTHONPATH=src python - <<'PY'
from orchestration.runner import OrchestrationConfig, run_all

config = OrchestrationConfig()
results = run_all(config=config)
print(results)
PY
```

## Repository Structure (Planned)
```
finances_hub/
  docs/
    PROJECT_GUIDE.md
    ARCHITECTURE.md
    DATA_SOURCES.md
  src/
    ingestion/
    orchestration/
    processing/
    storage/
    api/
    analytics/
  tests/
```

## First Step
See the project documentation in docs/PROJECT_GUIDE.md for the full requirements, architecture, and implementation notes.
