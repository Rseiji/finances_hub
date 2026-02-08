# Backlog Checklist

This backlog is a living checklist. Check items as they are completed.

## 1) Orchestration & Backfills
- [ ] Add a minimal orchestrator to run ingestion jobs.
- [ ] Implement backfill logic (detect last successful date and fill gaps).
- [ ] Define retry strategy and logging for failed runs.

## 2) Bronze Storage & Retention
- [ ] Confirm indexing strategy for `bronze.ingestion_events` based on query patterns.
- [ ] Define a retention/archival policy for raw envelopes.
- [ ] Add a lightweight maintenance job (optional) for pruning or archiving.

## 3) Configuration & Dependency Injection
- [ ] Centralize configuration (DSN, sink, API settings) in a config module.
- [ ] Ensure storage and ingestion accept injected configuration instead of reading env directly.
- [ ] Document required environment variables.

## 4) Data Quality & Quarantine
- [ ] Add validation rules for timestamps, prices, symbols, and duplicates.
- [ ] Create a quarantine table for invalid records with error reasons.
- [ ] Ensure validation is applied before silver/staged transforms.

## 5) Testing Strategy
- [ ] Add a Postgres smoke test (connect, insert, read).
- [ ] Add integration tests for ingestion → bronze write path.
- [ ] Keep unit tests for envelope creation and validation logic.

## 6) Documentation & Runbook
- [ ] Add “Run end-to-end locally” steps to the README.
- [ ] Add sample SQL queries for exploring bronze data in DBeaver.
- [ ] Document ingestion backfill behavior and scheduling.

## 7) Roadmap (Optional Next Phase)
- [ ] Add staged (silver) models for normalized prices.
- [ ] Add curated (gold) models for PnL and performance metrics.
- [ ] Add simple API endpoints for analytics and exports.

## Insights Log
- [ ] Add new insight: ...
- [ ] Add new insight: ...
