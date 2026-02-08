# Architecture

## Service Boundaries
- **Ingestion**: connectors for each provider.
- **Processing**: normalization and validation.
- **Storage**: raw, staged, curated schemas.
- **API**: optional REST/CLI interface for reports.

## Data Flow
1. Fetch from provider API.
2. Store raw payload + metadata.
3. Normalize to staging schema.
4. Create curated facts and dimensions.
5. Expose analytics.

## Deployment Model
- Local, scheduled tasks or lightweight orchestrator.
- Environment variables for keys.
- Logs persisted locally.

## Observability
- Structured logs per pipeline.
- Metrics: records ingested, errors, latency.

## Tech Stack (Proposed)
- Language: Python.
- Storage: Postgres.
- Orchestration: cron-compatible runner or lightweight scheduler.
