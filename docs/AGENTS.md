# Agent Instructions

This document defines the required implementation rules for any agent working on Finances Hub. It is the source of truth for agent behavior, coding policies, and delivery requirements.

## 1. Dependency Policy
- Use only standard libraries unless a written justification is added to docs/PROJECT_GUIDE.md and approved.
- No new dependency without explicit approval.
- Use **Poetry** for dependency management and lockfile updates.

## 2. Testing Policy
- Every new pipeline must include:
  - Unit tests for parsing/validation.
  - An integration test using mocked HTTP or fixtures.
- No pipeline merges without tests.
- Use **pytest** for all unit and integration tests.
- Ensure tests can import from src/ by maintaining tests/conftest.py.

## 3. File Size Policy
- No file may exceed **250 lines**.
- If needed, split into modules.
- Exceptions require a written rationale in the file header.

## 4. Schema Policy
- All ingest/stage/curated schemas must be explicitly typed and validated (Pydantic or dataclasses + validators).
- No untyped dicts in processing layers.
- All staged and curated tables must be defined with explicit column names, types, and keys in the schema contract.

## 5. Idempotency & Keying
- All ingestion connectors must be idempotent and support backfill windows; repeated runs must not create duplicates.
- Use deterministic primary keys for upserts (e.g., symbol + timestamp + source).
- No surrogate keys in staged/curated tables unless justified.

## 6. Raw Metadata Policy
- Store source, endpoint, params, asset, currency, uid, and fetched_at for every raw record.
- Raw tables are append-only and use the generic envelope pattern.

## 7. Logging Policy
- Structured JSON logs with correlation IDs per run.
- Errors must include source and request context.

## 8. Secrets Policy
- No secrets in code or tests.
- Use environment variables and .env templates only.

## 9. Documentation Policy
- Each new pipeline requires a dedicated doc in docs/ describing inputs, outputs, cadence, schema, and backfill strategy.
- Every schema change must include a migration note and backward-compatibility strategy.

## 10. Code Style & Typing
- Conform to PEP 8 with a maximum line length of **120** characters.
- All new code must pass `mypy` with strict type checking for core pipeline modules.
- Prefer PEP 604 unions (e.g., `str | None`) over `typing.Optional`.
- Order code top-down: public API first, helpers last.
- Every new Python package or submodule directory must include an `__init__.py`.

## 11. Agent Checklist (Must-Read)
- Uses Postgres and Medallion layers.
- Raw tables are append-only with generic envelope metadata.
- Staged and curated tables follow the schema contract.
- Backfill logic uses last successful date through current date.
- Data quality rules enforced and quarantine on invalid records.
- Idempotent ingestion with deterministic keys.
- PEP 8, line length ≤ 120, and `mypy` strict for core modules.
- Poetry + pytest required for dependencies and tests.
- New Python submodules include `__init__.py`.
- tests/conftest.py exists and adds src/ to the import path.
