# Raw Envelope Standard

All ingestion connectors must return data wrapped in a `RawEnvelope`. This provides a consistent, audit-friendly envelope for any API or file integration.

## Purpose
- Standardize ingestion metadata across all sources.
- Ensure traceability (source, endpoint, params, run ID).
- Support append-only raw storage and replay.

## Required Fields
| Field | Type | Description |
|---|---|---|
| source | string | Provider name (e.g., coingecko, stock) |
| endpoint | string | Full URL or endpoint path used for the fetch |
| request_params | object | Parameters sent with the request |
| request_id | string | Unique ID for the request |
| fetched_at | string | ISO 8601 UTC timestamp |
| run_id | string | ID of the ingestion run |
| payload | object | Raw or lightly structured payload from the provider |

## Example (Conceptual)
- `source`: "coingecko"
- `endpoint`: "https://api.coingecko.com/api/v3/coins/bitcoin/market_chart"
- `request_params`: `{ "vs_currency": "usd", "days": "7", "coin_id": "bitcoin" }`
- `request_id`: "uuid"
- `fetched_at`: "2026-02-08T03:32:09.877048+00:00"
- `run_id`: "uuid"
- `payload`: `{ ... }`

## Implementation
See the data class in [src/ingestion/models.py](models.py).

## Rules for New Integrations
- Always return `List[RawEnvelope]`.
- Do not mutate raw payload contents; place provider data under `payload`.
- If light structuring is required, store it inside `payload` without removing raw data.
- Keep ingestion append-only; never overwrite raw envelopes.
