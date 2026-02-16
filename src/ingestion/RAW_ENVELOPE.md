# Raw Envelope Standard

All ingestion connectors must return data wrapped in a `RawEnvelope`. This provides a consistent, audit-friendly envelope for any API or file integration.

## Purpose
- Standardize ingestion metadata across all sources.
- Ensure traceability (source, endpoint, params, uid).
- Support append-only raw storage and replay.

## Required Fields
| Field | Type | Description |
|---|---|---|
| source | string | Provider name (e.g., binance, yfinance) |
| endpoint | string | Full URL or endpoint path used for the fetch |
| request_params | object | Parameters sent with the request |
| asset | string | Asset being ingested (e.g., bitcoin, ^GSPC) |
| currency | string | Quote currency used for valuation (e.g., usd, brl) |
| uid | string | Unique ID for the envelope |
| fetched_at | string | ISO 8601 UTC timestamp |
| payload | object | Raw or lightly structured payload from the provider |

## Example (Conceptual)
- `source`: "binance"
- `endpoint`: "https://api.binance.com/api/v3/klines"
- `request_params`: `{ "vs_currency": "usd", "days": "7", "coin_id": "bitcoin" }`
- `asset`: "bitcoin"
- `currency`: "usd"
- `uid`: "uuid"
- `fetched_at`: "2026-02-08T03:32:09.877048+00:00"
- `payload`: `{ ... }`

## Implementation
See the data class in [src/ingestion/models.py](models.py).

## Rules for New Integrations
- Always return `List[RawEnvelope]`.
- Do not mutate raw payload contents; place provider data under `payload`.
- If light structuring is required, store it inside `payload` without removing raw data.
- Keep ingestion append-only; never overwrite raw envelopes.
