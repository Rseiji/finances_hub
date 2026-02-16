from __future__ import annotations

import uuid
from datetime import date, datetime, time, timezone
from typing import Any, Dict, List

from ingestion.http_client import fetch_json_any
from ingestion.models import RawEnvelope

BINANCE_BASE = "https://api.binance.com/api/v3"
BINANCE_INTERVAL = "1d"
BINANCE_LIMIT = 1000


def fetch_klines_daily(
    symbol: str,
    start_date: str,
    end_date: str | None = None,
    quote_currency: str = "usdt",
    asset: str | None = None,
) -> List[RawEnvelope]:
    start = _parse_date(start_date)
    end = _parse_date(end_date) if end_date else date.today()
    if start > end:
        raise ValueError("start_date must be <= end_date")

    start_ms = _to_epoch_ms(start)
    end_ms = _to_epoch_ms(end) + 86_399_000
    envelopes: List[RawEnvelope] = []

    while start_ms <= end_ms:
        endpoint = f"{BINANCE_BASE}/klines"
        params: Dict[str, Any] = {
            "symbol": symbol,
            "interval": BINANCE_INTERVAL,
            "startTime": start_ms,
            "endTime": end_ms,
            "limit": BINANCE_LIMIT,
        }
        payload = fetch_json_any(endpoint, params=params)
        if not isinstance(payload, list):
            raise ValueError("Unexpected Binance response shape")
        if not payload:
            break

        rows = _to_rows(payload)
        envelope = RawEnvelope(
            source="binance",
            endpoint=endpoint,
            request_params={
                **params,
                "start_date": start_date,
                "end_date": end_date or end.isoformat(),
                "quote_currency": quote_currency,
            },
            asset=asset or symbol,
            currency=quote_currency,
            uid=str(uuid.uuid4()),
            fetched_at=RawEnvelope.utc_now_iso(),
            payload={
                "symbol": symbol,
                "interval": BINANCE_INTERVAL,
                "rows": rows,
            },
        )
        envelopes.append(envelope)

        last_open = payload[-1][0]
        start_ms = int(last_open) + 1

    return envelopes


def _to_rows(payload: List[List[Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for item in payload:
        if len(item) < 7:
            continue
        rows.append(
            {
                "open_time": item[0],
                "close_time": item[6],
                "close": item[4],
            }
        )
    return rows


def _parse_date(value: str | None) -> date:
    if not value:
        raise ValueError("date is required")
    return date.fromisoformat(value)


def _to_epoch_ms(value: date) -> int:
    dt = datetime.combine(value, time.min, tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)
