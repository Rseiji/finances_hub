from __future__ import annotations

import uuid
from typing import Any, Dict, List

from ingestion.http_client import fetch_json
from ingestion.models import RawEnvelope

COINGECKO_BASE = "https://api.coingecko.com/api/v3"


def fetch_market_chart(
    coin_id: str,
    days: str = "max",
    vs_currency: str = "usd",
    asset: str | None = None,
) -> List[RawEnvelope]:
    endpoint = f"{COINGECKO_BASE}/coins/{coin_id}/market_chart"
    params: Dict[str, Any] = {
        "vs_currency": vs_currency,
        "days": days,
    }
    payload = fetch_json(endpoint, params=params)

    envelope = RawEnvelope(
        source="coingecko",
        endpoint=endpoint,
        request_params={**params, "coin_id": coin_id},
        asset=asset or coin_id,
        currency=vs_currency,
        uid=str(uuid.uuid4()),
        fetched_at=RawEnvelope.utc_now_iso(),
        payload=payload,
    )
    return [envelope]


def fetch_current_price(
    coin_id: str,
    vs_currency: str = "usd",
    asset: str | None = None,
) -> List[RawEnvelope]:
    endpoint = f"{COINGECKO_BASE}/simple/price"
    params: Dict[str, Any] = {
        "ids": coin_id,
        "vs_currencies": vs_currency,
    }
    payload = fetch_json(endpoint, params=params)

    envelope = RawEnvelope(
        source="coingecko",
        endpoint=endpoint,
        request_params={**params, "coin_id": coin_id},
        asset=asset or coin_id,
        currency=vs_currency,
        uid=str(uuid.uuid4()),
        fetched_at=RawEnvelope.utc_now_iso(),
        payload=payload,
    )
    return [envelope]


def fetch_bitcoin_daily_prices(
    days: int,
    vs_currency: str = "usd",
    asset: str | None = None,
) -> List[RawEnvelope]:
    endpoint = f"{COINGECKO_BASE}/coins/bitcoin/market_chart"
    params: Dict[str, Any] = {
        "vs_currency": vs_currency,
        "days": str(days),
        "interval": "daily",
    }
    payload = fetch_json(endpoint, params=params)

    envelope = RawEnvelope(
        source="coingecko",
        endpoint=endpoint,
        request_params={**params, "coin_id": "bitcoin"},
        asset=asset or "bitcoin",
        currency=vs_currency,
        uid=str(uuid.uuid4()),
        fetched_at=RawEnvelope.utc_now_iso(),
        payload=payload,
    )
    return [envelope]
