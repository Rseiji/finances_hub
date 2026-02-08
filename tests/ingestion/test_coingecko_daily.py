from __future__ import annotations

import json
from typing import Any, Dict
from unittest.mock import patch

from ingestion.coingecko import fetch_bitcoin_daily_prices


def _fake_response(payload: Dict[str, Any]) -> bytes:
    return json.dumps(payload).encode("utf-8")


def test_fetch_bitcoin_daily_prices_returns_envelope() -> None:
    payload = {"prices": [[1, 100.0]]}

    class _Resp:
        def __enter__(self) -> "_Resp":
            return self

        def __exit__(self, exc_type, exc, tb) -> None:  # type: ignore[no-untyped-def]
            return None

        def read(self) -> bytes:
            return _fake_response(payload)

    with patch("ingestion.http_client.urlopen", return_value=_Resp()):
        envelopes = fetch_bitcoin_daily_prices(days=7, vs_currency="usd")

    assert len(envelopes) == 1
    assert envelopes[0].payload["prices"] == payload["prices"]