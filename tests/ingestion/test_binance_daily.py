from __future__ import annotations

from typing import Any, Dict, List
from unittest.mock import patch

from ingestion.binance import fetch_klines_daily


def test_fetch_klines_daily_builds_envelopes() -> None:
    payload: List[List[Any]] = [
        [
            1704067200000,
            "1",
            "2",
            "0.5",
            "1.5",
            "100",
            1704153599999,
        ]
    ]

    def _fetch_json(url: str, params: Dict[str, Any] | None = None, **_kwargs) -> List[List[Any]]:
        assert params is not None
        assert params["symbol"] == "BTCUSDT"
        return payload

    with patch("ingestion.binance.fetch_json_any", side_effect=_fetch_json):
        envelopes = fetch_klines_daily(
            symbol="BTCUSDT",
            start_date="2024-01-01",
            end_date="2024-01-01",
            quote_currency="usdt",
            asset="BTC",
        )

    assert len(envelopes) == 1
    assert envelopes[0].currency == "usdt"
    assert envelopes[0].payload["rows"][0]["close"] == "1.5"
