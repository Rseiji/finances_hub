from __future__ import annotations

from datetime import datetime
from typing import Any
from unittest.mock import patch

import pandas as pd

from ingestion.yfinance_stock import fetch_close_prices


def _history_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "Date": [datetime(2024, 1, 2), datetime(2024, 1, 3)],
            "Close": [10.5, 11.0],
        }
    ).set_index("Date")


def test_fetch_close_prices_returns_envelope() -> None:
    class _Ticker:
        def history(self, start: str, end: str) -> pd.DataFrame:  # type: ignore[override]
            return _history_frame()

    with patch("ingestion.yfinance_stock.yf.Ticker", return_value=_Ticker()):
        envelopes = fetch_close_prices("^GSPC", "2024-01-01", "2024-01-04")

    assert len(envelopes) == 1
    payload = envelopes[0].payload
    assert payload["symbol"] == "^GSPC"
    assert len(payload["rows"]) == 2
    assert payload["rows"][0]["value"] == 10.5
