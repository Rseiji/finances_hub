from __future__ import annotations

import uuid
from typing import Any, Dict, List

import pandas as pd
import yfinance as yf

from ingestion.models import RawEnvelope


def _to_rows(df: pd.DataFrame) -> List[Dict[str, Any]]:
    rows = df["Close"].reset_index()
    rows.rename(columns={"Close": "value", "Date": "date"}, inplace=True)
    rows["date"] = rows["date"].dt.date.astype(str)
    return rows[["date", "value"]].to_dict(orient="records")


def fetch_close_prices(
    symbol: str,
    start_date: str,
    end_date: str,
    run_id: str | None = None,
) -> List[RawEnvelope]:
    ticker = yf.Ticker(symbol)
    hist = ticker.history(start=start_date, end=end_date)

    if hist.empty:
        return []

    payload = {
        "symbol": symbol,
        "start_date": start_date,
        "end_date": end_date,
        "rows": _to_rows(hist),
    }

    envelope = RawEnvelope(
        source="yfinance",
        endpoint="yfinance.Ticker.history",
        request_params={
            "symbol": symbol,
            "start_date": start_date,
            "end_date": end_date,
        },
        request_id=str(uuid.uuid4()),
        fetched_at=RawEnvelope.utc_now_iso(),
        run_id=run_id or str(uuid.uuid4()),
        payload=payload,
    )
    return [envelope]


def fetch_sp500_close_prices(
    start_date: str,
    end_date: str,
    run_id: str | None = None,
) -> List[RawEnvelope]:
    return fetch_close_prices("^GSPC", start_date, end_date, run_id=run_id)


def fetch_ibov_close_prices(
    start_date: str,
    end_date: str,
    run_id: str | None = None,
) -> List[RawEnvelope]:
    return fetch_close_prices("^BVSP", start_date, end_date, run_id=run_id)
