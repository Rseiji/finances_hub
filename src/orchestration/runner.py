from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date, timedelta

from ingestion.coingecko import fetch_market_chart
from ingestion.persist import Sink, inject_envelopes
from ingestion.yfinance_stock import fetch_close_prices


@dataclass
class OrchestrationConfig:
    sink: Sink | None = None
    dsn: str | None = None


def run_all(
    jobs: dict[str, callable] | None = None,
    config: OrchestrationConfig | None = None,
) -> dict[str, int]:
    config = config or OrchestrationConfig()
    end_date = date.today().isoformat()
    start_date = (date.today() - timedelta(days=7)).isoformat()
    job_map = jobs or {
        "coingecko_bitcoin_daily": make_coingecko_job("bitcoin", days="7"),
        "yfinance_sp500": make_yfinance_job(
            "^GSPC",
            job_name="yfinance_sp500",
            start_date=start_date,
            end_date=end_date,
            currency="usd",
        ),
        "yfinance_ibov": make_yfinance_job(
            "^BVSP",
            job_name="yfinance_ibov",
            start_date=start_date,
            end_date=end_date,
            currency="brl",
        ),
    }

    return {name: job(config) for name, job in job_map.items()}


def make_coingecko_job(
    coin_id: str,
    vs_currency: str = "usd",
    job_name: str | None = None,
    days: str | None = None,
    interval: str | None = None,
    asset: str | None = None,
) -> callable:
    def _runner(config: OrchestrationConfig | None = None) -> int:
        merged_kwargs = {"coin_id": coin_id, "vs_currency": vs_currency}
        if days is not None:
            merged_kwargs["days"] = days
        if interval is not None:
            merged_kwargs["interval"] = interval
        if asset is not None:
            merged_kwargs["asset"] = asset
        return run_asset_ingestion(
            config=config,
            job_name=job_name or f"coingecko_{coin_id}",
            category="coingecko",
            fetcher=fetch_market_chart,
            fetch_kwargs=merged_kwargs,
        )

    return _runner


def make_yfinance_job(
    symbol: str,
    job_name: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    currency: str | None = None,
    asset: str | None = None,
) -> callable:
    def _runner(config: OrchestrationConfig | None = None) -> int:
        merged_kwargs = {"symbol": symbol}
        if start_date is not None:
            merged_kwargs["start_date"] = start_date
        if end_date is not None:
            merged_kwargs["end_date"] = end_date
        if currency is not None:
            merged_kwargs["currency"] = currency
        if asset is not None:
            merged_kwargs["asset"] = asset
        return run_asset_ingestion(
            config=config,
            job_name=job_name or f"yfinance_{symbol}",
            category="yfinance",
            fetcher=fetch_close_prices,
            fetch_kwargs=merged_kwargs,
        )

    return _runner


def run_asset_ingestion(
    *,
    config: OrchestrationConfig | None = None,
    job_name: str,
    category: str,
    fetcher,
    fetch_kwargs: dict[str, str],
) -> int:
    config = config or OrchestrationConfig()
    _apply_dsn(config.dsn)
    envelopes = fetcher(**fetch_kwargs)
    inject_envelopes(envelopes, category=category, sink=config.sink)
    return len(envelopes)


def _apply_dsn(dsn: str | None) -> None:
    if dsn:
        os.environ["FINANCES_HUB_PG_DSN"] = dsn
