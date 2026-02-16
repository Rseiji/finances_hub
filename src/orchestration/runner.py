from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date, timedelta

from ingestion.binance import fetch_klines_daily
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
    history_start_date = "2020-01-01"
    yfinance_start_date = history_start_date
    job_map = jobs or {
        "binance_btcusdt_daily": make_binance_job(
            "BTCUSDT",
            start_date=history_start_date,
            end_date=end_date,
            quote_currency="usdt",
            asset="BTC",
        ),
        "binance_ethusdt_daily": make_binance_job(
            "ETHUSDT",
            start_date=history_start_date,
            end_date=end_date,
            quote_currency="usdt",
            asset="ETH",
        ),
        "binance_solusdt_daily": make_binance_job(
            "SOLUSDT",
            start_date=history_start_date,
            end_date=end_date,
            quote_currency="usdt",
            asset="SOL",
        ),
        "yfinance_sp500": make_yfinance_job(
            "^GSPC",
            job_name="yfinance_sp500",
            start_date=yfinance_start_date,
            end_date=end_date,
            currency="usd",
        ),
        "yfinance_ibov": make_yfinance_job(
            "^BVSP",
            job_name="yfinance_ibov",
            start_date=yfinance_start_date,
            end_date=end_date,
            currency="brl",
        ),
    }

    return {name: job(config) for name, job in job_map.items()}


def make_binance_job(
    symbol: str,
    start_date: str,
    end_date: str | None = None,
    quote_currency: str = "usdt",
    job_name: str | None = None,
    asset: str | None = None,
) -> callable:
    def _runner(config: OrchestrationConfig | None = None) -> int:
        merged_kwargs = {
            "symbol": symbol,
            "start_date": start_date,
            "quote_currency": quote_currency,
        }
        if end_date is not None:
            merged_kwargs["end_date"] = end_date
        if asset is not None:
            merged_kwargs["asset"] = asset
        return run_asset_ingestion(
            config=config,
            job_name=job_name or f"binance_{symbol}",
            category="binance",
            fetcher=fetch_klines_daily,
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
