from __future__ import annotations

from typing import Any, List

import pytest

from ingestion.models import RawEnvelope
from orchestration import runner
from orchestration.runner import OrchestrationConfig


def _sample_envelope() -> RawEnvelope:
    return RawEnvelope(
        source="unit",
        endpoint="unit.endpoint",
        request_params={"foo": "bar"},
        asset="BTC",
        currency="usd",
        uid="11111111-1111-1111-1111-111111111111",
        fetched_at="2024-01-01T00:00:00+00:00",
        payload={"value": 1},
    )


def test_run_asset_ingestion_calls_fetch_and_inject(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}

    def _fetch_klines_daily(*, symbol: str, start_date: str, end_date: str) -> List[RawEnvelope]:
        captured["symbol"] = symbol
        captured["start_date"] = start_date
        captured["end_date"] = end_date
        return [_sample_envelope()]

    def _inject_envelopes(envelopes: List[RawEnvelope], category: str, sink: Any = None) -> None:
        captured["category"] = category
        captured["count"] = len(envelopes)

    monkeypatch.setattr(runner, "inject_envelopes", _inject_envelopes)

    config = OrchestrationConfig(sink="none")

    result = runner.run_asset_ingestion(
        config=config,
        job_name="binance_btcusdt",
        category="binance",
        fetcher=_fetch_klines_daily,
        fetch_kwargs={
            "symbol": "BTCUSDT",
            "start_date": "2020-01-01",
            "end_date": "2020-01-02",
        },
    )

    assert result == 1
    assert captured == {
        "symbol": "BTCUSDT",
        "start_date": "2020-01-01",
        "end_date": "2020-01-02",
        "category": "binance",
        "count": 1,
    }


def test_run_asset_ingestion_accepts_stock_fetcher(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}

    def _fetch_close_prices(symbol: str, start_date: str, end_date: str) -> List[RawEnvelope]:
        captured["symbol"] = symbol
        captured["start_date"] = start_date
        captured["end_date"] = end_date
        return [_sample_envelope()]

    monkeypatch.setattr(runner, "fetch_close_prices", _fetch_close_prices)
    monkeypatch.setattr(runner, "inject_envelopes", lambda *args, **kwargs: None)

    config = OrchestrationConfig(sink="none")

    result = runner.run_asset_ingestion(
        config=config,
        job_name="yfinance_^GSPC",
        category="yfinance",
        fetcher=runner.fetch_close_prices,
        fetch_kwargs={"symbol": "^GSPC", "start_date": "2023-12-31", "end_date": "2024-01-10"},
    )

    assert result == 1
    assert captured == {
        "symbol": "^GSPC",
        "start_date": "2023-12-31",
        "end_date": "2024-01-10",
    }


def test_factory_jobs_call_runners(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: list[dict[str, Any]] = []

    def _run_asset_ingestion(*, fetch_kwargs: dict[str, str], **kwargs) -> int:
        captured.append(fetch_kwargs)
        return 5

    monkeypatch.setattr(runner, "run_asset_ingestion", _run_asset_ingestion)
    config = OrchestrationConfig()

    binance_job = runner.make_binance_job(
        "BTCUSDT",
        start_date="2020-01-01",
        end_date="2020-01-02",
        quote_currency="usdt",
        asset="BTC",
    )
    stock_job = runner.make_yfinance_job(
        "^BVSP",
        job_name="yfinance_ibov",
        start_date="2023-12-31",
        end_date="2024-01-10",
        currency="brl",
        asset="IBOV",
    )

    assert binance_job(config) == 5
    assert stock_job(config) == 5
    assert captured == [
        {
            "symbol": "BTCUSDT",
            "start_date": "2020-01-01",
            "end_date": "2020-01-02",
            "quote_currency": "usdt",
            "asset": "BTC",
        },
        {
            "symbol": "^BVSP",
            "start_date": "2023-12-31",
            "end_date": "2024-01-10",
            "currency": "brl",
            "asset": "IBOV",
        },
    ]


def test_run_all_custom_jobs() -> None:
    config = OrchestrationConfig()

    def _job_one(_config: OrchestrationConfig) -> int:
        return 1

    def _job_two(_config: OrchestrationConfig) -> int:
        return 2

    results = runner.run_all(jobs={"one": _job_one, "two": _job_two}, config=config)

    assert results == {"one": 1, "two": 2}
