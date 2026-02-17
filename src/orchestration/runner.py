from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
import re

from ingestion.binance import fetch_klines_daily
from ingestion.persist import Sink, inject_envelopes
from ingestion.yfinance_stock import fetch_close_prices
from processing.sql_tests import run_sql_tests


@dataclass
class OrchestrationConfig:
    sink: Sink | None = None
    dsn: str | None = None


def run_all(
    jobs: dict[str, callable] | None = None,
    config: OrchestrationConfig | None = None,
    run_tests: bool = True,
) -> dict[str, int]:
    config = config or OrchestrationConfig()
    end_date = date.today().isoformat()
    start_date = "2020-01-01"
    job_map = jobs or _load_jobs_from_yaml(_jobs_path(), start_date, end_date)

    results = {name: job(config) for name, job in job_map.items()}
    if run_tests:
        run_sql_tests(config.dsn, _tests_dir())
    return results


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


def _tests_dir() -> Path:
    return Path(__file__).resolve().parents[2] / "sql" / "bronze" / "tests"


def _jobs_path() -> Path:
    return Path(__file__).resolve().parents[2] / "config" / "ingestion_jobs.yaml"


def _load_jobs_from_yaml(path: Path, start_date: str, end_date: str) -> dict[str, callable]:
    if not path.exists():
        raise FileNotFoundError(f"Missing ingestion jobs file: {path}")
    jobs = _parse_jobs_yaml(path.read_text(encoding="utf-8"))
    return _build_job_map(jobs, start_date, end_date)


def _build_job_map(
    jobs: list[dict[str, str]],
    start_date: str,
    end_date: str,
) -> dict[str, callable]:
    job_map: dict[str, callable] = {}
    for job in jobs:
        name = job.get("name")
        kind = job.get("type")
        if not name or not kind:
            raise ValueError("Each job requires name and type")
        if kind == "binance":
            job_map[name] = make_binance_job(
                job["symbol"],
                start_date=job.get("start_date") or start_date,
                end_date=job.get("end_date") or end_date,
                quote_currency=job.get("quote_currency", "usdt"),
                job_name=name,
                asset=job.get("asset"),
            )
        elif kind == "yfinance":
            job_map[name] = make_yfinance_job(
                job["symbol"],
                job_name=name,
                start_date=job.get("start_date") or start_date,
                end_date=job.get("end_date") or end_date,
                currency=job.get("currency"),
                asset=job.get("asset"),
            )
        else:
            raise ValueError(f"Unknown job type: {kind}")
    return job_map


def _parse_jobs_yaml(text: str) -> list[dict[str, str]]:
    jobs: list[dict[str, str]] = []
    current: dict[str, str] | None = None
    for raw_line in text.splitlines():
        line = raw_line.split("#", 1)[0].rstrip()
        if not line.strip():
            continue
        if line.strip() == "jobs:":
            continue
        if line.lstrip().startswith("-"):
            current = {}
            jobs.append(current)
            line = line.lstrip()[1:].strip()
            if line:
                key, value = _parse_kv(line)
                current[key] = _substitute(value)
            continue
        if current is None:
            raise ValueError("Jobs YAML must start with a list item")
        key, value = _parse_kv(line.strip())
        current[key] = _substitute(value)
    return jobs


def _parse_kv(line: str) -> tuple[str, str]:
    if ":" not in line:
        raise ValueError(f"Invalid line in jobs YAML: {line}")
    key, value = line.split(":", 1)
    return key.strip(), value.strip().strip("'\"")


def _substitute(value: str) -> str:
    pattern = re.compile(r"\$\{([^}]+)\}")

    def _replace(match: re.Match[str]) -> str:
        return os.environ.get(match.group(1), "")

    return pattern.sub(_replace, value)
