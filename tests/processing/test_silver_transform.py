from __future__ import annotations

from pathlib import Path


def test_silver_transform_sql_contains_upserts() -> None:
    sql_dir = Path(__file__).resolve().parents[2] / "sql" / "silver"
    yfinance_sql = (sql_dir / "yfinance_prices.sql").read_text(encoding="utf-8")
    binance_sql = (sql_dir / "binance_prices.sql").read_text(encoding="utf-8")

    assert "INSERT INTO silver.yfinance_prices" in yfinance_sql
    assert "ON CONFLICT (symbol, currency, price_date)" in yfinance_sql
    assert "INSERT INTO silver.binance_prices" in binance_sql
    assert "ON CONFLICT (symbol, currency, price_date)" in binance_sql
