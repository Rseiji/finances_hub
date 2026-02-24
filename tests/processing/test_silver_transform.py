from __future__ import annotations

from pathlib import Path


def test_silver_transform_sql_contains_upserts() -> None:
    sql_dir = Path(__file__).resolve().parents[2] / "sql" / "silver"
    yfinance_sql = (sql_dir / "yfinance_prices.sql").read_text(encoding="utf-8")
    binance_sql = (sql_dir / "binance_prices.sql").read_text(encoding="utf-8")
    nubank_sql = (sql_dir / "nubank_trade_events.sql").read_text(encoding="utf-8")

    assert "INSERT INTO silver.yfinance_prices" in yfinance_sql
    assert "ON CONFLICT (symbol, currency, price_date)" in yfinance_sql
    assert "INSERT INTO silver.binance_prices" in binance_sql
    assert "ON CONFLICT (symbol, currency, price_date)" in binance_sql
    assert "INSERT INTO silver.nubank_trade_events" in nubank_sql
    assert "FROM bronze.pdf_nubank_trade_events" in nubank_sql
    assert "replace(replace(trim(b.valor), '.', ''), ',', '.')" in nubank_sql
    assert "WHEN position(',' IN b.preco) > 0 THEN replace(replace(trim(b.preco), '.', ''), ',', '.')" in nubank_sql
    assert "WHEN trim(b.quantidade) ~ '^[0-9]{1,3}(\\.[0-9]{3})+$' THEN replace(trim(b.quantidade), '.', '')" in nubank_sql
    assert "([A-Za-z]{4,6}[0-9]{1,2}F?)" in nubank_sql
