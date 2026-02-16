from __future__ import annotations

from pathlib import Path


def test_gold_transform_sql_contains_upserts() -> None:
    sql_dir = Path(__file__).resolve().parents[2] / "sql" / "gold"
    crypto_sql = (sql_dir / "crypto_prices_daily.sql").read_text(encoding="utf-8")
    fx_sql = (sql_dir / "fiat_fx_rates_daily.sql").read_text(encoding="utf-8")
    indices_sql = (sql_dir / "indices_daily.sql").read_text(encoding="utf-8")
    etfs_sql = (sql_dir / "etfs_daily.sql").read_text(encoding="utf-8")
    br_sql = (sql_dir / "br_stocks_daily.sql").read_text(encoding="utf-8")

    assert "INSERT INTO gold.crypto_prices_daily" in crypto_sql
    assert "FROM silver.binance_prices" in crypto_sql
    assert "ON CONFLICT (asset, currency, price_date)" in crypto_sql
    assert "INSERT INTO gold.fiat_fx_rates_daily" in fx_sql
    assert "ON CONFLICT (pair_code, rate_date)" in fx_sql
    assert "INSERT INTO gold.indices_daily" in indices_sql
    assert "ON CONFLICT (index_code, price_date)" in indices_sql
    assert "INSERT INTO gold.etfs_daily" in etfs_sql
    assert "ON CONFLICT (symbol, price_date)" in etfs_sql
    assert "INSERT INTO gold.br_stocks_daily" in br_sql
