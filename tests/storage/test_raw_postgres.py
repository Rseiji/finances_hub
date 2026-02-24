from __future__ import annotations

from typing import Any

import pandas as pd
from psycopg.types.json import Json

from storage.raw_postgres import append_dataframe_to_bronze, overwrite_dataframe_in_bronze


class _FakeCursor:
    def __init__(self) -> None:
        self.query: Any | None = None
        self.rows: list[tuple[Any, ...]] | None = None
        self.executed_queries: list[Any] = []

    def execute(self, query: Any, params: Any | None = None) -> None:
        self.executed_queries.append((query, params))

    def executemany(self, query: Any, rows: list[tuple[Any, ...]]) -> None:
        self.query = query
        self.rows = rows

    def __enter__(self) -> "_FakeCursor":
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        return None


class _FakeConn:
    def __init__(self) -> None:
        self.cursor_obj = _FakeCursor()
        self.committed = False

    def cursor(self) -> _FakeCursor:
        return self.cursor_obj

    def commit(self) -> None:
        self.committed = True

    def __enter__(self) -> "_FakeConn":
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        return None


def test_append_dataframe_to_bronze_appends_rows_with_control_columns(
    monkeypatch: Any,
) -> None:
    fake_conn = _FakeConn()

    def _connect(*args: Any, **kwargs: Any) -> _FakeConn:
        return fake_conn

    monkeypatch.setenv("FINANCES_HUB_PG_DSN", "postgresql://local/test")
    monkeypatch.setattr("storage.raw_postgres.psycopg.connect", _connect)

    df = pd.DataFrame(
        [
            {"asset": "PETR4", "close": 37.52},
            {"asset": "VALE3", "close": 61.13},
        ]
    )

    inserted = append_dataframe_to_bronze(
        df,
        "prices_daily",
        source="yfinance",
        endpoint="yfinance.Ticker.history",
        request_params={"symbol": "PETR4.SA"},
        uid="11111111-1111-1111-1111-111111111111",
        fetched_at="2026-02-23T12:00:00+00:00",
    )

    assert inserted == 2
    assert fake_conn.committed is True
    assert fake_conn.cursor_obj.query is not None
    assert fake_conn.cursor_obj.rows is not None

    first_row = fake_conn.cursor_obj.rows[0]
    second_row = fake_conn.cursor_obj.rows[1]

    assert first_row[0:2] == ("PETR4", 37.52)
    assert second_row[0:2] == ("VALE3", 61.13)
    assert first_row[2] == "yfinance"
    assert first_row[3] == "yfinance.Ticker.history"
    assert isinstance(first_row[4], Json)
    assert first_row[5] == "11111111-1111-1111-1111-111111111111"
    assert first_row[6] == "2026-02-23T12:00:00+00:00"


def test_append_dataframe_to_bronze_empty_dataframe_returns_zero(
    monkeypatch: Any,
) -> None:
    called = {"connect": False}

    def _connect(*args: Any, **kwargs: Any) -> _FakeConn:
        called["connect"] = True
        return _FakeConn()

    monkeypatch.setenv("FINANCES_HUB_PG_DSN", "postgresql://local/test")
    monkeypatch.setattr("storage.raw_postgres.psycopg.connect", _connect)

    inserted = append_dataframe_to_bronze(
        pd.DataFrame(),
        "prices_daily",
        source="yfinance",
        endpoint="yfinance.Ticker.history",
        request_params={"symbol": "PETR4.SA"},
    )

    assert inserted == 0
    assert called["connect"] is False


def test_overwrite_dataframe_in_bronze_replaces_rows(
    monkeypatch: Any,
) -> None:
    fake_conn = _FakeConn()

    def _connect(*args: Any, **kwargs: Any) -> _FakeConn:
        return fake_conn

    monkeypatch.setenv("FINANCES_HUB_PG_DSN", "postgresql://local/test")
    monkeypatch.setattr("storage.raw_postgres.psycopg.connect", _connect)

    df = pd.DataFrame(
        [
            {
                "date": "2024-05-02",
                "taxa_liquidacao": 5.93,
                "emolumento": 1.18,
                "transf_ativos": 0.0,
            },
            {
                "date": "2024-06-05",
                "taxa_liquidacao": 1.47,
                "emolumento": 0.29,
                "transf_ativos": 0.0,
            },
        ]
    )

    inserted = overwrite_dataframe_in_bronze(df, "nubank_trade_taxes")

    assert inserted == 2
    assert fake_conn.committed is True
    assert len(fake_conn.cursor_obj.executed_queries) == 1
    assert fake_conn.cursor_obj.query is not None
    assert fake_conn.cursor_obj.rows is not None
    assert fake_conn.cursor_obj.rows[0] == ("2024-05-02", 5.93, 1.18, 0.0)
