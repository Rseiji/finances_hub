from __future__ import annotations

from types import SimpleNamespace
from typing import Any

from ingestion.nubank_trading_notes import ingest_nubank_trading_notes


class _FakePdf:
    def __init__(self, page_tables: list[list[list[list[str | None]]]]) -> None:
        self.pages = [SimpleNamespace(extract_tables=lambda tables=tables: tables) for tables in page_tables]

    def __enter__(self) -> "_FakePdf":
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        return None


def test_ingest_nubank_trading_notes_appends_dataframe_to_bronze(monkeypatch: Any, tmp_path) -> None:
    pdf_file = tmp_path / "statement.pdf"
    pdf_file.write_bytes(b"%PDF-1.4")

    page_tables = [
        [[
            ["VISTA", "C", "FRACIONARIO", "PETR4", "", "10", "12,34", "123,40", "D"],
            ["VISTA", "V", "FRACIONARIO", "VALE3", "", "2", "68,50", "137,00", "C"],
        ]]
    ]

    captured: dict[str, Any] = {}

    def _append_dataframe_to_bronze(df, table_name: str, **kwargs: Any) -> int:
        captured["df"] = df
        captured["table_name"] = table_name
        captured["kwargs"] = kwargs
        return len(df)

    monkeypatch.setattr(
        "ingestion.nubank_trading_notes.pdfplumber.open",
        lambda _path: _FakePdf(page_tables),
    )
    monkeypatch.setattr(
        "ingestion.nubank_trading_notes.append_dataframe_to_bronze",
        _append_dataframe_to_bronze,
    )

    inserted = ingest_nubank_trading_notes(
        path=str(pdf_file),
        date="2025-11-05",
    )

    assert inserted == 2
    assert captured["table_name"] == "pdf_nubank_trade_events"
    assert captured["kwargs"]["source"] == "nubank_trading_notes_pdf"
    assert captured["kwargs"]["endpoint"] == "pdfplumber.extract_tables"
    assert captured["kwargs"]["request_params"] == {"pdf_path": str(pdf_file)}

    df = captured["df"]
    assert list(df["mercado"]) == ["VISTA", "VISTA"]
    assert list(df["cv"]) == ["C", "V"]
    assert list(df["espec_titulo"]) == ["PETR4", "VALE3"]
    assert list(df["quantidade"]) == ["10", "2"]
    assert list(df["dc"]) == ["D", "C"]
    assert list(df["date"]) == ["2025-11-05", "2025-11-05"]
    assert list(df["file_path"]) == [str(pdf_file), str(pdf_file)]
    assert list(df["file_name"]) == ["statement.pdf", "statement.pdf"]
