from __future__ import annotations

from typing import Any

import pandas as pd

from trading_notes_ingestion import run


def test_main_confirms_and_ingests(monkeypatch: Any, capsys) -> None:
    parsed = pd.DataFrame(
        [
            {
                "mercado": "VISTA",
                "cv": "C",
                "tipo_mercado": "FRACIONARIO",
                "espec_titulo": "PETR4",
                "observacao": "",
                "quantidade": "10",
                "preco": "12,34",
                "valor": "123,40",
                "dc": "D",
                "taxa_liquidacao": "0",
                "emolumentos": "0",
                "taxa_transf_ativos": "0",
                "file_path": "/tmp/statement.pdf",
                "file_name": "statement.pdf",
                "statement_date": None,
            }
        ]
    )

    calls: dict[str, Any] = {}

    monkeypatch.setattr(
        "trading_notes_ingestion.run.build_parser",
        lambda: type("P", (), {"parse_args": lambda self: type("A", (), {"pdf_path": "/tmp/statement.pdf", "bank": "nubank"})()})(),
    )
    monkeypatch.setattr("trading_notes_ingestion.run.parse_nubank_trade_notes", lambda _path: parsed)
    monkeypatch.setattr("builtins.input", lambda _prompt: "yes")

    def _ingest(path: str, *, parsed_df: pd.DataFrame | None = None) -> int:
        calls["path"] = path
        calls["parsed_df"] = parsed_df
        return 1

    monkeypatch.setattr("trading_notes_ingestion.run.ingest_nubank_trading_notes", _ingest)

    code = run.main()

    out = capsys.readouterr().out
    assert code == 0
    assert "Parsed data preview:" in out
    assert "Inserted 1 Nubank trading note rows into bronze layer." in out
    assert calls["path"] == "/tmp/statement.pdf"
    assert calls["parsed_df"].equals(parsed)


def test_main_cancels_when_not_confirmed(monkeypatch: Any, capsys) -> None:
    parsed = pd.DataFrame(
        [
            {
                "mercado": "VISTA",
                "cv": "C",
                "tipo_mercado": "FRACIONARIO",
                "espec_titulo": "PETR4",
                "observacao": "",
                "quantidade": "10",
                "preco": "12,34",
                "valor": "123,40",
                "dc": "D",
                "taxa_liquidacao": "0",
                "emolumentos": "0",
                "taxa_transf_ativos": "0",
                "file_path": "/tmp/statement.pdf",
                "file_name": "statement.pdf",
                "statement_date": None,
            }
        ]
    )

    monkeypatch.setattr(
        "trading_notes_ingestion.run.build_parser",
        lambda: type("P", (), {"parse_args": lambda self: type("A", (), {"pdf_path": "/tmp/statement.pdf", "bank": "nubank"})()})(),
    )
    monkeypatch.setattr("trading_notes_ingestion.run.parse_nubank_trade_notes", lambda _path: parsed)
    monkeypatch.setattr("builtins.input", lambda _prompt: "n")

    called = {"ingest": False}

    def _ingest(path: str, *, parsed_df: pd.DataFrame | None = None) -> int:
        called["ingest"] = True
        return 0

    monkeypatch.setattr("trading_notes_ingestion.run.ingest_nubank_trading_notes", _ingest)

    code = run.main()

    out = capsys.readouterr().out
    assert code == 0
    assert "Ingestion cancelled by user." in out
    assert called["ingest"] is False
