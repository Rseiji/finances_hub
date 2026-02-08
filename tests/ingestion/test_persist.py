from __future__ import annotations

from typing import List

import pytest

from ingestion.models import RawEnvelope
from ingestion.persist import inject_envelopes


def _sample_envelopes() -> List[RawEnvelope]:
    return [
        RawEnvelope(
            source="unit",
            endpoint="unit.endpoint",
            request_params={"foo": "bar"},
            request_id="11111111-1111-1111-1111-111111111111",
            fetched_at="2024-01-01T00:00:00+00:00",
            run_id="22222222-2222-2222-2222-222222222222",
            payload={"value": 1},
        )
    ]


def test_inject_envelopes_none_sink_does_nothing(monkeypatch: pytest.MonkeyPatch) -> None:
    called = {"file": False, "postgres": False}

    def _write_envelopes(*, category: str, envelopes: List[RawEnvelope]) -> None:
        called["file"] = True

    def _insert_envelopes(envelopes: List[RawEnvelope]) -> None:
        called["postgres"] = True

    monkeypatch.setenv("FINANCES_HUB_SINK", "none")
    monkeypatch.setattr("ingestion.persist.write_envelopes", _write_envelopes)
    monkeypatch.setattr("ingestion.persist.insert_envelopes", _insert_envelopes)

    result = inject_envelopes(_sample_envelopes(), category="coingecko")

    assert result == "none"
    assert called == {"file": False, "postgres": False}


def test_inject_envelopes_file_sink(monkeypatch: pytest.MonkeyPatch) -> None:
    captured = {}

    def _write_envelopes(*, category: str, envelopes: List[RawEnvelope]) -> None:
        captured["category"] = category
        captured["count"] = len(envelopes)

    monkeypatch.setenv("FINANCES_HUB_SINK", "file")
    monkeypatch.setattr("ingestion.persist.write_envelopes", _write_envelopes)

    result = inject_envelopes(_sample_envelopes(), category="coingecko")

    assert result == "file"
    assert captured == {"category": "coingecko", "count": 1}


def test_inject_envelopes_postgres_sink(monkeypatch: pytest.MonkeyPatch) -> None:
    captured = {}

    def _insert_envelopes(envelopes: List[RawEnvelope]) -> None:
        captured["count"] = len(envelopes)

    monkeypatch.setenv("FINANCES_HUB_SINK", "postgres")
    monkeypatch.setattr("ingestion.persist.insert_envelopes", _insert_envelopes)

    result = inject_envelopes(_sample_envelopes(), category="coingecko")

    assert result == "postgres"
    assert captured == {"count": 1}


def test_inject_envelopes_both_sink(monkeypatch: pytest.MonkeyPatch) -> None:
    called = {"file": False, "postgres": False}

    def _write_envelopes(*, category: str, envelopes: List[RawEnvelope]) -> None:
        called["file"] = True

    def _insert_envelopes(envelopes: List[RawEnvelope]) -> None:
        called["postgres"] = True

    monkeypatch.setenv("FINANCES_HUB_SINK", "both")
    monkeypatch.setattr("ingestion.persist.write_envelopes", _write_envelopes)
    monkeypatch.setattr("ingestion.persist.insert_envelopes", _insert_envelopes)

    result = inject_envelopes(_sample_envelopes(), category="coingecko")

    assert result == "both"
    assert called == {"file": True, "postgres": True}


def test_inject_envelopes_sink_argument_overrides_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured = {}

    def _write_envelopes(*, category: str, envelopes: List[RawEnvelope]) -> None:
        captured["category"] = category

    monkeypatch.setenv("FINANCES_HUB_SINK", "postgres")
    monkeypatch.setattr("ingestion.persist.write_envelopes", _write_envelopes)

    result = inject_envelopes(_sample_envelopes(), category="coingecko", sink="file")

    assert result == "file"
    assert captured == {"category": "coingecko"}


def test_inject_envelopes_invalid_sink_raises() -> None:
    with pytest.raises(ValueError):
        inject_envelopes(_sample_envelopes(), category="coingecko", sink="nope")
