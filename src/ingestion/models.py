from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any


@dataclass(frozen=True)
class RawEnvelope:
    source: str
    endpoint: str
    request_params: dict[str, Any]
    asset: str
    currency: str
    uid: str
    fetched_at: str
    payload: dict[str, Any]

    @staticmethod
    def utc_now_iso() -> str:
        return datetime.now(timezone.utc).isoformat()

    def to_dict(self) -> dict[str, Any]:
        return {
            "source": self.source,
            "endpoint": self.endpoint,
            "request_params": self.request_params,
            "asset": self.asset,
            "currency": self.currency,
            "uid": self.uid,
            "fetched_at": self.fetched_at,
            "payload": self.payload,
        }


@dataclass(frozen=True)
class PdfTradeEvent:
    uid: str
    source: str
    request_params: dict[str, Any]
    fetched_at: str
    mercado: str
    cv: str
    tipo_mercado: str
    espec_titulo: str
    observacao: str
    quantidade: int
    preco: Decimal
    valor: Decimal
    dc: str
    taxa_liquidacao: Decimal
    emolumentos: Decimal
    taxa_transf_ativos: Decimal
    file_path: str
    file_name: str
    statement_date: date | None

    def __post_init__(self) -> None:
        if self.dc not in {"C", "D"}:
            raise ValueError("dc must be 'C' or 'D'")
        if self.quantidade < 0:
            raise ValueError("quantidade must be >= 0")
