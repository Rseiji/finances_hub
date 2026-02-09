from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
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
