from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict


@dataclass(frozen=True)
class RawEnvelope:
    source: str
    endpoint: str
    request_params: Dict[str, Any]
    request_id: str
    fetched_at: str
    run_id: str
    payload: Dict[str, Any]

    @staticmethod
    def utc_now_iso() -> str:
        return datetime.now(timezone.utc).isoformat()

    def to_dict(self) -> Dict[str, Any]:
        return {
            "source": self.source,
            "endpoint": self.endpoint,
            "request_params": self.request_params,
            "request_id": self.request_id,
            "fetched_at": self.fetched_at,
            "run_id": self.run_id,
            "payload": self.payload,
        }
