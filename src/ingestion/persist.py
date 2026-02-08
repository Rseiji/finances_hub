from __future__ import annotations

import os
from typing import Iterable, Literal, Optional

from ingestion.models import RawEnvelope
from storage.raw_file_store import write_envelopes
from storage.raw_postgres import insert_envelopes

Sink = Literal["file", "postgres", "both", "none"]


def inject_envelopes(
    envelopes: Iterable[RawEnvelope],
    category: str,
    sink: Optional[Sink] = None,
) -> Sink:
    target = sink or os.environ.get("FINANCES_HUB_SINK", "none")
    if target not in {"file", "postgres", "both", "none"}:
        raise ValueError(
            "FINANCES_HUB_SINK must be one of: file, postgres, both, none."
        )

    if target in {"file", "both"}:
        write_envelopes(category=category, envelopes=envelopes)

    if target in {"postgres", "both"}:
        insert_envelopes(envelopes)

    return target


persist_envelopes = inject_envelopes
