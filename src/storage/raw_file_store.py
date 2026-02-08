from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable, Optional

from ingestion.models import RawEnvelope


def write_envelopes(
    category: str,
    envelopes: Iterable[RawEnvelope],
    base_dir: Optional[Path] = None,
) -> Path:
    root = base_dir or Path("data/raw")
    target_dir = root / category
    target_dir.mkdir(parents=True, exist_ok=True)
    file_path = target_dir / "raw.jsonl"

    with file_path.open("a", encoding="utf-8") as handle:
        for envelope in envelopes:
            handle.write(json.dumps(envelope.to_dict(), ensure_ascii=False))
            handle.write("\n")

    return file_path
