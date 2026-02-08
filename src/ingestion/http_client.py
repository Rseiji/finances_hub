from __future__ import annotations

import json
from typing import Any, Dict, Optional
from urllib.parse import urlencode
from urllib.request import Request, urlopen


def _build_url(url: str, params: Optional[Dict[str, Any]] = None) -> str:
    if not params:
        return url
    return f"{url}?{urlencode(params)}"


def fetch_json(
    url: str,
    params: Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, str]] = None,
    timeout: int = 30,
) -> Dict[str, Any]:
    full_url = _build_url(url, params)
    req = Request(full_url, headers=headers or {})
    with urlopen(req, timeout=timeout) as response:
        raw = response.read().decode("utf-8")
    return json.loads(raw)
