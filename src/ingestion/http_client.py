from __future__ import annotations

import json
from typing import Any, Dict
from urllib.parse import urlencode
from urllib.request import Request, urlopen


def _build_url(url: str, params: Dict[str, Any] | None = None) -> str:
    if not params:
        return url
    return f"{url}?{urlencode(params)}"


def fetch_json(
    url: str,
    params: Dict[str, Any] | None = None,
    headers: Dict[str, str] | None = None,
    timeout: int = 30,
) -> Dict[str, Any]:
    full_url = _build_url(url, params)
    req = Request(full_url, headers=headers or {})
    with urlopen(req, timeout=timeout) as response:
        raw = response.read().decode("utf-8")
    return json.loads(raw)


def fetch_json_any(
    url: str,
    params: Dict[str, Any] | None = None,
    headers: Dict[str, str] | None = None,
    timeout: int = 30,
) -> Any:
    full_url = _build_url(url, params)
    req = Request(full_url, headers=headers or {})
    with urlopen(req, timeout=timeout) as response:
        raw = response.read().decode("utf-8")
    return json.loads(raw)
