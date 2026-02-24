import os
import re

import pandas as pd
import pdfplumber

from storage.raw_postgres import append_dataframe_to_bronze

TABLE_COLUMNS = [
    "mercado",
    "cv",
    "tipo_mercado",
    "espec_titulo",
    "observacao",
    "quantidade",
    "preco",
    "valor",
    "dc"
]

BRONZE_COLUMNS = [
    "mercado",
    "cv",
    "tipo_mercado",
    "espec_titulo",
    "observacao",
    "quantidade",
    "preco",
    "valor",
    "dc",
    "file_path",
    "file_name",
    "date",
]


def _is_header_row(row: list[object | None]) -> bool:
    if len(row) < 2:
        return False
    first = str(row[0]).strip().lower()
    second = str(row[1]).strip().lower()
    return first == "mercado" and second in {"c/v", "cv"}


def _normalize_row(row: list[object | None]) -> list[str]:
    padded = list(row[: len(TABLE_COLUMNS)])
    if len(padded) < len(TABLE_COLUMNS):
        padded.extend([None] * (len(TABLE_COLUMNS) - len(padded)))
    return ["" if value is None else str(value).strip() for value in padded]


TRADE_LINE_PATTERN = re.compile(
    r"^(BOVESPA)\s+([CV])\s+(.*?)\s+([0-9\.]+)\s+([0-9\.,]+)\s+([0-9\.,]+)\s+([DC])$",
    re.IGNORECASE,
)

TRADE_LINE_NO_TITLE_PATTERN = re.compile(
    r"^(BOVESPA)\s+([CV])\s+([0-9\.]+)\s+([0-9\.,]+)\s+([0-9\.,]+)\s+([DC])$",
    re.IGNORECASE,
)

TICKER_HINT_PATTERN = re.compile(r"\b[A-Za-z]{4,6}[0-9]{1,2}F?\b")


def _parse_trade_line(line: str) -> list[str] | None:
    compact = re.sub(r"\s+", " ", line).strip()
    match = TRADE_LINE_PATTERN.match(compact)
    if not match:
        return None

    mercado, cv, espec_titulo, quantidade, preco, valor, dc = match.groups()
    return [
        mercado.upper(),
        cv.upper(),
        "",
        espec_titulo.strip(),
        "",
        quantidade,
        preco,
        valor,
        dc.upper(),
    ]


def _looks_like_market_line(line: str) -> bool:
    return re.sub(r"\s+", " ", line).strip().upper().startswith("BOVESPA ")


def _infer_title_from_context(lines: list[str], line_index: int) -> str:
    anchor: str | None = None
    for back in range(line_index - 1, max(-1, line_index - 4), -1):
        candidate = re.sub(r"\s+", " ", lines[back]).strip()
        if not candidate:
            continue
        if _looks_like_market_line(candidate):
            continue
        if TICKER_HINT_PATTERN.search(candidate):
            anchor = candidate
            break

    if anchor is None:
        return ""

    title_parts: list[str] = [anchor]

    if line_index + 1 < len(lines):
        candidate = re.sub(r"\s+", " ", lines[line_index + 1]).strip()
        if (
            candidate
            and not _looks_like_market_line(candidate)
            and not TICKER_HINT_PATTERN.search(candidate)
            and re.fullmatch(r"[A-Za-z0-9 ]{1,20}", candidate)
        ):
            title_parts.append(candidate)

    return " ".join(title_parts).strip()


def _parse_trade_line_with_context(lines: list[str], line_index: int) -> list[str] | None:
    line = re.sub(r"\s+", " ", lines[line_index]).strip()
    parsed = _parse_trade_line(line)
    if parsed is not None:
        return parsed

    match = TRADE_LINE_NO_TITLE_PATTERN.match(line)
    if not match:
        return None

    mercado, cv, quantidade, preco, valor, dc = match.groups()
    inferred_title = _infer_title_from_context(lines, line_index)

    if not inferred_title:
        return None

    return [
        mercado.upper(),
        cv.upper(),
        "",
        inferred_title,
        "",
        quantidade,
        preco,
        valor,
        dc.upper(),
    ]


def _trade_row_key(row: list[str]) -> tuple[str, str, str, str, str, str] | None:
    mercado, cv, _, _, _, quantidade, preco, valor, dc = row
    if not all([mercado, cv, quantidade, preco, valor, dc]):
        return None
    return (mercado.upper(), cv.upper(), quantidade, preco, valor, dc.upper())


def _pick_better_row(current: list[str], candidate: list[str]) -> list[str]:
    current_score = len(current[2]) + len(current[3]) + len(current[4])
    candidate_score = len(candidate[2]) + len(candidate[3]) + len(candidate[4])
    return candidate if candidate_score > current_score else current


def _extract_rows_from_text(page: object) -> list[list[str]]:
    extractor = getattr(page, "extract_text", None)
    if not callable(extractor):
        return []

    text = extractor() or ""
    lines = [line for line in text.splitlines() if line.strip()]
    rows: list[list[str]] = []
    for index in range(len(lines)):
        parsed = _parse_trade_line_with_context(lines, index)
        if parsed is not None:
            rows.append(parsed)
    return rows


def _merge_rows(text_rows: list[list[str]], table_rows: list[list[object | None]]) -> list[list[object | None]]:
    merged: dict[tuple[str, str, str, str, str, str], list[str]] = {}

    for source_row in text_rows + table_rows:
        normalized = _normalize_row(source_row)
        if _is_header_row(normalized):
            continue
        key = _trade_row_key(normalized)
        if key is None:
            continue

        existing = merged.get(key)
        if existing is None:
            merged[key] = normalized
        else:
            merged[key] = _pick_better_row(existing, normalized)

    return list(merged.values())


def _extract_rows(path: str) -> list[list[object | None]]:
    rows: list[list[object | None]] = []
    with pdfplumber.open(path) as pdf:
        for page in pdf.pages:
            page_table_rows: list[list[object | None]] = []
            tables = page.extract_tables() or []
            for table in tables:
                for row in table:
                    if row and any(cell is not None and str(cell).strip() for cell in row):
                        page_table_rows.append(list(row[: len(TABLE_COLUMNS)]))
            page_text_rows = _extract_rows_from_text(page)
            rows.extend(_merge_rows(page_text_rows, page_table_rows))
    return rows


def parse_nubank_trade_notes(
    path: str,
    date: str,
) -> pd.DataFrame:
    raw_rows = _extract_rows(path)
    if not raw_rows:
        return pd.DataFrame()

    rows = [_normalize_row(row) for row in raw_rows if not _is_header_row(row)]
    frame = pd.DataFrame(rows, columns=TABLE_COLUMNS)
    frame = frame[frame["mercado"].str.strip() != ""]

    if frame.empty:
        return pd.DataFrame()

    frame["date"] = date
    frame["file_name"] = os.path.basename(path)
    frame["file_path"] = path
    return frame[BRONZE_COLUMNS]


def ingest_nubank_trading_notes(
    path: str,
    date: str,
) -> int:
    df = parse_nubank_trade_notes(
        path,
        date,
    )
    if df.empty:
        return 0

    return append_dataframe_to_bronze(
        df,
        "pdf_nubank_trade_events",
        source="nubank_trading_notes_pdf",
        endpoint="pdfplumber.extract_tables",
        request_params={"pdf_path": path},
    )
