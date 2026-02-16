from __future__ import annotations

import os
from pathlib import Path

import psycopg


def _conninfo(dsn: str | None) -> str:
    conninfo = dsn or os.environ.get("FINANCES_HUB_PG_DSN")
    if not conninfo:
        raise ValueError("Missing FINANCES_HUB_PG_DSN environment variable.")
    return conninfo


def run_gold_transforms(dsn: str | None = None) -> dict[str, int]:
    statements = _load_statements(_sql_dir())
    with psycopg.connect(_conninfo(dsn)) as conn:
        with conn.cursor() as cur:
            for statement in statements:
                cur.execute(statement)
        conn.commit()

    return {"statements": len(statements)}


def _sql_dir() -> Path:
    return Path(__file__).resolve().parents[3] / "sql" / "gold"


def _load_statements(sql_dir: Path) -> list[str]:
    statements: list[str] = []
    for path in sorted(sql_dir.glob("*.sql")):
        if path.name == "schema.sql":
            continue
        sql = path.read_text(encoding="utf-8")
        statements.extend(_split_sql(sql))
    return statements


def _split_sql(sql: str) -> list[str]:
    return [statement.strip() for statement in sql.split(";") if statement.strip()]
