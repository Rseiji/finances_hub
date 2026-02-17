from __future__ import annotations

import os
from pathlib import Path

import psycopg


def _conninfo(dsn: str | None) -> str:
    conninfo = dsn or os.environ.get("FINANCES_HUB_PG_DSN")
    if not conninfo:
        raise ValueError("Missing FINANCES_HUB_PG_DSN environment variable.")
    return conninfo


def run_sql_tests(dsn: str | None, tests_dir: Path) -> int:
    statements = _load_statements(tests_dir)
    if not statements:
        return 0
    with psycopg.connect(_conninfo(dsn)) as conn:
        with conn.cursor() as cur:
            for statement in statements:
                cur.execute(statement)
        conn.commit()
    return len(statements)


def _load_statements(tests_dir: Path) -> list[str]:
    statements: list[str] = []
    for path in sorted(tests_dir.glob("*.sql")):
        sql = path.read_text(encoding="utf-8")
        if "DO $$" in sql:
            statements.append(sql.strip())
        else:
            statements.extend(_split_sql(sql))
    return statements


def _split_sql(sql: str) -> list[str]:
    return [statement.strip() for statement in sql.split(";") if statement.strip()]
