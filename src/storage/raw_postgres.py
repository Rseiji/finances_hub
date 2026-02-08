from __future__ import annotations

import os
from typing import Iterable

import psycopg
from psycopg.types.json import Json
from psycopg.rows import dict_row

from ingestion.models import RawEnvelope


def _conninfo() -> str:
    dsn = os.environ.get("FINANCES_HUB_PG_DSN")
    if not dsn:
        raise ValueError("Missing FINANCES_HUB_PG_DSN environment variable.")
    return dsn


def insert_envelopes(envelopes: Iterable[RawEnvelope]) -> int:
    rows = [
        (
            env.source,
            env.endpoint,
            Json(env.request_params),
            env.request_id,
            env.fetched_at,
            env.run_id,
            Json(env.payload),
        )
        for env in envelopes
    ]
    if not rows:
        return 0

    with psycopg.connect(_conninfo(), row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO bronze.ingestion_events (
                    source,
                    endpoint,
                    request_params,
                    request_id,
                    fetched_at,
                    run_id,
                    payload
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                rows,
            )
        conn.commit()
    return len(rows)
