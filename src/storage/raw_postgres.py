from __future__ import annotations

import os
import uuid
from typing import Iterable

import pandas as pd
import psycopg
from psycopg.types.json import Json
from psycopg.rows import dict_row
from psycopg import sql

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
            env.asset,
            env.currency,
            env.uid,
            env.fetched_at,
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
                    asset,
                    currency,
                    uid,
                    fetched_at,
                    payload
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                rows,
            )
        conn.commit()
    return len(rows)


def append_dataframe_to_bronze(
    df: pd.DataFrame,
    table_name: str,
    *,
    source: str,
    endpoint: str,
    request_params: dict[str, object],
    uid: str | None = None,
    fetched_at: str | None = None,
    schema_name: str = "bronze",
) -> int:
    if df.empty:
        return 0

    if not table_name.strip():
        raise ValueError("table_name must not be empty.")

    if not schema_name.strip():
        raise ValueError("schema_name must not be empty.")

    batch_uid = uid or str(uuid.uuid4())
    batch_fetched_at = fetched_at or RawEnvelope.utc_now_iso()

    prepared = df.copy()
    prepared["source"] = source
    prepared["endpoint"] = endpoint
    prepared["request_params"] = Json(request_params)
    prepared["uid"] = batch_uid
    prepared["fetched_at"] = batch_fetched_at
    prepared = prepared.where(pd.notna(prepared), None)

    columns = list(prepared.columns)
    rows = [tuple(record[col] for col in columns) for record in prepared.to_dict(orient="records")]

    identifiers = sql.SQL(", ").join(sql.Identifier(column) for column in columns)
    placeholders = sql.SQL(", ").join(sql.Placeholder() for _ in columns)
    query = sql.SQL(
        """
        INSERT INTO {}.{} ({})
        VALUES ({})
        """
    ).format(
        sql.Identifier(schema_name),
        sql.Identifier(table_name),
        identifiers,
        placeholders,
    )

    with psycopg.connect(_conninfo(), row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.executemany(query, rows)
        conn.commit()
    return len(rows)
