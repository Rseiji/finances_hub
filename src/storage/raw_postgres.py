from __future__ import annotations

import os
from typing import Iterable

import psycopg
from psycopg.types.json import Json
from psycopg.rows import dict_row

from ingestion.models import PdfTradeEvent, RawEnvelope


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


def insert_pdf_trade_events(events: Iterable[PdfTradeEvent]) -> int:
    rows = [
        (
            event.uid,
            event.source,
            Json(event.request_params),
            event.fetched_at,
            event.mercado,
            event.cv,
            event.tipo_mercado,
            event.espec_titulo,
            event.observacao,
            event.quantidade,
            event.preco,
            event.valor,
            event.dc,
            event.taxa_liquidacao,
            event.emolumentos,
            event.taxa_transf_ativos,
            event.file_path,
            event.file_name,
            event.statement_date,
        )
        for event in events
    ]
    if not rows:
        return 0

    with psycopg.connect(_conninfo(), row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO bronze.pdf_nuinvest_trade_events (
                    uid,
                    source,
                    request_params,
                    fetched_at,
                    mercado,
                    cv,
                    tipo_mercado,
                    espec_titulo,
                    observacao,
                    quantidade,
                    preco,
                    valor,
                    dc,
                    taxa_liquidacao,
                    emolumentos,
                    taxa_transf_ativos,
                    file_path,
                    file_name,
                    statement_date
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                rows,
            )
        conn.commit()
    return len(rows)
