from pathlib import Path
import sys
import argparse
from datetime import datetime
import os
from typing import Any

import psycopg
import pandas as pd

# Add the parent directory to the path if needed
sys.path.insert(0, str(Path(__file__).parent))

from ingestion.nubank_trading_notes import ingest_nubank_trading_notes
from processing.silver.silver_transform import run_silver_transforms
from processing.gold.gold_transform import run_gold_transforms


def _conninfo() -> str:
    dsn = os.environ.get("FINANCES_HUB_PG_DSN")
    if not dsn:
        raise ValueError("Missing FINANCES_HUB_PG_DSN environment variable.")
    return dsn


def _fetch_touched_gold_rows(started_at: datetime) -> list[dict[str, Any]]:
    query = """
        SELECT *
        FROM gold.nubank_trade_events
        WHERE ingested_at >= %s
        ORDER BY ingested_at, date, ticker, quantidade, preco
    """
    with psycopg.connect(_conninfo()) as conn:
        with conn.cursor() as cur:
            cur.execute(query, (started_at,))
            columns = [desc.name for desc in cur.description]
            return [dict(zip(columns, row)) for row in cur.fetchall()]


def _fetch_current_run_bronze_batch_ids(
    *,
    path: str,
    date: datetime.date,
    started_at: datetime,
) -> list[str]:
    query = """
        SELECT DISTINCT uid::text
        FROM bronze.pdf_nubank_trade_events
        WHERE file_path = %s
          AND date = %s
          AND fetched_at >= %s
        ORDER BY uid::text
    """
    with psycopg.connect(_conninfo()) as conn:
        with conn.cursor() as cur:
            cur.execute(query, (path, date, started_at))
            return [row[0] for row in cur.fetchall()]


def _fetch_touched_gold_rows_for_batches(batch_ids: list[str]) -> list[dict[str, Any]]:
    if not batch_ids:
        return []

    query = """
        SELECT *
        FROM gold.nubank_trade_events
        WHERE batch_id::text = ANY(%s)
        ORDER BY ingested_at, date, ticker, quantidade, preco
    """
    with psycopg.connect(_conninfo()) as conn:
        with conn.cursor() as cur:
            cur.execute(query, (batch_ids,))
            columns = [desc.name for desc in cur.description]
            return [dict(zip(columns, row)) for row in cur.fetchall()]


def _print_gold_rows(rows: list[dict[str, Any]]) -> None:
    print("Gold rows touched in this run:")
    if not rows:
        print("  (none)")
        return
    frame = pd.DataFrame(rows)
    ordered_columns = [
        "event_id",
        "batch_id",
        "date",
        "ticker",
        "quantidade",
        "preco",
        "valor",
        "fetched_at",
        "ingested_at",
    ]
    frame = frame[ordered_columns]
    print(frame.to_string(index=False))

def run_trading_notes(
    path: str,
    date: datetime.date,
    run_tests: bool = True,
) -> None:
    """Execute Nubank trading notes bronze ingestion followed by silver and gold transforms."""
    try:
        bronze_started_at = datetime.now()
        bronze_rows = ingest_nubank_trading_notes(
            path=path,
            date=date,
        )
        bronze_batch_ids = _fetch_current_run_bronze_batch_ids(
            path=path,
            date=date,
            started_at=bronze_started_at,
        )
        silver_result = run_silver_transforms(run_tests=run_tests)
        gold_started_at = datetime.now()
        gold_result = run_gold_transforms(run_tests=run_tests)
        touched_gold_rows = _fetch_touched_gold_rows_for_batches(bronze_batch_ids)

        print("Trading notes pipeline completed successfully")
        print(f"Bronze rows ingested: {bronze_rows}")
        print(f"Silver statements executed: {silver_result['statements']}")
        print(f"Gold statements executed: {gold_result['statements']}")
        _print_gold_rows(touched_gold_rows)
    except Exception as e:
        print(f"Error executing script: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run Nubank trading notes bronze/silver/gold pipeline")
    parser.add_argument("--path", type=str, required=True, help="Path to PDF")
    parser.add_argument("--date", type=str, required=True, help="Trading date in YYYY-MM-DD format")
    parser.add_argument("--skip-tests", action="store_true", help="Skip silver and gold SQL tests")
    
    args = parser.parse_args()
    run_trading_notes(
        path=args.path,
        date=datetime.strptime(args.date, "%Y-%m-%d").date(),
        run_tests=not args.skip_tests,
    )
