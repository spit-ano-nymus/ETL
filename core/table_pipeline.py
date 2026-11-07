"""
table_pipeline.py
-----------------
The one-click run for one table, driven by a short YAML file.

    python main.py --pipeline config/tables/customers.yaml

Steps (all table rules come from Postgres, not from the YAML):

  0. setup   run new/changed sql/*.sql files          (db/migrations.py)
  1. fetch   stream the source (Trino or file) into staging.<raw_table>,
             every value as text, no PK, no dedup      (this file)
  2. raw     SELECT meta.load_raw(table_key)           (sql/002_load_raw.sql)
  3. final   SELECT meta.build_final(table_key)        (sql/003_build_final.sql)

Steps 2 and 3 run in one transaction: if step 3 fails, step 2 is undone too,
so raw and final never disagree. Each run is logged in meta.etl_runs.

The YAML holds only what Postgres cannot know: where to read from and how
to connect. See config/tables/*.yaml for examples.
"""

from __future__ import annotations

import io
import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Iterator

import pandas as pd
import yaml
from sqlalchemy import create_engine
from sqlalchemy.engine import URL

from core.consts import DEFAULT_CHUNK_SIZE, DEFAULT_ENCODING, DEFAULT_SEPARATOR
from db.migrations import run_migrations
from utils.file_utils import assert_readable, resolve_path

logger = logging.getLogger(__name__)

STAGING_SCHEMA = "staging"
_NULL = r"\N"      # NULL marker for COPY, so NULL and '' stay different


def _qi(name: str) -> str:
    """Quote a Postgres identifier (handles spaces, Hebrew, embedded quotes)."""
    return '"' + str(name).replace('"', '""') + '"'


# ── config ────────────────────────────────────────────────────────────────────

def load_yaml(path: str) -> dict:
    p = resolve_path(path)
    assert_readable(p)
    with open(p, encoding="utf-8") as fh:
        return yaml.safe_load(fh) or {}


def is_table_pipeline(cfg: dict) -> bool:
    """True for the new one-click format (pipeline.table_key present)."""
    return bool((cfg.get("pipeline") or {}).get("table_key"))


def postgres_engine(pg: dict):
    """Build a SQLAlchemy engine from the YAML `postgres:` block."""
    for key in ("host", "database", "user"):
        if not pg.get(key):
            raise ValueError(f"postgres.{key} is missing in the YAML")

    password = pg.get("password")
    if pg.get("password_env"):
        password = os.environ.get(pg["password_env"])
        if password is None:
            raise ValueError(
                f"Environment variable {pg['password_env']!r} (postgres.password_env) is not set"
            )

    url = URL.create(
        "postgresql+psycopg2",
        username=pg["user"],
        password=password,
        host=pg["host"],
        port=int(pg.get("port", 5432)),
        database=pg["database"],
    )
    return create_engine(url)


# ── source ────────────────────────────────────────────────────────────────────

def _stream_source(src: dict, file_override: str | None) -> tuple[Iterator[pd.DataFrame], str]:
    """Return (chunk iterator, label for the run log)."""
    chunk_size = int(src.get("chunk_size", DEFAULT_CHUNK_SIZE))
    kind = src.get("type", "file")

    if kind == "trino":
        from web.services.trino_service import stream_trino_table  # no Streamlit import
        password = src.get("password")
        if src.get("password_env"):
            password = os.environ.get(src["password_env"])
        it = stream_trino_table(
            host=src["host"],
            port=int(src.get("port", 8080)),
            user=src["user"],
            catalog=src["catalog"],
            schema=src["schema"],
            table=src["table"],
            chunk_size=chunk_size,
            password=password,
        )
        return it, f"trino:{src['catalog']}.{src['schema']}.{src['table']}"

    if kind == "file":
        from extract.csv_reader import stream_csv
        path = file_override or src.get("path")
        if not path:
            raise ValueError("source.path is missing in the YAML (or pass --file)")
        it = stream_csv(
            path=path,
            separator=src.get("separator", DEFAULT_SEPARATOR),
            encoding=src.get("encoding", DEFAULT_ENCODING),
            chunk_size=chunk_size,
        )
        return it, f"file:{path}"

    raise ValueError(f"source.type must be 'trino' or 'file', got {kind!r}")


def _as_text(df: pd.DataFrame) -> pd.DataFrame:
    """Every value as str, missing values as None (-> NULL)."""
    out = df.astype(object).where(df.notna(), None)
    for col in out.columns:
        out[col] = out[col].map(lambda v: None if v is None else str(v))
    return out


# ── step 1: fetch into staging ────────────────────────────────────────────────

def _load_staging(engine, table: str, chunks: Iterator[pd.DataFrame]) -> int:
    """Recreate staging.<table> with the source's columns (all text) and COPY every chunk in."""
    raw = engine.raw_connection()
    try:
        cur = raw.cursor()
        qtable = f"{_qi(STAGING_SCHEMA)}.{_qi(table)}"
        columns: list[str] | None = None
        total = 0

        for i, chunk in enumerate(chunks):
            if columns is None:
                columns = [str(c) for c in chunk.columns]
                bad = [c for c in columns if not c.strip()]
                dup = sorted({c for c in columns if columns.count(c) > 1})
                if bad or dup:
                    raise ValueError(f"Source has empty or duplicate column names: {bad or dup}")
                col_defs = ", ".join(f"{_qi(c)} text" for c in columns)
                cur.execute(f"CREATE SCHEMA IF NOT EXISTS {_qi(STAGING_SCHEMA)}")
                cur.execute(f"DROP TABLE IF EXISTS {qtable}")
                cur.execute(f"CREATE TABLE {qtable} ({col_defs})")
            elif [str(c) for c in chunk.columns] != columns:
                raise ValueError(f"Chunk {i} has different columns than chunk 0")

            if chunk.empty:
                continue
            buf = io.StringIO()
            _as_text(chunk).to_csv(buf, index=False, header=False, na_rep=_NULL)
            buf.seek(0)
            col_list = ", ".join(_qi(c) for c in columns)
            cur.copy_expert(
                f"COPY {qtable} ({col_list}) FROM STDIN WITH (FORMAT csv, NULL '{_NULL}')",
                buf,
            )
            total += len(chunk)
            logger.info("fetch: chunk %d -> %d rows (total %d)", i, len(chunk), total)

        if columns is None:
            raise ValueError("The source returned no data (not even column names)")

        raw.commit()
        return total
    except Exception:
        raw.rollback()
        raise
    finally:
        raw.close()


# ── run log ───────────────────────────────────────────────────────────────────

def _log_start(engine, run_id: str, table_key: str, source: str) -> None:
    raw = engine.raw_connection()
    try:
        cur = raw.cursor()
        cur.execute(
            "INSERT INTO meta.etl_runs (run_id, table_key, source, started_at, status) "
            "VALUES (%s, %s, %s, %s, 'running')",
            (run_id, table_key, source, datetime.now(timezone.utc)),
        )
        raw.commit()
    finally:
        raw.close()


def _log_end(engine, run_id: str, stats: dict, status: str, error: str | None) -> None:
    try:
        raw = engine.raw_connection()
        try:
            cur = raw.cursor()
            cur.execute(
                """
                UPDATE meta.etl_runs
                   SET finished_at = %s, staged_rows = %s, raw_inserted = %s,
                       final_rows = %s, cast_failures = %s, status = %s, error_detail = %s
                 WHERE run_id = %s
                """,
                (datetime.now(timezone.utc), stats.get("staged_rows"), stats.get("raw_inserted"),
                 stats.get("final_rows"), stats.get("cast_failures"), status, error, run_id),
            )
            raw.commit()
        finally:
            raw.close()
    except Exception:
        logger.exception("Could not write the run log — the run result itself is unaffected")


# ── checks ────────────────────────────────────────────────────────────────────

def _table_config(engine, table_key: str) -> dict:
    raw = engine.raw_connection()
    try:
        cur = raw.cursor()
        cur.execute(
            "SELECT raw_table, table_type, is_active FROM meta.etl_tables WHERE table_key = %s",
            (table_key,),
        )
        row = cur.fetchone()
        raw.commit()
    finally:
        raw.close()
    if row is None:
        raise ValueError(
            f"table_key {table_key!r} has no row in meta.etl_tables — "
            "add it (and its mapper rows) before the first run"
        )
    raw_table, table_type, is_active = row
    if not is_active:
        raise ValueError(f"table_key {table_key!r} is marked inactive in meta.etl_tables")
    return {"raw_table": raw_table, "table_type": table_type}


def _log_notices(raw) -> None:
    for n in getattr(raw, "notices", []):
        msg = n.strip()
        if msg.startswith("WARNING"):
            logger.warning(msg)
        elif "load_raw" in msg or "build_final" in msg:
            logger.info(msg.removeprefix("NOTICE:").strip())
        else:
            logger.debug(msg)   # e.g. "schema already exists, skipping"
    if hasattr(raw, "notices"):
        del raw.notices[:]


# ── steps 2 + 3 ───────────────────────────────────────────────────────────────

def _raw_and_final(engine, table_key: str, final_only: bool) -> dict:
    raw = engine.raw_connection()
    try:
        cur = raw.cursor()
        stats: dict = {}
        if not final_only:
            cur.execute("SELECT staged_rows, inserted_rows FROM meta.load_raw(%s)", (table_key,))
            stats["staged_rows"], stats["raw_inserted"] = cur.fetchone()
            _log_notices(raw)
        cur.execute("SELECT final_rows, cast_failures FROM meta.build_final(%s)", (table_key,))
        stats["final_rows"], stats["cast_failures"] = cur.fetchone()
        _log_notices(raw)
        raw.commit()
        return stats
    except Exception:
        _log_notices(raw)
        raw.rollback()
        raise
    finally:
        raw.close()


# ── entry point ───────────────────────────────────────────────────────────────

def run_table_pipeline(
    cfg: dict,
    file_override: str | None = None,
    setup_only: bool = False,
    final_only: bool = False,
) -> dict:
    """
    Run the one-click pipeline for the table in `cfg` (a parsed YAML).

    setup_only : only apply sql/ files (first-time setup, no data moved)
    final_only : skip fetch + raw; rebuild the final table from raw
                 (use after changing the mapper or exclude_where)
    """
    table_key = cfg["pipeline"]["table_key"]
    engine = postgres_engine(cfg.get("postgres") or {})

    run_migrations(engine)
    if setup_only:
        logger.info("setup only: sql/ files applied, nothing else run")
        return {}

    tcfg = _table_config(engine, table_key)

    if final_only:
        source_label = "final-only rebuild"
    else:
        chunks, source_label = _stream_source(cfg.get("source") or {}, file_override)

    run_id = str(uuid.uuid4())
    _log_start(engine, run_id, table_key, source_label)
    logger.info("run %s: table_key=%s (%s) source=%s",
                run_id, table_key, tcfg["table_type"], source_label)

    stats: dict = {}
    try:
        if not final_only:
            staged = _load_staging(engine, tcfg["raw_table"], chunks)
            logger.info("step 1 fetch: %d rows in %s.%s", staged, STAGING_SCHEMA, tcfg["raw_table"])

        stats = _raw_and_final(engine, table_key, final_only)
        if not final_only:
            logger.info("step 2 raw: %s new row(s) out of %s", stats["raw_inserted"], stats["staged_rows"])
        logger.info("step 3 final: %s row(s), %s value(s) could not be converted",
                    stats["final_rows"], stats["cast_failures"])
    except Exception as exc:
        _log_end(engine, run_id, stats, "failure", str(exc))
        logger.error("run %s FAILED: %s", run_id, exc)
        raise

    _log_end(engine, run_id, stats, "success", None)
    logger.info("run %s finished successfully", run_id)
    return stats
