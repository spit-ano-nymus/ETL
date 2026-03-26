"""
destination_service.py
----------------------
Build destination writers and check whether a target table already exists.
"""
from __future__ import annotations

import logging
from typing import Callable

import pandas as pd

logger = logging.getLogger(__name__)


# ── table existence check ─────────────────────────────────────────────────────

def check_table_exists(table: str, schema: str, engine) -> dict | None:
    """
    Return {"row_count": int, "columns": list[str]} if the table exists,
    otherwise None.
    """
    try:
        from sqlalchemy import inspect, text
        inspector = inspect(engine)
        if not inspector.has_table(table, schema=schema):
            return None
        columns = [col["name"] for col in inspector.get_columns(table, schema=schema)]
        with engine.connect() as conn:
            result = conn.execute(
                text(f"SELECT COUNT(*) FROM [{schema}].[{table}]")
            )
            row_count = result.scalar()
        return {"row_count": row_count, "columns": columns}
    except Exception as exc:
        logger.warning("check_table_exists failed: %s", exc)
        return None


# ── connection-string builder ─────────────────────────────────────────────────

def build_sqlserver_connection_string(creds: dict) -> str:
    """
    Build a SQLAlchemy mssql+pyodbc connection URL from the credential dict
    that the destination form collects.

    creds keys: server, database, username, password, driver, trusted_connection
    """
    from urllib.parse import quote_plus

    driver = creds.get("driver", "ODBC Driver 17 for SQL Server")
    server = creds["server"]
    database = creds["database"]
    trusted = creds.get("trusted_connection", False)

    if trusted:
        params = (
            f"driver={driver};server={server};database={database};"
            f"trusted_connection=yes;TrustServerCertificate=yes"
        )
    else:
        username = creds.get("username", "")
        password = creds.get("password", "")
        params = (
            f"driver={driver};server={server};database={database};"
            f"uid={username};pwd={password};TrustServerCertificate=yes"
        )

    return f"mssql+pyodbc:///?odbc_connect={quote_plus(params)}"


# ── destination writer factory ─────────────────────────────────────────────────

def get_destination_writer(job: dict) -> Callable[[pd.DataFrame, int], dict]:
    """
    Return a callable ``write(df, chunk_index) → stats_dict`` for the job's
    destination type.

    Supports: "sqlserver" | "s3"
    """
    dest = job["destination"]
    dest_type = dest["type"]

    if dest_type == "sqlserver":
        from db.engine import get_engine
        from db.loader import bulk_load

        conn_str = build_sqlserver_connection_string(dest)
        engine = get_engine(connection_string=conn_str)
        table = job["table_name"]
        schema = job.get("schema_name", "dbo")
        load_mode = job.get("load_mode", "append")
        primary_keys = job.get("primary_keys", [])
        progress_cb = _make_progress_cb(job)

        def write_sqlserver(df: pd.DataFrame, chunk_index: int) -> dict:
            return bulk_load(
                df=df,
                table=table,
                schema=schema,
                engine=engine,
                load_mode=load_mode,
                primary_keys=primary_keys,
                chunk_index=chunk_index,
                progress_callback=progress_cb,
            )

        return write_sqlserver

    if dest_type == "s3":
        from web.s3.engine import get_s3_client
        from web.s3.loader import s3_load

        client = get_s3_client(
            access_key=dest["access_key"],
            secret_key=dest["secret_key"],
            region=dest.get("region", "us-east-1"),
        )
        bucket = dest["bucket"]
        key_prefix = dest.get("key_prefix", job["table_name"])
        load_mode = job.get("load_mode", "append")
        progress_cb = _make_progress_cb(job)

        # For replace mode, delete existing objects on first chunk only
        _first_chunk_done = {"done": False}

        def write_s3(df: pd.DataFrame, chunk_index: int) -> dict:
            actual_mode = load_mode
            if load_mode == "replace" and not _first_chunk_done["done"]:
                actual_mode = "replace"
                _first_chunk_done["done"] = True
            elif load_mode == "replace":
                actual_mode = "append"

            result = s3_load(
                df=df,
                bucket=bucket,
                key_prefix=key_prefix,
                client=client,
                load_mode=actual_mode,
                chunk_index=chunk_index,
            )
            if progress_cb:
                progress_cb({"chunk": chunk_index, "rows": len(df)})
            return result

        return write_s3

    raise ValueError(f"Unknown destination type: {dest_type!r}")


def _make_progress_cb(job: dict):
    """Return a progress callback that posts to the job's progress queue if set."""
    pq = job.get("_progress_queue")
    job_id = job.get("job_id", "")
    if pq is None:
        return None

    def cb(info: dict) -> None:
        from web.services.progress_service import ProgressEvent
        pq.put(ProgressEvent(
            job_id=job_id,
            chunk=info.get("chunk", 0),
            total_chunks_est=job.get("_total_chunks_est", 0),
            rows=info.get("rows", 0),
            rows_total=job.get("_rows_total", 0),
            status="running",
        ))

    return cb
