"""
loader.py
---------
Bulk-load DataFrames into SQL Server with four load modes:

  replace        – DROP + recreate on first chunk, append after
  append         – always append (create if missing)
  skip_existing  – INSERT only rows whose PKs don't already exist
  upsert         – INSERT new rows + UPDATE existing rows

Also writes one audit-log row per pipeline run to etl_audit_log.
"""

import logging
import re
from datetime import datetime

import pandas as pd
from sqlalchemy import text, types

from core.consts import AUDIT_TABLE, DEFAULT_SCHEMA, MAX_SQL_PARAMS

logger = logging.getLogger(__name__)

# Max PKs to include in a single IN-clause batch
_PK_BATCH = 1_000
# Max PK values to collect per chunk for the UI sample
_PK_SAMPLE = 100


def _safe_param(col: str) -> str:
    """Return a bind-parameter-safe name: replace non-alphanumeric chars with '_'."""
    return re.sub(r"[^a-zA-Z0-9]", "_", col)


# ── helpers ───────────────────────────────────────────────────────────────────

def _nvarchar_dtype(df: pd.DataFrame) -> dict:
    """Map every object column to NVARCHAR(MAX) to prevent truncation."""
    return {
        col: types.NVARCHAR(length=None)
        for col in df.columns
        if df[col].dtype == object
    }


def _batch_size(num_columns: int) -> int:
    """Rows per INSERT batch — respects SQL Server's 2100-parameter limit."""
    return max(1, MAX_SQL_PARAMS // num_columns)


# ── public API ────────────────────────────────────────────────────────────────

def bulk_load(
    df: pd.DataFrame,
    table: str,
    schema: str,
    engine,
    load_mode: str,
    primary_keys: list[str],
    chunk_index: int,
    progress_callback=None,
) -> dict:
    """
    Load one DataFrame chunk into SQL Server.

    Parameters
    ----------
    progress_callback : optional callable(dict).
        Called after the load with ``{"chunk": chunk_index, "rows": int}``.

    Returns
    -------
    dict with keys: inserted, updated, skipped
    """
    if df.empty:
        return {"inserted": 0, "updated": 0, "skipped": 0}

    dtype = _nvarchar_dtype(df)
    bs = _batch_size(len(df.columns))

    def _notify(result: dict) -> dict:
        if progress_callback is not None:
            try:
                progress_callback({"chunk": chunk_index, "rows": len(df)})
            except Exception:
                pass
        return result

    if load_mode == "replace":
        if_exists = "replace" if chunk_index == 0 else "append"
        df.to_sql(
            name=table, con=engine, schema=schema,
            if_exists=if_exists, index=False, dtype=dtype,
            method="multi", chunksize=bs,
        )
        return _notify({"inserted": len(df), "updated": 0, "skipped": 0})

    if load_mode == "append":
        df.to_sql(
            name=table, con=engine, schema=schema,
            if_exists="append", index=False, dtype=dtype,
            method="multi", chunksize=bs,
        )
        return _notify({"inserted": len(df), "updated": 0, "skipped": 0})

    if load_mode == "skip_existing":
        new_df = _filter_new(df, table, schema, engine, primary_keys)
        skipped = len(df) - len(new_df)
        if not new_df.empty:
            new_df.to_sql(
                name=table, con=engine, schema=schema,
                if_exists="append", index=False, dtype=_nvarchar_dtype(new_df),
                method="multi", chunksize=_batch_size(len(new_df.columns)),
            )
        logger.debug("skip_existing: %d inserted, %d skipped", len(new_df), skipped)
        return _notify({"inserted": len(new_df), "updated": 0, "skipped": skipped})

    if load_mode == "upsert":
        new_df, existing_df, table_existed = _split_new_existing(df, table, schema, engine, primary_keys)
        skipped = 0

        if not new_df.empty:
            new_df.to_sql(
                name=table, con=engine, schema=schema,
                if_exists="append", index=False, dtype=_nvarchar_dtype(new_df),
                method="multi", chunksize=_batch_size(len(new_df.columns)),
            )

        updated = 0
        if not existing_df.empty:
            updated = _batch_update(existing_df, table, schema, engine, primary_keys)

        logger.debug(
            "upsert: %d inserted, %d updated, %d skipped",
            len(new_df), updated, skipped,
        )
        return _notify({
            "inserted": len(new_df), "updated": updated, "skipped": skipped,
            "table_existed": table_existed,
            "inserted_rows_sample": _extract_row_sample(new_df) if table_existed else [],
            "updated_rows_sample": _extract_row_sample(existing_df),
        })

    raise ValueError(
        f"Unknown load_mode '{load_mode}'. "
        "Must be one of: replace | append | skip_existing | upsert"
    )


# ── PK helpers ────────────────────────────────────────────────────────────────

def _get_existing_pks(
    df: pd.DataFrame,
    table: str,
    schema: str,
    engine,
    primary_keys: list[str],
) -> set:
    """
    Query existing PKs from the target table in batches.
    Returns a set of string PKs (single) or tuples of strings (composite).
    """
    if not primary_keys:
        return set()

    if len(primary_keys) == 1:
        pk_col = primary_keys[0]
        values = df[pk_col].dropna().astype(str).unique().tolist()
        existing: set = set()
        for i in range(0, len(values), _PK_BATCH):
            batch = values[i : i + _PK_BATCH]
            placeholders = ", ".join(f":v{j}" for j in range(len(batch)))
            sql = text(
                f"SELECT [{pk_col}] "
                f"FROM [{schema}].[{table}] "
                f"WHERE [{pk_col}] IN ({placeholders})"
            )
            params = {f"v{j}": v for j, v in enumerate(batch)}
            with engine.connect() as conn:
                result = conn.execute(sql, params)
                existing.update(str(row[0]) for row in result)
        return existing

    # Composite PK: fetch all and compare in Python (safe for moderate table sizes)
    cols_sql = ", ".join(f"[{c}]" for c in primary_keys)
    sql = text(f"SELECT {cols_sql} FROM [{schema}].[{table}]")
    with engine.connect() as conn:
        result = conn.execute(sql)
        return {tuple(str(v) for v in row) for row in result}


def _pk_series(df: pd.DataFrame, primary_keys: list[str]):
    """Return a Series of hashable PK values (str or tuple of str)."""
    if len(primary_keys) == 1:
        return df[primary_keys[0]].astype(str)
    return df.apply(lambda row: tuple(str(row[c]) for c in primary_keys), axis=1)


def _filter_new(
    df: pd.DataFrame,
    table: str,
    schema: str,
    engine,
    primary_keys: list[str],
) -> pd.DataFrame:
    """Return only rows whose PKs do not exist in the target table."""
    try:
        existing = _get_existing_pks(df, table, schema, engine, primary_keys)
    except Exception:
        return df  # table likely doesn't exist yet — all rows are new

    if not existing:
        return df

    return df[~_pk_series(df, primary_keys).isin(existing)]


def _split_new_existing(
    df: pd.DataFrame,
    table: str,
    schema: str,
    engine,
    primary_keys: list[str],
) -> tuple[pd.DataFrame, pd.DataFrame, bool]:
    """Split df into (new_rows, existing_rows, table_existed) based on PK presence."""
    try:
        existing = _get_existing_pks(df, table, schema, engine, primary_keys)
    except Exception:
        return df, pd.DataFrame(columns=df.columns), False  # table doesn't exist yet

    table_existed = True  # _get_existing_pks succeeded → table exists
    if not existing:
        return df, pd.DataFrame(columns=df.columns), table_existed

    existing_mask = _pk_series(df, primary_keys).isin(existing)
    return df[~existing_mask].copy(), df[existing_mask].copy(), table_existed


def _extract_row_sample(df: pd.DataFrame) -> list[dict]:
    """Return up to _PK_SAMPLE full rows as a list of dicts."""
    if df.empty:
        return []
    return df.head(_PK_SAMPLE).astype(str).to_dict("records")


def _batch_update(
    df: pd.DataFrame,
    table: str,
    schema: str,
    engine,
    primary_keys: list[str],
) -> int:
    """UPDATE existing rows via executemany. Returns number of rows updated."""
    non_pk_cols = [c for c in df.columns if c not in primary_keys]
    if not non_pk_cols:
        return 0

    # Use _safe_param() for bind names — column names with spaces (e.g. "Job Title")
    # would break SQLAlchemy's :name tokeniser if used literally.
    set_clause = ", ".join(f"[{c}] = :{_safe_param(c)}" for c in non_pk_cols)
    where_clause = " AND ".join(f"[{c}] = :{_safe_param(c)}" for c in primary_keys)
    sql = text(
        f"UPDATE [{schema}].[{table}] SET {set_clause} WHERE {where_clause}"
    )

    # Replace NaN with None so SQLAlchemy sends NULL; use safe param keys
    records = [
        {_safe_param(k): v for k, v in row.items()}
        for row in df.where(pd.notnull(df), other=None).to_dict("records")
    ]

    with engine.begin() as conn:
        conn.execute(sql, records)

    return len(df)


# ── audit log ─────────────────────────────────────────────────────────────────

def write_audit_log(
    engine,
    schema: str,
    run_id: str,
    pipeline: str,
    source_file: str,
    started_at: datetime,
    finished_at: datetime,
    rows_read: int,
    rows_inserted: int,
    rows_updated: int,
    rows_skipped: int,
    rows_errored: int,
    status: str,
    error_detail: str | None,
) -> None:
    """Ensure the audit table exists, then insert one run record."""
    _ensure_audit_table(engine, schema)

    sql = text(f"""
        INSERT INTO [{schema}].[{AUDIT_TABLE}]
            (run_id, pipeline, source_file, started_at, finished_at,
             rows_read, rows_inserted, rows_updated, rows_skipped, rows_errored,
             status, error_detail)
        VALUES
            (:run_id, :pipeline, :source_file, :started_at, :finished_at,
             :rows_read, :rows_inserted, :rows_updated, :rows_skipped, :rows_errored,
             :status, :error_detail)
    """)

    with engine.begin() as conn:
        conn.execute(sql, {
            "run_id": run_id,
            "pipeline": pipeline,
            "source_file": source_file,
            "started_at": started_at,
            "finished_at": finished_at,
            "rows_read": rows_read,
            "rows_inserted": rows_inserted,
            "rows_updated": rows_updated,
            "rows_skipped": rows_skipped,
            "rows_errored": rows_errored,
            "status": status,
            "error_detail": error_detail,
        })


def _ensure_audit_table(engine, schema: str) -> None:
    """Create etl_audit_log if it doesn't already exist."""
    ddl = text(f"""
        IF OBJECT_ID(N'[{schema}].[{AUDIT_TABLE}]', N'U') IS NULL
        CREATE TABLE [{schema}].[{AUDIT_TABLE}] (
            id            INT IDENTITY(1,1) PRIMARY KEY,
            run_id        NVARCHAR(36)      NOT NULL,
            pipeline      NVARCHAR(255),
            source_file   NVARCHAR(MAX),
            started_at    DATETIME2,
            finished_at   DATETIME2,
            rows_read     INT DEFAULT 0,
            rows_inserted INT DEFAULT 0,
            rows_updated  INT DEFAULT 0,
            rows_skipped  INT DEFAULT 0,
            rows_errored  INT DEFAULT 0,
            status        NVARCHAR(50),
            error_detail  NVARCHAR(MAX)
        )
    """)
    with engine.begin() as conn:
        conn.execute(ddl)
