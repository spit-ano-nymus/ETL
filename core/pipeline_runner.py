"""
pipeline_runner.py
------------------
Reads a YAML pipeline config and orchestrates extract → transform → load.
"""

import logging
import uuid
from datetime import datetime

import yaml

from core.consts import (
    DEFAULT_CHUNK_SIZE,
    DEFAULT_ENCODING,
    DEFAULT_SCHEMA,
    DEFAULT_SEPARATOR,
)
from core.registry import resolve_step

# Import all transform modules to trigger @register side-effects
import transform.cleaners  # noqa: F401
import transform.validators  # noqa: F401
import transform.parsers.date_parser  # noqa: F401
import transform.parsers.phone_parser  # noqa: F401
import transform.parsers.email_parser  # noqa: F401
import transform.parsers.numeric_parser  # noqa: F401

from db.engine import get_engine
from db.loader import bulk_load, write_audit_log
from extract.csv_reader import stream_csv
from transform.validators import ValidationResult
from utils.file_utils import resolve_path, assert_readable

logger = logging.getLogger(__name__)


def load_pipeline_config(yaml_path: str) -> dict:
    path = resolve_path(yaml_path)
    assert_readable(path)
    with open(path, encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def run_pipeline(yaml_path: str, file_override: str | None = None) -> None:
    """
    Execute a full ETL pipeline from a YAML config file.

    Parameters
    ----------
    yaml_path     : Path to the pipeline YAML.
    file_override : Override the source.path in the YAML (--file CLI arg).
    """
    cfg = load_pipeline_config(yaml_path)

    pipeline_name = cfg["pipeline"]["name"]
    source_cfg = cfg["source"]
    dest_cfg = cfg["destination"]
    transform_cfg = cfg.get("transform", {})
    audit_cfg = cfg.get("audit", {})

    source_path = file_override or source_cfg["path"]

    # Resolve transform steps once before streaming starts
    steps = []
    for step_cfg in transform_cfg.get("steps", []):
        fn, params = resolve_step(step_cfg)
        steps.append((fn, params))

    engine = get_engine()
    run_id = str(uuid.uuid4())
    started_at = datetime.utcnow()

    rows_read = rows_inserted = rows_updated = rows_skipped = rows_errored = 0
    status = "success"
    error_detail = None

    table = dest_cfg["table"]
    schema = dest_cfg.get("schema", DEFAULT_SCHEMA)
    load_mode = dest_cfg.get("load_mode", "append")
    primary_keys = dest_cfg.get("primary_keys", [])
    quarantine_table = dest_cfg.get("quarantine_table")

    try:
        logger.info(
            "Pipeline '%s' started  run_id=%s  mode=%s",
            pipeline_name, run_id, load_mode,
        )

        chunk_iter = stream_csv(
            path=source_path,
            separator=source_cfg.get("separator", DEFAULT_SEPARATOR),
            encoding=source_cfg.get("encoding", DEFAULT_ENCODING),
            chunk_size=source_cfg.get("chunk_size", DEFAULT_CHUNK_SIZE),
        )

        for chunk_index, chunk in enumerate(chunk_iter):
            rows_read += len(chunk)

            # Apply transform steps in order
            for fn, params in steps:
                result = fn(chunk, **params)

                if isinstance(result, ValidationResult):
                    for msg in result.messages:
                        logger.warning(msg)
                    if len(result.invalid_df):
                        rows_errored += result.error_count
                        if quarantine_table:
                            _write_quarantine(
                                result.invalid_df, quarantine_table,
                                schema, engine,
                            )
                    chunk = result.valid_df
                else:
                    chunk = result

            counts = bulk_load(
                df=chunk,
                table=table,
                schema=schema,
                engine=engine,
                load_mode=load_mode,
                primary_keys=primary_keys,
                chunk_index=chunk_index,
            )
            rows_inserted += counts["inserted"]
            rows_updated += counts["updated"]
            rows_skipped += counts["skipped"]

            logger.info(
                "Chunk %d: +%d ins  ~%d upd  -%d skip  !%d err",
                chunk_index,
                counts["inserted"], counts["updated"],
                counts["skipped"], 0,
            )

    except Exception as exc:
        status = "failure"
        error_detail = str(exc)
        logger.exception("Pipeline '%s' failed: %s", pipeline_name, exc)
        raise

    finally:
        finished_at = datetime.utcnow()
        if audit_cfg.get("enabled", False):
            try:
                write_audit_log(
                    engine=engine,
                    schema=audit_cfg.get("schema", DEFAULT_SCHEMA),
                    run_id=run_id,
                    pipeline=pipeline_name,
                    source_file=str(source_path),
                    started_at=started_at,
                    finished_at=finished_at,
                    rows_read=rows_read,
                    rows_inserted=rows_inserted,
                    rows_updated=rows_updated,
                    rows_skipped=rows_skipped,
                    rows_errored=rows_errored,
                    status=status,
                    error_detail=error_detail,
                )
            except Exception:
                logger.exception("Failed to write audit log — pipeline result unaffected")

        logger.info(
            "Pipeline '%s' %s | read=%d ins=%d upd=%d skip=%d err=%d",
            pipeline_name, status,
            rows_read, rows_inserted, rows_updated, rows_skipped, rows_errored,
        )


def _write_quarantine(
    df, table: str, schema: str, engine
) -> None:
    """Append invalid rows to the quarantine table (create it if absent)."""
    from sqlalchemy import types

    dtype = {
        col: types.NVARCHAR(length=None)
        for col in df.columns
        if df[col].dtype == object
    }
    try:
        df.to_sql(
            name=table, con=engine, schema=schema,
            if_exists="append", index=False, dtype=dtype, method="multi",
        )
    except Exception:
        logger.exception("Failed to write %d row(s) to quarantine table '%s'", len(df), table)
