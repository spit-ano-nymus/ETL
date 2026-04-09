"""
pipeline_service.py
-------------------
Background worker thread that runs the full ETL loop for one job at a time.
Keeps peak RAM to one chunk regardless of file size.
"""
from __future__ import annotations

import logging
import threading
import time
import uuid
from datetime import datetime

import pandas as pd

from web.services.file_service import stream_file
from web.services.transform_service import apply_selected_transforms
from web.services.destination_service import get_destination_writer
from web.services.progress_service import ProgressEvent, ProgressQueue

logger = logging.getLogger(__name__)


def start_pipeline(
    job: dict,
    progress_queue: ProgressQueue,
    on_done: callable = None,
) -> threading.Thread:
    """
    Launch the ETL loop for `job` in a daemon background thread.

    The thread posts ProgressEvent objects to `progress_queue`.
    Optionally calls `on_done(job_id, stats_dict)` when finished.

    Returns the Thread object (already started).
    """
    job["_progress_queue"] = progress_queue
    t = threading.Thread(
        target=_run_job,
        args=(job, progress_queue, on_done),
        daemon=True,
        name=f"etl-worker-{job['job_id']}",
    )
    t.start()
    return t


def _run_job(
    job: dict,
    pq: ProgressQueue,
    on_done: callable,
) -> None:
    job_id = job["job_id"]
    source = job["file_source"]
    path = source["path"]
    actions = job.get("actions", {})
    column_renames = job.get("column_renames", {})
    chunk_size = 10_000

    started_at = datetime.utcnow()
    rows_read = 0
    rows_inserted = 0
    rows_updated = 0
    rows_skipped = 0
    rows_dropped = 0
    chunk_index = 0
    inserted_rows_sample: list = []
    updated_rows_sample: list = []
    dropped_rows_sample: list = []
    table_existed = False
    _ROW_SAMPLE_CAP = 200

    try:
        writer = get_destination_writer(job)

        for chunk in stream_file(path, chunk_size=chunk_size):
            rows_read += len(chunk)

            # Apply column renames before any transform
            if column_renames:
                chunk = chunk.rename(columns=column_renames)

            # Transform
            transformed, dropped = apply_selected_transforms(chunk, actions)
            rows_dropped += len(chunk) - len(transformed)

            if len(dropped_rows_sample) < _ROW_SAMPLE_CAP and not dropped.empty:
                dropped_rows_sample.extend(dropped.head(_ROW_SAMPLE_CAP).astype(str).to_dict("records"))
                dropped_rows_sample[:] = dropped_rows_sample[:_ROW_SAMPLE_CAP]

            # Load
            stats = writer(transformed, chunk_index)
            rows_inserted += stats.get("inserted", 0)
            rows_updated += stats.get("updated", 0)
            rows_skipped += stats.get("skipped", 0)

            if stats.get("table_existed"):
                table_existed = True
            if len(inserted_rows_sample) < _ROW_SAMPLE_CAP:
                inserted_rows_sample.extend(stats.get("inserted_rows_sample", []))
                inserted_rows_sample[:] = inserted_rows_sample[:_ROW_SAMPLE_CAP]
            if len(updated_rows_sample) < _ROW_SAMPLE_CAP:
                updated_rows_sample.extend(stats.get("updated_rows_sample", []))
                updated_rows_sample[:] = updated_rows_sample[:_ROW_SAMPLE_CAP]

            pq.put(ProgressEvent(
                job_id=job_id,
                chunk=chunk_index,
                total_chunks_est=0,  # unknown for streaming
                rows=len(chunk),
                rows_total=rows_read,
                status="running",
            ))
            chunk_index += 1

        finished_at = datetime.utcnow()
        elapsed = (finished_at - started_at).total_seconds()

        final_stats = {
            "rows_read": rows_read,
            "rows_inserted": rows_inserted,
            "rows_updated": rows_updated,
            "rows_skipped": rows_skipped,
            "rows_dropped": rows_dropped,
            "elapsed_seconds": round(elapsed, 2),
            "chunks": chunk_index,
            "table_existed": table_existed,
            "inserted_rows_sample": inserted_rows_sample,
            "updated_rows_sample": updated_rows_sample,
            "dropped_rows_sample": dropped_rows_sample,
        }

        pq.put(ProgressEvent(
            job_id=job_id,
            chunk=chunk_index,
            total_chunks_est=chunk_index,
            rows=rows_read,
            rows_total=rows_read,
            status="done",
            extra=final_stats,
        ))

        if on_done:
            on_done(job_id, {**final_stats, "status": "done"})

    except Exception as exc:
        logger.exception("Pipeline error for job %s", job_id)
        pq.put(ProgressEvent(
            job_id=job_id,
            chunk=chunk_index,
            total_chunks_est=0,
            rows=rows_read,
            rows_total=rows_read,
            status="error",
            error=str(exc),
        ))
        if on_done:
            on_done(job_id, {"status": "error", "error": str(exc)})


def run_batch_sequential(
    jobs: list[dict],
    progress_queue: ProgressQueue,
    on_job_done: callable = None,
) -> threading.Thread:
    """
    Run all jobs one after another in a single background thread.
    This prevents OOM with large files.
    """
    def _run_all():
        for job in jobs:
            if job.get("status") != "pending":
                continue
            job["status"] = "running"
            done_evt = threading.Event()

            def _done(jid, stats, evt=done_evt, j=job):
                j["status"] = stats.get("status", "done")
                j["stats"] = stats
                if on_job_done:
                    on_job_done(jid, stats)
                evt.set()

            _run_job(job, progress_queue, _done)
            done_evt.wait()

    t = threading.Thread(target=_run_all, daemon=True, name="etl-batch-worker")
    t.start()
    return t
