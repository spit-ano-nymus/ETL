"""
progress_tracker.py
-------------------
Polling progress bars and live metrics for Step 4.
Call render_progress() inside a st.empty() container; it will call st.rerun()
until all jobs are done.
"""
from __future__ import annotations

import time

import streamlit as st

from web.services.progress_service import ProgressQueue


def render_progress(
    batch_queue: list[dict],
    pq: ProgressQueue,
    worker_done: bool = False,
) -> bool:
    """
    Render per-job progress bars.

    Returns True when all jobs have finished (done or error).
    """
    # Drain events from the queue
    events = pq.get_all()
    for evt in events:
        for job in batch_queue:
            if job["job_id"] == evt.job_id:
                job["_last_event"] = evt
                if evt.status in ("done", "error"):
                    job["status"] = evt.status
                    if evt.extra:
                        job["stats"] = evt.extra

    all_done = all(j.get("status") in ("done", "error") for j in batch_queue)

    for job in batch_queue:
        evt = job.get("_last_event")
        status = job.get("status", "pending")
        label = job["file_source"]["path"].split("/")[-1].split("\\")[-1]

        with st.container():
            cols = st.columns([3, 1, 1, 1])
            cols[0].markdown(f"**{label}**")

            if status == "running" and evt:
                chunks = evt.chunk + 1
                rows_total = evt.rows_total
                cols[1].metric("Chunks", chunks)
                cols[2].metric("Rows", f"{rows_total:,}")
                cols[3].markdown(f"▶ running")
                st.progress(min(1.0, chunks / max(1, job.get("_total_chunks_est", chunks))))

            elif status == "done":
                stats = job.get("stats", {})
                cols[1].metric("Rows read", f"{stats.get('rows_read', 0):,}")
                cols[2].metric("Inserted", f"{stats.get('rows_inserted', 0):,}")
                cols[3].markdown("✅ done")
                st.progress(1.0)

            elif status == "error":
                err = evt.error if evt else "unknown"
                cols[3].markdown("❌ error")
                st.error(f"Error: {err}")
                st.progress(0.0)

            else:
                cols[3].markdown("⏳ pending")

    if not all_done and not worker_done:
        time.sleep(0.3)
        st.rerun()

    return all_done
