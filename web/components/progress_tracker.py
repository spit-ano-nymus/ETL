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


def _render_colored_rows(rows: list[dict], total: int, color: str) -> None:
    """Render full rows with every cell painted in the given background color."""
    import pandas as pd
    if not rows:
        return
    df = pd.DataFrame(rows)
    styled = df.style.set_properties(**{"background-color": color, "color": "black"})
    st.dataframe(styled, use_container_width=True, hide_index=True)
    if total > len(rows):
        st.caption(f"Showing first {len(rows):,} of {total:,} rows.")


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
        dest = job.get("destination", {})
        table_name = job.get("table_name", "")
        load_mode = job.get("load_mode", "")

        with st.container():
            hdr_cols = st.columns([4, 1])
            hdr_cols[0].markdown(f"**{label}**")
            if table_name:
                hdr_cols[1].caption(f"→ `{job.get('schema_name','dbo')}.{table_name}` ({load_mode})")

            if status == "running" and evt:
                chunks = evt.chunk + 1
                rows_total = evt.rows_total
                metric_cols = st.columns(4)
                metric_cols[0].metric("Chunks processed", chunks)
                metric_cols[1].metric("Rows read", f"{rows_total:,}")
                metric_cols[2].markdown("")
                metric_cols[3].markdown("▶ **running**")
                st.progress(min(1.0, chunks / max(1, job.get("_total_chunks_est", chunks))))

            elif status == "done":
                stats = job.get("stats", {})
                metric_cols = st.columns(6)
                metric_cols[0].metric("Read", f"{stats.get('rows_read', 0):,}")
                metric_cols[1].metric("Inserted", f"{stats.get('rows_inserted', 0):,}")
                metric_cols[2].metric("Updated", f"{stats.get('rows_updated', 0):,}")
                metric_cols[3].metric("Skipped", f"{stats.get('rows_skipped', 0):,}")
                metric_cols[4].metric("Dropped", f"{stats.get('rows_dropped', 0):,}")
                metric_cols[5].metric("Duration (s)", stats.get("elapsed_seconds", "—"))
                st.progress(1.0)
                st.caption("✅ done")

                # Row detail expander
                updated_rows = stats.get("updated_rows_sample", [])
                dropped_rows = stats.get("dropped_rows_sample", [])
                inserted_rows = stats.get("inserted_rows_sample", [])
                table_existed = stats.get("table_existed", False)
                pk_cols = job.get("primary_keys", [])

                has_detail = updated_rows or dropped_rows or (inserted_rows and table_existed)
                if has_detail:
                    with st.expander(f"Row details (PK: {', '.join(pk_cols) or 'n/a'})"):
                        tab_labels = [
                            f"Updated ({stats.get('rows_updated', 0):,})",
                            f"Dropped ({stats.get('rows_dropped', 0):,})",
                        ]
                        if table_existed:
                            tab_labels.append(f"Inserted ({stats.get('rows_inserted', 0):,})")

                        tabs = st.tabs(tab_labels)

                        with tabs[0]:
                            if updated_rows:
                                _render_colored_rows(updated_rows, stats.get("rows_updated", 0), "#d4edda")
                            else:
                                st.caption("No rows were updated.")

                        with tabs[1]:
                            if dropped_rows:
                                _render_colored_rows(dropped_rows, stats.get("rows_dropped", 0), "#f8d7da")
                            else:
                                st.caption("No rows were dropped.")

                        if table_existed and len(tabs) > 2:
                            with tabs[2]:
                                if inserted_rows:
                                    _render_colored_rows(inserted_rows, stats.get("rows_inserted", 0), "#ffffff")
                                else:
                                    st.caption("No new rows were inserted.")

            elif status == "error":
                err = evt.error if evt else "unknown"
                st.markdown("❌ **error**")
                st.error(f"Error: {err}")
                st.progress(0.0)

            else:
                st.caption("⏳ pending")

        st.divider()

    if not all_done and not worker_done:
        time.sleep(0.3)
        st.rerun()

    return all_done
