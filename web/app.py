"""
app.py
------
ETL Studio — Streamlit entry point.

Run with:
    streamlit run web/app.py
"""
from __future__ import annotations

import sys
import os

# Ensure the project root is on the path so existing ETL modules are importable
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

import uuid

import streamlit as st

from web.config import init_session_state, STEPS, LOAD_MODES

# ── page config ────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="ETL Studio",
    page_icon="⚙",
    layout="wide",
    initial_sidebar_state="collapsed",
)

init_session_state()


# ── helpers ────────────────────────────────────────────────────────────────────

def _go(step: int) -> None:
    st.session_state["etl.step"] = step


def _step_header() -> None:
    step = st.session_state["etl.step"]
    cols = st.columns(len(STEPS))
    for i, (s, label) in enumerate(STEPS.items()):
        with cols[i]:
            if s == step:
                st.markdown(f"**Step {s}: {label}**")
            elif s < step:
                st.markdown(f"~~Step {s}: {label}~~")
            else:
                st.markdown(f"Step {s}: {label}")
    st.divider()


# ── step 1 ─────────────────────────────────────────────────────────────────────

def _step1() -> None:
    st.header("Step 1 — Input & Destination")

    from web.components.file_input import render_file_input
    from web.components.destination_form import render_destination_form

    session_id = st.session_state["etl.session_id"]
    file_source = render_file_input(session_id)

    st.divider()
    destination = render_destination_form()

    if destination and destination.get("type") == "sqlserver":
        load_mode = st.selectbox(
            "Load mode",
            LOAD_MODES,
            key="step1_load_mode",
            help="replace = DROP+recreate | append = always insert | upsert = insert+update | skip_existing = ignore existing PKs",
        )
        primary_keys_raw = st.text_input(
            "Primary key column(s) (comma-separated, required for upsert/skip)",
            key="step1_pks",
            placeholder="id  or  id,date",
        )
    else:
        load_mode = "append"
        primary_keys_raw = ""

    st.divider()
    col_add, col_next = st.columns([1, 1])

    with col_add:
        if st.button("Add to queue", disabled=(file_source is None or destination is None)):
            if file_source and destination:
                pks = [pk.strip() for pk in primary_keys_raw.split(",") if pk.strip()]
                table_name = (
                    destination.get("table", "")
                    or destination.get("key_prefix", "output").split("/")[-1]
                )
                schema_name = destination.get("schema", "dbo")
                job: dict = {
                    "job_id": str(uuid.uuid4()),
                    "file_source": file_source,
                    "destination": destination,
                    "actions": {},
                    "load_mode": load_mode,
                    "table_name": table_name,
                    "schema_name": schema_name,
                    "primary_keys": pks,
                    "status": "pending",
                    "preview_df": None,
                    "stats": {},
                }
                st.session_state["etl.batch_queue"].append(job)
                st.session_state["etl.active_job_id"] = job["job_id"]
                st.success(f"Added job {job['job_id'][:8]} to queue.")

    with col_next:
        queue_len = len(st.session_state["etl.batch_queue"])
        if st.button(
            f"Next → ({queue_len} job{'s' if queue_len != 1 else ''} queued)",
            disabled=queue_len == 0,
        ):
            _go(2)
            st.rerun()


# ── step 2 ─────────────────────────────────────────────────────────────────────

def _step2() -> None:
    st.header("Step 2 — Actions & Batch Review")

    from web.components.action_selector import render_action_selector
    from web.components.batch_queue import render_batch_queue

    actions = render_action_selector()

    # Persist actions on all queued jobs
    for job in st.session_state["etl.batch_queue"]:
        job["actions"] = dict(actions)

    st.divider()
    st.subheader("Batch queue")
    render_batch_queue(st.session_state["etl.batch_queue"])

    st.divider()
    col_back, col_preview, col_run = st.columns(3)

    with col_back:
        if st.button("← Back"):
            _go(1)
            st.rerun()

    with col_preview:
        active_id = st.session_state.get("etl.active_job_id")
        if st.button("Preview active job", disabled=active_id is None):
            _load_preview(active_id)
            _go(3)
            st.rerun()

    with col_run:
        if st.button("Run all →", disabled=len(st.session_state["etl.batch_queue"]) == 0):
            # Mark all pending jobs with current actions
            for job in st.session_state["etl.batch_queue"]:
                job["actions"] = dict(actions)
            _go(4)
            st.rerun()


def _load_preview(job_id: str) -> None:
    from web.services.file_service import sample_rows, compute_column_stats
    for job in st.session_state["etl.batch_queue"]:
        if job["job_id"] == job_id:
            try:
                df = sample_rows(job["file_source"]["path"])
                job["preview_df"] = df
                job["stats"]["column_stats"] = compute_column_stats(df).to_dict("records")
                job["status"] = "previewing"
            except Exception as exc:
                st.error(f"Preview failed: {exc}")
            break


# ── step 3 ─────────────────────────────────────────────────────────────────────

def _step3() -> None:
    st.header("Step 3 — Review")

    active_id = st.session_state.get("etl.active_job_id")
    job = next(
        (j for j in st.session_state["etl.batch_queue"] if j["job_id"] == active_id),
        None,
    )

    if job is None:
        st.warning("No active job selected. Go back to Step 2.")
        if st.button("← Back"):
            _go(2)
            st.rerun()
        return

    # Preview
    from web.components.preview_table import render_preview_table, render_column_stats
    from web.components.sql_window import render_sql_window

    if job.get("preview_df") is not None:
        render_preview_table(job["preview_df"])
    else:
        st.info("No preview loaded. Use 'Preview active job' in Step 2.")

    st.divider()

    # Existing table check (SQL Server only)
    dest = job["destination"]
    if dest["type"] == "sqlserver":
        _check_existing_table(job)

    st.divider()

    # Mining stats
    col_stats = job.get("stats", {}).get("column_stats")
    if col_stats and job.get("actions", {}).get("mining", False):
        import pandas as pd
        render_column_stats(pd.DataFrame(col_stats))
        st.divider()

    # SQL scratch pad
    table = job.get("table_name", "")
    schema = job.get("schema_name", "dbo")
    if dest["type"] == "sqlserver":
        render_sql_window(table=table, schema=schema)
        st.divider()

    col_back, col_confirm = st.columns(2)
    with col_back:
        if st.button("← Back"):
            _go(2)
            st.rerun()
    with col_confirm:
        if st.button("Confirm & Upload →"):
            _go(4)
            st.rerun()


def _check_existing_table(job: dict) -> None:
    from web.services.destination_service import (
        check_table_exists,
        build_sqlserver_connection_string,
    )
    from db.engine import get_engine

    dest = job["destination"]
    try:
        conn_str = build_sqlserver_connection_string(dest)
        engine = get_engine(connection_string=conn_str)
        table_info = check_table_exists(
            table=job["table_name"],
            schema=job.get("schema_name", "dbo"),
            engine=engine,
        )
    except Exception as exc:
        st.warning(f"Could not check table existence: {exc}")
        return

    if table_info:
        st.warning(
            f"Table **[{job.get('schema_name', 'dbo')}].[{job['table_name']}]** already exists "
            f"({table_info['row_count']:,} rows, {len(table_info['columns'])} columns)."
        )
        new_mode = st.radio(
            "Load mode for existing table",
            options=["replace", "append", "upsert", "skip_existing"],
            index=["replace", "append", "upsert", "skip_existing"].index(
                job.get("load_mode", "append")
            ),
            horizontal=True,
            key="step3_load_mode",
        )
        job["load_mode"] = new_mode
    else:
        st.success(
            f"Table **[{job.get('schema_name', 'dbo')}].[{job['table_name']}]** does not exist — will be created."
        )


# ── step 4 ─────────────────────────────────────────────────────────────────────

def _step4() -> None:
    st.header("Step 4 — Upload & Progress")

    from web.services.pipeline_service import run_batch_sequential
    from web.services.progress_service import ProgressQueue
    from web.components.progress_tracker import render_progress

    # Start worker on first render of this step
    if st.session_state["etl.worker_thread"] is None:
        pq = ProgressQueue()
        st.session_state["etl.progress_queue"] = pq
        worker = run_batch_sequential(
            jobs=st.session_state["etl.batch_queue"],
            progress_queue=pq,
        )
        st.session_state["etl.worker_thread"] = worker

    pq = st.session_state["etl.progress_queue"]
    worker = st.session_state["etl.worker_thread"]

    all_done = render_progress(
        batch_queue=st.session_state["etl.batch_queue"],
        pq=pq,
        worker_done=not worker.is_alive(),
    )

    if all_done:
        st.divider()
        _render_summary()

        col_csv, col_new = st.columns(2)
        with col_csv:
            _download_audit_log()
        with col_new:
            if st.button("Start new session"):
                _reset_session()
                st.rerun()


def _render_summary() -> None:
    import pandas as pd
    rows = []
    for job in st.session_state["etl.batch_queue"]:
        stats = job.get("stats", {})
        rows.append({
            "File": job["file_source"]["path"].split("/")[-1].split("\\")[-1],
            "Status": job.get("status", "?"),
            "Rows read": stats.get("rows_read", "—"),
            "Inserted": stats.get("rows_inserted", "—"),
            "Dropped": stats.get("rows_dropped", "—"),
            "Updated": stats.get("rows_updated", "—"),
            "Skipped": stats.get("rows_skipped", "—"),
            "Duration (s)": stats.get("elapsed_seconds", "—"),
        })
    st.subheader("Run summary")
    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)


def _download_audit_log() -> None:
    import io
    import csv
    import pandas as pd

    rows = []
    for job in st.session_state["etl.batch_queue"]:
        stats = job.get("stats", {})
        rows.append({
            "job_id": job["job_id"],
            "file": job["file_source"]["path"],
            "destination_type": job["destination"]["type"],
            "table": job.get("table_name", ""),
            "load_mode": job.get("load_mode", ""),
            "status": job.get("status", ""),
            "rows_read": stats.get("rows_read", 0),
            "rows_inserted": stats.get("rows_inserted", 0),
            "rows_updated": stats.get("rows_updated", 0),
            "rows_skipped": stats.get("rows_skipped", 0),
            "elapsed_seconds": stats.get("elapsed_seconds", 0),
        })

    df = pd.DataFrame(rows)
    csv_bytes = df.to_csv(index=False).encode("utf-8")
    st.download_button(
        "Download audit log CSV",
        data=csv_bytes,
        file_name="etl_audit_log.csv",
        mime="text/csv",
    )


def _reset_session() -> None:
    from web.services.file_service import cleanup_session_uploads
    session_id = st.session_state.get("etl.session_id", "")
    cleanup_session_uploads(session_id)

    keys_to_clear = [k for k in st.session_state if k.startswith("etl.")]
    for k in keys_to_clear:
        del st.session_state[k]

    init_session_state()


# ── router ─────────────────────────────────────────────────────────────────────

def main() -> None:
    st.title("ETL Studio")
    _step_header()

    step = st.session_state["etl.step"]
    if step == 1:
        _step1()
    elif step == 2:
        _step2()
    elif step == 3:
        _step3()
    elif step == 4:
        _step4()
    else:
        st.error(f"Unknown step: {step}")


if __name__ == "__main__":
    main()
