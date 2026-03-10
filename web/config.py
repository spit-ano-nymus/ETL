"""
config.py
---------
Session-state initialisation and page-level constants.
"""
import uuid
import streamlit as st

# ── step labels ───────────────────────────────────────────────────────────────
STEPS = {
    1: "Input & Destination",
    2: "Actions & Batch Review",
    3: "Review",
    4: "Upload & Progress",
}

LOAD_MODES = ["replace", "append", "upsert", "skip_existing"]

ODBC_DRIVERS = [
    "ODBC Driver 18 for SQL Server",
    "ODBC Driver 17 for SQL Server",
    "ODBC Driver 13 for SQL Server",
    "SQL Server",
]

# ── session state schema ───────────────────────────────────────────────────────

def init_session_state() -> None:
    """Initialise all ETL session-state keys with safe defaults."""
    defaults = {
        "etl.step": 1,
        "etl.session_id": str(uuid.uuid4()),
        "etl.batch_queue": [],
        "etl.active_job_id": None,
        "etl.progress_queue": None,
        "etl.worker_thread": None,
        # form scratch state
        "etl.form.file_mode": "path",
        "etl.form.dest_type": "sqlserver",
    }
    for key, default in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = default
