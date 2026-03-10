"""
batch_queue.py
--------------
Render the batch queue as a read-only st.dataframe summary table.
"""
from __future__ import annotations

import pandas as pd
import streamlit as st

_STATUS_ICONS = {
    "pending": "⏳",
    "previewing": "🔍",
    "running": "▶",
    "done": "✅",
    "error": "❌",
}


def render_batch_queue(batch_queue: list[dict]) -> None:
    """Render the queued jobs as a summary table."""
    if not batch_queue:
        st.info("No jobs queued yet.")
        return

    rows = []
    for job in batch_queue:
        dest = job["destination"]
        dest_label = (
            f"SQL Server / {dest.get('database', '')}:{dest.get('table', job.get('table_name', ''))}"
            if dest["type"] == "sqlserver"
            else f"S3 / {dest.get('bucket', '')}:{dest.get('key_prefix', '')}"
        )
        actions_on = [k for k, v in job.get("actions", {}).items() if v]
        status = job.get("status", "pending")
        rows.append({
            "Job": job["job_id"][:8],
            "File": job["file_source"]["path"].split("/")[-1].split("\\")[-1],
            "Destination": dest_label,
            "Actions": ", ".join(actions_on) if actions_on else "—",
            "Load mode": job.get("load_mode", "append"),
            "Status": f"{_STATUS_ICONS.get(status, '')} {status}",
        })

    df = pd.DataFrame(rows)
    st.dataframe(df, use_container_width=True, hide_index=True)
