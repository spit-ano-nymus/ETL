"""
file_input.py
-------------
Renders the input-source selector.
Returns a dict describing the chosen source, or None if not yet configured.

Source dict shapes:
  {"mode": "path",   "path": str}
  {"mode": "upload", "path": str}
  {"mode": "trino",  "host", "port", "user", "password", "catalog", "schema", "table", "path"}
"""
from __future__ import annotations

import streamlit as st


def render_file_input(session_id: str) -> dict | None:
    source_type = st.radio(
        "Input source",
        ["Path on server", "Upload file", "Trino"],
        key="file_input_source_type",
        horizontal=True,
    )

    st.divider()

    if source_type == "Path on server":
        path_val = st.text_input(
            "File path",
            key="file_input_path",
            placeholder="C:/data/myfile.csv  or  /mnt/shared/data.csv",
            help="Absolute path accessible by the server running Streamlit.",
        )
        if path_val.strip():
            return {"mode": "path", "path": path_val.strip()}

    elif source_type == "Upload file":
        st.caption("Files ≤ 200 MB recommended. Larger files: use 'Path on server'.")
        uploaded = st.file_uploader(
            "Upload CSV or Excel",
            type=["csv", "xlsx", "xls"],
            key="file_input_upload",
        )
        if uploaded is not None:
            from web.services.file_service import save_upload
            saved_path = save_upload(uploaded, session_id)
            st.success(f"Saved to: `{saved_path}`")
            return {"mode": "upload", "path": saved_path}

    elif source_type == "Trino":
        from web.components.trino_input import render_trino_input
        return render_trino_input()

    return None
