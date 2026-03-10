"""
file_input.py
-------------
Renders the file input widget (server path tab OR upload tab).
Returns a dict: {"mode": "path"|"upload", "path": str} or None if incomplete.
"""
from __future__ import annotations

import streamlit as st


def render_file_input(session_id: str) -> dict | None:
    """
    Render two tabs:
      - Tab 1: path on server
      - Tab 2: file upload (streamed to /tmp)

    Returns a source dict or None if the user hasn't filled in the required field.
    """
    tab_path, tab_upload = st.tabs(["Path on server", "Upload file"])

    with tab_path:
        path_val = st.text_input(
            "File path",
            key="file_input_path",
            placeholder="C:/data/myfile.csv  or  /mnt/shared/data.csv",
            help="Absolute path accessible by the server running Streamlit.",
        )
        if path_val.strip():
            return {"mode": "path", "path": path_val.strip()}

    with tab_upload:
        st.caption("Files ≤ 200 MB recommended for upload. Larger files: use 'Path on server'.")
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

    return None
