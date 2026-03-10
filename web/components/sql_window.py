"""
sql_window.py
-------------
SQL scratch pad — draft-only text area, no execution.
"""
from __future__ import annotations

import streamlit as st


def render_sql_window(table: str = "", schema: str = "dbo") -> None:
    """Render a read-only SQL scratch pad pre-populated with a SELECT template."""
    placeholder = (
        f"-- Draft your query here (not executed)\n"
        f"SELECT TOP 100 *\n"
        f"FROM [{schema}].[{table}]\n"
        f"WHERE 1=1\n"
        f"ORDER BY 1;"
    ) if table else "-- Draft your query here (not executed)"

    st.subheader("SQL scratch pad  _(draft only — not executed)_")
    st.text_area(
        "SQL",
        value=placeholder,
        height=180,
        key="sql_scratch_pad",
        label_visibility="collapsed",
        help="Copy and run this in your SQL client. Nothing is executed here.",
    )
