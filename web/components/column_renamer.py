"""
column_renamer.py
-----------------
Step 1 sub-component: show every column from the selected file and let the
user type a new name.  Pre-populated from consts/column_aliases.py.

Returns a dict {original_name: new_name} containing only columns where the
name was actually changed (empty dict = no renames).
"""
from __future__ import annotations

import streamlit as st


def render_column_renamer(columns: list[str]) -> dict[str, str]:
    """
    Render an editable rename table for `columns`.
    Returns {original: new_name} for every column whose name was changed.
    """
    from consts.column_aliases import COLUMN_ALIASES

    with st.expander("Rename columns (optional)", expanded=False):
        st.caption(
            "Leave a field blank or unchanged to keep the original name. "
            "Defaults come from `consts/column_aliases.py`."
        )

        renames: dict[str, str] = {}
        # Show in two-column layout: original | new name input
        col_a, col_b = st.columns(2)
        col_a.markdown("**Original name**")
        col_b.markdown("**New name**")

        for orig in columns:
            default = COLUMN_ALIASES.get(orig, "")
            c1, c2 = st.columns(2)
            c1.text(orig)
            new_name = c2.text_input(
                label=orig,
                value=default,
                placeholder=orig,
                key=f"rename_{orig}",
                label_visibility="collapsed",
            )
            stripped = new_name.strip()
            if stripped and stripped != orig:
                renames[orig] = stripped

    return renames
