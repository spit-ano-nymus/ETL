"""
column_renamer.py
-----------------
Step 1 sub-component: show every column from the selected file, let the
user type a new name, and optionally drop columns entirely.  Renames are
pre-populated per-table from consts/column_aliases.py.

Returns a tuple (renames, drops):
  - renames: {original_name: new_name} for columns whose name was changed
  - drops: [original_name, ...] for columns checked "Drop"
"""
from __future__ import annotations

import streamlit as st


def render_column_renamer(columns: list[str], table_key: str = "") -> tuple[dict[str, str], list[str]]:
    """
    Render an editable rename/drop table for `columns`.
    `table_key` identifies the source (Trino table name, or file name
    without extension) so the matching block of consts/column_aliases.py
    is used to pre-populate defaults.
    Returns ({original: new_name}, [dropped_original_names]).
    """
    from consts.column_aliases import aliases_for

    aliases = aliases_for(table_key)

    with st.expander("Rename / drop columns (optional)", expanded=False):
        st.caption(
            "Leave a field blank or unchanged to keep the original name. "
            "Check 'Drop' to exclude a column from the destination entirely. "
            "Defaults come from `consts/column_aliases.py`."
            + (f" (source: `{table_key}`)" if table_key else "")
        )

        renames: dict[str, str] = {}
        drops: list[str] = []
        # Show in three-column layout: original | new name input | drop checkbox
        col_a, col_b, col_c = st.columns([2, 2, 1])
        col_a.markdown("**Original name**")
        col_b.markdown("**New name**")
        col_c.markdown("**Drop**")

        for orig in columns:
            default = aliases.get(orig, "")
            c1, c2, c3 = st.columns([2, 2, 1])
            c1.text(orig)
            drop = c3.checkbox(
                label=orig,
                key=f"drop_{orig}_{table_key}",
                label_visibility="collapsed",
            )
            new_name = c2.text_input(
                label=orig,
                value=default,
                placeholder=orig,
                key=f"rename_{orig}_{table_key}",
                label_visibility="collapsed",
                disabled=drop,
            )
            if drop:
                drops.append(orig)
                continue
            stripped = new_name.strip()
            if stripped and stripped != orig:
                renames[orig] = stripped

    return renames, drops
