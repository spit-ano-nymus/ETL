"""
preview_table.py
----------------
Display a preview DataFrame and optional column statistics.
"""
from __future__ import annotations

import pandas as pd
import streamlit as st


def render_preview_table(df: pd.DataFrame, title: str = "Preview (first 1 000 rows)") -> None:
    """Render a scrollable preview of the DataFrame."""
    st.subheader(title)
    st.caption(f"{len(df):,} rows × {len(df.columns)} columns shown")
    st.dataframe(df, use_container_width=True, height=350)


def render_column_stats(stats_df: pd.DataFrame) -> None:
    """Render the column mining statistics table."""
    st.subheader("Column statistics (mining)")
    st.dataframe(stats_df, use_container_width=True, hide_index=True)
