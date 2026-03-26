"""
action_selector.py
------------------
Render action checkboxes built from the transform registry.
Returns dict: {"cleaning": bool, "validation": bool, "parsing": bool, "mining": bool}
"""
from __future__ import annotations

import streamlit as st


_ACTION_LABELS = {
    "cleaning": "Cleaning  —  trim whitespace, normalize nulls",
    "validation": "Validation  —  required fields, no duplicates",
    "parsing": "Parsing  —  date, email, phone, numeric parsers",
    "mining": "Mining  —  column statistics (null %, unique, min/max/mean)",
}

_ACTION_DEFAULTS = {
    "cleaning": True,
    "validation": False,  # opt-in: only enable if you know which columns are required
    "parsing": False,
    "mining": True,
}


def render_action_selector() -> dict[str, bool]:
    """Render action checkboxes and return current selection."""
    st.subheader("Actions")
    from web.services.transform_service import get_available_actions
    available = get_available_actions()

    selections: dict[str, bool] = {}
    for key, label in _ACTION_LABELS.items():
        # Show note if steps aren't registered
        if key not in ("mining",) and key not in available:
            label += "  _(not available)_"
        selections[key] = st.checkbox(
            label,
            value=_ACTION_DEFAULTS.get(key, False),
            key=f"action_{key}",
        )

    return selections
