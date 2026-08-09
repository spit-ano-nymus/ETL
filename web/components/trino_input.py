"""
trino_input.py
--------------
Streamlit UI for selecting a Trino table as the ETL input source.

Returns a source dict:
  {"mode": "trino", "host", "port", "user", "password", "catalog", "schema", "table", "path"}
or None if the user hasn't finished configuring a table.
"""
from __future__ import annotations

import streamlit as st


# ── on_change callbacks (run before page re-renders) ──────────────────────────

def _fetch_schemas() -> None:
    host = st.session_state.get("trino_host", "")
    port = int(st.session_state.get("trino_port", 8080))
    user = st.session_state.get("trino_user", "")
    password = st.session_state.get("trino_password") or None
    catalog = st.session_state.get("trino_catalog_sel", "")
    if not (host and user and catalog):
        return
    # Clear downstream selections
    for k in ("trino_schema_sel", "trino_table_sel"):
        st.session_state.pop(k, None)
    st.session_state.pop("trino.tables", None)
    from web.services.trino_service import list_schemas
    try:
        st.session_state["trino.schemas"] = list_schemas(host, port, user, catalog, password)
        st.session_state["trino.schema_error"] = None
    except Exception as exc:
        st.session_state["trino.schemas"] = []
        st.session_state["trino.schema_error"] = str(exc)


def _fetch_tables() -> None:
    host = st.session_state.get("trino_host", "")
    port = int(st.session_state.get("trino_port", 8080))
    user = st.session_state.get("trino_user", "")
    password = st.session_state.get("trino_password") or None
    catalog = st.session_state.get("trino_catalog_sel", "")
    schema = st.session_state.get("trino_schema_sel", "")
    if not (host and user and catalog and schema):
        return
    st.session_state.pop("trino_table_sel", None)
    from web.services.trino_service import list_tables
    try:
        st.session_state["trino.tables"] = list_tables(host, port, user, catalog, schema, password)
        st.session_state["trino.table_error"] = None
    except Exception as exc:
        st.session_state["trino.tables"] = []
        st.session_state["trino.table_error"] = str(exc)


# ── main render ───────────────────────────────────────────────────────────────

def render_trino_input() -> dict | None:
    """
    Render the Trino connection form and cascading catalog/schema/table selectors.
    Returns a fully-populated source dict once a table is selected, else None.
    """
    st.caption("Connect to a Trino cluster, then pick a catalog, schema, and table.")

    col_host, col_port = st.columns([3, 1])
    with col_host:
        host = st.text_input(
            "Trino host",
            key="trino_host",
            placeholder="trino.example.com",
        )
    with col_port:
        port = st.number_input(
            "Port",
            key="trino_port",
            value=8080,
            min_value=1,
            max_value=65535,
            step=1,
        )

    col_user, col_pass = st.columns(2)
    with col_user:
        user = st.text_input("User", key="trino_user", placeholder="admin")
    with col_pass:
        st.text_input("Password (optional)", key="trino_password", type="password")

    if st.button("Connect", key="trino_connect_btn", disabled=not (host and user)):
        from web.services.trino_service import list_catalogs
        password = st.session_state.get("trino_password") or None
        try:
            catalogs = list_catalogs(host, int(port), user, password)
            st.session_state["trino.catalogs"] = catalogs
            # Clear all downstream state on reconnect
            for k in ("trino_catalog_sel", "trino_schema_sel", "trino_table_sel"):
                st.session_state.pop(k, None)
            for k in ("trino.schemas", "trino.tables", "trino.schema_error", "trino.table_error"):
                st.session_state.pop(k, None)
            st.success(f"Connected — {len(catalogs)} catalog(s) available.")
        except Exception as exc:
            st.error(f"Connection failed: {exc}")
            return None

    catalogs = st.session_state.get("trino.catalogs")
    if not catalogs:
        return None

    st.selectbox(
        "Catalog",
        catalogs,
        key="trino_catalog_sel",
        on_change=_fetch_schemas,
    )
    catalog = st.session_state.get("trino_catalog_sel", "")

    schemas = st.session_state.get("trino.schemas")
    if st.session_state.get("trino.schema_error"):
        st.error(f"Could not load schemas: {st.session_state['trino.schema_error']}")
    if not schemas:
        return None

    st.selectbox(
        "Schema (database)",
        schemas,
        key="trino_schema_sel",
        on_change=_fetch_tables,
    )
    schema = st.session_state.get("trino_schema_sel", "")

    tables = st.session_state.get("trino.tables")
    if st.session_state.get("trino.table_error"):
        st.error(f"Could not load tables: {st.session_state['trino.table_error']}")
    if not tables:
        return None

    table = st.selectbox("Table", tables, key="trino_table_sel")
    if not table:
        return None

    password_val = st.session_state.get("trino_password") or None

    return {
        "mode": "trino",
        "host": host,
        "port": int(port),
        "user": user,
        "password": password_val,
        "catalog": catalog,
        "schema": schema,
        "table": table,
        "path": f"{catalog}.{schema}.{table}",
    }
