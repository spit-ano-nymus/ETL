"""
destination_form.py
-------------------
Dynamic credential form for SQL Server and AWS S3 destinations.
Returns a credential dict or None when the form is incomplete.
"""
from __future__ import annotations

import streamlit as st
from web.config import ODBC_DRIVERS


def render_destination_form() -> dict | None:
    """
    Render the destination radio + credential fields.
    Returns {"type": "sqlserver"|"s3", ...credentials} or None.
    """
    dest_type = st.radio(
        "Destination",
        options=["sqlserver", "s3"],
        format_func=lambda x: "SQL Server" if x == "sqlserver" else "AWS S3",
        horizontal=True,
        key="dest_type_radio",
    )
    st.session_state["etl.form.dest_type"] = dest_type

    if dest_type == "sqlserver":
        return _sql_server_form()
    return _s3_form()


def _sql_server_form() -> dict | None:
    col1, col2 = st.columns(2)
    with col1:
        server = st.text_input("Server", key="sql_server", placeholder="localhost\\SQLEXPRESS")
        username = st.text_input("Username", key="sql_user", placeholder="sa")
        driver = st.selectbox("ODBC Driver", ODBC_DRIVERS, key="sql_driver")
    with col2:
        database = st.text_input("Database", key="sql_db", placeholder="MyDatabase")
        password = st.text_input("Password", key="sql_pass", type="password")
        trusted = st.checkbox("Windows Auth (trusted connection)", key="sql_trusted")

    table = st.text_input("Target table name", key="sql_table", placeholder="my_table")
    schema = st.text_input("Schema", key="sql_schema", value="dbo")

    if not server.strip() or not database.strip() or not table.strip():
        return None

    return {
        "type": "sqlserver",
        "server": server.strip(),
        "database": database.strip(),
        "username": username.strip(),
        "password": password,
        "driver": driver,
        "trusted_connection": trusted,
        "table": table.strip(),
        "schema": schema.strip() or "dbo",
    }


def _s3_form() -> dict | None:
    col1, col2 = st.columns(2)
    with col1:
        access_key = st.text_input("AWS Access Key ID", key="s3_access_key")
        region = st.text_input("Region", key="s3_region", value="us-east-1")
    with col2:
        secret_key = st.text_input("AWS Secret Access Key", key="s3_secret_key", type="password")
        bucket = st.text_input("Bucket name", key="s3_bucket")

    key_prefix = st.text_input(
        "Key prefix (folder)",
        key="s3_prefix",
        placeholder="etl-output/my_table",
    )

    if not access_key.strip() or not secret_key.strip() or not bucket.strip():
        return None

    return {
        "type": "s3",
        "access_key": access_key.strip(),
        "secret_key": secret_key,
        "region": region.strip() or "us-east-1",
        "bucket": bucket.strip(),
        "key_prefix": key_prefix.strip() or "etl-output",
    }
