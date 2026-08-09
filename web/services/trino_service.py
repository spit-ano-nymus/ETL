"""
trino_service.py
----------------
Helpers for connecting to a Trino cluster and reading table data.
Requires the `trino` package (pip install trino).
"""
from __future__ import annotations

from typing import Generator

import pandas as pd


def _connect(
    host: str,
    port: int,
    user: str,
    password: str | None = None,
    catalog: str | None = None,
    schema: str | None = None,
):
    from trino.dbapi import connect

    kwargs: dict = dict(host=host, port=port, user=user)
    if password:
        from trino.auth import BasicAuthentication
        kwargs["auth"] = BasicAuthentication(user, password)
    if catalog:
        kwargs["catalog"] = catalog
    if schema:
        kwargs["schema"] = schema
    return connect(**kwargs)


def list_catalogs(
    host: str, port: int, user: str, password: str | None = None
) -> list[str]:
    conn = _connect(host, port, user, password)
    cur = conn.cursor()
    cur.execute("SHOW CATALOGS")
    return [row[0] for row in cur.fetchall()]


def list_schemas(
    host: str, port: int, user: str, catalog: str, password: str | None = None
) -> list[str]:
    conn = _connect(host, port, user, password, catalog=catalog)
    cur = conn.cursor()
    cur.execute("SHOW SCHEMAS")
    return [row[0] for row in cur.fetchall()]


def list_tables(
    host: str, port: int, user: str, catalog: str, schema: str, password: str | None = None
) -> list[str]:
    conn = _connect(host, port, user, password, catalog=catalog, schema=schema)
    cur = conn.cursor()
    cur.execute("SHOW TABLES")
    return [row[0] for row in cur.fetchall()]


def get_trino_columns(
    host: str,
    port: int,
    user: str,
    catalog: str,
    schema: str,
    table: str,
    password: str | None = None,
) -> list[str]:
    conn = _connect(host, port, user, password, catalog=catalog, schema=schema)
    cur = conn.cursor()
    cur.execute(f'SELECT * FROM "{catalog}"."{schema}"."{table}" LIMIT 0')
    return [desc[0] for desc in cur.description]


def sample_trino_rows(
    host: str,
    port: int,
    user: str,
    catalog: str,
    schema: str,
    table: str,
    n: int = 1000,
    password: str | None = None,
) -> pd.DataFrame:
    conn = _connect(host, port, user, password, catalog=catalog, schema=schema)
    cur = conn.cursor()
    cur.execute(f'SELECT * FROM "{catalog}"."{schema}"."{table}" LIMIT {n}')
    rows = cur.fetchall()
    cols = [desc[0] for desc in cur.description]
    return pd.DataFrame(rows, columns=cols)


def stream_trino_table(
    host: str,
    port: int,
    user: str,
    catalog: str,
    schema: str,
    table: str,
    chunk_size: int = 10_000,
    password: str | None = None,
) -> Generator[pd.DataFrame, None, None]:
    conn = _connect(host, port, user, password, catalog=catalog, schema=schema)
    cur = conn.cursor()
    cur.execute(f'SELECT * FROM "{catalog}"."{schema}"."{table}"')
    cols = [desc[0] for desc in cur.description]
    while True:
        rows = cur.fetchmany(chunk_size)
        if not rows:
            break
        yield pd.DataFrame(rows, columns=cols)
