"""
engine.py
---------
Engine factory — adapt the import and call below to match your infra library's API.

Common patterns your infra might use:
    from infra.mssql_client import MSSQLClient  ->  MSSQLClient().get_engine()
    from infra.db import get_engine             ->  get_engine()
    from infra import mssql                     ->  mssql.engine()
"""

from functools import lru_cache

# ── ADAPT THIS BLOCK ──────────────────────────────────────────────────────────
try:
    from infra.mssql_client import MSSQLClient  # <-- change to your actual import
    _INFRA_AVAILABLE = True
except ImportError:
    _INFRA_AVAILABLE = False


@lru_cache(maxsize=1)
def _get_engine_from_infra():
    """Return a cached SQLAlchemy engine from your infra library."""
    client = MSSQLClient()       # <-- adapt
    return client.get_engine()   # <-- adapt


def get_engine(connection_string: str | None = None):
    """
    Return a SQLAlchemy engine.

    Parameters
    ----------
    connection_string : optional explicit SQLAlchemy URL.
        When provided, creates a new engine directly from the URL —
        useful for the web UI where credentials are supplied at runtime.
        When omitted, delegates to the infra library (legacy CLI path).

    Supported dialects: mssql+pyodbc, postgresql+psycopg2.
    ``fast_executemany`` is only passed for MSSQL connections (pyodbc feature).
    """
    if connection_string is not None:
        from sqlalchemy import create_engine
        kwargs = {}
        if connection_string.startswith("mssql"):
            kwargs["fast_executemany"] = True
        return create_engine(connection_string, **kwargs)
    return _get_engine_from_infra()
# ─────────────────────────────────────────────────────────────────────────────
