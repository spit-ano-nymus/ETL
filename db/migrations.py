"""
migrations.py
-------------
Run the SQL files in sql/ against PostgreSQL, so nobody has to paste them
into pgAdmin.

Rules:
  - files run in name order (001_..., 002_..., 003_...)
  - a file runs if it is new, or if its content changed since it last ran
  - each file runs in its own transaction: it either applies fully or not at all
  - every applied file is recorded in meta.schema_migrations (name, md5,
    time, full content), so Postgres keeps a copy of what was installed

All files in sql/ are written to be safe to run again (IF NOT EXISTS /
CREATE OR REPLACE), so re-running a changed file is harmless.
"""

from __future__ import annotations

import hashlib
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

SQL_DIR = Path(__file__).resolve().parent.parent / "sql"


def _read(path: Path) -> str:
    # utf-8-sig: tolerate a BOM if an editor added one; normalise Windows newlines
    return path.read_text(encoding="utf-8-sig").replace("\r\n", "\n")


def run_migrations(engine, sql_dir: Path = SQL_DIR) -> list[str]:
    """Apply new or changed sql/*.sql files. Returns the names that ran."""
    files = sorted(sql_dir.glob("*.sql"))
    if not files:
        logger.warning("run_migrations: no .sql files found in %s", sql_dir)
        return []

    applied: dict[str, str] = {}
    raw = engine.raw_connection()
    try:
        cur = raw.cursor()
        cur.execute("SELECT to_regclass('meta.schema_migrations') IS NOT NULL")
        if cur.fetchone()[0]:
            cur.execute("SELECT filename, checksum FROM meta.schema_migrations")
            applied = dict(cur.fetchall())
        raw.commit()

        ran: list[str] = []
        for path in files:
            content = _read(path)
            checksum = hashlib.md5(content.encode("utf-8")).hexdigest()
            if applied.get(path.name) == checksum:
                continue

            logger.info("run_migrations: applying %s%s", path.name,
                        " (changed)" if path.name in applied else "")
            try:
                # Plain DBAPI execute with no parameters: the files contain
                # % and : characters that must reach Postgres untouched.
                cur.execute(content)
                # 001 creates meta.schema_migrations, so record after running
                cur.execute(
                    """
                    INSERT INTO meta.schema_migrations (filename, checksum, applied_at, content)
                    VALUES (%s, %s, now(), %s)
                    ON CONFLICT (filename) DO UPDATE
                       SET checksum = EXCLUDED.checksum,
                           applied_at = EXCLUDED.applied_at,
                           content = EXCLUDED.content
                    """,
                    (path.name, checksum, content),
                )
                raw.commit()
            except Exception:
                raw.rollback()
                logger.exception("run_migrations: %s failed — nothing from it was applied", path.name)
                raise
            ran.append(path.name)

        if not ran:
            logger.info("run_migrations: database is up to date (%d file(s))", len(files))
        return ran
    finally:
        raw.close()
