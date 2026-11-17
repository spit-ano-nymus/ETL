"""
sql_runner.py
-------------
Run a .sql file from the project against the pipeline's PostgreSQL and print
whatever it returns, so no separate SQL client (pgAdmin) is needed:

    python main.py --pipeline config/tables/customer.yaml --sql config/tables/customer_setup.sql

Used for per-table config files (the INSERTs into meta.etl_tables and
meta.etl_column_map), for check queries, and for one-off maintenance SQL.

The file is split into statements so that every SELECT in it prints its own
table. The split respects '' strings, "" identifiers, $$ / $tag$ blocks and
-- and /* */ comments, so function bodies stay in one piece.
"""

from __future__ import annotations

import logging
import re
from pathlib import Path

logger = logging.getLogger(__name__)

_TAG = re.compile(r"\$[A-Za-z_][A-Za-z0-9_]*\$|\$\$")


def split_statements(sql: str) -> list[str]:
    """Split a SQL script on the semicolons that actually end a statement."""
    out: list[str] = []
    buf: list[str] = []
    i, n = 0, len(sql)

    while i < n:
        ch = sql[i]

        # -- line comment
        if sql.startswith("--", i):
            j = sql.find("\n", i)
            j = n if j == -1 else j + 1
            buf.append(sql[i:j])
            i = j
            continue

        # /* block comment */ (nests in Postgres)
        if sql.startswith("/*", i):
            depth, j = 1, i + 2
            while j < n and depth:
                if sql.startswith("/*", j):
                    depth += 1; j += 2
                elif sql.startswith("*/", j):
                    depth -= 1; j += 2
                else:
                    j += 1
            buf.append(sql[i:j])
            i = j
            continue

        # 'string' with '' escapes
        if ch == "'":
            j = i + 1
            while j < n:
                if sql[j] == "'":
                    if j + 1 < n and sql[j + 1] == "'":
                        j += 2
                        continue
                    j += 1
                    break
                j += 1
            buf.append(sql[i:j])
            i = j
            continue

        # "identifier"
        if ch == '"':
            j = sql.find('"', i + 1)
            j = n if j == -1 else j + 1
            buf.append(sql[i:j])
            i = j
            continue

        # $$ ... $$ or $tag$ ... $tag$
        if ch == "$":
            m = _TAG.match(sql, i)
            if m:
                tag = m.group(0)
                end = sql.find(tag, m.end())
                j = n if end == -1 else end + len(tag)
                buf.append(sql[i:j])
                i = j
                continue

        if ch == ";":
            stmt = "".join(buf).strip()
            if stmt:
                out.append(stmt)
            buf = []
            i += 1
            continue

        buf.append(ch)
        i += 1

    stmt = "".join(buf).strip()
    if stmt:
        out.append(stmt)
    return out


def _is_noise(stmt: str) -> bool:
    """True if the statement is only comments / whitespace."""
    s = re.sub(r"/\*.*?\*/", " ", stmt, flags=re.S)
    s = re.sub(r"(?m)--.*$", " ", s)
    return not s.strip()


def _width(text: str) -> int:
    """Printed width, counting a Hebrew/Arabic combining mark as zero."""
    import unicodedata
    return sum(0 if unicodedata.combining(c) else 1 for c in str(text))


def _print_table(columns: list[str], rows: list[tuple]) -> None:
    cells = [[("" if v is None else str(v)) for v in row] for row in rows]
    widths = [max(_width(c), *(_width(r[i]) for r in cells)) if cells else _width(c)
              for i, c in enumerate(columns)]

    def line(values: list[str]) -> str:
        return " | ".join(v + " " * (widths[i] - _width(v)) for i, v in enumerate(values))

    print(line(list(columns)))
    print("-+-".join("-" * w for w in widths))
    for r in cells:
        print(line(r))
    print(f"({len(cells)} row{'s' if len(cells) != 1 else ''})\n")


def run_sql_file(engine, path: str | Path, autocommit: bool = False) -> None:
    """
    Run every statement in `path` and print the result sets.

    One transaction by default: if a statement fails, nothing in the file is
    applied. autocommit=True for statements Postgres refuses to run inside a
    transaction (VACUUM, CREATE INDEX CONCURRENTLY, CREATE DATABASE).
    """
    p = Path(path)
    if not p.is_file():
        raise FileNotFoundError(f"SQL file not found: {p}")

    sql = p.read_text(encoding="utf-8-sig").replace("\r\n", "\n")
    statements = [s for s in split_statements(sql) if not _is_noise(s)]
    if not statements:
        logger.warning("%s has no statements to run", p.name)
        return

    raw = engine.raw_connection()
    try:
        if autocommit:
            raw.autocommit = True
        cur = raw.cursor()
        for k, stmt in enumerate(statements, 1):
            first = " ".join(re.sub(r"(?m)^\s*--.*$", "", stmt).split())[:70]
            logger.info("[%d/%d] %s", k, len(statements), first)
            cur.execute(stmt)

            for note in getattr(raw, "notices", []):
                logger.info(note.strip().removeprefix("NOTICE:").strip())
            if hasattr(raw, "notices"):
                del raw.notices[:]

            if cur.description:                      # a SELECT or RETURNING
                _print_table([d[0] for d in cur.description], cur.fetchall())
            elif cur.rowcount is not None and cur.rowcount >= 0:
                logger.info("%d row(s) affected", cur.rowcount)

        if not autocommit:
            raw.commit()
        logger.info("%s: %d statement(s) done", p.name, len(statements))
    except Exception:
        if not autocommit:
            raw.rollback()
            logger.error("%s failed — nothing in the file was applied", p.name)
        raise
    finally:
        raw.close()
