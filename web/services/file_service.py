"""
file_service.py
---------------
Resolve file inputs (path on server or uploaded bytes) to a usable path,
then provide sampling + Excel chunking helpers.
"""
import os
import shutil
import tempfile
from pathlib import Path
from typing import Generator

import pandas as pd

from extract.csv_reader import stream_csv
from utils.file_utils import resolve_path, assert_readable


# ── path helpers ──────────────────────────────────────────────────────────────

def save_upload(uploaded_file, session_id: str) -> str:
    """
    Persist a Streamlit UploadedFile to /tmp/etl_uploads/<session_id>/.
    Returns the absolute path string.
    """
    upload_dir = Path(tempfile.gettempdir()) / "etl_uploads" / session_id
    upload_dir.mkdir(parents=True, exist_ok=True)
    dest = upload_dir / uploaded_file.name
    with open(dest, "wb") as f:
        f.write(uploaded_file.getbuffer())
    return str(dest)


def cleanup_session_uploads(session_id: str) -> None:
    """Delete all temp files for a session."""
    upload_dir = Path(tempfile.gettempdir()) / "etl_uploads" / session_id
    if upload_dir.exists():
        shutil.rmtree(upload_dir, ignore_errors=True)


def validate_path(path: str) -> str:
    """Resolve and assert path is readable; return the resolved string path."""
    p = resolve_path(path)
    assert_readable(p)
    return str(p)


# ── preview helpers ───────────────────────────────────────────────────────────

def get_file_columns(path: str) -> list[str]:
    """Return column names from a CSV or Excel file without loading any data rows."""
    p = Path(path)
    if p.suffix.lower() in (".xlsx", ".xls"):
        return list(pd.read_excel(path, nrows=0, dtype=str).columns)
    for chunk in stream_csv(path, chunk_size=1):
        return list(chunk.columns)
    return []


def sample_rows(path: str, n: int = 1000) -> pd.DataFrame:
    """
    Return the first `n` rows of a CSV or Excel file without loading it fully.
    Uses stream_csv for CSV; pd.read_excel with nrows for Excel.
    """
    p = Path(path)
    if p.suffix.lower() in (".xlsx", ".xls"):
        return pd.read_excel(path, nrows=n, dtype=str)

    # CSV path — grab first chunk
    for chunk in stream_csv(path, chunk_size=n):
        return chunk
    return pd.DataFrame()


def stream_file(
    path: str,
    chunk_size: int = 10_000,
) -> Generator[pd.DataFrame, None, None]:
    """
    Yield DataFrame chunks from CSV or Excel.

    Excel files are read in successive windows of `chunk_size` rows so
    peak RAM stays bounded regardless of file size.
    """
    p = Path(path)
    if p.suffix.lower() in (".xlsx", ".xls"):
        yield from _stream_excel(path, chunk_size)
    else:
        yield from stream_csv(path, chunk_size=chunk_size)


def _stream_excel(path: str, chunk_size: int) -> Generator[pd.DataFrame, None, None]:
    """Read an Excel file in fixed-size windows via skiprows/nrows."""
    offset = 0
    # Read header once to know column names
    header_df = pd.read_excel(path, nrows=0, dtype=str)
    columns = list(header_df.columns)

    while True:
        chunk = pd.read_excel(
            path,
            skiprows=range(1, offset + 1),  # skip header + already-read rows
            nrows=chunk_size,
            header=0 if offset == 0 else None,
            names=columns if offset > 0 else None,
            dtype=str,
        )
        if chunk.empty:
            break
        chunk.columns = [str(c).strip() for c in chunk.columns]
        yield chunk
        if len(chunk) < chunk_size:
            break
        offset += chunk_size


# ── column stats (mining) ─────────────────────────────────────────────────────

def compute_column_stats(df: pd.DataFrame) -> pd.DataFrame:
    """
    Return a summary DataFrame with per-column stats:
    null_pct, unique_count, min, max, mean, most_frequent.
    """
    records = []
    for col in df.columns:
        s = df[col]
        null_pct = round(s.isna().mean() * 100, 2)
        unique_count = s.nunique(dropna=True)
        most_frequent = s.mode(dropna=True).iloc[0] if not s.mode(dropna=True).empty else None

        numeric = pd.to_numeric(s, errors="coerce")
        if numeric.notna().any():
            col_min = numeric.min()
            col_max = numeric.max()
            col_mean = round(numeric.mean(), 4)
        else:
            col_min = s.dropna().min() if not s.dropna().empty else None
            col_max = s.dropna().max() if not s.dropna().empty else None
            col_mean = None

        records.append({
            "column": col,
            "null_%": null_pct,
            "unique": unique_count,
            "min": col_min,
            "max": col_max,
            "mean": col_mean,
            "most_frequent": most_frequent,
        })
    return pd.DataFrame(records)
