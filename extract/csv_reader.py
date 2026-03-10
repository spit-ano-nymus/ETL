"""
csv_reader.py
-------------
Stream a CSV file in chunks, preserving all safety measures from the
original csv_uploader.py:
  - dtype=str + keep_default_na=False  → no silent type coercion
  - on_bad_lines="warn"                → malformed lines skip, don't crash
  - BOM-safe utf-8-sig encoding
  - Column names stripped of whitespace
"""

import logging
from pathlib import Path
from typing import Generator

import pandas as pd

from core.consts import DEFAULT_CHUNK_SIZE, DEFAULT_ENCODING, DEFAULT_SEPARATOR
from utils.file_utils import resolve_path, assert_readable

logger = logging.getLogger(__name__)


def stream_csv(
    path: str,
    separator: str = DEFAULT_SEPARATOR,
    encoding: str = DEFAULT_ENCODING,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
) -> Generator[pd.DataFrame, None, None]:
    """
    Yield DataFrame chunks from a CSV file.

    Parameters
    ----------
    path       : Path to the CSV file.
    separator  : Column delimiter (default: ',').
    encoding   : File encoding (default: 'utf-8-sig' handles BOM).
    chunk_size : Number of rows per chunk.
    """
    file_path = resolve_path(path)
    assert_readable(file_path)

    logger.info("Streaming CSV: %s  (chunk_size=%d)", file_path.name, chunk_size)

    reader = pd.read_csv(
        file_path,
        sep=separator,
        dtype=str,              # read everything as text — no type coercion surprises
        encoding=encoding,
        keep_default_na=False,  # don't let pandas silently convert values to NaN
        on_bad_lines="warn",    # skip malformed lines instead of crashing
        low_memory=False,
        chunksize=chunk_size,
    )

    for chunk_index, chunk in enumerate(reader):
        chunk.columns = chunk.columns.str.strip()
        logger.debug("Yielding chunk %d (%d rows)", chunk_index, len(chunk))
        yield chunk
