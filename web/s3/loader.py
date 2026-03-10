"""
web/s3/loader.py
----------------
Load a DataFrame chunk to S3 as a Parquet object.
Mirrors the interface of db/loader.py::bulk_load().
"""
from __future__ import annotations

import io
import logging

import pandas as pd

logger = logging.getLogger(__name__)

_REPLACE_CLEANED: set[str] = set()  # tracks prefixes already cleared this run


def s3_load(
    df: pd.DataFrame,
    bucket: str,
    key_prefix: str,
    client,
    load_mode: str,
    chunk_index: int,
) -> dict:
    """
    Write one DataFrame chunk to S3 as a Parquet file.

    Parameters
    ----------
    df          : chunk to write
    bucket      : S3 bucket name
    key_prefix  : key prefix / "folder" path (no trailing slash needed)
    client      : boto3 S3 client
    load_mode   : "replace" clears the prefix on the first chunk; "append" adds
    chunk_index : chunk sequence number (used for unique key suffix)

    Returns
    -------
    dict with keys: inserted, updated, skipped
    """
    if df.empty:
        return {"inserted": 0, "updated": 0, "skipped": 0}

    prefix = key_prefix.rstrip("/")

    # For replace mode, delete all existing objects at the prefix once
    if load_mode == "replace" and prefix not in _REPLACE_CLEANED:
        _delete_prefix(client, bucket, prefix)
        _REPLACE_CLEANED.add(prefix)

    # Serialise to Parquet in memory
    key = f"{prefix}/part-{chunk_index:05d}.parquet"
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, engine="pyarrow")
    buffer.seek(0)

    client.put_object(Bucket=bucket, Key=key, Body=buffer.getvalue())
    logger.debug("s3_load: wrote %d rows to s3://%s/%s", len(df), bucket, key)

    return {"inserted": len(df), "updated": 0, "skipped": 0}


def _delete_prefix(client, bucket: str, prefix: str) -> None:
    """Delete all objects under `prefix` in the bucket (paginated)."""
    paginator = client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix + "/")

    to_delete = []
    for page in pages:
        for obj in page.get("Contents", []):
            to_delete.append({"Key": obj["Key"]})

    if not to_delete:
        return

    # DeleteObjects accepts at most 1000 keys per request
    for i in range(0, len(to_delete), 1000):
        batch = to_delete[i : i + 1000]
        client.delete_objects(Bucket=bucket, Delete={"Objects": batch})
    logger.info("s3_load: deleted %d existing objects at s3://%s/%s/", len(to_delete), bucket, prefix)
