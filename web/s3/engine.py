"""
web/s3/engine.py
----------------
boto3 session / client factory with per-credential caching.
"""
from __future__ import annotations

from functools import lru_cache


@lru_cache(maxsize=8)
def get_s3_client(access_key: str, secret_key: str, region: str = "us-east-1"):
    """
    Return a cached boto3 S3 client for the given credentials.

    Parameters are used as the cache key, so different credential sets each
    get their own client instance.
    """
    import boto3
    session = boto3.Session(
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region,
    )
    return session.client("s3")
