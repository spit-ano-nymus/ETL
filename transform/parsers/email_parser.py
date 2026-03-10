import logging
import re

import pandas as pd

from core.registry import register

logger = logging.getLogger(__name__)

_EMAIL_RE = re.compile(r"^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$")


@register("email_parser")
def email_parser(
    df: pd.DataFrame,
    columns: list[str],
    **kwargs,
) -> pd.DataFrame:
    """
    Normalize email addresses: lowercase + strip whitespace.
    Values that don't match a basic email pattern become None.
    """
    df = df.copy()
    for col in columns:
        if col not in df.columns:
            logger.warning("email_parser: column '%s' not found — skipping", col)
            continue
        df[col] = df[col].apply(_normalize_email)
    return df


def _normalize_email(value) -> str | None:
    if pd.isna(value) or str(value).strip() == "":
        return None
    normalized = str(value).strip().lower()
    return normalized if _EMAIL_RE.match(normalized) else None
