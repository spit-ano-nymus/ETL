import logging

import pandas as pd

from core.registry import register

logger = logging.getLogger(__name__)


@register("date_parser")
def date_parser(
    df: pd.DataFrame,
    columns: list[str],
    format: str = None,
    **kwargs,
) -> pd.DataFrame:
    """
    Parse date columns using an optional strptime format string.
    Unparseable values become NaT (null).

    Parameters
    ----------
    columns : Column names to parse.
    format  : strptime format, e.g. "%d/%m/%Y". If None, pandas infers.
    """
    df = df.copy()
    for col in columns:
        if col not in df.columns:
            logger.warning("date_parser: column '%s' not found — skipping", col)
            continue
        df[col] = pd.to_datetime(df[col], format=format, errors="coerce")
    return df
