import logging

import pandas as pd

from core.registry import register

logger = logging.getLogger(__name__)


@register("numeric_parser")
def numeric_parser(
    df: pd.DataFrame,
    columns: list[str],
    thousands_sep: str = ",",
    decimal_sep: str = ".",
    **kwargs,
) -> pd.DataFrame:
    """
    Parse numeric columns: remove thousands separator, convert to float.
    Unparseable values become NaN.

    Parameters
    ----------
    columns       : Column names to parse.
    thousands_sep : Thousands grouping character to strip (default: ',').
    decimal_sep   : Decimal separator (default: '.'). Non-'.' values are
                    replaced with '.' before conversion.
    """
    df = df.copy()
    for col in columns:
        if col not in df.columns:
            logger.warning("numeric_parser: column '%s' not found — skipping", col)
            continue
        series = df[col].astype(str).str.strip()
        if thousands_sep:
            series = series.str.replace(thousands_sep, "", regex=False)
        if decimal_sep != ".":
            series = series.str.replace(decimal_sep, ".", regex=False)
        df[col] = pd.to_numeric(series, errors="coerce")
    return df
