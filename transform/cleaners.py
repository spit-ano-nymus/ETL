import pandas as pd

from core.registry import register


@register("trim_whitespace")
def trim_whitespace(df: pd.DataFrame, **kwargs) -> pd.DataFrame:
    """Strip leading/trailing whitespace from all string columns."""
    str_cols = df.select_dtypes(include="object").columns
    df = df.copy()
    df[str_cols] = df[str_cols].apply(lambda col: col.str.strip())
    return df


@register("normalize_nulls")
def normalize_nulls(df: pd.DataFrame, **kwargs) -> pd.DataFrame:
    """Replace empty strings and 'nan' string artifacts with None → SQL NULL."""
    df = df.copy()
    df = df.replace("", None)
    df = df.replace("nan", None)
    return df
