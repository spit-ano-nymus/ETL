from dataclasses import dataclass, field

import pandas as pd

from core.registry import register


@dataclass
class ValidationResult:
    valid_df: pd.DataFrame
    invalid_df: pd.DataFrame
    error_count: int
    messages: list[str] = field(default_factory=list)


@register("validate_required")
def validate_required(
    df: pd.DataFrame, columns: list[str], **kwargs
) -> ValidationResult:
    """Quarantine rows where any of the specified columns are null or blank."""
    valid_mask = pd.Series(True, index=df.index)
    messages = []

    for col in columns:
        if col not in df.columns:
            messages.append(f"validate_required: column '{col}' not found — skipping")
            continue
        null_mask = df[col].isna() | (df[col].astype(str).str.strip() == "")
        valid_mask &= ~null_mask

    valid_df = df[valid_mask].copy()
    invalid_df = df[~valid_mask].copy()

    if len(invalid_df):
        messages.append(
            f"validate_required: {len(invalid_df)} row(s) missing required "
            f"field(s) {columns}"
        )

    return ValidationResult(
        valid_df=valid_df,
        invalid_df=invalid_df,
        error_count=len(invalid_df),
        messages=messages,
    )


@register("validate_no_duplicates")
def validate_no_duplicates(
    df: pd.DataFrame, columns: list[str], **kwargs
) -> ValidationResult:
    """Quarantine duplicate rows on the specified columns (keep first occurrence)."""
    dup_mask = df.duplicated(subset=columns, keep="first")

    valid_df = df[~dup_mask].copy()
    invalid_df = df[dup_mask].copy()

    messages = []
    if len(invalid_df):
        messages.append(
            f"validate_no_duplicates: {len(invalid_df)} duplicate row(s) on {columns}"
        )

    return ValidationResult(
        valid_df=valid_df,
        invalid_df=invalid_df,
        error_count=len(invalid_df),
        messages=messages,
    )
