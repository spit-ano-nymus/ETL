import logging
import re

import pandas as pd

from core.registry import register

logger = logging.getLogger(__name__)

_NON_DIGIT = re.compile(r"\D")


@register("phone_parser")
def phone_parser(
    df: pd.DataFrame,
    columns: list[str],
    region: str = "US",
    **kwargs,
) -> pd.DataFrame:
    """
    Normalize phone numbers.
    Uses the 'phonenumbers' library for E.164 formatting when installed;
    falls back to stripping all non-digit characters.

    Parameters
    ----------
    columns : Column names to normalize.
    region  : Default region for parsing (e.g. "US", "GB"). Used by phonenumbers.
    """
    try:
        import phonenumbers  # noqa: F401
        _parse_fn = _make_phonenumbers_parser(region)
        logger.debug("phone_parser: using phonenumbers library (region=%s)", region)
    except ImportError:
        logger.debug("phone_parser: phonenumbers not installed — using digit-strip fallback")
        _parse_fn = _strip_non_digits

    df = df.copy()
    for col in columns:
        if col not in df.columns:
            logger.warning("phone_parser: column '%s' not found — skipping", col)
            continue
        df[col] = df[col].apply(_parse_fn)
    return df


def _strip_non_digits(value) -> str | None:
    if pd.isna(value) or str(value).strip() == "":
        return None
    digits = _NON_DIGIT.sub("", str(value))
    return digits or None


def _make_phonenumbers_parser(region: str):
    import phonenumbers

    def _parse(value) -> str | None:
        if pd.isna(value) or str(value).strip() == "":
            return None
        try:
            parsed = phonenumbers.parse(str(value), region)
            if phonenumbers.is_valid_number(parsed):
                return phonenumbers.format_number(
                    parsed, phonenumbers.PhoneNumberFormat.E164
                )
            return None
        except Exception:
            return None

    return _parse
