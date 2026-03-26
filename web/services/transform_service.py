"""
transform_service.py
--------------------
Apply the user-selected action groups to a DataFrame chunk via the registry.
"""
import logging

import pandas as pd

from core.registry import resolve_step

# Ensure all transforms are registered by importing their modules
import transform.cleaners  # noqa: F401
import transform.validators  # noqa: F401
import transform.parsers.date_parser  # noqa: F401
import transform.parsers.email_parser  # noqa: F401
import transform.parsers.phone_parser  # noqa: F401
import transform.parsers.numeric_parser  # noqa: F401

logger = logging.getLogger(__name__)

# Mapping from UI action group name → list of registered transform names
ACTION_STEPS: dict[str, list[str]] = {
    "cleaning": ["trim_whitespace", "normalize_nulls"],
    "validation": ["validate_required", "validate_no_duplicates"],
    # Parsers require explicit column lists; they are skipped unless columns provided
    "parsing": ["date_parser", "email_parser", "phone_parser", "numeric_parser"],
}


def get_available_actions() -> dict[str, list[str]]:
    """Return the action-group → step-names mapping (filters to registered only)."""
    from core.registry import _REGISTRY
    result: dict[str, list[str]] = {}
    for group, steps in ACTION_STEPS.items():
        available = [s for s in steps if s in _REGISTRY]
        if available:
            result[group] = available
    return result


def apply_selected_transforms(
    df: pd.DataFrame,
    actions: dict[str, bool],
    columns: list[str] | None = None,
) -> pd.DataFrame:
    """
    Apply enabled action groups to `df` in order.

    Parameters
    ----------
    df      : input chunk
    actions : {"cleaning": bool, "validation": bool, "parsing": bool, "mining": bool}
    columns : optional column list for validators that need them

    Returns
    -------
    Transformed DataFrame (invalid rows from validators are dropped).
    """
    from core.registry import _REGISTRY
    result = df.copy()

    for group, steps in ACTION_STEPS.items():
        if not actions.get(group, False):
            continue
        for step_name in steps:
            if step_name not in _REGISTRY:
                logger.debug("Step '%s' not registered — skipping", step_name)
                continue
            fn, _ = resolve_step(step_name)
            try:
                kwargs = {}
                # validate_required: skip entirely when no columns specified (avoids wiping all rows)
                if step_name == "validate_required":
                    if not columns:
                        logger.debug("validate_required skipped — no required columns specified")
                        continue
                    kwargs["columns"] = columns
                # validate_no_duplicates: full-row dedup is safe even without explicit columns
                elif step_name == "validate_no_duplicates":
                    kwargs["columns"] = columns or list(result.columns)
                # Parsers require explicit column lists — skip if none provided
                elif step_name in ("date_parser", "email_parser", "phone_parser", "numeric_parser"):
                    if not columns:
                        logger.debug("Step '%s' skipped — no columns specified", step_name)
                        continue
                    kwargs["columns"] = columns
                out = fn(result, **kwargs)
                # Validators return a ValidationResult dataclass
                if hasattr(out, "valid_df"):
                    if out.error_count:
                        logger.info(
                            "Step '%s': %d invalid rows removed",
                            step_name, out.error_count,
                        )
                    result = out.valid_df
                else:
                    result = out
            except Exception as exc:
                logger.warning("Step '%s' failed: %s", step_name, exc)

    return result
