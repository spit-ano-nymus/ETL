from typing import Callable, Union

_REGISTRY: dict[str, Callable] = {}


def register(name: str):
    """Decorator that registers a transform function under the given name."""
    def decorator(fn: Callable) -> Callable:
        _REGISTRY[name] = fn
        return fn
    return decorator


def resolve_step(step_config: Union[str, dict]) -> tuple[Callable, dict]:
    """
    Resolve a step config to (function, params).

    Accepts either:
      - a string  → name with no params
      - a dict    → {"name": "...", "params": {...}}
    """
    if isinstance(step_config, str):
        name, params = step_config, {}
    else:
        name = step_config["name"]
        params = step_config.get("params", {})

    if name not in _REGISTRY:
        raise KeyError(
            f"Transform step '{name}' is not registered. "
            f"Available: {sorted(_REGISTRY)}"
        )

    return _REGISTRY[name], params


# ── column functions (one value in, one value out) ────────────────────────────
# Used by the one-click pipeline: a column in the final table can name a Python
# function here, for work SQL cannot do (calling an API, for example).
# See transform/column_functions.py.

_COLUMN_FNS: dict[str, Callable] = {}


def register_column_fn(name: str):
    """
    Decorator registering a column function under `name`.

    The function takes the distinct values of one column and returns the new
    value for each:

        @register_column_fn("my_fn")
        def my_fn(values: list[str]) -> dict[str, str]:
            return {v: v.upper() for v in values}

    A list of the same length and order as `values` is accepted too. Results
    are cached per distinct value in meta.py_func_cache, so the function is
    called only for values the pipeline has not seen before.
    """
    def decorator(fn: Callable) -> Callable:
        _COLUMN_FNS[name] = fn
        return fn
    return decorator


def resolve_column_fn(name: str) -> Callable:
    """Look up a column function by name, with a helpful error if it is missing."""
    import transform.column_functions  # noqa: F401  (registers the built-ins)

    if name not in _COLUMN_FNS:
        raise KeyError(
            f"Python column function '{name}' is not registered. "
            f"Available: {sorted(_COLUMN_FNS)}. "
            f"Add it with @register_column_fn('{name}') in transform/column_functions.py."
        )
    return _COLUMN_FNS[name]
