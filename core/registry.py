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
