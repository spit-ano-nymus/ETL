from pathlib import Path


def resolve_path(path: str) -> Path:
    """Resolve a string path to an absolute Path object."""
    return Path(path).expanduser().resolve()


def assert_readable(path: Path) -> None:
    """Raise if the path does not exist or is not a regular file."""
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")
    if not path.is_file():
        raise ValueError(f"Path is not a file: {path}")
