import os

def env_bool(name: str, default: bool = False) -> bool:
    """
    Read boolean from environment variables with common truthy values.
    Accepted truthy: 1, true, yes, y, on (case-insensitive).
    """
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in ("1", "true", "yes", "y", "on")

def env_str(name: str, default: str = "") -> str:
    """Read string from environment variables with default."""
    return os.getenv(name, default)
