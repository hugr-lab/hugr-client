"""Connection manager integration — reads connections from ~/.hugr/connections.json.

This module provides read-only access to the connection configuration file
managed by hugr_connection_service (JupyterLab/Hub). The client reads
credentials from the file but never writes to it.
"""

import json
import os
from pathlib import Path


def config_path() -> Path:
    """Return path to connections.json. Respects HUGR_CONFIG_PATH env."""
    env = os.environ.get("HUGR_CONFIG_PATH")
    if env:
        return Path(env)
    return Path.home() / ".hugr" / "connections.json"


def load_config() -> dict:
    """Load connections config from disk. Returns empty config if file missing."""
    p = config_path()
    if not p.exists():
        return {"connections": [], "default": ""}
    return json.loads(p.read_text())


def get_connection(name: str = None) -> dict:
    """Get connection config by name, or default if name is None.

    Raises ValueError if connection not found.
    """
    cfg = load_config()
    if name is None:
        name = cfg.get("default", "")
    if not name:
        raise ValueError(
            f"No default connection configured in {config_path()}. "
            "Set HUGR_URL env var or configure a connection in JupyterLab."
        )
    for c in cfg.get("connections", []):
        if c.get("name") == name:
            return c
    raise ValueError(
        f"Connection '{name}' not found in {config_path()}. "
        f"Available: {[c.get('name') for c in cfg.get('connections', [])]}"
    )
