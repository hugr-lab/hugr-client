"""Precedence: env (HUGR_URL/HUGR_TOKEN) must win over ~/.hugr/connections.json.

Regression for the headless-runtime 401: when a host has a
connections.json with a default (e.g. a stale browser/OIDC connection),
HugrClient() used to silently prefer that file over the injected
HUGR_URL/HUGR_TOKEN env, so every call 401'd. Env must win; the
connection file is the fallback only when no url/env is provided.
"""
import json

from hugr.client import HugrClient


def _write_connections(tmp_path):
    cfg = {
        "default": "stale",
        "connections": [
            {
                "name": "stale",
                "url": "http://connfile:9999/ipc",
                "auth_type": "api_key",
                "api_key": "stale-key",
            }
        ],
    }
    p = tmp_path / "connections.json"
    p.write_text(json.dumps(cfg))
    return p


def test_env_wins_over_connections_file(tmp_path, monkeypatch):
    """HUGR_URL/HUGR_TOKEN env take precedence over a default connection."""
    monkeypatch.setenv("HUGR_CONFIG_PATH", str(_write_connections(tmp_path)))
    monkeypatch.setenv("HUGR_URL", "http://from-env:15004/ipc")
    monkeypatch.setenv("HUGR_TOKEN", "fresh-jwt")
    monkeypatch.delenv("HUGR_API_KEY", raising=False)

    c = HugrClient()
    assert c._url == "http://from-env:15004/ipc"
    assert c._token == "fresh-jwt"
    # the stale connection-file credentials must NOT leak in
    assert c._api_key != "stale-key"


def test_connections_file_used_when_no_env(tmp_path, monkeypatch):
    """With no url/env, fall back to the default connection in the file."""
    monkeypatch.setenv("HUGR_CONFIG_PATH", str(_write_connections(tmp_path)))
    monkeypatch.delenv("HUGR_URL", raising=False)
    monkeypatch.delenv("HUGR_TOKEN", raising=False)
    monkeypatch.delenv("HUGR_API_KEY", raising=False)

    c = HugrClient()
    assert c._url == "http://connfile:9999/ipc"
    assert c._api_key == "stale-key"


def test_explicit_url_arg_wins(tmp_path, monkeypatch):
    """An explicit url= still beats both env and the connection file."""
    monkeypatch.setenv("HUGR_CONFIG_PATH", str(_write_connections(tmp_path)))
    monkeypatch.setenv("HUGR_URL", "http://from-env:15004/ipc")

    c = HugrClient(url="http://explicit:1/ipc", token="t")
    assert c._url == "http://explicit:1/ipc"
    assert c._token == "t"
