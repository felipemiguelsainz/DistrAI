"""Direct psycopg connection for queries that bypass PostgREST timeouts."""

from __future__ import annotations

from functools import lru_cache
from urllib.parse import quote

import psycopg

from core.config import get_settings


def _normalize_url(url: str) -> str:
    if "://" not in url or "@" not in url:
        return url
    scheme, rest = url.split("://", 1)
    auth_host, path = rest.split("/", 1) if "/" in rest else (rest, "")
    auth, host = auth_host.rsplit("@", 1)
    if ":" not in auth:
        return url
    user, pw = auth.split(":", 1)
    if pw.startswith("[") and pw.endswith("]"):
        pw = pw[1:-1]
    return f"{scheme}://{user}:{quote(pw, safe='')}@{host}/{path}"


def get_direct_conn() -> psycopg.Connection:
    """Return a fresh psycopg connection (caller must close it)."""
    url = _normalize_url(get_settings().database_url)
    return psycopg.connect(url)
