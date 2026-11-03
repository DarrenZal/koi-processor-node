"""Shared DB configuration for router v0.

The router only permits the local personal_koi target (localhost, port 5432,
database personal_koi); ensure_safe_dsn() refuses anything else.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping
from urllib.parse import quote, urlparse

DEFAULT_PERSONAL_ENV = Path("/Users/example/projects/RegenAI/koi-processor/config/personal.env")
DEFAULT_DSN = "postgresql://localhost:5432/personal_koi"
SAFE_HOSTS = frozenset({"localhost", "127.0.0.1"})
SAFE_DB_NAME = "personal_koi"
SAFE_PORT = 5432


class RouterConfigError(RuntimeError):
    """Raised when router DB configuration fails closed."""


@dataclass(frozen=True)
class ResolvedDsn:
    dsn: str
    source: str


def resolve_dsn(environ: Mapping[str, str] | None = None, env_path: Path = DEFAULT_PERSONAL_ENV) -> ResolvedDsn:
    env = dict(os.environ if environ is None else environ)

    if env.get("ROUTER_PG_DSN"):
        return ResolvedDsn(env["ROUTER_PG_DSN"], "ROUTER_PG_DSN")
    if env.get("PERSONAL_KOI_DSN"):
        return ResolvedDsn(env["PERSONAL_KOI_DSN"], "PERSONAL_KOI_DSN")

    file_values = _read_env_file(env_path)
    if file_values:
        host = file_values.get("POSTGRES_HOST", "localhost")
        port = file_values.get("POSTGRES_PORT", "5432")
        db = file_values.get("POSTGRES_DB", "personal_koi")
        user = file_values.get("POSTGRES_USER")
        password = file_values.get("POSTGRES_PASSWORD")
        auth = ""
        if user:
            auth = quote(user)
            if password:
                auth += f":{quote(password)}"
            auth += "@"
        return ResolvedDsn(f"postgresql://{auth}{host}:{port}/{db}", str(env_path))

    return ResolvedDsn(DEFAULT_DSN, "default")


def ensure_safe_dsn(dsn: str) -> None:
    parsed = urlparse(dsn)
    if parsed.scheme not in {"postgresql", "postgres"}:
        raise RouterConfigError("DSN safety check FAILED: unsupported scheme")
    if parsed.hostname not in SAFE_HOSTS:
        raise RouterConfigError(f"DSN safety check FAILED: host {parsed.hostname!r} is not local")
    if (parsed.port or SAFE_PORT) != SAFE_PORT:
        raise RouterConfigError(f"DSN safety check FAILED: port {parsed.port!r} is not {SAFE_PORT}")
    db_name = parsed.path.lstrip("/")
    if db_name != SAFE_DB_NAME:
        raise RouterConfigError(f"DSN safety check FAILED: database {db_name!r} is not {SAFE_DB_NAME}")


def connect(dsn: str | None = None):
    resolved = ResolvedDsn(dsn, "argument") if dsn else resolve_dsn()
    ensure_safe_dsn(resolved.dsn)

    import psycopg2

    return psycopg2.connect(resolved.dsn)


def _read_env_file(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}

    values: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip().strip("\"'")
    return values
