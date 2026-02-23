"""
Base database setup scaffold.

Purpose:
  - Centralize DB configuration parsing.
  - Return a connection factory by dialect.
  - Keep runtime ready for future PostgreSQL/MSSQL adapters.

For now:
  - SQLite connector is implemented.
  - Other dialects are explicit stubs.
"""

from __future__ import annotations

import os
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from enum import Enum
from typing import Iterator, Mapping, Optional
from urllib.parse import unquote, urlparse


class DatabaseConfigError(RuntimeError):
    """Raised when DB env configuration is missing/invalid."""


class DatabaseDialect(str, Enum):
    SQLITE = "sqlite"
    POSTGRESQL = "postgresql"
    MSSQL = "mssql"


@dataclass(frozen=True)
class DatabaseSettings:
    url: str
    dialect: DatabaseDialect
    sqlite_path: Optional[str] = None


class ConnectionFactory:
    """Base connection-factory contract."""

    dialect: DatabaseDialect

    @contextmanager
    def connect(self) -> Iterator[object]:
        raise NotImplementedError

    def healthcheck(self) -> None:
        raise NotImplementedError


@dataclass
class SQLiteConnectionFactory(ConnectionFactory):
    settings: DatabaseSettings

    def __post_init__(self) -> None:
        if not self.settings.sqlite_path:
            raise DatabaseConfigError("SQLite DATABASE_URL is missing a file path.")
        self.dialect = DatabaseDialect.SQLITE

    @contextmanager
    def connect(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self.settings.sqlite_path)
        conn.row_factory = sqlite3.Row
        try:
            conn.execute("PRAGMA foreign_keys = ON;")
            conn.execute("PRAGMA busy_timeout = 5000;")
            conn.execute("PRAGMA journal_mode = WAL;")
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def healthcheck(self) -> None:
        with self.connect() as conn:
            conn.execute("SELECT 1;").fetchone()


@dataclass
class DeferredConnectionFactory(ConnectionFactory):
    settings: DatabaseSettings

    def __post_init__(self) -> None:
        self.dialect = self.settings.dialect

    @contextmanager
    def connect(self) -> Iterator[object]:
        raise NotImplementedError(
            f"Dialect '{self.settings.dialect.value}' is configured but not implemented yet.",
        )
        yield  # pragma: no cover

    def healthcheck(self) -> None:
        raise NotImplementedError(
            f"Dialect '{self.settings.dialect.value}' is configured but not implemented yet.",
        )


def get_database_url_from_env(env: Optional[Mapping[str, str]] = None) -> Optional[str]:
    """
    Resolve DB URL from environment.

    Precedence:
      1. DATABASE_URL
      2. OLJ_DB_PATH -> converted to sqlite:/// URL
    """
    env_map = env if env is not None else os.environ
    database_url = (env_map.get("DATABASE_URL") or "").strip()
    if database_url:
        return database_url

    legacy_path = (env_map.get("OLJ_DB_PATH") or "").strip()
    if legacy_path:
        normalized = legacy_path.replace("\\", "/")
        return f"sqlite:///{normalized}"

    return None


def parse_database_url(database_url: str) -> DatabaseSettings:
    parsed = urlparse(database_url)
    scheme = (parsed.scheme or "").lower()

    if scheme == "sqlite":
        sqlite_path = _extract_sqlite_path(parsed.path)
        return DatabaseSettings(url=database_url, dialect=DatabaseDialect.SQLITE, sqlite_path=sqlite_path)

    if scheme.startswith("postgres"):
        return DatabaseSettings(url=database_url, dialect=DatabaseDialect.POSTGRESQL)

    if scheme.startswith("mssql"):
        return DatabaseSettings(url=database_url, dialect=DatabaseDialect.MSSQL)

    raise DatabaseConfigError(
        "Unsupported DATABASE_URL scheme. Use sqlite:///, postgresql://, or mssql+pyodbc://.",
    )


def build_connection_factory_from_env(
    env: Optional[Mapping[str, str]] = None,
    require_config: bool = False,
) -> Optional[ConnectionFactory]:
    database_url = get_database_url_from_env(env)
    if not database_url:
        if require_config:
            raise DatabaseConfigError(
                "No database configured. Set DATABASE_URL (or OLJ_DB_PATH for sqlite fallback).",
            )
        return None

    settings = parse_database_url(database_url)
    if settings.dialect == DatabaseDialect.SQLITE:
        return SQLiteConnectionFactory(settings=settings)
    return DeferredConnectionFactory(settings=settings)


def _extract_sqlite_path(raw_path: str) -> str:
    path = unquote(raw_path or "")
    if not path:
        raise DatabaseConfigError("SQLite DATABASE_URL is missing a file path.")

    # Windows drive URLs look like /D:/...
    if path.startswith("/") and len(path) >= 3 and path[2] == ":":
        return path[1:]
    return path
