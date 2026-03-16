"""Shared helpers for PostgreSQL-backed database tests."""

import os
import uuid
from contextlib import contextmanager
from urllib.parse import urlparse, urlunparse

import fx_sr.db as db_module


def default_postgres_url() -> str:
    return os.environ.get("RESISTANCE_DATABASE_URL", db_module.DEFAULT_POSTGRES_URL)


def _with_db_name(url: str, db_name: str) -> str:
    parsed = urlparse(url)
    return urlunparse((parsed.scheme, parsed.netloc, f"/{db_name}", "", "", ""))


def _admin_url(url: str) -> str:
    parsed = urlparse(url)
    return urlunparse((parsed.scheme, parsed.netloc, "/postgres", "", "", ""))


def _connect_admin(url: str):
    try:
        import psycopg  # type: ignore

        return psycopg.connect(_admin_url(url), autocommit=True)
    except Exception:  # pragma: no cover
        import psycopg2  # type: ignore

        conn = psycopg2.connect(_admin_url(url))
        conn.autocommit = True
        return conn


def _safe_quote(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


@contextmanager
def temporary_test_database():
    """Yield an ephemeral Postgres database URL for isolation."""

    base_url = default_postgres_url()
    base_db = (urlparse(base_url).path or "").strip("/") or "resistance"
    test_db = f"{base_db}_test_{uuid.uuid4().hex}"
    test_url = _with_db_name(base_url, test_db)

    admin = _connect_admin(base_url)
    try:
        with admin.cursor() as cursor:
            cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", (test_db,))
            if cursor.fetchone() is None:
                cursor.execute(f"CREATE DATABASE {_safe_quote(test_db)}")
        yield test_url
    finally:
        with admin.cursor() as cursor:
            cursor.execute(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = %s",
                (test_db,),
            )
            cursor.execute(f'DROP DATABASE IF EXISTS {_safe_quote(test_db)}')
        admin.close()
