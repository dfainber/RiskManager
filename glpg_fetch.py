"""
glpg_fetch.py
=============
Thin wrapper for querying GLPG-DB01 (PostgreSQL).
Credentials loaded from .env in this directory, then env vars, then fallback.

Usage:
    from glpg_fetch import read_sql
    df = read_sql("SELECT ... FROM ...")
"""

from __future__ import annotations

import os
import threading
from pathlib import Path

import pandas as pd
import psycopg2

try:
    from dotenv import load_dotenv
    load_dotenv(dotenv_path=Path(__file__).parent / ".env", override=False)
except ImportError:
    pass


def _env(name: str, fallback: str) -> str:
    return os.environ.get(name) or fallback


_DB_HOST = _env("GLPG_DB_HOST",     "GLPG-DB01")
_DB_PORT = _env("GLPG_DB_PORT",     "5432")
_DB_NAME = _env("GLPG_DB_NAME",     "DATA_DEV_DB")
_DB_USER = _env("GLPG_DB_USER",     "svc_automation")
_DB_PASS = _env("GLPG_DB_PASSWORD", "")


# Per-thread connection cache — each thread keeps one live connection and reuses it
# across multiple read_sql() calls. Cuts ~50-100ms handshake overhead per query.
_tl = threading.local()


def _get_conn():
    c = getattr(_tl, "conn", None)
    if c is not None and c.closed == 0:
        return c
    c = psycopg2.connect(
        host=_DB_HOST, port=int(_DB_PORT), dbname=_DB_NAME,
        user=_DB_USER, password=_DB_PASS,
        connect_timeout=30,
        options="-c statement_timeout=120000",
    )
    _tl.conn = c
    return c


def read_sql(query: str) -> pd.DataFrame:
    """Execute a SELECT query and return a DataFrame. Reuses a per-thread connection."""
    conn = _get_conn()
    try:
        return pd.read_sql(query, conn)
    except psycopg2.Error:
        # If the connection got poisoned, drop it so next call gets a fresh one.
        try: conn.close()
        except Exception: pass
        _tl.conn = None
        raise
