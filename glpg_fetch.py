"""
glpg_fetch.py
=============
Thin wrapper for querying GLPG-DB01 (PostgreSQL).
Credentials are loaded from a .env file next to this module (preferred) or
from process environment variables. Required keys:

    GLPG_DB_HOST, GLPG_DB_PORT, GLPG_DB_NAME, GLPG_DB_USER, GLPG_DB_PASSWORD

If any required key is missing, import fails fast with an explicit error
(no host/user fallbacks — see global security policy).

Usage:
    from glpg_fetch import read_sql
    df = read_sql("SELECT ... FROM ...")
"""

from __future__ import annotations

import os
import sys
import threading
from pathlib import Path

import pandas as pd
import psycopg2

try:
    from dotenv import load_dotenv
    load_dotenv(dotenv_path=Path(__file__).parent / ".env", override=False)
except ImportError:
    pass


_REQUIRED_VARS = ("GLPG_DB_HOST", "GLPG_DB_PORT", "GLPG_DB_NAME", "GLPG_DB_USER", "GLPG_DB_PASSWORD")
_missing = [name for name in _REQUIRED_VARS if not os.environ.get(name)]
if _missing:
    raise RuntimeError(
        "glpg_fetch: missing required env vars: " + ", ".join(_missing) +
        f". Populate {Path(__file__).parent / '.env'} or set them in the process environment."
    )

_DB_HOST = os.environ["GLPG_DB_HOST"]
_DB_PORT = os.environ["GLPG_DB_PORT"]
_DB_NAME = os.environ["GLPG_DB_NAME"]
_DB_USER = os.environ["GLPG_DB_USER"]
_DB_PASS = os.environ["GLPG_DB_PASSWORD"]


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
    except Exception:
        # Any error here may have left the connection in an unusable state
        # (poisoned txn, broken socket, etc.). Drop it so the next call reconnects.
        try:
            conn.close()
        except Exception as close_err:
            print(f"[glpg_fetch] warning: failed to close poisoned conn: {close_err!r}", file=sys.stderr)
        _tl.conn = None
        raise
