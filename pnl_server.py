"""
pnl_server.py — Daily PnL & Peers live monitor server
======================================================
Serves daily_monitor.html + two JSON API endpoints:

    GET /            → daily_monitor.html
    GET /api/pnl     → book_pnl.json (UNC share) or DB fallback
    GET /api/peers   → peers_data.json (UNC share)

Usage:
    python pnl_server.py [--port 5050]
"""

from __future__ import annotations

import argparse
import base64
import json
import logging
import os
from datetime import date
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

from dotenv import load_dotenv

# ── Credentials ──────────────────────────────────────────────────────────────
load_dotenv(Path(__file__).parent / ".env", override=False)

# ── Data file paths (env override or default UNC share) ──────────────────────
BOOK_PNL_PATH = Path(os.environ.get(
    "BOOK_PNL_PATH",
    r"\\fs02\FS_GALAPAGOS\Bloomberg\Quant\Claude_GLPG_Fetch\book_pnl.json",
))
PEERS_PATH = Path(os.environ.get(
    "PEERS_DATA_PATH",
    r"\\fs02\FS_GALAPAGOS\Bloomberg\Quant\Claude_GLPG_Fetch\peers_data.json",
))

THIS_DIR = Path(__file__).parent
GALAPAGOS_LOGO_B64 = THIS_DIR / "credit" / "_galapagos_logo_b64.txt"

# Decode logo once at startup; serve raw bytes on every request.
try:
    _GALAPAGOS_PNG = base64.b64decode(GALAPAGOS_LOGO_B64.read_text(encoding="ascii").strip())
except Exception:
    _GALAPAGOS_PNG = None

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


def _fetch_pnl_from_db() -> dict:
    from data_fetch import fetch_book_pnl
    today = str(date.today())
    data  = fetch_book_pnl(today)
    log.info(f"DB PnL fetched: {len(data.get('funds', {}))} funds, val_date={data.get('val_date')}")
    return data


# ── HTTP handler ──────────────────────────────────────────────────────────────
class Handler(BaseHTTPRequestHandler):

    def do_GET(self):
        path = self.path.split("?")[0]
        if path in ("/", "/index.html"):
            self._serve_file(THIS_DIR / "daily_monitor.html", "text/html; charset=utf-8")
        elif path == "/api/pnl":
            self._serve_pnl()
        elif path == "/api/peers":
            self._serve_peers()
        elif path == "/assets/galapagos.png":
            self._serve_logo()
        else:
            self.send_error(404, "Not found")

    def _serve_logo(self):
        if _GALAPAGOS_PNG is None:
            self.send_error(404, "logo not available")
            return
        self.send_response(200)
        self.send_header("Content-Type", "image/png")
        self.send_header("Content-Length", str(len(_GALAPAGOS_PNG)))
        self.send_header("Cache-Control", "public, max-age=86400")
        self.end_headers()
        self.wfile.write(_GALAPAGOS_PNG)

    def _serve_file(self, fpath: Path, content_type: str):
        try:
            body = fpath.read_bytes()
        except FileNotFoundError:
            self.send_error(404, f"{fpath.name} not found")
            return
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _serve_pnl(self):
        try:
            data = _fetch_pnl_from_db()
            data["_source"] = "db"
        except Exception as e:
            log.error(f"DB PnL fetch failed: {e}")
            self._json_error(str(e))
            return
        self._send_json(data)

    def _serve_peers(self):
        try:
            data = json.loads(PEERS_PATH.read_text(encoding="utf-8"))
            log.info("Peers served from file")
        except Exception as e:
            log.error(f"Peers file not available: {e}")
            self._json_error(str(e))
            return
        self._send_json(data)

    def _send_json(self, data):
        body = json.dumps(data, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)

    def _json_error(self, msg: str):
        body = json.dumps({"error": msg}).encode("utf-8")
        self.send_response(500)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt, *args):
        log.info("[%s] %s", self.address_string(), fmt % args)


# ── Entry point ───────────────────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser(description="PnL & Peers monitor server")
    ap.add_argument("--port", type=int, default=5050)
    args = ap.parse_args()

    log.info(f"Starting PnL server on http://localhost:{args.port}/")
    log.info(f"  PnL file  : {BOOK_PNL_PATH}")
    log.info(f"  Peers file: {PEERS_PATH}")

    server = HTTPServer(("", args.port), Handler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        log.info("Server stopped.")


if __name__ == "__main__":
    main()
