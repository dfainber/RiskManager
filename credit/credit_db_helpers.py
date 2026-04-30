"""
credit_db_helpers.py
====================
Schema Credit DDL + xlsm ingest (Cadastro Ativos, Limites_GrupoEconomico).

Usage:
    python -m credit.credit_db_helpers ddl     # create tables
    python -m credit.credit_db_helpers asset   # ingest Cadastro Ativos
    python -m credit.credit_db_helpers limits  # ingest Limites_GrupoEconomico
    python -m credit.credit_db_helpers all     # all three
"""
from __future__ import annotations

import os
import re
import zipfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Optional

import psycopg2
from psycopg2.extras import execute_values

from .credit_config import DEFAULT_XLSM_PATH

# Reuse env vars (same convention as glpg_fetch.py)
try:
    from dotenv import load_dotenv
    load_dotenv(dotenv_path=Path(__file__).resolve().parent.parent / ".env", override=False)
except ImportError:
    pass

_DB_CONFIG = dict(
    host=os.environ.get("GLPG_DB_HOST", "GLPG-DB01"),
    port=int(os.environ.get("GLPG_DB_PORT", "5432")),
    dbname=os.environ.get("GLPG_DB_NAME", "DATA_DEV_DB"),
    user=os.environ.get("GLPG_DB_USER", "svc_automation"),
    password=os.environ.get("GLPG_DB_PASSWORD", ""),
)


def _connect():
    return psycopg2.connect(connect_timeout=30, **_DB_CONFIG)


# ────────────────────────────────────────────────────────────────────────────
# DDL
# ────────────────────────────────────────────────────────────────────────────

DDL_ASSET_MASTER = """
CREATE TABLE IF NOT EXISTS credit.asset_master (
  ref               TEXT,
  nome_lote45       TEXT PRIMARY KEY,
  isin              TEXT,
  cod_cetip         TEXT,
  tipo_ativo        TEXT,
  classe            TEXT,
  nome_emissor      TEXT,
  apelido_emissor   TEXT,
  cnpj_emissor      TEXT,
  capital_emissor   TEXT,
  setor             TEXT,
  grupo_economico   TEXT,
  fiduciario_adm    TEXT,
  indexador         TEXT,
  taxa_emissao      NUMERIC,
  marcacao          TEXT,
  spread            NUMERIC,
  data_vencimento   DATE,
  duration          NUMERIC,
  rating            TEXT,
  data_call         DATE,
  data_emissao      DATE,
  per_amortizacao   TEXT,
  per_juros         TEXT,
  subordinacao      TEXT,
  garantia_1        TEXT,
  garantia_2        TEXT,
  garantia_3        TEXT,
  covenant_1        TEXT,
  covenant_2        TEXT,
  covenant_3        TEXT,
  loaded_at         TIMESTAMP DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_asset_master_grupo ON credit.asset_master (grupo_economico);
CREATE INDEX IF NOT EXISTS idx_asset_master_setor ON credit.asset_master (setor);
"""

DDL_ISSUER_LIMITS = """
CREATE TABLE IF NOT EXISTS credit.issuer_limits (
  emissor          TEXT,
  fund_name        TEXT,
  limit_pct        NUMERIC,
  limit_text       TEXT,
  analista         TEXT,
  last_approval    DATE,
  renewal_date     DATE,
  obs              TEXT,
  loaded_at        TIMESTAMP DEFAULT NOW(),
  PRIMARY KEY (emissor, fund_name)
);
CREATE INDEX IF NOT EXISTS idx_issuer_limits_fund ON credit.issuer_limits (fund_name);
"""


def create_schema_tables() -> None:
    """Idempotent: creates credit.asset_master and credit.issuer_limits if missing."""
    with _connect() as c, c.cursor() as cur:
        cur.execute(DDL_ASSET_MASTER)
        cur.execute(DDL_ISSUER_LIMITS)
        c.commit()
    print("DDL: credit.asset_master + credit.issuer_limits ready")


# ────────────────────────────────────────────────────────────────────────────
# xlsm parsing (XML-direct — handles 260 MB workbook in seconds)
# ────────────────────────────────────────────────────────────────────────────

def _read_sheet(xlsm_path: str, sheet_idx: int) -> dict[int, dict[str, Any]]:
    """Parse one sheet by index (1-based). Returns {row_num: {col_letter: value}}."""
    with zipfile.ZipFile(xlsm_path, "r") as z:
        with z.open("xl/sharedStrings.xml") as f:
            ss_xml = f.read().decode("utf-8")
        sst: list[str] = []
        for m in re.finditer(r"<si\b[^>]*>(.*?)</si>", ss_xml, re.DOTALL):
            body = m.group(1)
            ts = re.findall(r"<t[^>]*>(.*?)</t>", body, re.DOTALL)
            sst.append("".join(ts))
        with z.open(f"xl/worksheets/sheet{sheet_idx}.xml") as f:
            xml = f.read().decode("utf-8", errors="replace")

    rows_xml = re.findall(r'<row\s+r="(\d+)"[^>]*>(.*?)</row>', xml, re.DOTALL)
    cell_re = re.compile(
        r'<c\s+r="([A-Z]+\d+)"(?:\s+s="\d+")?(?:\s+t="(\w+)")?[^>]*>'
        r'(?:<f[^>]*>.*?</f>)?(?:<v>(.*?)</v>)?',
        re.DOTALL,
    )

    out: dict[int, dict[str, Any]] = {}
    for r_num, row_body in rows_xml:
        row: dict[str, Any] = {}
        for ref, t, v in cell_re.findall(row_body):
            col = re.match(r"([A-Z]+)", ref).group(1)
            if t == "s" and v:
                idx = int(v)
                row[col] = sst[idx] if idx < len(sst) else None
            elif v:
                try:
                    row[col] = float(v)
                except ValueError:
                    row[col] = v
            else:
                row[col] = None
        out[int(r_num)] = row
    return out


# ────────────────────────────────────────────────────────────────────────────
# value coercers
# ────────────────────────────────────────────────────────────────────────────

def _to_text(v: Any) -> Optional[str]:
    if v is None:
        return None
    if isinstance(v, float) and v.is_integer():
        return str(int(v))
    if isinstance(v, (int, float)):
        return str(v)
    s = str(v).strip()
    return s if s else None


def _to_number(v: Any) -> Optional[float]:
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip().rstrip("%").replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return None


def _excel_serial_to_date(v: Any):
    """Excel serial number (or date string) → Python date. Returns None on failure."""
    if v is None:
        return None
    if isinstance(v, str):
        s = v.strip()
        if not s or s in ("-", "—"):
            return None
        for fmt in ("%m/%d/%Y", "%d/%m/%Y", "%Y-%m-%d"):
            try:
                return datetime.strptime(s, fmt).date()
            except ValueError:
                continue
        return None
    if isinstance(v, (int, float)) and v > 0:
        # Excel epoch with the 1900 leap-year bug: serial 1 = 1900-01-01,
        # but for serial > 60 (after the fake Feb 29, 1900), it's days since 1899-12-30.
        return (datetime(1899, 12, 30) + timedelta(days=int(v))).date()
    return None


# ────────────────────────────────────────────────────────────────────────────
# ingest: Cadastro Ativos → credit.asset_master
# ────────────────────────────────────────────────────────────────────────────

ASSET_MASTER_COLS = [
    "ref", "nome_lote45", "isin", "cod_cetip", "tipo_ativo", "classe",
    "nome_emissor", "apelido_emissor", "cnpj_emissor", "capital_emissor",
    "setor", "grupo_economico", "fiduciario_adm", "indexador",
    "taxa_emissao", "marcacao", "spread", "data_vencimento",
    "duration", "rating", "data_call", "data_emissao",
    "per_amortizacao", "per_juros", "subordinacao",
    "garantia_1", "garantia_2", "garantia_3",
    "covenant_1", "covenant_2", "covenant_3",
]


def ingest_asset_master(xlsm_path: str = DEFAULT_XLSM_PATH) -> int:
    """Read 'Cadastro Ativos' (sheet 17) and upsert into credit.asset_master."""
    sheet = _read_sheet(xlsm_path, 17)
    rows: list[tuple] = []
    for rn, rc in sorted(sheet.items()):
        if rn < 2:
            continue
        nome = _to_text(rc.get("B"))
        if not nome:
            continue
        rows.append((
            _to_text(rc.get("A")),                  # ref
            nome,                                   # nome_lote45 (PK)
            _to_text(rc.get("C")),                  # isin
            _to_text(rc.get("D")),                  # cod_cetip
            _to_text(rc.get("E")),                  # tipo_ativo
            _to_text(rc.get("F")),                  # classe
            _to_text(rc.get("G")),                  # nome_emissor
            _to_text(rc.get("H")),                  # apelido_emissor
            _to_text(rc.get("I")),                  # cnpj_emissor
            _to_text(rc.get("J")),                  # capital_emissor
            _to_text(rc.get("K")),                  # setor
            _to_text(rc.get("L")),                  # grupo_economico
            _to_text(rc.get("M")),                  # fiduciario_adm
            _to_text(rc.get("N")),                  # indexador
            _to_number(rc.get("O")),                # taxa_emissao
            _to_text(rc.get("P")),                  # marcacao
            _to_number(rc.get("Q")),                # spread
            _excel_serial_to_date(rc.get("R")),     # data_vencimento
            _to_number(rc.get("S")),                # duration
            _to_text(rc.get("T")),                  # rating
            _excel_serial_to_date(rc.get("U")),     # data_call
            _excel_serial_to_date(rc.get("X")),     # data_emissao
            _to_text(rc.get("Z")),                  # per_amortizacao
            _to_text(rc.get("AA")),                 # per_juros
            _to_text(rc.get("AB")),                 # subordinacao
            _to_text(rc.get("AC")),                 # garantia_1
            _to_text(rc.get("AD")),                 # garantia_2
            _to_text(rc.get("AE")),                 # garantia_3
            _to_text(rc.get("AF")),                 # covenant_1
            _to_text(rc.get("AG")),                 # covenant_2
            _to_text(rc.get("AH")),                 # covenant_3
        ))

    cols_csv = ", ".join(ASSET_MASTER_COLS)
    update_set = ", ".join(
        f"{c}=EXCLUDED.{c}" for c in ASSET_MASTER_COLS if c != "nome_lote45"
    )
    sql = f"""
INSERT INTO credit.asset_master ({cols_csv})
VALUES %s
ON CONFLICT (nome_lote45) DO UPDATE SET
  {update_set},
  loaded_at = NOW()
"""
    with _connect() as c, c.cursor() as cur:
        execute_values(cur, sql, rows, page_size=500)
        c.commit()
    print(f"asset_master: upserted {len(rows)} rows")
    return len(rows)


# ────────────────────────────────────────────────────────────────────────────
# ingest: Limites_GrupoEconomico → credit.issuer_limits (long format)
# ────────────────────────────────────────────────────────────────────────────

# Sheet 16 header maps cols E→L to fund canonical names
LIMITS_FUND_COLS = {
    "E": "Galapagos Dragon FIDC",
    "F": "Galapagos Evo Strategy FIM CP",
    "G": "GALAPAGOS BALTRA ICATU QUALIF PREV FIM CP",
    "H": "Galapagos Iguana FIRF Liquidez Top CP",
    "I": "FIC FIDC Galapagos Sea Lion",
    "J": "Galapagos Nazca FIRF",
    "K": "Barbacena FIRF CP Infraestrutura",
    "L": "Galapagos Pelican FIRF",
}


def _parse_limit_cell(raw: Any) -> tuple[Optional[float], Optional[str]]:
    """Returns (limit_pct, limit_text). Numeric → pct as decimal; string overrides preserved."""
    if raw is None:
        return None, None
    if isinstance(raw, (int, float)):
        return float(raw), None
    s = str(raw).strip()
    if not s or s in ("-", "—"):
        return None, "-"
    # Try '5.0%' style
    has_pct = "%" in s
    s2 = s.rstrip("%").replace(",", ".").strip()
    try:
        v = float(s2)
        if has_pct:
            v = v / 100.0
        return v, None
    except ValueError:
        return None, s


def ingest_issuer_limits(xlsm_path: str = DEFAULT_XLSM_PATH) -> int:
    """Read 'Limites_GrupoEconomico' (sheet 16), unpivot 8 fund cols, upsert."""
    sheet = _read_sheet(xlsm_path, 16)
    rows: list[tuple] = []
    for rn, rc in sorted(sheet.items()):
        if rn < 2:
            continue
        emissor = _to_text(rc.get("A"))
        if not emissor:
            continue
        analista = _to_text(rc.get("B"))
        last_app = _excel_serial_to_date(rc.get("C"))
        renewal = _excel_serial_to_date(rc.get("D"))
        obs = _to_text(rc.get("N"))
        for col, fund in LIMITS_FUND_COLS.items():
            raw = rc.get(col)
            if raw is None:
                continue
            limit_pct, limit_text = _parse_limit_cell(raw)
            if limit_pct is None and limit_text is None:
                continue
            rows.append((emissor, fund, limit_pct, limit_text,
                         analista, last_app, renewal, obs))

    sql = """
INSERT INTO credit.issuer_limits
  (emissor, fund_name, limit_pct, limit_text, analista, last_approval, renewal_date, obs)
VALUES %s
ON CONFLICT (emissor, fund_name) DO UPDATE SET
  limit_pct=EXCLUDED.limit_pct,
  limit_text=EXCLUDED.limit_text,
  analista=EXCLUDED.analista,
  last_approval=EXCLUDED.last_approval,
  renewal_date=EXCLUDED.renewal_date,
  obs=EXCLUDED.obs,
  loaded_at = NOW()
"""
    with _connect() as c, c.cursor() as cur:
        execute_values(cur, sql, rows, page_size=500)
        c.commit()
    print(f"issuer_limits: upserted {len(rows)} rows")
    return len(rows)


# ────────────────────────────────────────────────────────────────────────────
# CLI
# ────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys
    cmd = sys.argv[1] if len(sys.argv) > 1 else "all"
    xlsm = sys.argv[2] if len(sys.argv) > 2 else DEFAULT_XLSM_PATH
    if cmd in ("ddl", "all"):
        create_schema_tables()
    if cmd in ("asset", "all"):
        ingest_asset_master(xlsm)
    if cmd in ("limits", "all"):
        ingest_issuer_limits(xlsm)
