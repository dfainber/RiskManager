"""
credit_config.py
================
Canonical names, benchmarks, and constants for the credit sub-project.
"""
from __future__ import annotations

from pathlib import Path

# Galápagos logo as base64 — extracted from generate_risk_report.py so the
# credit report uses the exact same brand mark in the "Powered by" footer.
_LOGO_B64_PATH = Path(__file__).resolve().parent / "_galapagos_logo_b64.txt"
GALAPAGOS_LOGO_B64 = _LOGO_B64_PATH.read_text(encoding="ascii").strip() if _LOGO_B64_PATH.exists() else ""

# Inline SVG for the Risk Monitor brand mark (used at the top of every report).
# Same chart-icon used in generate_risk_report.py for visual consistency.
RISK_MONITOR_LOGO_SVG = (
    '<svg class="rm-logo" viewBox="0 0 100 100" xmlns="http://www.w3.org/2000/svg" '
    'aria-label="Risk Monitor">'
    '<defs><linearGradient id="rmGrad" x1="0%" y1="0%" x2="100%" y2="100%">'
    '<stop offset="0%" stop-color="#2a7ec8"/>'
    '<stop offset="100%" stop-color="#0a1f4a"/>'
    '</linearGradient></defs>'
    '<rect width="100" height="100" rx="18" fill="url(#rmGrad)"/>'
    '<path d="M 16 74 L 32 58 L 44 64 L 58 44 L 70 52 L 80 30" '
    'stroke="#5db6f3" stroke-width="7" stroke-linecap="round" '
    'stroke-linejoin="round" fill="none"/>'
    '<path d="M 80 30 L 68 28 M 80 30 L 80 42" '
    'stroke="#5db6f3" stroke-width="7" stroke-linecap="round" fill="none"/>'
    '</svg>'
)

POWERED_BY_FOOTER = (
    '<footer class="powered-by">'
    '<span>Powered by</span>'
    f'<img src="data:image/png;base64,{GALAPAGOS_LOGO_B64}" alt="Galápagos Capital">'
    '</footer>'
) if GALAPAGOS_LOGO_B64 else ''

# Canonical TRADING_DESK names (match LOTE45 tables exactly)
CREDIT_FUNDS = {
    "SEA_LION":  {"trading_desk": "FIC FIDC Galapagos Sea Lion",
                  "anbima_name":  "GALAPAGOS SEA LION FIC FIDC HIGH GRADE INSTITUCIONAL",
                  "benchmark":    "CDI",
                  "label":        "Sea Lion"},
    "IGUANA":    {"trading_desk": "Galapagos Iguana FIRF Liquidez Top CP",
                  "benchmark":    "CDI",
                  "label":        "Iguana"},
    "NAZCA":     {"trading_desk": "Galapagos Nazca FIRF",
                  "benchmark":    "IMA-B",
                  "label":        "Nazca"},
    "PELICAN":   {"trading_desk": "Galapagos Pelican FIRF",
                  "benchmark":    "CDI",
                  "label":        "Pelican"},
    "DRAGON":    {"trading_desk": "Galapagos Dragon FIDC",
                  "benchmark":    "CDI",
                  "label":        "Dragon"},
    "BARBACENA": {"trading_desk": "Barbacena FIRF CP Infraestrutura",
                  "benchmark":    "IMA-B",
                  "label":        "Barbacena"},
}

# Benchmark indices on ECO_INDEX (INSTRUMENT, FIELD)
INDEX_MAP = {
    "CDI":           ("CDI",                          "YIELD"),
    "IMA-B":         ("IMA-B",                        "INDEX"),
    "IDA-Infra":     ("IDA-IPCA_INFRAESTRUTURA",      "INDEX"),
    "IDA-Ex-Infra":  ("IDA-IPCA_EX_INFRAESTRUTURA",   "INDEX"),
    "IDA-DI":        ("IDA-DI",                       "INDEX"),
    "IDA-Geral":     ("IDA-GERAL",                    "INDEX"),
}

# ANBIMA credit-rating curves on MAPS_GLOBAL_CURVES.CHAIN
CREDIT_CURVE_CHAINS = {
    "AAA": "CREDIT_AAA",
    "AA":  "CREDIT_AA",
    "A":   "CREDIT_A",
}

# Tipo de Ativo grouping for the Alocação card (mirrors monthly review)
TIPO_GROUPS = {
    "FIDC":            "Estruturados",
    "FIDC NP":         "Estruturados",
    "Debenture":       "Crédito",
    "Debenture Infra": "Crédito Infra",
    "CRA":             "Crédito",
    "CRI":             "Crédito",
    "NTN-B":           "Títulos Públicos",
    "LFT":             "Títulos Públicos",
    "Cash":            "Caixa",
    "Funds BR":        "Caixa",          # e.g. Pinzon FIRF Ref DI
    "Provisions and Costs": "Custos & Provisões",
}

# PRODUCT_CLASS values from LOTE_BOOK_OVERVIEW that are NOT credit-relevant
EXCLUDE_PRODUCT_CLASSES = ("Provisions and Costs", "Cash")

# Default xlsm path (override via env CREDIT_XLSM_PATH)
import os
DEFAULT_XLSM_PATH = os.environ.get(
    "CREDIT_XLSM_PATH",
    r"C:\Users\diego.fainberg\Downloads\Trading Credito.xlsm",
)
