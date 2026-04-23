"""
risk_config.py — pure data configuration for the Risk Monitor kit.

Fund mandates, limit thresholds, stop-loss formula base values, display
order / labels, and PA key mappings. Only module-level data — no function
calls, no SQL, no I/O — so the module is safe to import from anywhere
(data_fetch, metrics, html_builders, generate_risk_report).
"""

# ── Fund mandates ─────────────────────────────────────────────────────────────
# FUNDS  → funds in LOTE_FUND_STRESS_RPM (has LEVEL=10 fund-level VaR/Stress).
# RAW_FUNDS → funds in LOTE_FUND_STRESS (product-level, SUM by TRADING_DESK).
# IDKA_FUNDS → benchmarked RF in LOTE_PARAMETRIC_VAR_TABLE (BVaR primary).
FUNDS = {
    "Galapagos Macro FIM":           {"short": "MACRO",      "level": 2, "stress_col": "spec",  "var_soft": 2.10, "var_hard": 3.00, "stress_soft": 21.0, "stress_hard": 30.0},
    "Galapagos Quantitativo FIM":    {"short": "QUANT",      "level": 2, "stress_col": "macro", "var_soft": 2.10, "var_hard": 3.00, "stress_soft": 21.0, "stress_hard": 30.0},
    "Galapagos Evolution FIC FIM CP":{"short": "EVOLUTION",  "level": 3, "stress_col": "spec",  "var_soft": 1.75, "var_hard": 2.50, "stress_soft": 10.5, "stress_hard": 15.0},
}
# Funds in LOTE_FUND_STRESS (product-level, not RPM). Limits provisional — to be calibrated.
# informative=True → VaR/Stress shown as reference (no limit, no util %). Used for Frontier (LO equity).
RAW_FUNDS = {
    "GALAPAGOS ALBATROZ FIRF LP": {"short": "ALBATROZ", "stress_col": "macro", "var_soft": 1.0, "var_hard": 1.5, "stress_soft": 5.0, "stress_hard": 8.0},
    "Galapagos Global Macro Q":   {"short": "MACRO_Q",  "stress_col": "spec",  "var_soft": 2.10, "var_hard": 3.00, "stress_soft": 21.0, "stress_hard": 30.0},
    "Frontier A\u00e7\u00f5es FIC FI": {"short": "FRONTIER", "stress_col": "macro", "var_soft": 99.0, "var_hard": 99.0, "stress_soft": 99.0, "stress_hard": 99.0, "informative": True},
}
# IDKA funds — benchmarked RF. Primary metric = BVaR (relative), secondary = VaR (reference, no limit).
# Data source: LOTE45.LOTE_PARAMETRIC_VAR_TABLE (RELATIVE_VAR_PCT, ABSOLUTE_VAR_PCT).
# Shape mirrors FUNDS/RAW_FUNDS but maps: var_* -> BVaR limits, stress_* -> VaR (no hard limit).
IDKA_FUNDS = {
    # Limites BVaR 95% 1-day (daily). Confirmados 2026-04-22 com mandato:
    #   IDKA 3Y  soft = 0.40%
    #   IDKA 10Y soft = 1.00%
    # Hard = 1.5× soft (convenção). Stress sem limite explícito (→ 99 = informativo).
    "IDKA IPCA 3Y FIRF":  {"short": "IDKA_3Y",  "primary": "bvar", "var_soft": 0.40, "var_hard": 0.60, "stress_soft": 99.0, "stress_hard": 99.0},
    "IDKA IPCA 10Y FIRF": {"short": "IDKA_10Y", "primary": "bvar", "var_soft": 1.00, "var_hard": 1.50, "stress_soft": 99.0, "stress_hard": 99.0},
}
ALL_FUNDS = {**FUNDS, **RAW_FUNDS, **IDKA_FUNDS}


# ── Alert / utilization thresholds ────────────────────────────────────────────
ALERT_THRESHOLD = 80.0   # % no range 12m acima do qual gera alerta
UTIL_WARN       = 70.0   # util % abaixo do qual = verde
UTIL_HARD       = 100.0  # util % acima do qual = vermelho (limite atingido)


# ── Stop-loss / carry formula (MACRO PMs) ─────────────────────────────────────
STOP_BASE = 63.0   # base monthly stop (bps)
STOP_SEM  = 128.0  # semi-annual hard stop (bps)
STOP_ANO  = 252.0  # annual hard stop (bps)

# ALBATROZ: fixed 150 bps/month loss budget, no carry.
ALBATROZ_STOP_BPS = 150.0


# ── Display / navigation ──────────────────────────────────────────────────────
REPORTS = [
    ("performance",     "PA"),
    ("exposure",        "Exposure"),
    ("exposure-map",    "Exposure Map"),
    ("single-name",     "Single-Name"),
    ("risk-monitor",    "Risk Monitor"),
    ("diversification", "Diversificação"),
    ("analise",         "Análise"),
    ("distribution",    "Distribuição 252d"),
    ("vol-regime",      "Vol Regime"),
    ("stop-monitor",    "Risk Budget"),
    ("briefing",        "Briefing"),
]
FUND_ORDER  = ["MACRO", "QUANT", "EVOLUTION", "MACRO_Q", "ALBATROZ", "FRONTIER", "IDKA_3Y", "IDKA_10Y"]
FUND_LABELS = {
    "MACRO": "Macro", "QUANT": "Quantitativo", "EVOLUTION": "Evolution",
    "MACRO_Q": "Macro Q", "ALBATROZ": "Albatroz", "FRONTIER": "Frontier",
    "IDKA_3Y": "IDKA 3Y", "IDKA_10Y": "IDKA 10Y",
}


# ── PA key mapping ────────────────────────────────────────────────────────────
# PA key in q_models.REPORT_ALPHA_ATRIBUTION for each fund short.
#
# IDKA calibration vs. xlsx (F:/Macros/DADOS/Projetos/PA_Fundos/PA_REPORTS/YYYY_MM_DD_DAILY_PA.xlsx):
# ---------------------------------------------------------------------------------------------------
# Snapshot 2026-04-17 · IDKA 3Y "DISCRICIONÁRIO" TOTAL row (RF BZ + RF BZ IPCA only, exclui Caixa/Custos):
#   Xlsx:  DIA +7.58 bps · MÊS +85.98 bps · ANO -65.94 bps
#   DB:    DIA +7.58 bps · MÊS +86.17 bps · ANO -61.93 bps
# DIA bate exato. MÊS/ANO com residual de ~2–4 bps — provável rounding de display no xlsx vs floats
# no banco, não problema estrutural. Revisitar se algum dia o gap crescer para > 10 bps.
#
# IDKAs não têm coluna LEVEL em REPORT_ALPHA_ATRIBUTION — apenas leaf rows (87 p/ 3Y, 85 p/ 10Y).
# A hierarquia de níveis vem das colunas dimensionais: CLASSE → GRUPO → SUBCLASSE → LIVRO →
# BOOK → PRODUCT_CLASS → PRODUCT.
_FUND_PA_KEY = {
    "MACRO":     "MACRO",
    "QUANT":     "QUANT",
    "EVOLUTION": "EVOLUTION",
    "MACRO_Q":   "GLOBAL",
    "ALBATROZ":  "ALBATROZ",
    "FRONTIER":  "GFA",
    "IDKA_3Y":   "IDKAIPCAY3",
    "IDKA_10Y":  "IDKAIPCAY10",
}

# Livros that are benchmark-tracking (passive replica) per fund PA key.
# Their dia_bps contribution moves with the index, not with alpha — exclude
# from "≥ 1% NAV" alerts since the index itself is not a risk event.
_PA_BENCH_LIVROS = {
    "IDKAIPCAY3":  {"CI"},   # CI book tracks the IDKA 3Y index
    "IDKAIPCAY10": {"CI"},   # CI book tracks the IDKA 10Y index
}
