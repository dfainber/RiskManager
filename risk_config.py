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
    "GALAPAGOS ALBATROZ FIRF LP":                 {"short": "ALBATROZ", "stress_col": "macro", "var_soft": 1.0,  "var_hard": 1.5,  "stress_soft": 5.0,  "stress_hard": 8.0},
    "Galapagos Global Macro Q":                   {"short": "MACRO_Q",  "stress_col": "spec",  "var_soft": 2.10, "var_hard": 3.00, "stress_soft": 21.0, "stress_hard": 30.0},
    "Frontier A\u00e7\u00f5es FIC FI":            {"short": "FRONTIER", "stress_col": "macro", "var_soft": 99.0, "var_hard": 99.0, "stress_soft": 99.0, "stress_hard": 99.0, "informative": True},
    # Limites provis\u00f3rios \u2014 confirmar mandato (fundo prev multimercado, duration real ~3-4 anos)
    "Galapagos Baltra Icatu Qualif Prev FIM CP":  {"short": "BALTRA",   "stress_col": "macro", "var_soft": 1.75, "var_hard": 2.50, "stress_soft": 12.6, "stress_hard": 18.0},
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
    ("peers",           "Peers"),
]
FUND_ORDER  = ["MACRO", "QUANT", "EVOLUTION", "MACRO_Q", "ALBATROZ", "BALTRA", "FRONTIER", "IDKA_3Y", "IDKA_10Y"]
FUND_LABELS = {
    "MACRO": "Macro", "QUANT": "Quantitativo", "EVOLUTION": "Evolution",
    "MACRO_Q": "Macro Q", "ALBATROZ": "Albatroz", "BALTRA": "Baltra",
    "FRONTIER": "Frontier", "IDKA_3Y": "IDKA 3Y", "IDKA_10Y": "IDKA 10Y",
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
    "BALTRA":    "BALTRA",
    "FRONTIER":  "GFA",
    "IDKA_3Y":   "IDKAIPCAY3",
    "IDKA_10Y":  "IDKAIPCAY10",
}

# Peers group key per fund (None = no peers comparison available).
# QUANT uses the same MACRO peer group; MACRO_Q uses EVOLUTION.
_FUND_PEERS_GROUP: dict[str, str | None] = {
    "MACRO":     "MACRO",
    "QUANT":     "MACRO",
    "EVOLUTION": "EVOLUTION",
    "MACRO_Q":   "GLOBAL",
    "ALBATROZ":  "ALBATROZ",
    "BALTRA":    None,
    "FRONTIER":  "FRONTIER",
    "IDKA_3Y":   None,
    "IDKA_10Y":  None,
}

# Livros that are benchmark-tracking (passive replica) per fund PA key.
# Their dia_bps contribution moves with the index, not with alpha — exclude
# from "≥ 1% NAV" alerts since the index itself is not a risk event.
_PA_BENCH_LIVROS = {
    "IDKAIPCAY3":  {"CI"},            # CI book tracks the IDKA 3Y index
    "IDKAIPCAY10": {"CI"},            # CI book tracks the IDKA 10Y index
    "BALTRA":      {"Benchmark_IDKA", "Caixa", "Caixa USD"},  # passive tracking + cash books
}


# ── IDKA / benchmark wiring ───────────────────────────────────────────────────
_IDKA_BENCH_INSTRUMENT = {
    "IDKA IPCA 3Y FIRF":  "IDKA_IPCA_3A",
    "IDKA IPCA 10Y FIRF": "IDKA_IPCA_10A",
}


# ── MACRO exposure factor taxonomy ────────────────────────────────────────────
_RF_ORDER  = ["RF-BZ","RF-DM","RF-EM","FX-BRL","FX-DM","FX-EM",
              "RV-BZ","RV-DM","RV-EM","COMMODITIES","P-Metals"]
_RF_COLOR  = {"RF-BZ":"#60a5fa","RF-DM":"#93c5fd","RF-EM":"#bfdbfe",
              "FX-BRL":"#a78bfa","FX-DM":"#c4b5fd","FX-EM":"#ddd6fe",
              "RV-BZ":"#34d399","RV-DM":"#6ee7b7","RV-EM":"#a7f3d0",
              "COMMODITIES":"#fb923c","P-Metals":"#fbbf24"}
_EXCL_PRIM = {"Cash", "Provisions and Costs", "Margin"}
_RATE_PRIM = {"Brazil Sovereign Yield", "BRL Rate Curve", "BRD Rate Curve"}

# MACRO PM shorthand → canonical LIVRO name in REPORT_ALPHA_ATRIBUTION.
_PM_LIVRO = {"CI": "CI", "LF": "Macro_LF", "JD": "Macro_JD", "RJ": "Macro_RJ", "QM": "Macro_QM"}


# ── ETF → index composition list (for look-through explosion) ─────────────────
_ETF_TO_LIST = {"BOVA11": "IBOV", "SMAL11": "SMLLBV"}


# ── QUANT book → factor taxonomy ──────────────────────────────────────────────
# QUANT exposure classification: BOOK → risk factor
_QUANT_BOOK_FACTOR = {
    "Bracco":            "Equity BR",
    "Quant_PA":          "Equity BR",
    "SIST_COMMO":        "Commodities",
    "SIST_FX":           "FX",
    "SIST_RF":           "Juros Nominais",
    "SIST_GLOBAL":       "FX",         # fallback (tipically FX/global factor)
    "HEDGE_SISTEMATICO": "Equity BR",  # default; override via primitive below
    "Caixa":             "CDI",
}

_QUANT_FACTOR_ORDER = [
    "Equity BR", "FX", "Commodities",
    "Juros Nominais", "Juros Reais (IPCA)", "CDI", "Outros",
]

# QUANT BOOK → factor mapping for VaR aggregation (LEVEL=3 is by BOOK).
_QUANT_VAR_BOOK_FACTOR = {
    "Bracco":      "Equity BR",
    "Quant_PA":    "Equity BR",
    "SIST_COMMO":  "Commodities",
    "SIST_FX":     "FX",
    "SIST_RF":     "Juros Nominais",
    "SIST_GLOBAL": "FX",
    "Caixa":       "CDI",
    "CAIXA USD":   "CDI",
}


# ── RF exposure-map buckets & factor classification ───────────────────────────
# Factor classification by PRODUCT_CLASS. Real = IPCA-linked (sens to real rates).
# Nominal = pre-fixed rate. CDI = floating (no rate sensitivity). Other = everything else.
_RF_FACTOR_MAP = {
    # Real rates (IPCA-linked)
    "NTN-B": "real", "NTN-C": "real",
    "DAP Future": "real", "DAPFuture": "real", "DAC Future": "real",
    # Nominal rates (pre)
    "DI1Future": "nominal", "NTN-F": "nominal", "LTN": "nominal",
    # CDI / floating
    "LFT": "cdi", "Cash": "cdi", "Cash BRL": "cdi",
    "Overnight": "cdi", "Compromissada": "cdi",
}

# Maturity buckets: 6m, then 1-yr blocks to concentrate allocation granularity
# key → (lower_yrs, upper_yrs, label)
_RF_BUCKETS = [
    ("0-6m",   0.0,  0.5),
    ("6-12m",  0.5,  1.0),
    ("1-2y",   1.0,  2.0),
    ("2-3y",   2.0,  3.0),
    ("3-4y",   3.0,  4.0),
    ("4-5y",   4.0,  5.0),
    ("5-6y",   5.0,  6.0),
    ("6-7y",   6.0,  7.0),
    ("7-8y",   7.0,  8.0),
    ("8-9y",   8.0,  9.0),
    ("9-10y",  9.0, 10.0),
    ("10y+",  10.0, 99.0),
]


# ── EVOLUTION strategy taxonomy (look-through) ────────────────────────────────
_EVO_STRATEGY_ORDER = ["MACRO", "SIST", "FRONTIER", "CREDITO", "EVO_STRAT", "CAIXA", "OUTROS"]
_EVO_STRATEGY_COLOR = {
    "MACRO":     "#60a5fa",
    "SIST":      "#a78bfa",
    "FRONTIER":  "#34d399",
    "CREDITO":   "#fbbf24",
    "EVO_STRAT": "#fb923c",
    "CAIXA":     "#64748b",
    "OUTROS":    "#94a3b8",
}

# LIVROs that don't appear in the skill's livros-map.json — mapped here so
# they roll up into the right strategy. Keeps the JSON canonical (extracted
# from the skill) while still covering actual Evolution positions.
_EVO_LIVRO_EXTRA_STRATEGY = {
    "AÇÕES BR LONG":            "EVO_STRAT",
    "RV BZ":                    "EVO_STRAT",
    "Crédito":                  "CREDITO",
    "GIS FI FUNDO IMOBILIÁRIO": "CREDITO",
    "GIS CUSTOS E PROVISÕES":   "CAIXA",
    "Caixa":                    "CAIXA",
    "Caixa USD":                "CAIXA",
    "Taxas e Custos":           "CAIXA",
}

# Strategies the EVOLUTION diversification card expects to render, in order.
_EVO_EXPECTED_STRATS = ["MACRO", "SIST", "FRONTIER", "CREDITO", "EVO_STRAT", "CAIXA"]


# ── Distribution 252d / Vol regime portfolios ─────────────────────────────────
# PORTIFOLIO names → label + scope used when joining with REPORT_ALPHA_ATRIBUTION
# Scope kind: 'fund' (sum all MACRO), 'livro' (LIVRO = value), 'rf' (books where _parse_rf == value)
_DIST_PORTFOLIOS = [
    # (portfolio_name, label, kind, key, fund_short)
    ("MACRO",       "MACRO total",        "fund",  "MACRO",    "MACRO"),
    ("EVOLUTION",   "EVOLUTION total",    "fund",  "EVOLUTION","EVOLUTION"),
    ("SIST",        "QUANT total",        "fund",  "QUANT",    "QUANT"),
    ("CI",          "CI (Comitê)",        "livro", "CI",       "MACRO"),
    ("LF",          "LF — Luiz Felipe",   "livro", "Macro_LF", "MACRO"),
    ("JD",          "JD — Joca Dib",      "livro", "Macro_JD", "MACRO"),
    ("RJ",          "RJ — Rodrigo Jafet", "livro", "Macro_RJ", "MACRO"),
    ("QM",          "QM",                 "livro", "Macro_QM", "MACRO"),
    ("RF-BZ",       "Fator · RF-BZ",      "rf",    "RF-BZ",    "MACRO"),
    ("RF-DM",       "Fator · RF-DM",      "rf",    "RF-DM",    "MACRO"),
    ("RF-EM",       "Fator · RF-EM",      "rf",    "RF-EM",    "MACRO"),
    ("FX-BRL",      "Fator · FX-BRL",     "rf",    "FX-BRL",   "MACRO"),
    ("FX-DM",       "Fator · FX-DM",      "rf",    "FX-DM",    "MACRO"),
    ("FX-EM",       "Fator · FX-EM",      "rf",    "FX-EM",    "MACRO"),
    ("RV-BZ",       "Fator · RV-BZ",      "rf",    "RV-BZ",    "MACRO"),
    ("RV-DM",       "Fator · RV-DM",      "rf",    "RV-DM",    "MACRO"),
    ("RV-EM",       "Fator · RV-EM",      "rf",    "RV-EM",    "MACRO"),
    ("COMMODITIES", "Fator · COMMO",      "rf",    "COMMODITIES","MACRO"),
    ("P-Metals",    "Fator · P-Metals",   "rf",    "P-Metals", "MACRO"),
    # QUANT sub-books (livros = sub-books, not PMs — reused the "livro" kind for drill-down layout)
    ("SIST_RF",     "Sub · RF",           "livro", "SIST_RF",     "QUANT"),
    ("SIST_FX",     "Sub · FX",           "livro", "SIST_FX",     "QUANT"),
    ("SIST_COMMO",  "Sub · Commo",        "livro", "SIST_COMMO",  "QUANT"),
    ("SIST_GLOBAL", "Sub · Global",       "livro", "SIST_GLOBAL", "QUANT"),
    ("Bracco",      "Bracco",             "livro", "Bracco",      "QUANT"),
    ("Quant_PA",    "Quant PA",           "livro", "Quant_PA",    "QUANT"),
    # FRONTIER — realized alpha vs IBOV series (no HS simulation available).
    # Series is fetched separately from frontier.LONG_ONLY_DAILY_REPORT_MAINBOARD.
    ("FRONTIER",    "FRONTIER α vs IBOV", "fund",  "FRONTIER",    "FRONTIER"),
    # IDKAs — realized active return (fund − benchmark) series.
    # Series fetched separately via fetch_idka_active_series.
    ("IDKA_3Y",     "IDKA 3Y α vs Bench",  "fund",  "IDKA_3Y",     "IDKA_3Y"),
    ("IDKA_10Y",    "IDKA 10Y α vs Bench", "fund",  "IDKA_10Y",    "IDKA_10Y"),
    # IDKA replication benchmark — DV-matched NTN-B portfolio at current date × historical prices.
    ("IDKA_3Y_REP",  "vs Replication", "fund", "IDKA_3Y",  "IDKA_3Y_REP"),
    ("IDKA_10Y_REP", "vs Replication", "fund", "IDKA_10Y", "IDKA_10Y_REP"),
    # IDKA comparison — benchmark, replication, and spread (bench − repl) in one table.
    ("IDKA_3Y",        "vs Benchmark",        "fund", "IDKA_3Y",  "IDKA_3Y_CMP"),
    ("IDKA_3Y_REP",    "vs Replication",      "fund", "IDKA_3Y",  "IDKA_3Y_CMP"),
    ("IDKA_3Y_SPREAD", "Repl − Bench (spread)","fund", "IDKA_3Y",  "IDKA_3Y_CMP"),
    ("IDKA_10Y",        "vs Benchmark",        "fund", "IDKA_10Y", "IDKA_10Y_CMP"),
    ("IDKA_10Y_REP",    "vs Replication",      "fund", "IDKA_10Y", "IDKA_10Y_CMP"),
    ("IDKA_10Y_SPREAD", "Repl − Bench (spread)","fund", "IDKA_10Y", "IDKA_10Y_CMP"),
    # ALBATROZ — HS gross return (PORTIFOLIO_DAILY_HISTORICAL_SIMULATION key 'ALBATROZ').
    ("ALBATROZ", "ALBATROZ total", "fund", "ALBATROZ", "ALBATROZ"),
]

# Vol regime portfolios per fund (subset of _DIST_PORTFOLIOS with fund+livro kinds).
# _VR_PORTFOLIOS is built by referencing _DIST_PORTFOLIOS, so it must come after.
_VR_PORTFOLIOS = {
    # fund_short -> list of (pkey, label, kind). kind in {"fund","livro"}.
    # Scope = fund total + books/PMs only (factor breakdown excluded).
    "MACRO": [
        (pk, lbl, kd) for pk, lbl, kd, _, fs in _DIST_PORTFOLIOS
        if fs == "MACRO" and kd in ("fund", "livro")
    ],
    "EVOLUTION": [("EVOLUTION", "EVOLUTION total", "fund")],
    "QUANT": [
        ("SIST",        "QUANT total",  "fund"),
        ("SIST_RF",     "Sub · RF",     "livro"),
        ("SIST_FX",     "Sub · FX",     "livro"),
        ("SIST_COMMO",  "Sub · Commo",  "livro"),
        ("SIST_GLOBAL", "Sub · Global", "livro"),
        ("Bracco",      "Bracco",       "livro"),
        ("Quant_PA",    "Quant PA",     "livro"),
    ],
}


# ── Alert commentary (headline text per (metric, fund_short)) ─────────────────
ALERT_COMMENTS = {
    ("var",    "MACRO"):     "VaR MACRO no percentil elevado do ano — verificar concentração de posições e se houve aumento intencional de risco.",
    ("var",    "QUANT"):     "VaR QUANT próximo do máximo histórico recente — confirmar se expansão é consistente com o regime de mercado.",
    ("var",    "EVOLUTION"): "VaR EVOLUTION alto em termos históricos. Fundo multi-estratégia — revisar qual book está puxando (FRONTIER, CREDITO ou MACRO interno).",
    ("stress", "MACRO"):     "Stress MACRO acima do 80° percentil — cenário de stress testado está materializando-se? Revisar posição de maior contribuição.",
    ("stress", "QUANT"):     "Stress QUANT elevado no histórico. Verificar concentração em classes sistemáticas (RF, FX, RV) que dominam o cenário de stress.",
    ("stress", "EVOLUTION"): "Stress EVOLUTION alto. Atenção especial à parcela de crédito (cotas júnior) — marcação pode subestimar perda real no cenário de stress.",
}


# ── Fund short → TRADING_DESK name (for LOTE_PRODUCT_EXPO lookups) ────────────
_FUND_DESK_FOR_EXPO = {
    "MACRO":     "Galapagos Macro FIM",
    "QUANT":     "Galapagos Quantitativo FIM",
    "EVOLUTION": "Galapagos Evolution FIC FIM CP",
    "MACRO_Q":   "Galapagos Global Macro Q",
    "ALBATROZ":  "GALAPAGOS ALBATROZ FIRF LP",
    "FRONTIER":  "Frontier A\u00e7\u00f5es FIC FI",
}

# PRODUCT_CLASS → higher-level risk factor. None = non-directional (Cash, LFT) — excluded.
_PRODCLASS_TO_FACTOR = {
    "DI1Future": "Pré BZ", "NTN-F": "Pré BZ", "LTN": "Pré BZ",
    "NTN-B": "IPCA BZ", "DAP Future": "IPCA BZ", "DAPFuture": "IPCA BZ",
    "NTN-C": "IGP-M BZ", "DAC Future": "IGP-M BZ",
    "Equity": "Equity BR", "Equity Receipts": "Equity BR",
    "Equity Options": "Equity BR", "IBOVSPFuture": "Equity BR",
    "ETF BR": "Equity BR",
    "ADR": "ADR", "ADR Options": "ADR", "ADR Receipts": "ADR",
    "USTreasury": "Rates DM", "USTreasuryFuture": "Rates DM", "BondFuture": "Rates DM",
    "CommodityFuture": "Commodities", "CommodityOption": "Commodities",
    "Currencies Forward": "FX", "USDBRLFuture": "FX", "Currencies": "FX",
    "ExchangeFuture": "FX", "FXOption": "FX",
    # Non-directional (excluded)
    "LFT": None, "Cash": None, "Margin": None, "Provisions and Costs": None,
    "Overnight": None, "Compromissada": None, "ETF BR RF": None,
}


# ── PA display / ordering ─────────────────────────────────────────────────────
# PA display renames — strip "Macro_" prefix on PM names
_PA_LIVRO_RENAMES = {
    "Macro_JD": "JD", "Macro_LF": "LF", "Macro_RJ": "RJ",
    "Macro_MD": "MD", "Macro_QM": "QM", "Macro_AC": "AC",
    "Macro_DF": "DF", "Macro_FG": "FG",
}
# Rows that should always sit at the bottom of their sibling group and never be sorted.
_PA_PINNED_BOTTOM = {"Caixa", "Caixa USD", "Taxas e Custos", "Custos", "Caixa & Custos"}

# Default display orders (per level type). Lower = earlier.
# Names not found fall back to |YTD| desc.
_PA_ORDER_CLASSE = {
    "RF BZ": 10, "RF BZ IPCA": 11, "RF BZ IGP-M": 12, "RV BZ": 13,
    "RF Intl": 20, "RV Intl": 21,
    "Commodities": 32, "ETF Options": 33,
    "FX Basis Risk & Carry": 35,
    # Legacy keys kept p/ retrocompat caso o remap não esteja ativo
    "BRLUSD": 30, "FX": 31,
    "Credito": 50, "Crédito": 50, "CRÉDITO": 50, "CredEstr": 51,
}
_PA_ORDER_LIVRO = {
    # Macro PMs — prefer renamed, but also accept raw
    "CI": 100,
    "JD": 101, "Macro_JD": 101, "LF": 102, "Macro_LF": 102,
    "RJ": 103, "Macro_RJ": 103, "MD": 104, "Macro_MD": 104,
    "QM": 105, "Macro_QM": 105, "AC": 106, "Macro_AC": 106,
    "DF": 107, "Macro_DF": 107, "FG": 108, "Macro_FG": 108,
    # Quant
    "Bracco": 200, "Quant_PA": 201,
    "SIST_GLOBAL": 210, "SIST_COMMO": 211, "SIST_FX": 212, "SIST_RF": 213,
    "HEDGE_SISTEMATICO": 220,
    # Equities
    "FMN": 300, "FCO": 301, "FLO": 302, "GIS FI FUNDO IMOBILIÁRIO": 303,
    # Crédito
    "Crédito": 500, "CRÉDITO": 500, "CredEstr": 501,
    # Prev (Albatroz)
    "Prev": 600,
    # RV BZ sub-book inside LIVRO
    "RV BZ": 400,
}
_PA_ORDER_STRATEGY = {"Macro": 1, "Quant": 2, "Equities": 3, "Crédito": 4}

# Mapping from level column name → order dict
_PA_ORDER_BY_LEVEL = {
    "CLASSE":   _PA_ORDER_CLASSE,
    "LIVRO":    _PA_ORDER_LIVRO,
    "STRATEGY": _PA_ORDER_STRATEGY,
}

# Number of aggregated fields per PA node: dia, mtd, ytd, m12, position_brl.
_PA_AGG_LEN = 5
