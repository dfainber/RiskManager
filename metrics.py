"""
metrics.py — pure statistical/risk computation helpers.

Takes data (dicts, dataframes, numpy arrays) and produces summary
metrics. No DB access except where a compute also needs a secondary
fetch (frontier BVaR pulls price history, IDKA BVaR pulls NAV cota —
both go through read_sql, not through the fetch_* layer, since they
are single-purpose and self-contained).
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from glpg_fetch import read_sql
from risk_runtime import DATA_STR
from risk_config import _IDKA_BENCH_INSTRUMENT


def compute_pm_hs_var(dist_map: dict,
                       windows: tuple = (21, 63, 252),
                       z: float = 1.645) -> dict[str, dict]:
    """Parametric 1d VaR per PM, computed by fitting σ to the Historical
       Simulation W series (z·σ, default z=1.645 → 95%) — NOT the empirical
       quantile of W. For per-position outliers based on σ of implied returns,
       use compute_pa_outliers instead. The HS source already represents
       TODAY's portfolio × N historical factor-return days.
       Source: dist_map (PORTIFOLIO_DAILY_HISTORICAL_SIMULATION via fetch_pnl_distribution).
       For each PM (PORTFOLIO key in dist_map ∈ {"CI","LF","JD","RJ"}):
         σ_N  = sample std of W[-N:]  (W already in bps of NAV)
         VaR_N = z × σ_N
       Also reports the worst simulated day (magnitude, bps) across the full window.
       Returns {pm_code: {"v21": float, "v63": float, "v252": float, "worst": float, "n": int}}.
    """
    out: dict[str, dict] = {}
    if not dist_map:
        return out
    for pm in ("CI", "LF", "JD", "RJ"):
        w = dist_map.get(pm)
        if w is None or len(w) < 21:
            continue
        w = np.asarray(w, dtype=float)
        w = w[~np.isnan(w)]
        if len(w) < 21:
            continue
        row: dict = {"n": int(len(w))}
        for N in windows:
            if len(w) >= N:
                sigma = float(np.std(w[-N:], ddof=1))
                row[f"v{N}"] = z * sigma
            else:
                row[f"v{N}"] = float("nan")
        wmin = float(np.min(w))
        row["worst"] = -wmin if wmin < 0 else 0.0
        out[pm] = row
    return out


def compute_frontier_bvar_hs(df_frontier: pd.DataFrame, date_str: str = DATA_STR,
                              window_days: int = 756) -> dict | None:
    """Frontier BVaR 95% 1d vs. IBOV via historical simulation on current weights.
       window_days ≈ 3 business years. Returns None if data is insufficient.
    """
    if df_frontier is None or df_frontier.empty:
        return None

    stocks = df_frontier[df_frontier["BOOK"].astype(str).str.strip() != ""].copy()
    stocks = stocks[~stocks["PRODUCT"].isin(["TOTAL", "SUBTOTAL"])]
    stocks = stocks[stocks["% Cash"].notna()][["PRODUCT", "% Cash"]]
    stocks = stocks.rename(columns={"PRODUCT": "ticker", "% Cash": "weight"})
    if stocks.empty:
        return None
    weights = dict(zip(stocks["ticker"], stocks["weight"].astype(float)))

    tks = ",".join(f"'{t}'" for t in list(weights.keys()) + ["IBOV"])
    q = f"""
    SELECT "INSTRUMENT", "DATE", "CLOSE"
    FROM public."EQUITIES_PRICES"
    WHERE "INSTRUMENT" IN ({tks})
      AND "DATE" >= DATE '{date_str}' - INTERVAL '{window_days + 120} days'
      AND "DATE" <= DATE '{date_str}'
    """
    df = read_sql(q)
    if df.empty or "IBOV" not in df["INSTRUMENT"].unique():
        return None
    df["DATE"] = pd.to_datetime(df["DATE"])
    wide = (df.pivot_table(index="DATE", columns="INSTRUMENT",
                           values="CLOSE", aggfunc="last")
              .sort_index())
    wide = wide.dropna(subset=["IBOV"])  # align to IBOV trading days
    wide = wide.ffill()                   # carry last known close across non-trading days
    rets = wide.pct_change().dropna(subset=["IBOV"])
    # EQUITIES_PRICES.CLOSE is raw (non-split-adjusted). Drop corporate-action jumps
    # by zeroing per-stock returns with |r| > 30% (B3 individual circuit breakers
    # cap real daily moves well below this — anything larger is a data artifact).
    stock_cols = [c for c in rets.columns if c != "IBOV"]
    mask_ca = rets[stock_cols].abs() > 0.30
    rets[stock_cols] = rets[stock_cols].mask(mask_ca, 0.0)
    rets = rets.tail(window_days)
    if len(rets) < 50:
        return None

    fund_ret = pd.Series(0.0, index=rets.index)
    for t, w in weights.items():
        if t in rets.columns:
            fund_ret = fund_ret + w * rets[t].fillna(0.0)
    er = fund_ret - rets["IBOV"]

    return {
        "bvar_pct":    -float(er.quantile(0.05)) * 100,  # 1d 95% BVaR, % of NAV
        "n_obs":       int(len(er)),
        "window_days": int(window_days),
        "mean_er_pct": float(er.mean()) * 100,
        "std_er_pct":  float(er.std())  * 100,
    }


def compute_idka_bvar_hs(desk: str, date_str: str = DATA_STR,
                          window_days: int = 756,
                          bench_lag_bdays: int = 2) -> dict | None:
    """IDKA realized HS BVaR 95% 1d vs. its IDKA index benchmark.
       Uses fund cota (SHARE from LOTE_TRADING_DESKS_NAV_SHARE — flow-adjusted)
       minus IDKA index daily returns over up to `window_days` trading days.
       Fund-of-cota returns reflect historical positioning, which for replica
       funds should be close to current positioning.

       ``bench_lag_bdays`` shifts the benchmark return by N trading days to
       align the active-return axis with the fund's cotização calendar. IDKAs
       use D-2 cotização: SHARE on date D reflects portfolio activity through
       D-2 close, while the IDKA index on D moves D-1 → D. Without the shift,
       σ(active) inflates ~10–30% vs the engine's RELATIVE_VAR_PCT. Default 2
       bdays matches IDKA admin convention; pass 0 for funds with same-day
       cotização.
    """
    bench = _IDKA_BENCH_INSTRUMENT.get(desk)
    if not bench:
        return None

    q_fund = f"""
    SELECT "VAL_DATE" AS date, "SHARE"
    FROM "LOTE45"."LOTE_TRADING_DESKS_NAV_SHARE"
    WHERE "TRADING_DESK" = '{desk}'
      AND "VAL_DATE" >= DATE '{date_str}' - INTERVAL '{window_days + 200} days'
      AND "VAL_DATE" <= DATE '{date_str}'
      AND "SHARE" IS NOT NULL
    ORDER BY "VAL_DATE"
    """
    q_bench = f"""
    SELECT "DATE" AS date, "VALUE"
    FROM public."ECO_INDEX"
    WHERE "INSTRUMENT" = '{bench}' AND "FIELD" = 'INDEX'
      AND "DATE" >= DATE '{date_str}' - INTERVAL '{window_days + 200} days'
      AND "DATE" <= DATE '{date_str}'
    ORDER BY "DATE"
    """
    df_f = read_sql(q_fund)
    df_b = read_sql(q_bench)
    if df_f.empty or df_b.empty:
        return None

    df_f["date"] = pd.to_datetime(df_f["date"])
    df_b["date"] = pd.to_datetime(df_b["date"])
    df_f = df_f.drop_duplicates("date").set_index("date").sort_index()
    df_b = df_b.drop_duplicates("date").set_index("date").sort_index()

    # Align to fund's calendar (fund has fewer days than index); inner-join dates
    merged = df_f.join(df_b, how="inner").dropna()
    if len(merged) < 30 + max(0, bench_lag_bdays):
        return None
    merged["r_fund"]  = merged["SHARE"].pct_change()
    # Shift bench return by N rows of the joined calendar (≈ N trading days)
    # to align with cotização axis — see docstring.
    merged["r_bench"] = merged["VALUE"].pct_change().shift(bench_lag_bdays)
    merged = merged.dropna()
    er = (merged["r_fund"] - merged["r_bench"]).tail(window_days)
    if len(er) < 30:
        return None

    return {
        "bvar_pct":    -float(er.quantile(0.05)) * 100,
        "n_obs":       int(len(er)),
        "window_days": int(window_days),
        "mean_er_pct": float(er.mean()) * 100,
        "std_er_pct":  float(er.std())  * 100,
    }


def compute_portfolio_vol_regime(dist_map: dict, vol_window: int = 21) -> dict:
    """Vol regime from the Historical-Simulation W series (today's portfolio
    × N historical days). Isolates carteira-driven vol regime: independent
    of flows, consistent with the 252d distribution card.

    For each portfolio key (e.g. "MACRO", "EVOLUTION"):
      vol_recent  = std(W[-21:]) * sqrt(252)    — last 21 historical days
      vol_full    = std(W)       * sqrt(252)    — full available window
      ratio       = vol_recent / vol_full
      pct_rank    = percentile of std(W[-21:]) among all rolling 21d stds in W
      regime      = low/normal/elevated/stressed based on pct_rank

    W is in bps of NAV. Returned vols are in %.
    """
    out = {}
    if not dist_map:
        return out
    for pkey, w in dist_map.items():
        if w is None:
            continue
        w = np.asarray(w, dtype=float)
        w = w[~np.isnan(w)]
        if len(w) < vol_window + 30:
            continue
        vol_recent_bps = float(np.std(w[-vol_window:], ddof=1))
        vol_full_bps   = float(np.std(w,               ddof=1))
        if vol_full_bps < 1e-9:
            continue
        # Annualized vol in % of NAV: bps * sqrt(252) / 100
        vol_recent_pct = vol_recent_bps * np.sqrt(252) / 100.0
        vol_full_pct   = vol_full_bps   * np.sqrt(252) / 100.0
        ratio          = vol_recent_pct / vol_full_pct

        # Rolling 21d std across W → percentile + z-score of most-recent window
        n_roll = len(w) - vol_window + 1
        rolling_std = np.array([
            np.std(w[i:i+vol_window], ddof=1) for i in range(n_roll)
        ])
        cur_std = rolling_std[-1]
        pct_rank = float((rolling_std[:-1] < cur_std).mean()) * 100.0 \
                   if len(rolling_std) > 1 else None

        # Range stats (annualized %), for the visual strip
        sqrt252 = np.sqrt(252)
        rolling_ann_pct = rolling_std * sqrt252 / 100.0
        vol_min_pct = float(np.min(rolling_ann_pct))
        vol_max_pct = float(np.max(rolling_ann_pct))
        vol_p25_pct = float(np.percentile(rolling_ann_pct, 25))
        vol_p50_pct = float(np.percentile(rolling_ann_pct, 50))
        vol_p75_pct = float(np.percentile(rolling_ann_pct, 75))

        # z-score of current rolling std within the rolling-std distribution.
        # Note: samples overlap by construction (21d window, shift 1), so
        # N_eff ~ n_roll / 21. Treat z as directional, not a hard threshold.
        roll_mean = float(np.mean(rolling_std))
        roll_sd   = float(np.std(rolling_std, ddof=1))
        if roll_sd > 1e-12:
            z_series = (rolling_std - roll_mean) / roll_sd
            z_cur   = float(z_series[-1])
            z_min   = float(np.min(z_series))
            z_max   = float(np.max(z_series))
            z_p25   = float(np.percentile(z_series, 25))
            z_p50   = float(np.percentile(z_series, 50))
            z_p75   = float(np.percentile(z_series, 75))
        else:
            z_cur = z_min = z_max = z_p25 = z_p50 = z_p75 = None

        if pct_rank is None:        regime = "—"
        elif pct_rank < 20:         regime = "low"
        elif pct_rank < 70:         regime = "normal"
        elif pct_rank < 90:         regime = "elevated"
        else:                       regime = "stressed"

        out[pkey] = {
            "vol_recent_pct": vol_recent_pct,
            "vol_full_pct":   vol_full_pct,
            "ratio":          ratio,
            "pct_rank":       pct_rank,
            "regime":         regime,
            "n_obs":          len(w),
            "n_roll":         n_roll,
            "vol_min_pct":    vol_min_pct,
            "vol_max_pct":    vol_max_pct,
            "vol_p25_pct":    vol_p25_pct,
            "vol_p50_pct":    vol_p50_pct,
            "vol_p75_pct":    vol_p75_pct,
            "z":              z_cur,
            "z_min":          z_min,
            "z_max":          z_max,
            "z_p25":          z_p25,
            "z_p50":          z_p50,
            "z_p75":          z_p75,
        }
    return out


def compute_top_windows(w_series, k: int = 5, window_days: int = 21) -> dict | None:
    """Find k worst and k best NON-OVERLAPPING rolling windows in a return series.

    Greedy: sort window starts by cumulative sum (asc for worst, desc for best),
    pick the first k whose window [start, end] doesn't overlap any previously
    picked window. Returns None if series shorter than window_days.

    Returns:
      {"n_obs": N, "worst": [item, ...], "best": [item, ...]}
      item = {start_idx, end_idx, n_back, sum_bps, mean_bps, min_day, max_day}
        n_back = bus-days from window end to series end (0 = ending today)
    """
    import numpy as np
    if w_series is None or len(w_series) < window_days:
        return None
    w = np.asarray(w_series, dtype=float)
    w = w[~np.isnan(w)]
    n = len(w)
    if n < window_days:
        return None
    sums = np.array([float(np.sum(w[i:i + window_days])) for i in range(n - window_days + 1)])

    def _greedy(asc: bool) -> list:
        order = np.argsort(sums) if asc else np.argsort(-sums)
        picked = []
        for idx in order:
            i = int(idx)
            e = i + window_days - 1
            if any(not (e < u[0] or i > u[1]) for u in picked):
                continue
            picked.append((i, e))
            if len(picked) >= k:
                break
        out = []
        for s, e in picked:
            seg = w[s:e + 1]
            out.append({
                "start_idx": s, "end_idx": e,
                "n_back":  n - 1 - e,
                "sum_bps":  float(np.sum(seg)),
                "mean_bps": float(np.mean(seg)),
                "min_day":  float(np.min(seg)),
                "max_day":  float(np.max(seg)),
            })
        return out

    return {"n_obs": n, "worst": _greedy(True), "best": _greedy(False)}


def compute_distribution_stats(w_series, actual_bps=None):
    """Descriptive stats over the HS portfolio W series (sd, percentiles,
       worst/best) — purely empirical, not predictive. Optional `actual_bps`
       lets the caller compare today's realized PnL against the same window."""
    import numpy as np
    if w_series is None or len(w_series) < 30:
        return None
    w = np.asarray(w_series, dtype=float)
    w = w[~np.isnan(w)]
    if len(w) < 30:
        return None
    sd = float(np.std(w, ddof=1))
    out = {
        "n":     int(len(w)),
        "min":   float(np.min(w)),
        "max":   float(np.max(w)),
        "mean":  float(np.mean(w)),
        "sd":    sd,
        "var95": float(np.percentile(w,  5)),   # 5th pct (loss tail) = VaR 95%
        "var_p95": float(np.percentile(w, 95)), # 95th pct (gain tail)
    }
    if actual_bps is not None:
        pct = float((w < actual_bps).sum()) / len(w) * 100.0
        out["actual"]     = float(actual_bps)
        out["percentile"] = pct
        out["nvols"]      = actual_bps / sd if sd > 1e-9 else None
    return out


def compute_pa_outliers(df_daily: pd.DataFrame, date_str: str,
                         z_min: float = 2.0, bps_min: float = 3.0,
                         abs_floor_bps: float = 10.0,
                         min_obs: int = 20) -> pd.DataFrame:
    """
    Flags products where today's alpha contribution is either:
      (a) statistically unusual (|z-score| >= z_min) AND materially impactful (|bps| >= bps_min), OR
      (b) simply absolutely large (|bps| >= abs_floor_bps), regardless of z —
          catches moves on historically-volatile names (e.g. AXIA) where σ is too wide
          for the z-test to trigger even on a real sell-off.
    Excludes non-directional livros (Caixa, Taxas e Custos).
    """
    if df_daily is None or df_daily.empty:
        return pd.DataFrame()

    today = pd.Timestamp(date_str)
    excluded_livros = {"Caixa", "Caixa USD", "Taxas e Custos", "Prev"}

    d = df_daily[~df_daily["LIVRO"].isin(excluded_livros)]

    past  = d[d["DATE"] < today]
    today_df = d[d["DATE"] == today]

    keys = ["FUNDO", "LIVRO", "PRODUCT"]

    # Implied daily return = dia_bps / |position_brl| (asset return, position-neutral).
    # Use abs() so short books (negative position_brl) also enter σ-stats and z-scoring;
    # the prior `> 0` filter silently dropped Bracco / Quant_PA / IBOV-future shorts.
    pos_col = "position_brl" if "position_brl" in past.columns else None
    if pos_col and (past[pos_col].abs() > 1e-9).any():
        valid_pos = past[past[pos_col].abs() > 1e-9].copy()
        valid_pos["implied_return"] = valid_pos["dia_bps"] / valid_pos[pos_col].abs()
        stats = valid_pos.groupby(keys)["implied_return"].agg(sigma="std", n_obs="count").reset_index()
    else:
        stats = past.groupby(keys)["dia_bps"].agg(sigma="std", n_obs="count").reset_index()

    today_agg = today_df.groupby(keys).agg(
        today_bps=("dia_bps", "sum"),
        today_pos=("position_brl", "sum") if pos_col else ("dia_bps", "count"),
    ).reset_index()

    merged = today_agg.merge(stats, on=keys, how="left")
    merged["sigma"] = merged["sigma"].fillna(0.0)
    merged["n_obs"] = merged["n_obs"].fillna(0).astype(int)

    # Z = today's implied return / σ_implied_return (asset vol, position-neutral)
    merged["z"] = 0.0
    if pos_col:
        valid = (merged["sigma"] > 1e-9) & (merged["today_pos"].abs() > 1e-9)
        merged.loc[valid, "z"] = (
            (merged.loc[valid, "today_bps"] / merged.loc[valid, "today_pos"].abs())
            / merged.loc[valid, "sigma"]
        )
    else:
        valid = merged["sigma"] > 0.05
        merged.loc[valid, "z"] = merged.loc[valid, "today_bps"] / merged.loc[valid, "sigma"]

    statistical = (
        (merged["z"].abs() >= z_min)
        & (merged["today_bps"].abs() >= bps_min)
        & (merged["n_obs"] >= min_obs)
    )
    absolute_big = (
        (merged["today_bps"].abs() >= abs_floor_bps)
        & (merged["n_obs"] >= min_obs)
    )
    flagged = merged[statistical | absolute_big].copy()
    flagged = flagged.sort_values("today_bps", key=lambda s: s.abs(), ascending=False)
    return flagged
