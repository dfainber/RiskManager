"""
svg_renderers.py — visual primitives for the Risk Monitor HTML report.

Self-contained: each helper takes plain numbers / pandas Series and returns
an SVG or a base64-encoded PNG string. No DB access, no module-level
runtime state. Import freely from anywhere that produces HTML.
"""
import base64
import io

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from risk_config import ALERT_THRESHOLD, STOP_BASE, UTIL_HARD, UTIL_WARN


def make_sparkline(series: pd.Series, color: str, width=160, height=50) -> str:
    s = series.dropna().iloc[-60:]  # last 60 obs
    if len(s) < 2:
        return ""
    fig, ax = plt.subplots(figsize=(width / 72, height / 72), dpi=96)
    fig.patch.set_facecolor("none")
    ax.set_facecolor("none")
    ax.plot(s.values, color=color, linewidth=1.8, solid_capstyle="round")
    ax.fill_between(range(len(s)), s.values, alpha=0.15, color=color)
    ax.axis("off")
    ax.set_xlim(0, len(s) - 1)
    plt.tight_layout(pad=0)
    buf = io.BytesIO()
    fig.savefig(buf, format="png", transparent=True, bbox_inches="tight", pad_inches=0)
    plt.close(fig)
    buf.seek(0)
    return base64.b64encode(buf.read()).decode()


def range_bar_svg(val, vmin, vmax, soft, hard, width=220, height=48) -> str:
    if vmax == vmin:
        pct = 50.0
    else:
        pct = (val - vmin) / (vmax - vmin) * 100
    pct = max(0, min(100, pct))

    soft_x = (soft - vmin) / (vmax - vmin) * width if vmax != vmin else width * 0.7
    hard_x = (hard - vmin) / (vmax - vmin) * width if vmax != vmin else width * 0.9
    alert_x = ALERT_THRESHOLD / 100 * width
    soft_x = max(0, min(width, soft_x))
    hard_x = max(0, min(width, hard_x))
    dot_x  = pct / 100 * width

    util = val / soft * 100 if soft else 0
    bar_color = "#4ade80" if util < UTIL_WARN else "#facc15" if util < UTIL_HARD else "#f87171"
    above_alert = pct >= ALERT_THRESHOLD

    # Dot: pulsing ring if above alert threshold
    dot_r   = 7
    ring_r  = 11
    dot_color = "#fb923c" if above_alert else bar_color
    ring_svg = f'<circle cx="{dot_x:.1f}" cy="{height//2}" r="{ring_r}" fill="none" stroke="#fb923c" stroke-width="2" opacity="0.5"/>' if above_alert else ""

    pos_label = f"{pct:.0f}%"
    pos_color = "#fb923c" if above_alert else "white"

    # Alert zone: shaded region from 80% to end
    alert_zone = f'<rect x="{alert_x:.1f}" y="{height//2-4}" width="{width-alert_x:.1f}" height="8" rx="2" fill="#fb923c" opacity="0.12"/>' if True else ""

    svg = f"""<svg width="{width}" height="{height}" xmlns="http://www.w3.org/2000/svg">
  <!-- background track -->
  <rect x="0" y="{height//2-4}" width="{width}" height="8" rx="4" fill="#2d2d3d"/>
  <!-- alert zone (>80%) -->
  {alert_zone}
  <!-- filled portion -->
  <rect x="0" y="{height//2-4}" width="{dot_x:.1f}" height="8" rx="4" fill="{dot_color}" opacity="0.7"/>
  <!-- 80% threshold tick -->
  <line x1="{alert_x:.1f}" y1="{height//2-8}" x2="{alert_x:.1f}" y2="{height//2+8}" stroke="#fb923c" stroke-width="1" stroke-dasharray="2,2" opacity="0.6"/>
  <!-- soft limit line -->
  <line x1="{soft_x:.1f}" y1="{height//2-10}" x2="{soft_x:.1f}" y2="{height//2+10}" stroke="#facc15" stroke-width="1.5" stroke-dasharray="3,2"/>
  <!-- hard limit line -->
  <line x1="{hard_x:.1f}" y1="{height//2-10}" x2="{hard_x:.1f}" y2="{height//2+10}" stroke="#f87171" stroke-width="1.5" stroke-dasharray="3,2"/>
  <!-- alert ring (if above 80%) -->
  {ring_svg}
  <!-- dot -->
  <circle cx="{dot_x:.1f}" cy="{height//2}" r="{dot_r}" fill="{dot_color}" stroke="#111" stroke-width="1.5"/>
  <!-- min label -->
  <text x="2" y="{height-2}" font-size="9" fill="#555" font-family="monospace">{vmin:.2f}</text>
  <!-- max label -->
  <text x="{width-2}" y="{height-2}" font-size="9" fill="#555" font-family="monospace" text-anchor="end">{vmax:.2f}</text>
  <!-- position % label above dot -->
  <text x="{dot_x:.1f}" y="{height//2-12}" font-size="11" fill="{pos_color}" font-family="monospace" text-anchor="middle" font-weight="bold">{pos_label}</text>
</svg>"""
    return svg


def stop_bar_svg(budget_abs: float, pnl_mtd: float, budget_max: float,
                 width=300, height=54, soft_mark=None) -> str:
    """
    Single-track bidirectional bar.
    Origin = start of month (zero). Left = loss (red). Right = gain (green).
    Stop line = red vertical at -budget_abs.
    Soft mark = yellow dashed (CI only).
    Right side: 'margem Xbps' = distance from current PnL to hard stop.
    """
    LPAD, RPAD = 4, 4
    bmax = max(budget_max, STOP_BASE, 1.0)
    bar_w = width - LPAD - RPAD

    # origin at 55% → more space on the loss side
    origin_frac = 0.55
    origin_x    = LPAD + bar_w * origin_frac
    loss_px     = bar_w * origin_frac          # pixels available left of origin
    gain_px     = bar_w * (1 - origin_frac)    # pixels available right of origin
    loss_scale  = loss_px / (bmax * 1.05)      # px per bps, loss side
    gain_scale  = gain_px / (bmax * 0.65)      # px per bps, gain side (compressed)

    # current PnL dot/bar end position
    if pnl_mtd < 0:
        pnl_x = max(LPAD + 2.0, origin_x + pnl_mtd * loss_scale)
    else:
        pnl_x = min(float(width - RPAD - 2), origin_x + pnl_mtd * gain_scale)

    # stop line position
    if budget_abs > 0:
        stop_x = max(LPAD + 2.0, origin_x - budget_abs * loss_scale)
    else:
        stop_x = origin_x   # gancho: stop at zero

    # soft mark position (CI)
    soft_x = None
    if soft_mark is not None and soft_mark > 0:
        soft_x = max(LPAD + 2.0, origin_x - soft_mark * loss_scale)

    is_gain   = pnl_mtd >= 0
    bar_color = "#4ade80" if is_gain else "#f87171"
    fill_x    = min(origin_x, pnl_x)
    fill_w    = abs(pnl_x - origin_x)

    dist       = budget_abs + pnl_mtd      # room left (positive = safe)
    dist_color = "#4ade80" if dist > 0 else "#f87171"
    dist_label = f"+{dist:.0f}" if dist > 0 else f"{dist:.0f}"

    y_mid = 22   # shifted down from 16 so top label has room above the bar
    bh    = 12

    parts = [f'<svg width="{width}" height="{height}" xmlns="http://www.w3.org/2000/svg">']

    # Background track
    parts.append(f'<rect x="{LPAD}" y="{y_mid-bh//2}" width="{bar_w}" height="{bh}" rx="3" fill="#1e293b"/>')
    # Subtle gain-side tint
    parts.append(f'<rect x="{origin_x:.1f}" y="{y_mid-bh//2}" width="{gain_px:.1f}" height="{bh}" fill="#14241a" opacity="0.6"/>')

    # Blue budget-available bar: stop_x → origin_x (shows remaining room before stop)
    if budget_abs > 0:
        bud_x = stop_x
        bud_w = origin_x - stop_x
        if bud_w > 1:
            parts.append(f'<rect x="{bud_x:.1f}" y="{y_mid-bh//2}" width="{bud_w:.1f}" height="{bh}" fill="#1e4976" opacity="0.55"/>')

    # PnL fill (drawn on top of budget bar)
    if fill_w > 0.5:
        parts.append(f'<rect x="{fill_x:.1f}" y="{y_mid-bh//2}" width="{fill_w:.1f}" height="{bh}" rx="2" fill="{bar_color}" opacity="0.85"/>')

    # Origin tick
    parts.append(f'<line x1="{origin_x:.1f}" y1="{y_mid-bh//2-3}" x2="{origin_x:.1f}" y2="{y_mid+bh//2+3}" stroke="#64748b" stroke-width="1.5"/>')

    # Hard stop line
    if budget_abs > 0:
        parts.append(f'<line x1="{stop_x:.1f}" y1="{y_mid-bh//2-4}" x2="{stop_x:.1f}" y2="{y_mid+bh//2+4}" stroke="#f87171" stroke-width="2.5"/>')
        parts.append(f'<text x="{stop_x:.1f}" y="{y_mid+bh//2+15}" font-size="8" fill="#f87171" font-family="monospace" text-anchor="middle">-{budget_abs:.0f}</text>')
        # Base-63 tick: always shown as reference for non-CI PMs
        if soft_mark is None:  # i.e. not CI
            base_x = max(LPAD + 2.0, origin_x - STOP_BASE * loss_scale)
            # Only draw if base is not the same position as the stop line
            if abs(base_x - stop_x) > 3:
                parts.append(f'<line x1="{base_x:.1f}" y1="{y_mid-bh//2}" x2="{base_x:.1f}" y2="{y_mid+bh//2+13}" stroke="#60a5fa" stroke-width="1" stroke-dasharray="2,2" opacity="0.7"/>')
                parts.append(f'<text x="{base_x:.1f}" y="{y_mid+bh//2+13}" font-size="8" fill="#60a5fa" font-family="monospace" text-anchor="middle">-{STOP_BASE:.0f}</text>')
    else:
        # Gancho: shade entire loss territory red
        parts.append(f'<rect x="{LPAD}" y="{y_mid-bh//2}" width="{origin_x-LPAD:.1f}" height="{bh}" rx="3" fill="#f8717130"/>')
        parts.append(f'<text x="{origin_x-4:.1f}" y="{y_mid+bh//2+13}" font-size="8" fill="#fb923c" font-family="monospace" text-anchor="end">GANCHO</text>')

    # Soft mark (CI only)
    if soft_x is not None:
        parts.append(f'<line x1="{soft_x:.1f}" y1="{y_mid-bh//2-4}" x2="{soft_x:.1f}" y2="{y_mid+bh//2+4}" stroke="#facc15" stroke-width="1.5" stroke-dasharray="3,2"/>')
        parts.append(f'<text x="{soft_x:.1f}" y="{y_mid+bh//2+13}" font-size="8" fill="#facc15" font-family="monospace" text-anchor="middle">-{soft_mark:.0f}</text>')

    # MTD value label above the bar tip (the only text label — keeps graphic self-contained)
    pnl_label = f"+{pnl_mtd:.1f}" if is_gain else f"{pnl_mtd:.1f}"
    parts.append(f'<text x="{pnl_x:.1f}" y="{y_mid-bh//2-5}" font-size="9" fill="{bar_color}" font-family="monospace" text-anchor="middle" font-weight="bold">{pnl_label}</text>')

    parts.append('</svg>')
    return '\n'.join(parts)


def range_line_svg(v_cur, v_min, v_max, v_p50=None,
                   width=220, height=28, fmt="{:.2f}") -> str:
    """Simple horizontal line from min to max with a highlighted dot at current.
       Optional median tick if v_p50 given. Labels for min and max at edges.
    """
    if v_cur is None or v_min is None or v_max is None or v_max <= v_min:
        return ""
    pad = 6
    x_min = pad
    x_max = width - pad
    def _x(v):
        return x_min + (v - v_min) / (v_max - v_min) * (x_max - x_min)
    cur_x = _x(v_cur)
    p50_x = _x(v_p50) if v_p50 is not None else None
    # Dot color by position within range (terciles)
    third = (v_max - v_min) / 3.0
    if v_cur >= v_min + 2*third:     dot_color = "#f87171"
    elif v_cur <= v_min + third:     dot_color = "#4ade80"
    else:                            dot_color = "#facc15"
    y = height // 2
    p50_svg = f'<line x1="{p50_x:.1f}" y1="{y-5}" x2="{p50_x:.1f}" y2="{y+5}" stroke="#64748b" stroke-width="1" opacity="0.7"/>' if p50_x is not None else ""
    return f"""<svg width="{width}" height="{height}" xmlns="http://www.w3.org/2000/svg">
  <line x1="{x_min}" y1="{y}" x2="{x_max}" y2="{y}" stroke="#475569" stroke-width="2" stroke-linecap="round"/>
  <circle cx="{x_min}" cy="{y}" r="2.5" fill="#475569"/>
  <circle cx="{x_max}" cy="{y}" r="2.5" fill="#475569"/>
  {p50_svg}
  <circle cx="{cur_x:.1f}" cy="{y}" r="5.5" fill="{dot_color}" stroke="#0b1220" stroke-width="1.5"/>
  <text x="0" y="{height-2}" font-size="8" fill="#64748b" font-family="monospace">{fmt.format(v_min)}</text>
  <text x="{width}" y="{height-2}" font-size="8" fill="#64748b" font-family="monospace" text-anchor="end">{fmt.format(v_max)}</text>
</svg>"""


def evo_spark_svg(series: "pd.Series", today_val: float,
                  width: int = 560, height: int = 90) -> str:
    if series is None or series.empty:
        return ""
    ys = series.values.astype(float)
    y_min, y_max = float(np.nanmin(ys)), float(np.nanmax(ys))
    if y_max - y_min < 1e-9:
        y_max = y_min + 0.01
    pad = 10
    w, h = width - 2 * pad, height - 2 * pad
    def xy(i, v):
        x = pad + (i / (len(ys) - 1 or 1)) * w
        y = pad + (1 - (v - y_min) / (y_max - y_min)) * h
        return (x, y)
    pts = " ".join(f"{x:.1f},{y:.1f}"
                   for i, v in enumerate(ys) for (x, y) in [xy(i, v)])
    dx, dy = xy(len(ys) - 1, today_val)
    mean_val = float(np.nanmean(ys))
    my = pad + (1 - (mean_val - y_min) / (y_max - y_min)) * h
    return f"""
    <svg viewBox="0 0 {width} {height}" style="width:100%;max-width:{width}px;height:{height}px">
      <line x1="{pad}" y1="{my:.1f}" x2="{width - pad}" y2="{my:.1f}"
            stroke="var(--line-2)" stroke-dasharray="3 3" stroke-width="1"/>
      <polyline points="{pts}" fill="none" stroke="var(--accent-2)" stroke-width="1.6"/>
      <circle cx="{dx:.1f}" cy="{dy:.1f}" r="4"
              fill="var(--accent-2)" stroke="var(--bg)" stroke-width="2"/>
      <text x="{width - pad}" y="{my - 4:.1f}" text-anchor="end"
            font-size="10" fill="var(--muted)">média 252d: {mean_val:.2f}</text>
    </svg>"""


def multi_line_chart_svg(dates, series, *, width=760, height=280,
                          title="", y_suffix="%") -> str:
    """Multi-series line chart over a shared date axis.

    Parameters
    ----------
    dates  : iterable of pd.Timestamp / datetime / strings (will be cast)
    series : list of dicts: {"label": str, "values": list[float], "color": str,
                              "stroke": float (default 1.6), "dash": str|None}
    """
    dates = list(dates)
    n = len(dates)
    if n < 2 or not series:
        return ""

    # Top padding generous to fit horizontal legend without overlap.
    pad_l, pad_r, pad_t, pad_b = 60, 18, 44, 38
    plot_w = width  - pad_l - pad_r
    plot_h = height - pad_t - pad_b

    flat = [v for s in series
              for v in s["values"]
              if v is not None and not (isinstance(v, float) and np.isnan(v))]
    if not flat:
        return ""
    y_min, y_max = float(min(flat)), float(max(flat))
    if y_max == y_min:
        y_max = y_min + 1.0
    pad_y = (y_max - y_min) * 0.08
    y_min, y_max = y_min - pad_y, y_max + pad_y

    def xpx(i):  return pad_l + (i / (n - 1)) * plot_w
    def ypx(v):
        if v is None or (isinstance(v, float) and np.isnan(v)):
            return None
        return pad_t + (1 - (v - y_min) / (y_max - y_min)) * plot_h

    # Y gridlines + labels (5 ticks)
    grid_lines = []
    for k in range(5):
        v = y_min + (y_max - y_min) * k / 4
        y = pad_t + (1 - k / 4) * plot_h
        grid_lines.append(
            f'<line x1="{pad_l}" y1="{y:.1f}" x2="{pad_l + plot_w:.1f}" y2="{y:.1f}" '
            f'stroke="var(--line-2)" stroke-width="0.4" opacity="0.5"/>'
            f'<text x="{pad_l - 6}" y="{y + 3:.1f}" text-anchor="end" '
            f'font-size="10" fill="var(--muted)">{v:.0f}{y_suffix}</text>'
        )

    # X axis labels: 5 evenly spaced dates
    x_labels = []
    for k in range(5):
        i = int(round(k * (n - 1) / 4))
        d = pd.Timestamp(dates[i])
        lbl = d.strftime("%d-%b")
        x = xpx(i)
        x_labels.append(
            f'<text x="{x:.1f}" y="{pad_t + plot_h + 18:.1f}" text-anchor="middle" '
            f'font-size="10" fill="var(--muted)">{lbl}</text>'
        )

    # Series polylines
    line_svgs = []
    for s in series:
        pts = []
        for i, v in enumerate(s["values"]):
            yv = ypx(v)
            if yv is None:
                continue
            pts.append(f"{xpx(i):.1f},{yv:.1f}")
        if len(pts) < 2:
            continue
        stroke = s.get("stroke", 1.6)
        dash = f' stroke-dasharray="{s["dash"]}"' if s.get("dash") else ""
        line_svgs.append(
            f'<polyline points="{" ".join(pts)}" fill="none" '
            f'stroke="{s["color"]}" stroke-width="{stroke}"{dash} '
            f'stroke-linejoin="round" stroke-linecap="round"/>'
        )

    # Legend (top, horizontal)
    leg_x = pad_l
    leg_y = pad_t - 18
    legend = []
    for s in series:
        legend.append(
            f'<rect x="{leg_x}" y="{leg_y - 6:.1f}" width="14" height="3" '
            f'rx="1.5" fill="{s["color"]}"/>'
            f'<text x="{leg_x + 18}" y="{leg_y:.1f}" font-size="10.5" '
            f'fill="var(--text)">{s["label"]}</text>'
        )
        leg_x += 18 + len(s["label"]) * 6.6 + 14

    title_html = (f'<text x="{pad_l}" y="14" font-size="11" font-weight="600" '
                   f'fill="var(--text)">{title}</text>') if title else ""

    return f"""
    <svg viewBox="0 0 {width} {height}" xmlns="http://www.w3.org/2000/svg"
         style="width:100%;max-width:{width}px;height:{height}px;display:block;font-family:inherit">
      {title_html}
      {"".join(grid_lines)}
      <line x1="{pad_l}" y1="{pad_t + plot_h:.1f}" x2="{pad_l + plot_w:.1f}" y2="{pad_t + plot_h:.1f}"
            stroke="var(--line-2)" stroke-width="0.6"/>
      <line x1="{pad_l}" y1="{pad_t}" x2="{pad_l}" y2="{pad_t + plot_h:.1f}"
            stroke="var(--line-2)" stroke-width="0.6"/>
      {"".join(x_labels)}
      {"".join(line_svgs)}
      {"".join(legend)}
    </svg>"""
