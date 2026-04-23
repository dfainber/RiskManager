"""
smoke_test.py — regression guard for the Risk Monitor report generator.

Usage:
  python smoke_test.py                             # defaults to 2 business days ago
  python smoke_test.py 2026-04-20                  # run checks (+ snapshot diff if snapshot exists)
  python smoke_test.py 2026-04-20 --save-snapshot  # save numeric snapshot to data/.smoke_snapshots/
  python smoke_test.py 2026-04-20 --no-snapshot    # skip snapshot check even if one exists

Exit:   0 = all checks passed
        1 = one or more checks failed (details printed to stderr)

Typical runtime: ~35 seconds (runs both generators then checks outputs).

Snapshot mode:
  The snapshot is a sorted multiset of every numeric value visible in the
  generated HTML — percentages and bps — with PNG base64 and inline scripts
  stripped out. Two runs with the same code and the same DB state produce
  identical snapshots. Used to validate refactors: save snapshot before
  change, diff after change, fail if numeric content diverged.
"""
import json
import re
import sys
import subprocess
from pathlib import Path
from datetime import date, timedelta

# ── Config ────────────────────────────────────────────────────────────────────
REPO   = Path(__file__).parent
PYTHON = Path(r"C:\Users\diego.fainberg\.venvs\risk_monitor\Scripts\python.exe")
SNAP_DIR = REPO / "data" / ".smoke_snapshots"

# Absolute lower bounds — if either file shrinks past these something big broke.
MIN_RISK_REPORT_BYTES = 800_000   # normally ~1.7 MB
MIN_VOL_CARD_BYTES    =  30_000   # normally ~58 KB

# All of these strings must appear somewhere in the risk report HTML.
REQUIRED_FUND_NAMES = ["Macro", "Quantitativo", "Evolution", "ALBATROZ", "Frontier", "IDKA"]
REQUIRED_SECTIONS   = ["Risk Monitor", "Performance Attribution", "Summary", "Risk Budget"]

# VaR/BVaR readings must fall within this range (%).
# 99.0 is a sentinel limit value for Frontier (informative=True) — excluded by upper bound.
VAR_READING_RANGE = (0.05, 15.0)

# ── State ─────────────────────────────────────────────────────────────────────
_failures: list[str] = []

def _fail(msg: str) -> None:
    _failures.append(msg)
    print(f"  FAIL  {msg}")

def _ok(msg: str) -> None:
    print(f"  ok    {msg}")

# ── Checks ────────────────────────────────────────────────────────────────────
def check_risk_report(path: Path) -> None:
    print(f"\n-- risk_report: {path.name}")

    if not path.exists():
        _fail(f"file not found: {path}")
        return
    _ok("file exists")

    size = path.stat().st_size
    if size < MIN_RISK_REPORT_BYTES:
        _fail(f"file too small: {size:,} B (min {MIN_RISK_REPORT_BYTES:,})")
    else:
        _ok(f"size {size / 1024:.0f} KB")

    html  = path.read_text(encoding="utf-8", errors="replace")
    clean = re.sub(r"<script[^>]*>.*?</script>", "", html, flags=re.DOTALL)

    # NaN / None leaking into visible cell content
    leaks = re.findall(r">[^<]*(?:NaN|None)[^<]*<", clean)
    leaks = [l for l in leaks if "<td" in l or "<th" in l or "<span" in l or l.startswith(">")]
    if leaks:
        _fail(f"NaN/None leaked into cells ({len(leaks)} hit(s)): {leaks[:3]}")
    else:
        _ok("no NaN/None in cells")

    # Fund names
    missing = [f for f in REQUIRED_FUND_NAMES if f not in html]
    if missing:
        _fail(f"fund name(s) missing: {missing}")
    else:
        _ok(f"all {len(REQUIRED_FUND_NAMES)} fund names present")

    # Section markers
    missing_sec = [s for s in REQUIRED_SECTIONS if s not in html]
    if missing_sec:
        _fail(f"section(s) missing: {missing_sec}")
    else:
        _ok(f"all {len(REQUIRED_SECTIONS)} sections present")

    # VaR values in plausible range (exclude 99.0 Frontier sentinels)
    # Negative lookbehind (?<![+\-]) excludes signed %s — those are exposure deltas, not VaR readings.
    blocks = re.findall(r"VaR.{0,300}", clean)
    pcts   = [float(m) for b in blocks for m in re.findall(r"(?<![+\-])\b(\d{1,2}\.\d{1,2})%", b)]
    pcts   = [p for p in pcts if p > 0 and p < 90]   # 99.0 sentinel excluded by < 90
    if not pcts:
        _fail("no VaR % values found in HTML")
    else:
        bad = [p for p in pcts if not (VAR_READING_RANGE[0] <= p <= VAR_READING_RANGE[1])]
        if bad:
            _fail(f"VaR values out of range {VAR_READING_RANGE}: {sorted(set(bad))}")
        else:
            _ok(f"{len(pcts)} VaR values in range {VAR_READING_RANGE}")


def check_vol_card(path: Path) -> None:
    print(f"\n-- pm_vol_card: {path.name}")

    if not path.exists():
        _fail(f"file not found: {path}")
        return
    _ok("file exists")

    size = path.stat().st_size
    if size < MIN_VOL_CARD_BYTES:
        _fail(f"file too small: {size:,} B (min {MIN_VOL_CARD_BYTES:,})")
    else:
        _ok(f"size {size / 1024:.0f} KB")

    html  = path.read_text(encoding="utf-8", errors="replace")
    clean = re.sub(r"<script[^>]*>.*?</script>", "", html, flags=re.DOTALL)

    # NaN / None leaking into visible content
    leaks = re.findall(r">[^<]*(?:NaN|None)[^<]*<", clean)
    if leaks:
        _fail(f"NaN/None leaked into cells ({len(leaks)} hit(s)): {leaks[:3]}")
    else:
        _ok("no NaN/None in cells")

    # PM names
    for pm in ("CI", "LF", "JD", "RJ"):
        if pm not in html:
            _fail(f"PM {pm!r} missing from vol card")
    _ok("all PM names present")

    # Vol values exist and are numeric (non-zero annualised vol should be > 1 bps)
    vol_vals = [float(m) for m in re.findall(r"\b(\d{1,3}\.\d{1,2})\b", clean)]
    reasonable = [v for v in vol_vals if 1.0 <= v <= 200.0]
    if len(reasonable) < 10:
        _fail(f"too few numeric vol values found ({len(reasonable)})")
    else:
        _ok(f"{len(reasonable)} numeric values in plausible vol range")


# ── Numeric snapshot ──────────────────────────────────────────────────────────
def extract_numeric_content(path: Path) -> list[str]:
    """
    Extract every numeric value visible in the HTML as a sorted list of strings.

    Strips:
      - <script>...</script>  (JS array ordering is non-deterministic)
      - data:image/...;base64,...  (matplotlib PNGs embed a timestamp)

    Captures:
      - percentages        e.g.  +1.23%, -0.45%, 12.34%
      - bps values         e.g.  +7 bps, -123 bps, 4.5 bps
      - bare decimals      e.g.  1.234, -0.56  (for NAV, ratios, etc.)
    """
    if not path.exists():
        return []
    html = path.read_text(encoding="utf-8", errors="replace")
    html = re.sub(r"<script[^>]*>.*?</script>", "", html, flags=re.DOTALL)
    html = re.sub(r"data:image[^\"']+", "", html)

    pcts = re.findall(r"[-+]?\d+\.\d+%", html)
    bps  = re.findall(r"[-+]?\d+\.?\d*\s*bps", html)
    return sorted(pcts + bps)


def snapshot_path(date_str: str, kind: str) -> Path:
    """Path for a numeric snapshot. kind is 'risk_report' or 'vol_card'."""
    return SNAP_DIR / f"{date_str}_{kind}.json"


def save_snapshot(path: Path, nums: list[str]) -> None:
    SNAP_DIR.mkdir(parents=True, exist_ok=True)
    payload = {"count": len(nums), "values": nums}
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def load_snapshot(path: Path) -> list[str] | None:
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))["values"]


def check_snapshot(report_path: Path, date_str: str, kind: str, save: bool) -> None:
    """Compare current numeric content vs saved snapshot. Fail on any divergence."""
    snap = snapshot_path(date_str, kind)
    now  = extract_numeric_content(report_path)
    if save:
        save_snapshot(snap, now)
        _ok(f"snapshot saved ({len(now)} values) -> {snap.relative_to(REPO)}")
        return

    prev = load_snapshot(snap)
    if prev is None:
        print(f"  skip  no snapshot at {snap.relative_to(REPO)} (run with --save-snapshot)")
        return

    a, b = set(prev), set(now)
    if prev == now:
        _ok(f"numeric snapshot matches ({len(now)} values)")
        return
    only_prev = sorted(a - b)[:5]
    only_now  = sorted(b - a)[:5]
    _fail(
        f"numeric snapshot diverged: {len(a ^ b)} values differ "
        f"(sample prev-only={only_prev}, now-only={only_now})"
    )


# ── Generator runner ──────────────────────────────────────────────────────────
def run(script: str, date_str: str) -> bool:
    print(f"\nRunning {script} {date_str} ...")
    result = subprocess.run(
        [str(PYTHON), script, date_str],
        cwd=str(REPO),
        capture_output=True,
        text=True,
        timeout=300,
    )
    if result.returncode != 0:
        _fail(f"{script} exited {result.returncode}:\n{result.stderr[-800:]}")
        return False
    _ok(f"{script} exited 0")
    return True


# ── Main ──────────────────────────────────────────────────────────────────────
def _default_test_date() -> str:
    """
    Default smoke-test date: 2 business days before today (roll back from
    weekends). Picked so the report uses data that's already settled in the
    DB, avoiding the intraday churn that makes 'today' flaky as a baseline.
    """
    d = date.today() - timedelta(days=2)
    # If we landed on Saturday, go to Friday; Sunday → Thursday.
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d.strftime("%Y-%m-%d")


def main() -> None:
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    flags = {a for a in sys.argv[1:] if a.startswith("--")}
    date_str = args[0] if args else _default_test_date()
    save_snapshot_mode = "--save-snapshot" in flags
    no_snapshot_mode   = "--no-snapshot"   in flags
    mc_dir = REPO / "data" / "morning-calls"

    mode = "save-snapshot" if save_snapshot_mode else ("no-snapshot" if no_snapshot_mode else "check")
    print(f"\n{'='*50}")
    print(f"  Smoke test — {date_str}  [mode: {mode}]")
    print(f"{'='*50}")

    run("generate_risk_report.py", date_str)
    run("pm_vol_card.py",          date_str)

    risk_path = mc_dir / f"{date_str}_risk_monitor.html"
    vol_path  = mc_dir / f"pm_vol_card_{date_str}.html"

    check_risk_report(risk_path)
    check_vol_card(vol_path)

    if not no_snapshot_mode:
        print(f"\n-- numeric snapshot check")
        check_snapshot(risk_path, date_str, "risk_report", save=save_snapshot_mode)
        check_snapshot(vol_path,  date_str, "vol_card",    save=save_snapshot_mode)

    print(f"\n{'='*50}")
    if _failures:
        print(f"  FAILED — {len(_failures)} check(s):")
        for f in _failures:
            print(f"    x {f}")
        sys.exit(1)
    else:
        print(f"  ALL CHECKS PASSED ({date_str})")
        sys.exit(0)


if __name__ == "__main__":
    main()
