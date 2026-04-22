"""
smoke_test.py — regression guard for the Risk Monitor report generator.

Run:    python smoke_test.py 2026-04-22
Exit:   0 = all checks passed
        1 = one or more checks failed (details printed to stderr)

Typical runtime: ~35 seconds (runs both generators then checks outputs).
"""
import re
import sys
import subprocess
from pathlib import Path

# ── Config ────────────────────────────────────────────────────────────────────
REPO   = Path(__file__).parent
PYTHON = Path(r"C:\Users\diego.fainberg\.venvs\risk_monitor\Scripts\python.exe")

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
    blocks = re.findall(r"VaR.{0,300}", clean)
    pcts   = [float(m) for b in blocks for m in re.findall(r"\b(\d{1,2}\.\d{1,2})%", b)]
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
def main() -> None:
    date_str = sys.argv[1] if len(sys.argv) > 1 else "2026-04-22"
    mc_dir   = REPO / "data" / "morning-calls"

    print(f"\n{'='*50}")
    print(f"  Smoke test — {date_str}")
    print(f"{'='*50}")

    run("generate_risk_report.py", date_str)
    run("pm_vol_card.py",          date_str)

    check_risk_report(mc_dir / f"{date_str}_risk_monitor.html")
    check_vol_card(mc_dir    / f"pm_vol_card_{date_str}.html")

    print(f"\n{'='*50}")
    if _failures:
        print(f"  FAILED — {len(_failures)} check(s):")
        for f in _failures:
            print(f"    x {f}")
        sys.exit(1)
    else:
        checks_run = 5 * 2   # ~5 checks per report × 2 reports
        print(f"  ALL CHECKS PASSED  ({checks_run} assertions, {date_str})")
        sys.exit(0)


if __name__ == "__main__":
    main()
