#!/usr/bin/env python3
"""
Dashboard of Arcus funding rates: trailing 1h / 8h / 24h cumulative funding
for every ONLINE market.

  python3 funding_dashboard.py --mainnet
  python3 funding_dashboard.py --testnet
  python3 funding_dashboard.py --mainnet --delay 0.25   # gentler pacing

Arcus funding is HOURLY. This lists the ONLINE markets from GET /v1/markets, then
for each one pulls GET /v1/fundingRates?market=<display> (hourly history, newest
first) and SUMS the most recent 1, 8, and 24 hourly funding rates -- i.e. the
realized funding over the last 1h / 8h / 24h. Both endpoints are public,
unsigned reads, so no creds are needed.

Rates are fractions; displayed as PERCENT (x100), e.g. 0.000012 -> 0.0012%.
"Annualized" projects the trailing-24h (one-day) funding forward: 24h sum x 365.

Arcus has no isolated margin (all markets cross) with a single default funding of
0.00125%/hr. Markets sitting EXACTLY at that default for all three windows (1hr 0.001250%,
8hr 0.010000%, 24hr 0.030000%) are HIDDEN (no signal); pass --all to show them.

Output (one row per market, sorted by ticker):

  BTC       0.137200% (1hr)  0.553200% (8hr)  2.170400% (24hr) Annualized: 792.20%
  ...
  Generated: <UTC ts> / Runtime <n> seconds / Backoffs: <n> / Hidden: <n> at default funding / Partial: <n> marked *

A window with fewer than N hourly points (new market or an API gap) is PARTIAL and would
understate funding; such values (and the annualized figure) are marked "*" rather than shown
as a full 1h/8h/24h number. A missing/unparseable fundingRate is a hard error, not silently 0.

The footer's "Backoffs" is the number of retry-sleeps incurred; a high count means you're being
rate-limited, so raise --delay. "Hidden" is how many default-funding rows were suppressed;
"Partial" is how many shown rows have an incomplete 24h window (marked *).
"""

import argparse
import html
import json
import math
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from decimal import Decimal, InvalidOperation

NETWORKS = {
    "testnet": "https://api.testnet.arcus.xyz",
    "staging": "https://api.staging.arcus.xyz",
    "mainnet": "https://api.arcus.xyz",       # live 2026-06-25 (reads only for now)
}
BASE = None   # set in main() from the required --testnet/--staging/--mainnet selector
BACKOFFS = 0  # count of retry backoffs (429/transient) across all requests; reported in the footer

WINDOWS = (1, 8, 24)   # trailing hours to sum (Arcus funding is hourly)

# Arcus has no isolated margin (all markets cross), with a single default funding of 0.00125%/hr.
# A market whose trailing window sums are EXACTLY that default for every window carries no signal
# -> hidden unless --all: 1hr 0.001250%, 8hr 0.010000%, 24hr 0.030000%.
DEFAULT_HOURLY = Decimal("0.0000125")

# --html cell shading. Compared against abs(displayed percent = sum x 100) per window.
# The three tiers per window are the same underlying funding pace -- ~50% / 75% / 100%
# annualized -- expressed at each window's scale (8hr = 8x the 1hr level, 24hr = 24x).
HTML_THRESHOLDS = {
    1:  (Decimal("0.00570385"), Decimal("0.0085557"), Decimal("0.0114077")),
    8:  (Decimal("0.04563084"), Decimal("0.0684462"), Decimal("0.0912616")),
    24: (Decimal("0.1368925"),  Decimal("0.2053388"), Decimal("0.2737850")),
}
# (background, text) per tier: light / medium / maximum-deep red.
HTML_COLORS = (
    ("#f4cccc", "#000000"),   # light red  -> dark text
    ("#e06666", "#ffffff"),   # medium red -> white text
    ("#cc0000", "#ffffff"),   # deep red   -> white text
)


def cell_tier(window, pct):
    """Red-shade tier index (0=light, 1=medium, 2=maximum-deep) for a funding cell, or None.
    pct is the displayed percent (sum x 100); compared by absolute value to this window's tiers."""
    tiers = HTML_THRESHOLDS.get(window)
    if tiers is None:
        return None
    a = abs(pct)
    idx = None
    for i, threshold in enumerate(tiers):
        if a > threshold:
            idx = i
    return idx


def cell_style(window, pct):
    """Inline CSS for a funding cell: right-aligned, plus a red shade at its tier."""
    style = "text-align:right;padding:2px 8px"
    idx = cell_tier(window, pct)
    if idx is not None:
        bg, fg = HTML_COLORS[idx]
        style += f";background-color:{bg};color:{fg}"
    return style


def _hex_rgb(h):
    h = h.lstrip("#")
    return int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)


def ansi_paint(text, idx):
    """Wrap text in a 24-bit ANSI background+foreground matching HTML tier `idx`, so the
    terminal shows the same 3 shades of red as the --html output. Applied AFTER width padding
    (the escape codes are zero-width) so column alignment is unaffected."""
    bg, fg = HTML_COLORS[idx]
    br, bgc, bb = _hex_rgb(bg)
    fr, fgc, fb = _hex_rgb(fg)
    return f"\x1b[48;2;{br};{bgc};{bb}m\x1b[38;2;{fr};{fgc};{fb}m{text}\x1b[0m"


def dec(value):
    """Parse a decimal string -> Decimal, or None if absent/invalid/non-finite."""
    try:
        d = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    return d if d.is_finite() else None


def is_default_funding(sums, hourly):
    """True if every window sum equals n * hourly exactly -- i.e. funding sat at the default
    across the whole trailing window, so the row carries no information."""
    return all(sums[n] == hourly * n for n in WINDOWS)


def get_json(url, what, delay, retries=4):
    """GET url -> parsed JSON, with retry+backoff on transient failures.

    A 429/5xx or a network/timeout error is retried (exponential backoff); a 4xx
    other than 429, or exhausted retries, becomes a clean SystemExit. `delay` is a
    politeness pause applied before the request so a whole-universe sweep doesn't
    hammer the rate limit. Each retry-sleep bumps the module BACKOFFS counter
    (reported in the footer, to help tune --delay).
    """
    global BACKOFFS
    if delay:
        time.sleep(delay)
    last = None
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, method="GET")
            with urllib.request.urlopen(req, timeout=15) as r:
                return json.loads(r.read() or b"{}")
        except urllib.error.HTTPError as e:
            last = f"HTTP {e.code}"
            if e.code != 429 and e.code < 500:
                # A permanent client error (bad market etc.) -- don't retry.
                try:
                    msg = json.loads(e.read() or b"{}").get("error", "")
                except (ValueError, TypeError):
                    msg = ""
                raise SystemExit(f"funding_dashboard: {what}: HTTP {e.code}: {msg or 'request failed'}")
        except (urllib.error.URLError, TimeoutError, OSError) as e:
            last = str(e)
        except json.JSONDecodeError as e:
            last = f"bad JSON: {e}"
        # transient -> back off and retry
        if attempt < retries - 1:
            BACKOFFS += 1
            time.sleep(min(2 ** attempt, 8))
    raise SystemExit(f"funding_dashboard: {what}: failed after {retries} attempts ({last}).")


def fetch_online_markets(delay):
    """GET /v1/markets -> list of ONLINE market dicts, sorted by baseAsset ticker."""
    data = get_json(f"{BASE}/v1/markets", "markets", delay)
    markets = data.get("markets")
    if not isinstance(markets, list):
        raise SystemExit("funding_dashboard: unexpected /v1/markets response (no 'markets' list).")
    online = [m for m in markets if str(m.get("status")) == "ONLINE"]
    return sorted(online, key=lambda m: str(m.get("baseAsset", m.get("marketDisplayName", ""))).upper())


def window_sums(market_display, delay):
    """Return (sums, points): sums = {hours: Decimal-sum} of the most recent N hourly funding
    rates per window, and points = how many hourly rows were actually available (<= max(WINDOWS)).

    Sorts the history newest-first locally (not trusting API ordering). A window with fewer than
    N points is PARTIAL -- the caller marks it (and its annualization) so short history isn't read
    as a full 1h/8h/24h figure. A missing/unparseable fundingRate is a HARD ERROR (surfaced, never
    silently summed as 0, which would make funding look artificially quiet).

    Only the most recent max(WINDOWS) hourly points are needed, so &limit caps the payload
    (the API returns ~200 rows otherwise, newest-first).
    """
    q = urllib.parse.urlencode({"market": market_display, "limit": max(WINDOWS)})
    data = get_json(f"{BASE}/v1/fundingRates?{q}", f"fundingRates {market_display}", delay)
    rates = data.get("fundingRates")
    if not isinstance(rates, list):
        raise SystemExit(f"funding_dashboard: unexpected /v1/fundingRates response for {market_display}.")

    def tkey(r):
        try:
            return int(r.get("time"))
        except (TypeError, ValueError):
            return -1

    rates = sorted(rates, key=tkey, reverse=True)     # newest first
    vals = []
    for r in rates:
        v = dec(r.get("fundingRate"))
        if v is None:                                 # missing/unparseable -> surface, don't hide as 0
            raise SystemExit(f"funding_dashboard: {market_display}: malformed/missing fundingRate "
                             f"{r.get('fundingRate')!r} in /v1/fundingRates response.")
        vals.append(v)
    sums = {n: sum(vals[:n], Decimal(0)) for n in WINDOWS}
    return sums, len(vals)


def render_text(rows, footer, use_color=False):
    """Aligned plain-text dashboard + footer line. use_color shades funding cells with ANSI
    (same tiers/colors as --html) -- typically on only when stdout is a TTY."""
    # Ticker column left-justified; keep >=10 wide to match the standard layout.
    tickw = max(10, max((len(t) for t, _, _ in rows), default=0) + 2)

    # Rates are fractions; show as percent (x100) for readability, e.g. 0.000012 -> 0.0012%. A window
    # with fewer than N hourly points is PARTIAL -> marked "*" so short history isn't read as full.
    def pct_str(sums, points, n):
        s = f"{sums[n] * 100:.6f}%"
        return s + "*" if points < n else s

    # Annualized = the trailing-24h (one-day) funding projected forward: 24h sum x 365; "*" if partial.
    def ann_str(sums, points):
        s = f"{sums[max(WINDOWS)] * 365 * 100:.2f}%"
        return s + "*" if points < max(WINDOWS) else s

    # Right-justify every numeric column to its OWN max width, measured across all rows (data is
    # all in hand). A fixed ".6f%"/".2f%" suffix means aligning the strings aligns the decimals --
    # so negative signs and large/hot values can't shove the "(Nhr)" labels or later columns askew.
    colw = {n: max((len(pct_str(sums, pts, n)) for _, sums, pts in rows), default=0) for n in WINDOWS}
    annw = max((len(ann_str(sums, pts)) for _, sums, pts in rows), default=0)

    for ticker, sums, points in rows:
        parts = []
        for n in WINDOWS:
            cell = f"{pct_str(sums, points, n):>{colw[n]}}"   # pad first (color codes are zero-width)
            if use_color:
                idx = cell_tier(n, sums[n] * 100)
                if idx is not None:
                    cell = ansi_paint(cell, idx)
            parts.append(f"{cell} ({n}hr)")
        print(f"{ticker:<{tickw}}{'  '.join(parts)}  Annualized: {ann_str(sums, points):>{annw}}")

    print(footer)


def render_html(rows, footer):
    """Emit ONLY a <table> element (no surrounding page) with red-shaded funding cells,
    so another script can drop the fragment into its own HTML."""
    print("<table>")
    heads = "".join(f"<th>{n}hr</th>" for n in WINDOWS)
    print(f"  <thead><tr><th>Market</th>{heads}<th>Annualized</th></tr></thead>")
    print("  <tbody>")
    for ticker, sums, points in rows:
        tds = [f"<td>{html.escape(ticker)}</td>"]
        for n in WINDOWS:
            pct = sums[n] * 100
            mark = "*" if points < n else ""          # partial window (fewer than N hourly points)
            tds.append(f'<td style="{cell_style(n, pct)}">{pct:.6f}%{mark}</td>')
        ann = sums[max(WINDOWS)] * 365 * 100
        amark = "*" if points < max(WINDOWS) else ""
        tds.append(f'<td style="text-align:right;padding:2px 8px">{ann:.2f}%{amark}</td>')
        print("    <tr>" + "".join(tds) + "</tr>")
    print("  </tbody>")
    print("</table>")
    print(html.escape(footer))


def main():
    global BASE
    parser = argparse.ArgumentParser(description="Dashboard of Arcus 1h/8h/24h funding rates for ONLINE markets.")
    parser.add_argument("--delay", type=float, default=0.1,
                        help="seconds to pause before each request, to respect the rate limit (default 0.1)")
    parser.add_argument("--html", action="store_true",
                        help="output a <table> element (with red-shaded funding cells) instead of text")
    parser.add_argument("--color", choices=("auto", "always", "never"), default="auto",
                        help="ANSI red shading of the text output (same tiers as --html): "
                             "auto = on when stdout is a TTY (default), always, or never")
    parser.add_argument("--all", action="store_true",
                        help="also show markets sitting exactly at the default funding "
                             "(0.00125%%/hr; hidden by default as they carry no signal)")
    net = parser.add_mutually_exclusive_group(required=True)
    net.add_argument("--testnet", dest="network", action="store_const", const="testnet",
                     help="query the testnet server")
    net.add_argument("--staging", dest="network", action="store_const", const="staging",
                     help="query the staging server")
    net.add_argument("--mainnet", dest="network", action="store_const", const="mainnet",
                     help="query the mainnet server")
    args = parser.parse_args()
    if not math.isfinite(args.delay) or args.delay < 0:
        raise SystemExit("funding_dashboard: --delay must be a finite value >= 0.")
    BASE = NETWORKS[args.network]

    start = time.time()
    markets = fetch_online_markets(args.delay)

    rows = []
    hidden = 0
    for m in markets:
        display = m.get("marketDisplayName")
        ticker = str(m.get("baseAsset") or str(display).split("-")[0])
        sums, points = window_sums(display, args.delay)
        if not args.all and is_default_funding(sums, DEFAULT_HOURLY):
            hidden += 1
            continue
        rows.append((ticker, sums, points))

    # UTC generation time (matches the shell `date` look) + wall-clock runtime, plus operational counts.
    runtime = round(time.time() - start)
    g = time.gmtime()
    stamp = time.strftime("%a %b %e %I:%M:%S %p", g) + " UTC " + time.strftime("%Y", g)
    partial = sum(1 for _, _, p in rows if p < max(WINDOWS))   # rows with an incomplete 24h window
    footer = (f"Generated: {stamp} / Runtime {runtime} seconds / Backoffs: {BACKOFFS} "
              f"/ Hidden: {hidden} at default funding / Partial: {partial} marked *")

    if args.html:
        render_html(rows, footer)
    else:
        use_color = args.color == "always" or (args.color == "auto" and sys.stdout.isatty())
        render_text(rows, footer, use_color)


if __name__ == "__main__":
    main()
