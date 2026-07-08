"""
Display open positions for an account.

  python3 display_positions.py <eth_address>
  python3 display_positions.py <eth_address> --condensed   # CSV market,quantity (for scripts)

Uses GET /v1/positions, a public account-scoped read that takes only the
`address` query parameter and needs NO signature -- so this display tool needs
just the address, not the creds file (same as display_orders.py).

The endpoint returns positions keyed by stringified marketId; an account with
no open positions returns an empty object `{}`. Mark price comes from
`oraclePx` ("0" when no oracle is available -- shown as "-" here, since the
venue then falls back to the entry price for its notional / PnL math).
"""

import argparse
import csv
import json
import re
import sys
import urllib.error
import urllib.parse
import urllib.request
from decimal import Decimal, InvalidOperation

NETWORKS = {
    "testnet": "https://api.testnet.arcus.xyz",
    "staging": "https://api.staging.arcus.xyz",
    "mainnet": "https://api.arcus.xyz",       # live 2026-06-25 (reads only for now)
}
BASE = None   # set in main() from the required --testnet/--staging selector
ADDR_RE = re.compile(r"^0x[0-9a-fA-F]{40}$")


def dec(value):
    """Parse a decimal string -> Decimal, or None if not numeric."""
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None


def num(value, decimals=2):
    """Decimal-string -> fixed-precision, comma-grouped; '-' if not numeric.

    Uses Decimal (not float) -- sizes/PnL/notional are decimal strings, so this
    avoids binary floating-point rounding artifacts.
    """
    d = dec(value)
    return f"{d:,.{decimals}f}" if d is not None else "-"


def funding_of(p):
    return (p.get("cumulativeFunding") or {}).get("sinceOpen")


def mark_str(p):
    """MARK column: oraclePx, or '-' when there's no oracle (oraclePx == 0/absent)."""
    d = dec(p.get("oraclePx"))
    if d is None or d == 0:
        return "-"
    return num(p.get("oraclePx"))


def market_id_key(p):
    """Sort key by NUMERIC marketId (so 2 < 10); missing/non-numeric sort last."""
    try:
        return (0, int(p.get("marketId")))
    except (TypeError, ValueError):
        return (1, 0)


# (header, alignment, value-getter -> display string). Widths are sized to data.
COLS = [
    ("MARKET", "<", lambda p: str(p.get("marketDisplayName", ""))),
    ("SIDE", "<", lambda p: str(p.get("side", ""))),
    ("SIZE", ">", lambda p: num(p.get("size"), 4)),
    ("ENTRY", ">", lambda p: num(p.get("averageEntryPrice"))),
    ("MARK", ">", mark_str),
    ("LEV", ">", lambda p: str(p.get("leverage", ""))),
    ("MARGIN", "<", lambda p: str(p.get("marginMode", ""))),
    ("NOTIONAL", ">", lambda p: num(p.get("positionValueNotional"))),
    ("uPnL", ">", lambda p: num(p.get("unrealizedPnl"))),
    ("FUNDING", ">", lambda p: num(funding_of(p))),
]


def fetch_positions(address):
    """GET /v1/positions -> dict keyed by marketId; clean CLI errors on failure."""
    query = urllib.parse.urlencode({"address": address})
    req = urllib.request.Request(f"{BASE}/v1/positions?{query}", method="GET")
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            data = json.loads(r.read() or b"{}")
    except urllib.error.HTTPError as e:
        try:
            msg = json.loads(e.read() or b"{}").get("error", "")
        except (ValueError, TypeError):
            msg = ""
        raise SystemExit(f"HTTP {e.code}: {msg or 'request failed'}")
    except urllib.error.URLError as e:
        raise SystemExit(f"display_positions: could not reach {BASE}: {e.reason}")
    except (TimeoutError, OSError) as e:
        raise SystemExit(f"display_positions: network error: {e}")
    except json.JSONDecodeError as e:
        raise SystemExit(f"display_positions: invalid JSON from server: {e}")

    if not isinstance(data, dict):          # a non-object 200 body would crash data.get below
        raise SystemExit("display_positions: unexpected response (expected a JSON object).")
    positions = data.get("positions")
    if positions is None:
        return {}
    if not isinstance(positions, dict):
        raise SystemExit("display_positions: unexpected 'positions' shape (expected object).")
    return positions


def main():
    global BASE
    parser = argparse.ArgumentParser(description="Display open positions for an account.")
    parser.add_argument("address", help="Ethereum address of the account to display")
    parser.add_argument("--condensed", action="store_true",
                        help="machine-readable: CSV 'market,quantity' per line, "
                             "raw values (no header, no totals)")
    parser.add_argument("--header", action="store_true",
                        help="with --condensed, emit a CSV header row first "
                             "(error if used without --condensed)")
    net = parser.add_mutually_exclusive_group(required=True)
    net.add_argument("--testnet", dest="network", action="store_const", const="testnet",
                     help="query the testnet server")
    net.add_argument("--staging", dest="network", action="store_const", const="staging",
                     help="query the staging server")
    net.add_argument("--mainnet", dest="network", action="store_const", const="mainnet",
                     help="query the mainnet server")
    args = parser.parse_args()
    BASE = NETWORKS[args.network]
    if not ADDR_RE.match(args.address):
        raise SystemExit(f"display_positions: invalid Ethereum address {args.address!r} "
                         f"(expected 0x + 40 hex chars).")
    if args.header and not args.condensed:
        raise SystemExit("display_positions: --header requires --condensed.")

    positions = sorted((p for p in fetch_positions(args.address).values() if isinstance(p, dict)),
                       key=market_id_key)   # drop any null/non-dict position value defensively

    if args.condensed:
        # Raw signed quantity straight from the API, CSV-escaped for downstream scripts.
        writer = csv.writer(sys.stdout, lineterminator="\n")
        if args.header:
            writer.writerow(["marketDisplayName", "size"])
        for p in positions:
            writer.writerow([p.get("marketDisplayName", ""), p.get("size", "")])
        return

    print(f"{len(positions)} open position(s) for {args.address}\n")
    if not positions:
        return

    widths = [max(len(h), max((len(get(p)) for p in positions), default=0))
              for h, _, get in COLS]
    header = "  ".join(f"{h:{a}{w}}" for (h, a, _), w in zip(COLS, widths))
    print(header)
    print("-" * len(header))
    for p in positions:
        print("  ".join(f"{get(p):{a}{w}}" for (_, a, get), w in zip(COLS, widths)))

    total_pnl = sum((dec(p.get("unrealizedPnl")) or Decimal(0) for p in positions), Decimal(0))
    total_funding = sum((dec(funding_of(p)) or Decimal(0) for p in positions), Decimal(0))
    print("-" * len(header))
    # Right-align both totals in a shared field so their cents line up vertically.
    NUMW = 20
    print(f"{'Total unrealized PnL:':21} {total_pnl:>{NUMW},.2f}")
    print(f"{'Total funding:':21} {total_funding:>{NUMW},.2f}")


if __name__ == "__main__":
    main()
