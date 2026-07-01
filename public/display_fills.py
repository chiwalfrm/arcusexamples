"""
Display fills (trade history) for an account, newest-first.

  python3 display_fills.py <eth_address>                     # latest fills (up to 1000)
  python3 display_fills.py <eth_address> --market BTC-USD     # only that market
  python3 display_fills.py <eth_address> --from 1782000000000 --to 1782600000000
  python3 display_fills.py <eth_address> --limit 50 --condensed

Uses GET /v1/fills (fill/trade history). This is a public, account-scoped read -- it
takes only the `address` query parameter and needs NO signature, so this display tool
needs just the address, not the creds file. The API has NO server-side market filter
(only limit/from/to), so --market is resolved to a canonical marketId and applied
locally WITHIN the fetched window; use --from/--to (epoch ms) to walk older history.
Output is sorted newest-first locally (not trusting API ordering).
"""

import argparse
import csv
import json
import re
import sys
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation

NETWORKS = {
    "testnet": "https://api.testnet.arcus.xyz",
    "staging": "https://api.staging.arcus.xyz",
    "mainnet": "https://api.arcus.xyz",       # live 2026-06-25 (reads only for now)
}
BASE = None   # set in main() from the required --testnet/--staging/--mainnet selector
ADDR_RE = re.compile(r"^0x[0-9a-fA-F]{40}$")

# Fields emitted by --condensed (raw, one CSV row per fill; no header, per the display_* convention).
CONDENSED_KEYS = [
    "createdAt", "marketDisplayName", "side", "size", "price", "fee",
    "role", "closedPnl", "positionEffect", "tradeId", "orderId",
]


def limit_arg(s):
    """argparse type: an integer in [1, 1000] (the API's max)."""
    v = int(s)
    if not 1 <= v <= 1000:
        raise argparse.ArgumentTypeError("must be between 1 and 1000")
    return v


def epoch_ms_arg(s):
    """argparse type: a non-negative epoch-milliseconds integer."""
    v = int(s)
    if v < 0:
        raise argparse.ArgumentTypeError("must be a non-negative epoch-ms timestamp")
    return v


def when(micros):
    """Epoch microseconds -> 'YYYY-MM-DD HH:MM:SS' UTC, or '-' if absent/invalid."""
    if micros is None or micros == "":
        return "-"
    try:
        return datetime.fromtimestamp(int(micros) / 1_000_000, timezone.utc) \
            .strftime("%Y-%m-%d %H:%M:%S")
    except (ValueError, TypeError, OverflowError, OSError):
        return "-"


def created_key(fill):
    """Sort key by createdAt (desc via reverse=True); missing/bad sorts oldest."""
    try:
        return int(fill.get("createdAt"))
    except (TypeError, ValueError):
        return -1


def dec(v):
    """Parse a decimal string -> Decimal, or None if absent/invalid/non-finite."""
    try:
        d = Decimal(str(v))
    except (InvalidOperation, TypeError, ValueError):
        return None
    return d if d.is_finite() else None


def _get_json(url, what):
    """GET url -> parsed JSON dict, turning network/HTTP/JSON failures into clean CLI errors."""
    req = urllib.request.Request(url, method="GET")
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
        raise SystemExit(f"display_fills: could not reach {BASE}: {e.reason}")
    except (TimeoutError, OSError) as e:
        raise SystemExit(f"display_fills: network error fetching {what}: {e}")
    except json.JSONDecodeError as e:
        raise SystemExit(f"display_fills: invalid JSON from {what}: {e}")
    if not isinstance(data, dict):
        raise SystemExit(f"display_fills: unexpected {what} response (expected a JSON object).")
    return data


def resolve_market_id(market):
    """Resolve --market (display name case-insensitive OR numeric marketId) to a canonical marketId
    string via GET /v1/markets. FAILS on an unknown market -- a typo must not silently show 0 fills."""
    markets = _get_json(f"{BASE}/v1/markets", "markets").get("markets")
    if not isinstance(markets, list):
        raise SystemExit("display_fills: unexpected /v1/markets response (no 'markets' list).")
    for m in markets:
        if (str(market).upper() == str(m.get("marketDisplayName", "")).upper()
                or str(market) == str(m.get("marketId"))):
            return str(m.get("marketId"))
    raise SystemExit(f"display_fills: unknown market {market!r} (not found in /v1/markets).")


def fetch_fills(address, limit, from_ms=None, to_ms=None):
    """GET /v1/fills (newest-first). Validates the response shape and returns the fills list."""
    q = {"address": address, "limit": limit}
    if from_ms is not None:
        q["from"] = from_ms
    if to_ms is not None:
        q["to"] = to_ms
    data = _get_json(f"{BASE}/v1/fills?{urllib.parse.urlencode(q)}", "fills")
    fills = data.get("fills")
    if fills is None:
        return []
    if not isinstance(fills, list):
        raise SystemExit("display_fills: unexpected /v1/fills response ('fills' is not a list).")
    return fills


def print_table(fills, address, label, note):
    """Aligned table with column widths sized to the data, plus a fee/PnL totals footer."""
    # (header, alignment, value-getter). closedPnl/positionEffect are optional on REST -> "".
    cols = [
        ("CREATED (UTC)", "<", lambda f: when(f.get("createdAt"))),
        ("MARKET", "<", lambda f: str(f.get("marketDisplayName", ""))),
        ("SIDE", "<", lambda f: str(f.get("side", ""))),
        ("SIZE", ">", lambda f: str(f.get("size", ""))),
        ("PRICE", ">", lambda f: str(f.get("price", ""))),
        ("FEE", ">", lambda f: str(f.get("fee", ""))),
        ("ROLE", "<", lambda f: str(f.get("role", ""))),
        ("CLOSEDPNL", ">", lambda f: str(f.get("closedPnl", ""))),
        ("EFFECT", "<", lambda f: str(f.get("positionEffect", ""))),
        ("TRADE ID", "<", lambda f: str(f.get("tradeId", ""))),
        ("ORDER ID", "<", lambda f: str(f.get("orderId", ""))),
    ]
    widths = [max(len(h), max((len(get(f)) for f in fills), default=0)) for h, _, get in cols]

    print(f"{len(fills)} fill(s) [{label}] for {address}{note}\n")
    head = "  ".join(f"{h:{a}{w}}" for (h, a, _), w in zip(cols, widths))
    print(head)
    print("-" * len(head))
    for f in fills:
        print("  ".join(f"{get(f):{a}{w}}" for (_, a, get), w in zip(cols, widths)))

    total_fee = sum((dec(f.get("fee")) or Decimal(0)) for f in fills)
    total_pnl = sum((dec(f.get("closedPnl")) or Decimal(0)) for f in fills)
    print("-" * len(head))
    print(f"  TOTAL  fees {total_fee:,.6f}   realized PnL {total_pnl:,.6f}   over {len(fills)} fill(s)")


def main():
    global BASE
    parser = argparse.ArgumentParser(description="Display account fills (trade history).")
    parser.add_argument("address", help="Ethereum address of the account to display")
    parser.add_argument("--market",
                        help="show only fills in this market (display name or marketId; default: all)")
    parser.add_argument("--limit", type=limit_arg, default=1000,
                        help="max fills to fetch, 1-1000 (default/max 1000)")
    parser.add_argument("--from", dest="from_ms", type=epoch_ms_arg, metavar="EPOCH_MS",
                        help="only fills at/after this start time (epoch ms, inclusive)")
    parser.add_argument("--to", dest="to_ms", type=epoch_ms_arg, metavar="EPOCH_MS",
                        help="only fills at/before this end time (epoch ms, inclusive)")
    parser.add_argument("--condensed", action="store_true",
                        help="machine-readable: one CSV row per fill "
                             "(createdAt,market,side,size,price,fee,role,closedPnl,effect,tradeId,orderId), "
                             "raw values, no header/padding/totals")
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
        raise SystemExit(f"display_fills: invalid Ethereum address {args.address!r} "
                         f"(expected 0x + 40 hex chars).")
    if args.from_ms is not None and args.to_ms is not None and args.from_ms > args.to_ms:
        raise SystemExit("display_fills: --from must be <= --to.")
    if args.header and not args.condensed:
        raise SystemExit("display_fills: --header requires --condensed.")

    # Validate/resolve --market up front (a typo must FAIL, not silently return 0 fills). The fills
    # API has no server-side market filter, so we filter locally by the canonical marketId.
    target_mid = resolve_market_id(args.market) if args.market else None

    raw = fetch_fills(args.address, args.limit, args.from_ms, args.to_ms)
    truncated = len(raw) >= args.limit            # a full page back -> older fills may exist
    fills = [f for f in raw if target_mid is None or str(f.get("marketId")) == target_mid]
    fills.sort(key=created_key, reverse=True)      # enforce newest-first locally

    if args.condensed:
        writer = csv.writer(sys.stdout, lineterminator="\n")
        if args.header:
            writer.writerow(CONDENSED_KEYS)
        for f in fills:
            writer.writerow([f.get(k, "") for k in CONDENSED_KEYS])
        return

    label = args.market.upper() if args.market else "ALL"
    # Be honest about scope: --market filters only WITHIN the fetched (possibly truncated) page.
    note = ""
    if truncated:
        if target_mid is not None:
            note = (f"  (within the latest {args.limit} account-wide fills; older {label} fills may "
                    f"exist -- narrow with --from/--to)")
        else:
            note = f"  (latest {args.limit} shown; older fills exist -- use --from/--to or a larger --limit)"
    if not fills:
        print(f"0 fill(s) [{label}] for {args.address}{note}\n")
        return
    print_table(fills, args.address, label, note)


if __name__ == "__main__":
    main()
