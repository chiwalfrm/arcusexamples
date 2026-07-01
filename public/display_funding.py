"""
Display funding payments for an account, newest-first.

  python3 display_funding.py <eth_address>                      # last 30 days (API default)
  python3 display_funding.py <eth_address> --market BTC-USD      # only that market
  python3 display_funding.py <eth_address> --from 1782000000000  # walk older history
  python3 display_funding.py <eth_address> --limit 50 --condensed

Uses GET /v1/funding (per-account funding payment history). This is a public, account-scoped
read -- it takes only the `address` query parameter and needs NO signature, so this display
tool needs just the address, not the creds file. Sign convention: payment positive = RECEIVED,
negative = PAID. The API has NO server-side market filter (only from/to/limit) and DEFAULTS to
the last 30 days when --from is omitted, so --market is resolved to a canonical marketId and
applied locally WITHIN the fetched window; use --from/--to (epoch ms) to widen/walk history.
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

# Fields emitted by --condensed (raw, one CSV row per payment; no header, per the display_* convention).
CONDENSED_KEYS = ["time", "marketDisplayName", "fundingRate", "size", "payment"]


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
    """Epoch MICROseconds -> 'YYYY-MM-DD HH:MM:SS' UTC, or '-' if absent/invalid.

    NB the Arcus API mixes units (verified live + docs): RESPONSE timestamps -- this `time`
    field -- are epoch microseconds ("user-facing timestamps are now microseconds"), while the
    REQUEST filters --from/--to are epoch MILLISECONDS. So divide by 1e6 here; pass --from/--to
    through as-is. A real value 1782583200000000 -> 2026-06-27 (µs); /1e3 would be year 58457.
    """
    if micros is None or micros == "":
        return "-"
    try:
        return datetime.fromtimestamp(int(micros) / 1_000_000, timezone.utc) \
            .strftime("%Y-%m-%d %H:%M:%S")
    except (ValueError, TypeError, OverflowError, OSError):
        return "-"


def time_key(payment):
    """Sort key by time (desc via reverse=True); missing/bad sorts oldest."""
    try:
        return int(payment.get("time"))
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
    """GET url -> parsed JSON object, turning network/HTTP/JSON failures into clean CLI errors."""
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
        raise SystemExit(f"display_funding: could not reach {BASE}: {e.reason}")
    except (TimeoutError, OSError) as e:
        raise SystemExit(f"display_funding: network error fetching {what}: {e}")
    except json.JSONDecodeError as e:
        raise SystemExit(f"display_funding: invalid JSON from {what}: {e}")
    if not isinstance(data, dict):
        raise SystemExit(f"display_funding: unexpected {what} response (expected a JSON object).")
    return data


def resolve_market_id(market):
    """Resolve --market (display name case-insensitive OR numeric marketId) to a canonical marketId
    string via GET /v1/markets. FAILS on an unknown market -- a typo must not silently show 0 rows."""
    markets = _get_json(f"{BASE}/v1/markets", "markets").get("markets")
    if not isinstance(markets, list):
        raise SystemExit("display_funding: unexpected /v1/markets response (no 'markets' list).")
    for m in markets:
        if (str(market).upper() == str(m.get("marketDisplayName", "")).upper()
                or str(market) == str(m.get("marketId"))):
            return str(m.get("marketId"))
    raise SystemExit(f"display_funding: unknown market {market!r} (not found in /v1/markets).")


def fetch_funding(address, limit, from_ms=None, to_ms=None):
    """GET /v1/funding (newest-first). Validates the response shape and returns the payments list."""
    q = {"address": address, "limit": limit}
    if from_ms is not None:
        q["from"] = from_ms
    if to_ms is not None:
        q["to"] = to_ms
    data = _get_json(f"{BASE}/v1/funding?{urllib.parse.urlencode(q)}", "funding")
    payments = data.get("fundingPayments")
    if payments is None:
        return []
    if not isinstance(payments, list):
        raise SystemExit("display_funding: unexpected /v1/funding response ('fundingPayments' is not a list).")
    return payments


def print_table(payments, address, label, note):
    """Aligned table with column widths sized to the data, plus a received/paid/net footer."""
    cols = [
        ("TIME (UTC)", "<", lambda p: when(p.get("time"))),
        ("MARKET", "<", lambda p: str(p.get("marketDisplayName", ""))),
        ("FUNDING RATE", ">", lambda p: str(p.get("fundingRate", ""))),
        ("SIZE", ">", lambda p: str(p.get("size", ""))),
        ("PAYMENT", ">", lambda p: str(p.get("payment", ""))),
    ]
    widths = [max(len(h), max((len(get(p)) for p in payments), default=0)) for h, _, get in cols]

    print(f"{len(payments)} funding payment(s) [{label}] for {address}{note}\n")
    head = "  ".join(f"{h:{a}{w}}" for (h, a, _), w in zip(cols, widths))
    print(head)
    print("-" * len(head))
    for p in payments:
        print("  ".join(f"{get(p):{a}{w}}" for (_, a, get), w in zip(cols, widths)))

    # payment: positive = received, negative = paid. Net = received - paid = sum of all.
    amounts = [dec(p.get("payment")) or Decimal(0) for p in payments]
    received = sum((a for a in amounts if a > 0), Decimal(0))
    paid = sum((-a for a in amounts if a < 0), Decimal(0))
    net = received - paid
    print("-" * len(head))
    print(f"  TOTAL  net {net:+,.6f}   (received {received:,.6f}, paid {paid:,.6f})   "
          f"over {len(payments)} payment(s)")


def main():
    global BASE
    parser = argparse.ArgumentParser(description="Display account funding payments.")
    parser.add_argument("address", help="Ethereum address of the account to display")
    parser.add_argument("--market",
                        help="show only payments in this market (display name or marketId; default: all)")
    parser.add_argument("--limit", type=limit_arg, default=1000,
                        help="max payments to fetch, 1-1000 (default/max 1000)")
    # NB --from/--to are epoch MILLISECONDS (API request-filter unit), even though the response
    # `time` field is microseconds -- this asymmetry is the Arcus API's, not a bug here (see when()).
    parser.add_argument("--from", dest="from_ms", type=epoch_ms_arg, metavar="EPOCH_MS",
                        help="only payments at/after this start time (epoch ms, inclusive); "
                             "omit and the API defaults to the last 30 days")
    parser.add_argument("--to", dest="to_ms", type=epoch_ms_arg, metavar="EPOCH_MS",
                        help="only payments at/before this end time (epoch ms, inclusive; default: now)")
    parser.add_argument("--condensed", action="store_true",
                        help="machine-readable: one CSV row per payment "
                             "(time,market,fundingRate,size,payment), raw values, no header/padding/totals")
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
        raise SystemExit(f"display_funding: invalid Ethereum address {args.address!r} "
                         f"(expected 0x + 40 hex chars).")
    if args.from_ms is not None and args.to_ms is not None and args.from_ms > args.to_ms:
        raise SystemExit("display_funding: --from must be <= --to.")
    if args.header and not args.condensed:
        raise SystemExit("display_funding: --header requires --condensed.")

    # Validate/resolve --market up front (a typo must FAIL, not silently return 0 rows). The funding
    # API has no server-side market filter, so we filter locally by the canonical marketId.
    target_mid = resolve_market_id(args.market) if args.market else None

    raw = fetch_funding(args.address, args.limit, args.from_ms, args.to_ms)
    truncated = len(raw) >= args.limit            # a full page back -> older payments may exist
    payments = [p for p in raw if target_mid is None or str(p.get("marketId")) == target_mid]
    payments.sort(key=time_key, reverse=True)      # enforce newest-first locally

    if args.condensed:
        writer = csv.writer(sys.stdout, lineterminator="\n")
        if args.header:
            writer.writerow(CONDENSED_KEYS)
        for p in payments:
            writer.writerow([p.get(k, "") for k in CONDENSED_KEYS])
        return

    label = args.market.upper() if args.market else "ALL"
    # Be honest about scope: the default window is only the last 30 days, and --market filters only
    # WITHIN the fetched (possibly truncated) page.
    notes = []
    if args.from_ms is None:
        notes.append("default window: last 30 days -- pass --from (epoch ms) for older history")
    if truncated:
        if target_mid is not None:
            notes.append(f"latest {args.limit} payments scanned; older {label} payments may exist")
        else:
            notes.append(f"latest {args.limit} shown; older payments may exist")
    note = ("  (" + "; ".join(notes) + ")") if notes else ""
    if not payments:
        print(f"0 funding payment(s) [{label}] for {args.address}{note}\n")
        return
    print_table(payments, args.address, label, note)


if __name__ == "__main__":
    main()
