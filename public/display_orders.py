"""
Display orders for an account, newest-first.

  python3 display_orders.py <eth_address>                 # every order, any status
  python3 display_orders.py <eth_address> --status OPEN    # only OPEN orders
  python3 display_orders.py <eth_address> --status CANCELED --limit 50

Uses GET /v1/orders (order history: open, filled, canceled, rejected).
This is a public, account-scoped read -- it takes only the `address`
query parameter and needs NO signature, so this display tool needs just
the address, not the creds file. The API has no server-side status filter
(only limit/from/to), so --status is applied locally; output is sorted
newest-first locally too (not trusting API ordering).
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
BASE = None   # set in main() from the required --testnet/--staging selector
ADDR_RE = re.compile(r"^0x[0-9a-fA-F]{40}$")

# Status values the venue can report, per the order schema.
STATUSES = [
    "PENDING", "OPEN", "PARTIALLY_FILLED", "FILLED", "CANCELED",
    "MARGIN_CANCELED", "REJECTED", "UNTRIGGERED", "TPSL_PLACED",
    "TPSL_TRIGGERED", "TPSL_CANCELED", "LIQUIDATED", "ADL", "ACK",
    "CANCEL_ACKNOWLEDGED", "CANCEL_ALL_ACKNOWLEDGED", "CANCEL_PENDING",
    "ERROR",
]

# Fields emitted by --condensed (raw, one CSV row per order).
CONDENSED_KEYS = [
    "marketDisplayName", "side", "status", "type",
    "price", "originalSize", "remainingSize", "orderId", "clientId",
]


def limit_arg(s):
    """argparse type: an integer in [1, 1000] (the API's max)."""
    v = int(s)
    if not 1 <= v <= 1000:
        raise argparse.ArgumentTypeError("must be between 1 and 1000")
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


def created_key(order):
    """Sort key by createdAt (desc via reverse=True); missing/bad sorts oldest."""
    try:
        return int(order.get("createdAt"))
    except (TypeError, ValueError):
        return -1


def partially_filled(order):
    """True when some -- but not all -- of the order has filled.

    i.e. 0 < remainingSize < originalSize. A fully filled order has
    remainingSize 0 (not partial); an untouched order has remaining == size.
    Uses Decimal (sizes are decimal strings) to avoid float precision issues.
    """
    try:
        remaining = Decimal(str(order.get("remainingSize")))
        size = Decimal(str(order.get("originalSize")))
    except (InvalidOperation, TypeError, ValueError):
        return False
    return 0 < remaining < size


def require_dict(data, what):
    """A decoded JSON body that must be an object -> raise a clean CLI error if the server returned
    null / a list / a scalar instead (so the .get(...) below can't AttributeError)."""
    if not isinstance(data, dict):
        raise SystemExit(f"display_orders: unexpected {what} response shape (not a JSON object)")
    return data


def fetch_orders(address, limit):
    """GET /v1/orders, turning network/HTTP/JSON failures into clean CLI errors."""
    query = urllib.parse.urlencode({"address": address, "limit": limit})
    req = urllib.request.Request(f"{BASE}/v1/orders?{query}", method="GET")
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return require_dict(json.loads(r.read() or b"{}"), "orders").get("orders") or []
    except urllib.error.HTTPError as e:
        try:
            msg = json.loads(e.read() or b"{}").get("error", "")
        except (ValueError, TypeError, AttributeError):   # AttributeError = non-dict error body
            msg = ""
        raise SystemExit(f"HTTP {e.code}: {msg or 'request failed'}")
    except urllib.error.URLError as e:
        raise SystemExit(f"display_orders: could not reach {BASE}: {e.reason}")
    except (TimeoutError, OSError) as e:
        raise SystemExit(f"display_orders: network error: {e}")
    except json.JSONDecodeError as e:
        raise SystemExit(f"display_orders: invalid JSON from server: {e}")


def print_table(orders, address, label):
    """Aligned table with column widths sized to the data (handles long values)."""
    has_partial = any(partially_filled(o) for o in orders)

    # (header, alignment, value-getter). A 1-char '*' flag column is inserted
    # after REMAINING only when something is partially filled.
    cols = [
        ("CREATED (UTC)", "<", lambda o: when(o.get("createdAt"))),
        ("MARKET", "<", lambda o: str(o.get("marketDisplayName", ""))),
        ("SIDE", "<", lambda o: str(o.get("side", ""))),
        ("STATUS", "<", lambda o: str(o.get("status", ""))),
        ("TYPE", "<", lambda o: str(o.get("type", ""))),
        ("PRICE", ">", lambda o: str(o.get("price", ""))),
        ("SIZE", ">", lambda o: str(o.get("originalSize", ""))),
        ("REMAINING", ">", lambda o: str(o.get("remainingSize", ""))),
    ]
    if has_partial:
        cols.append(("", "<", lambda o: "*" if partially_filled(o) else ""))
    cols += [
        ("ORDER ID", "<", lambda o: str(o.get("orderId", ""))),
        ("CLIENTID", "<", lambda o: str(o.get("clientId", ""))),
    ]

    widths = []
    for header, _, get in cols:
        widths.append(max(len(header), max((len(get(o)) for o in orders), default=0)))

    legend = "    (* = partially filled)" if has_partial else ""
    print(f"{len(orders)} order(s) [{label}] for {address}{legend}\n")

    head = "  ".join(f"{h:{a}{w}}" for (h, a, _), w in zip(cols, widths))
    print(head)
    print("-" * len(head))
    for o in orders:
        print("  ".join(f"{get(o):{a}{w}}" for (_, a, get), w in zip(cols, widths)))


def main():
    global BASE
    parser = argparse.ArgumentParser(description="Display account orders.")
    parser.add_argument("address", help="Ethereum address of the account to display")
    parser.add_argument("--status", choices=STATUSES,
                        help="show only orders in this status (default: all)")
    parser.add_argument("--limit", type=limit_arg, default=1000,
                        help="max orders to fetch, 1-1000 (default/max 1000)")
    parser.add_argument("--condensed", action="store_true",
                        help="machine-readable: one CSV row per order "
                             "(market,side,status,type,price,size,remaining,orderid,clientid), "
                             "raw values, no header/padding/'*' marker")
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
        raise SystemExit(f"display_orders: invalid Ethereum address {args.address!r} "
                         f"(expected 0x + 40 hex chars).")
    if args.header and not args.condensed:
        raise SystemExit("display_orders: --header requires --condensed.")

    orders = fetch_orders(args.address, args.limit)
    if args.status:
        orders = [o for o in orders if o.get("status") == args.status]
    orders.sort(key=created_key, reverse=True)   # enforce newest-first locally

    if args.condensed:
        writer = csv.writer(sys.stdout, lineterminator="\n")
        if args.header:
            writer.writerow(CONDENSED_KEYS)
        for o in orders:
            writer.writerow([o.get(k, "") for k in CONDENSED_KEYS])
        return

    label = args.status or "ALL"
    if not orders:
        print(f"0 order(s) [{label}] for {args.address}\n")
        return
    print_table(orders, args.address, label)


if __name__ == "__main__":
    main()
