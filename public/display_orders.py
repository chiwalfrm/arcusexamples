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
import os
import re
import sys
import urllib.parse
from decimal import Decimal, InvalidOperation
from arcus_common_public import NETWORKS, created_key, get_json_dict, when   # shared public helpers (formerly local copies)

BASE = None   # set in main() from the required --testnet/--staging/--mainnet selector
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


def partially_filled(order):
    """True when some -- but not all -- of the order has filled.

    i.e. 0 < remainingSize < originalSize. A fully filled order has
    remainingSize 0 (not partial); an untouched order has remaining == size.
    Uses Decimal (sizes are decimal strings) to avoid float precision issues.
    """
    try:
        remaining = Decimal(str(order.get("remainingSize")))
        size = Decimal(str(order.get("originalSize")))
        if not (remaining.is_finite() and size.is_finite()):   # Decimal(str("NaN")) constructs fine, but the ordered
            return False                                        # compare below RAISES InvalidOperation on a NaN -> crash
        return 0 < remaining < size
    except (InvalidOperation, TypeError, ValueError):
        return False


def fetch_orders(address, limit):
    """GET /v1/orders, turning network/HTTP/JSON failures into clean CLI errors."""
    query = urllib.parse.urlencode({"address": address, "limit": limit})
    # Shared retrying reader: Retry-After/backoff on 429 (incl. Cloudflare 1015) + 5xx, clean CLI errors,
    # require_dict included -- so this tool is no longer fragile under a rate-limit burst in cron/ops.
    body = get_json_dict(f"{BASE}/v1/orders?{query}", "orders", "display_orders")
    # Validate the shape (mirrors display_fills/funding): a MISSING 'orders' key is "no orders" -> [],
    # but a PRESENT-but-non-list value (dict/str/number) must be a clean error, not fall through `or []`
    # (truthy non-lists slip past that) and crash the downstream sort/comprehension with AttributeError.
    orders = body.get("orders")
    if orders is None:
        return []
    if not isinstance(orders, list):
        raise SystemExit("display_orders: unexpected /v1/orders response ('orders' is not a list).")
    return [o for o in orders if isinstance(o, dict)]   # drop non-dict elements so downstream .get()/sort-key can't crash


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
    try:
        main()
    except BrokenPipeError:
        # A downstream reader closed early (e.g. `... | head`). Point stdout at devnull so the interpreter's
        # shutdown flush can't re-raise BrokenPipeError, then exit cleanly -- this tool is meant for piping.
        try:
            os.dup2(os.open(os.devnull, os.O_WRONLY), sys.stdout.fileno())
        except Exception:
            pass
        sys.exit(0)
