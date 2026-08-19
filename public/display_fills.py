"""
Display fills (trade history) for an account, newest-first.

  python3 display_fills.py <eth_address>                     # latest fills (up to 1000)
  python3 display_fills.py <eth_address> --market BTC-USD     # only that market
  python3 display_fills.py <eth_address> --from 1782000000000000 --to 1782600000000000
  python3 display_fills.py <eth_address> --limit 50 --condensed
  python3 display_fills.py <eth_address> --limit unlimited     # page the full history

Uses GET /v1/fills (fill/trade history). This is a public, account-scoped read -- it
takes only the `address` query parameter and needs NO signature, so this display tool
needs just the address, not the creds file. The API has NO server-side market filter
(only limit/from/to), so --market is resolved to a canonical marketId and applied
locally WITHIN the fetched window; use --from/--to (epoch MICROseconds -- the createdAt
unit, NOT ms) to walk older history.
Output is sorted newest-first locally (not trusting API ordering).
"""

import argparse
import csv
import json
import os
import re
import sys
import urllib.error
import urllib.parse
import urllib.request
from decimal import Decimal
from functools import partial
from arcus_common_public import NETWORKS, UNLIMITED, created_key, dec, epoch_us_arg, get_json_dict, limit_arg, page_pace_delay, resolve_market_id, when   # shared public helpers (formerly local copies)

_get_json = partial(get_json_dict, prog="display_fills")   # get_json + require_dict, this tool's prog


BASE = None   # set in main() from the required --testnet/--staging/--mainnet selector
ADDR_RE = re.compile(r"^0x[0-9a-fA-F]{40}$")
PAGE_SIZE = 1000          # API max per request; page size used by --limit unlimited

# Fields emitted by --condensed (raw, one CSV row per fill; no header, per the display_* convention).
CONDENSED_KEYS = [
    "createdAt", "marketDisplayName", "side", "size", "price", "fee",
    "role", "closedPnl", "positionEffect", "tradeId", "orderId",
]


def fetch_fills(address, limit, from_us=None, to_us=None):
    """GET /v1/fills (newest-first). Validates the response shape and returns the fills list.
    from_us/to_us are epoch MICROseconds (the /v1/fills from/to unit, same as createdAt)."""
    q = {"address": address, "limit": limit}
    if from_us is not None:
        q["from"] = from_us
    if to_us is not None:
        q["to"] = to_us
    data = _get_json(f"{BASE}/v1/fills?{urllib.parse.urlencode(q)}", "fills")
    fills = data.get("fills")
    if fills is None:
        return []
    if not isinstance(fills, list):
        raise SystemExit("display_fills: unexpected /v1/fills response ('fills' is not a list).")
    return [f for f in fills if isinstance(f, dict)]   # drop any non-dict element so downstream .get() can't crash


def iter_fills_pages(address, from_us=None, to_us=None):
    """--limit unlimited, as a GENERATOR: yield each page's FRESH (deduped) fills LIST, newest-first, as it
    pages BACKWARD via the `to` cursor -- so a caller can STREAM output instead of buffering the whole history
    before the first line. /v1/fills has only from/to bounds; `from` (if set) is a SERVER-SIDE lower bound so
    paging terminates at it. from/to/createdAt are all epoch MICROseconds (same unit), so the cursor is the
    oldest createdAt directly -- NO conversion. `to` is INCLUSIVE (closes over the microsecond it names), so the
    boundary fill re-reads next page; dedup by tradeId drops it. fetch_fills_all() is just this, flattened."""
    seen, cursor = set(), to_us
    first = True
    while True:
        q = {"address": address, "limit": PAGE_SIZE}
        if from_us is not None:
            q["from"] = from_us
        if cursor is not None:
            q["to"] = cursor
        # Pace pages AFTER the first: a full 1000-row /v1/fills page costs ~70 IP-weight and the per-IP
        # bucket refills 25/s, so unpaced back-to-back paging drives the bucket negative -> 429. The first
        # page rides the already-full bucket (no pause).
        data = _get_json(f"{BASE}/v1/fills?{urllib.parse.urlencode(q)}", "fills",
                         delay=(0.0 if first else page_pace_delay()))
        first = False
        fills = data.get("fills")
        if fills is None:
            break
        if not isinstance(fills, list):
            raise SystemExit("display_fills: unexpected /v1/fills response ('fills' is not a list).")
        if not fills:
            break
        fresh = []
        for f in fills:
            if not isinstance(f, dict):        # skip a malformed non-dict element
                continue
            tid = f.get("tradeId")
            # Dedup key: tradeId when present, else the full-fill content. A fill with NO tradeId would
            # otherwise NEVER dedup, so the pagination-boundary fill (re-read because `to` is inclusive) would
            # be counted twice and inflate the totals. The boundary re-read is the IDENTICAL fill, so its JSON matches.
            key = tid if tid is not None else json.dumps(f, sort_keys=True, separators=(",", ":"))
            if key in seen:
                continue
            seen.add(key)
            fresh.append(f)
        if fresh:
            yield fresh                 # STREAM this page's fresh fills to the caller before fetching the next
        if len(fills) < PAGE_SIZE:      # fewer than a full page -> reached the oldest fill / `from`
            break
        if not fresh:                    # no new tradeIds -> stop, never loop
            break
        positives = [c for c in (created_key(f) for f in fills if isinstance(f, dict)) if c > 0]
        if not positives:                # nothing with a usable timestamp to advance the cursor
            break
        nc = min(positives)              # oldest createdAt (epoch MICROseconds) -> `to` for the next page, inclusive
        if cursor is not None and nc >= cursor:    # cursor didn't decrease -> avoid an infinite loop
            break
        cursor = nc


def fetch_fills_all(address, from_us=None, to_us=None):
    """--limit unlimited: page /v1/fills to completeness, COLLECTED into one list (for the table path, which
    needs the full set for column widths + the totals footer). Streaming callers use iter_fills_pages()."""
    return [f for page in iter_fills_pages(address, from_us, to_us) for f in page]


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
    parser.add_argument("--limit", type=limit_arg, default=1000, metavar="N",
                        help="max fills to fetch: 1-1000 (default/max 1000), or 'unlimited' to page "
                             "the FULL history backward (honors --from/--to as server-side bounds)")
    parser.add_argument("--from", dest="from_us", type=epoch_us_arg, metavar="EPOCH_US",
                        help="only fills at/after this start time (epoch MICROseconds, inclusive -- the "
                             "createdAt unit; e.g. a ms value x 1000)")
    parser.add_argument("--to", dest="to_us", type=epoch_us_arg, metavar="EPOCH_US",
                        help="only fills at/before this end time (epoch MICROseconds, inclusive)")
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
    if args.from_us is not None and args.to_us is not None and args.from_us > args.to_us:
        raise SystemExit("display_fills: --from must be <= --to.")
    if args.header and not args.condensed:
        raise SystemExit("display_fills: --header requires --condensed.")

    # Validate/resolve --market up front (a typo must FAIL, not silently return 0 fills). The fills
    # API has no server-side market filter, so we filter locally by the canonical marketId.
    target_mid = resolve_market_id(BASE, args.market, "display_fills") if args.market else None

    # --condensed is a machine-readable firehose (usually piped): STREAM each page as it pages in -- the
    # backward walk yields pages newest-first -- so a HUGE account starts printing immediately and shows steady
    # progress instead of buffering the whole history before the first line (and `| head` can stop it early).
    # No global re-sort in stream mode: the pagination order IS newest-first; a consumer needing a strict order
    # sorts its own copy.
    if args.condensed:
        writer = csv.writer(sys.stdout, lineterminator="\n")
        if args.header:
            writer.writerow(CONDENSED_KEYS)
        pages = (iter_fills_pages(args.address, args.from_us, args.to_us) if args.limit == UNLIMITED
                 else [fetch_fills(args.address, args.limit, args.from_us, args.to_us)])
        for page in pages:
            for f in page:
                if target_mid is not None and str(f.get("marketId")) != target_mid:
                    continue
                writer.writerow([f.get(k, "") for k in CONDENSED_KEYS])
            sys.stdout.flush()        # push each page so output is visible as it pages, even without `python -u`
        return

    # Human table: needs the FULL set (column widths + totals footer), so it collects then sorts -- can't stream.
    if args.limit == UNLIMITED:
        raw = fetch_fills_all(args.address, args.from_us, args.to_us)
        truncated = False                         # paginated to completeness (within --from/--to)
    else:
        raw = fetch_fills(args.address, args.limit, args.from_us, args.to_us)
        truncated = len(raw) >= args.limit        # a full page back -> older fills may exist
    fills = [f for f in raw if target_mid is None or str(f.get("marketId")) == target_mid]
    fills.sort(key=created_key, reverse=True)      # enforce newest-first locally

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
