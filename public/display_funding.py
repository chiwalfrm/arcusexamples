"""
Display funding payments for an account, newest-first.

  python3 display_funding.py <eth_address>                      # last 30 days (API default)
  python3 display_funding.py <eth_address> --market BTC-USD      # only that market
  python3 display_funding.py <eth_address> --from 1782000000000  # walk older history
  python3 display_funding.py <eth_address> --limit 50 --condensed
  python3 display_funding.py <eth_address> --limit unlimited      # full history (bypasses 30-day default)

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
import os
import re
import sys
import urllib.error
import urllib.parse
import urllib.request
from decimal import Decimal
from functools import partial
from arcus_common_public import NETWORKS, UNLIMITED, dec, epoch_ms_arg, get_json_dict, limit_arg, page_pace_delay, resolve_market_id, when   # shared public helpers (formerly local copies)

_get_json = partial(get_json_dict, prog="display_funding")   # get_json + require_dict, this tool's prog


BASE = None   # set in main() from the required --testnet/--staging/--mainnet selector
ADDR_RE = re.compile(r"^0x[0-9a-fA-F]{40}$")
PAGE_SIZE = 1000          # API max per request; page size used by --limit unlimited

# Fields emitted by --condensed (raw, one CSV row per payment; no header, per the display_* convention).
CONDENSED_KEYS = ["time", "marketDisplayName", "fundingRate", "size", "payment"]


def time_key(payment):
    """Sort key by time (desc via reverse=True); missing/bad sorts oldest."""
    try:
        return int(payment.get("time"))
    except (TypeError, ValueError):
        return -1


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
    return [p for p in payments if isinstance(p, dict)]   # drop any non-dict element so downstream .get() can't crash


def iter_funding_pages(address, from_ms=None, to_ms=None):
    """--limit unlimited, as a GENERATOR: yield each page's FRESH (deduped) payments LIST, newest-first, as it
    pages BACKWARD via the `to` cursor -- so a caller can STREAM output instead of buffering the whole history
    first. /v1/funding has only from/to bounds and DEFAULTS to the last 30 days when `from` is omitted, so we
    send from=0 (unless --from) to reach genuine full history. `time` is MICROseconds but `to` is MILLIseconds,
    so the cursor is ceil(oldest_time_us / 1000): rounding UP keeps `to` inclusive of the boundary row so no
    payment is skipped, and dedup by (marketDisplayName, time) drops the re-read. fetch_funding_all() flattens."""
    seen = set()
    eff_from = from_ms if from_ms is not None else 0
    cursor = to_ms
    first = True
    while True:
        q = {"address": address, "limit": PAGE_SIZE, "from": eff_from}
        if cursor is not None:
            q["to"] = cursor
        # Pace pages AFTER the first: a full 1000-row /v1/funding page costs ~70 IP-weight and the per-IP
        # bucket refills 25/s, so unpaced back-to-back paging drives the bucket negative -> 429. The first
        # page rides the already-full bucket (no pause).
        data = _get_json(f"{BASE}/v1/funding?{urllib.parse.urlencode(q)}", "funding",
                         delay=(0.0 if first else page_pace_delay()))
        first = False
        payments = data.get("fundingPayments")
        if payments is None:
            break
        if not isinstance(payments, list):
            raise SystemExit("display_funding: unexpected /v1/funding response ('fundingPayments' is not a list).")
        if not payments:
            break
        fresh = []
        for p in payments:
            if not isinstance(p, dict):        # skip a malformed non-dict element
                continue
            key = (p.get("marketDisplayName"), p.get("time"))
            if key in seen:
                continue
            seen.add(key)
            fresh.append(p)
        if fresh:
            yield fresh                     # STREAM this page's fresh payments before fetching the next
        if len(payments) < PAGE_SIZE:      # fewer than a full page -> reached the oldest / `from`
            break
        if not fresh:                       # no new rows -> stop, never loop
            break
        positives = [c for c in (time_key(p) for p in payments if isinstance(p, dict)) if c > 0]
        if not positives:                   # nothing with a usable timestamp to advance the cursor
            break
        nc = (min(positives) + 999) // 1000        # ceil us -> ms, inclusive of the boundary row
        if cursor is not None and nc >= cursor:    # cursor didn't decrease -> avoid an infinite loop
            break
        cursor = nc


def fetch_funding_all(address, from_ms=None, to_ms=None):
    """--limit unlimited: page /v1/funding to completeness, COLLECTED into one list (for the table path, which
    needs the full set for column widths + the totals footer). Streaming callers use iter_funding_pages()."""
    return [p for page in iter_funding_pages(address, from_ms, to_ms) for p in page]


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
    parser.add_argument("--limit", type=limit_arg, default=1000, metavar="N",
                        help="max payments to fetch: 1-1000 (default/max 1000), or 'unlimited' to page "
                             "the FULL history backward (sends from=0 to bypass the 30-day default; "
                             "honors --from/--to)")
    # NB --from/--to are epoch MILLISECONDS (API request-filter unit), even though the response
    # `time` field is microseconds -- this asymmetry is the Arcus API's, not a bug here (see when()).
    parser.add_argument("--from", dest="from_ms", type=epoch_ms_arg, metavar="EPOCH_MS",
                        help="only payments at/after this start time (epoch MILLIseconds, inclusive -- NB "
                             "/v1/funding takes ms, unlike display_fills/display_transfers which take µs); "
                             "omit and the API defaults to the last 30 days")
    parser.add_argument("--to", dest="to_ms", type=epoch_ms_arg, metavar="EPOCH_MS",
                        help="only payments at/before this end time (epoch MILLIseconds, inclusive; default: now)")
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
    target_mid = resolve_market_id(BASE, args.market, "display_funding") if args.market else None

    # --condensed is a machine-readable firehose (usually piped): STREAM each page as it pages in (newest-first
    # via the backward walk) so a huge account starts printing immediately + shows steady progress instead of
    # buffering the whole history first (and `| head` can stop it early). No global re-sort in stream mode --
    # the pagination order IS newest-first; a consumer needing a strict order sorts its own copy.
    if args.condensed:
        writer = csv.writer(sys.stdout, lineterminator="\n")
        if args.header:
            writer.writerow(CONDENSED_KEYS)
        pages = (iter_funding_pages(args.address, args.from_ms, args.to_ms) if args.limit == UNLIMITED
                 else [fetch_funding(args.address, args.limit, args.from_ms, args.to_ms)])
        for page in pages:
            for p in page:
                if target_mid is not None and str(p.get("marketId")) != target_mid:
                    continue
                writer.writerow([p.get(k, "") for k in CONDENSED_KEYS])
            sys.stdout.flush()        # push each page so output is visible as it pages, even without `python -u`
        return

    # Human table: needs the FULL set (column widths + totals footer), so it collects then sorts -- can't stream.
    if args.limit == UNLIMITED:
        raw = fetch_funding_all(args.address, args.from_ms, args.to_ms)
        truncated = False                         # paginated to completeness (within --from/--to)
    else:
        raw = fetch_funding(args.address, args.limit, args.from_ms, args.to_ms)
        truncated = len(raw) >= args.limit        # a full page back -> older payments may exist
    payments = [p for p in raw if target_mid is None or str(p.get("marketId")) == target_mid]
    payments.sort(key=time_key, reverse=True)      # enforce newest-first locally

    label = args.market.upper() if args.market else "ALL"
    # Be honest about scope: the default window is only the last 30 days, and --market filters only
    # WITHIN the fetched (possibly truncated) page.
    notes = []
    if args.from_ms is None and args.limit != UNLIMITED:
        notes.append("default window: last 30 days -- pass --from (epoch ms) or --limit unlimited for older history")
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
