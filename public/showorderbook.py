#!/usr/bin/env python3
import sys
import os
import time
import json
import argparse
import urllib.error
import urllib.request

# ── Constants ────────────────────────────────────────────────────────────────
NETWORKS = {
    "testnet": "https://api.testnet.arcus.xyz",
    "staging": "https://api.staging.arcus.xyz",
    "mainnet": "https://api.arcus.xyz",       # live 2026-06-25 (reads only for now)
}
MARKETS_URL = None   # set in main() from the required --testnet/--staging/--mainnet selector
MARKETS_CACHE = None
# Must match wsorderbook.py: HTTP port = PORT_BASE[network] + marketId.
PORT_BASE = {"mainnet": 10000, "testnet": 11000, "staging": 12000}
# Launcher handoff cache written by showmarkets.py --createjson; read fail-open so this tool still
# works standalone when the file is absent. The launcher exports ARCUS_MARKETS_CACHE with a per-run
# path (so a foreign/stale file can't be trusted); we fall back to this NETWORK-scoped predictable
# path for manual use (testnet/staging/mainnet marketId maps differ). See wsorderbook.py for details.
MARKETS_CACHE_FMT = "/tmp/arcus_markets_{network}.json"


def _read_markets_cache(path):
    """Return a parsed /v1/markets response from the launcher's shared cache file, or None.
    Fail-open: any problem (missing / unreadable / corrupt / wrong shape) returns None so the caller
    does a live fetch. Only a payload whose 'markets' is a list is trusted."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError):
        return None
    if isinstance(data, dict) and isinstance(data.get("markets"), list):
        return data
    return None


def _write_markets_cache(path, data):
    """Best-effort ATOMIC write (temp + os.replace, so a reader never sees a partial file) to warm
    the launcher's shared cache. Never raises: a cache-write failure must not break a working tool."""
    tmp = f"{path}.{os.getpid()}.tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f)
        os.replace(tmp, path)
    except OSError:
        try:
            os.unlink(tmp)
        except OSError:
            pass


# ── Market ID lookup ──────────────────────────────────────────────────────────
def fetch_market_id(market: str) -> int:
    """Resolve a market to its numeric marketId.

    Accepts a numeric marketId directly, or a display name matched
    case-insensitively. Network/parse/not-found failures become clean CLI
    errors rather than tracebacks.
    """
    if market.isdigit():
        return int(market)
    data = _read_markets_cache(MARKETS_CACHE)
    if data is None:                        # cache miss -> live fetch, then warm the cache
        try:
            with urllib.request.urlopen(MARKETS_URL, timeout=10) as r:
                data = json.loads(r.read())
        except urllib.error.HTTPError as e:
            raise SystemExit(f"showorderbook: HTTP {e.code} fetching markets: {e.reason}")
        except urllib.error.URLError as e:
            raise SystemExit(f"showorderbook: could not reach {MARKETS_URL}: {e.reason}")
        except (TimeoutError, OSError) as e:
            raise SystemExit(f"showorderbook: network error: {e}")
        except json.JSONDecodeError as e:
            raise SystemExit(f"showorderbook: invalid JSON from markets API: {e}")
        _write_markets_cache(MARKETS_CACHE, data)
    target = market.upper()
    for m in data.get("markets", []):
        if str(m.get("marketDisplayName", "")).upper() == target:
            return int(m["marketId"])
    raise SystemExit(f"showorderbook: market '{market}' not found.")

# ── Crossed-price resolution ──────────────────────────────────────────────────
def _seq(entry):
    """Sequence id of a level (entry[2]); 0.0 if missing/non-numeric -- a malformed
    metadata field must not crash the display tool."""
    try:
        return float(entry[2])
    except (IndexError, ValueError, TypeError):
        return 0.0


def resolve_crosses(bids: list, asks: list):
    removed = []
    bi, ai = 0, 0
    while bi < len(bids) and ai < len(asks):
        if float(bids[bi][0]) < float(asks[ai][0]):
            break
        if _seq(bids[bi]) < _seq(asks[ai]):   # numeric compare, tolerant of bad ids
            removed.append(("bid", bids.pop(bi)))
        else:
            removed.append(("ask", asks.pop(ai)))
    return removed

# ── Display ───────────────────────────────────────────────────────────────────
def display(market: str, bids: list, asks: list, crossed=None, usd: bool = False):
    crossed = crossed or []   # avoid mutable-default-arg footgun

    def split_num(s):
        if "." in s:
            i, d = s.split(".", 1)
            return i, "." + d
        return s, ""

    def col_widths_for(str_list):
        max_i = max_d = 0
        for s in str_list:
            i, d = split_num(s)
            max_i = max(max_i, len(i))
            max_d = max(max_d, len(d))
        return max_i, max_d

    def fmt(s, int_w, dec_w):
        i, d = split_num(s)
        return f"{i:>{int_w}}{d:<{dec_w}}"

    def fmt_cum(v):
        if usd:
            return f"${v:,.2f}"
        return f"{v:.8f}".rstrip('0').rstrip('.')

    def to_size_str(entry):
        v = float(entry[1])
        if usd:
            v *= float(entry[0])
            return f"${v:,.2f}"
        return f"{v:.8f}".rstrip('0').rstrip('.')

    # ── Pre-compute display strings ───────────────────────────────────────────
    bid_size_strs = [to_size_str(b) for b in bids]
    ask_size_strs = [to_size_str(a) for a in asks]

    cum_bid_strs, cum = [], 0.0
    for b in bids:
        v = float(b[1]) * (float(b[0]) if usd else 1.0)
        cum += v
        cum_bid_strs.append(fmt_cum(cum))

    cum_ask_strs, cum = [], 0.0
    for a in asks:
        v = float(a[1]) * (float(a[0]) if usd else 1.0)
        cum += v
        cum_ask_strs.append(fmt_cum(cum))

    # ── Column widths ─────────────────────────────────────────────────────────
    all_entries = bids + asks
    if not all_entries:
        pi, pd = 1, 0
    else:
        price_strs = [e[0] for e in all_entries]
        pi, pd = col_widths_for(price_strs)

    si, sd = col_widths_for(bid_size_strs + ask_size_strs) if (bid_size_strs or ask_size_strs) else (1, 0)
    cbi, cbd = col_widths_for(cum_bid_strs + cum_ask_strs) if (cum_bid_strs or cum_ask_strs) else (1, 0)

    col_w = pi + pd
    siz_w = si + sd
    cum_w = cbi + cbd

    def fmt_c(s):
        i, d = split_num(s)
        return f"{i:>{cbi}}{d:<{cbd}}"

    # ── Spread ────────────────────────────────────────────────────────────────
    if bids and asks:
        best_bid   = float(bids[0][0])
        best_ask   = float(asks[0][0])
        spread     = best_ask - best_bid
        midpoint   = (best_bid + best_ask) / 2
        spread_pct = (spread / midpoint) * 100
        spread_str = f"  Spread: {spread:.1f} ({spread_pct:.2f}%)"
    else:
        spread_str = ""

    # ── Print ─────────────────────────────────────────────────────────────────
    header = (
        f"{'CBidS':>{cum_w}}  {'BidS':>{siz_w}}  {'BidP':>{col_w}}  "
        f"{'AskP':<{col_w}}  {'AskS':<{siz_w}}  {'CAskS':<{cum_w}}"
    )
    separator = "-" * len(header)

    print(f"\n  Orderbook: {market}{spread_str}\n")
    print(header)
    print(separator)

    rows = max(len(bids), len(asks))
    for i in range(rows):
        if i < len(bids):
            b_cum   = fmt_c(cum_bid_strs[i])
            b_size  = fmt(bid_size_strs[i], si, sd)
            b_price = fmt(bids[i][0], pi, pd)
        else:
            b_cum   = " " * cum_w
            b_size  = " " * siz_w
            b_price = " " * col_w

        if i < len(asks):
            a_price = fmt(asks[i][0], pi, pd)
            a_size  = fmt(ask_size_strs[i], si, sd)
            a_cum   = fmt_c(cum_ask_strs[i])
        else:
            a_price = " " * col_w
            a_size  = " " * siz_w
            a_cum   = " " * cum_w

        print(f"{b_cum}  {b_size}  {b_price}  {a_price}  {a_size}  {a_cum}")

    print(separator)
    print(f"  {len(bids)} bids   {len(asks)} asks")
    for side, entry in crossed:
        seq = entry[2] if len(entry) > 2 else "?"
        print(f"  Crossed {side} removed: {entry[0]} {entry[1]} {seq}")
    print()

# ── Entry point ───────────────────────────────────────────────────────────────
def positive_int(s):
    """argparse type: a positive integer (rejects 0 and negatives)."""
    v = int(s)
    if v <= 0:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return v


def check_server(url):
    """Verify the orderbook HTTP server is responding, then exit.

    Any HTTP response -- including an error status like 503 'not ready' -- means
    the server is up and answering, so we exit 0. Only a failure to get an HTTP
    response at all (connection refused, DNS failure, timeout) is treated as the
    server being down -> exit 1. The orderbook payload itself is not inspected.
    """
    try:
        with urllib.request.urlopen(url, timeout=10) as r:
            print(f"server responding: HTTP {r.status} at {url}")
            sys.exit(0)
    except urllib.error.HTTPError as e:
        print(f"server responding: HTTP {e.code} at {url}")
        sys.exit(0)
    except urllib.error.URLError as e:
        print(f"server not responding at {url}: {e.reason}")
        sys.exit(1)
    except (TimeoutError, OSError) as e:
        print(f"server not responding at {url}: {e}")
        sys.exit(1)


def fetch_book(url):
    """Fetch the orderbook, or return None (after printing why) on error / not-ready.

    The wsorderbook server returns HTTP 503 {"ready": false} while warming up or
    resyncing (and may, depending on proxies, send 200 with ready=false). Either
    way we report 'not ready' rather than crash or show a stale/empty book.
    """
    try:
        with urllib.request.urlopen(url, timeout=10) as r:
            data = json.loads(r.read())
    except urllib.error.HTTPError as e:
        if e.code == 503:
            print(f"orderbook not ready yet (HTTP 503, warming up/resyncing) at {url}")
        else:
            print(f"orderbook query failed: HTTP {e.code} at {url}")
        return None
    except urllib.error.URLError as e:
        print(f"could not reach {url}: {e.reason}")
        return None
    except (TimeoutError, OSError) as e:
        print(f"network error querying {url}: {e}")
        return None
    except json.JSONDecodeError as e:
        print(f"invalid JSON from {url}: {e}")
        return None
    if data.get("ready") is False:             # 200 but explicitly flagged not-ready
        print(f"orderbook not ready yet (ready=false) at {url}")
        return None
    return data


def main():
    global MARKETS_URL, MARKETS_CACHE
    parser = argparse.ArgumentParser(description="Display orderbook for a market.")
    parser.add_argument("market",                                     help="Market symbol (e.g. BTC-USD) or numeric marketId")
    parser.add_argument("--server",  default="localhost",             help="Orderbook server host (default: localhost)")
    parser.add_argument("--nlevels", type=positive_int, default=None,  help="Number of price levels to display (default: all)")
    parser.add_argument("--looping", action="store_true",             help="Continuously refresh the orderbook")
    parser.add_argument("--usd",     action="store_true",             help="Show size columns in USD value")
    parser.add_argument("--checkserver", action="store_true",         help="Only verify the orderbook HTTP server responds, then exit (0=up, 1=down)")
    net = parser.add_mutually_exclusive_group(required=True)
    net.add_argument("--testnet", dest="network", action="store_const", const="testnet",
                     help="resolve the market against the testnet server")
    net.add_argument("--staging", dest="network", action="store_const", const="staging",
                     help="resolve the market against the staging server")
    net.add_argument("--mainnet", dest="network", action="store_const", const="mainnet",
                     help="resolve the market against the mainnet server")
    args = parser.parse_args()
    MARKETS_URL = NETWORKS[args.network] + "/v1/markets"
    MARKETS_CACHE = os.environ.get("ARCUS_MARKETS_CACHE") or MARKETS_CACHE_FMT.format(network=args.network)

    market = args.market
    host   = args.server

    market_id = fetch_market_id(market)
    port      = PORT_BASE[args.network] + market_id
    url       = f"http://{host}:{port}/orderbook"

    if args.checkserver:
        check_server(url)   # connects, prints status, and exits

    while True:
        data = fetch_book(url)
        if data is None:                       # not ready / transient error
            if not args.looping:
                sys.exit(1)
            time.sleep(1)
            continue

        bids = data.get("bids", [])
        asks = data.get("asks", [])

        bids.sort(key=lambda x: float(x[0]), reverse=True)
        asks.sort(key=lambda x: float(x[0]))

        crossed = resolve_crosses(bids, asks)

        if args.nlevels is not None:
            bids = bids[:args.nlevels]
            asks = asks[:args.nlevels]

        if args.looping and sys.stdout.isatty():
            print("\033[2J\033[H", end="")

        display(market, bids, asks, crossed, args.usd)

        if not args.looping:
            break

        time.sleep(1)

if __name__ == "__main__":
    main()
