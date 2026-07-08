#!/usr/bin/env python3
"""List Arcus testnet markets.

  showmarkets.py                     # aligned table of the common columns
  showmarkets.py --condensed         # CSV of the common columns (for piping)
  showmarkets.py --verbose           # aligned table of ALL fields
  showmarkets.py --verbosecondensed  # pipe-delimited ALL fields (for piping)
"""

import argparse
import csv
import json
import os
import sys
import urllib.error
import urllib.request

NETWORKS = {
    "testnet": "https://api.testnet.arcus.xyz",
    "staging": "https://api.staging.arcus.xyz",
    "mainnet": "https://api.arcus.xyz",       # live 2026-06-25 (reads only for now)
}
MARKETS_URL = None   # set in main() from the required --testnet/--staging selector

# --createjson writes the raw /v1/markets response here for the launcher's other tools (wsorderbook /
# showorderbook) to read, collapsing a whole launcher run to ONE server call. This tool runs FIRST in
# the launcher, so it is the natural creator. The launcher exports ARCUS_MARKETS_CACHE with a PER-RUN
# path so a foreign/stale file at the predictable path can never be trusted (a unique-path write
# failure just leaves no file -> readers fail-open to a live fetch, never to stale data). Falls back to
# a NETWORK-scoped predictable path for manual use (testnet/staging/mainnet marketId maps differ).
MARKETS_CACHE_FMT = "/tmp/arcus_markets_{network}.json"


def _write_markets_cache(path, data):
    """Best-effort ATOMIC write (temp + os.replace, so a reader never sees a partial file) of the
    raw markets response. Never raises: failing to warm the cache must not break normal output."""
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

# (key, alignment) for the default/condensed common columns.
COMMON_COLS = [
    ("marketId", ">"), ("marketDisplayName", "<"), ("tickSize", ">"),
    ("stepSize", ">"), ("oraclePrice", ">"), ("status", "<"),
]


def market_id_key(m):
    """Sort key by numeric marketId; missing/non-numeric ids sort last (no crash)."""
    try:
        return (0, int(m["marketId"]))
    except (KeyError, ValueError, TypeError):
        return (1, 0)


def require_dict(data, what):
    """A decoded JSON body that must be an object -> clean CLI error if the server returned null /
    a list / a scalar instead (so the .get(...) below can't AttributeError)."""
    if not isinstance(data, dict):
        raise SystemExit(f"showmarkets: unexpected {what} response shape (not a JSON object)")
    return data


def fetch_markets(cache_path=None):
    """Fetch and sort markets, turning network/parse failures into clean CLI errors. When cache_path
    is set, the RAW response is also written there (best-effort) to warm the launcher's shared cache
    -- we always fetch fresh (never read that cache) so displayed output is never stale."""
    try:
        with urllib.request.urlopen(MARKETS_URL, timeout=10) as r:
            data = json.loads(r.read())
    except urllib.error.HTTPError as e:
        raise SystemExit(f"showmarkets: HTTP {e.code} fetching markets: {e.reason}")
    except urllib.error.URLError as e:
        raise SystemExit(f"showmarkets: could not reach {MARKETS_URL}: {e.reason}")
    except (TimeoutError, OSError) as e:        # socket timeout / connection issues
        raise SystemExit(f"showmarkets: network error: {e}")
    except json.JSONDecodeError as e:
        raise SystemExit(f"showmarkets: invalid JSON from server: {e}")

    markets = require_dict(data, "markets").get("markets")
    if not isinstance(markets, list):
        raise SystemExit("showmarkets: unexpected response shape (no 'markets' list).")
    if cache_path is not None:
        _write_markets_cache(cache_path, data)   # raw shape {"markets": [...]} for sibling tools
    return sorted(markets, key=market_id_key)


def all_keys(markets):
    """Every field key seen across markets, in first-seen order."""
    return list(dict.fromkeys(k for m in markets for k in m.keys()))


def print_table(markets, cols):
    """Print an aligned table for the given (key, align) columns, widths sized to data."""
    widths = {k: len(k) for k, _ in cols}
    for m in markets:
        for k, _ in cols:
            widths[k] = max(widths[k], len(str(m.get(k, ""))))
    header = "  ".join(f"{k:{align}{widths[k]}}" for k, align in cols)
    separator = "-" * len(header)
    print(f"\n  Markets: {MARKETS_URL}\n")
    print(header)
    print(separator)
    for m in markets:
        print("  ".join(f"{str(m.get(k, '')):{align}{widths[k]}}" for k, align in cols))
    print(separator)
    print(f"  {len(markets)} markets\n")


def write_delimited(markets, keys, delimiter):
    """Write header + rows via csv.writer so commas/pipes/quotes are escaped safely."""
    w = csv.writer(sys.stdout, delimiter=delimiter, lineterminator="\n")
    w.writerow(keys)
    for m in markets:
        w.writerow([m.get(k, "") for k in keys])


def main():
    global MARKETS_URL
    parser = argparse.ArgumentParser(description="List markets.")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--condensed", action="store_true",
                       help="CSV of the common columns (safe for piping)")
    group.add_argument("--verbose", action="store_true",
                       help="aligned table of all fields")
    group.add_argument("--verbosecondensed", action="store_true",
                       help="pipe-delimited all fields (safe for piping)")
    net = parser.add_mutually_exclusive_group(required=True)
    net.add_argument("--testnet", dest="network", action="store_const", const="testnet",
                     help="query the testnet server")
    net.add_argument("--staging", dest="network", action="store_const", const="staging",
                     help="query the staging server")
    net.add_argument("--mainnet", dest="network", action="store_const", const="mainnet",
                     help="query the mainnet server")
    parser.add_argument("--createjson", action="store_true",
                        help="also write the raw /v1/markets response to "
                             "/tmp/arcus_markets_<network>.json so sibling launcher tools resolve "
                             "markets from that file instead of re-hitting the server")
    args = parser.parse_args()
    MARKETS_URL = NETWORKS[args.network] + "/v1/markets"

    cache_path = (os.environ.get("ARCUS_MARKETS_CACHE")
                  or MARKETS_CACHE_FMT.format(network=args.network)) if args.createjson else None
    markets = fetch_markets(cache_path)

    if args.condensed:
        write_delimited(markets, [k for k, _ in COMMON_COLS], delimiter=",")
    elif args.verbosecondensed:
        write_delimited(markets, all_keys(markets), delimiter="|")
    elif args.verbose:
        print_table(markets, [(k, ">") for k in all_keys(markets)])
    else:
        print_table(markets, COMMON_COLS)


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
