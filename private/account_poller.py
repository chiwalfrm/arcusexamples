#!/usr/bin/env python3
"""OPTIONAL poller: keep Arcus account-wide REST reads warm in Redis so a fleet of per-market
market_maker bots never has to fetch them itself (rate limits). Polls openOrders / positions /
account for the given address + the exchange-wide markets list, writing each to the SAME cache
the bots read (account_cache.py), with a TTL LONGER than the poll interval -- so while this runs
the keys never expire, the bots always get cache hits, and they stop fetching those endpoints
entirely. One process does the N account-wide fetches per poll instead of every bot doing them.

  account_poller.py --mainnet [--interval 5] [--ttl 15]
  account_poller.py --testnet --interval 4 --ttl 12

Account-scoped Arcus reads are UNSIGNED, so this signs nothing and uses no API keys -- it just
reads eth_address from arcus_creds_<network>.json (the bots' account) to know what to warm. If
it's NOT running, the bots fall back to their own short-TTL cache-aside refresh (no harm).
Run it with `python3 -u` so the log streams. Ctrl-C to stop. stdlib + redis only.
"""
import argparse
import os
import sys
import time
import urllib.parse

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _HERE)

import account_cache
from arcus_common import add_network_args, describe_error, load_creds, request, select_network


def main():
    p = argparse.ArgumentParser(description="Keep Arcus account-wide reads warm in Redis for the market_maker fleet.")
    p.add_argument("--interval", type=float, default=4, help="seconds between polls (default 4)")
    p.add_argument("--ttl", type=int, default=12,
                   help="Redis TTL for the cached blobs (s); MUST be > --interval so keys stay warm (default 12)")
    add_network_args(p)
    a = p.parse_args()
    if a.interval <= 0:
        raise SystemExit("account_poller: --interval must be > 0.")
    if a.ttl <= a.interval:
        raise SystemExit("account_poller: --ttl must be > --interval (so keys stay warm between polls).")
    select_network(a.network)
    a.address = load_creds()["eth_address"]   # always the bots' creds account (this is their helper)

    if account_cache._redis() is None:
        raise SystemExit("account_poller: Redis is unavailable -- nothing to warm. Start Redis (ARCUS_REDIS_URL) first.")

    q = urllib.parse.urlencode({"address": a.address})
    # (cache-name, address-for-key, REST path). markets is exchange-wide -> address None.
    endpoints = [
        ("openOrders", a.address, f"/v1/openOrders?{q}"),
        ("positions",  a.address, f"/v1/positions?{q}"),
        ("account",    a.address, f"/v1/account?{q}"),
        ("markets",    None,      "/v1/markets"),
    ]
    print(f"account_poller: {a.address} [{a.network}] every {a.interval}s, TTL {a.ttl}s -> Redis "
          f"({', '.join(n for n, _, _ in endpoints)}). Logs a CACHE MISS whenever a key it should "
          f"keep warm has already EXPIRED before its refresh (a 'TTL too low for the refresh rate' "
          f"signal), with a per-day summary at midnight. Ctrl-C to stop.", flush=True)

    last_hb = 0.0
    warmed = set()                                   # keys written >=1 time, so a miss = real expiry not cold start
    gaps_total = {n: 0 for n, _, _ in endpoints}     # cumulative expire-before-refresh detections, per key
    gaps_today = {n: 0 for n, _, _ in endpoints}     # same, reset at each local-day rollover
    cur_day = time.strftime("%Y-%m-%d")
    try:
        while True:
            ts = time.strftime("%H:%M:%S")
            # MEASURE expire-before-refresh: a key we already warmed that is GONE (ttl == -2) at the
            # top of this cycle lapsed since its last write -> bots would have cache-missed it. ttl is
            # None when Redis is down (a separate, write-failure condition) -> don't count that as a gap.
            for name, addr, _ in endpoints:
                if name in warmed and account_cache.ttl(a.network, addr, name) == -2:
                    gaps_total[name] += 1
                    gaps_today[name] += 1
                    print(f"[{ts}] CACHE MISS: '{name}' expired before refresh (TTL {a.ttl}s too low "
                          f"for the refresh cadence) -- gaps {gaps_today[name]} today, "
                          f"{gaps_total[name]} total", flush=True)

            ok, errs = 0, []
            for name, addr, path in endpoints:
                try:
                    data = request("GET", path)
                    if account_cache.write(a.network, addr, name, data, a.ttl):
                        ok += 1
                        warmed.add(name)
                    else:
                        errs.append(f"{name}:redis-write-failed")
                except (OSError, ValueError) as e:
                    errs.append(f"{name}:{describe_error(e)}")
                except Exception as e:
                    errs.append(f"{name}:{e}")

            # Daily rollover: emit a per-day gap summary so "expired ~X times/day" is directly readable.
            day = time.strftime("%Y-%m-%d")
            if day != cur_day:
                summary = ", ".join(f"{n}={gaps_today[n]}" for n, _, _ in endpoints)
                print(f"[{ts}] daily cache-gap summary for {cur_day}: {summary} "
                      f"(total {sum(gaps_today.values())})", flush=True)
                cur_day = day
                gaps_today = {n: 0 for n, _, _ in endpoints}

            now = time.time()
            if errs:
                print(f"[{ts}] warmed {ok}/{len(endpoints)}; errors: {'; '.join(errs)}", flush=True)
            elif now - last_hb >= 60:        # quiet on the happy path; heartbeat once a minute
                print(f"[{ts}] warming {ok}/{len(endpoints)} OK "
                      f"(cache gaps {sum(gaps_today.values())} today, {sum(gaps_total.values())} total)",
                      flush=True)
                last_hb = now
            time.sleep(a.interval)
    except KeyboardInterrupt:
        run_summary = ", ".join(f"{n}={gaps_total[n]}" for n, _, _ in endpoints)
        print(f"\naccount_poller: stopped. Cache-gap totals this run: {run_summary} "
              f"(total {sum(gaps_total.values())}). Cached keys expire within the TTL; "
              f"bots resume self-refresh.", flush=True)


if __name__ == "__main__":
    main()
