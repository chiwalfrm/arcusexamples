"""
Redis-backed cache of STATIC Arcus market metadata (marketId, tickSize, stepSize).

These values rarely change, so caching them avoids a GET /v1/markets on every
invocation -- worthwhile for loops (e.g. a market-maker calling modify_order.py
repeatedly). One API call populates the cache for ALL markets at once.

  python3 marketcache.py BTC-USD            # show the entry (and cache hit/miss)
  python3 marketcache.py BTC-USD --refresh  # force a re-fetch

Keys: arcus:<network>:market:<sanitized display name> AND arcus:<network>:market:<marketId>
      (network=testnet/mainnet, so one Redis db serves both) -> JSON
      {marketId, marketDisplayName, tickSize, stepSize}  (dual-indexed so numeric
      lookups hit too; marketDisplayName lets callers show the canonical name)
TTL:  MARKET_TTL seconds. Redis config: ARCUS_REDIS_URL (default redis://127.0.0.1:6379/0).

Caveat: marketId/tickSize/stepSize are treated as static (1h TTL). That holds while
the venue doesn't recycle/relist markets; for operational listing changes, use
--refresh (or a shorter TTL). Live fields (markPrice/oraclePrice) are deliberately
NOT cached -- read those fresh. Falls back to a direct API call whenever Redis is
unavailable, so callers work with or without Redis.

Library use: get_market() raises MarketNotFound / MarketCacheError (not SystemExit),
so callers can handle them; the CLI entrypoint converts them to clean exits.
"""

import json
import os
import re
import time
import urllib.error
import urllib.request

# Per-network server + Redis namespace. The caller MUST pass a network (testnet/
# staging) into get_market(); kept local (not imported from arcus_common) so this
# cache module stays importable without pulling in the signing stack.
NETWORKS = {
    "testnet": "https://api.testnet.arcus.xyz",
    "staging": "https://api.staging.arcus.xyz",
    "mainnet": "https://api.arcus.xyz",       # live 2026-06-25 (reads only for now)
}
MARKET_TTL = 3600          # 1 hour; tick/step are static but re-verify hourly
REDIS_URL = os.environ.get("ARCUS_REDIS_URL", "redis://127.0.0.1:6379/0")

_KEY_UNSAFE = re.compile(r"[^A-Za-z0-9._:-]")


class MarketCacheError(Exception):
    """Network/API/parse failure while fetching market metadata."""


class MarketNotFound(MarketCacheError):
    """The requested market is not in the venue's universe."""


def _markets_url(network):
    if network not in NETWORKS:
        raise MarketCacheError(f"unknown network {network!r}; expected one of {sorted(NETWORKS)}.")
    return NETWORKS[network] + "/v1/markets"


def _key(display_name, network):
    """Redis key for a market, namespaced by network and sanitized so odd
    characters can't make messy keys (e.g. arcus:testnet:market:BTC-USD)."""
    return f"arcus:{network}:market:" + _KEY_UNSAFE.sub("_", str(display_name))


def _valid_entry(e):
    """A cached/fetched entry needs a numeric id, a name, and non-empty tick/step."""
    return (isinstance(e, dict)
            and isinstance(e.get("marketId"), int)
            and isinstance(e.get("marketDisplayName"), str) and e["marketDisplayName"]
            and isinstance(e.get("tickSize"), str) and e["tickSize"]
            and isinstance(e.get("stepSize"), str) and e["stepSize"])


def _identity_match(entry, ident):
    """True iff a (shape-valid) cached entry's IDENTITY matches the requested ident: a numeric
    ident must equal marketId, otherwise (case-insensitive) the marketDisplayName. Guards against a
    cache hit on key X returning a well-shaped entry for a DIFFERENT market (corrupt/poisoned key) --
    which would otherwise feed the wrong tick/step/marketId into trading."""
    ident = str(ident)
    if ident.isdigit():
        return entry.get("marketId") == int(ident)
    return str(entry.get("marketDisplayName", "")).upper() == ident.upper()


# Cache the client across calls in one process (cheap for CLIs, avoids
# reconnect/ping per call for any longer-running caller).
_redis_client = None
_redis_last_fail = 0.0
_REDIS_RETRY_COOLDOWN = 5.0    # s; retry a DOWN Redis at most this often


def _redis():
    """A live Redis client, or None if Redis isn't reachable. A SUCCESSFUL client is cached; a FAILED
    connect is NOT memoized permanently -- it is retried every _REDIS_RETRY_COOLDOWN s, so a process that
    started while Redis was briefly down (or whose Redis restarts) re-establishes the cache instead of
    skipping it (and hammering the REST API) for its whole lifetime."""
    global _redis_client, _redis_last_fail
    if _redis_client is not None:
        return _redis_client
    if _redis_last_fail and (time.monotonic() - _redis_last_fail) < _REDIS_RETRY_COOLDOWN:
        return None                                    # recently failed -> back off before retrying
    try:
        import redis
        r = redis.Redis.from_url(REDIS_URL, socket_timeout=0.5, decode_responses=True)
        r.ping()
        _redis_client = r
    except Exception:
        _redis_client = None
        _redis_last_fail = time.monotonic()
    return _redis_client


def _fetch_all(network):
    """GET /v1/markets (for `network`) -> list of market dicts, or raise MarketCacheError."""
    markets_url = _markets_url(network)
    try:
        with urllib.request.urlopen(markets_url, timeout=10) as resp:
            data = json.loads(resp.read())
    except urllib.error.HTTPError as e:
        raise MarketCacheError(f"HTTP {e.code} fetching markets")
    except urllib.error.URLError as e:
        raise MarketCacheError(f"could not reach {markets_url}: {e.reason}")
    except (TimeoutError, OSError) as e:
        raise MarketCacheError(f"network error: {e}")
    except json.JSONDecodeError as e:
        raise MarketCacheError(f"invalid JSON from markets API: {e}")
    if not isinstance(data, dict):
        raise MarketCacheError("unexpected markets response shape (not a JSON object)")
    markets = data.get("markets")
    if not isinstance(markets, list):
        raise MarketCacheError("unexpected markets response shape (no 'markets' list)")
    return markets


def get_market(display_name, network, refresh=False, redis_client=None):
    """Return {marketId:int, marketDisplayName:str, tickSize:str, stepSize:str}.

    `network` (testnet/staging) selects both the /v1/markets server and the Redis
    key namespace, so testnet and staging entries never collide in one Redis db.
    Checks Redis first (unless refresh=True) and only trusts a cache entry that
    passes shape validation; otherwise fetches GET /v1/markets, caches every
    market, and returns the requested one. Raises MarketNotFound for an unknown
    market, MarketCacheError for fetch/parse failures. Works without Redis.
    """
    r = redis_client if redis_client is not None else _redis()
    if r is not None and not refresh:
        try:
            cached = r.get(_key(display_name, network))
        except Exception:
            cached = None
        if cached:
            try:
                entry = json.loads(cached)
            except (ValueError, TypeError):
                entry = None
            if _valid_entry(entry) and _identity_match(entry, display_name):
                return entry
            # corrupt/stale/missing fields OR identity mismatch -> ignore and refetch

    # Match by numeric marketId or case-insensitive display name.
    numeric = str(display_name).isdigit()
    want_id = int(display_name) if numeric else None
    want_name = str(display_name).upper()

    found = None
    for m in _fetch_all(network):
        try:
            entry = {"marketId": int(m["marketId"]),
                     "marketDisplayName": str(m["marketDisplayName"]),
                     "tickSize": str(m["tickSize"]),
                     "stepSize": str(m["stepSize"])}
        except (KeyError, ValueError, TypeError):
            continue
        if not _valid_entry(entry):
            continue
        if r is not None:
            # Cache under BOTH the display name AND the numeric id, so a numeric
            # get_market("1") hits Redis instead of triggering a /v1/markets fetch.
            payload = json.dumps(entry)
            try:
                r.set(_key(m.get("marketDisplayName", ""), network), payload, ex=MARKET_TTL)
                r.set(_key(str(entry["marketId"]), network), payload, ex=MARKET_TTL)
            except Exception:
                pass
        if (numeric and entry["marketId"] == want_id) or \
           (not numeric and str(m.get("marketDisplayName", "")).upper() == want_name):
            found = entry
    if found is None:
        raise MarketNotFound(f"market {display_name!r} not found")
    return found


if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser(description="Inspect the Redis market-metadata cache.")
    p.add_argument("market", help="market display name, e.g. BTC-USD")
    p.add_argument("--refresh", action="store_true", help="force re-fetch from the API")
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--testnet", dest="network", action="store_const", const="testnet",
                   help="use the testnet server + arcus:testnet:market: keys")
    g.add_argument("--staging", dest="network", action="store_const", const="staging",
                   help="use the staging server + arcus:staging:market: keys")
    g.add_argument("--mainnet", dest="network", action="store_const", const="mainnet",
                   help="use the mainnet server + arcus:mainnet:market: keys")
    a = p.parse_args()
    r = _redis()
    pre_cached = bool(r and r.exists(_key(a.market, a.network))) and not a.refresh
    try:
        entry = get_market(a.market, a.network, refresh=a.refresh)
    except MarketCacheError as e:
        raise SystemExit(f"marketcache: {e}")
    print(f"{a.market} [{a.network}]: {entry}")
    print(f"  redis: {'connected' if r else 'UNAVAILABLE (used API)'}; "
          f"source: {'cache hit' if pre_cached else 'API (now cached)'}")
