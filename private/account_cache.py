#!/usr/bin/env python3
"""Redis-backed short-TTL cache for Arcus ACCOUNT-WIDE / exchange-wide REST reads, so a fleet
of per-market market_maker bots doesn't each re-fetch the same data every loop and trip rate
limits. (These reads are account-scoped and UNSIGNED -- safe to share.)

Cache-aside, NO locks: a bot calls cached_get(...); on a hit it uses the cached blob, on a miss
it fetches live and writes it back with a short TTL. Concurrent writes are harmless -- Redis is
single-threaded so it serializes them, and the racing values are near-identical fresh snapshots
(last-write-wins). An optional account_poller.py can keep the keys warm with a LONGER TTL than
its poll interval, so the bots always hit and never fetch at all. If Redis is down, cached_get
just calls the live fetch (no cache) -- mirrors marketcache.py's graceful fallthrough.

Keys: arcus:<network>:acct:<address>:<name>  (per-address: openOrders/positions/account)
      arcus:<network>:<name>                 (exchange-wide, address=None: e.g. markets)
stdlib + redis only -- no coupling to ordersign/creds, so it imports without the signing stack.
"""
import json
import os

DEFAULT_REDIS_URL = os.environ.get("ARCUS_REDIS_URL", "redis://127.0.0.1:6379/0")
_CLIENT = "<unset>"   # per-process memo: a redis client, or None if unavailable


def _redis(client=None):
    """A redis client, or None if redis-py is absent / the server is unreachable. Memoized."""
    global _CLIENT
    if client is not None:
        return client
    if _CLIENT != "<unset>":
        return _CLIENT
    try:
        import redis
        c = redis.Redis.from_url(DEFAULT_REDIS_URL, socket_timeout=2, decode_responses=True)
        c.ping()
        _CLIENT = c
    except Exception:
        _CLIENT = None
    return _CLIENT


def _key(network, address, name):
    return f"arcus:{network}:acct:{address}:{name}" if address else f"arcus:{network}:{name}"


def read(network, address, name, redis_client=None):
    """Cached blob for (network, address, name), or None on miss / Redis down / parse error."""
    client = _redis(redis_client)
    if not client:
        return None
    try:
        raw = client.get(_key(network, address, name))
        return json.loads(raw) if raw else None
    except Exception:
        return None


def ttl(network, address, name, redis_client=None):
    """Remaining TTL (seconds) for the key: >=0 live, -2 missing/expired, -1 set-without-expiry,
    or None if Redis is unavailable -- so a caller can tell 'key expired' (-2) apart from
    'Redis down' (None). Used by account_poller to measure expire-before-refresh gaps."""
    client = _redis(redis_client)
    if not client:
        return None
    try:
        return client.ttl(_key(network, address, name))
    except Exception:
        return None


def write(network, address, name, data, ttl, redis_client=None):
    """Cache `data` (a JSON-able blob) under the key with TTL `ttl` seconds. Best-effort:
    returns True if written, False if Redis is down / the write failed. Single-key SET, so the
    write is atomic -- a concurrent reader sees either the whole old or whole new value."""
    client = _redis(redis_client)
    if not client:
        return False
    try:
        client.set(_key(network, address, name), json.dumps(data), ex=ttl)
        return True
    except Exception:
        return False


def cached_get(network, address, name, fetch_fn, ttl, redis_client=None):
    """Cache-aside read: return the cached blob if present, else fetch_fn() and cache it with
    `ttl`. Redis down / any cache error -> fetch_fn() directly (no caching). No locking: a
    simultaneous miss in several callers just means a few redundant fetches + last-write-wins."""
    cached = read(network, address, name, redis_client)
    if cached is not None:
        return cached
    data = fetch_fn()
    write(network, address, name, data, ttl, redis_client)
    return data
