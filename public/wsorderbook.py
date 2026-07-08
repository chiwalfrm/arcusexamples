#!/usr/bin/env python3
"""Maintain a market's L2 order book from the WebSocket stream and serve it over HTTP.

  wsorderbook.py BTC-USD
  wsorderbook.py BTC-USD --host 0.0.0.0 --log-dir /tmp/ob

Subscribes to l2OrderbookUpdates/trades/oraclePrices, applies the snapshot +
deltas into a local book, and serves it at http://<host>:<PORT_BASE[net]+marketId>/orderbook
(PORT_BASE: mainnet 10000 / testnet 11000 / staging 12000)
(the endpoint showorderbook.py reads). The book is only served once the first
snapshot has arrived (503 {"ready": false} until then), and the delta stream's
per-market lastSequenceId is checked for gaps -- on a gap it re-subscribes
(fresh snapshot) so the local book can't drift silently.
"""
import argparse
import asyncio
import json
import logging
import os
import re
import sys
import time
import urllib.error
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from logging.handlers import RotatingFileHandler

import aiohttp
import websockets
from aiohttp import web

try:
    import redis.asyncio as aioredis      # OPTIONAL dependency: absent => the BBO→Redis feature is simply off
except ImportError:
    aioredis = None

# ── Constants ────────────────────────────────────────────────────────────────
NETWORKS = {
    "testnet": "https://api.testnet.arcus.xyz",
    "staging": "https://api.staging.arcus.xyz",
    "mainnet": "https://api.arcus.xyz",       # live 2026-06-25 (reads only for now)
}
BASE_URL     = None   # all four set in main() from the required --testnet/--staging/--mainnet selector
MARKETS_URL  = None
WS_URL       = None
MARKETS_CACHE = None

# Launcher handoff cache. showmarkets.py --createjson writes the raw /v1/markets response here so the
# many per-market tools the launcher starts sequentially resolve their market from ONE file instead of
# each re-hitting the server. The launcher exports ARCUS_MARKETS_CACHE with a PER-RUN path so a
# foreign/stale file at the predictable path can never be trusted; we fall back to this NETWORK-scoped
# predictable path only for manual/standalone use (testnet/staging/mainnet marketId maps differ, so
# they must never be crossed). Read is fail-open -- a missing/corrupt file just falls back to a live
# fetch. The launcher removes its per-run file when it finishes.
MARKETS_CACHE_FMT = "/tmp/arcus_markets_{network}.json"

# HTTP port = PORT_BASE[network] + marketId. Per-network base keeps the 3 networks'
# orderbook servers on disjoint ranges (1000 markets each): mainnet 10xxx, testnet
# 11xxx, staging 12xxx.
PORT_BASE = {"mainnet": 10000, "testnet": 11000, "staging": 12000}
LOG_BASE        = "/mnt/arcuslogs"   # logs go under LOG_BASE/<network> (subdir auto-created)
LOG_MAX_BYTES   = 2097152
LOG_BACKUP      = 4
RECONNECT_BASE  = 1
RECONNECT_MAX   = 60
STABLE_AFTER    = 30         # s a connection must STAY UP before backoff resets (else an accept-then-close flap busy-loops)
OPEN_TIMEOUT    = 10
PING_INTERVAL   = 20
PING_TIMEOUT    = 20
MARKET_RE       = re.compile(r"^[A-Za-z0-9._-]+$")   # safe for filenames + ids

# Optional BBO → Redis publisher. redis-py is an OPTIONAL dependency; if it's absent (aioredis is None)
# the bbo subscription is skipped and this tool behaves exactly as before (HTTP orderbook only).
REDIS_URL   = os.environ.get("ARCUS_REDIS_URL", "redis://127.0.0.1:6379/0")
BBO_KEY_FMT = "arcus:{network}:bbo:{market}"   # one key per market; value = native bbo `contents` + our `ts`
BBO_TTL     = 3        # s; ≈ the reader's age-guard AND > HEARTBEAT so a live-but-quiet key never expires between beats
HEARTBEAT   = 1.0      # s; refresh `ts` at least this often while the socket is up (idle markets stay "alive")


class SequenceGap(Exception):
    """Raised when the L2 delta stream skips a sequence id (local book may be stale)."""


# ── Order book state ──────────────────────────────────────────────────────────
class OrderBook:
    """Local L2 book keyed by Decimal price (so "1.0" and "1.00" collapse)."""

    def __init__(self):
        self.reset()

    def reset(self):
        self.bids = {}
        self.asks = {}
        self.ready = False
        self.last_seq = None
        self._awaiting_first_delta = False

    def _apply(self, book, levels, seq):
        for price, size in levels:
            try:
                key = Decimal(price)
            except (InvalidOperation, TypeError, ValueError):
                continue
            try:
                is_zero = Decimal(size) == 0
            except (InvalidOperation, TypeError, ValueError):
                is_zero = (str(size) == "0")
            if is_zero:
                book.pop(key, None)
            else:
                book[key] = [price, size, seq]

    @staticmethod
    def _seq(contents):
        """lastSequenceId as int (accepts int or numeric string), else None."""
        try:
            return int(contents.get("lastSequenceId"))
        except (TypeError, ValueError):
            return None

    def on_snapshot(self, contents):
        seq = self._seq(contents)             # normalized to int (or None)
        self.bids.clear()
        self.asks.clear()
        self._apply(self.bids, contents.get("bids", []), seq)
        self._apply(self.asks, contents.get("asks", []), seq)
        self.last_seq = seq
        # The first delta after a snapshot is NOT snapshot_seq+1 (the snapshot's
        # ln lags the live stream), so adopt its seq as baseline without a gap check.
        self._awaiting_first_delta = True
        self.ready = True

    def on_delta(self, contents):
        seq = self._seq(contents)
        if seq is None:                       # missing/malformed -> resync
            raise SequenceGap(f"missing/non-numeric lastSequenceId: {contents.get('lastSequenceId')!r}")
        if self.last_seq is not None and not self._awaiting_first_delta:
            if seq != self.last_seq + 1:
                raise SequenceGap(f"expected {self.last_seq + 1}, got {seq}")
        self._apply(self.bids, contents.get("bids", []), seq)
        self._apply(self.asks, contents.get("asks", []), seq)
        self.last_seq = seq
        self._awaiting_first_delta = False

    def payload(self):
        bids = sorted(self.bids.values(), key=lambda x: Decimal(x[0]), reverse=True)
        asks = sorted(self.asks.values(), key=lambda x: Decimal(x[0]))
        return {"ready": self.ready, "bids": bids, "asks": asks}


# ── Logger setup ─────────────────────────────────────────────────────────────
def setup_logger(name: str, path: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger
    handler = RotatingFileHandler(path, maxBytes=LOG_MAX_BYTES, backupCount=LOG_BACKUP)
    handler.setFormatter(logging.Formatter("%(message)s"))
    logger.setLevel(logging.DEBUG)
    logger.addHandler(handler)
    logger.propagate = False
    return logger


def _emit(logger, line):
    try:
        logger.debug(line)
    except Exception as e:
        print(f"[log error] {e}", file=sys.stderr)


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def require_dict(data, what):
    """A decoded JSON body that must be an object -> clean CLI error if the server returned null /
    a list / a scalar instead (so the .get(...) below can't AttributeError)."""
    if not isinstance(data, dict):
        raise SystemExit(f"wsorderbook: unexpected {what} response shape (not a JSON object)")
    return data


def describe_error(e):
    """Readable one-line error (uniform with market_maker.py)."""
    if isinstance(e, urllib.error.HTTPError):
        try:
            return f"HTTP {e.code}: {e.read().decode()[:160]}"
        except Exception:
            return f"HTTP {e.code}"
    if isinstance(e, urllib.error.URLError):
        return f"unreachable: {e.reason}"
    if isinstance(e, json.JSONDecodeError):
        return f"bad JSON: {e}"
    return f"{type(e).__name__}: {e}"


# ── HTTP handler ─────────────────────────────────────────────────────────────
async def handle_orderbook(request):
    book = request.app["book"]
    payload = book.payload()
    # 503 until the first snapshot, so a caller never mistakes "not ready yet"
    # for "no liquidity".
    return web.json_response(payload, status=200 if book.ready else 503)


# ── BBO → Redis publisher (optional) ─────────────────────────────────────────
_PUB_ERR_THROTTLE = 30.0            # s; at most one "[bbo redis]" line per this window -- a down/slow Redis on
_pub_err_last = 0.0                 # a busy market would otherwise print after EVERY frame + heartbeat and
_pub_err_suppressed = 0            # flood the redirected stdout/err log.


async def publish_bbo(r, key, contents, ts):
    """Write the native `bbo` contents verbatim + our liveness `ts` to Redis. Guarded + async so a
    Redis hiccup or slow SET can NEVER stall or crash the WS/book/HTTP path (the existing contract).
    `ts` is our write time (unix seconds); the reader ages off it to decide the feed is alive."""
    if r is None:
        return
    blob = dict(contents)          # bestBid/bestAsk (price+size), lastSequenceId, globalSequenceId, timestamp -- verbatim
    blob["ts"] = ts
    global _pub_err_last, _pub_err_suppressed
    try:
        await r.set(key, json.dumps(blob, separators=(",", ":")), ex=BBO_TTL)
        _pub_err_suppressed = 0     # recovered -> the next distinct error window logs its first line immediately
    except Exception as e:         # redis down / slow / misconfigured -- log (throttled), never propagate
        now = time.monotonic()
        if now - _pub_err_last >= _PUB_ERR_THROTTLE:
            extra = f" (+{_pub_err_suppressed} suppressed)" if _pub_err_suppressed else ""
            print(f"[bbo redis] {describe_error(e)}{extra}", file=sys.stderr)
            _pub_err_last, _pub_err_suppressed = now, 0
        else:
            _pub_err_suppressed += 1


# ── Frame handling / WebSocket loop ──────────────────────────────────────────
async def handle_frame(raw, book, loggers, add_ts, r, key, state):
    try:
        msg = json.loads(raw)
    except json.JSONDecodeError as e:
        print(f"[parse error] {e} | {raw}")     # unparseable -> stdout (captured)
        return
    if not isinstance(msg, dict):               # valid JSON but not an object -> stdout, keep reading
        print(raw)
        return
    channel = msg.get("channel")
    if channel == "bbo":
        # Native best-bid/offer -> Redis (no file log: high-frequency, Redis is its sink). ON-CHANGE: write
        # immediately on every bbo msg (sub-second freshness), and stamp last_pub so the ws_loop time-gate
        # heartbeat won't double-write right after. contents may be {} for an empty market -> null sides.
        # (The heartbeat covers the SPARSE-bbo case: this socket is often flooded by oraclePrices while bbo
        # rarely arrives -- measured AAPL mainnet ~69 oracle vs 1 bbo per 20s -- so the key stays warm.)
        if r is not None and msg.get("type") in ("subscribed", "channel_data"):
            state["bbo"] = msg.get("contents") or {}
            await publish_bbo(r, key, state["bbo"], time.time())
            state["last_pub"] = time.monotonic()
        return
    logger = loggers.get(channel)
    if logger is None:                          # not a subscribed channel -> stdout
        print(raw)
        return
    line = (json.dumps({"receivedAt": now_iso(), "msg": msg}, separators=(",", ":"))
            if add_ts else raw)
    _emit(logger, line)
    if channel == "l2OrderbookUpdates":
        contents = msg.get("contents") or {}
        if msg.get("type") == "subscribed":
            book.on_snapshot(contents)
        elif msg.get("type") == "channel_data":
            book.on_delta(contents)            # may raise SequenceGap


async def ws_loop(url, subscriptions, book, loggers, add_ts, r, key):
    delay = RECONNECT_BASE
    while True:
        conn_start = None
        backoff = True                         # sleep+backoff before reconnecting, UNLESS a seq-gap (resync now)
        state = {"bbo": None, "last_pub": 0.0}  # last bbo contents + monotonic time of last publish (on-change + heartbeat gate), per-connection
        try:
            async with websockets.connect(
                url, open_timeout=OPEN_TIMEOUT,
                ping_interval=PING_INTERVAL, ping_timeout=PING_TIMEOUT,
            ) as ws:
                conn_start = time.monotonic()
                for sub in subscriptions:
                    await ws.send(json.dumps(sub))
                if r is not None:
                    # Redis present: ON-CHANGE writes happen in handle_frame (every bbo msg). Here we add the
                    # HEARTBEAT: republish the latest bbo whenever it's been quiet >= HEARTBEAT (driven by any
                    # socket activity OR the recv timeout). Needed because the bbo channel is often sparse while
                    # this shared socket is flooded by oraclePrices, so a recv-timeout-only heartbeat starves
                    # and the key expires between bbo msgs. state["last_pub"] (reset by each on-change write)
                    # keeps this from double-writing right after one. A clean close raises ConnectionClosedOK.
                    while True:
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=HEARTBEAT)
                            await handle_frame(raw, book, loggers, add_ts, r, key, state)
                        except asyncio.TimeoutError:
                            pass
                        now = time.monotonic()
                        if state["bbo"] is not None and now - state["last_pub"] >= HEARTBEAT:
                            await publish_bbo(r, key, state["bbo"], time.time())
                            state["last_pub"] = now
                else:
                    # No redis: the original loop, untouched (no heartbeat needed).
                    async for raw in ws:
                        await handle_frame(raw, book, loggers, add_ts, None, None, state)
            # Clean close (server ended the stream without an exception): the book
            # is now stale, so stop serving it as ready until the next snapshot.
            book.reset()
            print("[ws] connection closed — resubscribing for a fresh snapshot", file=sys.stderr)
        except SequenceGap as e:
            print(f"[seq gap] {e} — resubscribing for a fresh snapshot", file=sys.stderr)
            book.reset()
            backoff = False                    # immediate resync, no backoff
        except websockets.ConnectionClosedOK:
            # Redis branch only: a clean server close surfaces as an exception (unlike async-for). Treat
            # it exactly like the clean-close path above (reset + normal backoff), NOT as an error.
            book.reset()
            print("[ws] connection closed — resubscribing for a fresh snapshot", file=sys.stderr)
        except Exception as e:
            print(f"[ws error] {describe_error(e)} — reconnecting", file=sys.stderr)   # actual delay set after the stability reset below
            book.reset()
        # Sleep/backoff for a clean close OR an error (NOT a seq-gap). Previously a CLEAN close skipped
        # every except and busy-looped at 0 delay; and resetting delay on connect defeated backoff on a
        # flap. Now: reset delay only if the connection proved STABLE (>= STABLE_AFTER s), then sleep.
        if backoff:
            if conn_start is not None and (time.monotonic() - conn_start) >= STABLE_AFTER:
                delay = RECONNECT_BASE
            await asyncio.sleep(delay)
            delay = min(delay * 2, RECONNECT_MAX)


# ── Market lookup ────────────────────────────────────────────────────────────
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


async def resolve_market(market: str):
    """Resolve a market (numeric id OR case-insensitive name) -> (marketId, displayName).

    Returns the CANONICAL display name -- it's reused as the WebSocket subscription
    `id` and in log filenames, where the server expects the name, not the id.
    """
    data = _read_markets_cache(MARKETS_CACHE)
    if data is None:                        # cache miss -> live fetch, then warm the cache
        timeout = aiohttp.ClientTimeout(total=10)
        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(MARKETS_URL) as resp:
                    resp.raise_for_status()
                    data = await resp.json(content_type=None)
        except aiohttp.ClientError as e:
            raise SystemExit(f"wsorderbook: error fetching markets: {e}")
        except asyncio.TimeoutError:
            raise SystemExit(f"wsorderbook: timed out fetching {MARKETS_URL}")
        except json.JSONDecodeError as e:
            raise SystemExit(f"wsorderbook: invalid JSON from markets API: {e}")
        _write_markets_cache(MARKETS_CACHE, data)
    data = require_dict(data, "markets")       # live-fetch path lacked the shape guard the cache path has
    if not isinstance(data.get("markets"), list):   # unify with the cache path (_read_markets_cache requires a list)
        raise SystemExit("wsorderbook: unexpected markets response shape (no 'markets' list)")
    numeric = market.isdigit()
    up = market.upper()
    for m in data.get("markets", []):
        if (numeric and str(m.get("marketId")) == str(int(market))) or \
           (not numeric and str(m.get("marketDisplayName", "")).upper() == up):
            return int(m["marketId"]), str(m["marketDisplayName"])
    raise SystemExit(f"wsorderbook: market '{market}' not found.")


# ── Entry point ──────────────────────────────────────────────────────────────
async def amain(args):
    if not MARKET_RE.match(args.market):
        raise SystemExit(f"wsorderbook: invalid market {args.market!r} "
                         f"(allowed: letters, digits, . _ -).")

    try:
        os.makedirs(args.log_dir, exist_ok=True)
    except OSError as e:
        raise SystemExit(f"wsorderbook: cannot create log dir {args.log_dir!r}: {e}")

    print(f"[{args.market}] Resolving market …")
    market_id, market = await resolve_market(args.market)   # canonical display name
    port = PORT_BASE[args.network] + market_id
    print(f"[{market}] marketId={market_id} ({args.network})  →  HTTP port {port}")

    # One log per subscribed channel; unknown/malformed frames go to stdout.
    loggers = {
        "l2OrderbookUpdates": setup_logger(f"wsob.l2.{market}", f"{args.log_dir}/wsorderbook{market}.log"),
        "trades":             setup_logger(f"wsob.trades.{market}", f"{args.log_dir}/wstrades{market}.log"),
        "oraclePrices":       setup_logger(f"wsob.oracle.{market}", f"{args.log_dir}/oraclePrices{market}.log"),
    }

    book = OrderBook()
    subscriptions = [
        {"type": "subscribe", "channel": "l2OrderbookUpdates", "id": market, "nLevels": 100, "snapshot": True},
        {"type": "subscribe", "channel": "trades", "id": market, "snapshot": True},
        {"type": "subscribe", "channel": "oraclePrices", "id": market, "snapshot": True},
    ]
    # Optional BBO → Redis publisher. redis-py is an OPTIONAL dependency: if it's not installed we skip
    # the bbo subscription entirely and this tool behaves exactly as before (HTTP orderbook only).
    r = aioredis.from_url(REDIS_URL) if aioredis is not None else None
    key = None
    if r is not None:
        key = BBO_KEY_FMT.format(network=args.network, market=market)
        subscriptions.append({"type": "subscribe", "channel": "bbo", "id": market})
        print(f"[{market}] publishing BBO → redis '{key}' (TTL {BBO_TTL}s, heartbeat {HEARTBEAT}s)")
    asyncio.create_task(ws_loop(args.url, subscriptions, book, loggers, args.timestamp, r, key))

    app = web.Application()
    app["book"] = book
    app.router.add_get("/orderbook", handle_orderbook)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, args.host, port)
    await site.start()
    print(f"[{market}] Serving orderbook at http://{args.host}:{port}/orderbook")

    await asyncio.Event().wait()


def main():
    global BASE_URL, MARKETS_URL, WS_URL, MARKETS_CACHE
    parser = argparse.ArgumentParser(description="Serve a market's L2 order book over HTTP.")
    parser.add_argument("market", help="market display name, e.g. BTC-USD")
    parser.add_argument("--host", default="127.0.0.1",
                        help="HTTP bind host (default 127.0.0.1; use 0.0.0.0 to expose)")
    parser.add_argument("--log-dir", default=None,
                        help="log directory (default: /mnt/arcuslogs/<network>)")
    parser.add_argument("--url", default=None,
                        help="override the WebSocket URL (default: derived from the network)")
    parser.add_argument("--timestamp", action="store_true",
                        help="wrap each logged line as JSONL with a local receivedAt "
                             "(default: log the raw server frame)")
    net = parser.add_mutually_exclusive_group(required=True)
    net.add_argument("--testnet", dest="network", action="store_const", const="testnet",
                     help="use the testnet server + WebSocket")
    net.add_argument("--staging", dest="network", action="store_const", const="staging",
                     help="use the staging server + WebSocket")
    net.add_argument("--mainnet", dest="network", action="store_const", const="mainnet",
                     help="use the mainnet server + WebSocket")
    args = parser.parse_args()
    BASE_URL = NETWORKS[args.network]
    MARKETS_URL = f"{BASE_URL}/v1/markets"
    WS_URL = BASE_URL.replace("https://", "wss://") + "/v1/ws"
    MARKETS_CACHE = os.environ.get("ARCUS_MARKETS_CACHE") or MARKETS_CACHE_FMT.format(network=args.network)
    if args.url is None:
        args.url = WS_URL
    if args.log_dir is None:
        args.log_dir = os.path.join(LOG_BASE, args.network)
    try:
        asyncio.run(amain(args))
    except KeyboardInterrupt:
        print(f"\n[{args.market}] stopped")


if __name__ == "__main__":
    main()
