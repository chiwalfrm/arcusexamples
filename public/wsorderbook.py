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
import urllib.error
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from logging.handlers import RotatingFileHandler

import aiohttp
import websockets
from aiohttp import web

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
OPEN_TIMEOUT    = 10
PING_INTERVAL   = 20
PING_TIMEOUT    = 20
MARKET_RE       = re.compile(r"^[A-Za-z0-9._-]+$")   # safe for filenames + ids


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


# ── Frame handling / WebSocket loop ──────────────────────────────────────────
def handle_frame(raw, book, loggers, add_ts):
    try:
        msg = json.loads(raw)
    except json.JSONDecodeError as e:
        print(f"[parse error] {e} | {raw}")     # unparseable -> stdout (captured)
        return
    channel = msg.get("channel")
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


async def ws_loop(url, subscriptions, book, loggers, add_ts):
    delay = RECONNECT_BASE
    while True:
        try:
            async with websockets.connect(
                url, open_timeout=OPEN_TIMEOUT,
                ping_interval=PING_INTERVAL, ping_timeout=PING_TIMEOUT,
            ) as ws:
                delay = RECONNECT_BASE
                for sub in subscriptions:
                    await ws.send(json.dumps(sub))
                async for raw in ws:
                    handle_frame(raw, book, loggers, add_ts)
            # Clean close (server ended the stream without an exception): the book
            # is now stale, so stop serving it as ready until the next snapshot.
            book.reset()
            print("[ws] connection closed — resubscribing for a fresh snapshot", file=sys.stderr)
        except SequenceGap as e:
            print(f"[seq gap] {e} — resubscribing for a fresh snapshot", file=sys.stderr)
            book.reset()                       # immediate resync, no backoff
        except Exception as e:
            print(f"[ws error] {describe_error(e)} — reconnecting in {delay}s", file=sys.stderr)
            book.reset()
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
    asyncio.create_task(ws_loop(args.url, subscriptions, book, loggers, args.timestamp))

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
