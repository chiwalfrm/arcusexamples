#!/usr/bin/env python3
"""Subscribe to exchange-wide WebSocket channels and log each to a rotating file.

  wsexchange.py
  wsexchange.py --log-dir /tmp/ws --max-bytes 5000000 --no-snapshot --timestamp

Logs `exchangeAttributeUpdates` and `markets` to one rotating file each.
Unrecognized channels / unparseable frames are printed to stdout (capture the
program's stdout to keep them). Reconnects with exponential backoff.
"""
import argparse
import asyncio
import json
import logging
import os
import sys
import time
import urllib.error
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler

import websockets

# ── Constants ────────────────────────────────────────────────────────────────
NETWORKS = {
    "testnet": "wss://api.testnet.arcus.xyz/v1/ws",
    "staging": "wss://api.staging.arcus.xyz/v1/ws",
    "mainnet": "wss://api.arcus.xyz/v1/ws",   # live 2026-06-25 (reads only for now)
}
LOG_BASE        = "/mnt/arcuslogs"   # logs go under LOG_BASE/<network> (subdir auto-created)
LOG_MAX_BYTES   = 2097152
LOG_BACKUP      = 4
RECONNECT_BASE  = 1          # seconds; doubles per failure
RECONNECT_MAX   = 60
STABLE_AFTER    = 30         # s a connection must STAY UP before backoff resets (else an accept-then-close flap busy-loops)
OPEN_TIMEOUT    = 10
PING_INTERVAL   = 20
PING_TIMEOUT    = 20

CHANNELS = ["exchangeAttributeUpdates", "markets"]


# ── Logger setup ─────────────────────────────────────────────────────────────
def setup_logger(name: str, path: str, max_bytes: int) -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.handlers:                       # idempotent: don't stack handlers
        return logger
    handler = RotatingFileHandler(path, maxBytes=max_bytes, backupCount=LOG_BACKUP)
    handler.setFormatter(logging.Formatter("%(message)s"))
    logger.setLevel(logging.DEBUG)
    logger.addHandler(handler)
    logger.propagate = False                  # don't bubble to the root logger
    return logger


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


# ── Message handling ──────────────────────────────────────────────────────────
def handle_message(raw, loggers, add_ts):
    """Route a frame to its channel log; unknown/malformed frames go to STDOUT
    (the operator captures stdout, so they aren't lost)."""
    try:
        msg = json.loads(raw)
    except json.JSONDecodeError as e:
        print(f"[parse error] {e} | {raw}")
        return
    if not isinstance(msg, dict):          # valid JSON but not an object (bare array/number) -> STDOUT, keep reading
        print(raw)
        return

    logger = loggers.get(msg.get("channel"))
    if logger is None:                        # not a subscribed channel -> stdout
        print(raw)
        return
    line = json.dumps({"receivedAt": now_iso(), "msg": msg}, separators=(",", ":")) \
        if add_ts else raw
    _emit(logger, line)


def _emit(logger, line):
    """Write one line; a logging failure is loud (not silently swallowed)."""
    try:
        logger.debug(line)
    except Exception as e:                     # disk full, rotation error, etc.
        print(f"[log error] {e}", file=sys.stderr)


# ── WebSocket loop ───────────────────────────────────────────────────────────
async def ws_loop(url, subscriptions, loggers, add_ts):
    delay = RECONNECT_BASE
    while True:
        conn_start = None
        try:
            async with websockets.connect(
                url, open_timeout=OPEN_TIMEOUT,
                ping_interval=PING_INTERVAL, ping_timeout=PING_TIMEOUT,
            ) as ws:
                conn_start = time.monotonic()
                for sub in subscriptions:
                    await ws.send(json.dumps(sub))
                async for raw in ws:
                    handle_message(raw, loggers, add_ts)
        except Exception as e:
            print(f"[ws error] {describe_error(e)} — reconnecting", file=sys.stderr)   # actual delay set after the stability reset below
        # Sleep/backoff ALWAYS (outside the except) so a CLEAN server close (async-for ended, no exception)
        # also backs off -- previously that path skipped the except and busy-looped at 0 delay. Reset backoff
        # ONLY if the connection proved STABLE (>= STABLE_AFTER s); resetting on connect defeated backoff.
        if conn_start is not None and (time.monotonic() - conn_start) >= STABLE_AFTER:
            delay = RECONNECT_BASE
        await asyncio.sleep(delay)
        delay = min(delay * 2, RECONNECT_MAX)


# ── Entry point ──────────────────────────────────────────────────────────────
async def amain(args):
    try:
        os.makedirs(args.log_dir, exist_ok=True)
    except OSError as e:
        raise SystemExit(f"wsexchange: cannot create log dir {args.log_dir!r}: {e}")

    loggers = {
        ch: setup_logger(f"wsexchange.{ch}", f"{args.log_dir}/{ch}.log", args.max_bytes)
        for ch in CHANNELS
    }

    snapshot = not args.no_snapshot
    subscriptions = [
        {"type": "subscribe", "channel": ch, "snapshot": snapshot} for ch in CHANNELS
    ]

    print(f"[wsexchange] Subscribing to {CHANNELS} (snapshot={snapshot})")
    for ch in CHANNELS:
        print(f"  {ch} → {args.log_dir}/{ch}.log")

    await ws_loop(args.url, subscriptions, loggers, args.timestamp)


def positive_int(s):
    """argparse type: a positive integer (rejects 0 and negatives).

    maxBytes=0 disables RotatingFileHandler's rotation, so keep it > 0.
    """
    v = int(s)
    if v <= 0:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return v


def main():
    parser = argparse.ArgumentParser(description="Log exchange-wide WebSocket channels to rotating files.")
    parser.add_argument("--log-dir", default=None,
                        help="log directory (default: /mnt/arcuslogs/<network>)")
    parser.add_argument("--url", default=None,
                        help="override the WebSocket URL (default: derived from the network)")
    parser.add_argument("--max-bytes", type=positive_int, default=LOG_MAX_BYTES,
                        help=f"rotating-file size cap in bytes, > 0 (default {LOG_MAX_BYTES})")
    parser.add_argument("--no-snapshot", action="store_true",
                        help="subscribe without an initial snapshot")
    parser.add_argument("--timestamp", action="store_true",
                        help="wrap each line as JSONL with a local receivedAt "
                             "(default: log the raw server frame)")
    net = parser.add_mutually_exclusive_group(required=True)
    net.add_argument("--testnet", dest="network", action="store_const", const="testnet",
                     help="subscribe to the testnet WebSocket")
    net.add_argument("--staging", dest="network", action="store_const", const="staging",
                     help="subscribe to the staging WebSocket")
    net.add_argument("--mainnet", dest="network", action="store_const", const="mainnet",
                     help="subscribe to the mainnet WebSocket")
    args = parser.parse_args()
    if args.url is None:
        args.url = NETWORKS[args.network]
    if args.log_dir is None:
        args.log_dir = os.path.join(LOG_BASE, args.network)
    try:
        asyncio.run(amain(args))
    except KeyboardInterrupt:
        print("\n[wsexchange] stopped")


if __name__ == "__main__":
    main()
