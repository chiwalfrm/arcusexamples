#!/usr/bin/env python3
"""Subscribe to an account's WebSocket channels and log each to a rotating file.

  wsaccount.py 0xADDRESS
  wsaccount.py 0xADDRESS --log-dir /tmp/ws --channels account,orders --timestamp

One rotating log file per channel. Unrecognized channels / unparseable frames
are printed to stdout (capture stdout to keep them). Reconnects with backoff.
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
OPEN_TIMEOUT    = 10
PING_INTERVAL   = 20
PING_TIMEOUT    = 20
ADDR_RE         = re.compile(r"^0x[0-9a-fA-F]{40}$")

CHANNELS = [
    "accountAttributeUpdates",
    "account",
    "funding",
    "orders",
    "positions",
    "userFills",
]


# ── Logger setup ─────────────────────────────────────────────────────────────
def setup_logger(name: str, path: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.handlers:                       # idempotent: don't stack handlers
        return logger
    handler = RotatingFileHandler(path, maxBytes=LOG_MAX_BYTES, backupCount=LOG_BACKUP)
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
        try:
            async with websockets.connect(
                url, open_timeout=OPEN_TIMEOUT,
                ping_interval=PING_INTERVAL, ping_timeout=PING_TIMEOUT,
            ) as ws:
                delay = RECONNECT_BASE          # connected: reset backoff
                for sub in subscriptions:
                    await ws.send(json.dumps(sub))
                async for raw in ws:
                    handle_message(raw, loggers, add_ts)
        except Exception as e:
            print(f"[ws error] {describe_error(e)} — reconnecting in {delay}s", file=sys.stderr)
            await asyncio.sleep(delay)
            delay = min(delay * 2, RECONNECT_MAX)


# ── Entry point ──────────────────────────────────────────────────────────────
async def amain(args):
    address = args.address
    if not ADDR_RE.match(address):
        raise SystemExit(f"wsaccount: invalid Ethereum address {address!r} "
                         f"(expected 0x + 40 hex chars).")

    channels = [c.strip() for c in args.channels.split(",") if c.strip()]
    if not channels:
        raise SystemExit("wsaccount: --channels is empty.")
    # Reject typos (e.g. 'order' for 'orders') unless explicitly allowed -- an
    # unknown channel would silently create order<addr>.log and subscribe to nothing.
    unknown = [c for c in channels if c not in CHANNELS]
    if unknown and not args.allow_unknown_channel:
        raise SystemExit(f"wsaccount: unknown channel(s) {unknown}; known: {CHANNELS}. "
                         f"Use --allow-unknown-channel to subscribe anyway.")

    try:
        os.makedirs(args.log_dir, exist_ok=True)
    except OSError as e:
        raise SystemExit(f"wsaccount: cannot create log dir {args.log_dir!r}: {e}")

    # A logger per channel. Logger names are namespaced by address so multiple
    # runs don't collide. Unknown channels / unparseable frames go to stdout.
    loggers = {
        ch: setup_logger(f"wsaccount.{ch}.{address}", f"{args.log_dir}/{ch}{address}.log")
        for ch in channels
    }

    subscriptions = [
        {"type": "subscribe", "channel": ch, "id": address, "snapshot": True}
        for ch in channels
    ]

    print(f"[{address}] Subscribing to {channels}")
    for ch in channels:
        print(f"  {ch} → {args.log_dir}/{ch}{address}.log")

    await ws_loop(args.url, subscriptions, loggers, args.timestamp)


def main():
    parser = argparse.ArgumentParser(description="Log an account's WebSocket channels to rotating files.")
    parser.add_argument("address", help="Ethereum address (0x + 40 hex)")
    parser.add_argument("--log-dir", default=None,
                        help="log directory (default: /mnt/arcuslogs/<network>)")
    parser.add_argument("--url", default=None,
                        help="override the WebSocket URL (default: derived from the network)")
    parser.add_argument("--channels", default=",".join(CHANNELS),
                        help="comma-separated channels (default: all known account channels)")
    parser.add_argument("--allow-unknown-channel", action="store_true",
                        help="permit channels not in the known list (otherwise a typo is rejected)")
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
    asyncio.run(amain(args))


if __name__ == "__main__":
    main()
