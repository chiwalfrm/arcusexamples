#!/usr/bin/env python3
"""Shared helpers for the Arcus PUBLIC (read-only) tools.

The public tools were originally each fully self-contained (no shared import). That copied a lot of
identical code -- argparse validators, Decimal/number/time formatters, the HTTP JSON GET, the
WebSocket scaffolding, the markets cache. This module is the deliberate reversal of that rule (a
design-goal change: accept ONE shared helper to cut the copy-paste tech debt and keep the shared
code in one maintainable place).

LIGHT by design: the utilities, the HTTP get_json (urllib/http.client), and the WebSocket-tool
scaffolding (logging / error-describe / the shared CachePublisher) all need nothing beyond the stdlib
EXCEPT the CachePublisher, whose redis client uses `redis.asyncio` -- imported GUARDED (aioredis=None
if absent), so a plain display tool that imports this module never needs redis or any third-party
package. The `websockets` reconnect body (ws_loop) and each tool's frame transform / handle_message
stay in the WS tools. It imports NO private module and NOT the heavy arcus_common_private -- the public tools
stay runnable on any Python.
"""
import argparse
import asyncio
import http.client
import json
import logging
import math
import os
import random
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from logging.handlers import RotatingFileHandler

try:
    import redis.asyncio as aioredis      # OPTIONAL: absent => CachePublisher unavailable (make_publisher -> None)
except ImportError:
    aioredis = None


# ── networks / endpoints ─────────────────────────────────────────────────────
# The Arcus REST API base per network. The canonical map for the PUBLIC tools (the private tools have
# their own copy in arcus_common_private -- the public/private boundary forbids sharing one).
NETWORKS = {
    "testnet": "https://api.testnet.arcus.xyz",
    "staging": "https://api.staging.arcus.xyz",
    "mainnet": "https://api.arcus.xyz",
}


def ws_url(network):
    """The WebSocket URL for `network`, derived from the REST base (https -> wss, + /v1/ws)."""
    return NETWORKS[network].replace("https://", "wss://", 1) + "/v1/ws"


def plan_reconnect_sleep(conn_start, now, delay, base, max_delay, stable_after,
                         reconnect_interval=None, immediate_jitter=2.0):
    """Compute (sleep_seconds, next_delay) for a WS reconnect loop. Pure (no I/O, no await) so it's
    unit-testable; the caller does the actual `await asyncio.sleep(sleep_seconds)` and carries next_delay.

    `conn_start` is the monotonic time the just-closed connection was established (None if the attempt
    never connected -- e.g. an HTTP 429 on connect); `now` is time.monotonic(). A drop counts as GENUINE
    (a real connection that dropped, not a failed connect or an accept-then-close flap) iff the connection
    stayed up >= stable_after.

    reconnect_interval is None  -> EXPONENTIAL backoff (the DEFAULT, unchanged): full-jitter sleep over
        [0, delay]; delay doubles up to max_delay; delay resets to base when the drop was genuine. Fast to
        recover an isolated drop, but on a SYNCHRONIZED mass-disconnect the 1->2->4->8->16->32 ramp fires
        ~6 attempts/program in the first minute (N*6 attempts/min), which trips a per-IP new-conns/min cap.

    reconnect_interval is a positive number -> FIXED-INTERVAL mode (opt-in via --reconnect-interval): a
        GENUINE drop reconnects IMMEDIATELY (jittered over [0, immediate_jitter] to de-sync a fleet that
        dropped on the same instant); a FAILED or short-lived attempt waits ~reconnect_interval (jittered
        up to +25%) before the next try. This bounds a mass-disconnect of N programs to ~N attempts per
        interval -- for a large fleet behind one IP (e.g. an unstable VPN that rotates the public IP and
        drops all connections at once), that keeps the reconnect rate under the cap instead of storming.
    """
    genuine = conn_start is not None and (now - conn_start) >= stable_after
    if reconnect_interval is not None:
        if genuine:
            return random.uniform(0, immediate_jitter), delay        # real connection dropped -> retry now
        # failed connect (e.g. 429) or accept-then-close flap -> wait a full interval before the next attempt
        return reconnect_interval + random.uniform(0, reconnect_interval * 0.25), delay
    # default: exponential backoff with full jitter (behaviour unchanged)
    if genuine:
        delay = base
    return random.uniform(0, delay), min(delay * 2, max_delay)


# ── HTTP JSON reader ─────────────────────────────────────────────────────────
HTTP_TIMEOUT = 10.0     # default per-request read timeout (s)
RETRY_AFTER_CAP = 30.0  # s; cap an honored Retry-After so a hostile/misconfigured header can't park a caller
IP_WEIGHT_REFILL_PER_S = 25.0   # per-IP weight-bucket refill (rate-limits: 1500 weight/min); see page_pace_delay


def page_pace_delay(weight=70.0, jitter=0.3):
    """Politeness inter-page delay (s) for a paginated list read, so a full-history walk doesn't blow the
    per-IP weight budget. `weight` = one page's cost: a list endpoint is base 20 + floor(rows/20), so a full
    1000-row page = 70. The per-IP bucket refills IP_WEIGHT_REFILL_PER_S/s, so weight/refill is the MINIMUM
    sustainable spacing (below it the bucket goes negative post-flight and the next page 429s). Full-jitter up
    to +`jitter` fraction so repeated runs / concurrent tools on the same IP don't align into a burst. Pass the
    result as get_json's `delay` on every page AFTER the first (the first rides the already-full bucket)."""
    return (weight / IP_WEIGHT_REFILL_PER_S) * (1 + random.uniform(0, jitter))


def get_json(url, *, what="request", prog="arcus", retries=5, none_on_404=False,
             timeout=HTTP_TIMEOUT, delay=0.0, on_retry=None):
    """GET `url` -> parsed JSON, retrying TRANSIENT failures with capped exponential backoff. The
    canonical Arcus-API reader (adopts the dydx get_json engineering, arcus-tuned).

    TRANSIENT (retried): network / connect-or-read timeout / OSError; HTTP 429 -- BOTH the app
    rate-limiter AND Cloudflare's edge limiter (`error code: 1015`) surface as 429, classified by
    status alone (mirrors private arcus_common_private.retry_after_seconds), honoring `Retry-After` over
    blind backoff and clamped to RETRY_AFTER_CAP; HTTP 5xx; a truncated response
    (http.client.HTTPException, e.g. IncompleteRead under load); and truncated / invalid JSON.

    TERMINAL: HTTP 404 -> return None when `none_on_404` (caller treats it as 'none of this kind'),
    else a hard error like any other; any NON-429 4xx or exhausting `retries` raises
    SystemExit(f"{prog}: {what}: HTTP {code}: {msg}") -- `msg` is the Arcus API's own `{"error": ...}`
    response body when present (more specific than the bare HTTP reason).

    `delay` is a politeness PRE-pause; `on_retry` (if given) is called with no args on each
    backoff-sleep (a hook to count retries). Returns parsed JSON of ANY type -- callers that need an
    object wrap it in require_dict(). Stdlib urllib, SYNCHRONOUS."""
    if delay:
        time.sleep(delay)
    last = None
    for attempt in range(retries):
        retry_after = None
        try:
            with urllib.request.urlopen(urllib.request.Request(url, method="GET"), timeout=timeout) as r:
                return json.loads(r.read() or b"{}")
        except urllib.error.HTTPError as e:
            if e.code == 404 and none_on_404:
                return None
            if e.code != 429 and e.code < 500:                # non-retryable 4xx -> fatal (with the API's error body)
                try:
                    body = json.loads(e.read() or b"{}")
                    msg = body.get("error", "") if isinstance(body, dict) else ""   # non-object body -> no msg, not AttributeError
                except (ValueError, TypeError):
                    msg = ""
                raise SystemExit(f"{prog}: {what}: HTTP {e.code}: {msg or 'request failed'}")
            last = f"HTTP {e.code}"                            # 429 / 5xx -> transient
            if e.code == 429 and e.headers:                   # honor Retry-After over blind backoff (covers the 1015 edge case)
                ra = e.headers.get("Retry-After")
                try:
                    retry_after = float(ra) if ra is not None else None
                except (TypeError, ValueError):
                    retry_after = None                        # HTTP-date form (rare) or junk -> exponential only
                if retry_after is not None and not math.isfinite(retry_after):   # "nan"/"inf" parse fine; make finiteness
                    retry_after = None                                            # EXPLICIT so it can never feed the sleep
        except (urllib.error.URLError, TimeoutError, OSError) as e:
            last = f"could not reach the Arcus API: {e}"
        except http.client.HTTPException as e:                # IncompleteRead / BadStatusLine (truncated under load)
            last = f"HTTP protocol error: {type(e).__name__}: {e}"
        except json.JSONDecodeError as e:
            last = f"invalid JSON from the Arcus API: {e}"
        if attempt < retries - 1:
            backoff = min(2 ** attempt, 8)                    # 1, 2, 4, 8, 8, ...
            if retry_after is not None:
                backoff = max(backoff, min(retry_after, RETRY_AFTER_CAP))
            # Full-jitter ON TOP (never below `backoff`, so an honored Retry-After floor is preserved) so
            # repeated runs / multiple tools don't re-collide on the same retry instant after a 429.
            backoff *= 1 + random.uniform(0, 0.25)
            if on_retry is not None:
                on_retry()
            time.sleep(backoff)
    raise SystemExit(f"{prog}: {what}: failed after {retries} attempts ({last}).")


def get_json_dict(url, what, prog, **kw):
    """get_json + require_dict in one: fetch a REQUIRED JSON OBJECT from the Arcus API. Most Arcus
    reads return an object ({"fills": [...]}, {"account": {...}}, ...); this is the reader for them,
    so a tool binds it once (e.g. `_get_json = functools.partial(get_json_dict, prog="display_fills")`)
    instead of repeating the get_json+require_dict pair. `**kw` forwards to get_json (retries/timeout/
    delay/on_retry). NOT for none_on_404 -- a None would fail the object-check; use bare get_json there."""
    return require_dict(get_json(url, what=what, prog=prog, **kw), what, prog)


# ── markets cache + resolution ───────────────────────────────────────────────
# The launcher warms a shared /v1/markets cache file so the many per-market tools it starts resolve
# their market from ONE file instead of each re-hitting the API. Read is fail-open (missing/corrupt ->
# live fetch). Shared by showorderbook / showmarkets / wsorderbook.
MARKETS_CACHE_FMT = "/tmp/arcus_markets_{network}.json"


def markets_cache_path(network):
    """The shared /v1/markets cache file for `network`: $ARCUS_MARKETS_CACHE if the launcher set it (a
    per-run path, so a foreign/stale file at the predictable path can't be trusted), else the
    predictable MARKETS_CACHE_FMT default."""
    return os.environ.get("ARCUS_MARKETS_CACHE") or MARKETS_CACHE_FMT.format(network=network)


def read_markets_cache(path):
    """Return a parsed /v1/markets response from the launcher's shared cache file, or None.
    Fail-open: any problem (missing / unreadable / corrupt / wrong shape) returns None so the caller
    does a live fetch. Only a payload whose 'markets' is a list is trusted."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, ValueError):        # ValueError covers json.JSONDecodeError AND UnicodeDecodeError (invalid bytes)
        return None
    if isinstance(data, dict) and isinstance(data.get("markets"), list):
        return data
    return None


def write_markets_cache(path, data):
    """Best-effort ATOMIC write (temp + os.replace, so a reader never sees a partial file) to warm
    the launcher's shared cache. Never raises: a cache-write failure must not break a working tool."""
    tmp = f"{path}.{os.getpid()}.tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f)
        os.replace(tmp, path)
    except (OSError, TypeError, ValueError):   # OSError=I/O; TypeError/ValueError=json.dump on non-serializable data
        try:
            os.unlink(tmp)
        except OSError:
            pass


def resolve_market_id(base, market, prog):
    """Resolve `market` (display name case-insensitive OR numeric marketId) to a canonical marketId
    STRING via a LIVE GET /v1/markets. SystemExit(f"{prog}: ...") on a bad response or an unknown
    market -- a typo must never silently return an empty result. Used by the display tools' --market
    filter. (The orderbook tools resolve cache-first + inline -- see read_markets_cache callers.)"""
    markets = get_json_dict(f"{base}/v1/markets", "markets", prog).get("markets")
    if not isinstance(markets, list):
        raise SystemExit(f"{prog}: unexpected /v1/markets response (no 'markets' list).")
    for m in markets:
        if not isinstance(m, dict):     # malformed element -> skip, don't AttributeError on .get()
            continue
        if (str(market).upper() == str(m.get("marketDisplayName", "")).upper()
                or str(market) == str(m.get("marketId"))):
            return str(m.get("marketId"))
    raise SystemExit(f"{prog}: unknown market {market!r} (not found in /v1/markets).")


# ── argparse types ───────────────────────────────────────────────────────────
def positive_int(s):
    """argparse type: a positive integer (rejects 0 and negatives). Used e.g. for --max-bytes /
    --log-backups, where a 0 would disable RotatingFileHandler rollover entirely."""
    v = int(s)
    if v <= 0:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return v


def epoch_ms_arg(s):
    """argparse type: a non-negative epoch-milliseconds integer."""
    v = int(s)
    if v < 0:
        raise argparse.ArgumentTypeError("must be a non-negative epoch-ms timestamp")
    return v


def epoch_us_arg(s):
    """argparse type: a non-negative epoch-MICROseconds integer -- the unit the /v1/fills and
    /v1/accountTransferUpdates from/to filters use (same as their createdAt field, so a createdAt from one
    page is a valid bound for the next). NB /v1/funding is different (ms). 0 is allowed (no bound);
    a seconds/milliseconds-scale value (0 < v < 1e14) is a near-certain unit mistake (it would land in 1970,
    and the API rejects it), so catch it here with a clearer message than the server's 400."""
    v = int(s)
    if v < 0:
        raise argparse.ArgumentTypeError("must be a non-negative epoch-microseconds timestamp")
    if 0 < v < 100_000_000_000_000:      # < 1e14 -> seconds/milliseconds scale, not microseconds
        raise argparse.ArgumentTypeError(
            f"{v} looks like seconds/milliseconds; these from/to are epoch MICROseconds (>= 1e14, the "
            f"createdAt unit) -- multiply a millisecond value by 1000")
    return v


UNLIMITED = "unlimited"   # --limit sentinel (limit_arg): paginate to completeness


def limit_arg(s):
    """argparse type for a paginating --limit: an integer in [1, 1000] (the API's max), or the
    sentinel 'unlimited' (returned as UNLIMITED) to paginate fully. Used by the display tools whose
    --limit can be uncapped; a tool with a hard cap keeps its own bounded type."""
    if s.strip().lower() == UNLIMITED:
        return UNLIMITED
    v = int(s)
    if not 1 <= v <= 1000:
        raise argparse.ArgumentTypeError("must be between 1 and 1000, or 'unlimited'")
    return v


# ── Decimal / formatting ─────────────────────────────────────────────────────
def dec(v):
    """Parse a decimal string -> Decimal, or None if absent/invalid/non-finite."""
    try:
        d = Decimal(str(v))
    except (InvalidOperation, TypeError, ValueError):
        return None
    return d if d.is_finite() else None


def num(value, decimals=2):
    """Decimal-string -> fixed-precision, comma-grouped; '-' if not numeric. Uses Decimal (not
    float) -- sizes/PnL/notional are decimal strings, so this avoids binary rounding artifacts."""
    d = dec(value)
    return f"{d:,.{decimals}f}" if d is not None else "-"


# ── time ─────────────────────────────────────────────────────────────────────
def when(micros):
    """Epoch MICROseconds -> 'YYYY-MM-DD HH:MM:SS' UTC, or '-' if absent/invalid.

    NB the Arcus API mixes units (verified live + docs): RESPONSE timestamps (this `time`/`createdAt`
    field) are epoch MICROseconds ("user-facing timestamps are now microseconds"), while the REQUEST
    filters --from/--to are epoch MILLISECONDS. So divide by 1e6 here; pass --from/--to through as-is.
    A real value 1782583200000000 -> 2026-06-27 (µs); /1e3 would be year 58457."""
    if micros is None or micros == "":
        return "-"
    try:
        return datetime.fromtimestamp(int(micros) / 1_000_000, timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    except (ValueError, TypeError, OverflowError, OSError):
        return "-"


# ── misc ─────────────────────────────────────────────────────────────────────
def require_dict(data, what, prog):
    """A decoded JSON body that must be an object -> raise a clean CLI error (f"{prog}: ...") if the
    server returned null / a list / a scalar instead, so the .get(...) callers can't AttributeError."""
    if not isinstance(data, dict):
        raise SystemExit(f"{prog}: unexpected {what} response shape (not a JSON object)")
    return data


def created_key(row):
    """Sort key by createdAt (use reverse=True for newest-first); missing/bad sorts oldest."""
    try:
        return int(row.get("createdAt"))
    except (TypeError, ValueError):
        return -1


def market_id_key(row):
    """Sort key by NUMERIC marketId (so 2 < 10); missing/non-numeric sort last."""
    try:
        return (0, int(row.get("marketId")))
    except (TypeError, ValueError):
        return (1, 0)


# ── WebSocket-tool scaffolding ───────────────────────────────────────────────
# Shared by the streaming tools (wsaccount / wsexchange / wsorderbook): rotating per-channel logger,
# error description, and (for the account/exchange cache warmers) the self-healing Redis CachePublisher.
# Each tool keeps its OWN ws_loop reconnect body + frame transform / handle_message. wsorderbook has a
# separate BboPublisher (BBO is derived, not cache-warmed), left inlined there.
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


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def log_ts():
    """UTC "YYYY-MM-DD HH:MM:SS <epoch>" prefix for operational stdout/stderr log lines (WS reconnect/error
    events span days and previously had NO timestamp, forcing incident forensics onto file mtimes). Human date
    for eyeballing; the trailing unix-epoch is what showlogs.sh windows on (ABSOLUTE, no date-guessing). UTC
    to match the venue's epoch-nanos frames."""
    now = datetime.now(timezone.utc)
    return f"{now.strftime('%Y-%m-%d %H:%M:%S')} {int(now.timestamp())}"


def setup_logger(name, path, max_bytes, backup_count):
    logger = logging.getLogger(name)
    if logger.handlers:                       # idempotent: don't stack handlers
        return logger
    handler = RotatingFileHandler(path, maxBytes=max_bytes, backupCount=backup_count)
    handler.setFormatter(logging.Formatter("%(message)s"))
    logger.setLevel(logging.DEBUG)
    logger.addHandler(handler)
    logger.propagate = False                  # don't bubble to the root logger
    return logger


def emit(logger, line):
    """Write one line; a logging failure is loud (not silently swallowed)."""
    try:
        logger.debug(line)
    except Exception as e:                     # disk full, rotation error, etc.
        print(f"[log error] {e}", file=sys.stderr)


# ── Redis cache publisher (shared by wsaccount + wsexchange; self-healing, never raises) ──────────────
REDIS_URL = os.environ.get("ARCUS_REDIS_URL", "redis://127.0.0.1:6379/0")   # the WS publishers' Redis endpoint
CACHE_TTL = 20            # s; Redis TTL on every warmed key (>= 4x HEARTBEAT -> several missed beats of headroom)
PUB_SOCKET_TIMEOUT = 1.0  # s; bound every Redis op AND connect so a WEDGED (half-open) socket RAISES, feeding the self-heal
PUB_RECREATE_AFTER = 20   # consecutive publish failures before the client is rebuilt (clears a stuck pool)
_PUB_ERR_THROTTLE = 30.0  # s; min gap between logged publish-error lines (a flapping Redis can't flood the log)


async def _close_client(client):
    """Best-effort, bounded close of a redis.asyncio client. Never raises."""
    try:
        closer = getattr(client, "aclose", None) or getattr(client, "close", None)  # aclose() (redis-py>=5) or close()
        if closer is not None:
            await asyncio.wait_for(closer(), PUB_SOCKET_TIMEOUT)
    except Exception:
        pass


class CachePublisher:
    """Owns the optional Redis client and SELF-HEALS. publish() NEVER raises into the caller (the WS
    loop must survive a Redis outage untouched). Throttle/fail state is per-instance."""

    def __init__(self, url, log_prefix):
        self._url = url
        self._prefix = log_prefix
        self._r = self._new()          # from_url is lazy -- connects on the first command
        self._fails = 0
        self._err_last = 0.0
        self._err_suppressed = 0
        self._recreate_last = 0.0

    def _new(self):
        return aioredis.from_url(self._url, socket_timeout=PUB_SOCKET_TIMEOUT,
                                 socket_connect_timeout=PUB_SOCKET_TIMEOUT)

    def _log(self, msg):
        try:
            print(f"[{log_ts()}] {self._prefix} {msg}", file=sys.stderr, flush=True)
        except Exception:
            pass

    async def publish(self, key, blob, ttl=CACHE_TTL):
        """SET key = json(blob) EX ttl. Best-effort: logs (throttled) + self-heals, never propagates."""
        try:
            await self._r.set(key, json.dumps(blob, separators=(",", ":")), ex=ttl)
            self._fails = 0
            self._err_suppressed = 0     # recovered; _err_last kept so a FLAPPING Redis still can't flood
        except Exception as e:
            self._fails += 1
            now = time.monotonic()
            if now - self._err_last >= _PUB_ERR_THROTTLE:
                extra = f" (+{self._err_suppressed} suppressed)" if self._err_suppressed else ""
                self._log(f"{describe_error(e)}{extra}")
                self._err_last, self._err_suppressed = now, 0
            else:
                self._err_suppressed += 1
            if self._fails >= PUB_RECREATE_AFTER:
                await self._recreate()

    async def _recreate(self):
        """Rebuild the client (clears a stuck pool). Fully guarded: MUST NOT raise (runs inside publish's except)."""
        old = self._r
        try:
            self._r = self._new()
        except Exception:
            pass
        self._fails = 0
        now = time.monotonic()
        if now - self._recreate_last >= _PUB_ERR_THROTTLE:
            self._log(f"recreated client after {PUB_RECREATE_AFTER} consecutive failures")
            self._recreate_last = now
        if self._r is not old:                          # only close the OLD client if _new() actually produced a new one
            await _close_client(old)

    async def close(self):
        await _close_client(self._r)


def make_publisher(url, log_prefix):
    """A CachePublisher, or None if redis.asyncio is unavailable (publishing simply off)."""
    return None if aioredis is None else CachePublisher(url, log_prefix)
