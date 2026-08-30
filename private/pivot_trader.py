"""
pivot_trader.py -- pivot-based stop-and-reverse bot for ONE Arcus market, run from cron.

Premise: a fixed pivot price P. Oracle ABOVE P => bullish (hold +q); BELOW P => bearish (hold -q).
The bot trades only when the oracle crosses P, always holding its OWN +q or -q on top of whatever else
is in the account (it never reads the account position -- manual/human trades are ignored entirely).

  # arm it once (sets P and q, records the starting side, stays flat, waits for a flip):
  pivot_trader.py --init --market BTC-USD --pivot 64000 --quantity 0.5 --mainnet

  # cron runs it (no P/q needed -- read from the state file):
  pivot_trader.py --market BTC-USD --mainnet

Per-run logic (normal mode):
  1. flock (one instance at a time).  2. Load state (error if none -- run --init first).
  3. Read the oracle (Redis 'markets' cache if present, else REST /v1/markets; must be finite > 0, else SKIP).
  4. side = bullish if oracle >= P else bearish.
  5. If position == 0 and side == starting_side -> WAIT (still on the entry side; first trade fires on a flip).
     Else target = +q (bullish) / -q (bearish); delta = target - position; trade the delta at market (IOC).
  This one `delta = target - position` gives the 1x first trade, the 2x flip, and self-correcting top-ups
  (a rare short fill is picked up next run) -- no partial fill can compound because `position` only moves by
  the ACTUAL fill (read from GET /v1/order/{orderId}, since placeOrder is async).

State file: pivot_state_<network>_<market>.json in the CWD (locked convention, not overridable). One bot per
directory. `position` is the bot's OWN net (sum of its own fills); deleting the file = "roll to the next pivot"
(the profitable position rides on as untracked baseline). Resolves ordersign / arcus_creds_<network>.json /
the market-order helper relative to THIS script, so it runs from any cwd.
"""

import argparse
import fcntl
import hashlib
import json
import os
import sys
import tempfile
import time
import urllib.parse
from datetime import datetime, timezone
from decimal import Decimal

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _HERE)

from ordersign import Signer
import arcus_redis as account_cache
from arcus_common_private import (add_network_args, call, dec, describe_error, load_creds,
                                  place_market_ioc, resolve_market, select_network)

PROG = "pivot_trader"
MARKETS_CACHE_TTL = 5          # s; cache-aside TTL for the shared /v1/markets blob (oracle + metadata)
FILL_POLL_TRIES = 6            # GET /v1/order retries to observe an IOC's terminal fill (placeOrder is async)
FILL_POLL_DELAY = 0.5         # s between fill polls
# An IOC never rests, so it reaches one of these the instant the engine processes it; the poll only rides out
# the brief REST propagation delay. filledSize is the source of truth regardless of which terminal status it is.
TERMINAL_STATUSES = {"FILLED", "PARTIALLY_FILLED", "CANCELED", "REJECTED"}


def _now_iso():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def log(msg):
    print(f"[{_now_iso()}] {PROG}: {msg}", flush=True)


# ── State file (the bot's entire memory) ─────────────────────────────────────────
def state_path(network, market):
    """pivot_state_<network>_<market>.json in the CWD. Convention is FIXED (not overridable). A market name
    is sanitized defensively (arcus names like BTC-USD are already filename-safe)."""
    safe = "".join(c if (c.isalnum() or c in "._-") else "_" for c in f"{network}_{market}")
    return os.path.join(os.getcwd(), f"pivot_state_{safe}.json")


def load_state(path):
    """Return the parsed state dict, or None if the file doesn't exist. A corrupt/unreadable file RAISES
    (SystemExit) rather than being silently treated as a fresh start -- that could abandon a live +/-q."""
    if not os.path.exists(path):
        return None
    try:
        with open(path) as f:
            state = json.load(f)
    except (OSError, json.JSONDecodeError) as e:
        raise SystemExit(f"{PROG}: state file {os.path.basename(path)} is unreadable/corrupt ({e}). "
                         f"Fix or remove it (rm = roll to a new pivot) -- NOT auto-recovering to avoid "
                         f"abandoning a live position.")
    if not isinstance(state, dict):
        raise SystemExit(f"{PROG}: state file {os.path.basename(path)} is not a JSON object.")
    return state


def save_state(path, state):
    """Atomic write (temp + os.replace) so a crash mid-write can never corrupt the baseline/bias."""
    state["updated_at"] = _now_iso()
    tmp = f"{path}.{os.getpid()}.tmp"
    with open(tmp, "w") as f:
        json.dump(state, f, indent=2)
        f.write("\n")
    os.replace(tmp, path)


def lock_path(path):
    """Lock file lives in the temp dir (tempfile.gettempdir(), honours TMPDIR, else /tmp) so it never clutters
    the working dir. The name keeps the state file's basename for legibility AND appends a short hash of the
    state file's ABSOLUTE path -- that preserves the current per-state-file (per-directory) lock scope, so two
    bots on the same market run from different dirs get distinct locks instead of colliding on a shared name."""
    abspath = os.path.abspath(path)
    tag = hashlib.sha256(abspath.encode()).hexdigest()[:8]
    return os.path.join(tempfile.gettempdir(), f"{os.path.basename(path)}.{tag}.lock")


def acquire_lock(path):
    """Single-instance lock (flock). A slow order must not let two cron runs overlap and double-trade.
    Returns the open file handle -- the CALLER must keep it referenced for the process lifetime."""
    lp = lock_path(path)
    lockf = open(lp, "w")
    try:
        fcntl.flock(lockf, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        raise SystemExit(f"{PROG}: another instance holds {os.path.basename(lp)}; skipping this run.")
    return lockf


# ── Oracle + market metadata (one cached /v1/markets read gives both) ────────────
def read_market_and_oracle(network, market):
    """Resolve `market` and read its oracle price from the shared 'markets' cache (Redis if present, else REST
    /v1/markets -- cache-aside, redis-optional). Returns (mkt_dict, oracle_Decimal_or_None). Mirrors
    market_maker.oracle_price(): the field is `oraclePrice`, and only a FINITE POSITIVE value is usable (arcus
    emits "0" when there is no oracle -- the caller treats None/<=0 as 'no usable oracle' and skips the run)."""
    blob = account_cache.cached_get(network, None, "markets",
                                    lambda: call("GET", "/v1/markets"), MARKETS_CACHE_TTL)
    markets = blob.get("markets") if isinstance(blob, dict) else None
    if not isinstance(markets, list):
        raise SystemExit(f"{PROG}: unexpected /v1/markets response (no 'markets' list).")
    mkt = resolve_market(markets, market)
    if mkt is None:
        raise SystemExit(f"{PROG}: unknown market {market!r}.")
    oracle = dec(mkt.get("oraclePrice"))
    if oracle is not None and (not oracle.is_finite() or oracle <= 0):
        oracle = None                                # 0 / non-finite = no usable oracle
    return mkt, oracle


def side_of(oracle, pivot):
    """Bullish at or above the pivot, bearish below. (Exact equality is a formality with a real oracle.)"""
    return "bullish" if oracle >= pivot else "bearish"


def min_trade_size(mkt):
    """Smallest size the bot will place: the market's minOrderSize, or the step size if that's absent."""
    ms = dec(mkt.get("minOrderSize"))
    if ms is not None and ms > 0:
        return ms
    step = dec(mkt.get("stepSize"))
    return step if (step is not None and step > 0) else Decimal(0)


# ── Reading the bot's OWN fill (placeOrder is async -> read GET /v1/order/{orderId}) ──
def read_order_terminal(order_id, address, account_index):
    """Poll GET /v1/order/{orderId} until the order is terminal (an IOC settles at once; this just rides out
    REST propagation). Returns the order dict (terminal if reached, else the last body / {} on failure)."""
    q = urllib.parse.urlencode({"address": address, "accountIndex": account_index})
    o = {}
    for i in range(FILL_POLL_TRIES):
        o = call("GET", f"/v1/order/{urllib.parse.quote(str(order_id))}?{q}")
        if isinstance(o, dict) and str(o.get("status", "")).upper() in TERMINAL_STATUSES:
            return o
        if i < FILL_POLL_TRIES - 1:
            time.sleep(FILL_POLL_DELAY)
    return o if isinstance(o, dict) else {}


def apply_fill(order, state):
    """Add an order's DEFINITIVE fill to the bot's own `position` (signed by the order's side). Returns
    (signed_fill, side, filled, status)."""
    side = str(order.get("side", "")).upper()
    filled = dec(order.get("filledSize")) or Decimal(0)
    if filled < 0:
        filled = Decimal(0)
    signed = filled if side == "BUY" else -filled
    state["position"] = str(dec(state["position"]) + signed)
    return signed, side, filled, str(order.get("status"))


def settle_pending(address, account_index, state, path, tag=""):
    """If a `pending_order_id` is recorded (a prior run placed an order but crashed before accounting for it),
    read its definitive fill and fold it into `position`, then clear it. If the order still isn't terminal,
    LEAVE it pending (retried next run) rather than applying an unconfirmed fill -- never double-counts."""
    oid = state.get("pending_order_id")
    if not oid:
        return
    order = read_order_terminal(oid, address, account_index)
    if str(order.get("status", "")).upper() not in TERMINAL_STATUSES:
        log(f"{tag}pending order {oid} not yet terminal (status={order.get('status')}); leaving it to reconcile next run")
        return
    signed, side, filled, status = apply_fill(order, state)
    state["pending_order_id"] = None
    save_state(path, state)
    log(f"{tag}reconciled pending order {oid}: {side} filled {filled} (status {status}); position -> {state['position']}")


def execute_trade(signer, address, account_index, mkt, trade_side, size, state, path):
    """Place a MARKET IOC for `size` (Decimal > 0) on `trade_side`, then read the DEFINITIVE fill and fold it
    into `position`. Records `pending_order_id` BEFORE reading the fill so a crash mid-read is reconciled next
    run (not double-traded). A slippage-guard block just skips this run (position unchanged; retried next)."""
    market_name = mkt.get("marketDisplayName", "?")
    result = place_market_ioc(signer, address, account_index, mkt, trade_side, size)
    if not result["placed"]:
        log(f"[{market_name}] NOT PLACED ({result['reason']}; mid={result['mid']}, est fill={result['avg_fill']}) "
            f"-- position unchanged, will retry next run")
        return
    order = result["order"]
    order_id = order.get("orderId")
    if not order_id:
        raise SystemExit(f"{PROG}: placeOrder response carries no orderId -- cannot confirm the fill: {order}")
    state["pending_order_id"] = order_id                 # persist BEFORE reading the fill (crash-safe)
    save_state(path, state)
    order_status = read_order_terminal(order_id, address, account_index)
    if str(order_status.get("status", "")).upper() not in TERMINAL_STATUSES:
        log(f"[{market_name}] order {order_id} not terminal after polling (status={order_status.get('status')}) "
            f"-- leaving it PENDING to reconcile next run (position NOT advanced)")
        return
    signed, side, filled, status = apply_fill(order_status, state)
    state["pending_order_id"] = None
    save_state(path, state)
    short = "" if result["enough"] else " [book thinner than size -- partial]"
    log(f"[{market_name}] {trade_side} {size} @ bound {result['bound']} (est slippage {result['slippage'] * 100:.3f}%): "
        f"orderId {order_id} status {status} filled {filled}{short}; position -> {state['position']}")


# ── --init: arm / re-arm the state file ──────────────────────────────────────────
def do_init(args):
    select_network(args.network)
    path = state_path(args.network, args.market)
    lock = acquire_lock(path)                                                     # noqa: F841 (held for lifetime)
    existing = load_state(path)
    if existing is not None:
        pos = dec(existing.get("position", "0")) or Decimal(0)
        if pos != 0:
            raise SystemExit(f"{PROG}: {os.path.basename(path)} exists with position {existing.get('position')} "
                             f"(the bot is IN A TRADE) -- refusing to overwrite. To roll to a new pivot, bank the "
                             f"position and `rm {os.path.basename(path)}` first, then re-run --init.")
    pivot = dec(args.pivot)
    if pivot is None or not pivot.is_finite() or pivot <= 0:
        raise SystemExit(f"{PROG}: --pivot must be a positive number (got {args.pivot!r}).")
    q = dec(args.quantity)
    if q is None or not q.is_finite() or q <= 0:
        raise SystemExit(f"{PROG}: --quantity must be a positive number (got {args.quantity!r}).")

    mkt, oracle = read_market_and_oracle(args.network, args.market)
    market_name = mkt.get("marketDisplayName", args.market)
    if oracle is None:
        raise SystemExit(f"{PROG}: [{market_name}] oracle is unusable right now (oraclePrice="
                         f"{mkt.get('oraclePrice')!r}); can't record the starting side. Retry when the oracle is live.")
    step = dec(mkt.get("stepSize"))
    if step is not None and step > 0 and (q % step) != 0:
        raise SystemExit(f"{PROG}: --quantity {q} is not a multiple of the market step size {step}.")
    mn = min_trade_size(mkt)
    if mn > 0 and q < mn:
        raise SystemExit(f"{PROG}: --quantity {q} is below the market minimum order size {mn}.")

    starting_side = side_of(oracle, pivot)
    now = _now_iso()
    state = {"pivot": str(pivot), "quantity": str(q), "starting_side": starting_side,
             "position": "0", "pending_order_id": None, "created_at": now, "updated_at": now}
    save_state(path, state)
    log(f"[INIT {market_name}] pivot={pivot} q={q}  oracle={oracle} -> starting side {starting_side}; "
        f"position 0. Idle until the oracle crosses the pivot. State: {os.path.basename(path)}")


# ── normal mode: one cron tick ───────────────────────────────────────────────────
def do_run(args):
    select_network(args.network)
    path = state_path(args.network, args.market)
    lock = acquire_lock(path)                                                     # noqa: F841 (held for lifetime)
    state = load_state(path)
    if state is None:
        raise SystemExit(f"{PROG}: no state file {os.path.basename(path)} -- run with --init first to arm the bot.")
    for k in ("pivot", "quantity", "starting_side", "position"):
        if k not in state:
            raise SystemExit(f"{PROG}: state file {os.path.basename(path)} is missing '{k}'.")
    pivot = dec(state["pivot"]); q = dec(state["quantity"])
    starting_side = state["starting_side"]
    if pivot is None or q is None or dec(state["position"]) is None:
        raise SystemExit(f"{PROG}: state file has a non-numeric pivot/quantity/position.")

    creds = load_creds()
    address, account_index = creds["eth_address"], creds["account_index"]
    signer = Signer.from_private_key_hex(creds["api_private_key"])

    mkt, oracle = read_market_and_oracle(args.network, args.market)
    market_name = mkt.get("marketDisplayName", args.market)
    if oracle is None:
        log(f"[{market_name}] no usable oracle (oraclePrice={mkt.get('oraclePrice')!r}); skipping this run")
        return
    side = side_of(oracle, pivot)

    # Fold in any unaccounted prior order first, then act on the current side.
    settle_pending(address, account_index, state, path, tag=f"[{market_name}] ")
    position = dec(state["position"])

    if position == 0 and side == starting_side:
        log(f"[{market_name}] oracle={oracle} pivot={pivot} -> {side} (== starting side); waiting for a flip, no trade")
        return
    target = q if side == "bullish" else -q
    delta = target - position
    ms = min_trade_size(mkt)
    if delta == 0 or abs(delta) < ms:
        log(f"[{market_name}] oracle={oracle} -> {side}; position {position} at/near target {target} "
            f"(delta {delta} < min {ms}); no trade")
        return
    execute_trade(signer, address, account_index, mkt, "BUY" if delta > 0 else "SELL", abs(delta), state, path)


def main():
    p = argparse.ArgumentParser(description="Pivot stop-and-reverse bot for one Arcus market (cron-driven).")
    p.add_argument("--market", required=True, help="market display name, e.g. BTC-USD")
    p.add_argument("--init", action="store_true",
                   help="create/re-arm the state file: locks --pivot and --quantity and records the starting "
                        "side. Refuses if the state file exists and the bot is in a position (rm it to roll).")
    p.add_argument("--pivot", help="pivot price (required with --init; ignored otherwise)")
    p.add_argument("--quantity", help="position size q in base-asset units (required with --init; ignored otherwise)")
    add_network_args(p)
    args = p.parse_args()

    if args.init:
        if not args.pivot or not args.quantity:
            raise SystemExit(f"{PROG}: --init requires --pivot and --quantity.")
        do_init(args)
    else:
        if args.pivot or args.quantity:
            print(f"{PROG}: note -- --pivot/--quantity are ignored in normal mode (read from the state file).",
                  file=sys.stderr)
        do_run(args)


if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except Exception as e:                       # a cron bot must exit CLEANLY (nonzero) with a readable line,
        raise SystemExit(f"{PROG}: unexpected error: {describe_error(e)}")   # not a raw traceback, so logs stay legible
