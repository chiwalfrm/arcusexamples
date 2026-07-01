"""
market_maker.py -- simple two-sided POST-ONLY quoter loop for Arcus testnet.

  python3 market_maker.py 500 0.03                      # BTC-USD, $500/side, +/-3%
  python3 market_maker.py 500 0.03 --market ETH-USD --interval 15
  python3 market_maker.py 500 0.03 --max-position 0.05 --min-collateral 1000

Each cycle (every --interval seconds, default 15):
  sort book; mid = (best bid + best ask) / 2
  bid = mid*(1-spread) rounded DOWN to tick; ask = mid*(1+spread) rounded UP
  qty = usd / price rounded DOWN to step, per side
Quotes are placed POST-ONLY (ALO) so they can never take liquidity, with a local
passive check as a backstop. Stable clientIds (mm-<market>-b/-a); each cycle it
MODIFIES the live quote, PLACES it if missing, or CANCELS it if a guard disables
that side. Cancels its quotes on exit.

Risk guards (optional):
  --max-position N    stop quoting the side that would grow |position| past N (base units).
                      ALSO enables INVENTORY SKEW: once |position| >= 50% of N, the REDUCING side
                      quotes at 2x <usd> (the growing side stays <usd>) to mean-revert toward flat
                      faster -- so with --max-position set, ONE side can rest up to 2*<usd> notional
                      (still bounded by N).
  --min-collateral C  stop quoting (and pull quotes) when freeCollateral < C (USD)

Persistent in-process loop: creds, signer, and market metadata are loaded ONCE.
Resolves ordersign.py / arcus_creds_<network>.json relative to this script.
"""

import argparse
import json
import os
import random
import signal
import sys
import time
import urllib.parse
from decimal import Decimal, InvalidOperation, ROUND_CEILING, ROUND_FLOOR

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _HERE)

import account_cache
import ordersign
from ordersign import Signer
from arcus_common import (add_network_args, check_order_response, describe_error, load_creds,
                          positive_decimal, request, resolve_market, select_network)

QUOTE_TIF = "ALO"                       # post-only: a quote can never take liquidity
FAR_FUTURE_US = lambda: str(int(time.time() * 1_000_000) + 365 * 86_400 * 1_000_000)
# Inventory skew: once |position| exceeds SKEW_THRESHOLD * max_position, quote the REDUCING side at
# SKEW_MULT x size (the growing side stays normal) to mean-revert inventory toward flat faster.
# Active only when --max-position is set (the threshold is a fraction of it).
SKEW_THRESHOLD = Decimal("0.5")
SKEW_MULT = Decimal(2)

RUNNING = True


def _stop(_sig, _frame):
    global RUNNING
    RUNNING = False


def to_inc(value, increment, rounding):
    return (value / increment).to_integral_value(rounding=rounding) * increment


class MarketMaker:
    def __init__(self, args, creds, mkt):
        self.market = mkt["marketDisplayName"]   # canonical (used in l2OrderBook path + cids)
        self.market_id = int(mkt["marketId"])
        self.tick = Decimal(mkt["tickSize"])
        self.step = Decimal(mkt["stepSize"])
        self.usd = args.usd            # already-parsed Decimals (see parse_args)
        self.spread = args.spread
        self.max_position = args.max_position
        self.min_collateral = args.min_collateral
        self.address = creds["eth_address"]
        self.account_index = creds["account_index"]
        self.signer = Signer.from_private_key_hex(creds["api_private_key"])
        self.query = urllib.parse.urlencode({"address": self.address})
        self.bid_cid = f"mm-{self.market}-b"
        self.ask_cid = f"mm-{self.market}-a"
        self.net = args.network                  # for the account_cache key namespace
        self.cache_ttl = args.cache_ttl
        self.cache_enabled = not args.no_cache
        # last (price, qty) we believe is RESTING per side -> skip a modify when nothing changed
        self.last_quote = {self.bid_cid: None, self.ask_cid: None}

    def _cached(self, name, address, fetch_fn):
        """Account-wide reads go through the short-TTL Redis cache (shared across the fleet) so
        N market bots don't each re-fetch the same data every loop. --no-cache fetches live."""
        if not self.cache_enabled:
            return fetch_fn()
        return account_cache.cached_get(self.net, address, name, fetch_fn, self.cache_ttl)

    # ── HTTP (raises on error; the loop classifies it and survives) ──────────
    def call(self, method, path, body=None, headers=None):
        return request(method, path, body, headers)

    # ── Order ops (POST-ONLY / ALO) ──────────────────────────────────────────
    def place_quote(self, order_side, sside, cid, price, qty):
        ct = time.time_ns()
        gtt = FAR_FUTURE_US()
        headers = self.signer.sign_place_order(
            address=self.address, account_index=self.account_index, client_id=cid,
            client_timestamp_ns=ct, good_til_time_ns_=ordersign.good_til_time_ns(gtt),
            market_id=self.market_id, price_ticks=ordersign.price_to_ticks(f"{price:f}", self.tick),
            quantity_quantums=ordersign.size_to_quantums(f"{qty:f}", self.step),
            side=sside, time_in_force=ordersign.TIF_ALO)
        body = {"address": self.address, "accountIndex": self.account_index, "marketId": self.market_id,
                "orderSide": order_side, "orderType": "LIMIT", "quantity": f"{qty:f}",
                "price": f"{price:f}", "timeInForce": QUOTE_TIF, "goodTilTime": gtt,
                "timestamp": ct, "clientId": cid}
        check_order_response(self.call("POST", f"/v1/placeOrder?{self.query}", body, headers), "placeOrder")

    def modify_quote(self, order_side, sside, cid, price, qty, order_id):
        # Modify now identifies by orderId and signs the immutable fields (g/r/s/t) +
        # the clientId echo; the replacement carries a fresh far-future goodTilTime.
        gtt = FAR_FUTURE_US()
        headers = self.signer.sign_modify_order(
            address=self.address, account_index=self.account_index, market_id=self.market_id,
            price_ticks=ordersign.price_to_ticks(f"{price:f}", self.tick),
            quantity_quantums=ordersign.size_to_quantums(f"{qty:f}", self.step),
            good_til_time_ns_=ordersign.good_til_time_ns(gtt),
            reduce_only=False, side=sside, time_in_force=ordersign.TIF_ALO,
            order_id=order_id, client_id=cid)
        body = {"address": self.address, "accountIndex": self.account_index, "marketId": self.market_id,
                "orderId": order_id, "clientId": cid, "side": order_side, "timeInForce": QUOTE_TIF,
                "price": f"{price:f}", "quantity": f"{qty:f}", "reduceOnly": False, "goodTilTime": gtt}
        check_order_response(self.call("POST", f"/v1/modifyOrder?{self.query}", body, headers), "modifyOrder")

    def cancel_quote(self, cid):
        headers = self.signer.sign_cancel_order(address=self.address, account_index=self.account_index,
                                                market_id=self.market_id, client_id=cid)
        body = {"address": self.address, "accountIndex": self.account_index, "marketId": self.market_id,
                "kind": "clientId", "clientId": cid}
        check_order_response(self.call("POST", f"/v1/cancelOrder?{self.query}", body, headers), "cancelOrder")

    def pull_quotes(self):
        """Cancel any of THIS bot's resting quotes (both sides). Called when fresh pricing
        can't be established for a cycle, so stale 365-day GTT orders don't keep resting at
        old prices -- ALO/POST-ONLY stops us TAKING, but a resting quote can still be picked
        off as the market moves away from it. Best-effort: a failed read/cancel is logged and
        last_quote is cleared so the side re-places cleanly once data returns. (Confirmed-clean
        cancellation on EXIT remains shutdown()'s job.)"""
        try:
            live = self.live_quotes(fresh=True)   # uncached: a stale cache must not hide a resting GTT quote
        except (OSError, json.JSONDecodeError) as e:
            print(f"  could not read open orders to pull quotes: {describe_error(e)}")
            for cid in (self.bid_cid, self.ask_cid):
                self.last_quote[cid] = None
            return
        for cid in (self.bid_cid, self.ask_cid):
            if cid in live:
                try:
                    self.cancel_quote(cid)
                    print(f"  pulled {cid}")
                except (OSError, json.JSONDecodeError, SystemExit) as e:   # SystemExit = 2xx REJECTED/ERROR body
                    print(f"  pull {cid} failed: {describe_error(e)}")
            self.last_quote[cid] = None

    # ── State reads ───────────────────────────────────────────────────────────
    def live_quotes(self, fresh=False):
        """Map our resting clientId -> orderId IN THIS MARKET (scoped). modify needs
        the orderId, and the orderId is preserved across modifies.

        fresh=True bypasses the Redis account cache for an uncached /v1/openOrders read
        -- used by the fail-closed pull path (pull_quotes), where a stale or poller-warmed
        cache could hide a currently-resting quote and leave a 365-day GTT order live, the
        same reason shutdown() confirms cancellation against a fresh read."""
        fetch = lambda: self.call("GET", f"/v1/openOrders?{self.query}")
        src = fetch() if fresh else self._cached("openOrders", self.address, fetch)
        orders = src.get("orders", [])
        live = {}
        for o in orders:
            cid = o.get("clientId")
            if cid in (self.bid_cid, self.ask_cid) and str(o.get("marketId")) == str(self.market_id):
                live[cid] = o.get("orderId")
        return live

    def position(self):
        """Signed position size for this market.

        Decimal(0) when genuinely flat (no position), the signed size when
        parseable, or None when a position EXISTS but its size is unparseable --
        so the risk guard can fail CLOSED on an unknown position (not treat it as flat).
        """
        positions = self._cached("positions", self.address,
                                 lambda: self.call("GET", f"/v1/positions?{self.query}")).get("positions") or {}
        p = positions.get(str(self.market_id))
        if not p:
            return Decimal(0)
        try:
            return Decimal(str(p.get("size")))
        except (InvalidOperation, TypeError):
            return None

    def free_collateral(self):
        acct = self._cached("account", self.address, lambda: self.call("GET", f"/v1/account?{self.query}"))
        try:
            return Decimal(str(acct.get("freeCollateral")))
        except (InvalidOperation, TypeError):
            return None

    def oracle_price(self):
        """Live oracle price for this market (Decimal > 0), or None if unavailable.
        The quoting reference when the book isn't two-sided. One extra /v1/markets
        read, only taken on the fallback path."""
        markets = self._cached("markets", None, lambda: self.call("GET", "/v1/markets")).get("markets", [])
        for m in markets:
            if str(m.get("marketId")) == str(self.market_id):
                try:
                    v = Decimal(str(m.get("oraclePrice")))
                except (InvalidOperation, TypeError):
                    return None
                return v if v > 0 else None
        return None

    def quote_prices(self, mid):
        bid = to_inc(mid * (1 - self.spread), self.tick, ROUND_FLOOR)
        ask = to_inc(mid * (1 + self.spread), self.tick, ROUND_CEILING)
        return bid, ask

    def preflight_max_position(self):
        """Warn at startup if --max-position is smaller than a single quote. The inventory guard
        is fail-closed and size-aware, so if one quote already exceeds the cap it pulls BOTH sides
        every cycle and NOTHING rests (a low-priced market like DYDX-USD makes this easy to hit,
        since max-position is in BASE UNITS, not USD). Estimates the size from the oracle price."""
        if self.max_position is None:
            return
        ref = self.oracle_price()
        if ref is None or ref <= 0:
            return                                       # can't estimate; the cycle notes will show it
        bid_px = to_inc(ref * (1 - self.spread), self.tick, ROUND_FLOOR)
        ask_px = to_inc(ref * (1 + self.spread), self.tick, ROUND_CEILING)
        if bid_px <= 0 or ask_px <= 0:
            return
        worst = max(to_inc(self.usd / bid_px, self.step, ROUND_FLOOR),
                    to_inc(self.usd / ask_px, self.step, ROUND_FLOOR))
        if worst > self.max_position:
            print(f"WARNING: --max-position {self.max_position} (BASE UNITS, not USD) is smaller than a "
                  f"single ~{worst:f}-unit quote (${self.usd}/side at ~{ref:f}). The inventory guard will "
                  f"pull BOTH sides every cycle and NO orders will rest -- raise --max-position above "
                  f"{worst:f}, or lower --usd.")

    # ── One cycle ─────────────────────────────────────────────────────────────
    def cycle(self):
        try:
            ob = self.call("GET", f"/v1/l2OrderBook/{urllib.parse.quote(self.market)}")
        except (OSError, json.JSONDecodeError) as e:
            # Can't establish fresh pricing this cycle -> don't leave stale 365-day GTT quotes
            # resting at old prices; pull them and re-quote once the book is readable again.
            print(f"[{time.strftime('%H:%M:%S')}] l2OrderBook unavailable ({describe_error(e)}); pulling quotes")
            self.pull_quotes()
            return
        bids, asks = ob.get("bids", []), ob.get("asks", [])
        # Don't trust server ordering: best bid = highest, best ask = lowest.
        asks = sorted(asks, key=lambda lv: Decimal(lv[0]))
        bids = sorted(bids, key=lambda lv: Decimal(lv[0]), reverse=True)
        best_bid = Decimal(bids[0][0]) if bids else None
        best_ask = Decimal(asks[0][0]) if asks else None
        # Reference price: book mid when two-sided, else fall back to the oracle so we
        # can still quote (and bootstrap liquidity) on a one-sided / empty book.
        if best_bid is not None and best_ask is not None:
            mid, ref = (best_bid + best_ask) / 2, "book"
        else:
            try:
                mid = self.oracle_price()      # reads /v1/markets; can raise on transport/JSON error
            except (OSError, json.JSONDecodeError) as e:
                # Oracle fallback read failed -> no fresh reference; pull quotes rather than let the
                # raise bubble to run() (log-only) and leave stale 365-day GTT quotes resting.
                print(f"[{time.strftime('%H:%M:%S')}] oracle read failed ({describe_error(e)}); pulling quotes")
                self.pull_quotes()
                return
            if mid is None:
                # No reference price -> pull quotes rather than leave them resting at stale prices.
                print(f"[{time.strftime('%H:%M:%S')}] no two-sided book and no usable oracle; pulling quotes")
                self.pull_quotes()
                return
            ref = "oracle"
        bid_px, ask_px = self.quote_prices(mid)

        want_bid, want_ask, notes = True, True, []

        # Read the position ONCE -- used for both inventory-skew sizing and the guard below.
        pos = self.position() if self.max_position is not None else None

        # Inventory skew: past SKEW_THRESHOLD * max_position, quote the REDUCING side SKEW_MULT x
        # larger (growing side stays normal) so inventory mean-reverts toward flat faster.
        bid_usd = ask_usd = self.usd
        if self.max_position is not None and pos is not None:
            skew_at = SKEW_THRESHOLD * self.max_position
            if pos >= skew_at:                         # long at/beyond threshold -> grow the SELL side
                ask_usd = self.usd * SKEW_MULT; notes.append(f"skew-ask-{SKEW_MULT}x")
            elif pos <= -skew_at:                      # short at/beyond threshold -> grow the BUY side
                bid_usd = self.usd * SKEW_MULT; notes.append(f"skew-bid-{SKEW_MULT}x")
        bid_qty = to_inc(bid_usd / bid_px, self.step, ROUND_FLOOR)
        ask_qty = to_inc(ask_usd / ask_px, self.step, ROUND_FLOOR)

        # Inventory guard (fail-closed): account for the PENDING quote size -- a
        # bid fill takes position to pos+bid_qty, an ask fill to pos-ask_qty -- so
        # don't quote a side that could breach +/-max. Unknown position pulls both.
        if self.max_position is not None:
            if pos is None:
                want_bid = want_ask = False; notes.append("position-unknown")
            else:
                if pos + bid_qty > self.max_position:
                    want_bid = False; notes.append(f"max-long(pos={pos}+{bid_qty})")
                if pos - ask_qty < -self.max_position:
                    want_ask = False; notes.append(f"max-short(pos={pos}-{ask_qty})")

        # Collateral guard (fail-closed): pull both quotes when free collateral is
        # low OR unknown (missing/unparseable).
        if self.min_collateral is not None:
            fc = self.free_collateral()
            if fc is None:
                want_bid = want_ask = False; notes.append("collateral-unknown")
            elif fc < self.min_collateral:
                want_bid = want_ask = False; notes.append(f"low-collateral({fc})")

        # Passive backstop (ALO already enforces this server-side): never quote
        # at/through the opposite top-of-book. Only check a side that EXISTS -- on
        # the oracle-fallback path one side of the book may be empty.
        if best_ask is not None and bid_px >= best_ask:
            want_bid = False; notes.append("bid-not-passive")
        if best_bid is not None and ask_px <= best_bid:
            want_ask = False; notes.append("ask-not-passive")
        if bid_qty <= 0:
            want_bid = False
        if ask_qty <= 0:
            want_ask = False

        live = self.live_quotes()
        actions = []
        for cid, oside, sside, px, qty, want in (
            (self.bid_cid, "BUY", ordersign.SIDE_BUY, bid_px, bid_qty, want_bid),
            (self.ask_cid, "SELL", ordersign.SIDE_SELL, ask_px, ask_qty, want_ask),
        ):
            try:
                if want and cid in live:
                    if self.last_quote.get(cid) == (px, qty):
                        actions.append(f"{oside}:keep")        # unchanged vs resting -> no modify (saves an order-pool unit + a REST call)
                    else:
                        self.modify_quote(oside, sside, cid, px, qty, live[cid])
                        self.last_quote[cid] = (px, qty)
                        actions.append(f"{oside}:modify {qty:f}@{px:f}")
                elif want:
                    self.place_quote(oside, sside, cid, px, qty)
                    self.last_quote[cid] = (px, qty)
                    actions.append(f"{oside}:place {qty:f}@{px:f}")
                elif cid in live:
                    self.cancel_quote(cid)
                    self.last_quote[cid] = None
                    actions.append(f"{oside}:cancel(guard)")
                else:
                    self.last_quote[cid] = None
                    actions.append(f"{oside}:skip")
            except (OSError, json.JSONDecodeError, SystemExit) as e:
                # OSError/JSON = transport/HTTP-error (incl. 4xx rejects); SystemExit = check_order_response
                # flagged a 2xx body with status REJECTED/ERROR. Either way this side FAILED this cycle:
                # clear last_quote (uncertain state -> re-place/modify next cycle) and log it -- NOT a
                # success, and never fatal (e.g. a transient POST_ONLY_WOULD_CROSS just re-quotes next loop).
                self.last_quote[cid] = None
                actions.append(f"{oside}:ERR {describe_error(e)}")
        tail = ("  [" + " ".join(notes) + "]") if notes else ""
        print(f"[{time.strftime('%H:%M:%S')}] mid={mid:.4f}({ref})  " + "  ".join(actions) + tail)

    def run(self, interval, cycles):
        n = 0
        while RUNNING:
            n += 1
            try:
                self.cycle()
            except (OSError, json.JSONDecodeError) as e:
                print(f"[{time.strftime('%H:%M:%S')}] cycle error: {describe_error(e)}")
            except Exception as e:
                print(f"[{time.strftime('%H:%M:%S')}] cycle error: {e}")
            if cycles and n >= cycles:
                break
            slept = 0.0
            while RUNNING and slept < interval:
                time.sleep(min(0.5, interval - slept))
                slept += 0.5

    def shutdown(self, retries=3):
        """Cancel both quotes and CONFIRM they're gone via a FRESH (uncached) openOrders read,
        retrying any that still rest. Arcus quotes are GTT -- they do NOT self-expire -- so a
        silently-failed cancel can leave an order resting indefinitely; fail closed (nonzero exit)
        if cancellation can't be confirmed, so the operator knows to clean up manually."""
        print("shutting down -- canceling quotes...")
        remaining = None                                    # None = unknown; else list of resting cids
        for attempt in range(1, retries + 1):
            targets = remaining if remaining is not None else [self.bid_cid, self.ask_cid]
            for cid in targets:
                try:
                    self.cancel_quote(cid)
                except (OSError, json.JSONDecodeError, SystemExit) as e:   # SystemExit = 2xx REJECTED/ERROR body
                    print(f"  {cid}: cancel error: {describe_error(e)}")    # the fresh openOrders read below is the source of truth
            # Confirm via a FRESH read (NOT the cache -- it could be stale and falsely show gone).
            try:
                orders = self.call("GET", f"/v1/openOrders?{self.query}").get("orders", [])
                remaining = [o.get("clientId") for o in orders
                             if o.get("clientId") in (self.bid_cid, self.ask_cid)
                             and str(o.get("marketId")) == str(self.market_id)]
            except (OSError, json.JSONDecodeError) as e:
                print(f"  could not confirm cancellation (openOrders read failed: {describe_error(e)})")
                remaining = None
                continue
            if not remaining:
                print("  all quotes canceled (confirmed gone).")
                return
            print(f"  still resting: {remaining}; retrying ({attempt}/{retries})")
        # Exhausted retries without confirming a clean cancel -> FAIL CLOSED.
        if remaining:
            raise SystemExit(f"market_maker: WARNING could not confirm cancellation of {remaining} -- these "
                             f"GTT orders may STILL BE RESTING. Cancel manually: "
                             f"cancel_order.py --clientid <id> --{self.net}")
        raise SystemExit("market_maker: could not confirm cancellation (openOrders unreadable); "
                         "verify open orders manually.")


# ── CLI ────────────────────────────────────────────────────────────────────────
def parse_args():
    p = argparse.ArgumentParser(description="Two-sided post-only market-maker loop for Arcus testnet.")
    p.add_argument("usd", help="USD to quote per side (> 0)")
    p.add_argument("spread", help="half-spread off mid, fraction in [0, 1) (e.g. 0.03 = 3%%)")
    p.add_argument("--market", default="BTC-USD", help="market display name (default BTC-USD)")
    p.add_argument("--interval", type=float, default=15, help="refresh seconds (> 0, default 15)")
    p.add_argument("--cycles", type=int, default=0, help="stop after N cycles (>= 0; 0 = forever)")
    p.add_argument("--max-position", help="cap |position| in base units; stop growing past it. ALSO enables "
                                          "inventory-skew: reducing side quotes 2x usd once |pos| >= 50%% of this")
    p.add_argument("--min-collateral", help="pull quotes when freeCollateral < this (USD)")
    p.add_argument("--cache-ttl", type=int, default=5,
                   help="Redis TTL (s) for shared account-wide reads (openOrders/positions/account/markets); "
                        "default 5. Must be < --interval. See account_cache.py / account_poller.py")
    p.add_argument("--no-cache", action="store_true",
                   help="bypass the Redis account cache; fetch every account-wide read live")
    add_network_args(p)
    a = p.parse_args()

    a.usd = positive_decimal(a.usd, "usd")
    a.spread = positive_decimal(a.spread, "spread", allow_zero=True)
    if a.spread >= 1:
        raise SystemExit("spread: must be < 1 (a fraction, e.g. 0.03 for 3%).")
    if a.interval <= 0:
        raise SystemExit("--interval: must be > 0.")
    if a.cycles < 0:
        raise SystemExit("--cycles: must be >= 0.")
    if a.cache_ttl < 1:
        raise SystemExit("--cache-ttl: must be >= 1.")
    if a.cache_ttl >= a.interval:
        print(f"WARNING: --cache-ttl {a.cache_ttl}s >= --interval {a.interval}s; the cache may not refresh "
              f"each cycle (and a bot may not see its own just-placed orders). Use a TTL below the interval.")
    a.max_position = positive_decimal(a.max_position, "--max-position") if a.max_position is not None else None
    a.min_collateral = positive_decimal(a.min_collateral, "--min-collateral", allow_zero=True) if a.min_collateral is not None else None
    return a


def fetch_startup_markets(args, attempts=6):
    """Startup /v1/markets with retry + JITTERED backoff. A cold-cache mass launch can make 35
    bots hit the heavy /v1/markets at once and some time out; rather than die on the spot (the
    old call()->SystemExit), retry so the bot rides out the burst -- and likely hits a now-warm
    cache on the next try. Uses request() (raises, so failures are catchable) and serves a warm
    cache instantly. Jitter is important: without it 35 bots would retry in lockstep and re-herd."""
    def fetch():
        return request("GET", "/v1/markets")
    for i in range(1, attempts + 1):
        try:
            if args.no_cache:
                return fetch()
            return account_cache.cached_get(args.network, None, "markets", fetch, args.cache_ttl)
        except (OSError, json.JSONDecodeError) as e:    # URLError/timeouts are OSError subclasses
            if i == attempts:
                raise SystemExit(f"startup /v1/markets failed after {attempts} attempts: {describe_error(e)}")
            delay = min(2 ** (i - 1), 30) * random.uniform(0.5, 1.5)
            print(f"startup /v1/markets {describe_error(e)}; retry {i}/{attempts - 1} in {delay:.1f}s", flush=True)
            time.sleep(delay)


def main():
    args = parse_args()
    select_network(args.network)
    creds = load_creds()
    # Startup market resolution goes through the shared markets cache (poller-warmed, or the
    # first bot fetches and the rest hit it) with retry+backoff, so a mass launch neither fires
    # 35x /v1/markets nor dies when the burst times out.
    markets = fetch_startup_markets(args).get("markets", [])
    mkt = resolve_market(markets, args.market)
    if mkt is None:
        raise SystemExit(f"Unknown market {args.market!r}.")

    mm = MarketMaker(args, creds, mkt)
    print(f"market-maker: {mm.market} (id {mm.market_id})  ${mm.usd}/side  spread +/-{mm.spread * 100:.2f}%  "
          f"every {args.interval}s  tick={mm.tick} step={mm.step}  TIF={QUOTE_TIF}"
          + (f"  max-pos={mm.max_position} (skew {SKEW_MULT}x reducing side at |pos|>={SKEW_THRESHOLD * mm.max_position})"
             if mm.max_position is not None else "")
          + (f"  min-collat={mm.min_collateral}" if mm.min_collateral is not None else ""))

    mm.preflight_max_position()

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)
    mm.run(args.interval, args.cycles)
    mm.shutdown()


if __name__ == "__main__":
    main()
