"""
Place an order on Arcus testnet (v5 typed-payload signing).

LIMIT (a --price is given):
  python3 place_order.py --market BTC-USD --quantity 0.1 --price 50000
  python3 place_order.py --market ETH-USD --side SELL --quantity 1 --price 4000 --tif GTT

MARKET (no --price): orderType=MARKET, --tif defaults to IOC (override with --tif):
  python3 place_order.py --market BTC-USD --quantity 0.5
  python3 place_order.py --market BTC-USD --side SELL --quantity 0.5 --force

Sizing -- give exactly one of --quantity / --quantityusd:
  --quantity      order size in base-asset units (e.g. 0.5 BTC).
  --quantityusd   spend this many USD; the coin quantity is derived -- from --price
                  for a limit order, or by walking the book to the budget for a
                  market order -- then rounded DOWN to the market step size.

Market-order slippage guard:
  Pulls GET /v1/l2OrderBook/<market>, sorts the book, computes mid (best bid/ask),
  and walks it (asks for BUY, bids for SELL) to estimate the average fill price for
  --quantity. If that average deviates more than MAX_SLIPPAGE (3%) from mid, the
  order is NOT placed unless --force (market only). The protective price bound is
  the worst consumed level +/-PRICE_BUFFER (normal) or mark +/-FORCE_MARK_BOUND
  (--force), tick-aligned.

Signing (see ordersign.py): placeOrder is signed over the TYPED canonical payload.
This is the only step that uses the Ed25519 API PRIVATE key.

Resolves ordersign.py and arcus_creds.json relative to this script (the ~/info
symlink dir), so it works from any working directory.
"""

import argparse
import json
import os
import sys
import time
import urllib.parse
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR

# Resolve ordersign / arcus_common / arcus_creds.json relative to THIS script
# (the ~/info symlink dir), so it works from any cwd and can't import a stray module.
_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _HERE)

import ordersign
from ordersign import Signer
from arcus_common import (add_network_args, call, check_order_response, clock_delta_ns, dec,
                          describe_error, load_creds, positive_decimal, resolve_market,
                          select_network, to_quantums, to_ticks, validate_client_id)

MAX_SLIPPAGE = Decimal("0.03")          # 3% of mid, for market orders
PRICE_BUFFER = Decimal("0.01")          # normal: pad the bound +/-1% past the worst level
FORCE_MARK_BOUND = Decimal("0.09")      # --force: bound = mark +/-9% (under the API's 10% cap)

SIDES = {"BUY": ordersign.SIDE_BUY, "SELL": ordersign.SIDE_SELL}
TIFS = {"GTT": ordersign.TIF_GTC, "FOK": ordersign.TIF_FOK,
        "IOC": ordersign.TIF_IOC, "ALO": ordersign.TIF_ALO}

# The venue now REQUIRES a goodTilTime >= 1 month in the future on EVERY order
# (incl. IOC/FOK/market -- IOC/market won't actually rest, but the field is
# validated). 365 days clears the minimum with a wide margin.
GOOD_TIL_DAYS = 365


def clock_delta():
    """Server-minus-local clock offset (ns) from /v1/time; 0 (use local clock) if
    it's unavailable -- a /v1/time hiccup shouldn't block an order, and the 365-day
    expiry clears the 1-month minimum regardless of small drift."""
    try:
        return clock_delta_ns()
    except Exception as e:
        print(f"warning: /v1/time unavailable ({describe_error(e)}); using local clock.",
              file=sys.stderr)
        return 0


# ── place_order-specific helpers ──────────────────────────────────────────────
def round_to_increment(value, increment, rounding):
    inc = Decimal(increment)
    return (value / inc).to_integral_value(rounding=rounding) * inc


def to_step(qty, step_size):
    """Round a base-asset quantity DOWN to a valid step multiple (never overspends)."""
    q = round_to_increment(qty, step_size, ROUND_FLOOR)
    if q <= 0:
        raise SystemExit(f"computed quantity rounds to 0 at step {step_size}; increase --quantityusd.")
    return q


# ── Order-book walking ────────────────────────────────────────────────────────
def walk_book(levels, qty):
    """Walk pre-sorted levels filling up to qty -> (avg, worst, filled, enough)."""
    remaining, cost, filled, worst = qty, Decimal(0), Decimal(0), None
    for price_s, size_s in levels:
        if remaining <= 0:
            break
        price, size = Decimal(price_s), Decimal(size_s)
        take = size if size < remaining else remaining
        cost += price * take
        filled += take
        remaining -= take
        worst = price_s
    avg = (cost / filled) if filled > 0 else None
    return avg, worst, filled, remaining <= 0


def walk_book_usd(levels, budget):
    """Walk pre-sorted levels by USD budget -> (filled_qty, avg, worst, enough)."""
    spent, filled, worst = Decimal(0), Decimal(0), None
    for price_s, size_s in levels:
        remaining = budget - spent
        if remaining <= 0:
            break
        price, size = Decimal(price_s), Decimal(size_s)
        level_cost = price * size
        if level_cost <= remaining:
            spent += level_cost
            filled += size
        else:
            filled += remaining / price
            spent = budget
        worst = price_s
    avg = (spent / filled) if filled > 0 else None
    return filled, avg, worst, spent >= budget


# ── CLI ────────────────────────────────────────────────────────────────────────
def parse_args():
    parser = argparse.ArgumentParser(description="Place an order on Arcus testnet.")
    parser.add_argument("--market", default="BTC-USD", help="market display name, e.g. BTC-USD")
    parser.add_argument("--side", default="BUY", choices=list(SIDES))
    qty_group = parser.add_mutually_exclusive_group(required=True)
    qty_group.add_argument("--quantity", help="order size in base-asset units (decimal > 0)")
    qty_group.add_argument("--quantityusd", help="order size in USD (decimal > 0); rounded DOWN to step")
    parser.add_argument("--price", default=None, help="limit price (decimal > 0). Omit for a MARKET order.")
    parser.add_argument("--tif", default=None, choices=list(TIFS),
                        help="time-in-force (default GTT for limit, IOC for market)")
    parser.add_argument("--reduce-only", action="store_true", help="reduce-only order")
    parser.add_argument("--clientid", "--client-id", dest="client_id",
                        help="client-assigned order id (1-36 chars, [A-Za-z0-9_-])")
    parser.add_argument("--force", action="store_true",
                        help="market orders only: place even if slippage exceeds 3%%")
    add_network_args(parser)
    return parser.parse_args()


def main():
    args = parse_args()
    select_network(args.network)
    is_market = args.price is None
    tif = args.tif or ("IOC" if is_market else "GTT")
    # Server rule: MARKET orders must be IOC (no resting/FOK market orders).
    if is_market and tif != "IOC":
        raise SystemExit(f"--tif {tif}: a MARKET order (no --price) must be IOC.")
    # Server rule: a reduce-only order must be IOC or FOK -- a resting GTT/ALO reduce-only is
    # rejected at validation ("must be IOC or FOK when reduce_only is true", verified live 2026-06-28).
    if args.reduce_only and tif not in ("IOC", "FOK"):
        raise SystemExit(f"--tif {tif}: a reduce-only order must be IOC or FOK "
                         f"(the venue rejects a resting reduce-only GTT/ALO).")

    # --- Validate all inputs locally, before any signing/sending --------------
    if args.price is not None:
        positive_decimal(args.price, "--price")
    if args.quantity is not None:
        positive_decimal(args.quantity, "--quantity")
    if args.quantityusd is not None:
        positive_decimal(args.quantityusd, "--quantityusd")
    if args.client_id is not None:
        validate_client_id(args.client_id)
    if args.force and not is_market:
        print("note: --force is ignored for limit orders (slippage guard is market-only).")

    creds = load_creds()
    address = creds["eth_address"]
    account_index = creds["account_index"]
    signer = Signer.from_private_key_hex(creds["api_private_key"])

    # --- Resolve market (by id or case-insensitive name) -> canonical name ----
    markets = call("GET", "/v1/markets")["markets"]
    mkt = resolve_market(markets, args.market)
    if mkt is None:
        raise SystemExit(f"Unknown market {args.market!r}.")
    market_id = int(mkt["marketId"])
    tick_size, step_size = mkt["tickSize"], mkt["stepSize"]
    args.market = mkt["marketDisplayName"]   # canonicalize (l2OrderBook path needs it)
    usd_mode = args.quantityusd is not None

    # --- Determine the order's quantity and price -----------------------------
    if is_market:
        ob = call("GET", f"/v1/l2OrderBook/{urllib.parse.quote(args.market)}")
        bids, asks = ob.get("bids", []), ob.get("asks", [])
        if not bids or not asks:
            raise SystemExit(f"{args.market}: order book has no two-sided liquidity.")
        # Sort defensively (don't trust the server's ordering): asks ascending,
        # bids descending -> best bid/ask are index 0; walk in consume order.
        asks = sorted(asks, key=lambda lv: Decimal(lv[0]))
        bids = sorted(bids, key=lambda lv: Decimal(lv[0]), reverse=True)
        mid = (Decimal(bids[0][0]) + Decimal(asks[0][0])) / 2
        levels = asks if args.side == "BUY" else bids

        if usd_mode:
            budget = Decimal(args.quantityusd)
            raw_qty, _, _, budget_ok = walk_book_usd(levels, budget)
            qty = to_step(raw_qty, step_size)
            print(f"USD {budget} -> quantity {qty:f} {args.market} (rounded to step {step_size})")
            if not budget_ok:
                print(f"  WARNING: book can't absorb the full ${budget}; sized to available liquidity.")
        else:
            qty = Decimal(args.quantity)

        avg_fill, worst_price, filled, enough = walk_book(levels, qty)
        if avg_fill is None:
            raise SystemExit(f"{args.market}: empty book on the {args.side} side.")
        slippage = abs(avg_fill - mid) / mid
        print(f"MARKET {args.side} {qty:f} {args.market}: mid={mid:.6f}  "
              f"est avg fill={avg_fill:.6f}  slippage={slippage * 100:.2f}%  worst level={worst_price}")
        if not enough:
            print(f"  WARNING: book only covers {filled} of {qty}; an IOC order will partially fill.")
        if slippage > MAX_SLIPPAGE and not args.force:
            raise SystemExit(
                f"  Slippage {slippage * 100:.2f}% exceeds {MAX_SLIPPAGE * 100:.0f}% limit "
                f"-- not placing. Re-run with --force to override.")
        if slippage > MAX_SLIPPAGE:
            print(f"  --force: placing despite {slippage * 100:.2f}% slippage.")

        if args.force:
            mark = dec(mkt.get("markPrice"))
            if mark is None or mark <= 0:
                mark = mid          # fall back to mid if markPrice missing/zero
                print("  note: markPrice unavailable/zero; using mid for the --force bound.")
            target = mark * (1 + FORCE_MARK_BOUND) if args.side == "BUY" else mark * (1 - FORCE_MARK_BOUND)
            label = f"mark {mark} +/-{FORCE_MARK_BOUND * 100:.0f}% (--force)"
        else:
            worst = Decimal(worst_price)
            target = worst * (1 + PRICE_BUFFER) if args.side == "BUY" else worst * (1 - PRICE_BUFFER)
            label = f"worst level {worst_price} +/-{PRICE_BUFFER * 100:.0f}%"
        # Round the protective bound AWAY from mid so tick-rounding never makes it TIGHTER than the
        # intended target: BUY (max acceptable price) rounds UP, SELL (min acceptable price) rounds
        # DOWN. (Rounding toward mid could trim the worst consumable level and cause avoidable
        # partial/no fills. The 1%/9% buffers are far wider than one tick, so this can't breach the
        # API's 10%-of-mark cap.)
        bound = round_to_increment(target, tick_size, ROUND_CEILING if args.side == "BUY" else ROUND_FLOOR)
        print(f"  bound={bound:f}  ({label})")

        order_type = "MARKET"
        price = f"{bound:f}"
    else:
        order_type = "LIMIT"
        price = str(args.price)
        if usd_mode:
            qty = to_step(Decimal(args.quantityusd) / Decimal(price), step_size)
            print(f"USD {args.quantityusd} @ {price} -> quantity {qty:f} (rounded to step {step_size})")
        else:
            qty = Decimal(args.quantity)
        print(f"{args.market} (marketId {market_id})  LIMIT {args.side} {qty:f} @ {price}"
              f"  tick={tick_size} step={step_size}")

    qty_str = f"{qty:f}"
    # Exact-multiple conversion (rejects mis-aligned price/qty with a clear error
    # rather than silently submitting something different from what was shown).
    price_ticks = to_ticks(price, tick_size)
    quantity_quantums = to_quantums(qty_str, step_size)

    # Every order needs a goodTilTime >= 1 month out, in SERVER time -- correct the
    # local clock by the /v1/time delta so the expiry (and the signed timestamp) are
    # server-aligned even if this box's clock drifts.
    delta_ns = clock_delta()
    server_us = (time.time_ns() + delta_ns) // 1000
    good_til_us = str(server_us + GOOD_TIL_DAYS * 86_400 * 1_000_000)

    # --- Sign the typed payload, then POST the REST body ----------------------
    ct = time.time_ns() + delta_ns           # server-aligned; also the X-Timestamp
    headers = signer.sign_place_order(
        address=address, account_index=account_index, client_id=args.client_id,
        client_timestamp_ns=ct, good_til_time_ns_=ordersign.good_til_time_ns(good_til_us),
        market_id=market_id, price_ticks=price_ticks, quantity_quantums=quantity_quantums,
        reduce_only=args.reduce_only, side=SIDES[args.side], time_in_force=TIFS[tif],
    )
    body = {
        "address": address, "accountIndex": account_index, "marketId": market_id,
        "orderSide": args.side, "orderType": order_type, "quantity": qty_str,
        "price": price, "timeInForce": tif, "timestamp": ct,
        "goodTilTime": good_til_us,           # REQUIRED on every order now
    }
    if args.reduce_only:
        body["reduceOnly"] = True
    if args.client_id:
        body["clientId"] = args.client_id

    path = "/v1/placeOrder?" + urllib.parse.urlencode({"address": address})
    order = call("POST", path, body, headers)
    print("Order response:", json.dumps(order, indent=2))
    check_order_response(order, "placeOrder")   # a 2xx can still carry status REJECTED/ERROR -> fail closed


if __name__ == "__main__":
    main()
