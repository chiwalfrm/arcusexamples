#!/usr/bin/env python3
"""Flatten arcus position(s) with reduce-only IOC MARKET orders -- a get-out button.

  close_position.py --testnet                         # close ALL open positions
  close_position.py --market BTC-USD --staging         # close just that market
  close_position.py --mainnet --max-slippage 0.03

Reads open positions (GET /v1/positions) and, for each, fires a REDUCE-ONLY IOC MARKET order on the
OPPOSITE side with a protective price bound = mark price +/- --max-slippage (default 0.05 = 5%).
Reduce-only guarantees it can only shrink/flatten, never flip (the venue rejects a reduce-only that
would grow a position: REDUCE_ONLY_WOULD_INCREASE). A thin book may leave a remainder (IOC) -- after
submitting, it RE-QUERIES positions and EXITS NONZERO if any close failed OR a position remains open
(including sub-step dust that can't be traded). Re-run to finish.

--testnet/--staging/--mainnet REQUIRED. Signs via ordersign; creds in
arcus_creds_<network>.json beside this script. (--max-slippage must stay < the venue's 10%-of-mark
market-order cap.)
"""
import argparse
import json
import os
import sys
import time
import urllib.parse
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))   # resolve ordersign/arcus_common beside this file
import ordersign
from ordersign import Signer
from arcus_common import (add_network_args, call, check_order_response, clock_delta_ns,
                          dec, describe_error, load_creds, positive_decimal, resolve_market,
                          select_network, to_quantums, to_ticks)

PROG = "close_position"
GOOD_TIL_DAYS = 365         # venue requires goodTilTime >= 1 month even on IOC; 365d clears it
SETTLE_SECONDS = 2          # let IOC fills settle before the residual re-query
SIDES = {"BUY": ordersign.SIDE_BUY, "SELL": ordersign.SIDE_SELL}


def clock_delta():
    """Server-minus-local clock offset (ns) from /v1/time; 0 (local clock) if unavailable."""
    try:
        return clock_delta_ns()
    except Exception as e:
        print(f"warning: /v1/time unavailable ({describe_error(e)}); using local clock.", file=sys.stderr)
        return 0


def round_to_increment(value, increment, rounding):
    inc = Decimal(increment)
    return (value / inc).to_integral_value(rounding=rounding) * inc


def fetch_positions(address):
    """GET /v1/positions -> {marketIdStr: posdict} (empty object if none)."""
    data = call("GET", "/v1/positions?" + urllib.parse.urlencode({"address": address}))
    positions = data.get("positions")
    if positions is None:
        return {}
    if not isinstance(positions, dict):
        raise SystemExit(f"{PROG}: unexpected /v1/positions shape (expected object).")
    return positions


def open_positions(address, target_mid):
    """Non-zero positions as [(marketIdStr, posdict)]; if target_mid is set, only that marketId."""
    out = []
    for mid, p in fetch_positions(address).items():
        size = dec(p.get("size"))
        if size is None or size == 0:
            continue
        if target_mid is not None and str(p.get("marketId")) != target_mid:
            continue
        out.append((mid, p))
    return out


def main():
    p = argparse.ArgumentParser(description="Flatten arcus position(s) with reduce-only IOC market orders.")
    p.add_argument("--market", help="only close this market (display name or marketId); default: ALL open positions")
    p.add_argument("--max-slippage", default="0.05",
                   help="market-order price bound as a fraction off the mark (default 0.05 = 5%%; must be < 0.10)")
    add_network_args(p)
    args = p.parse_args()
    select_network(args.network)
    slip = positive_decimal(args.max_slippage, "--max-slippage")
    if slip >= Decimal("0.10"):
        raise SystemExit(f"{PROG}: --max-slippage must be < 0.10 (the venue's 10%-of-mark market-order cap).")

    creds = load_creds()
    address = creds["eth_address"]
    account_index = creds["account_index"]
    signer = Signer.from_private_key_hex(creds["api_private_key"])

    # Fetch market metadata up front: it supplies mark/tick/step for the plan AND lets us VALIDATE
    # --market -- a typo (e.g. BTX-USD) must FAIL, not silently match no positions and "succeed".
    markets = call("GET", "/v1/markets")["markets"]
    by_id = {str(m["marketId"]): m for m in markets}
    target_mid, scope = None, ""
    if args.market:
        mkt = resolve_market(markets, args.market)
        if mkt is None:
            raise SystemExit(f"{PROG}: unknown market {args.market!r} (not found in /v1/markets).")
        target_mid = str(mkt["marketId"])
        scope = f" market {mkt['marketDisplayName']}"

    positions = open_positions(address, target_mid)
    if not positions:
        print(f"\n  No open positions for {address}{scope} [{args.network}]\n")
        return

    # Build the close plan. Sub-step dust (|size| < stepSize) CANNOT be traded -> record it (never a
    # silent skip + exit 0); the final re-query below reports it as still-open and exits nonzero.
    plan, dust = [], []
    for mid, pos in positions:
        m = by_id.get(str(mid))
        if m is None:
            raise SystemExit(f"{PROG}: marketId {mid} from positions not found in /v1/markets.")
        size = dec(pos.get("size"))
        # A MARKET order's protective bound is validated against markPrice (within 10% of mark, per docs).
        # markPrice "0" = no mark received yet; the docs are explicit that callers must NOT substitute
        # oraclePrice -- mark is an independent EWMA feed (verified live: mark != oracle on most markets),
        # so a bound built off oracle would be checked against the absent mark -> reject/inconsistent. Fail
        # clearly instead (operator can retry once a mark is available, or close via a limit order).
        mark = dec(m.get("markPrice"))
        if mark is None or mark <= 0:
            raise SystemExit(f"{PROG}: {m.get('marketDisplayName')} has no markPrice ('0' = no mark received "
                             f"yet) -- cannot bound a reduce-only MARKET close (the venue validates the bound "
                             f"against mark; per docs, must not substitute oraclePrice). Re-run when a mark is available.")
        tick, step = m["tickSize"], m["stepSize"]
        close_side = "SELL" if size > 0 else "BUY"           # reduce a long by selling, a short by buying
        qty = round_to_increment(abs(size), step, ROUND_FLOOR)
        if qty <= 0:
            dust.append(f"{m.get('marketDisplayName')}={pos.get('size')}")
            print(f"  skip {m.get('marketDisplayName')}: |size| {abs(size)} below stepSize {step} "
                  f"(can't trade sub-step) -- NOT closeable", flush=True)
            continue
        # Protective bound = mark +/- slippage, rounded AWAY from mid so tick-rounding can't tighten it
        # (BUY-to-close UP, SELL-to-close DOWN) -- same direction as place_order's market bound.
        target = mark * (1 + slip) if close_side == "BUY" else mark * (1 - slip)
        bound = round_to_increment(target, tick, ROUND_CEILING if close_side == "BUY" else ROUND_FLOOR)
        plan.append({"market": m["marketDisplayName"], "market_id": int(m["marketId"]),
                     "side": close_side, "qty": qty, "bound": bound, "mark": mark, "tick": tick, "step": step})

    if not plan:
        # Positions exist but none are closeable (all sub-step dust) -> NOT flat, fail closed.
        print(f"\n  {PROG}: nothing closeable for {address}{scope} [{args.network}]; "
              f"{len(dust)} sub-step position(s) NOT flat: {', '.join(dust)}\n")
        raise SystemExit(1)

    print(f"\n  {PROG}: flatten {len(plan)} position(s) for {address} [{args.network}]{scope}")
    for q in plan:
        print(f"    {q['market']:<16} close {q['side']} {q['qty']:f}  reduce-only IOC market "
              f"(mark {q['mark']:f}, bound {q['bound']:f})")
    if dust:
        print(f"    ({len(dust)} sub-step position(s) NOT closeable: {', '.join(dust)})")
    print()

    ok = fail = 0
    for q in plan:
        try:
            price, qty_str = f"{q['bound']:f}", f"{q['qty']:f}"
            delta_ns = clock_delta()
            ct = time.time_ns() + delta_ns                   # server-aligned; also the X-Timestamp
            good_til_us = str((ct // 1000) + GOOD_TIL_DAYS * 86_400 * 1_000_000)
            headers = signer.sign_place_order(
                address=address, account_index=account_index, client_id=None,
                client_timestamp_ns=ct, good_til_time_ns_=ordersign.good_til_time_ns(good_til_us),
                market_id=q["market_id"], price_ticks=to_ticks(price, q["tick"]),
                quantity_quantums=to_quantums(qty_str, q["step"]),
                reduce_only=True, side=SIDES[q["side"]], time_in_force=ordersign.TIF_IOC)
            body = {"address": address, "accountIndex": account_index, "marketId": q["market_id"],
                    "orderSide": q["side"], "orderType": "MARKET", "quantity": qty_str,
                    "price": price, "timeInForce": "IOC", "timestamp": ct,
                    "goodTilTime": good_til_us, "reduceOnly": True}
            resp = call("POST", "/v1/placeOrder?" + urllib.parse.urlencode({"address": address}), body, headers)
            check_order_response(resp, f"close {q['market']}")   # 2xx body can carry status REJECTED/ERROR
            ok += 1
            print(f"  closing {q['market']} -> {q['side']} {qty_str} reduce-only  (orderId {resp.get('orderId', '')})")
        except (Exception, SystemExit) as e:                  # SystemExit = check_order_response reject; never fatal here
            fail += 1
            print(f"  FAILED {q['market']}: {describe_error(e)}")
    print(f"\n  submitted {ok} close order(s), {fail} failed.")

    # Confirm FLAT by re-querying ALL in-scope positions (catches partial fills AND the sub-step dust
    # above) -- the account is flat only if NOTHING remains open in scope.
    time.sleep(SETTLE_SECONDS)
    try:
        residual = [f"{p.get('marketDisplayName')}={p.get('size')}" for _, p in open_positions(address, target_mid)]
    except (OSError, json.JSONDecodeError) as e:
        print(f"  WARNING could not re-query positions to confirm flat ({describe_error(e)}); "
              f"treating as INCOMPLETE.", file=sys.stderr)
        raise SystemExit(1)

    if residual:
        print("  NOT flat -- still open: " + ", ".join(residual)
              + "  (re-run to finish; sub-step dust can't be closed).")
    print(f"  Verify: display_positions.py {address} --{args.network}\n")
    # Panic/automation semantics: a failed submit OR any remaining position is NOT success.
    if fail or residual:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
