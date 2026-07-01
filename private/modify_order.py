"""
Modify (cancel + replace) an open order on Arcus, identified by orderId (the API
requires it) and/or clientId.

  # convenience: identify by clientId; ONE openOrders lookup resolves the orderId,
  # the clientId echo, the immutable fields, and any omitted price/quantity.
  python3 modify_order.py --clientid mmbid1 --price 41800 --quantity 0.002 --testnet
  python3 modify_order.py --clientid mmbid1 --price 41800 --testnet     # reprice only
  python3 modify_order.py --orderid 0xabc123 --quantity 0.002 --testnet # resize only

  # FAST PATH (no server call, for tight loops): pass --orderid + all immutables
  # (--market --side --tif) + BOTH --price and --quantity. Pass --clientid too if the
  # order has one (it's the signed echo) and --reduce-only if it's reduce-only.
  python3 modify_order.py --orderid 0xabc123 --clientid mmbid1 \
      --price 41800 --quantity 0.002 --market BTC-USD --side BUY --tif GTT --testnet

Identify with --orderid and/or --clientid (at least one). The API ALWAYS identifies
a modify by orderId; --clientid alone is resolved to the orderId via the lookup.
The orderId is preserved across modifies, so it's a stable handle to cache.

What modifyOrder is: the server does an atomic cancel + replace (orderId preserved).
Only `price`/`quantity`/`goodTilTime` change; `side`, `timeInForce`, `marketId`,
`reduceOnly`, and the `clientId` are IMMUTABLE and verified against the resting order
(mismatch -> rejected). When we look the order up, supplied overrides are checked first.

Signing (see ordersign.py): modifyOrder is the TYPED payload op=3
  {ad,ai,[c,]ct,g,[id,]m,op,p,q,r,s,t,v} -- orderId required; clientId is the signed
  immutable echo; goodTilTime/reduceOnly/side/timeInForce are all part of the signature.
Every replacement carries a fresh goodTilTime >= 1 month out (365 days).

Resolves ordersign.py / marketcache.py / arcus_creds_<network>.json relative to this script.
"""

import argparse
import json
import os
import sys
import time
import urllib.parse

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _HERE)

import marketcache
import ordersign
from ordersign import Signer
from arcus_common import (add_network_args, call, check_order_response, load_creds, positive_decimal,
                          select_network, to_quantums, to_ticks, validate_client_id)

TIFS = ("GTT", "IOC", "FOK", "ALO")
SIDES = {"BUY": ordersign.SIDE_BUY, "SELL": ordersign.SIDE_SELL}
TIF_INT = {"GTT": ordersign.TIF_GTC, "IOC": ordersign.TIF_IOC,
           "FOK": ordersign.TIF_FOK, "ALO": ordersign.TIF_ALO}
# The replacement order needs a fresh goodTilTime >= 1 month out (server rule).
GOOD_TIL_DAYS = 365


def parse_args():
    parser = argparse.ArgumentParser(description="Modify an open order by clientId/orderId (cancel + replace).")
    parser.add_argument("--clientid", "--client-id", dest="client_id",
                        help="the order's clientId — usable alone to identify it, and "
                             "sent as the signed immutable echo")
    parser.add_argument("--orderid", metavar="ID",
                        help="server order ID (the API identifies a modify by this); pass it "
                             "with --market/--side/--tif/--price/--quantity to skip the lookup")
    parser.add_argument("--price", help="new limit price (decimal > 0); omit to keep current")
    parser.add_argument("--quantity", help="new size in base-asset units (decimal > 0); omit to keep current")
    parser.add_argument("--market", help="market display name (e.g. BTC-USD)")
    parser.add_argument("--side", choices=["BUY", "SELL"])
    parser.add_argument("--tif", choices=list(TIFS))
    parser.add_argument("--reduce-only", action="store_true",
                        help="the order is reduce-only (must match the original)")
    parser.add_argument("--refresh-cache", action="store_true",
                        help="force re-fetch of market metadata (bypass the Redis cache)")
    add_network_args(parser)
    args = parser.parse_args()
    if not args.client_id and not args.orderid:
        parser.error("specify --orderid and/or --clientid")
    if args.price is None and args.quantity is None:
        parser.error("specify --price and/or --quantity")
    return args


def main():
    args = parse_args()
    select_network(args.network)

    # --- Validate inputs locally before any lookup/signing --------------------
    if args.client_id is not None:
        validate_client_id(args.client_id)
    if args.price is not None:
        positive_decimal(args.price, "--price")
    if args.quantity is not None:
        positive_decimal(args.quantity, "--quantity")

    creds = load_creds()
    address = creds["eth_address"]
    account_index = creds["account_index"]
    signer = Signer.from_private_key_hex(creds["api_private_key"])

    query = urllib.parse.urlencode({"address": address})
    ident_desc = f"clientId {args.client_id}" if args.client_id else f"orderId {args.orderid}"

    # The API REQUIRES orderId to identify a modify; clientId is a signed echo of the
    # resting order's clientId (engine rejects a mismatch). FAST PATH (no lookup):
    # --orderid present + all immutables (--market/--side/--tif) + BOTH --price/--quantity.
    # Otherwise ONE openOrders lookup resolves the orderId, recovers the clientId echo +
    # immutable fields, and supplies any omitted price/quantity.
    have_all_immutable = bool(args.market and args.side and args.tif)
    have_both_pq = args.price is not None and args.quantity is not None
    need_lookup = not (args.orderid and have_all_immutable and have_both_pq)

    match = None
    if need_lookup:
        orders = call("GET", f"/v1/openOrders?{query}").get("orders", [])
        if args.orderid:
            match = next((o for o in orders if o.get("orderId") == args.orderid), None)
        else:
            matches = [o for o in orders if o.get("clientId") == args.client_id]
            if len(matches) > 1:
                raise SystemExit(f"clientId {args.client_id!r} matches {len(matches)} open orders; "
                                 f"pass --orderid to disambiguate.")
            match = matches[0] if matches else None
        if match is None:
            raise SystemExit(f"No open order with {ident_desc} (already filled/canceled, or wrong id).")

    # Resolve the immutable fields + identity. openOrders echoes timeInForce as "GTC"
    # for t=0; normalize to GTT.
    if match is not None:
        order_id = match.get("orderId")
        client_echo = match.get("clientId")            # the order's ACTUAL clientId (may be None)
        actual_market = match.get("marketDisplayName")
        actual_side = match.get("side")
        actual_tif = {"GTC": "GTT"}.get(match.get("timeInForce"), match.get("timeInForce"))
        actual_reduce = bool(match.get("reduceOnly"))
        # Supplied overrides MUST agree with the live order, else we'd sign/send the
        # wrong (immutable) values and get a server rejection. A supplied --clientid must match
        # the order's ACTUAL clientId -- including the case where the order has NONE (client_echo
        # is None): a mismatched/extraneous --clientid must be rejected, not silently ignored.
        if args.client_id is not None and args.client_id != client_echo:
            if client_echo is None:
                raise SystemExit(f"--clientid {args.client_id!r} was given, but the order has NO clientId.")
            raise SystemExit(f"--clientid {args.client_id!r} does not match the order's {client_echo!r}.")
        if args.market is not None:
            ok = (str(args.market) == str(match.get("marketId")) if args.market.isdigit()
                  else args.market.upper() == str(actual_market).upper())
            if not ok:
                raise SystemExit(f"--market {args.market!r} does not match the order's "
                                 f"{actual_market!r} (id {match.get('marketId')}).")
        for name, supplied, actual in (("side", args.side, actual_side),
                                       ("tif", args.tif, actual_tif)):
            if supplied is not None and supplied != actual:
                raise SystemExit(f"--{name} {supplied!r} does not match the order's {actual!r}.")
        if args.reduce_only and not actual_reduce:
            raise SystemExit("--reduce-only given, but the order is not reduce-only.")
        market_name, side, tif, reduce_only = actual_market, actual_side, actual_tif, actual_reduce
    else:
        order_id = args.orderid
        client_echo = args.client_id                   # may be None -> order must have no clientId
        market_name, side, tif, reduce_only = args.market, args.side, args.tif, args.reduce_only

    # Price/quantity: user value if given, else keep the order's current value (lookup only).
    # For quantity, preserve REMAINING (still-open) size, NOT originalSize -- modify is an atomic
    # cancel+replace and `quantity` is the replacement order's full resting size, so re-using
    # originalSize on a PARTIALLY-FILLED order would re-expand it (e.g. a remaining 0.4 back to 1.0,
    # increasing exposure). remainingSize == originalSize for an unfilled order, so no change there.
    price = str(args.price) if args.price is not None else str(match.get("price"))
    quantity = str(args.quantity) if args.quantity is not None else str(match.get("remainingSize"))

    # Market metadata (Redis-cached) -> tick/step; exact-multiple conversion.
    try:
        mkt = marketcache.get_market(market_name, args.network, refresh=args.refresh_cache)
    except marketcache.MarketCacheError as e:
        raise SystemExit(f"marketcache: {e}")
    # Cross-check cached metadata against the live order (in-memory; no extra
    # network/Redis call) -- a stale/corrupt cache must never make us sign a
    # modify for the wrong marketId.
    if match is not None and str(mkt["marketId"]) != str(match.get("marketId")):
        raise SystemExit(f"market-cache mismatch: cache says marketId {mkt['marketId']} for "
                         f"{market_name!r}, but the open order is marketId {match.get('marketId')} "
                         f"(try --refresh-cache).")
    market_id = mkt["marketId"]
    market_name = mkt.get("marketDisplayName", market_name)   # canonical for display
    price_ticks = to_ticks(price, mkt["tickSize"])
    quantity_quantums = to_quantums(quantity, mkt["stepSize"])

    # Replacement order needs a fresh goodTilTime >= 1 month out (local clock; 365d
    # clears the minimum regardless of small drift -- and modify's X-Timestamp is
    # generated inside ordersign, so /v1/time correction couldn't help it anyway).
    good_til_us = str(int(time.time() * 1_000_000) + GOOD_TIL_DAYS * 86_400 * 1_000_000)

    kept = [n for n, v in (("price", args.price), ("quantity", args.quantity)) if v is None]
    src = "overrides (no lookup)" if not need_lookup else f"openOrders ({ident_desc})"
    echo_note = f" clientId={client_echo}" if client_echo else " (no clientId)"
    print(f"MODIFY orderId {order_id}{echo_note}  {side} {quantity} {market_name} @ {price}  "
          f"tif={tif} reduceOnly={reduce_only}")
    print(f"  fields from: {src}" + (f"; kept current {', '.join(kept)}" if kept else ""))

    # --- Sign the typed modify payload (op=3), then POST ----------------------
    headers = signer.sign_modify_order(
        address=address, account_index=account_index, market_id=market_id,
        price_ticks=price_ticks, quantity_quantums=quantity_quantums,
        good_til_time_ns_=ordersign.good_til_time_ns(good_til_us),
        reduce_only=reduce_only, side=SIDES[side], time_in_force=TIF_INT[tif],
        order_id=order_id, client_id=client_echo,
    )
    body = {
        "address": address, "accountIndex": account_index, "marketId": market_id,
        "orderId": order_id, "side": side, "timeInForce": tif,
        "price": price, "quantity": quantity, "reduceOnly": reduce_only,
        "goodTilTime": good_til_us,
    }
    if client_echo:
        body["clientId"] = client_echo

    resp = call("POST", f"/v1/modifyOrder?{query}", body, headers)
    print("Modify response:", json.dumps(resp, indent=2))
    check_order_response(resp, "modifyOrder")


if __name__ == "__main__":
    main()
