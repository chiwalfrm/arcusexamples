#!/usr/bin/env python3
"""Compute FIFO realized/unrealized PnL for ONE arcus market from a fills file.

Input is the --condensed output of display_fills.py (CSV), e.g.:
    python3 display_fills.py 0xADDR --market BTC-USD --limit unlimited --condensed --mainnet > fills.txt
    python3 calculate_pnl.py fills.txt --mainnet
The file (or stdin: pass '-' or pipe) has one fill per line in display_fills CONDENSED_KEYS order:
    createdAt,marketDisplayName,side,size,price,fee,role,closedPnl,positionEffect,tradeId,orderId
Only createdAt(0) marketDisplayName(1) side(2) size(3) price(4) fee(5) are used; a --header row
is skipped. Rows are ordered OLDEST-first (by createdAt, epoch microseconds) for FIFO matching.
(arcus fills also carry the API's own per-fill closedPnl, but this tool computes FIFO independently
so the result doesn't depend on the venue's accounting -- useful as a cross-check.)

Output: a human-readable summary by default. With --condensed, one comma-delimited (CSV) line
intended for other programs to parse (arcus market names never contain commas, unlike dydxv4
tickers which force pipe) --
    openRemaining,market,avgOpenPrice,realizedPnL,latestPrice,unrealizedPnL,totalFees,totalVolume
      openRemaining  net remaining position after FIFO (signed: + long, - short), in coin
      avgOpenPrice   size-weighted avg price of the REMAINING open lots only (0 if flat)
      realizedPnL    FIFO realized PnL, gross/price-only (fees are NOT deducted; see totalFees)
      latestPrice    oraclePrice from /v1/markets (or the --price override)
      unrealizedPnL  (latestPrice - avgOpenPrice) * openRemaining
      totalFees      sum of every fill's fee
      totalVolume    sum of size*price over every fill -- notional in USD, NOT coin quantity

IMPORTANT -- the fills file must be COMPLETE from the position's inception for FIFO to be correct.
display_fills defaults to the newest 1000; use `--limit unlimited` (and/or --from/--to) so the file
holds the whole history, otherwise FIFO treats the oldest fill in the file as the position's start.

All arithmetic is exact Decimal (no float rounding noise). Exactly one of --mainnet/--testnet/
--staging/--price is required (mutually exclusive): a network to fetch the live oraclePrice (an
unsigned public read), or --price to supply the latest price directly (a fully offline run).
Stdlib only.
"""

import argparse
import csv
import os
import sys
from collections import deque
from decimal import Decimal
from functools import partial
from arcus_common_public import NETWORKS, dec, get_json_dict   # shared public helpers (formerly local copies)

get_json = partial(get_json_dict, prog="calculate_pnl")   # get_json + require_dict, this tool's prog


# display_fills --condensed CONDENSED_KEYS positions.
C_CREATED, C_MARKET, C_SIDE, C_SIZE, C_PRICE, C_FEE = 0, 1, 2, 3, 4, 5
MIN_FIELDS = C_FEE + 1     # need at least through the fee column


def price_arg(s):
    d = dec(s)
    if d is None or d <= 0:
        raise argparse.ArgumentTypeError("must be a positive number")
    return d


def fmt_num(d, sign=False):
    """Human-readable Decimal: comma-grouped, up to 8 dp, trailing zeros stripped. Exact zero
    renders as '0' (no +/- sign). Used for PRICES and position size (sub-penny prices kept)."""
    if d == 0:
        return "0"
    s = f"{d:+,.8f}" if sign else f"{d:,.8f}"
    return s.rstrip("0").rstrip(".") if "." in s else s


def fmt_cents(d, sign=False):
    """Dollar amount rounded to cents (2 dp), comma-grouped; avoids -0.00/+0.00. Used for PnL,
    fees, and volume -- NOT for prices (which can be fractions of a cent)."""
    s = f"{d:+,.2f}" if sign else f"{d:,.2f}"
    return "0.00" if s.lstrip("+-") == "0.00" else s


def print_human(market, remaining, avg, realized, latest, unrealized, fees, volume, price_src):
    """Readable summary (default output). --condensed emits the machine line instead."""
    base = market.split("-")[0] if "-" in market else market
    print()
    print(f"  FIFO PnL - {market}   (latest price {fmt_num(latest)} via {price_src})")
    print()
    if remaining == 0:
        print("  Open position remaining   flat")
        print("  Average open price        -")
    else:
        side = "long" if remaining > 0 else "short"
        print(f"  Open position remaining   {fmt_num(remaining)} {base} ({side})")
        print(f"  Average open price        {fmt_num(avg)}")
    print(f"  Realized PnL (FIFO)       {fmt_cents(realized, sign=True)}")
    print(f"  Unrealized PnL (FIFO)     {fmt_cents(unrealized, sign=True)}")
    print(f"  Total fees                {fmt_cents(fees)}")
    print(f"  Total volume (USD)        {fmt_cents(volume)}")
    print()


def read_fills(fh, want_market):
    """Parse CSV rows into fill dicts. Skips a header row and warns on malformed rows. Returns
    (fills, markets_seen). createdAt is kept as an int (epoch microseconds) for sorting."""
    fills, markets = [], set()
    for lineno, parts in enumerate(csv.reader(fh), 1):
        if not parts or not any(c.strip() for c in parts):
            continue
        if len(parts) < MIN_FIELDS:
            print(f"calculate_pnl: line {lineno}: too few fields "
                  f"({len(parts)} < {MIN_FIELDS}), skipping", file=sys.stderr)
            continue
        side = parts[C_SIDE].strip().upper()
        size, price, fee = dec(parts[C_SIZE]), dec(parts[C_PRICE]), dec(parts[C_FEE])
        market, created_s = parts[C_MARKET].strip(), parts[C_CREATED].strip()
        if side not in ("BUY", "SELL") or size is None or price is None:
            if parts[C_CREATED].strip().lower() == "createdat":     # a --header row -> skip quietly
                continue
            print(f"calculate_pnl: line {lineno}: unparseable side/size/price, skipping",
                  file=sys.stderr)
            continue
        if want_market and market != want_market:
            continue
        # A real trade has size > 0 AND price > 0. dec() accepts "0"/"-5" (finite but <= 0), which would
        # corrupt FIFO: a negative size FLIPS the fill's side (q = size for a BUY), and a 0/negative price
        # gives garbage realized PnL + volume. Skip non-positive rows (size/price are both non-None here).
        if size <= 0 or price <= 0:
            print(f"calculate_pnl: line {lineno}: non-positive size/price ({size}/{price}), skipping",
                  file=sys.stderr)
            continue
        try:
            created = int(created_s)
        except (TypeError, ValueError):
            created = -1                                            # unparseable time -> sort oldest
        markets.add(market)
        fills.append({"created": created, "market": market, "side": side,
                      "size": size, "price": price, "fee": fee if fee is not None else Decimal(0)})
    return fills, markets


def fifo_pnl(fills):
    """`fills` OLDEST-first. Match opposing fills FIFO. Returns
    (realized, remaining_signed, avg_open_price). avg is 0 when flat."""
    lots = deque()                 # each: [signed_qty, price]; all entries share the position's sign
    realized = Decimal(0)
    for f in fills:
        q = f["size"] if f["side"] == "BUY" else -f["size"]
        p = f["price"]
        # Close against opposing lots FIFO until this fill is exhausted or the side flips.
        while q != 0 and lots and (lots[0][0] > 0) != (q > 0):
            lot = lots[0]
            close = min(abs(lot[0]), abs(q))
            if lot[0] > 0:
                realized += (p - lot[1]) * close     # long lot closed by a sell
            else:
                realized += (lot[1] - p) * close     # short lot closed by a buy
            lot[0] -= close if lot[0] > 0 else -close
            q -= close if q > 0 else -close
            if lot[0] == 0:
                lots.popleft()
        if q != 0:                                   # leftover opens/extends the position
            lots.append([q, p])
    remaining = sum((lot[0] for lot in lots), Decimal(0))
    if remaining != 0:
        avg = sum((lot[0] * lot[1] for lot in lots), Decimal(0)) / remaining
    else:
        avg = Decimal(0)
    return realized, remaining, avg


def fetch_oracle_price(base, market):
    """oraclePrice for `market` from /v1/markets (a list keyed by marketDisplayName), or clean exit."""
    data = get_json(f"{base}/v1/markets", "markets")
    markets = data.get("markets") if isinstance(data, dict) else None
    if not isinstance(markets, list):
        raise SystemExit("calculate_pnl: unexpected /v1/markets response (no 'markets' list).")
    m = next((x for x in markets if isinstance(x, dict)
              and str(x.get("marketDisplayName")) == market), None)
    if m is None:
        raise SystemExit(f"calculate_pnl: market {market!r} not found in /v1/markets.")
    op = dec(m.get("oraclePrice"))
    if op is None or op <= 0:      # dec() rejects non-finite; also reject <= 0 (a 0/negative oracle -> garbage unrealized PnL)
        raise SystemExit(f"calculate_pnl: no usable oraclePrice for {market!r} (got {m.get('oraclePrice')!r}).")
    return op


def main():
    p = argparse.ArgumentParser(description="FIFO PnL for one arcus market from a fills file.")
    p.add_argument("file", nargs="?", default="-",
                   help="fills file (display_fills --condensed output); '-' or omitted = stdin")
    p.add_argument("--market",
                   help="market to compute (marketDisplayName; required only if the file mixes markets)")
    p.add_argument("--condensed", action="store_true",
                   help="emit one machine-readable comma-delimited (CSV) line (for other programs) "
                        "instead of the human-readable summary")
    # Exactly one price SOURCE (mutually exclusive, required): a network to fetch the live
    # oraclePrice, or --price to supply it directly (a fully offline run). Everything else in the
    # output is computed from the file alone -- only latestPrice/unrealizedPnL need a price.
    src = p.add_mutually_exclusive_group(required=True)
    src.add_argument("--mainnet", dest="network", action="store_const", const="mainnet",
                     help="fetch the latest price from the mainnet server")
    src.add_argument("--testnet", dest="network", action="store_const", const="testnet",
                     help="fetch the latest price from the testnet server")
    src.add_argument("--staging", dest="network", action="store_const", const="staging",
                     help="fetch the latest price from the staging server")
    src.add_argument("--price", type=price_arg, metavar="PRICE",
                     help="use this exact latest price for unrealized PnL (no network fetch)")
    args = p.parse_args()

    if args.file == "-":
        if sys.stdin.isatty():
            raise SystemExit("calculate_pnl: no input file given and stdin is a tty (see --help).")
        fills, markets = read_fills(sys.stdin, args.market)
    else:
        try:
            with open(args.file) as fh:
                fills, markets = read_fills(fh, args.market)
        except OSError as e:
            raise SystemExit(f"calculate_pnl: cannot read {args.file}: {e}")

    if not fills:
        raise SystemExit("calculate_pnl: no fills parsed from input"
                         + (f" for market {args.market}" if args.market else "") + ".")
    if args.market:
        market = args.market
    elif len(markets) == 1:
        market = next(iter(markets))
    else:
        raise SystemExit("calculate_pnl: input mixes multiple markets "
                         f"({', '.join(sorted(markets))}); pass --market to pick one.")

    # Oldest-first for FIFO: reverse (display_fills emits newest-first) then a stable sort by
    # createdAt, so ties keep the reversed (oldest-first) order and non-sorted input is still fixed.
    fills.reverse()
    fills.sort(key=lambda f: f["created"])

    realized, remaining, avg = fifo_pnl(fills)
    total_fees = sum((f["fee"] for f in fills), Decimal(0))
    total_volume = sum((f["size"] * f["price"] for f in fills), Decimal(0))

    latest = args.price if args.price is not None else fetch_oracle_price(NETWORKS[args.network], market)
    unrealized = (latest - avg) * remaining          # signed remaining handles long/short

    if args.condensed:
        # CSV (comma): arcus marketDisplayNames never contain commas, so comma is safe here
        # (dydxv4 must use '|' because its tickers can). Matches display_fills --condensed (CSV).
        print(f"{remaining},{market},{avg},{realized},{latest},{unrealized},{total_fees},{total_volume}")
    else:
        price_src = "--price override" if args.price is not None else f"{args.network} oracle"
        print_human(market, remaining, avg, realized, latest, unrealized, total_fees, total_volume, price_src)


if __name__ == "__main__":
    try:
        main()
    except BrokenPipeError:
        try:
            os.dup2(os.open(os.devnull, os.O_WRONLY), sys.stdout.fileno())
        except Exception:
            pass
        sys.exit(0)
