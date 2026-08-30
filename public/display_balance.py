"""
Display account balance / collateral for an account.

  python3 display_balance.py <eth_address>
  python3 display_balance.py <eth_address> --condensed   # raw equity only (for scripts)

Uses GET /v1/account, a public account-scoped read that takes only the
`address` query parameter and needs NO signature -- so this display tool needs
just the address, not the creds file (same as display_orders/display_positions).

All monetary values are full quote-currency (USDC) decimal strings:
  equity          = netQuoteBalance + Σ(size × oracle)   -- total account value
  freeCollateral  = equity − Σ initial margin            -- available to trade
  netQuoteBalance = aggregate cash as of the last event  -- moves only on cash flows
"""

import argparse
import urllib.parse
from arcus_common_public import add_network_args, require_eth_address, run_pipe_safe, NETWORKS, get_json, num, require_dict   # shared public helpers (formerly local copies)

BASE = None   # set in main() from the required --testnet/--staging/--mainnet selector


def count_positions(positions):
    """Open-position count, tolerant of shape (dict keyed by marketId, or list)."""
    return len(positions) if isinstance(positions, (dict, list)) else 0


def fetch_account(address):
    """GET /v1/account, turning network/HTTP/JSON failures into clean CLI errors."""
    query = urllib.parse.urlencode({"address": address})
    # Shared retrying reader (Retry-After/backoff on 429 incl. Cloudflare 1015 + 5xx). none_on_404: a 404 means
    # the address is valid but never traded/deposited -> friendly message rather than a raw HTTP error. A bad
    # address (400) and other non-429 4xx still raise SystemExit with the API's own error body, inside get_json.
    data = get_json(f"{BASE}/v1/account?{query}", what="account", prog="display_balance", none_on_404=True)
    if data is None:
        raise SystemExit(f"No activity yet for {address} (account has never been touched).")
    return require_dict(data, "account", "display_balance")


def main():
    global BASE
    parser = argparse.ArgumentParser(description="Display account balance / collateral.")
    parser.add_argument("address", help="Ethereum address of the account to display")
    parser.add_argument("--condensed", action="store_true",
                        help="machine-readable: print only the raw equity value "
                             "(no label, no commas)")
    parser.add_argument("--header", action="store_true",
                        help="with --condensed, print an 'equity' header line first "
                             "(error if used without --condensed)")
    add_network_args(parser)
    args = parser.parse_args()
    BASE = NETWORKS[args.network]
    address = args.address

    # Cheap local check -> a clear error before any network round-trip.
    require_eth_address(address, "display_balance")
    if args.header and not args.condensed:
        raise SystemExit("display_balance: --header requires --condensed.")

    acct = fetch_account(address)

    if args.condensed:
        # Raw equity straight from the API (no commas) for downstream scripts.
        # Fail loudly if it's absent rather than emit an empty success line.
        equity = acct.get("equity")
        if equity is None or equity == "":
            raise SystemExit(f"display_balance: no 'equity' field in response for {address}.")
        if args.header:
            print("equity")
        print(equity)
        return

    # Label / value rows; values right-aligned in a shared field so cents line up.
    rows = [
        ("Equity",              num(acct.get("equity"))),
        ("Free collateral",     num(acct.get("freeCollateral"))),
        ("Net quote balance",   num(acct.get("netQuoteBalance"))),
        ("Net deposits",        num(acct.get("netDeposits"))),
        ("Pending deposits",    num(acct.get("pendingDeposits"))),
        ("Pending withdrawals", num(acct.get("pendingWithdrawals"))),
        ("Open positions",      str(count_positions(acct.get("positions")))),
    ]

    labelw = max(len(label) for label, _ in rows)
    valuew = max(len(value) for _, value in rows)

    print(f"Account {acct.get('address', address)}  (index {acct.get('accountIndex', '?')})\n")
    for label, value in rows:
        print(f"  {label:<{labelw}} : {value:>{valuew}}")


if __name__ == "__main__":
    run_pipe_safe(main)
