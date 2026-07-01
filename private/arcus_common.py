"""
Shared helpers for the Arcus order/signing CLIs:
place_order.py, modify_order.py, cancel_order.py, market_maker.py.

Lives in ~/info next to ordersign.py / marketcache.py / arcus_creds_<network>.json and
resolves them relative to ITS OWN location, so importers work from any cwd.
(The standalone tools in ~/extra -- show*/ws* -- deliberately don't depend on
this; they have no ordersign/creds coupling.)
"""

import json
import os
import re
import stat
import sys
import time
import urllib.error
import urllib.parse  # noqa: F401  (re-exported convenience for importers)
import urllib.request
from decimal import Decimal, InvalidOperation

_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)
import ordersign

CLIENT_ID_RE = re.compile(r"^[A-Za-z0-9_-]{1,36}$")
ADDR_RE = re.compile(r"^0x[0-9a-fA-F]{40}$")

# ── Network selection ─────────────────────────────────────────────────────────
# One of these MUST be chosen per invocation (see add_network_args, which makes
# --testnet/--staging a required mutually-exclusive pair). When mainnet launches,
# add it here, drop `required=True`, and default the absent case to "mainnet".
NETWORKS = {
    "testnet": "https://api.testnet.arcus.xyz",
    "staging": "https://api.staging.arcus.xyz",
    "mainnet": "https://api.arcus.xyz",       # live 2026-06-25 (reads only; trading not yet enabled)
}

NETWORK = None        # set by select_network()
BASE = None           # set by select_network() -> NETWORKS[NETWORK]
CREDS_PATH = None     # set by select_network() -> arcus_creds_<network>.json


def add_network_args(parser):
    """Register the REQUIRED, mutually-exclusive --testnet/--staging selector.

    Required for now (no mainnet yet); when mainnet ships, drop required=True and
    treat the absent case as mainnet. Sets args.network to the network string.
    """
    g = parser.add_mutually_exclusive_group(required=True)
    g.add_argument("--testnet", dest="network", action="store_const", const="testnet",
                   help="use the testnet server + arcus_creds_testnet.json")
    g.add_argument("--staging", dest="network", action="store_const", const="staging",
                   help="use the staging server + arcus_creds_staging.json")
    g.add_argument("--mainnet", dest="network", action="store_const", const="mainnet",
                   help="use the mainnet server + arcus_creds_mainnet.json")
    return parser


def select_network(network):
    """Resolve `network` into the module globals BASE/CREDS_PATH used by
    request()/load_creds(). Call once, right after parse_args()."""
    global NETWORK, BASE, CREDS_PATH
    if network not in NETWORKS:
        raise SystemExit(f"unknown network {network!r}; expected one of {sorted(NETWORKS)}.")
    NETWORK = network
    BASE = NETWORKS[network]
    CREDS_PATH = os.path.join(_HERE, f"arcus_creds_{network}.json")
    return network


# ── Credentials ──────────────────────────────────────────────────────────────
def load_creds():
    """Load arcus_creds_<network>.json (next to this module) with clean errors."""
    if CREDS_PATH is None:
        raise SystemExit("no network selected; pass --testnet or --staging.")
    try:
        # This file holds a fund-controlling Ed25519 api_private_key. A group/world-readable copy is
        # a real exposure -- FAIL CLOSED unless the operator explicitly opts out (e.g. a locked-down
        # shared box where perms can't be 600). generate_arcus_creds.sh writes 600; this enforces it.
        if os.stat(CREDS_PATH).st_mode & (stat.S_IRWXG | stat.S_IRWXO):
            if os.environ.get("ARCUS_ALLOW_INSECURE_CREDS") == "1":
                print(f"WARNING: {CREDS_PATH} is group/world-accessible "
                      "(allowed via ARCUS_ALLOW_INSECURE_CREDS=1).", file=sys.stderr)
            else:
                raise SystemExit(f"{CREDS_PATH} is group/world-accessible -- refusing to read a private "
                                 f"key from it.\n  Lock it:  chmod 600 {CREDS_PATH}\n"
                                 f"  Override: set ARCUS_ALLOW_INSECURE_CREDS=1 to read it anyway.")
        with open(CREDS_PATH) as f:
            creds = json.load(f)
    except FileNotFoundError:
        raise SystemExit(f"missing {CREDS_PATH} (create it with generate_arcus_creds.sh).")
    except json.JSONDecodeError as e:
        raise SystemExit(f"invalid JSON in {CREDS_PATH}: {e}")
    for k in ("eth_address", "account_index", "api_private_key"):
        if k not in creds:
            raise SystemExit(f"{CREDS_PATH}: missing required field {k!r}.")
    # Validate shapes here -- this feeds signing, so catch bad creds early rather
    # than fail deep inside signing or send a malformed request body.
    if not ADDR_RE.match(str(creds["eth_address"])):
        raise SystemExit(f"{CREDS_PATH}: eth_address must be 0x + 40 hex chars.")
    ai = creds["account_index"]
    if isinstance(ai, bool) or not isinstance(ai, int) or ai < 0:
        raise SystemExit(f"{CREDS_PATH}: account_index must be a non-negative integer.")
    try:
        if len(bytes.fromhex(str(creds["api_private_key"]))) < 32:
            raise ValueError
    except ValueError:
        raise SystemExit(f"{CREDS_PATH}: api_private_key must be hex encoding >= 32 bytes (Ed25519).")
    return creds


# Definitive-failure order statuses on the synchronous 200 path. The place/cancel/modify endpoints
# are ASYNC: 202 -> status ACK / CANCEL_ACKNOWLEDGED (accepted; lifecycle on the orders WS), 200 ->
# the gateway already had definitive state, which CAN be REJECTED/ERROR (a 2xx HTTP code carrying a
# FAILURE body), 400 -> HTTPError (request() raises). The HTTP layer catches the 400s; this catches
# the 200-with-failure-body case so a rejected order/cancel isn't mistaken for success.
FAILED_ORDER_STATUSES = frozenset({"REJECTED", "ERROR"})


def check_order_response(resp, what="order"):
    """Raise SystemExit if a place/cancel/modify response body reports a definitive failure
    (status REJECTED/ERROR on the 200 path). Returns resp otherwise (incl. the common 202 ACK)."""
    if isinstance(resp, dict):
        status = str(resp.get("status", "")).upper()
        if status in FAILED_ORDER_STATUSES:
            reason = resp.get("rejectionReason") or resp.get("error") or status
            raise SystemExit(f"{what} {status}: {reason}")
    return resp


# ── HTTP ─────────────────────────────────────────────────────────────────────
def describe_error(e):
    """One-line, readable rendering of a request/JSON error."""
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


def request(method, path, body=None, headers=None):
    """Perform an HTTP request and return parsed JSON. RAISES on failure --
    for callers that handle errors themselves (e.g. a long-running loop)."""
    if BASE is None:
        raise SystemExit("no network selected; pass --testnet or --staging.")
    hdrs = dict(headers or {})
    data = None
    if body is not None:
        data = ordersign.canonical_json(body)
        hdrs.setdefault("Content-Type", "application/json")  # only when there IS a body
    req = urllib.request.Request(BASE + path, data=data, method=method, headers=hdrs)
    with urllib.request.urlopen(req, timeout=10) as r:
        return json.loads(r.read() or b"{}")


def call(method, path, body=None, headers=None):
    """One-shot CLI variant of request(): any failure -> clean SystemExit."""
    try:
        return request(method, path, body, headers)
    except (urllib.error.URLError, OSError, json.JSONDecodeError) as e:
        raise SystemExit(describe_error(e))


# ── Server clock (drift correction) ──────────────────────────────────────────
def server_time_ns():
    """API server clock in ns, from GET /v1/time ({"timeNs": <int>}). RAISES."""
    return int(request("GET", "/v1/time")["timeNs"])


def clock_delta_ns():
    """Estimated (server - local) clock offset in ns: ADD to a local time.time_ns()
    to get server time. Measured at the request midpoint to net out round-trip
    latency. RAISES on failure -- callers decide whether to fall back to local."""
    before = time.time_ns()
    api = server_time_ns()
    after = time.time_ns()
    return api - (before + after) // 2


# ── Validation / conversion ──────────────────────────────────────────────────
def dec(value):
    """Decimal(value), or None if not numeric OR not finite (NaN/Infinity).

    Rejecting non-finite here protects every caller (e.g. dec(markPrice) for
    --force bounds), not just positive_decimal().
    """
    try:
        v = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    return v if v.is_finite() else None


def positive_decimal(s, name, allow_zero=False):
    """Parse a CLI numeric arg as a finite Decimal (> 0, or >= 0 if allow_zero).

    Rejects NaN/Infinity -- `Decimal("NaN"/"Infinity")` parse fine and slip past
    `<= 0` comparisons (NaN comparisons are always False), so guard explicitly.
    """
    v = dec(s)
    if v is None or not v.is_finite():
        raise SystemExit(f"{name}: {s!r} is not a valid finite decimal.")
    if v < 0 or (v == 0 and not allow_zero):
        raise SystemExit(f"{name}: must be {'>= 0' if allow_zero else '> 0'} (got {s}).")
    return v


def validate_client_id(cid):
    if not isinstance(cid, str) or not CLIENT_ID_RE.match(cid):
        raise SystemExit("--clientid: must be 1-36 chars of [A-Za-z0-9_-].")


def to_ticks(price_str, tick):
    """price -> integer ticks; clean error (not a ValueError traceback) if off-tick."""
    try:
        return ordersign.price_to_ticks(price_str, tick)
    except ValueError:
        raise SystemExit(f"price {price_str} is not a multiple of the tick size {tick}.")


def to_quantums(qty_str, step):
    """size -> integer quantums; clean error if off-step."""
    try:
        return ordersign.size_to_quantums(qty_str, step)
    except ValueError:
        raise SystemExit(f"quantity {qty_str} is not a multiple of the step size {step}.")


# ── Market resolution ────────────────────────────────────────────────────────
def resolve_market(markets, ident):
    """Find a market by numeric marketId or case-insensitive display name (or None).

    Returns the market dict; its marketDisplayName is the CANONICAL name to use
    for the /v1/l2OrderBook path etc. (a numeric id 404s there).
    """
    ident = str(ident).strip()                # tolerate int input / stray whitespace
    if ident.isdigit():
        return next((m for m in markets if str(m.get("marketId")) == str(int(ident))), None)
    up = ident.upper()
    return next((m for m in markets if str(m.get("marketDisplayName", "")).upper() == up), None)
