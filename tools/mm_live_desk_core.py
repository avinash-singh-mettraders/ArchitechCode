"""
REST helpers + browser UI for MM Live Desk (no tkinter).

Loads the same merged config as the C++ stack, opens a local dashboard, and can
**write back** to ``config/credentials.local.json`` and ``config/default_config.json``.
``POST /api/apply_persist`` persists desk fields to ``default_config.json``, re-authenticates if keys changed,
calls order-gateway **cancel-all-orders** for the MM symbol, then places a fresh bid/ask from the **external
theo** (Neon FIX when ``external_feed.provider`` is ``neon_fix``, else configurable REST bookTicker) with the same
inventory skew / max-position side-pull as ``MakeMarketStrategy`` (C++). (No sidebar button; use API or curl.)

On **feed start**, the desk **GET /instruments** and uses **only** that response for AX
``tick_size`` (minimum price increment): in-memory ``ax_tick_by_symbol``, persisted per leg to
``default_config.json``, and merged into ``mm_instruments`` for the UI. No ``PRICE_TICK`` /
``price_tick`` fallbacks on MM paths. Manual refresh: ``POST /api/desk/sync_instrument_ticks``.

Operator entry point (only supported mode):
  python3 /path/to/platform_core/tools/mm_live_desk.py
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import socket
import ssl
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import threading
import uuid
import time
import webbrowser
import urllib.error
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse
from pathlib import Path

try:
    import neon_fix_feed
except ImportError:
    import sys

    _TOOLS_DIR = str(Path(__file__).resolve().parent)
    if _TOOLS_DIR not in sys.path:
        sys.path.insert(0, _TOOLS_DIR)
    import neon_fix_feed  # noqa: F401

try:
    import mm_paired_cancel
except ImportError:
    import sys

    _TOOLS_DIR = str(Path(__file__).resolve().parent)
    if _TOOLS_DIR not in sys.path:
        sys.path.insert(0, _TOOLS_DIR)
    import mm_paired_cancel  # noqa: F401

# Server-layer registry for bid/ask order pairs. Populated when a pair is placed
# together (see desk_mm_stack_pair_place_on_gateway_then_tell_cpp /
# desk_requote_mm_pair_after_cancel) and consulted from the account poll loop to
# cancel the surviving leg on a margin-related reject. See tools/mm_paired_cancel.py.
_MM_PAIR_REGISTRY = mm_paired_cancel.PairRegistry()

REPO_ROOT = Path(__file__).resolve().parent.parent
SCRIPT_PATH_MAIN = (REPO_ROOT / "tools" / "mm_live_desk.py").resolve()
CREDENTIALS_PATH = REPO_ROOT / "config" / "credentials.local.json"
DEFAULT_CONFIG_PATH = REPO_ROOT / "config" / "default_config.json"
EXTERNAL_FEED_LOCAL_PATH = REPO_ROOT / "config" / "external_feed.local.json"
MM_LIVE_DESK_CORE_FILE = Path(__file__).resolve()
MM_LIVE_DESK_CLIENT_JS_FILE = MM_LIVE_DESK_CORE_FILE.with_name("mm_live_desk_client.js")
# Fallback when no default_config; production gateway (see https://docs.architect.exchange/api-reference/user-management/get-whoami).
DEFAULT_REST = "https://gateway.architect.exchange/api"


def _ws_endpoint_from_rest(rest_url: str) -> str:
    """Derive the orders WebSocket URL from the REST endpoint.

    ``https://gateway.architect.exchange/api`` → ``wss://gateway.architect.exchange/orders/ws``
    ``https://gateway.sandbox.architect.exchange/api`` → ``wss://gateway.sandbox.architect.exchange/orders/ws``
    Falls back to the AX PRODUCTION orders WS if the REST URL is unparseable so the
    desk fails closed against the configured environment rather than silently
    pointing at a stale sandbox host.
    """
    try:
        host = str(rest_url or "").split("://", 1)[1].split("/", 1)[0]
        if host:
            return f"wss://{host}/orders/ws"
    except (IndexError, AttributeError):
        pass
    return "wss://gateway.architect.exchange/orders/ws"
# Intentionally empty: the desk should discover AX symbols from config (`market_maker.instruments`,
# `market_maker.symbol`, or explicit UI input), not force a product default.
DEFAULT_AX_SYMBOL = ""
PRICE_TICK = 0.0001
BOOK_DEPTH = 20
FILLS_LIMIT = 80
ORDERS_LIMIT = 80
# Log once per symbol per process when depth falls back to /ticker L1 (avoids spam every poll).
_AX_TICKER_L1_NOTE_SYMS: set[str] = set()
REST_POLL_INTERVAL_SEC = float(os.environ.get("MM_DESK_REST_POLL_SEC", "10"))
# Architect: book refresh vs order-gateway + fills (separate intervals; WS not in this desk).
_AX_LEGACY = os.environ.get("MM_DESK_AX_POLL_SEC")
MM_DESK_AX_BOOK_POLL_SEC = float(
    _AX_LEGACY if _AX_LEGACY is not None else os.environ.get("MM_DESK_AX_BOOK_POLL_SEC", "2"),
)
MM_DESK_ACCOUNT_POLL_SEC = float(os.environ.get("MM_DESK_ACCOUNT_POLL_SEC", "5"))
MM_DESK_REF_MAX_SYMBOLS = int(os.environ.get("MM_DESK_REF_MAX_SYMBOLS", "12"))
# Shallow books per symbol: GET /instruments (list) + GET /book (auth). Disable: MM_DESK_AX_ALL_BOOKS=0
AX_ALL_BOOK_DEPTH = int(os.environ.get("MM_DESK_AX_GRID_DEPTH", str(BOOK_DEPTH)))
AX_MAX_SYMBOLS_ALL = int(os.environ.get("MM_DESK_AX_MAX_SYMBOLS", "48"))
MM_DESK_AX_ALL_BOOKS = os.environ.get("MM_DESK_AX_ALL_BOOKS", "1").strip().lower() not in ("0", "false", "no", "off")
# GET /instruments tick map refresh while AX feeds run (0 = disable).
MM_DESK_AX_INSTRUMENT_TICK_REFRESH_SEC = float(os.environ.get("MM_DESK_AX_INSTRUMENT_TICK_REFRESH_SEC", "120"))
MM_DESK_VERBOSE = os.environ.get("MM_DESK_VERBOSE", "").strip().lower() in ("1", "true", "yes")
REFERENCE_HTTP_TIMEOUT = float(os.environ.get("MM_DESK_REFERENCE_TIMEOUT_SEC", "15"))
ORDERS_HTTP_TIMEOUT = float(os.environ.get("MM_DESK_ORDERS_TIMEOUT_SEC", "15"))
AX_HTTP_TIMEOUT = float(os.environ.get("MM_DESK_AX_API_TIMEOUT_SEC", "20"))
# /book can be slow on sandbox; duplicate calls in one tick caused timeouts — keep a single poll per symbol.
AX_BOOK_HTTP_TIMEOUT = float(os.environ.get("MM_DESK_AX_BOOK_HTTP_TIMEOUT_SEC", "25"))
# When false and provider is neon_fix: reference column reads C++ logs/mm_external_depth.json (~1s updates).
# Set MM_LIVE_DESK_WEB_REFERENCE_FEED=1 to run Neon/REST reference + FIX Security List inside this Python process.
# Default unset = off: avoids a second FIX session while C++ trading_client already uses Neon (TLS OK but no FIX after Logon).
def _web_desk_run_reference_feed_from_env(reference_provider: str) -> bool:
    v = os.environ.get("MM_LIVE_DESK_WEB_REFERENCE_FEED", "").strip().lower()
    if v in ("0", "false", "no", "off"):
        return False
    if v in ("1", "true", "yes", "on"):
        return True
    return str(reference_provider or "").strip().lower() != "neon_fix"


def _reference_depth_http_url(
    base: str, symbol: str, use_spot: bool, *, depth_limit: int | None = None
) -> str:
    q = urllib.parse.quote(symbol)
    b = base.rstrip("/")
    lim = BOOK_DEPTH if depth_limit is None else int(depth_limit)
    if use_spot:
        return f"{b}/api/v3/depth?symbol={q}&limit={lim}"
    return f"{b}/fapi/v1/depth?symbol={q}&limit={lim}"


def _reference_book_ticker_http_url(base: str, symbol: str, use_spot: bool) -> str:
    q = urllib.parse.quote(symbol)
    b = base.rstrip("/")
    if use_spot:
        return f"{b}/api/v3/ticker/bookTicker?symbol={q}"
    return f"{b}/fapi/v1/ticker/bookTicker?symbol={q}"


def _is_hyperliquid_provider(provider: str) -> bool:
    p = str(provider or "").strip().lower()
    return p in ("hyperliquid", "hyper_liquid", "hl")


def _empty_feed_health() -> dict:
    """Default snapshot when the C++ side has not produced a health file yet.

    Keeps the same shape as the on-disk JSON so the JS pill renderer can stay
    in one branch (no special-casing for "file missing"). All three feeds
    default to ``up=False`` with ``last_error="awaiting trading_client"`` so
    the desk shows red until the C++ binary writes its first snapshot.
    """
    feed = {"up": False, "last_ok_ms": 0, "age_ms": -1, "last_error": "awaiting trading_client"}
    return {
        "updated_ms": 0,
        "enabled": False,
        "running": False,
        "up_count": 0,
        "mettraders": dict(feed),
        "hyperliquid": dict(feed),
        "neon": dict(feed),
        "legs": [],
        "stale": True,
    }


def _resolve_mm_feed_health_path(merged_cfg: dict) -> Path:
    """Resolve the absolute path to ``logs/mm_feed_health.json`` from config.

    Honors the ``mm_desk.mm_feed_health_path`` knob (relative paths are
    interpreted from the repo root, matching how the C++ writer behaves).
    """
    rel = ""
    try:
        rel = str((merged_cfg.get("mm_desk") or {}).get("mm_feed_health_path") or "")
    except (AttributeError, TypeError):
        rel = ""
    if not rel:
        rel = "logs/mm_feed_health.json"
    p = Path(rel)
    if not p.is_absolute():
        p = REPO_ROOT / p
    return p


def load_mm_feed_health(merged_cfg: dict) -> dict:
    """Read the C++-written feed-health snapshot and tag it with freshness.

    Returns the parsed JSON augmented with ``stale=True`` when the file is
    missing or older than 5s (so the UI can grey out the pill row instead of
    showing stale green pills after the C++ binary stops). On read or parse
    error returns the empty default — the desk should never crash because the
    health file is briefly mid-rename.
    """
    path = _resolve_mm_feed_health_path(merged_cfg)
    if not path.is_file():
        d = _empty_feed_health()
        d["last_error"] = f"file not found: {path}"
        return d
    try:
        raw = path.read_text(encoding="utf-8")
        data = json.loads(raw)
    except (OSError, json.JSONDecodeError) as exc:
        d = _empty_feed_health()
        d["last_error"] = f"read/parse: {exc}"
        return d
    if not isinstance(data, dict):
        return _empty_feed_health()
    now_ms = int(time.time() * 1000)
    upd = int(data.get("updated_ms") or 0)
    data["stale"] = (upd <= 0) or ((now_ms - upd) > 5000)
    return data


_MM_ORDERS_CONFIG_LOCK = threading.Lock()
_MM_TEMPLATES_LOCK = threading.Lock()
_MM_FORCE_CANCEL_LOCK = threading.Lock()
# Serializes every desk read-modify-write of default_config.json. That file holds the
# ENTIRE engine config (a clobber is strictly worse than the July orders.json incident),
# is read live by the C++ engine (per-instrument mm_orders_enabled gate) and hand-edited by
# operators. All desk writers must go through `_default_config_mutate_write` under this lock.
_DEFAULT_CONFIG_WRITE_LOCK = threading.Lock()

# C3 guard (2026-06-11): set of "<ax>::<stack_id>" placement keys with a gateway
# placement currently in flight. Checked + populated under _MM_ORDERS_CONFIG_LOCK in the
# place_order dup-check so two concurrent requests for the same stack_id cannot both pass the
# check and double-place at the venue (the lock is released before the gateway REST call, so
# unrelated stacks are never serialized). Always cleared on completion/failure.
_MM_PLACE_IN_PROGRESS: set = set()


def _resolve_mm_orders_config_path(merged_cfg: dict) -> Path:
    """``mm_desk.mm_orders_config_path`` — desk-owned desired-state config
    (``orders.json``) that the trading_client reconciles against every ~1s.

    Schema (Python sole writer)::

        {
          "version": 1,
          "updated_ms": 1234567890000,
          "products": {
            "SYMBOL-PERP": {
              "theo_source": "neon_fix",
              "theo_venue_symbol": "USD/JPY",
              "stacks": [
                {"id": "<uuid>", "width_ticks": 4, "order_size": 10000, ...},
                ...
              ]
            }
          }
        }

    Relative paths resolve from the repo root (same convention as the other
    desk JSON files). The lock above serialises read-modify-write across
    concurrent Flask handlers so two simultaneous Add/Cancel calls never
    clobber each other.
    """
    rel = ""
    try:
        rel = str((merged_cfg.get("mm_desk") or {}).get("mm_orders_config_path") or "")
    except (AttributeError, TypeError):
        rel = ""
    if not rel:
        rel = "logs/orders.json"
    p = Path(rel)
    if not p.is_absolute():
        p = REPO_ROOT / p
    return p


def _resolve_mm_orders_active_path(merged_cfg: dict) -> Path:
    """``mm_desk.mm_orders_active_path`` — C++-written live projection of
    ``orders.json`` (per-stack: AX bid/ask order_ids, prices, qtys, fills
    counter, last fill identifier). Only stacks the AX gateway has acked
    appear here, per the "ack-only" rule the desk uses to guard against
    reject-induced phantom rows."""
    rel = ""
    try:
        rel = str((merged_cfg.get("mm_desk") or {}).get("mm_orders_active_path") or "")
    except (AttributeError, TypeError):
        rel = ""
    if not rel:
        rel = "logs/mm_orders.json"
    p = Path(rel)
    if not p.is_absolute():
        p = REPO_ROOT / p
    return p


def _resolve_mm_orders_freeze_signal_path(merged_cfg: dict) -> Path:
    """``mm_desk.orders_config_freeze_signal_path`` — {active, seq} toggles C++ reconcile snapshot."""
    rel = ""
    try:
        rel = str((merged_cfg.get("mm_desk") or {}).get("orders_config_freeze_signal_path") or "")
    except (AttributeError, TypeError):
        rel = ""
    if not rel:
        rel = "logs/mm_orders_freeze.json"
    p = Path(rel)
    if not p.is_absolute():
        p = REPO_ROOT / p
    return p


def _resolve_force_cancel_signal_path(merged_cfg: dict) -> Path:
    """``mm_desk.force_cancel_signal_path`` — append-only JSONL the desk writes and the C++ trading
    loop drains (claim-by-rename) each ~1s. One line per force-cancel:
    ``{"ax_symbol","stack_id","reason","ts_ms"}``.

    This is the desk→C++ recovery channel for *orphan* stacks: rows that vanished from
    ``orders.json`` (desk desired-state) but still rest on the venue and are tracked by C++ in
    ``mm_orders.json`` / OrderManager. Python cannot cancel those directly because it only has the
    C++ *local* order ids (``mm_orders.json`` sides.*.order_id), not the exchange OIDs — so it hands
    ``(ax_symbol, stack_id)`` to C++, which owns the local-id → exchange-OID mapping and cancels the
    legs on the single mover worker. Removing the stack from ``orders.json`` only makes C++ stop
    *moving* it; this signal is what actually cancels the resting legs."""
    rel = ""
    try:
        rel = str((merged_cfg.get("mm_desk") or {}).get("force_cancel_signal_path") or "")
    except (AttributeError, TypeError):
        rel = ""
    if not rel:
        rel = "logs/mm_force_cancel.jsonl"
    p = Path(rel)
    if not p.is_absolute():
        p = REPO_ROOT / p
    return p


def write_force_cancel_signal(
    merged_cfg: dict, ax_symbol: str, stack_id: str, reason: str = "desk_force_cancel"
) -> bool:
    """Append one force-cancel command for the C++ loop to drain. Best-effort; returns False on IO
    error or when neither ax_symbol nor stack_id is provided.

    Cross-process safety: the append is serialized by ``_MM_FORCE_CANCEL_LOCK`` within this process,
    and the C++ drain claims the file with an atomic rename before reading — so a line is either fully
    present in the claimed copy or lands in the next file, never torn or lost. This is a low-frequency,
    user-initiated path (one write per Cancel click), so no cross-process file lock is warranted."""
    ax = str(ax_symbol or "").strip()
    sid = str(stack_id or "").strip()
    if not ax and not sid:
        return False
    p = _resolve_force_cancel_signal_path(merged_cfg)
    line = json.dumps(
        {
            "ax_symbol": ax,
            "stack_id": sid,
            "reason": str(reason or "desk_force_cancel"),
            "ts_ms": int(time.time() * 1000),
        },
        separators=(",", ":"),
    )
    try:
        p.parent.mkdir(parents=True, exist_ok=True)
        with _MM_FORCE_CANCEL_LOCK:
            with open(p, "a", encoding="utf-8") as f:
                f.write(line + "\n")
                f.flush()
        return True
    except OSError:
        return False


def _find_stack_in_mm_active_orders(
    merged_cfg: dict, stack_id: str, ax_hint: str = ""
) -> dict | None:
    """Locate a stack row in ``mm_orders.json`` (C++ venue truth) by ``stack_id``/``request_id``,
    optionally scoped to ``ax_hint`` (case-insensitive). Returns the row dict or None. Used by the
    orphan-cancel path to confirm a stack is live on the venue and to recover its AX symbol when the
    caller did not supply one."""
    want = str(stack_id or "").strip()
    if not want:
        return None
    axw = str(ax_hint or "").strip().upper()
    data = load_mm_active_orders(merged_cfg)
    for row in data.get("orders") or []:
        if not isinstance(row, dict):
            continue
        rid = str(row.get("stack_id") or row.get("request_id") or "").strip()
        if rid != want:
            continue
        if axw:
            rax = str(row.get("ax_symbol") or row.get("order_symbol") or "").strip().upper()
            if rax and rax != axw:
                continue
        return row
    return None


def write_orders_config_freeze(merged_cfg: dict, active: bool) -> tuple[bool, str, int]:
    """Signal C++ to snapshot live ``orders.json`` (active=True) or resume live file (False)."""
    p = _resolve_mm_orders_freeze_signal_path(merged_cfg)
    seq = int(time.time() * 1000)
    if seq <= 0:
        seq = 1
    doc = {"active": bool(active), "seq": seq}
    try:
        p.parent.mkdir(parents=True, exist_ok=True)
        _atomic_write_json(p, doc)
        return True, str(p), seq
    except Exception as e:
        return False, str(e), 0


def _empty_mm_orders_config() -> dict:
    return {"version": 1, "updated_ms": 0, "gen": 0, "stacks": [], "products": {}}


def load_mm_orders_config(merged_cfg: dict) -> dict:
    """Best-effort read of ``orders.json``. Always returns a dict shaped
    like ``{version, updated_ms, products{}}`` — missing/empty/corrupt files
    degrade to an empty config rather than raising, so the GUI continues
    rendering an empty state instead of 500-ing on a freshly-created repo."""
    p = _resolve_mm_orders_config_path(merged_cfg)
    if not p.is_file():
        return _empty_mm_orders_config()
    try:
        raw = p.read_text(encoding="utf-8")
    except OSError:
        return _empty_mm_orders_config()
    if not raw.strip():
        return _empty_mm_orders_config()
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return _empty_mm_orders_config()
    if not isinstance(data, dict):
        return _empty_mm_orders_config()
    if "stacks" not in data or not isinstance(data.get("stacks"), list):
        data["stacks"] = []
    if "products" not in data or not isinstance(data.get("products"), dict):
        data["products"] = {}
    # Generation counter (optimistic-concurrency token). Carried through a caller's
    # read-modify-write so save_mm_orders_config can compare-and-swap: a save may only
    # land on top of the exact generation it was loaded from. See save_mm_orders_config.
    try:
        data["gen"] = int(data.get("gen", 0) or 0)
    except (TypeError, ValueError):
        data["gen"] = 0
    return data


# Refuse to overwrite orders.json with a doc that drops >= this many stacks
# vs what is currently on disk, unless the caller explicitly authorises the
# shrink (clear-all / dedupe). This is the desk-side half of the "orders wiped
# out of nowhere" fix (2026-07-14): a submit/save must MERGE into the existing
# desired-state, never clobber the sibling stacks down to only the one being
# edited. See the C++ reconcile guardrail (examples/main.cpp
# MM_RECONCILE_MASS_TEARDOWN_*) for the engine-side backstop.
_ORDERS_JSON_MASS_SHRINK_MIN = 2


def _mm_orders_stack_count(doc: object) -> int:
    """Best-effort count of root ``stacks[]`` in an orders.json-shaped dict."""
    if not isinstance(doc, dict):
        return 0
    stacks = doc.get("stacks")
    return len(stacks) if isinstance(stacks, list) else 0


def _mm_orders_ondisk_probe(p: Path) -> dict:
    """Snapshot the current on-disk orders.json for the save-time preconditions.

    Returns ``{exists, readable, count, gen}``. ``readable`` is False only when the
    file EXISTS but could not be read or JSON-parsed (a transient/corrupt read) — the
    caller must never overwrite in that case, or a good file gets clobbered by a doc
    that was built on a silently-degraded (empty) load. A genuinely absent/empty file
    reports ``exists`` accordingly with ``readable=True``."""
    out = {"exists": False, "readable": True, "count": 0, "gen": 0}
    try:
        if not p.is_file():
            return out
    except OSError:
        return out
    out["exists"] = True
    try:
        raw = p.read_text(encoding="utf-8")
    except OSError:
        out["readable"] = False
        return out
    if not raw.strip():
        return out  # exists but empty → readable, count 0, gen 0
    try:
        prev = json.loads(raw)
    except json.JSONDecodeError:
        out["readable"] = False
        return out
    if isinstance(prev, dict):
        out["count"] = _mm_orders_stack_count(prev)
        try:
            out["gen"] = int(prev.get("gen", 0) or 0)
        except (TypeError, ValueError):
            out["gen"] = 0
    return out


def _orders_json_unique_tmp(base: str) -> str:
    """Per-write-unique temp path for orders.json.

    orders.json is written by several racing agents: this desk GUI (possibly more
    than one process) AND the C++ engine (clean-slate wipe + per-accept exchange-oid
    patch). A fixed ``orders.json.tmp`` shared by all of them lets two concurrent
    atomic writes collide on the same temp inode — a shorter document written over a
    longer one leaves the longer one's tail behind, so the final rename publishes a
    corrupt "valid-json + trailing garbage" file. A unique suffix keeps every atomic
    write isolated; os.replace stays atomic (last writer wins with a whole doc).
    """
    return f"{base}.tmp.{os.getpid()}.{time.time_ns()}.{threading.get_ident()}"


def save_mm_orders_config(
    merged_cfg: dict, doc: dict, allow_shrink: bool = False
) -> tuple[bool, str]:
    """Atomic-rename write of the desired-state config. Caller is expected
    to have built ``doc`` under ``_MM_ORDERS_CONFIG_LOCK`` so the read-
    modify-write window is closed even when two HTTP workers race on the
    same product.

    Three save-time preconditions protect the desired-state file (all skipped for
    an authorised ``allow_shrink`` clear/remove):

      1. NON-CLOBBER ON UNREADABLE DISK — if the on-disk file exists but cannot be
         read/parsed right now, REFUSE. Otherwise a doc built on a silently-degraded
         (empty) ``load_mm_orders_config`` would overwrite a perfectly good file.
      2. GENERATION CAS — the save may only land on top of the exact generation it
         was loaded from (``doc['gen'] == on-disk gen``). A mismatch means a foreign
         writer (a second desk instance, or a transient empty load) touched the file
         in between, so this doc is stale → REFUSE (caller must reload-and-merge).
         This is the direct fix for the "GUI serialized its 1-stack in-memory state
         over 12 live stacks" incident.
      3. MASS-SHRINK GUARD — refuse a write that drops ``_ORDERS_JSON_MASS_SHRINK_MIN``+
         stacks vs disk (belt-and-suspenders on top of the CAS).

    On success the written generation is bumped to ``on-disk gen + 1`` (monotonic)."""
    if not isinstance(doc, dict):
        return False, "doc must be a dict"
    p = _resolve_mm_orders_config_path(merged_cfg)
    probe = _mm_orders_ondisk_probe(p)
    if not allow_shrink:
        # (1) never overwrite a file we could not verify
        if probe["exists"] and not probe["readable"]:
            msg = (
                "refused write to orders.json: on-disk file exists but is currently "
                "unreadable/corrupt — refusing to overwrite (avoids clobbering live stacks "
                "on a transient read failure). Retry once the file reads cleanly."
            )
            print(f"[mm_live_desk] ORDERS_JSON_UNREADABLE_GUARD {msg}", flush=True)
            return False, msg
        # (2) generation compare-and-swap
        loaded_gen = 0
        try:
            loaded_gen = int(doc.get("gen", 0) or 0)
        except (TypeError, ValueError):
            loaded_gen = 0
        if probe["exists"] and probe["readable"] and loaded_gen != probe["gen"]:
            msg = (
                f"refused stale write to orders.json: this doc was loaded at generation "
                f"{loaded_gen} but on-disk generation is now {probe['gen']} "
                f"(on-disk stacks={probe['count']}). Another writer changed the file; "
                f"reload-and-merge before saving — never overwrite a newer generation."
            )
            print(f"[mm_live_desk] ORDERS_JSON_GEN_CAS_GUARD {msg}", flush=True)
            return False, msg
        # (3) mass-shrink guard
        new_count = _mm_orders_stack_count(doc)
        if probe["count"] - new_count >= _ORDERS_JSON_MASS_SHRINK_MIN:
            msg = (
                f"refused mass-shrink write to orders.json: on-disk stacks={probe['count']} "
                f"-> new stacks={new_count} (drop {probe['count'] - new_count} >= "
                f"{_ORDERS_JSON_MASS_SHRINK_MIN}). A submit/save must merge, not clobber "
                f"siblings; pass allow_shrink=True only for an intentional clear/remove."
            )
            print(f"[mm_live_desk] ORDERS_JSON_MASS_SHRINK_GUARD {msg}", flush=True)
            return False, msg
    try:
        p.parent.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        return False, f"mkdir: {exc}"
    doc.setdefault("version", 1)
    doc["updated_ms"] = int(time.time() * 1000)
    doc["gen"] = int(probe["gen"]) + 1  # monotonic bump for the next CAS
    tmp = _orders_json_unique_tmp(str(p))
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(doc, f, indent=2, sort_keys=False)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, str(p))
    except OSError as exc:
        try:
            os.remove(tmp)
        except OSError:
            pass
        return False, f"write: {exc}"
    return True, ""


# ─────────────────────────────────────────────────────────────────────────────
# Manual-order templates (templates.json)
#
# Persisted, reusable Place-order param sets. Stored at the project root in
# ``templates.json`` (git-ignored so it survives code upgrades). Schema is a
# flat map of instrument → list of templates, each carrying the full set of
# Place-order popup fields under ``params``::
#
#   {
#     "EURUSD-PERP": [
#       {
#         "name": "EURUSD-PERP_tmp1",
#         "params": {
#           "theo_source": "neon_fix",
#           "theo_venue_symbol": "EUR/USD",
#           "instrument_max_position": 200000,
#           "width_bps": 6, "order_size": 50000,
#           "adjust_position": 50000, "adjust_ticks": 1,
#           "min_theo_move_ticks_to_requote": 1, "max_reload_cycles": 0,
#           "quote_snapshot": 0, "pricer_snapshot": 0, "slope": 1
#         },
#         "created_ms": ..., "updated_ms": ...
#       }
#     ]
#   }
#
# Python is the sole writer; _MM_TEMPLATES_LOCK serialises read-modify-write so
# two concurrent saves never clobber each other. Templates are NEVER created
# automatically — only when the user explicitly clicks a save button in the GUI
# (the server simply persists what the client posts).
# ─────────────────────────────────────────────────────────────────────────────
# Fields the Place-order popup exposes once an instrument is selected. The stored
# ``params`` object carries exactly these (instrument itself is the map key).
_MM_TEMPLATE_PARAM_FIELDS = (
    "theo_source",
    "theo_venue_symbol",
    "instrument_max_position",
    "width_bps",
    "order_size",
    "adjust_position",
    "adjust_ticks",
    "min_theo_move_ticks_to_requote",
    "max_reload_cycles",
    "quote_snapshot",
    "pricer_snapshot",
    "slope",
)


def _resolve_mm_templates_path(merged_cfg: dict) -> Path:
    """``mm_desk.mm_templates_path`` — manual-order template store.

    Default ``templates.json`` at the project root. It is git-ignored, so a
    ``git pull`` / rebuild never touches it (durable user data, survives code
    upgrades). An end user may set ``mm_desk.mm_templates_path`` to an absolute
    path entirely outside the repo. Relative paths resolve from the repo root."""
    rel = ""
    try:
        rel = str((merged_cfg.get("mm_desk") or {}).get("mm_templates_path") or "")
    except (AttributeError, TypeError):
        rel = ""
    if not rel:
        rel = "templates.json"
    p = Path(rel)
    if not p.is_absolute():
        p = REPO_ROOT / p
    return p


def load_mm_templates(merged_cfg: dict) -> dict:
    """Best-effort read of templates.json → flat {instrument: [templates]} map.
    Missing / empty / malformed all degrade to an empty map (never raises)."""
    p = _resolve_mm_templates_path(merged_cfg)
    if not p.is_file():
        return {}
    try:
        raw = p.read_text(encoding="utf-8")
    except OSError:
        return {}
    if not raw.strip():
        return {}
    try:
        data = json.loads(raw)
    except (json.JSONDecodeError, ValueError):
        return {}
    if not isinstance(data, dict):
        return {}
    # Keep only well-formed instrument → list-of-template-dicts entries.
    clean: dict = {}
    for k, v in data.items():
        if isinstance(v, list):
            clean[str(k)] = [t for t in v if isinstance(t, dict)]
    return clean


def ensure_mm_templates_file(merged_cfg: dict) -> None:
    """Create an empty templates.json ({}) if it does not exist yet. Best-effort:
    a failure to create is non-fatal (load_mm_templates already tolerates a
    missing file)."""
    p = _resolve_mm_templates_path(merged_cfg)
    if p.is_file():
        return
    try:
        _atomic_write_json(p, {})
    except OSError:
        pass


def save_mm_templates(merged_cfg: dict, doc: dict) -> tuple[bool, str]:
    """Atomic write of the {instrument: [templates]} map. Caller builds ``doc``
    under _MM_TEMPLATES_LOCK to close the read-modify-write window."""
    if not isinstance(doc, dict):
        return False, "doc must be a dict"
    p = _resolve_mm_templates_path(merged_cfg)
    try:
        _atomic_write_json(p, doc)
    except OSError as exc:
        return False, f"write: {exc}"
    return True, ""


def _mm_template_next_name(doc: dict, instrument: str) -> str:
    """Default template name: ``{INSTRUMENT}_tmp{N}`` with the lowest free N."""
    key = str(instrument or "").strip().upper()
    arr = doc.get(key) if isinstance(doc, dict) else None
    if not isinstance(arr, list):
        arr = []
    existing = {str((t or {}).get("name") or "") for t in arr if isinstance(t, dict)}
    i = 1
    while f"{key}_tmp{i}" in existing:
        i += 1
    return f"{key}_tmp{i}"


def _mm_sanitize_template_params(params_in: object) -> dict:
    """Pick exactly the known popup fields from an incoming params object so the
    stored record can't accumulate arbitrary keys."""
    src = params_in if isinstance(params_in, dict) else {}
    return {k: src.get(k) for k in _MM_TEMPLATE_PARAM_FIELDS}


def mm_templates_upsert(
    merged_cfg: dict, instrument: str, name: str, params: object, old_name: str = ""
) -> tuple[bool, dict]:
    """Create or update a template (upsert by name within its instrument).

    Returns (ok, info) where info has ``template`` (the saved record) and
    ``templates`` (the full map) on success, or ``error`` on failure.
    """
    key = str(instrument or "").strip().upper()
    if not key:
        return False, {"error": "instrument is required"}
    with _MM_TEMPLATES_LOCK:
        doc = load_mm_templates(merged_cfg)
        arr = doc.get(key)
        if not isinstance(arr, list):
            arr = []
        final_name = str(name or "").strip() or _mm_template_next_name(doc, key)
        now_ms = int(time.time() * 1000)
        # Remove the row we are replacing (match old_name first, else final_name).
        match_name = str(old_name or "").strip() or final_name
        kept = []
        prev_created = now_ms
        for t in arr:
            tname = str((t or {}).get("name") or "")
            if tname == match_name or tname == final_name:
                if tname == match_name:
                    prev_created = int((t or {}).get("created_ms") or now_ms)
                continue
            kept.append(t)
        rec = {
            "name": final_name,
            "params": _mm_sanitize_template_params(params),
            "created_ms": prev_created,
            "updated_ms": now_ms,
        }
        kept.append(rec)
        doc[key] = kept
        ok, err = save_mm_templates(merged_cfg, doc)
        if not ok:
            return False, {"error": err}
        return True, {"template": rec, "templates": doc}


def mm_templates_delete(merged_cfg: dict, instrument: str, name: str) -> tuple[bool, dict]:
    key = str(instrument or "").strip().upper()
    nm = str(name or "").strip()
    if not key or not nm:
        return False, {"error": "instrument and name are required"}
    with _MM_TEMPLATES_LOCK:
        doc = load_mm_templates(merged_cfg)
        arr = doc.get(key)
        if isinstance(arr, list):
            new_arr = [t for t in arr if str((t or {}).get("name") or "") != nm]
            if new_arr:
                doc[key] = new_arr
            else:
                doc.pop(key, None)
        ok, err = save_mm_templates(merged_cfg, doc)
        if not ok:
            return False, {"error": err}
        return True, {"templates": doc}


def _orders_json_is_abandoned_pending_row(row: object) -> bool:
    if not isinstance(row, dict):
        return False
    if str(row.get("submit_status") or "").strip().lower() != "pending":
        return False
    bid = str(row.get("bid_exchange_oid") or "").strip()
    ask = str(row.get("ask_exchange_oid") or "").strip()
    return not bid and not ask


def _orders_json_pick_best_stack_row(rows: list[dict]) -> dict:
    """When multiple root stacks share one ax_symbol, keep the most complete row."""

    def score(r: dict) -> tuple[int, int, int, int]:
        bid = str(r.get("bid_exchange_oid") or "").strip()
        ask = str(r.get("ask_exchange_oid") or "").strip()
        legs = (1 if bid else 0) + (1 if ask else 0)
        ds = 1 if r.get("desk_seeded") is True else 0
        active = 1 if str(r.get("submit_status") or "").strip().lower() == "active" else 0
        try:
            cm = int(r.get("created_ms") or 0)
        except (TypeError, ValueError):
            cm = 0
        return (legs, ds, active, cm)

    return max(rows, key=score)


def _coerce_mm_stack_row_width_bps(row: dict) -> dict:
    """Normalize spread width for orders.json rows.

    C++ ``reconcileMmReqStrategiesFromOrdersJson`` reads ``width_bps`` (with legacy
    ``width_ticks`` / ``width`` fallbacks). If ``width_bps`` is missing but an alias is
    set, or if only a float-like value is present upstream, the engine can otherwise
    see ``0`` and fall back to the global MM spread (the EURUSD "40 tick" symptom).
    """
    out = dict(row)
    for k in ("width_bps", "width_ticks", "width"):
        if isinstance(out.get(k), bool):
            out.pop(k, None)

    def _try_int(x: object) -> int | None:
        if x is None or x == "":
            return None
        if isinstance(x, bool):
            return None
        try:
            return int(float(x))
        except (TypeError, ValueError):
            return None

    cur = _try_int(out.get("width_bps"))
    if cur is not None and cur >= 1:
        out["width_bps"] = cur
        return out
    for k in ("width_ticks", "width"):
        alt = _try_int(out.get(k))
        if alt is not None and alt >= 1:
            out["width_bps"] = alt
            return out
    # Width may live only under ``params`` (e.g. mm_orders.json-style rows); lift to root
    # so C++ ``mmOrdersJsonStackWidthBps`` and desk round-trips see a positive width_bps.
    p = out.get("params")
    if isinstance(p, dict):
        pc = _try_int(p.get("width_bps"))
        if pc is not None and pc >= 1:
            out["width_bps"] = pc
            return out
        for k in ("width_ticks", "width"):
            alt = _try_int(p.get(k))
            if alt is not None and alt >= 1:
                out["width_bps"] = alt
                return out
    if cur is not None:
        out["width_bps"] = cur
    return out


def orders_json_apply_stack_row_dual_view(merged_cfg: dict, row: dict) -> tuple[bool, str]:
    """Upsert *row* into root ``stacks[]`` and ``products[ax].stacks`` keyed by ``stack_id``.

    Multiple desk stacks must coexist on the same ``ax_symbol`` — that is the entire reason
    each spawned strategy is named ``mm_req_<AX>_<stack_id>``. Earlier behaviour collapsed
    every existing row for *ax_symbol* into a single entry before appending the new one
    (and rewrote ``products[ax]["stacks"]`` to a one-element list), which made the C++
    ``reconcileMmReqStrategiesFromOrdersJson`` poller see "old stack gone, new stack
    present" and tear down the prior strategy via ``mmTearDownByStackId`` — REST-cancelling
    its working bid+ask. That looked to the operator like "I placed a second pair and both
    pairs got cancelled automatically".

    The replacement semantics (upsert keyed on ``stack_id``):
      * Root ``stacks[]``: keep every row whose ``stack_id`` differs from this one (across
        any AX); replace the matching ``stack_id`` row in place; or append if new.
      * ``products[ax]["stacks"]``: same upsert applied to that AX's local list — preserves
        sibling rows on the same AX instead of clobbering them.
    """
    if not isinstance(row, dict):
        return False, "row must be a dict"
    row = _coerce_mm_stack_row_width_bps(row)
    ax = str(row.get("ax_symbol") or "").strip()
    sid = str(row.get("stack_id") or row.get("id") or "").strip()
    if not ax or not sid:
        return False, "ax_symbol and stack_id/id required"
    row_out = dict(row)
    row_out["stack_id"] = sid
    row_out["id"] = sid
    row_out["ax_symbol"] = ax
    with _MM_ORDERS_CONFIG_LOCK:
        d = load_mm_orders_config(merged_cfg)
        root = d.setdefault("stacks", [])
        if not isinstance(root, list):
            root = []
            d["stacks"] = root
        # Upsert root view by stack_id. Sibling rows on this AX (and on every other AX)
        # are preserved so multi-stack multi-AX coexistence works.
        d["stacks"] = [
            r for r in root
            if not (isinstance(r, dict) and _desk_stack_id_matches_row(r, sid))
        ]
        d["stacks"].append(dict(row_out))
        products = d.setdefault("products", {})
        if not isinstance(products, dict):
            products = {}
            d["products"] = products
        prod = products.setdefault(ax, {})
        if not isinstance(prod, dict):
            prod = {}
            products[ax] = prod
        if "mm_move_all_enabled" not in prod:
            prod["mm_move_all_enabled"] = True
        try:
            row_mp = int(row_out.get("max_position") or 0)
        except (TypeError, ValueError):
            row_mp = 0
        floor_mp = _instrument_max_position_floor_from_orders_doc(d, ax, exclude_stack_id=sid)
        if row_mp > 0 and floor_mp > 0 and row_mp < floor_mp:
            return (
                False,
                f"Instrument {ax}: max_position is already {floor_mp}; "
                f"cannot save stack with max_position={row_mp}",
            )
        if row_mp > 0:
            prod["max_position"] = max(floor_mp, row_mp)
        for k in (
            "theo_source",
            "theo_venue_symbol",
            "reference_fix_symbol",
            "order_symbol",
            "max_reload_cycles",
            "mm_move_all_enabled",
        ):
            if k in row_out and row_out[k] is not None and str(row_out[k]) != "":
                prod[k] = row_out[k]
        # Upsert per-product view by stack_id (preserve sibling stacks on the same AX).
        existing_prod_stacks = prod.get("stacks")
        if not isinstance(existing_prod_stacks, list):
            existing_prod_stacks = []
        prod["stacks"] = [
            s for s in existing_prod_stacks
            if not (isinstance(s, dict) and _desk_stack_id_matches_row(s, sid))
        ]
        prod["stacks"].append(dict(row_out))
        return save_mm_orders_config(merged_cfg, d)


def _orders_json_remove_stack_id_everywhere(doc: dict, stack_id: str, ax_hint: str = "") -> int:
    """Remove *stack_id* from root ``stacks`` and from ``products[*].stacks``. Returns rows removed."""
    want = str(stack_id or "").strip()
    if not want:
        return 0
    removed = 0
    root = doc.get("stacks")
    if isinstance(root, list):
        kept_r = [s for s in root if not (isinstance(s, dict) and _desk_stack_id_matches_row(s, want))]
        removed += len(root) - len(kept_r)
        doc["stacks"] = kept_r
    products = doc.get("products") if isinstance(doc.get("products"), dict) else {}
    axes: list[str] = []
    if str(ax_hint or "").strip():
        pk = _orders_json_resolve_product_key(products, ax_hint)
        if pk:
            axes.append(pk)
        else:
            axes.append(str(ax_hint).strip())
    if not axes:
        axes = list(products.keys())
    emptied: list[str] = []
    for ax in axes:
        prod = products.get(ax)
        if not isinstance(prod, dict):
            continue
        stacks = prod.get("stacks")
        if not isinstance(stacks, list):
            continue
        kept = [s for s in stacks if not (isinstance(s, dict) and _desk_stack_id_matches_row(s, want))]
        if len(kept) != len(stacks):
            removed += len(stacks) - len(kept)
            prod["stacks"] = kept
            if not kept:
                emptied.append(ax)
    for ax in emptied:
        try:
            del products[ax]
        except KeyError:
            pass
    return removed


def orders_json_reconcile_dual_views_at_startup(merged_cfg: dict) -> dict:
    """Heal root vs ``products`` drift; drop abandoned pending rows; dedupe root by ``stack_id``. Persists if needed.

    NOTE: dedup is keyed on ``stack_id``, NOT ``ax_symbol``. Multiple distinct stacks on the
    same instrument are a supported, first-class configuration (each spawns its own
    ``mm_req_<AX>_<stack_id>`` strategy) and MUST coexist. A previous version collapsed every
    same-AX row to a single "best" row here; on the next ~1 Hz C++ poll that made
    ``reconcileMmReqStrategiesFromOrdersJson`` see the sibling stack as no-longer-desired and
    tear it down — REST-cancelling its live bid+ask. That was the "I placed a second pair on
    the same instrument and the first pair died without a fill" symptom. Only TRUE duplicates
    (two rows carrying the identical ``stack_id``) are collapsed now.
    """
    stats = {
        "removed_abandoned": 0,
        "root_deduped": 0,
        "products_resynced": 0,
        "saved": False,
    }
    with _MM_ORDERS_CONFIG_LOCK:
        d = load_mm_orders_config(merged_cfg)
        changed = False
        root = d.setdefault("stacks", [])
        if not isinstance(root, list):
            root = []
            d["stacks"] = root
        new_root: list = []
        for r in root:
            if isinstance(r, dict) and _orders_json_is_abandoned_pending_row(r):
                stats["removed_abandoned"] += 1
                changed = True
                continue
            new_root.append(r)
        products = d.setdefault("products", {})
        if not isinstance(products, dict):
            products = {}
            d["products"] = products
        for _pk, prod in list(products.items()):
            if not isinstance(prod, dict):
                continue
            st_list = prod.get("stacks")
            if not isinstance(st_list, list):
                continue
            kept_p: list = []
            for r in st_list:
                if isinstance(r, dict) and _orders_json_is_abandoned_pending_row(r):
                    stats["removed_abandoned"] += 1
                    changed = True
                    continue
                kept_p.append(r)
            if len(kept_p) != len(st_list):
                prod["stacks"] = kept_p
                changed = True
        d["stacks"] = new_root
        # Dedupe by stack_id (NOT ax_symbol) so distinct sibling stacks on the same
        # instrument are preserved. Collapse only rows that carry the identical stack_id,
        # keeping the most-complete one. Original row order is preserved (each stack_id is
        # emitted at its first occurrence). Rows without a usable stack_id/id are kept
        # verbatim (the C++ poller ignores them, but we never silently drop operator data).
        by_sid: dict[str, list[dict]] = {}
        for r in d["stacks"]:
            if isinstance(r, dict):
                sid = str(r.get("stack_id") or r.get("id") or "").strip()
                if sid:
                    by_sid.setdefault(sid, []).append(r)
        emitted: set[str] = set()
        deduped: list = []
        for r in d["stacks"]:
            if not isinstance(r, dict):
                deduped.append(r)
                continue
            sid = str(r.get("stack_id") or r.get("id") or "").strip()
            if not sid:
                deduped.append(dict(r))
                continue
            if sid in emitted:
                continue  # duplicate stack_id already collapsed at its first occurrence
            emitted.add(sid)
            rows = by_sid[sid]
            if len(rows) == 1:
                deduped.append(dict(rows[0]))
            else:
                best = _orders_json_pick_best_stack_row(rows)
                deduped.append(dict(best))
                stats["root_deduped"] += len(rows) - 1
                changed = True
        d["stacks"] = deduped
        root_axes = set()
        for r in deduped:
            if isinstance(r, dict):
                axs = str(r.get("ax_symbol") or "").strip()
                if axs:
                    root_axes.add(axs)
        for ax_sym in sorted(root_axes):
            rows_for_ax = [
                dict(x) for x in deduped if isinstance(x, dict) and str(x.get("ax_symbol") or "").strip() == ax_sym
            ]
            prod = products.setdefault(ax_sym, {})
            if not isinstance(prod, dict):
                prod = {}
                products[ax_sym] = prod
            old_stacks = prod.get("stacks")
            old_json = json.dumps(old_stacks, sort_keys=True) if isinstance(old_stacks, list) else ""
            new_json = json.dumps(rows_for_ax, sort_keys=True)
            if old_json != new_json:
                prod["stacks"] = rows_for_ax
                stats["products_resynced"] += 1
                changed = True
        for pk, prod in list(products.items()):
            if not isinstance(prod, dict):
                continue
            if pk in root_axes:
                continue
            st_list = prod.get("stacks")
            if isinstance(st_list, list) and len(st_list) > 0:
                prod["stacks"] = []
                stats["products_resynced"] += 1
                changed = True
        if changed:
            # Dedupe/resync removes only provable duplicate stack_ids (never a unique
            # stack), so authorise the shrink past the mass-shrink guard.
            ok_w, err_w = save_mm_orders_config(merged_cfg, d, allow_shrink=True)
            if not ok_w:
                return {**stats, "saved": False, "error": err_w}
            stats["saved"] = True
    return stats


def _orders_json_boolish(val: object, default: bool = True) -> bool:
    if val is None:
        return default
    if isinstance(val, bool):
        return val
    if isinstance(val, (int, float)) and not isinstance(val, bool):
        return bool(val)
    s = str(val).strip().lower()
    if s in ("0", "false", "off", "no", ""):
        return False
    if s in ("1", "true", "on", "yes"):
        return True
    return default


def _norm_orders_ax(s: str) -> str:
    return str(s or "").strip().upper()


def _enrich_live_orders_with_orders_json_move_flags(
    mm_orders_cfg: dict, live: dict | None
) -> dict:
    """Attach ``mm_move_*`` from ``orders.json`` to C++ ``mm_orders.json`` rows and append
    ghost rows for stacks that exist only in desired state (paused / not yet first-ack)."""
    if not isinstance(live, dict):
        live = {"updated_ms": 0, "orders": [], "stale": True}
    out = dict(live)
    orders_in = out.get("orders")
    orders: list = list(orders_in) if isinstance(orders_in, list) else []
    products = (mm_orders_cfg or {}).get("products")
    if not isinstance(products, dict):
        products = {}
    stacks_root = (mm_orders_cfg or {}).get("stacks")
    if isinstance(stacks_root, list) and stacks_root:
        for stack in stacks_root:
            if not isinstance(stack, dict):
                continue
            ax0 = str(stack.get("ax_symbol") or "").strip()
            if not ax0:
                continue
            slot = products.setdefault(
                ax0,
                {
                    "mm_move_all_enabled": True,
                    "order_symbol": str(stack.get("order_symbol") or ax0).strip(),
                    "reference_fix_symbol": str(stack.get("reference_fix_symbol") or "").strip(),
                    "theo_venue_symbol": str(stack.get("theo_venue_symbol") or "").strip(),
                    "theo_source": str(stack.get("theo_source") or "").strip(),
                    "stacks": [],
                },
            )
            if not isinstance(slot.get("stacks"), list):
                slot["stacks"] = []
            slot["stacks"].append(stack)
    if not products:
        out["orders"] = orders
        return out

    by_key: dict[tuple[str, str], int] = {}
    for i, o in enumerate(orders):
        if not isinstance(o, dict):
            continue
        ax = _norm_orders_ax(str(o.get("ax_symbol") or ""))
        sid = str(o.get("stack_id") or o.get("request_id") or "").strip()
        if ax and sid:
            by_key[(ax, sid)] = i

    def _fnum(x: object, d: float = 0.0) -> float:
        try:
            return float(x)
        except (TypeError, ValueError):
            return d

    def _inum(x: object, d: int = 0) -> int:
        try:
            return int(x)
        except (TypeError, ValueError):
            return d

    seen_json_keys: set[tuple[str, str]] = set()
    for pk, prod in products.items():
        if not isinstance(prod, dict):
            continue
        prod_all = _orders_json_boolish(prod.get("mm_move_all_enabled"), True)
        stacks = prod.get("stacks")
        if not isinstance(stacks, list):
            continue
        ax_key = _norm_orders_ax(str(pk))
        for stack in stacks:
            if not isinstance(stack, dict):
                continue
            sid = str(stack.get("id") or "").strip()
            if not sid:
                continue
            seen_json_keys.add((ax_key, sid))
            stack_move = _orders_json_boolish(stack.get("mm_move_enabled"), True)
            eff = prod_all and stack_move
            tpl = (ax_key, sid)
            if tpl in by_key:
                row = orders[by_key[tpl]]
                if isinstance(row, dict):
                    row["mm_move_all_enabled"] = prod_all
                    row["mm_move_enabled"] = stack_move
                    row["mm_move_effective"] = eff
                continue
            bp = _fnum(stack.get("bid_price"), 0.0)
            ap = _fnum(stack.get("ask_price"), 0.0)
            bq = _inum(
                stack.get("bid_qty")
                if stack.get("bid_qty") is not None
                else stack.get("bid_quantity"),
                0,
            )
            aq = _inum(
                stack.get("ask_qty")
                if stack.get("ask_qty") is not None
                else stack.get("ask_quantity"),
                0,
            )
            oid_b = str(stack.get("bid_exchange_oid") or "").strip()
            oid_a = str(stack.get("ask_exchange_oid") or "").strip()
            ghost = {
                "request_id": sid,
                "stack_id": sid,
                "ax_symbol": str(pk).strip() or ax_key,
                "order_symbol": str(prod.get("order_symbol") or pk or "").strip(),
                "reference_fix_symbol": str(prod.get("reference_fix_symbol") or "").strip(),
                "theo_venue_symbol": str(prod.get("theo_venue_symbol") or "").strip(),
                "theo_source": str(prod.get("theo_source") or "").strip(),
                "ghost_from_orders_json": True,
                "mm_move_all_enabled": prod_all,
                "mm_move_enabled": stack_move,
                "mm_move_effective": eff,
                "blocked_by_feed": False,
                "block_reason": "",
                "first_ack_ms": 0,
                "fills_count": 0,
                "last_fill_id": "",
                "last_fill_ms": 0,
                "strategy_name": "",
                "params": {
                    "id": sid,
                "width_bps": _inum(
                    stack.get("width_bps")
                    if stack.get("width_bps") is not None
                    else stack.get("width_ticks") or stack.get("width"),
                    0,
                ),
                    "order_size": _inum(stack.get("order_size"), 0),
                    "max_position": _inum(stack.get("max_position"), 0),
                    "adjust_position": _inum(stack.get("adjust_position"), 0),
                    "adjust_ticks": _inum(stack.get("adjust_ticks"), 0),
                    "min_theo_move_ticks_to_requote": _inum(
                        stack.get("min_theo_move_ticks_to_requote")
                        or stack.get("min_drift_ticks"),
                        0,
                    ),
                    "max_reload_cycles": _inum(stack.get("max_reload_cycles"), 0),
                },
                "sides": {
                    "bid": {
                        "order_id": 0,
                        "price": bp,
                        "qty": float(bq),
                        "exchange_oid": oid_b or None,
                    },
                    "ask": {
                        "order_id": 0,
                        "price": ap,
                        "qty": float(aq),
                        "exchange_oid": oid_a or None,
                    },
                },
            }
            orders.append(ghost)
            by_key[tpl] = len(orders) - 1

    for i, o in enumerate(orders):
        if not isinstance(o, dict):
            continue
        ax = _norm_orders_ax(str(o.get("ax_symbol") or ""))
        sid = str(o.get("stack_id") or o.get("request_id") or "").strip()
        if not ax or not sid:
            continue
        if (ax, sid) not in seen_json_keys:
            continue
        prod_key = _orders_json_resolve_product_key(products, ax)
        if prod_key is None:
            continue
        prod = products.get(prod_key)
        if not isinstance(prod, dict):
            continue
        prod_all = _orders_json_boolish(prod.get("mm_move_all_enabled"), True)
        stack_move = True
        for row in prod.get("stacks") or []:
            if isinstance(row, dict) and _desk_stack_id_matches_row(row, sid):
                stack_move = _orders_json_boolish(row.get("mm_move_enabled"), True)
                break
        o["mm_move_all_enabled"] = prod_all
        o["mm_move_enabled"] = stack_move
        o["mm_move_effective"] = prod_all and stack_move

    out["orders"] = orders
    return out


def _empty_mm_orders_active() -> dict:
    return {"updated_ms": 0, "orders": [], "stale": True}


def load_mm_active_orders(merged_cfg: dict) -> dict:
    """Read ``mm_orders.json`` (C++-owned). Tags stale==True when the file is
    missing or older than 5s — the binary writes every ~1s, so 5s is a safe
    ceiling that survives a single missed write while still flagging a dead
    trading_client."""
    path = _resolve_mm_orders_active_path(merged_cfg)
    if not path.is_file():
        return _empty_mm_orders_active()
    try:
        raw = path.read_text(encoding="utf-8")
        data = json.loads(raw)
    except (OSError, json.JSONDecodeError):
        return _empty_mm_orders_active()
    if not isinstance(data, dict):
        return _empty_mm_orders_active()
    if not isinstance(data.get("orders"), list):
        data["orders"] = []
    now_ms = int(time.time() * 1000)
    upd = int(data.get("updated_ms") or 0)
    data["stale"] = (upd <= 0) or ((now_ms - upd) > 5000)
    return data


def fix_symbol_canonical_py(s: str) -> str:
    """Match C++ fixSymbolCanonical: alnum only, upper case (e.g. USD/JPY → USDJPY)."""
    t = str(s or "").strip()
    return "".join(ch for ch in t if ch.isalnum()).upper()


def _normalize_theo_source(
    theo_source: str | None, default_provider: str, _row: dict | None = None
) -> str:
    raw = (str(theo_source or "").strip() or str(default_provider or "neon_fix")).lower()
    t = raw
    if t in ("hl", "hyper_liquid", "hyperliquid"):
        return "hyperliquid"
    if t in ("neon", "neon_fix", "fix_neon", "fix_integral", "integral_fix"):
        return "neon_fix"
    if t in ("mettraders", "met_traders", "met-traders", "cme", "cme_gold", "cme_metal"):
        return "mettraders"
    return t


def _desk_is_multi_from_config(dc: dict) -> bool:
    mm = dc.get("market_maker")
    if not isinstance(mm, dict):
        return False
    if bool(mm.get("multi_theo")):
        return True
    inst = mm.get("instruments")
    if not isinstance(inst, list) or not inst:
        return False
    ext = (dc.get("external_feed") or {}) if isinstance(dc.get("external_feed"), dict) else {}
    dprov = str(ext.get("provider") or "neon_fix").lower()
    kinds: set[str] = set()
    for el in inst:
        if not isinstance(el, dict):
            continue
        t = (el.get("theo_source") or "").strip()
        r = _normalize_theo_source(t, dprov, el)
        kinds.add(r)
    return len(kinds) > 1


def _hyperliquid_all_mids_url(base: str) -> str:
    return f"{base.rstrip('/')}/info"


def _hyperliquid_l2_book_url(base: str) -> str:
    return f"{base.rstrip('/')}/info"


def _hyperliquid_symbol_candidates(raw_symbol: str) -> list[str]:
    s = str(raw_symbol or "").strip().upper().replace(" ", "")
    if not s:
        return []
    if s.endswith("-PERP"):
        s = s[: -len("-PERP")]
    canonical = "".join(ch for ch in s if ch.isalnum())
    if canonical.endswith("PERP") and len(canonical) > 4:
        canonical = canonical[:-4]
    out: list[str] = []
    seen: set[str] = set()

    def add(v: str) -> None:
        vv = str(v or "").strip().upper()
        if not vv or vv in seen:
            return
        seen.add(vv)
        out.append(vv)

    # Real-world perps live on the HIP-3 'xyz' sub-DEX with names like
    # HIP-3 xyz dex (e.g. xyz:JPY, xyz:GOLD, xyz:SILVER, xyz:CL, xyz:SP500).
    # Probe these FIRST for FX/commodity refs so we don't shadow-match HL
    # altcoins (e.g. the $0.38 'SPX' meme coin in default allMids).
    aliases = {
        "USDJPY": ("xyz:JPY", "xyz:USDJPY", "USDJPY", "JPY"),
        "JPY": ("xyz:JPY", "xyz:USDJPY", "JPY", "USDJPY"),
        "USDMXN": ("xyz:MXN", "xyz:USDMXN", "USDMXN", "MXN"),
        "MXN": ("xyz:MXN", "xyz:USDMXN", "MXN", "USDMXN"),
        "XAU": ("xyz:GOLD", "GOLD", "XAU"),
        "GOLD": ("xyz:GOLD", "GOLD", "XAU"),
        "XAG": ("xyz:SILVER", "SILVER", "XAG"),
        "SILVER": ("xyz:SILVER", "SILVER", "XAG"),
        "WTI": ("xyz:WTIOIL", "xyz:CL", "WTI", "WTIOIL"),
        "WTIOIL": ("xyz:WTIOIL", "xyz:CL", "WTIOIL", "WTI"),
        "CL": ("xyz:CL", "xyz:WTIOIL", "CL", "WTI", "WTIOIL"),
        "BRENT": ("xyz:BRENTOIL", "BRENT"),
        "BRENTOIL": ("xyz:BRENTOIL", "BRENT", "BRENTOIL"),
        "NATGAS": ("xyz:NG", "NATGAS", "GAS", "NG"),
        "NATURALGAS": ("xyz:NG", "NATGAS", "GAS", "NG"),
        "NGAS": ("xyz:NG", "NATGAS", "GAS", "NG"),
        "NG": ("xyz:NG", "NG", "NATGAS", "GAS"),
        "GAS": ("xyz:NG", "GAS", "NATGAS", "NG"),
        "SPX": ("xyz:SP500", "SP500"),
        "SP500": ("xyz:SP500", "SP500"),
        "AAPL": ("xyz:AAPL", "AAPL"),
        "TSM": ("xyz:TSM", "TSM"),
        "META": ("xyz:META", "META"),
        "MSFT": ("xyz:MSFT", "MSFT"),
        "ASML": ("xyz:ASML", "ASML"),
        "AVGO": ("xyz:AVGO", "AVGO")
    }
    if canonical in aliases:
        # Authoritative alias list — do NOT add the raw `SPX`/`GOLD`/... fallback
        # which would shadow-match HL altcoins (e.g. the $0.38 'SPX' meme coin
        # or 'GOLD' tokens in the default allMids dex).
        for key in aliases[canonical]:
            add(key)
    else:
        add(s)
        add(canonical)
    return out


def _hyperliquid_parse_all_mids(payload: object) -> dict[str, float]:
    src = payload
    if isinstance(payload, dict) and isinstance(payload.get("mids"), dict):
        src = payload.get("mids")
    if not isinstance(src, dict):
        return {}
    out: dict[str, float] = {}
    for k, v in src.items():
        if k is None:
            continue
        fv = _to_float(v)
        if fv is None or not math.isfinite(fv) or fv <= 0:
            continue
        out[str(k).strip().upper()] = float(fv)
    return out


def _hyperliquid_fetch_all_mids(base: str, timeout_sec: float) -> tuple[dict[str, float], float]:
    """Fetch allMids and merge the HIP-3 'xyz' sub-DEX (real-world FX/commodity perps).

    The 'xyz' dex (deployer-managed) lists xyz:JPY, xyz:GOLD, xyz:SILVER,
    xyz:CL (WTI), xyz:SP500, xyz:BRENTOIL, etc. — these are NOT in the default
    allMids and require an explicit ``dex`` parameter. Failure of the xyz call
    is non-fatal: we still return the base mids.
    """
    url = _hyperliquid_all_mids_url(base)
    t0 = time.monotonic()
    req = urllib.request.Request(url, method="POST")
    req.add_header("Content-Type", "application/json")
    data = json.dumps({"type": "allMids"}).encode("utf-8")
    with urllib.request.urlopen(req, data=data, context=_ssl_ctx(), timeout=timeout_sec) as resp:
        raw = json.loads(resp.read().decode("utf-8"))
    merged = _hyperliquid_parse_all_mids(raw)
    try:
        xreq = urllib.request.Request(url, method="POST")
        xreq.add_header("Content-Type", "application/json")
        xdata = json.dumps({"type": "allMids", "dex": "xyz"}).encode("utf-8")
        with urllib.request.urlopen(xreq, data=xdata, context=_ssl_ctx(), timeout=timeout_sec) as xresp:
            xraw = json.loads(xresp.read().decode("utf-8"))
        for k, v in _hyperliquid_parse_all_mids(xraw).items():
            merged[k] = v
    except Exception:
        # xyz fetch is best-effort; base allMids already populated `merged`.
        pass
    dt_ms = (time.monotonic() - t0) * 1000.0
    return merged, dt_ms


def _hyperliquid_pick_mid_for_symbol(mids: dict[str, float], symbol: str) -> tuple[float | None, str]:
    for cand in _hyperliquid_symbol_candidates(symbol):
        mv = mids.get(cand)
        if mv is not None and math.isfinite(mv) and mv > 0:
            return float(mv), cand
    return None, ""


def _hyperliquid_parse_l2_book(payload: object) -> tuple[list[tuple[float, float]], list[tuple[float, float]]]:
    if not isinstance(payload, dict):
        return ([], [])
    levels = payload.get("levels")
    if not isinstance(levels, list) or len(levels) < 2:
        return ([], [])
    bids_raw, asks_raw = levels[0], levels[1]
    bids: list[tuple[float, float]] = []
    asks: list[tuple[float, float]] = []

    def parse_rows(rows: object, out: list[tuple[float, float]]) -> None:
        if not isinstance(rows, list):
            return
        for row in rows:
            if not isinstance(row, dict):
                continue
            p = _to_float(row.get("px"))
            q = _to_float(row.get("sz"))
            if p is None or q is None:
                continue
            if not math.isfinite(p) or not math.isfinite(q):
                continue
            out.append((float(p), float(q)))

    parse_rows(bids_raw, bids)
    parse_rows(asks_raw, asks)
    bids.sort(key=lambda t: t[0], reverse=True)
    asks.sort(key=lambda t: t[0])
    return (bids, asks)


def _ssl_ctx() -> ssl.SSLContext:
    return ssl.create_default_context()


def merge_external_feed_local(dc: dict) -> dict:
    """Merge config/external_feed.local.json into dc['external_feed'] (same as C++ overlay)."""
    if not EXTERNAL_FEED_LOCAL_PATH.is_file():
        return dc
    try:
        ov = json.loads(EXTERNAL_FEED_LOCAL_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return dc
    if not isinstance(ov, dict):
        return dc
    ext_ov = ov.get("external_feed")
    if not isinstance(ext_ov, dict):
        return dc
    ext_base = dc.setdefault("external_feed", {})
    if not isinstance(ext_base, dict):
        dc["external_feed"] = dict(ext_ov)
        return dc

    def deep_merge(a: dict, b: dict) -> None:
        for k, v in b.items():
            if isinstance(k, str) and k.startswith("_"):
                continue
            if isinstance(v, dict) and isinstance(a.get(k), dict):
                deep_merge(a[k], v)
            else:
                a[k] = v

    deep_merge(ext_base, ext_ov)
    return dc


def get_merged_config_dict() -> dict:
    dc: dict = {}
    if DEFAULT_CONFIG_PATH.is_file():
        try:
            dc = json.loads(DEFAULT_CONFIG_PATH.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            pass
    return merge_external_feed_local(dc)


def http_json(
    method: str,
    url: str,
    body: dict | None = None,
    headers: dict | None = None,
    timeout: float = 25.0,
) -> tuple[int, dict | list]:
    data = None
    hdrs = dict(headers or {})
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        hdrs.setdefault("Content-Type", "application/json")
    req = urllib.request.Request(url, data=data, headers=hdrs, method=method)
    try:
        resp = urllib.request.urlopen(req, context=_ssl_ctx(), timeout=timeout)
    except urllib.error.HTTPError as e:
        raw = e.read().decode("utf-8", errors="replace")
        try:
            parsed = json.loads(raw) if raw else {}
        except json.JSONDecodeError:
            parsed = {"_raw": raw[:800]}
        return e.code, parsed
    except urllib.error.URLError as e:
        detail = e.reason if getattr(e, "reason", None) is not None else str(e)
        return 0, {"_network_error": True, "detail": str(detail)[:500]}
    except TimeoutError as e:
        return 0, {"_network_error": True, "detail": f"TimeoutError: {e}"}
    with resp:
        raw = resp.read().decode("utf-8")
        if not raw:
            return resp.status, {}
        try:
            return resp.status, json.loads(raw)
        except json.JSONDecodeError:
            return resp.status, {"_raw": raw[:800]}


def architect_orders_base(rest_api_url: str) -> str:
    b = rest_api_url.rstrip("/")
    if b.endswith("/api"):
        return b[:-4] + "/orders"
    return b + "/orders"


def extract_bearer_token(j: dict) -> str:
    if not j or not isinstance(j, dict):
        return ""
    # AX api-gateway AuthenticateResponse uses top-level "token" (string).
    if isinstance(j.get("token"), str) and j["token"].strip():
        return j["token"].strip()
    if isinstance(j.get("access_token"), str):
        return j["access_token"]
    if isinstance(j.get("jwt"), str):
        return j["jwt"]
    d = j.get("data")
    if isinstance(d, dict):
        if isinstance(d.get("access_token"), str):
            return d["access_token"]
        if isinstance(d.get("token"), str):
            return d["token"]
    return ""


def round_bid_down(p: float, tick: float) -> float:
    return math.floor(p / tick + 1e-12) * tick


def round_ask_up(p: float, tick: float) -> float:
    return math.ceil(p / tick - 1e-12) * tick


def skew_raw(
    mid: float,
    width_bps: int,
    tick: float,
    net_po: int,
    adjust_po: int,
    adjust_ticks: int,
    max_po: int | None = None,
) -> tuple[float, float]:
    """Same skew as compute_skewed_raw_bid_ask for symmetric width (optional max_po for skew gate)."""
    return compute_skewed_raw_bid_ask(
        mid,
        width_bps,
        width_bps,
        tick,
        net_po,
        adjust_po,
        adjust_ticks,
        max_po if max_po is not None else 10**18,
    )


def apply_pricer_snapshot_transform(
    theo: float,
    quote_snapshot: float,
    pricer_snapshot: float,
    slope: float,
) -> float:
    """Mirror of C++ MakeMarketStrategy::mmApplyPricerSnapshotTransform.

    Per-stack pricer-snapshot linear transform — desk's spec:

        newMidpoint = quote_snapshot + quote_snapshot * slope * ((theo - pricer_snapshot) / pricer_snapshot)

    THIS MUST STAY BIT-IDENTICAL TO THE C++ HELPER. The Python side computes the initial pair
    that hits the gateway and writes ``bid_price`` / ``ask_price`` into ``orders.json``. The C++
    side then adopts those OIDs and computes every subsequent cancel-replace using the same
    formula. If the two diverge by even one tick the C++ drift gate sees a permanent mismatch
    and cancel-replaces every cycle (the "moves to the same price forever" bug).

    Disabled (returns raw ``theo`` unchanged) when:
      * ``pricer_snapshot <= 0`` — divisor would explode / operator left field blank
      * ``quote_snapshot  <= 0`` — no AX-side anchor; transform is meaningless
      * result is non-finite or non-positive — failsafe so a misconfigured snapshot can't push
        a NaN/negative price into the gateway place
    """
    try:
        ps = float(pricer_snapshot)
        qs = float(quote_snapshot)
        sl = float(slope)
    except (TypeError, ValueError):
        return float(theo)
    if not (ps > 0.0) or not (qs > 0.0):
        return float(theo)
    new_mid = qs + qs * sl * ((float(theo) - ps) / ps)
    if not math.isfinite(new_mid) or new_mid <= 0.0:
        return float(theo)
    return new_mid


def compute_skewed_raw_bid_ask(
    adjusted_theo: float,
    bid_spread_bps: int,
    ask_spread_bps: int,
    quote_tick: float,
    net_po: int,
    adjust_po: int,
    adjust_ticks: int,
    max_po: int,
) -> tuple[float, float]:
    """
    MM_SKEW_FORMULA (symmetric long/short, 2026-05-21):
        skew_tick_units = floor(|NetPo| / adjust_position) * sign(NetPo) * adjust_ticks
            (when |NetPo| < max_position)

    Mirror C++ MakeMarketStrategy::computeInventorySkewedRawBidAsk.
    Pricing mid = adjusted_theo (newMidpoint + basis). Spread inputs are PER-SIDE widths in basis
    points of pricing mid (1 bp = 0.0001 of mid). Each side gets the full bps offset:
        bid_offset_px = adjusted_theo * bid_spread_bps / 10000
        ask_offset_px = adjusted_theo * ask_spread_bps / 10000
    Total spread = bid_offset + ask_offset = adjusted_theo * (bid_w + ask_w) / 10000.
    When abs(NetPo) < max_position:
        skew_tick_units = floor(|NetPo|/adjust_po) * sign(NetPo) * adjust_ticks
        skew_price = skew_tick_units * quote_tick   (same skew subtracted from bid and offer)
    Symmetry note: the previous form `floor(net_po / adjust_po)` used Python's floor (toward −∞),
    making any negative inventory below adjust_po jump straight to -adjust_ticks while the
    equivalent long position stayed at zero. Both desk-preview and C++ now use the symmetric form.
    Raw bid/ask:
        raw_bid = adjusted_theo - bid_offset_px - skew_price
        raw_ask = adjusted_theo + ask_offset_px - skew_price
    Identical math for every instrument — no symbol-specific branching, only the input bps and
    quote_tick differ.
    """
    if adjust_po <= 0:
        adjust_po = 1
    at = max(1, int(adjust_ticks))
    if max_po <= 0:
        max_po = 1
    skew_tick_units = 0.0
    if abs(int(net_po)) < int(max_po):
        np_i = int(net_po)
        abs_np = abs(np_i)
        np_sign = 1 if np_i > 0 else (-1 if np_i < 0 else 0)
        skew_tick_units = math.floor(abs_np / adjust_po) * np_sign * at
    skew_price = skew_tick_units * quote_tick
    bid_offset_px = adjusted_theo * (float(bid_spread_bps) / 10000.0)
    ask_offset_px = adjusted_theo * (float(ask_spread_bps) / 10000.0)
    raw_bid = adjusted_theo - bid_offset_px - skew_price
    raw_ask = adjusted_theo + ask_offset_px - skew_price
    if __debug__ and int(bid_spread_bps) == int(ask_spread_bps):
        expected_span = 2.0 * float(adjusted_theo) * (float(bid_spread_bps) / 10000.0)
        actual_span = raw_ask - raw_bid
        assert abs(actual_span - expected_span) < 1e-9, (
            "2*W invariant violated in symmetric-width path: "
            f"actual_span={actual_span!r} expected_span={expected_span!r}"
        )
    return raw_bid, raw_ask


def finalize_mm_pair_on_tick_grid(
    raw_bid: float,
    raw_ask: float,
    bid_spread_bps: int,
    ask_spread_bps: int,
    quote_tick: float,
) -> tuple[float, float]:
    """
    Match C++ MakeMarketStrategy::finalizeMmPairOnTickGrid: round bid down and ask up independently
    (with ask >= bid + 1 tick). The bid/ask spread bps params are accepted for caller-side symmetry
    with the pricing pipeline but are unused here — placement does NOT force a fixed span; the
    actual placed_span_px = roundAskUp(raw_ask,tick) - roundBidDown(raw_bid,tick) and may differ
    from the raw bps-derived span by up to ~1 tick.
    """
    if not math.isfinite(quote_tick) or quote_tick <= 0:
        return raw_bid, raw_ask
    _ = int(bid_spread_bps) + int(ask_spread_bps)
    bid_px = round_bid_down(raw_bid, quote_tick)
    ask_px = max(bid_px + quote_tick, round_ask_up(raw_ask, quote_tick))
    return bid_px, ask_px


def should_quote_mm_side(net_po: int, max_po: int, *, buy_side: bool) -> bool:
    """BUY only while net < max; SELL only while net > -max (strict at ±max).

    Matches C++ ``shouldQuoteSide`` inventory gating (inside cap → both sides allowed; at +max → SELL-only;
    at −max → BUY-only). Reload-cycle reduce-only is applied only in C++.
    """
    if max_po <= 0:
        max_po = 1
    if buy_side:
        return int(net_po) < int(max_po)
    return int(net_po) > -int(max_po)


def _mm_round_qty_to_step_down_int(q: int, step: int) -> int:
    """Floor to the nearest multiple of ``step`` (matches C++ mmRoundQtyToStepDown).

    If ``order_size`` is not divisible by ``order_size_step`` (e.g. 199 with step 100), this returns
    100 — not 200 — which can look like “half” the intended size. Align ``order_size`` to the lot
    step in ``default_config.json`` (or use config editor ``order_size_step``).
    """
    if q <= 0:
        return 0
    stp = max(1, int(step))
    if stp <= 1:
        return int(q)
    return max(0, (int(q) // stp) * stp)


def _clamp_min_theo_move_ticks_to_requote(v: int) -> int:
    """Match C++ Config::getMarketMakerMinTheoMoveTicksToRequote — minimum allowed is 1."""
    return max(1, min(int(v), 1_000_000))


def mm_desk_leg_qty_headroom_shares(
    net_po: int, max_po: int, base_qty: int, *, buy_side: bool, step: int = 1
) -> int:
    """Step-rounded configured order size (legacy name: we do *not* shrink the buy leg by max−net headroom).

    For placing both sides, prefer :func:`mm_desk_pair_leg_quantities` so bid/ask always match when both quote.
    """
    _ = net_po
    _ = max_po
    _ = buy_side
    if base_qty <= 0:
        return 0
    return _mm_round_qty_to_step_down_int(int(base_qty), step)


def mm_desk_pair_leg_quantities(
    want_bid: bool, want_ask: bool, base_qty: int, step: int = 1
) -> tuple[int, int]:
    """(qty_bid, qty_ask) for MM desk REST: same step-rounded size on every *active* leg.

    max_position is enforced only by ``should_quote_mm_side`` (side off) and C++ reconcile — never by
    asymmetric bid vs ask sizes while still inside the cap (that produced e.g. 100 bid / 200 ask).
    """
    stp = max(1, int(step))
    if base_qty <= 0:
        return 0, 0
    leg = _mm_round_qty_to_step_down_int(int(base_qty), stp)
    if leg <= 0:
        return 0, 0
    return (leg if want_bid else 0, leg if want_ask else 0)


def mm_position_gate_fields_from_preview(mm_preview: dict, max_po_cfg: int) -> dict:
    mx = int(max_po_cfg)
    if mx <= 0:
        mx = 1
    if not isinstance(mm_preview, dict) or not mm_preview.get("ok"):
        return {"mm_position_gate_alert": "", "mm_position_abs_net": None, "mm_max_position_gate": mx}
    if bool(mm_preview.get("net_position_unknown")):
        return {"mm_position_gate_alert": "", "mm_position_abs_net": None, "mm_max_position_gate": mx}
    try:
        n = int(mm_preview.get("net_position", 0))
    except (TypeError, ValueError):
        return {"mm_position_gate_alert": "", "mm_position_abs_net": None, "mm_max_position_gate": mx}
    an = abs(n)
    if an > mx:
        msg = (
            f"Position cap breach: |net|={an} > max_position={mx}. "
            f"Only the inventory-reducing side is quoted; reduce exposure manually if needed."
        )
    elif an >= mx:
        msg = f"At max_position: |net|={an} (cap {mx}). Add-side quotes are off until |net| < {mx}."
    else:
        msg = ""
    return {"mm_position_gate_alert": msg, "mm_position_abs_net": an, "mm_max_position_gate": mx}


def validate_mm_quotes_non_cross(
    bid_px: float,
    ask_px: float,
    market_bid: float | None,
    market_ask: float | None,
) -> tuple[bool, str]:
    """C++ QO rules: bid <= mkt ask, ask >= mkt bid, bid < ask."""
    if bid_px <= 0 or ask_px <= 0 or bid_px >= ask_px:
        return False, "invalid_spread"
    if market_ask is not None and market_ask > 0 and bid_px > market_ask:
        return False, "bid_would_cross"
    if market_bid is not None and market_bid > 0 and ask_px < market_bid:
        return False, "ask_would_cross"
    return True, ""


def _adjust_mm_pair_to_non_cross(
    bid_px: float,
    ask_px: float,
    *,
    bid_spread_bps: int,
    ask_spread_bps: int,
    quote_tick: float,
    market_bid: float | None,
    market_ask: float | None,
) -> tuple[float, float, bool]:
    """Shift a quote pair to the nearest non-crossing price grid when AX top-of-book is known.

    The shift PRESERVES the original (bps-derived) span: span_px = ask_px - bid_px from the
    incoming pair. Previously this function reconstructed span as (bid_w + ask_w) * quote_tick,
    which was correct only when those args were tick counts; once the pricing pipeline moved to
    bps, that formula was off by a factor of (adjusted_theo / 10000), most visibly compressing
    the cross-fixed pair on higher-priced instruments. The bid/ask spread bps params are kept
    for parity with the call signature elsewhere but are no longer used here.
    """
    _ = int(bid_spread_bps) + int(ask_spread_bps)
    ok0, reason0 = validate_mm_quotes_non_cross(bid_px, ask_px, market_bid, market_ask)
    if ok0:
        return bid_px, ask_px, False
    if not math.isfinite(quote_tick) or quote_tick <= 0:
        return bid_px, ask_px, False
    span_px = max(quote_tick, float(ask_px) - float(bid_px))
    if reason0 == "ask_would_cross" and market_bid is not None and market_bid > 0:
        ask_px = round_ask_up(float(market_bid), quote_tick)
        bid_px = ask_px - span_px
    elif reason0 == "bid_would_cross" and market_ask is not None and market_ask > 0:
        bid_px = round_bid_down(float(market_ask), quote_tick)
        ask_px = bid_px + span_px
    else:
        return bid_px, ask_px, False
    ok1, _ = validate_mm_quotes_non_cross(bid_px, ask_px, market_bid, market_ask)
    return bid_px, ask_px, ok1


def _to_float(x) -> float | None:
    if x is None:
        return None
    if isinstance(x, (int, float)):
        return float(x)
    try:
        return float(str(x).strip())
    except ValueError:
        return None


def _normalize_ax_symbol(s: str) -> str:
    """Match C++ PortfolioManager: ignore spaces, hyphens, underscores for symbol equality."""
    return "".join(c.upper() for c in str(s) if c not in " -_")


def _tick_from_gateway_map(tick_map: dict[str, float], sym: str) -> float | None:
    """Resolve positive tick from gateway map (keys: UPPER and _normalize_ax_symbol)."""
    s = (sym or "").strip()
    if not s or not tick_map:
        return None
    for key in (s.upper(), _normalize_ax_symbol(s)):
        if not key:
            continue
        v = tick_map.get(key)
        if v is None:
            continue
        try:
            vf = float(v)
        except (TypeError, ValueError):
            continue
        if vf > 0.0 and math.isfinite(vf):
            return vf
    return None


def _gateway_tick_for_ax(st: DeskSharedState, ax: str) -> float | None:
    with st.lock:
        tm = dict(st.ax_tick_by_symbol or {})
    return _tick_from_gateway_map(tm, ax)


def _merge_gateway_ticks_into_st_mm_instruments(st: DeskSharedState, tick_map: dict[str, float]) -> None:
    """Overwrite each leg's ``tick_size`` with the gateway map (authoritative)."""
    with st.lock:
        rows = list(st.mm_instruments or [])
    out: list[dict] = []
    for row in rows:
        if not isinstance(row, dict):
            out.append(row)
            continue
        sym = str(row.get("symbol") or row.get("ax_symbol") or "").strip()
        t = _tick_from_gateway_map(tick_map, sym) if sym else None
        nr = dict(row)
        if t is not None:
            nr["tick_size"] = t
        out.append(nr)
    with st.lock:
        st.mm_instruments = out


def _signed_qty_ax_row(p: dict, depth: int = 0) -> float | None:
    """Signed qty from GET /positions row. OpenAPI: required field ``signed_quantity`` (see Architect docs)."""
    if not isinstance(p, dict) or depth > 5:
        return None
    for k in ("signed_quantity", "signedQuantity"):
        if k in p:
            qv = _to_float(p[k])
            if qv is not None:
                return float(qv)
    flat_keys = (
        "open_quantity",
        "open_qty",
        "openQty",
        "net_position",
        "netPosition",
        "net_qty",
        "netQty",
        "positionAmt",
        "position_qty",
        "positionQty",
        "quantity",
        "qty",
        "q",
        "position",
        "size",
        "contracts",
        "contract_qty",
        "contractQty",
        "base_position",
        "basePosition",
        "base_qty",
        "baseQty",
        "pos",
    )
    for k in flat_keys:
        if k not in p:
            continue
        v = p[k]
        if isinstance(v, dict):
            inner = _signed_qty_ax_row(v, depth + 1)
            if inner is not None:
                return inner
            continue
        qv = _to_float(v)
        if qv is not None:
            return float(qv)
    for wrap in ("position", "pos", "info", "details", "data", "snapshot", "aggregate", "summary"):
        v = p.get(wrap)
        if isinstance(v, dict):
            inner = _signed_qty_ax_row(v, depth + 1)
            if inner is not None:
                return inner
    if "long_qty" in p or "short_qty" in p:
        lq = _to_float(p.get("long_qty")) or 0.0
        sq = _to_float(p.get("short_qty")) or 0.0
        net = lq - sq
        if abs(net) > 1e-15:
            return float(net)
    return None


def norm_px(p: float, tick: float = PRICE_TICK) -> float:
    return round(round(p / tick) * tick, 10)


def parse_orderbook_payload(payload) -> tuple[list[tuple[float, float]], list[tuple[float, float]]]:
    bids: list[tuple[float, float]] = []
    asks: list[tuple[float, float]] = []
    if not isinstance(payload, dict):
        return bids, asks
    # AX api-gateway: GET /book returns { "book": { "s", "b", "a", ... } }
    inner = payload.get("book")
    if isinstance(inner, dict):
        payload = inner
    else:
        for wrap in ("data", "result", "payload", "snapshot"):
            cand = payload.get(wrap)
            if isinstance(cand, dict) and (
                cand.get("bids")
                or cand.get("asks")
                or cand.get("b")
                or cand.get("a")
            ):
                payload = cand
                break
    raw_b = payload.get("bids") or payload.get("b") or []
    raw_a = payload.get("asks") or payload.get("a") or []
    for row in raw_b if isinstance(raw_b, list) else []:
        if isinstance(row, (list, tuple)) and len(row) >= 2:
            p, s = _to_float(row[0]), _to_float(row[1])
        elif isinstance(row, dict):
            p = _to_float(row.get("price") or row.get("p"))
            s = _to_float(row.get("quantity") or row.get("size") or row.get("q"))
        else:
            continue
        if p is not None and s is not None:
            bids.append((p, s))
    for row in raw_a if isinstance(raw_a, list) else []:
        if isinstance(row, (list, tuple)) and len(row) >= 2:
            p, s = _to_float(row[0]), _to_float(row[1])
        elif isinstance(row, dict):
            p = _to_float(row.get("price") or row.get("p"))
            s = _to_float(row.get("quantity") or row.get("size") or row.get("q"))
        else:
            continue
        if p is not None and s is not None:
            asks.append((p, s))
    bids.sort(key=lambda t: t[0], reverse=True)
    asks.sort(key=lambda t: t[0])
    return bids, asks


def _top_of_book_metrics(bids: list, asks: list) -> dict | None:
    """Mid = (best bid + best ask) / 2, spread in price units / bps / percent."""
    if not bids or not asks:
        return None
    try:
        bid = float(bids[0][0])
        ask = float(asks[0][0])
    except (IndexError, TypeError, ValueError):
        return None
    if bid <= 0 or ask <= 0 or ask < bid:
        return None
    mid = (bid + ask) / 2.0
    spr = ask - bid
    return {
        "bid": bid,
        "ask": ask,
        "mid": mid,
        "spread": spr,
        "spread_bps": (spr / mid) * 10000.0,
        "spread_pct": (spr / mid) * 100.0,
    }


def _cfg_int(d: dict, *keys: str, default: int) -> int:
    for k in keys:
        if k not in d or d[k] is None:
            continue
        try:
            return int(d[k])
        except (TypeError, ValueError):
            # Accept float-like strings such as "2.0" from manual edits.
            try:
                return int(round(float(d[k])))
            except (TypeError, ValueError):
                continue
    return default


def _cfg_float(d: dict, *keys: str, default: float) -> float:
    for k in keys:
        if k not in d or d[k] is None:
            continue
        try:
            return float(d[k])
        except (TypeError, ValueError):
            continue
    return default


def _desk_mm_ax_leg_symbols(dc: dict, ax_symbol: str) -> list[str]:
    """Ordered unique AX symbols from ``market_maker.instruments`` plus top-level MM symbol fields."""
    out: list[str] = []
    seen: set[str] = set()

    def add(s: str) -> None:
        t = (s or "").strip()
        if not t or t in seen:
            return
        seen.add(t)
        out.append(t)

    mm = dc.get("market_maker") if isinstance(dc.get("market_maker"), dict) else {}
    inst = mm.get("instruments")
    if isinstance(inst, list) and inst:
        for row in inst:
            if isinstance(row, dict):
                add(str(row.get("symbol") or row.get("ax_symbol") or ""))
                add(str(row.get("order_symbol") or ""))
    add(str(mm.get("symbol") or ""))
    add(str(mm.get("order_symbol") or ""))
    if not out:
        add(str(ax_symbol or ""))
    return out


def _desk_ax_orderbook_file_list(dc: dict) -> list[str] | None:
    """Raw ``mm_desk.ax_orderbook_symbols`` list from disk (deduped, order preserved), or None if key absent."""
    desk = dc.get("mm_desk") if isinstance(dc.get("mm_desk"), dict) else {}
    if "ax_orderbook_symbols" not in desk:
        return None
    raw = desk.get("ax_orderbook_symbols")
    if not isinstance(raw, list):
        return []
    seen: set[str] = set()
    out: list[str] = []
    for x in raw:
        t = str(x).strip() if x is not None else ""
        if not t or t in seen:
            continue
        seen.add(t)
        out.append(t)
    return out


def _desk_ax_book_symbols(dc: dict, ax_symbol: str) -> list[str]:
    """
    Symbols for MM web desk AX depth polling and the ARCHITECT grid (not trading.watchlist).

    When ``mm_desk.ax_architect_depth_multiselect_explicit`` is true and ``mm_desk.ax_orderbook_symbols``
    exists: non-empty list is the **exact** depth set; empty list falls back to all MM legs.

    Otherwise (legacy): union of every MM leg plus ``mm_desk.ax_orderbook_symbols`` (extras, primary stripped
    from the file row). When ``ax_orderbook_symbols`` is absent, merges optional inline
    ``orderbook_symbols`` / ``ax_orderbook_symbols`` lists as before.
    """
    desk = dc.get("mm_desk") if isinstance(dc.get("mm_desk"), dict) else {}
    explicit = bool(desk.get("ax_architect_depth_multiselect_explicit"))
    file_list = _desk_ax_orderbook_file_list(dc)

    if file_list is not None and explicit:
        if file_list:
            return list(file_list)
        return _desk_mm_ax_leg_symbols(dc, ax_symbol)

    out: list[str] = []
    seen: set[str] = set()

    def add(s: str) -> None:
        t = (s or "").strip()
        if not t or t in seen:
            return
        seen.add(t)
        out.append(t)

    for s in _desk_mm_ax_leg_symbols(dc, ax_symbol):
        add(s)

    if file_list is not None:
        saved_extras = _desk_saved_ax_orderbook_multiselect_extras(dc, ax_symbol)
        for x in saved_extras or []:
            add(str(x))
    else:
        extra = desk.get("ax_orderbook_symbols") or desk.get("orderbook_symbols")
        if isinstance(extra, list):
            for x in extra:
                if x is not None:
                    add(str(x))
    return out


def _desk_saved_ax_orderbook_multiselect_extras(dc: dict, primary_ax: str) -> list[str] | None:
    """
    Contents of ``mm_desk.ax_orderbook_symbols`` for UI / poll metadata.

    Returns None when that key is absent. When ``ax_architect_depth_multiselect_explicit`` is true,
    returns the full file list (including the active AX if stored). Legacy mode strips the primary
    symbol so the row stays "extras-only" for merge semantics.
    """
    fl = _desk_ax_orderbook_file_list(dc)
    if fl is None:
        return None
    desk = dc.get("mm_desk") if isinstance(dc.get("mm_desk"), dict) else {}
    if bool(desk.get("ax_architect_depth_multiselect_explicit")):
        return list(fl)
    prim = (primary_ax or "").strip()
    seen: set[str] = set()
    out: list[str] = []
    for t in fl:
        if not t or t == prim or t in seen:
            continue
        seen.add(t)
        out.append(t)
    return out


def _desk_ref_book_symbols(dc: dict, primary_ref_symbol: str) -> list[str]:
    """
    Reference depth symbols: Neon FIX md_symbols when external_feed.provider is neon_fix;
    else REST: mm_desk.reference_orderbook_symbols + primary external_feed.symbol.
    In multi-theo, uses every instrument reference symbol so the desk can show CME+HL+Neon columns.
    """
    mm = dc.get("market_maker") if isinstance(dc.get("market_maker"), dict) else {}
    if _desk_is_multi_from_config(dc) and isinstance(mm.get("instruments"), list):
        out: list[str] = []
        seen: set[str] = set()
        for row in mm.get("instruments") or []:
            if not isinstance(row, dict):
                continue
            ref = str(row.get("reference_fix_symbol") or row.get("theo_symbol") or "").strip()
            if not ref:
                continue
            c = fix_symbol_canonical_py(ref)
            if c in seen:
                continue
            seen.add(c)
            out.append(ref)
        if out:
            desk = dc.get("mm_desk") if isinstance(dc.get("mm_desk"), dict) else {}
            extra = desk.get("reference_orderbook_symbols") or desk.get("bn_orderbook_symbols")
            if isinstance(extra, list):
                for x in extra:
                    if x is not None:
                        t = str(x).strip()
                        if t and t not in seen:
                            seen.add(t)
                            out.append(t)
            return out
    ext = dc.get("external_feed") if isinstance(dc.get("external_feed"), dict) else {}
    if neon_fix_feed.is_neon_fix_provider(str(ext.get("provider", ""))):
        nfs = neon_fix_feed.settings_from_merged_config(dc)
        if nfs and nfs.md_symbols:
            return list(nfs.md_symbols)
        sym = str(ext.get("symbol", primary_ref_symbol) or primary_ref_symbol or "").strip()
        return [sym] if sym else []

    out: list[str] = []
    seen: set[str] = set()

    def add(s: str) -> None:
        t = (s or "").strip().upper()
        if not t or t in seen:
            return
        seen.add(t)
        out.append(t)

    add(primary_ref_symbol or "")
    desk = dc.get("mm_desk") if isinstance(dc.get("mm_desk"), dict) else {}
    extra = desk.get("reference_orderbook_symbols") or desk.get("bn_orderbook_symbols")
    if isinstance(extra, list):
        for x in extra:
            if x is not None:
                add(str(x))
    return out


def _primary_config_stat() -> tuple[int | None, str]:
    """mtime ms and absolute path for UI/debug — which file the desk reads."""
    try:
        st = DEFAULT_CONFIG_PATH.stat()
        return int(st.st_mtime * 1000), str(DEFAULT_CONFIG_PATH.resolve())
    except OSError:
        return None, str(DEFAULT_CONFIG_PATH.resolve())


def _sync_mm_trading_flags_from_primary_file(st) -> None:
    """
    Force reload limits + MM gate flags from default_config.json each snapshot (same file C++ reads).
    """
    if not DEFAULT_CONFIG_PATH.is_file():
        return
    try:
        dc = json.loads(DEFAULT_CONFIG_PATH.read_text(encoding="utf-8"))
        mm = dc.get("market_maker")
        if not isinstance(mm, dict):
            return
        cr = _cfg_int(mm, "current_reload_count", default=0)
        mx = _cfg_int(mm, "max_reload_cycles", default=100)
        v_oe = mm.get("mm_orders_enabled", True)
        if isinstance(v_oe, str):
            mm_orders_enabled = v_oe.strip().lower() in ("1", "true", "yes", "on")
        else:
            mm_orders_enabled = bool(v_oe)
        v_rq = mm.get("requote_on_theo_move", False)
        if isinstance(v_rq, str):
            requote_on_theo_move = v_rq.strip().lower() in ("1", "true", "yes", "on")
        else:
            requote_on_theo_move = bool(v_rq)
        with st.lock:
            st.mm_current_reload_count = cr
            st.mm_max_reload_cycles = mx
            st.mm_orders_enabled = mm_orders_enabled
            st.mm_requote_on_theo_move = requote_on_theo_move
    except (OSError, json.JSONDecodeError, TypeError, ValueError):
        return


def load_app_config() -> dict:
    dc: dict = {}
    rest = DEFAULT_REST
    ws = ""
    key = ""
    secret = ""
    session = ""
    ref_symbol = ""
    ref_display = ""
    ref_rest_url = ""
    ref_rest_uses_spot_path = True
    ax_symbol = DEFAULT_AX_SYMBOL
    mm_width = 10
    mm_order_size = 200
    mm_order_size_step = 1
    mm_max_position = 1000000
    # Default adjust_position = 50 (was 50000). The skew formula is
    #   skew_tick_units = floor(NetPo / adjust_position) * adjust_ticks
    # so adjust_position=50000 with realistic NetPo (a few hundred to a few
    # thousand) makes floor() always 0 → skew always zero → no inventory bias
    # at all. Live evidence (logs/20260505 17:37): mm_req_XAG_PERP at net_po=895
    # with adjust_position=50000 produced skew_tick_units=0.000 every tick, so
    # the offer side never got pulled in to actually reduce inventory. 50 is the
    # value we already hard-code for base make_market_XAG_PERP and it works.
    mm_adjust_position = 50
    mm_adjust_ticks = 2
    price_tick = PRICE_TICK
    mm_bid_width = 10
    mm_ask_width = 10
    mm_resting_ticks = 0
    mm_basis = 0.0
    mm_instruments: list[dict] = []
    mm_max_reload_cycles = 100
    mm_current_reload_count = 0
    mm_reload_cycles_count_fill_only = True
    mm_reload_limit_reset_nonce = ""
    mm_requote_on_theo_move = False
    mm_orders_enabled = True
    # Default min_theo_move_ticks_to_requote = 1 (was 2). With value 2, FIX-driven theo paths
    # (EURUSD/JPYUSD via neon_fix wiggling ±1 tick per quote) almost never satisfied the gate
    # and orders sat unmoved for minutes — see logs/20260505 17:38: desk_drift_gate_not_met
    # spammed `drift_ticks=1 min_drift=2` for the entire EURUSD session. 1 lets every real
    # tick of theo movement count, which is what the desk operator expects.
    mm_min_theo_move_ticks_to_requote = 1
    pricing_tick = PRICE_TICK
    if DEFAULT_CONFIG_PATH.is_file():
        try:
            dc = json.loads(DEFAULT_CONFIG_PATH.read_text(encoding="utf-8"))
            merge_external_feed_local(dc)
            api = dc.get("api") or {}
            rest = str(api.get("rest_endpoint", rest))
            ws = str(api.get("ws_endpoint", ws))
            key = str(api.get("api_key", key) or "")
            secret = str(api.get("api_secret", secret) or "")
            session = str(api.get("session_token", session) or "")
            ext = dc.get("external_feed") or {}
            ref_symbol = str(ext.get("symbol", ref_symbol))
            ref_display = str(ext.get("display_symbol", "") or "").strip()
            bu = str(ext.get("rest_url", "") or "").strip()
            if bu.startswith("http"):
                ref_rest_url = bu.rstrip("/")
            ref_rest_uses_spot_path = bool(ext.get("rest_uses_spot_ticker_path", True))
            mm = dc.get("market_maker") or {}
            if isinstance(mm, dict):
                inst = mm.get("instruments")
                if isinstance(inst, list) and inst:
                    for row in inst:
                        if not isinstance(row, dict):
                            continue
                        sym = str(row.get("symbol") or row.get("ax_symbol") or "").strip()
                        if not sym:
                            continue
                        ref = str(row.get("reference_fix_symbol") or row.get("theo_symbol") or "").strip()
                        osym = str(row.get("order_symbol") or "").strip()
                        ts = str(row.get("theo_source") or "").strip()
                        tvs = str(row.get("theo_venue_symbol") or "").strip()
                        d = {
                            "symbol": sym,
                            "reference_fix_symbol": ref,
                            "order_symbol": osym,
                        }
                        if ts:
                            d["theo_source"] = ts
                        if tvs:
                            d["theo_venue_symbol"] = tvs
                        # Per-leg overrides surfaced to GUI preview (mirrors C++
                        # MakeMarketStrategy::mmEffPriceTick / mmTransformTheo). Required for
                        # AX /instruments products whose tick differs from the global price_tick
                        # (XAU=0.1, XAG=0.01, JPYUSD=1e-6) and for cross-scale feeds that need
                        # the pricer-snapshot transform (JPYUSD-PERP off USD/JPY, MXNUSD-PERP off
                        # USD/MXN, SPY-PERP off xyz:SP500).
                        try:
                            _t = float(row.get("tick_size") or row.get("tick") or row.get("price_tick") or 0.0)
                            if _t > 0.0:
                                d["tick_size"] = _t
                        except (TypeError, ValueError):
                            pass
                        try:
                            _s = float(row.get("theo_scale") or 0.0)
                            if _s > 0.0:
                                d["theo_scale"] = _s
                        except (TypeError, ValueError):
                            pass
                        try:
                            _mp = int(row.get("max_position") or 0)
                            if _mp > 0:
                                d["max_position"] = _mp
                        except (TypeError, ValueError):
                            pass
                        _rq = row.get("requote_on_theo_move")
                        if isinstance(_rq, str):
                            d["requote_on_theo_move"] = _rq.strip().lower() in ("1", "true", "yes", "on")
                        elif isinstance(_rq, bool):
                            d["requote_on_theo_move"] = _rq
                        _oe = row.get("mm_orders_enabled")
                        if isinstance(_oe, str):
                            d["mm_orders_enabled"] = _oe.strip().lower() in ("1", "true", "yes", "on")
                        elif isinstance(_oe, bool):
                            d["mm_orders_enabled"] = _oe
                        mstk = row.get("manual_stacks")
                        if isinstance(mstk, list) and mstk:
                            d["manual_stacks"] = mstk
                        mm_instruments.append(d)
                    if mm_instruments:
                        ax_symbol = mm_instruments[0]["symbol"]
                if not mm_instruments:
                    ax_symbol = str(mm.get("symbol") or mm.get("order_symbol") or ax_symbol)
                mm_width = _cfg_int(mm, "width_bps", "width", "spread_ticks", default=mm_width)
                mm_order_size = _cfg_int(mm, "order_size", "quantity", default=mm_order_size)
                mm_order_size_step = _cfg_int(mm, "order_size_step", default=mm_order_size_step)
                mm_max_position = _cfg_int(mm, "max_position", default=mm_max_position)
                mm_adjust_position = _cfg_int(mm, "adjust_position", default=mm_adjust_position)
                mm_adjust_ticks = _cfg_int(mm, "adjust_ticks", default=mm_adjust_ticks)
                price_tick = _cfg_float(mm, "price_tick", default=price_tick)
                pricing_tick = _cfg_float(mm, "pricing_tick", default=price_tick)
                v_rq = mm.get("requote_on_theo_move", mm_requote_on_theo_move)
                if isinstance(v_rq, str):
                    mm_requote_on_theo_move = v_rq.strip().lower() in ("1", "true", "yes", "on")
                else:
                    mm_requote_on_theo_move = bool(v_rq)
                v_oe = mm.get("mm_orders_enabled", mm_orders_enabled)
                if isinstance(v_oe, str):
                    mm_orders_enabled = v_oe.strip().lower() in ("1", "true", "yes", "on")
                else:
                    mm_orders_enabled = bool(v_oe)
                mm_min_theo_move_ticks_to_requote = _clamp_min_theo_move_ticks_to_requote(
                    _cfg_int(mm, "min_theo_move_ticks_to_requote", default=mm_min_theo_move_ticks_to_requote)
                )
                mm_bid_width = _cfg_int(mm, "bid_width_bps", "bid_width", default=mm_width)
                mm_ask_width = _cfg_int(mm, "ask_width_bps", "ask_width", default=mm_width)
                mm_resting_ticks = _cfg_int(mm, "resting_depth_extra_ticks", default=mm_resting_ticks)
                mm_basis = _cfg_float(mm, "basis", default=mm_basis)
                mm_max_reload_cycles = _cfg_int(mm, "max_reload_cycles", default=mm_max_reload_cycles)
                mm_current_reload_count = _cfg_int(mm, "current_reload_count", default=mm_current_reload_count)
                v_r = mm.get("reload_cycles_count_fill_only", mm_reload_cycles_count_fill_only)
                if isinstance(v_r, str):
                    mm_reload_cycles_count_fill_only = v_r.strip().lower() in ("1", "true", "yes", "on")
                else:
                    mm_reload_cycles_count_fill_only = bool(v_r)
                mm_reload_limit_reset_nonce = str(mm.get("reload_limit_reset_nonce", "") or "")
        except (OSError, json.JSONDecodeError):
            pass

    if CREDENTIALS_PATH.is_file():
        try:
            cr = json.loads(CREDENTIALS_PATH.read_text(encoding="utf-8"))
            ap = cr.get("api") if isinstance(cr.get("api"), dict) else {}
            rest = str(ap.get("rest_endpoint") or cr.get("rest_endpoint", rest) or rest)
            key = str(ap.get("api_key") or cr.get("api_key", key) or key)
            secret = str(ap.get("api_secret") or cr.get("api_secret", secret) or secret)
            session = str(ap.get("session_token") or cr.get("session_token", session) or session)
        except (OSError, json.JSONDecodeError):
            pass

    key = os.environ.get("ARCHITECT_API_KEY", key)
    secret = os.environ.get("ARCHITECT_API_SECRET", secret)
    session = os.environ.get("ARCHITECT_SESSION_TOKEN", session)

    ax_book_symbols = _desk_ax_book_symbols(dc, ax_symbol)
    ref_book_symbols = _desk_ref_book_symbols(dc, ref_symbol)

    cme_url = ""
    cfe = dc.get("cme_feed") if isinstance(dc.get("cme_feed"), dict) else {}
    cme_url = str(cfe.get("rest_url") or "").strip()
    ext_final = dc.get("external_feed") if isinstance(dc.get("external_feed"), dict) else {}
    if _desk_is_multi_from_config(dc):
        reference_provider = "multi"
    elif neon_fix_feed.is_neon_fix_provider(str(ext_final.get("provider", ""))):
        reference_provider = "neon_fix"
    elif _is_hyperliquid_provider(str(ext_final.get("provider", ""))):
        reference_provider = "hyperliquid"
    else:
        reference_provider = "rest"
    external_feed_name = str(ext_final.get("name", "external") or "external")
    neon_fix_host = ""
    neon_fix_port = 0
    if reference_provider == "neon_fix":
        fix = ext_final.get("fix") if isinstance(ext_final.get("fix"), dict) else {}
        env_direct = os.environ.get("MM_DESK_NEON_DIRECT_TLS", "").strip().lower() in ("1", "true", "yes", "on")
        direct_tls = bool(fix.get("direct_tls", False)) or env_direct
        tls_rh = str(fix.get("tls_remote_host", "") or "").strip()
        try:
            tls_rp = int(fix.get("tls_remote_port", 0) or 0)
        except (TypeError, ValueError):
            tls_rp = 0
        if direct_tls and tls_rh and tls_rp > 0:
            neon_fix_host = tls_rh
            neon_fix_port = tls_rp
        else:
            neon_fix_host = str(fix.get("host", "127.0.0.1") or "127.0.0.1")
            try:
                neon_fix_port = int(fix.get("port", 14507) or 14507)
            except (TypeError, ValueError):
                neon_fix_port = 14507

    return {
        "rest_endpoint": rest,
        "ws_endpoint": ws,
        "api_key": key,
        "api_secret": secret,
        "session_token": session,
        "ref_symbol": ref_symbol,
        "ref_display_symbol": ref_display,
        "ref_rest_url": ref_rest_url,
        "ref_rest_uses_spot_path": ref_rest_uses_spot_path,
        "config_external_provider": str(ext_final.get("provider") or "neon_fix"),
        "ax_symbol": ax_symbol,
        "ax_book_symbols": ax_book_symbols,
        "ref_book_symbols": ref_book_symbols,
        "mm_instruments": mm_instruments,
        "mm_width": mm_width,
        "mm_order_size": mm_order_size,
        "mm_order_size_step": mm_order_size_step,
        "mm_max_position": mm_max_position,
        "mm_adjust_position": mm_adjust_position,
        "mm_adjust_ticks": mm_adjust_ticks,
        "price_tick": price_tick,
        "mm_bid_width": mm_bid_width,
        "mm_ask_width": mm_ask_width,
        "mm_resting_ticks": mm_resting_ticks,
        "mm_basis": mm_basis,
        "mm_max_reload_cycles": mm_max_reload_cycles,
        "mm_current_reload_count": mm_current_reload_count,
        "mm_reload_cycles_count_fill_only": mm_reload_cycles_count_fill_only,
        "mm_reload_limit_reset_nonce": mm_reload_limit_reset_nonce,
        "mm_requote_on_theo_move": mm_requote_on_theo_move,
        "mm_orders_enabled": mm_orders_enabled,
        "mm_min_theo_move_ticks_to_requote": mm_min_theo_move_ticks_to_requote,
        "mm_pricing_tick": pricing_tick,
        "reference_provider": reference_provider,
        "multi_theo": (
            (bool((dc.get("market_maker") or {}).get("multi_theo")) if isinstance(dc.get("market_maker"), dict) else False)  # noqa: E501
            or (reference_provider == "multi")
        ),
        "cme_feed_rest_url": cme_url,
        "external_feed_name": external_feed_name,
        "neon_fix_host": neon_fix_host,
        "neon_fix_port": neon_fix_port,
    }


class DeskSharedState:
    def __init__(self, cfg: dict) -> None:
        self.lock = threading.RLock()
        self.rest_endpoint = str(cfg["rest_endpoint"]).strip()
        self.ws_endpoint = str(cfg.get("ws_endpoint") or "").strip()
        self.ax_symbol = str(cfg.get("ax_symbol") or DEFAULT_AX_SYMBOL).strip()
        self.ref_symbol = str(cfg["ref_symbol"]).strip()
        self.ref_display = str(cfg.get("ref_display_symbol") or "").strip()
        self.ref_rest_url = str(cfg.get("ref_rest_url") or "").strip()
        self.ref_rest_uses_spot_path = bool(cfg.get("ref_rest_uses_spot_path", True))
        self.api_key = str(cfg.get("api_key") or "")
        self.api_secret = str(cfg.get("api_secret") or "")
        self.mm_width = int(cfg.get("mm_width", 10))
        self.mm_order_size = int(cfg.get("mm_order_size", 200))
        self.mm_order_size_step = max(1, int(cfg.get("mm_order_size_step", 1)))
        self.mm_max_position = int(cfg.get("mm_max_position", 1000000))
        self.mm_adjust_position = int(cfg.get("mm_adjust_position", 50))
        self.mm_adjust_ticks = int(cfg.get("mm_adjust_ticks", 2))
        self.price_tick = float(cfg.get("price_tick", PRICE_TICK))
        self.mm_bid_width = int(cfg.get("mm_bid_width", self.mm_width))
        self.mm_ask_width = int(cfg.get("mm_ask_width", self.mm_width))
        self.mm_resting_ticks = int(cfg.get("mm_resting_ticks", 0))
        self.mm_basis = float(cfg.get("mm_basis", 0.0))
        self.mm_max_reload_cycles = int(cfg.get("mm_max_reload_cycles", 100))
        self.mm_current_reload_count = int(cfg.get("mm_current_reload_count", 0))
        self.mm_reload_cycles_count_fill_only = bool(cfg.get("mm_reload_cycles_count_fill_only", True))
        self.mm_reload_limit_reset_nonce = str(cfg.get("mm_reload_limit_reset_nonce") or "")
        self.mm_requote_on_theo_move = bool(cfg.get("mm_requote_on_theo_move", False))
        self.mm_orders_enabled = bool(cfg.get("mm_orders_enabled", True))
        self.mm_min_theo_move_ticks_to_requote = _clamp_min_theo_move_ticks_to_requote(
            int(cfg.get("mm_min_theo_move_ticks_to_requote", 2))
        )
        self.mm_pricing_tick = float(cfg.get("mm_pricing_tick", self.price_tick))
        self.reference_provider = str(cfg.get("reference_provider") or "rest")
        self.multi_theo = bool(cfg.get("multi_theo", False))
        self.cme_feed_rest_url = str(cfg.get("cme_feed_rest_url") or "").strip()
        self.config_external_provider = str(cfg.get("config_external_provider") or "neon_fix")
        self.external_feed_name = str(cfg.get("external_feed_name") or "external")
        self.neon_fix_host = str(cfg.get("neon_fix_host") or "")
        self.neon_fix_port = int(cfg.get("neon_fix_port", 0) or 0)
        self.reference_error = ""
        self._neon_sock_lock = threading.Lock()
        self._neon_fix_sock: socket.socket | None = None
        tok = str(cfg.get("session_token") or "").strip()
        self.token: str | None = tok or None
        absyms = cfg.get("ax_book_symbols")
        if isinstance(absyms, list) and absyms:
            self.ax_book_symbols = [str(x).strip() for x in absyms if str(x).strip()]
        else:
            self.ax_book_symbols = [self.ax_symbol] if self.ax_symbol else []
        bns = cfg.get("ref_book_symbols")
        if isinstance(bns, list) and bns:
            if self.reference_provider in ("neon_fix", "multi"):
                self.ref_book_symbols = [str(x).strip() for x in bns if str(x).strip()]
            else:
                self.ref_book_symbols = [str(x).strip().upper() for x in bns if str(x).strip()]
        else:
            if self.reference_provider in ("neon_fix", "multi"):
                self.ref_book_symbols = [self.ref_symbol] if self.ref_symbol else []
            else:
                self.ref_book_symbols = [self.ref_symbol.upper()] if self.ref_symbol else []

        self.ax_grid_symbols: list[str] = list(self.ax_book_symbols)
        self.mm_instruments: list[dict] = list(cfg.get("mm_instruments") or [])
        self.last_ref_book: tuple[list, list] | None = None
        self.last_ax_book: tuple[list, list] | None = None
        self.my_prices: set[float] = set()
        self.my_prices_by_symbol: dict[str, set[float]] = {}
        self.fills_snapshot: list[dict] = []
        self.orders_snapshot: list[dict] = []
        self.position_line = "Position: —"
        self.position_snapshot: dict = {}
        self.stats_line = "Open orders: — | Loaded fills: —"
        self.ref_latency_text = "Neon FIX: —" if self.reference_provider == "neon_fix" else "Reference REST: —"
        self.ax_latency_text = "AX REST: —"
        self.status_line = "Starting…"
        self.tape_lines: list[str] = []
        self.feeds_running = False
        self.config_last_write: str | None = None
        self.ax_all_books: dict[str, dict] = {}
        self.ref_all_books: dict[str, dict] = {}
        self.ax_markets_error: str | None = None
        self.ax_markets_count: int = 0
        self._diag_log: list[str] = []
        self.ref_feed_mode: str = "rest"
        self.ax_all_books_updated_ms: int = 0
        self.ref_all_books_updated_ms: int = 0
        self._ax_skip_log_ts: float = 0.0
        self._ax_skip_log_key: tuple[str, ...] = ()
        self._ax_no_bearer_log_ts: float = -1e9
        self.orders_api_last_ok: bool = True
        self.fills_api_last_ok: bool = True
        self.account_api_detail: str = ""
        self.desk_last_action: str = ""
        self.desk_last_action_ms: int = 0
        self.neon_catalog_symbols: list[str] = []
        self.neon_security_list_updated_ms: int = 0
        self.web_desk_run_reference_feed = _web_desk_run_reference_feed_from_env(self.reference_provider)
        # Architect GET /instruments → minimum price increment per symbol (no config defaults on MM paths).
        self.ax_tick_by_symbol: dict[str, float] = {}
        self.ax_tick_map_error: str = ""
        self.ax_tick_map_updated_ms: int = 0
        self._ax_tick_map_refresh_mono: float = 0.0

    def norm_price(self, p: float) -> float:
        with self.lock:
            sym = (self.ax_symbol or "").strip()
        gt = _gateway_tick_for_ax(self, sym)
        if gt is None or float(gt) <= 0.0 or not math.isfinite(float(gt)):
            return float(p)
        t = float(gt)
        return round(round(float(p) / t) * t, 10)

    def snapshot_json(self) -> dict:
        desk_reload_config_into_state(self)
        _sync_mm_trading_flags_from_primary_file(self)
        _cfg_mtime, _cfg_abs = _primary_config_stat()
        _dc_snap = get_merged_config_dict()
        with self.lock:
            _need_cpp_depth = self.reference_provider in (
                "neon_fix",
                "multi",
            ) and not self.web_desk_run_reference_feed
        cpp_depth_dd = read_cpp_neon_depth_cache_for_desk() if _need_cpp_depth else None
        mm_preview = build_mm_order_preview_for_desk(self)
        mm_previews_all = build_mm_order_previews_for_all_legs(self)
        with self.lock:
            _gate_fields = mm_position_gate_fields_from_preview(mm_preview, self.mm_max_position)
            bb, ba = ([], [])
            if self.last_ref_book is not None:
                b0, a0 = self.last_ref_book[0], self.last_ref_book[1]
                if b0 or a0:
                    bb = [[float(p), float(s)] for p, s in b0[:BOOK_DEPTH]]
                    ba = [[float(p), float(s)] for p, s in a0[:BOOK_DEPTH]]
            ab, aa = ([], [])
            if self.last_ax_book is not None:
                x0, y0 = self.last_ax_book[0], self.last_ax_book[1]
                if x0 or y0:
                    ab = [[float(p), float(s)] for p, s in x0[:BOOK_DEPTH]]
                    aa = [[float(p), float(s)] for p, s in y0[:BOOK_DEPTH]]
            # Main AX panel: X/Primary fetch and multi-book sometimes diverge — fall back to ax_all_books.
            if (not ab and not aa) and self.ax_all_books:
                sym0 = self.ax_symbol.strip()
                d = self.ax_all_books.get(sym0) or {}
                if not d.get("_err"):
                    rawb, rawa = d.get("bids") or [], d.get("asks") or []
                    ab = [list(r) for r in rawb][:BOOK_DEPTH]
                    aa = [list(r) for r in rawa][:BOOK_DEPTH]
            if (not bb and not ba) and self.ref_all_books:
                symb = self.ref_symbol.strip()
                d2 = self.ref_all_books.get(symb) or self.ref_all_books.get(symb.upper()) or {}
                if not d2.get("_err"):
                    rawb, rawa = d2.get("bids") or [], d2.get("asks") or []
                    bb = [list(r) for r in rawb][:BOOK_DEPTH]
                    ba = [list(r) for r in rawa][:BOOK_DEPTH]
            mine = sorted(self.my_prices)
            key = (self.api_key or "").strip()
            masked = ""
            if len(key) > 4:
                masked = "…" + key[-4:]
            elif key:
                masked = "(set)"
            arch_configured = bool(self.token) or (bool(key) and bool((self.api_secret or "").strip()))
            sym_ax_q = self.ax_symbol.strip()
            dg_ax = self.ax_all_books.get(sym_ax_q) if sym_ax_q else None
            qm_ax = None
            if dg_ax and not dg_ax.get("_err"):
                qm_ax = _top_of_book_metrics(dg_ax.get("bids") or [], dg_ax.get("asks") or [])
            if qm_ax is None and ab and aa:
                qm_ax = _top_of_book_metrics(ab, aa)
            sym_ref_q = self.ref_symbol.strip()
            dg_bn = (
                (self.ref_all_books.get(sym_ref_q) or self.ref_all_books.get(sym_ref_q.upper()))
                if sym_ref_q
                else None
            )
            qm_bn = None
            if dg_bn and not dg_bn.get("_err"):
                qm_bn = _top_of_book_metrics(dg_bn.get("bids") or [], dg_bn.get("asks") or [])
            if qm_bn is None and bb and ba:
                qm_bn = _top_of_book_metrics(bb, ba)
            prim_q = self.ax_symbol.strip()
            desk_snap = _dc_snap.get("mm_desk") if isinstance(_dc_snap.get("mm_desk"), dict) else {}
            explicit_depth_ms = bool(desk_snap.get("ax_architect_depth_multiselect_explicit"))
            file_ob_syms = _desk_ax_orderbook_file_list(_dc_snap)
            if file_ob_syms is None:
                leg_ax: set[str] = set()
                for r in self.mm_instruments or []:
                    if not isinstance(r, dict):
                        continue
                    for _k in ("symbol", "ax_symbol", "order_symbol"):
                        t = str(r.get(_k) or "").strip()
                        if t:
                            leg_ax.add(t)
                mm_top = _dc_snap.get("market_maker") if isinstance(_dc_snap.get("market_maker"), dict) else {}
                for _k in ("symbol", "order_symbol"):
                    t = str(mm_top.get(_k) or "").strip()
                    if t:
                        leg_ax.add(t)
                ax_ob_extras = [
                    str(x).strip()
                    for x in self.ax_book_symbols
                    if str(x).strip() and str(x).strip() not in leg_ax
                ]
            elif explicit_depth_ms:
                # Empty file + explicit = MM fallback for depth; surface effective list so UI matches polled books.
                ax_ob_extras = list(file_ob_syms) if file_ob_syms else list(self.ax_book_symbols)
            else:
                ax_ob_extras = _desk_saved_ax_orderbook_multiselect_extras(_dc_snap, prim_q) or []
            # Depth grid reads ref_all_books / ax_all_books only. Merge primary ladders from last_*_book / fallbacks
            # when multi-symbol maps are empty or missing the primary row (race with WS, instruments filter, etc.).
            bn_all_out = dict(self.ref_all_books)
            if sym_ref_q and (bb or ba):
                cur_bn = bn_all_out.get(sym_ref_q) or bn_all_out.get(sym_ref_q.upper())
                if (
                    cur_bn is None
                    or cur_bn.get("_err")
                    or (not (cur_bn.get("bids") or []) and not (cur_bn.get("asks") or []))
                ):
                    bn_all_out = {**bn_all_out, sym_ref_q: {"bids": bb, "asks": ba}}
            ax_all_out = dict(self.ax_all_books)
            if sym_ax_q and (ab or aa):
                cur_ax = ax_all_out.get(sym_ax_q)
                if (
                    cur_ax is None
                    or cur_ax.get("_err")
                    or (not (cur_ax.get("bids") or []) and not (cur_ax.get("asks") or []))
                ):
                    ax_all_out = {**ax_all_out, sym_ax_q: {"bids": ab, "asks": aa}}
            show_ref_depth_col = self.web_desk_run_reference_feed or self.reference_provider in (
                "neon_fix",
                "multi",
            ) or self.multi_theo
            neon_cpp_file = self.reference_provider in ("neon_fix", "multi") and not self.web_desk_run_reference_feed
            cpp_depth_age_sec: float | None = None
            if neon_cpp_file:
                dd = cpp_depth_dd
                if dd:
                    nb2, na2 = _normalize_fix_depth_ladder(dd.get("bids"), dd.get("asks"))
                    if nb2 and na2:
                        cpp_book = {"bids": nb2, "asks": na2}
                        # mm_external_depth.json holds the L2 ladder for ONE
                        # symbol only (the C++ trading_client's primary
                        # external_feed.symbol, recorded in `fix_symbol`).
                        # Broadcasting that ladder to every leg in
                        # ref_book_symbols caused other legs' widgets to
                        # display the primary's prices — e.g. gold prices
                        # under USD/JPY. Match the depth to its owning
                        # symbol by canonical form ("USD/JPY" == "USDJPY")
                        # and fall back to the desk primary only when the
                        # file lacks fix_symbol.
                        syms = [str(x).strip() for x in self.ref_book_symbols if str(x).strip()]
                        if not syms and self.ref_symbol.strip():
                            syms = [self.ref_symbol.strip()]
                        depth_fix_sym = str(dd.get("fix_symbol") or "").strip()
                        depth_canon = fix_symbol_canonical_py(depth_fix_sym) if depth_fix_sym else ""
                        target_syms: list[str] = []
                        if depth_canon:
                            for sx in syms:
                                if fix_symbol_canonical_py(sx) == depth_canon:
                                    target_syms.append(sx)
                        if not target_syms:
                            primary = self.ref_symbol.strip()
                            primary_canon = fix_symbol_canonical_py(primary) if primary else ""
                            if primary_canon and (not depth_canon or primary_canon == depth_canon):
                                for sx in syms:
                                    if fix_symbol_canonical_py(sx) == primary_canon:
                                        target_syms.append(sx)
                        if not target_syms and depth_fix_sym:
                            target_syms = [depth_fix_sym]
                        for sx in target_syms:
                            bn_all_out = {**bn_all_out, sx: cpp_book}
                        ums = dd.get("updated_ms")
                        try:
                            if ums is not None:
                                cpp_depth_age_sec = max(
                                    0.0, (time.time() * 1000.0 - float(ums)) / 1000.0
                                )
                        except (TypeError, ValueError):
                            pass
                        dg_bn2 = (
                            (bn_all_out.get(sym_ref_q) or bn_all_out.get(sym_ref_q.upper()))
                            if sym_ref_q
                            else None
                        )
                        if dg_bn2 and not dg_bn2.get("_err"):
                            qm_bn = _top_of_book_metrics(
                                dg_bn2.get("bids") or [], dg_bn2.get("asks") or []
                            )
            ref_row_b, ref_row_a = len(bb), len(ba)
            if sym_ref_q:
                dfin = bn_all_out.get(sym_ref_q) or bn_all_out.get(sym_ref_q.upper())
                if dfin and not dfin.get("_err"):
                    ref_row_b = len(dfin.get("bids") or [])
                    ref_row_a = len(dfin.get("asks") or [])
            if self.reference_provider == "neon_fix":
                if self.web_desk_run_reference_feed:
                    data_feed_note = (
                        f"Neon FIX from this desk process (TOB or depth per feed settings; Security List when enabled). "
                        f"Architect /book every {MM_DESK_AX_BOOK_POLL_SEC:g}s; orders/fills every {MM_DESK_ACCOUNT_POLL_SEC:g}s."
                    )
                else:
                    data_feed_note = (
                        f"Neon column: C++ trading_client writes logs/mm_external_depth.json (top levels, ~1s) and "
                        f"mm_external_theo.json — run both from the same working directory. "
                        f"Set MM_LIVE_DESK_WEB_REFERENCE_FEED=1 to poll FIX in Python for symbol discovery (not while C++ holds the same Neon session). "
                        f"Architect every {MM_DESK_AX_BOOK_POLL_SEC:g}s; account every {MM_DESK_ACCOUNT_POLL_SEC:g}s."
                    )
            elif _is_hyperliquid_provider(self.reference_provider):
                data_feed_note = (
                    f"Hyperliquid reference via /info allMids every {REST_POLL_INTERVAL_SEC:g}s (l2Book optional). "
                    f"Desk falls back to synthetic TOB from mid when l2Book is unavailable for an asset alias. "
                    f"Architect every {MM_DESK_AX_BOOK_POLL_SEC:g}s; orders/fills every {MM_DESK_ACCOUNT_POLL_SEC:g}s."
                )
            else:
                data_feed_note = (
                    (
                        f"Reference REST depth every {REST_POLL_INTERVAL_SEC:g}s (external_feed.rest_url + symbol). "
                        f"Architect every {MM_DESK_AX_BOOK_POLL_SEC:g}s; orders/fills every {MM_DESK_ACCOUNT_POLL_SEC:g}s."
                    )
                    if self.web_desk_run_reference_feed
                    else (
                        f"Architect /book only (no reference column). "
                        f"Books every {MM_DESK_AX_BOOK_POLL_SEC:g}s; account every {MM_DESK_ACCOUNT_POLL_SEC:g}s."
                    )
                )
            config_editor = load_config_editor_snapshot()
            feed_health = load_mm_feed_health(_dc_snap)
            live_orders = load_mm_active_orders(_dc_snap)
            mm_orders_config = load_mm_orders_config(_dc_snap)
            live_orders = _enrich_live_orders_with_orders_json_move_flags(
                mm_orders_config, live_orders
            )

            # Expose every C++ external-theo midpoint to the browser.
            # Neon multi-symbol quotes are stored in mm_external_theo.json under
            # quotes_by_canonical and may not have corresponding ref_all_books rows.
            cpp_theo_cache = read_cpp_theo_cache_for_desk() or {}
            cpp_quotes = (
                cpp_theo_cache.get("quotes_by_canonical")
                if isinstance(cpp_theo_cache, dict)
                else {}
            )

            theo_mids_by_symbol: dict[str, float] = {}

            if isinstance(cpp_quotes, dict):
                for quote_symbol, quote_data in cpp_quotes.items():
                    if not isinstance(quote_data, dict):
                        continue

                    quote_mid = _to_float(quote_data.get("mid"))

                    if (
                        quote_mid is not None
                        and math.isfinite(quote_mid)
                        and quote_mid > 0
                    ):
                        theo_mids_by_symbol[str(quote_symbol)] = float(
                            quote_mid
                        )
            return {
                "rest_endpoint": self.rest_endpoint,
                "ws_endpoint": self.ws_endpoint or "(from default_config)",
                "ax_symbol": self.ax_symbol,
                "ref_symbol": self.ref_symbol,
                "ref_display_symbol": self.ref_display,
                "ref_rest_url": self.ref_rest_url,
                "price_tick": self.price_tick,
                "mm_pricing_tick": self.mm_pricing_tick,
                "mm_requote_on_theo_move": self.mm_requote_on_theo_move,
                "mm_orders_enabled": self.mm_orders_enabled,
                "mm_min_theo_move_ticks_to_requote": self.mm_min_theo_move_ticks_to_requote,
                "mm_width": self.mm_width,
                "mm_order_size": self.mm_order_size,
                "mm_order_size_step": self.mm_order_size_step,
                "mm_max_position": self.mm_max_position,
                "mm_adjust_position": self.mm_adjust_position,
                "mm_adjust_ticks": self.mm_adjust_ticks,
                "mm_bid_width": self.mm_bid_width,
                "mm_ask_width": self.mm_ask_width,
                "mm_resting_ticks": self.mm_resting_ticks,
                "mm_basis": self.mm_basis,
                "mm_max_reload_cycles": self.mm_max_reload_cycles,
                "mm_current_reload_count": self.mm_current_reload_count,
                "mm_reload_cycles_count_fill_only": self.mm_reload_cycles_count_fill_only,
                "mm_reload_limit_reset_nonce": self.mm_reload_limit_reset_nonce,
                "prefill_api_key": self.api_key,
                "prefill_api_secret": self.api_secret,
                "config_paths": {
                    "default_config": str(DEFAULT_CONFIG_PATH),
                    "credentials_local": str(CREDENTIALS_PATH),
                },
                "default_config_abs_path": _cfg_abs,
                "default_config_mtime_ms": _cfg_mtime,
                "has_token": bool(self.token),
                "architect_credentials_configured": arch_configured,
                "masked_api_key": masked,
                "feeds_running": self.feeds_running,
                "status_line": self.status_line,
                "ref_latency_text": self.ref_latency_text,
                "ax_latency_text": self.ax_latency_text,
                "position_line": self.position_line,
                "position_snapshot": dict(self.position_snapshot),
                "stats_line": self.stats_line,
                "book_reference": {"bids": bb, "asks": ba},
                "book_ax": {"bids": ab, "asks": aa},
                "ref_all_books": bn_all_out,
                "theo_mids_by_symbol": theo_mids_by_symbol,
                "ref_book_sources": {
                    str(ref_sym): _theo_source_for_ref_in_mm(self, str(ref_sym))
                    for ref_sym in self.ref_book_symbols
                    if str(ref_sym).strip()
                },
                "ref_book_symbols": list(self.ref_book_symbols),
                "ref_feed_mode": self.ref_feed_mode,
                "ax_feed_mode": "rest",
                "reference_provider": self.reference_provider,
                "external_feed_name": self.external_feed_name,
                "reference_error": self.reference_error,
                "reference_venue_label": (
                    f"Multi theo: Neon + HL + Mettraders ({self.external_feed_name})"
                    if (self.reference_provider == "multi" or self.multi_theo)
                    else (
                        f"Neon FIX ({self.external_feed_name})"
                        if self.reference_provider == "neon_fix"
                        else (
                            "Hyperliquid"
                            if _is_hyperliquid_provider(self.reference_provider)
                            else "External REST"
                        )
                    )
                ),
                "reference_feed_pill": (
                    "Multi"
                    if (self.reference_provider == "multi" or self.multi_theo)
                    else (
                        "Neon"
                        if self.reference_provider == "neon_fix"
                        else (
                            "Hyperliquid" if _is_hyperliquid_provider(self.reference_provider) else "REST"
                        )
                    )
                ),
                "reference_metrics_tag": (
                    "MT3"
                    if (self.reference_provider == "multi" or self.multi_theo)
                    else (
                        "NFX"
                        if self.reference_provider == "neon_fix"
                        else ("HLQ" if _is_hyperliquid_provider(self.reference_provider) else "REF")
                    )
                ),
                "reference_tcp_endpoint": (
                    f"{self.neon_fix_host}:{self.neon_fix_port}"
                    if self.reference_provider in ("neon_fix", "multi") and self.neon_fix_host
                    else ""
                ),
                "theo_leg_choices": _theo_leg_choices_from_ax_master(
                    self.ax_book_symbols,
                    self.ax_grid_symbols,
                    list(self.mm_instruments or []),
                    self.ref_symbol,
                ),
                "multi_theo": self.multi_theo,
                "data_feed_note": data_feed_note,
                "show_reference_depth_col": show_ref_depth_col,
                "neon_reference_from_cpp_file": neon_cpp_file,
                "cpp_neon_depth_age_sec": cpp_depth_age_sec,
                "server_time_ms": int(time.time() * 1000),
                "my_prices": mine,
                "mine_by_symbol": {k: sorted(v) for k, v in self.my_prices_by_symbol.items()},
                "fills": self.fills_snapshot[:FILLS_LIMIT],
                "orders": self.orders_snapshot[:ORDERS_LIMIT],
                "tape": self.tape_lines,
                "poll_sec": REST_POLL_INTERVAL_SEC,
                "ax_book_poll_sec": MM_DESK_AX_BOOK_POLL_SEC,
                "account_poll_sec": MM_DESK_ACCOUNT_POLL_SEC,
                "ax_poll_sec": MM_DESK_AX_BOOK_POLL_SEC,
                "orders_api_ok": self.orders_api_last_ok,
                "fills_api_ok": self.fills_api_last_ok,
                "account_api_detail": self.account_api_detail,
                "ui_poll_ms": int(os.environ.get("MM_DESK_UI_POLL_MS", "400")),
                "ui_poll_min_ms": int(os.environ.get("MM_DESK_UI_POLL_MIN_MS", "150")),
                "web_desk_reference_feed": self.web_desk_run_reference_feed,
                "book_row_counts": {
                    "ref_bids": ref_row_b,
                    "ref_asks": ref_row_a,
                    "ax_bids": len(ab),
                    "ax_asks": len(aa),
                },
                "config_last_write": self.config_last_write,
                "ax_all_books": ax_all_out,
                "ax_markets_error": self.ax_markets_error,
                "ax_markets_count": self.ax_markets_count,
                "ax_book_symbols": list(self.ax_book_symbols),
                "ax_orderbook_extras": ax_ob_extras,
                "ax_architect_depth_multiselect_explicit": explicit_depth_ms,
                "ax_grid_symbols": list(self.ax_grid_symbols),
                "mm_instruments": list(self.mm_instruments),
                "neon_fix_md_symbols": _neon_md_symbols_from_merged(_dc_snap),
                "neon_product_options": neon_product_dropdown_union(_dc_snap, list(self.neon_catalog_symbols)),
                "neon_security_list_updated_ms": int(self.neon_security_list_updated_ms),
                "ax_books_updated_ms": int(self.ax_all_books_updated_ms),
                "ref_books_updated_ms": int(self.ref_all_books_updated_ms),
                "quote_metrics": {
                    "architect": qm_ax,
                    "reference": qm_bn,
                    "ax_symbol": sym_ax_q,
                    "ref_symbol": sym_ref_q,
                },
                "desk_last_action": self.desk_last_action,
                "desk_last_action_ms": int(self.desk_last_action_ms),
                "mm_order_preview": mm_preview,
                "mm_order_previews_by_symbol": mm_previews_all,
                "config_editor": config_editor,
                "feed_health": feed_health,
                "live_orders": live_orders,
                "mm_orders_config": mm_orders_config,
                **_gate_fields,
            }


def desk_log(st: DeskSharedState | None, msg: str, *, verbose_only: bool = False) -> None:
    """Print to stdout and optionally append to an in-memory tail (terminal / verbose only — not sent to the browser)."""
    if verbose_only and not MM_DESK_VERBOSE:
        return
    ts = time.strftime("%H:%M:%S")
    line = f"{ts} {msg}"
    print(f"[mm_live_desk] {line}", flush=True)
    if st is not None:
        with st.lock:
            st._diag_log.append(line)
            if len(st._diag_log) > 120:
                st._diag_log = st._diag_log[-120:]


def log_mm_desk_order_trace(st: DeskSharedState | None, title: str, rows: list[tuple[str, object]]) -> None:
    """Console-only trace: variable names and values used before gateway place_order (web desk + optional Tk)."""
    desk_log(st, f"======== MM order trace: {title} ========")
    for k, v in rows:
        desk_log(st, f"  {k} = {v!r}")
    desk_log(st, "======== end MM order trace ========")


def _mm_skew_intermediate_for_log(
    adjusted_theo: float,
    bid_spread_bps: int,
    ask_spread_bps: int,
    quote_tick: float,
    net_po: int,
    adjust_po: int,
    adjust_ticks: int,
    max_po: int,
) -> dict[str, object]:
    """Same intermediate numbers as compute_skewed_raw_bid_ask (for logging only).

    BPS-NATIVE: spread inputs are basis points of pricing mid (1 bp = 0.0001), matching the actual
    pricing math in compute_skewed_raw_bid_ask. The previous implementation used a stale
    tick-based formula (rb = adjusted_theo - bid_spread*quote_tick - sp), which produced
    misleading log values that scaled with quote_tick instead of with adjusted_theo — most
    visibly wrong on higher-priced instruments. Same code path runs for every symbol; only inputs
    differ.
    """
    ap = max(1, int(adjust_po))
    at = max(1, int(adjust_ticks))
    mp = max(1, int(max_po))
    skew_gate = abs(int(net_po)) < mp
    # Symmetric long/short skew (2026-05-21): mirror C++
    # MakeMarketStrategy::computeInventorySkewedRawBidAsk so log values match
    # actual placed prices. See docstring on compute_skewed_raw_bid_ask above.
    if skew_gate:
        _np_i = int(net_po)
        _abs_np = abs(_np_i)
        _np_sign = 1 if _np_i > 0 else (-1 if _np_i < 0 else 0)
        stu = math.floor(_abs_np / ap) * _np_sign * at
    else:
        stu = 0.0
    sp = float(stu) * float(quote_tick)
    bid_offset = float(adjusted_theo) * (float(bid_spread_bps) / 10000.0)
    ask_offset = float(adjusted_theo) * (float(ask_spread_bps) / 10000.0)
    rb = float(adjusted_theo) - bid_offset - sp
    ra = float(adjusted_theo) + ask_offset - sp
    expected_raw_span_px = bid_offset + ask_offset
    return {
        "skew_gate_abs_net_lt_max": skew_gate,
        "skew_tick_units": stu,
        "skew_price": sp,
        "bid_offset_px": bid_offset,
        "ask_offset_px": ask_offset,
        "expected_raw_span_px": expected_raw_span_px,
        "raw_bid": rb,
        "raw_ask": ra,
        "adjust_ticks": int(at),
    }


def desk_register_neon_fix_socket(st: DeskSharedState, sock: socket.socket | None) -> None:
    """Neon FIX thread registers its TCP socket so shutdown can close it and unblock recv()."""
    with st._neon_sock_lock:
        st._neon_fix_sock = sock


def desk_interrupt_neon_fix_socket(st: DeskSharedState) -> None:
    """Close the active Neon→stunnel socket (if any) so feed threads exit promptly on Ctrl+C."""
    with st._neon_sock_lock:
        s = st._neon_fix_sock
        st._neon_fix_sock = None
    if s is not None:
        try:
            s.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        try:
            s.close()
        except OSError:
            pass


def _atomic_write_json(path: Path, obj: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = Path(_orders_json_unique_tmp(str(path)))
    tmp.write_text(json.dumps(obj, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    os.replace(tmp, path)


def _default_config_mutate_write(
    mutate_fn, *, _skip_key_drop_guard: bool = False
) -> tuple[bool, str]:
    """Lock-serialized, atomic read-modify-write of ``default_config.json``.

    ``default_config.json`` holds the ENTIRE engine config; a truncation/clobber is
    strictly worse than the July orders.json incident. It is written by the C++ engine
    (atomic ``Config::saveToFile`` at startup) and hand-edited by operators, and read live
    by the engine for the per-instrument ``mm_orders_enabled`` gate. Every desk write MUST
    go through here so it is:

      * SERIALIZED — under ``_DEFAULT_CONFIG_WRITE_LOCK`` so two desk HTTP workers cannot
        interleave a read-modify-write on the same doc.
      * READ-MODIFY-WRITE — load the whole current doc from disk, mutate only the targeted
        keys, write the whole doc back (never serialize a partial in-memory view).
      * UNREADABLE-REFUSE — if the on-disk file is missing / unreadable / unparseable,
        REFUSE. A torn read (e.g. mid another writer) must never be atomically republished
        as a truncated document. This is the direct analogue of the orders.json fix.
      * TOP-LEVEL-KEY-DROP GUARD — the doc written must retain every top-level key present
        on disk; refuse otherwise (belt-and-suspenders on top of pure RMW: catches a
        mutate_fn bug or a doc loaded from a partial read that parsed by luck).

    ``mutate_fn(dc)`` mutates ``dc`` in place and returns ``(ok, msg)``; the file is written
    only when ``ok`` is True. The atomic publish itself is ``_atomic_write_json`` (unique
    temp + ``os.replace``). No generation/CAS scheme is layered on this file: the engine
    writes it without gen awareness and operators hand-edit it, so a gen counter would give
    false safety; the catastrophic corruption/key-loss modes are already closed by the
    guards above, and lost-update on a concurrent toggle is non-catastrophic (and made
    visible by the engine->desk hold acknowledgement).
    """
    with _DEFAULT_CONFIG_WRITE_LOCK:
        if not DEFAULT_CONFIG_PATH.is_file():
            return False, f"missing {DEFAULT_CONFIG_PATH.name}"
        try:
            raw = DEFAULT_CONFIG_PATH.read_text(encoding="utf-8")
        except OSError as exc:
            msg = (
                f"refused write to {DEFAULT_CONFIG_PATH.name}: on-disk file could not be "
                f"read ({exc}) — refusing to overwrite (avoids clobbering the whole engine "
                f"config on a transient read failure)"
            )
            print(f"[mm_live_desk] DEFAULT_CONFIG_UNREADABLE_GUARD {msg}", flush=True)
            return False, msg
        try:
            dc = json.loads(raw)
        except (json.JSONDecodeError, ValueError) as exc:
            msg = (
                f"refused write to {DEFAULT_CONFIG_PATH.name}: on-disk file is unparseable "
                f"({exc}) — refusing to republish a torn read as a truncated document"
            )
            print(f"[mm_live_desk] DEFAULT_CONFIG_UNPARSEABLE_GUARD {msg}", flush=True)
            return False, msg
        if not isinstance(dc, dict):
            return False, f"{DEFAULT_CONFIG_PATH.name} root must be an object"
        disk_top_keys = set(dc.keys())
        try:
            ok, msg = mutate_fn(dc)
        except Exception as exc:  # noqa: BLE001 - a mutate bug must never crash the writer
            return False, str(exc)
        if not ok:
            return False, msg
        if not _skip_key_drop_guard:
            dropped = disk_top_keys - set(dc.keys())
            if dropped:
                gmsg = (
                    f"refused write to {DEFAULT_CONFIG_PATH.name}: would drop top-level "
                    f"key(s) {sorted(dropped)} present on disk — a targeted gate write must "
                    f"preserve the whole config"
                )
                print(f"[mm_live_desk] DEFAULT_CONFIG_KEY_DROP_GUARD {gmsg}", flush=True)
                return False, gmsg
        try:
            _atomic_write_json(DEFAULT_CONFIG_PATH, dc)
        except OSError as exc:
            return False, f"{DEFAULT_CONFIG_PATH.name} write failed: {exc}"
        return True, msg


def _json_scrub_non_finite(obj: object) -> object:
    """Replace NaN/±inf so json.dumps(..., allow_nan=False) cannot fail on /api/* snapshots."""
    if isinstance(obj, float):
        return obj if math.isfinite(obj) else None
    if isinstance(obj, dict):
        return {str(k): _json_scrub_non_finite(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_json_scrub_non_finite(v) for v in obj]
    if isinstance(obj, tuple):
        return [_json_scrub_non_finite(v) for v in obj]
    return obj


def _state_json_bytes(snap: dict) -> bytes:
    try:
        raw = json.dumps(snap, separators=(",", ":"), ensure_ascii=False, allow_nan=False)
    except (ValueError, TypeError):
        raw = json.dumps(
            _json_scrub_non_finite(snap), separators=(",", ":"), ensure_ascii=False, allow_nan=False
        )
    return raw.encode("utf-8")


def _safe_wfile_write(handler: BaseHTTPRequestHandler, data: bytes) -> None:
    """Ignore client disconnects during response body (avoids BrokenPipeError spam in logs)."""
    try:
        handler.wfile.write(data)
    except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
        pass


# MM sidebar / persisted strategy ints (product contract).
_MM_WIDTH_LO = 1
_MM_WIDTH_HI = 10**9
# Quantity-denominated fields are not product-limited (the exchange enforces margin).
# The only hard ceiling is the engine's 32-bit int storage for order_size / max_position /
# adjust_position, so we allow up to 2e9 (int32-safe) to accommodate large e.g. JPY sizes.
_MM_QTY_HI = 2_000_000_000
_MM_ORDER_SIZE_LO = 1
_MM_ORDER_SIZE_HI = _MM_QTY_HI
_MM_MAX_POSITION_LO = 1
_MM_MAX_POSITION_HI = _MM_QTY_HI
_MM_ADJUST_POSITION_LO = 1
_MM_ADJUST_POSITION_HI = _MM_QTY_HI
_MM_ADJUST_TICKS_LO = 1
_MM_ADJUST_TICKS_HI = 100
_MM_MAX_RELOAD_CYCLES_LO = 0
_MM_MAX_RELOAD_CYCLES_HI = 1_000_000
_MM_CURRENT_RELOAD_LO = 0
_MM_CURRENT_RELOAD_HI = 1_000_000
_MM_RELOAD_NONCE_MAX_LEN = 256


def _coerce_desk_mm_strict_int(label: str, raw: object) -> int:
    """Reject bool, float fractions, and non-numeric strings."""
    if raw is None:
        raise ValueError(f"{label} is required")
    if isinstance(raw, bool):
        raise ValueError(f"{label} must be an integer")
    if isinstance(raw, int):
        v = raw
    elif isinstance(raw, float):
        if not math.isfinite(raw) or raw != math.trunc(raw):
            raise ValueError(f"{label} must be an integer")
        v = int(raw)
    elif isinstance(raw, str):
        s = raw.strip()
        if not s:
            raise ValueError(f"{label} is required")
        if s.startswith(("+", "-")):
            raise ValueError(f"{label} must be a positive integer")
        if not s.isdigit():
            raise ValueError(f"{label} must be an integer")
        v = int(s)
    else:
        raise ValueError(f"{label} must be an integer")
    return v


def _coerce_reload_cycles_count_fill_only(raw: object) -> bool:
    if isinstance(raw, bool):
        return raw
    if isinstance(raw, (int, float)) and not isinstance(raw, bool):
        return int(raw) != 0
    if isinstance(raw, str):
        s = raw.strip().lower()
        if s in ("1", "true", "yes", "on"):
            return True
        if s in ("0", "false", "no", "off", ""):
            return False
    raise ValueError("reload_cycles_count_fill_only must be true or false")


def validate_mm_strategy_payload(body: dict) -> tuple[bool, str]:
    """
    When any MM numeric field is present in ``body``, it must be a strict integer in range:
    Width [1, 1e9], order_size / max_position / adjust_position [1, 2e9] (int32-safe; no product
    cap — the exchange enforces margin), adjust_ticks [1, 100].
    Omitted keys are skipped (e.g. login-only persist).
    """
    checks: list[tuple[str, str, int, int]] = [
        ("width_bps", "Width (bps)", _MM_WIDTH_LO, _MM_WIDTH_HI),
        ("order_size", "Order size", _MM_ORDER_SIZE_LO, _MM_ORDER_SIZE_HI),
        ("max_position", "Max position", _MM_MAX_POSITION_LO, _MM_MAX_POSITION_HI),
        ("adjust_position", "Adjust position", _MM_ADJUST_POSITION_LO, _MM_ADJUST_POSITION_HI),
        ("adjust_ticks", "Adjust ticks", _MM_ADJUST_TICKS_LO, _MM_ADJUST_TICKS_HI),
    ]
    for key, label, lo, hi in checks:
        if key not in body:
            continue
        raw = body[key]
        if raw is None or (isinstance(raw, str) and not str(raw).strip()):
            return False, f"{label} cannot be empty"
        try:
            v = _coerce_desk_mm_strict_int(label, raw)
        except ValueError as e:
            return False, str(e)
        if v < lo or v > hi:
            return False, f"{label} must be between {lo} and {hi} inclusive"
    if "max_reload_cycles" in body:
        raw = body["max_reload_cycles"]
        if raw is None or (isinstance(raw, str) and not str(raw).strip()):
            return False, "Max reload cycles cannot be empty"
        try:
            v = _coerce_desk_mm_strict_int("Max reload cycles", raw)
        except ValueError as e:
            return False, str(e)
        if v < _MM_MAX_RELOAD_CYCLES_LO or v > _MM_MAX_RELOAD_CYCLES_HI:
            return False, (
                f"Max reload cycles must be between {_MM_MAX_RELOAD_CYCLES_LO} and "
                f"{_MM_MAX_RELOAD_CYCLES_HI} inclusive (0 = unlimited in C++)"
            )
    if "current_reload_count" in body:
        raw = body["current_reload_count"]
        if raw is None or (isinstance(raw, str) and not str(raw).strip()):
            return False, "Current reload count cannot be empty"
        try:
            v = _coerce_desk_mm_strict_int("Current reload count", raw)
        except ValueError as e:
            return False, str(e)
        if v < _MM_CURRENT_RELOAD_LO or v > _MM_CURRENT_RELOAD_HI:
            return False, (
                f"Current reload count must be between {_MM_CURRENT_RELOAD_LO} and {_MM_CURRENT_RELOAD_HI} inclusive"
            )
    if "reload_limit_reset_nonce" in body:
        nv = body["reload_limit_reset_nonce"]
        if nv is not None and not isinstance(nv, (str, int, float, bool)):
            return False, "reload_limit_reset_nonce must be a string"
        s = "" if nv is None else str(nv)
        if "\n" in s or "\r" in s:
            return False, "reload_limit_reset_nonce cannot contain newlines"
        if len(s) > _MM_RELOAD_NONCE_MAX_LEN:
            return False, f"reload_limit_reset_nonce must be at most {_MM_RELOAD_NONCE_MAX_LEN} characters"
    if "min_theo_move_ticks_to_requote" in body:
        raw = body["min_theo_move_ticks_to_requote"]
        if raw is None or (isinstance(raw, str) and not str(raw).strip()):
            return False, "Min drift ticks cannot be empty"
        try:
            v = _coerce_desk_mm_strict_int("Min drift ticks (theo requote)", raw)
        except ValueError as e:
            return False, str(e)
        if v < 1 or v > 1_000_000:
            return False, "Min drift ticks must be between 1 and 1,000,000 inclusive"
    if "price_tick" in body and body["price_tick"] is not None:
        try:
            pt = float(body["price_tick"])
        except (TypeError, ValueError):
            return False, "price_tick must be a number"
        if pt <= 0.0 or pt > 1e12:
            return False, "price_tick must be > 0"
    if "pricing_tick" in body and body["pricing_tick"] is not None:
        try:
            pt2 = float(body["pricing_tick"])
        except (TypeError, ValueError):
            return False, "pricing_tick must be a number"
        if pt2 <= 0.0 or pt2 > 1e12:
            return False, "pricing_tick must be > 0"
    if "reload_cycles_count_fill_only" in body:
        try:
            _coerce_reload_cycles_count_fill_only(body["reload_cycles_count_fill_only"])
        except ValueError as e:
            return False, str(e)
    if "mm_orders_enabled" in body and body["mm_orders_enabled"] is not None:
        v = body["mm_orders_enabled"]
        if not isinstance(v, (bool, int, str, float)):
            return False, "mm_orders_enabled must be a boolean"
    return True, ""


def desk_apply_form_to_state(st: DeskSharedState, body: dict) -> None:
    """Update in-memory desk from a GUI / persist payload (same keys as web apply)."""
    with st.lock:
        if body.get("rest_endpoint"):
            st.rest_endpoint = str(body["rest_endpoint"]).strip().rstrip("/")
        if body.get("ax_symbol"):
            st.ax_symbol = str(body["ax_symbol"]).strip()
        _rs = body.get("ref_symbol") or body.get("bn_symbol")
        if _rs is not None and str(_rs).strip():
            st.ref_symbol = str(_rs).strip()
        if body.get("api_key"):
            st.api_key = str(body["api_key"]).strip()
        if body.get("api_secret"):
            st.api_secret = str(body["api_secret"]).strip()
        if body.get("width_bps") is not None:
            st.mm_width = int(body["width_bps"])
            st.mm_bid_width = st.mm_width
            st.mm_ask_width = st.mm_width
        if body.get("order_size") is not None:
            st.mm_order_size = int(body["order_size"])
        if body.get("max_position") is not None:
            st.mm_max_position = int(body["max_position"])
        if body.get("adjust_position") is not None:
            st.mm_adjust_position = int(body["adjust_position"])
        if body.get("adjust_ticks") is not None:
            st.mm_adjust_ticks = int(body["adjust_ticks"])
        if body.get("max_reload_cycles") is not None:
            st.mm_max_reload_cycles = int(body["max_reload_cycles"])
        if body.get("current_reload_count") is not None:
            st.mm_current_reload_count = int(body["current_reload_count"])
        if body.get("reload_cycles_count_fill_only") is not None:
            st.mm_reload_cycles_count_fill_only = _coerce_reload_cycles_count_fill_only(
                body["reload_cycles_count_fill_only"]
            )
        if "reload_limit_reset_nonce" in body:
            st.mm_reload_limit_reset_nonce = str(body.get("reload_limit_reset_nonce") or "")
        if body.get("requote_on_theo_move") is not None:
            st.mm_requote_on_theo_move = bool(body["requote_on_theo_move"])
        if body.get("mm_orders_enabled") is not None:
            st.mm_orders_enabled = bool(body["mm_orders_enabled"])
        if body.get("min_theo_move_ticks_to_requote") is not None:
            st.mm_min_theo_move_ticks_to_requote = _clamp_min_theo_move_ticks_to_requote(
                int(body["min_theo_move_ticks_to_requote"])
            )
        if body.get("price_tick") is not None:
            st.price_tick = float(body["price_tick"])
        if body.get("pricing_tick") is not None:
            st.mm_pricing_tick = float(body["pricing_tick"])


def persist_mm_orders_enabled_on_disk(enabled: bool) -> tuple[bool, str]:
    """Write ``market_maker.mm_orders_enabled`` to ``default_config.json`` (C++ reads on next MM tick)."""
    if not DEFAULT_CONFIG_PATH.is_file():
        return False, f"missing {DEFAULT_CONFIG_PATH.name}"
    try:
        dc = json.loads(DEFAULT_CONFIG_PATH.read_text(encoding="utf-8"))
        if not isinstance(dc, dict):
            return False, "default_config.json root must be an object"
        mm = dc.setdefault("market_maker", {})
        mm["mm_orders_enabled"] = bool(enabled)
        _atomic_write_json(DEFAULT_CONFIG_PATH, dc)
        return True, f"{DEFAULT_CONFIG_PATH.name}: market_maker.mm_orders_enabled={bool(enabled)}"
    except Exception as e:
        return False, str(e)


def persist_market_maker_gate_flags(body: dict, symbol: str = "") -> tuple[bool, str]:
    """
    Persist gate flags globally or per instrument.

    - No symbol: writes market_maker.{mm_orders_enabled,requote_on_theo_move}
    - symbol set: writes overrides on matching market_maker.instruments[] row
    """
    ok_v, err_v = validate_mm_strategy_payload(body)
    if not ok_v:
        return False, err_v
    if body.get("mm_orders_enabled") is None and body.get("requote_on_theo_move") is None:
        return False, "nothing to persist"

    sym = str(symbol or "").strip()

    def _mutate(dc: dict) -> tuple[bool, str]:
        mm = dc.setdefault("market_maker", {})
        if sym:
            def _norm(s: str) -> str:
                return "".join(str(s or "").split()).upper()

            want = _norm(sym)
            inst = mm.get("instruments")
            instruments_usable = isinstance(inst, list) and len(inst) > 0
            row = None
            if instruments_usable:
                for el in inst:
                    if not isinstance(el, dict):
                        continue
                    got = _norm(str(el.get("symbol") or el.get("ax_symbol") or ""))
                    if got and got == want:
                        row = el
                        break
            if row is not None:
                if body.get("mm_orders_enabled") is not None:
                    row["mm_orders_enabled"] = bool(body["mm_orders_enabled"])
                if body.get("requote_on_theo_move") is not None:
                    row["requote_on_theo_move"] = bool(body["requote_on_theo_move"])
                mm["instruments"] = inst
            else:
                legacy_sym = _norm(str(mm.get("symbol") or mm.get("order_symbol") or ""))
                if legacy_sym and legacy_sym == want:
                    if body.get("mm_orders_enabled") is not None:
                        mm["mm_orders_enabled"] = bool(body["mm_orders_enabled"])
                    if body.get("requote_on_theo_move") is not None:
                        mm["requote_on_theo_move"] = bool(body["requote_on_theo_move"])
                elif not instruments_usable:
                    return False, "market_maker.instruments must be a non-empty list for symbol-scoped gate updates"
                else:
                    return False, f"symbol {sym!r} not found in market_maker.instruments"
        else:
            if body.get("mm_orders_enabled") is not None:
                mm["mm_orders_enabled"] = bool(body["mm_orders_enabled"])
            if body.get("requote_on_theo_move") is not None:
                mm["requote_on_theo_move"] = bool(body["requote_on_theo_move"])
        parts = []
        if body.get("mm_orders_enabled") is not None:
            parts.append(f"mm_orders_enabled={bool(body['mm_orders_enabled'])}")
        if body.get("requote_on_theo_move") is not None:
            parts.append(f"requote_on_theo_move={bool(body['requote_on_theo_move'])}")
        if sym:
            return True, f"{DEFAULT_CONFIG_PATH.name}: instrument {sym} -> " + ", ".join(parts)
        return True, f"{DEFAULT_CONFIG_PATH.name}: " + ", ".join(parts)

    return _default_config_mutate_write(_mutate)


# --- Fast-market breaker (HL SPX) GUI config (Item 6, 2026-07-27) ---
# The engine reads market_maker.fast_market.* live per ~1 Hz tick (FastMarketMonitor::onMoverTick)
# after the mtime-gated primary-config reload, so a Save here is picked up within ~1-2s without a
# restart. Sane bounds guard against fat-finger values that would make the breaker useless or trip
# constantly.
_FAST_MARKET_INT_BOUNDS = {
    "size_of_move_ticks": (1, 100000),
    "time_of_move_sec": (1, 86400),
    "pull_sec": (1, 86400),
}
_FAST_MARKET_TICK_MAX = 1e6


def fast_market_config_get() -> dict:
    """Current ``market_maker.fast_market.*`` as written on disk (the engine's source of truth).
    Missing keys come back as defaults so the panel can always render read-only first."""
    fm: dict = {}
    try:
        dc = json.loads(DEFAULT_CONFIG_PATH.read_text(encoding="utf-8"))
        if isinstance(dc, dict):
            mm = dc.get("market_maker")
            if isinstance(mm, dict) and isinstance(mm.get("fast_market"), dict):
                fm = mm["fast_market"]
    except (OSError, json.JSONDecodeError):
        fm = {}

    def _i(k: str, d: int) -> int:
        try:
            return int(fm.get(k, d))
        except (TypeError, ValueError):
            return d

    def _f(k: str, d: float) -> float:
        try:
            return float(fm.get(k, d))
        except (TypeError, ValueError):
            return d

    def _b(v, d: bool = True) -> bool:
        if v is None:
            return d
        if isinstance(v, str):
            return v.strip().lower() in ("1", "true", "yes", "on")
        return bool(v)

    return {
        "ok": True,
        "hl_spx_symbol": str(fm.get("hl_spx_symbol", "") or ""),
        "size_of_move_ticks": _i("size_of_move_ticks", 0),
        "time_of_move_sec": _i("time_of_move_sec", 0),
        "pull_sec": _i("pull_sec", 10),
        "tick_size": _f("tick_size", 0.0),
        "enabled": _b(fm.get("enabled"), True),
    }


def _validate_fast_market_payload(body: dict) -> tuple[bool, str, dict]:
    """Reject anything that would make the breaker useless or misbehave. Returns
    (ok, message, normalised_dict)."""
    if not isinstance(body, dict):
        return False, "payload must be an object", {}
    sym = str(body.get("hl_spx_symbol", "") or "").strip()
    if not sym:
        return (
            False,
            "hl_spx_symbol must be non-empty — use the Enabled toggle to turn the breaker "
            "off (it keeps the symbol)",
            {},
        )
    out: dict = {"hl_spx_symbol": sym}
    for k, (lo, hi) in _FAST_MARKET_INT_BOUNDS.items():
        raw = body.get(k)
        try:
            v = int(raw)
        except (TypeError, ValueError):
            return False, f"{k} must be an integer", {}
        if v < lo or v > hi:
            return False, f"{k}={v} out of range [{lo}, {hi}]", {}
        out[k] = v
    try:
        tick = float(body.get("tick_size"))
    except (TypeError, ValueError):
        return False, "tick_size must be a number", {}
    if not math.isfinite(tick) or tick <= 0.0 or tick > _FAST_MARKET_TICK_MAX:
        return False, f"tick_size={body.get('tick_size')!r} must be > 0 and <= {_FAST_MARKET_TICK_MAX:g}", {}
    out["tick_size"] = tick
    en = body.get("enabled")
    if isinstance(en, str):
        en = en.strip().lower() in ("1", "true", "yes", "on")
    out["enabled"] = bool(en)
    return True, "ok", out


def fast_market_config_save(body: dict) -> tuple[bool, str]:
    """Validate then write ONLY the fast_market subkeys via the hardened mutate-write
    (atomic temp+rename, unreadable/unparseable/key-drop guarded). Never a raw file write."""
    ok_v, msg_v, norm = _validate_fast_market_payload(body)
    if not ok_v:
        return False, msg_v

    def _mutate(dc: dict) -> tuple[bool, str]:
        mm = dc.setdefault("market_maker", {})
        if not isinstance(mm, dict):
            return False, "market_maker must be an object"
        fm = mm.setdefault("fast_market", {})
        if not isinstance(fm, dict):
            return False, "market_maker.fast_market must be an object"
        for k, v in norm.items():
            fm[k] = v
        return True, (
            f"{DEFAULT_CONFIG_PATH.name}: fast_market -> symbol={norm['hl_spx_symbol']} "
            f"size={norm['size_of_move_ticks']}t window={norm['time_of_move_sec']}s "
            f"pull={norm['pull_sec']}s tick={norm['tick_size']} enabled={norm['enabled']}"
        )

    return _default_config_mutate_write(_mutate)


def persist_market_maker_current_reload_count_zero(st: DeskSharedState | None) -> tuple[bool, str]:
    """Set ``market_maker.current_reload_count`` to 0 in ``default_config.json`` (manual requote recovery)."""
    if not DEFAULT_CONFIG_PATH.is_file():
        return False, f"missing {DEFAULT_CONFIG_PATH.name}"
    try:
        dc = json.loads(DEFAULT_CONFIG_PATH.read_text(encoding="utf-8"))
        if not isinstance(dc, dict):
            return False, "default_config.json root must be an object"
        mm = dc.setdefault("market_maker", {})
        mm["current_reload_count"] = 0
        _atomic_write_json(DEFAULT_CONFIG_PATH, dc)
        if st is not None:
            # Do not call desk_reload_config_into_state here: that re-reads the entire MM block from
            # disk and can clobber in-memory mm_order_size / other fields unrelated to this write.
            with st.lock:
                st.mm_current_reload_count = 0
        return True, "current_reload_count=0"
    except Exception as e:
        return False, str(e)


def desk_bump_mm_reload_limit_nonce(st: DeskSharedState | None) -> tuple[bool, str, str]:
    """
    Bump ``market_maker.reload_limit_reset_nonce`` in default_config.json so the C++ MM
    clears its session reload counter when it next reads config (same semantics as editing the file by hand).
    """
    if not DEFAULT_CONFIG_PATH.is_file():
        return False, f"missing {DEFAULT_CONFIG_PATH.name}", ""
    try:
        dc = json.loads(DEFAULT_CONFIG_PATH.read_text(encoding="utf-8"))
        if not isinstance(dc, dict):
            return False, "default_config.json root must be an object", ""
        mm = dc.setdefault("market_maker", {})
        new_nonce = f"desk_{int(time.time() * 1000)}"
        mm["reload_limit_reset_nonce"] = new_nonce
        mm["current_reload_count"] = 0
        _atomic_write_json(DEFAULT_CONFIG_PATH, dc)
        msg = f"{DEFAULT_CONFIG_PATH.name}: reload_limit_reset_nonce → {new_nonce}"
        if st is not None:
            desk_reload_config_into_state(st)
            with st.lock:
                st.config_last_write = msg
        return True, msg, new_nonce
    except Exception as e:
        return False, str(e), ""


CONFIG_MERGE_ALLOWED_ROOTS: frozenset[str] = frozenset(
    {
        "api",
        "market_maker",
        "external_feed",
        "risk",
        "trading",
        "startup",
        "performance",
        "websocket",
        "feed",
        "features",
        "logging",
        "hedge",
        "orderbook",
        "mm_desk",
    }
)


def _strip_json_comment_keys(obj: object) -> object:
    """Drop ``_comment*`` keys recursively (JSON files use them as inline documentation)."""
    if isinstance(obj, dict):
        return {
            str(k): _strip_json_comment_keys(v)
            for k, v in obj.items()
            if not (isinstance(k, str) and k.startswith("_comment"))
        }
    if isinstance(obj, list):
        return [_strip_json_comment_keys(x) for x in obj]
    return obj


def load_config_editor_snapshot() -> dict:
    """Full editable snapshot of ``default_config.json`` for the desk overlay (no secrets redaction — local use)."""
    if not DEFAULT_CONFIG_PATH.is_file():
        return {}
    try:
        raw = json.loads(DEFAULT_CONFIG_PATH.read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            return {}
        return _strip_json_comment_keys(raw)  # type: ignore[return-value]
    except (OSError, json.JSONDecodeError, TypeError, ValueError):
        return {}


def _deep_merge_nested(dst: dict, patch: dict, *, depth: int) -> None:
    if depth > 32:
        raise ValueError("config_merge: nesting too deep")
    for k, v in patch.items():
        if isinstance(k, str) and k.startswith("_comment"):
            continue
        if isinstance(v, dict):
            cur = dst.get(k)
            if not isinstance(cur, dict):
                cur = {}
                dst[k] = cur
            _deep_merge_nested(cur, v, depth=depth + 1)
        elif isinstance(v, list):
            dst[k] = list(v)
        else:
            dst[k] = v


def _deep_merge_config_merge_into(dc: dict, patch: dict) -> None:
    """Merge ``patch`` into ``dc`` for whitelisted top-level roots only."""
    if not isinstance(patch, dict):
        return
    for root, sub in patch.items():
        rk = str(root)
        if rk not in CONFIG_MERGE_ALLOWED_ROOTS:
            continue
        if isinstance(sub, dict):
            tgt = dc.setdefault(rk, {})
            if not isinstance(tgt, dict):
                tgt = {}
                dc[rk] = tgt
            _deep_merge_nested(tgt, sub, depth=0)
        elif isinstance(sub, list):
            dc[rk] = list(sub)
        else:
            dc[rk] = sub


def validate_config_merge(patch: object) -> tuple[bool, str]:
    if patch is None:
        return True, ""
    if not isinstance(patch, dict):
        return False, "config_merge must be a JSON object"
    try:
        blob = json.dumps(patch, ensure_ascii=False)
    except (TypeError, ValueError):
        return False, "config_merge is not JSON-serializable"
    if len(blob) > 1_500_000:
        return False, "config_merge is too large"

    def _walk(o: object, depth: int) -> tuple[bool, str]:
        if depth > 40:
            return False, "config_merge: structure too deep"
        if o is None:
            return True, ""
        if isinstance(o, (bool, int, str)):
            return True, ""
        if isinstance(o, float):
            return (True, "") if math.isfinite(o) else (False, "config_merge: non-finite float")
        if isinstance(o, list):
            for i, x in enumerate(o):
                ok_i, err_i = _walk(x, depth + 1)
                if not ok_i:
                    return False, err_i or f"config_merge: bad list item at index {i}"
            return True, ""
        if isinstance(o, dict):
            for k, v in o.items():
                if not isinstance(k, str):
                    return False, "config_merge: object keys must be strings"
                ok_k, err_k = _walk(v, depth + 1)
                if not ok_k:
                    return False, err_k
            return True, ""
        return False, "config_merge: unsupported value type"

    ok, err = _walk(patch, 0)
    if not ok:
        return False, err
    bad_roots = [k for k in patch.keys() if isinstance(k, str) and k not in CONFIG_MERGE_ALLOWED_ROOTS]
    if bad_roots:
        return False, "config_merge: unknown top-level keys: " + ", ".join(bad_roots[:12])
    return True, ""


def persist_desk_config_to_repo(st: DeskSharedState | None, body: dict) -> tuple[bool, str]:
    """
    Merge GUI values into config/credentials.local.json and config/default_config.json
    (same files the C++ stack uses). Only overwrites keys that are present in body.
    """
    ok_v, err_v = validate_mm_strategy_payload(body)
    if not ok_v:
        return False, err_v
    try:
        rest = str(body.get("rest_endpoint", "") or "").strip().rstrip("/")
        key = str(body.get("api_key", "") or "").strip()
        sec = str(body.get("api_secret", "") or "").strip()
        ax = str(body.get("ax_symbol", "") or "").strip()
        bn = str(body.get("ref_symbol") or body.get("bn_symbol", "") or "").strip()

        cred_api: dict = {}
        if CREDENTIALS_PATH.is_file():
            try:
                raw = json.loads(CREDENTIALS_PATH.read_text(encoding="utf-8"))
                if isinstance(raw, dict):
                    if isinstance(raw.get("api"), dict):
                        cred_api.update(raw["api"])
                    if raw.get("rest_endpoint"):
                        cred_api.setdefault("rest_endpoint", str(raw["rest_endpoint"]))
                    if raw.get("api_key"):
                        cred_api.setdefault("api_key", str(raw["api_key"]))
                    if raw.get("api_secret"):
                        cred_api.setdefault("api_secret", str(raw["api_secret"]))
            except (OSError, json.JSONDecodeError):
                cred_api = {}
        if rest:
            cred_api["rest_endpoint"] = rest
            cred_api["ws_endpoint"] = _ws_endpoint_from_rest(rest)
        if key:
            cred_api["api_key"] = key
        if sec:
            cred_api["api_secret"] = sec
        cred_out = {"api": cred_api}
        if CREDENTIALS_PATH.is_file():
            try:
                prev = json.loads(CREDENTIALS_PATH.read_text(encoding="utf-8"))
                if isinstance(prev, dict) and isinstance(prev.get("_comment"), str):
                    cred_out["_comment"] = prev["_comment"]
            except (OSError, json.JSONDecodeError):
                pass
        _atomic_write_json(CREDENTIALS_PATH, cred_out)

        if not DEFAULT_CONFIG_PATH.is_file():
            return False, f"missing {DEFAULT_CONFIG_PATH.name}"
        dc = json.loads(DEFAULT_CONFIG_PATH.read_text(encoding="utf-8"))
        if not isinstance(dc, dict):
            return False, "default_config.json root must be an object"

        cm = body.get("config_merge")
        if cm is not None:
            if not isinstance(cm, dict):
                return False, "config_merge must be an object"
            ok_cm, err_cm = validate_config_merge(cm)
            if not ok_cm:
                return False, err_cm
            try:
                cm_copy = json.loads(json.dumps(cm))
            except (TypeError, ValueError):
                return False, "config_merge could not be copied"
            if isinstance(cm_copy, dict):
                api_cm = cm_copy.get("api")
                if isinstance(api_cm, dict):
                    for _zk in ("api_key", "api_secret"):
                        if api_cm.get(_zk) == "":
                            api_cm.pop(_zk, None)
                _deep_merge_config_merge_into(dc, cm_copy)
            else:
                return False, "config_merge must be an object"

        api = dc.setdefault("api", {})
        if rest:
            api["rest_endpoint"] = rest
            api["ws_endpoint"] = _ws_endpoint_from_rest(rest)
        if key:
            api["api_key"] = key
        if sec:
            api["api_secret"] = sec

        mm = dc.setdefault("market_maker", {})
        mm_instruments = mm.get("instruments")
        multi_instr = False
        if isinstance(mm_instruments, list):
            n_syms = 0
            for _row in mm_instruments:
                if not isinstance(_row, dict):
                    continue
                _sx = str(_row.get("symbol") or _row.get("ax_symbol") or "").strip()
                if _sx:
                    n_syms += 1
            multi_instr = n_syms > 1
        force_primary = bool(body.get("force_primary_symbol_write"))
        if ax and (force_primary or not multi_instr):
            mm["symbol"] = ax
            mm["order_symbol"] = ax
        if bn and (force_primary or not multi_instr):
            dc.setdefault("external_feed", {})["symbol"] = bn
        if body.get("width_bps") is not None:
            iw = int(body["width_bps"])
            mm["width_bps"] = iw
            mm["bid_width_bps"] = iw
            mm["ask_width_bps"] = iw
        if body.get("order_size") is not None:
            iq = int(body["order_size"])
            mm["order_size"] = iq
            mm["quantity"] = iq
        if body.get("max_position") is not None:
            mm["max_position"] = int(body["max_position"])
        if body.get("adjust_position") is not None:
            mm["adjust_position"] = int(body["adjust_position"])
        if body.get("adjust_ticks") is not None:
            mm["adjust_ticks"] = int(body["adjust_ticks"])
        if body.get("max_reload_cycles") is not None:
            mm["max_reload_cycles"] = int(body["max_reload_cycles"])
        if body.get("current_reload_count") is not None:
            mm["current_reload_count"] = int(body["current_reload_count"])
        if body.get("reload_cycles_count_fill_only") is not None:
            mm["reload_cycles_count_fill_only"] = _coerce_reload_cycles_count_fill_only(
                body["reload_cycles_count_fill_only"]
            )
        if "reload_limit_reset_nonce" in body:
            mm["reload_limit_reset_nonce"] = str(body.get("reload_limit_reset_nonce") or "")
        if body.get("requote_on_theo_move") is not None:
            mm["requote_on_theo_move"] = bool(body["requote_on_theo_move"])
        if body.get("mm_orders_enabled") is not None:
            mm["mm_orders_enabled"] = bool(body["mm_orders_enabled"])
        if body.get("min_theo_move_ticks_to_requote") is not None:
            mm["min_theo_move_ticks_to_requote"] = _clamp_min_theo_move_ticks_to_requote(
                int(body["min_theo_move_ticks_to_requote"])
            )
        if body.get("price_tick") is not None:
            mm["price_tick"] = float(body["price_tick"])
        if body.get("pricing_tick") is not None:
            mm["pricing_tick"] = float(body["pricing_tick"])
        _atomic_write_json(DEFAULT_CONFIG_PATH, dc)
        msg = f"{CREDENTIALS_PATH.name} + {DEFAULT_CONFIG_PATH.name} updated"
        if st is not None:
            with st.lock:
                st.config_last_write = msg
        return True, msg
    except Exception as e:
        return False, str(e)


def persist_desk_instruments_to_repo(
    st: DeskSharedState | None,
    *,
    ax_symbol: str,
    bn_symbol: str,
    ax_orderbook_extras: list[str] | None,
) -> tuple[bool, str]:
    """Write trading symbols + extra AX orderbook columns to default_config.json (credentials unchanged)."""
    try:
        if not DEFAULT_CONFIG_PATH.is_file():
            return False, f"missing {DEFAULT_CONFIG_PATH.name}"
        dc = json.loads(DEFAULT_CONFIG_PATH.read_text(encoding="utf-8"))
        if not isinstance(dc, dict):
            return False, "default_config.json root must be an object"
        ax = (ax_symbol or "").strip()
        bn = (bn_symbol or "").strip()
        extras_in = ax_orderbook_extras if ax_orderbook_extras is not None else []
        seen: set[str] = set()
        extras: list[str] = []
        for x in extras_in:
            t = str(x).strip()
            if not t or t in seen:
                continue
            seen.add(t)
            extras.append(t)
        if ax:
            mm = dc.setdefault("market_maker", {})
            mm["symbol"] = ax
            mm["order_symbol"] = ax
        if bn:
            dc.setdefault("external_feed", {})["symbol"] = bn
        desk = dc.setdefault("mm_desk", {})
        desk["ax_orderbook_symbols"] = extras
        desk["ax_architect_depth_multiselect_explicit"] = True
        _atomic_write_json(DEFAULT_CONFIG_PATH, dc)
        msg = f"{DEFAULT_CONFIG_PATH.name} instruments updated"
        if st is not None:
            with st.lock:
                st.config_last_write = msg
        return True, msg
    except Exception as e:
        return False, str(e)


def _mm_axes_from_config_mm(dc: dict) -> list[str]:
    mm = dc.get("market_maker") if isinstance(dc.get("market_maker"), dict) else {}
    inst = mm.get("instruments")
    out: list[str] = []
    if isinstance(inst, list) and inst:
        for row in inst:
            if isinstance(row, dict):
                ax = str(row.get("symbol") or row.get("ax_symbol") or "").strip()
                if ax:
                    out.append(ax)
        if out:
            return out
    ax = str(mm.get("symbol") or mm.get("order_symbol") or "").strip()
    return [ax] if ax else []


def _merge_fix_md_symbols_for_refs(dc: dict, reference_symbols: list[str]) -> None:
    ext = dc.setdefault("external_feed", {})
    fix = ext.setdefault("fix", {})
    cur = fix.get("md_symbols")
    out: list[str] = []
    seen: set[str] = set()

    def canon(s: str) -> str:
        return "".join(c for c in s.upper() if c.isalnum())

    def add(s: str) -> None:
        t = (s or "").strip()
        if not t:
            return
        k = canon(t)
        if k in seen:
            return
        seen.add(k)
        out.append(t)

    if isinstance(cur, list):
        for x in cur:
            add(str(x))
    for s in reference_symbols:
        add(str(s))
    primary = str(ext.get("symbol") or "").strip()
    add(primary)
    fix["md_symbols"] = out


def _mm_book_preview_text(st: DeskSharedState, rows: list[dict]) -> str:
    lines: list[str] = []
    with st.lock:
        for r in rows:
            ax = str(r.get("symbol") or "").strip()
            ref = str(r.get("reference_fix_symbol") or "").strip()
            if not ax:
                continue
            adb = st.ax_all_books.get(ax) or {}

            def top(book: dict, side: str) -> str:
                lv = book.get(side) or []
                if not lv:
                    return "—"
                row0 = lv[0]
                if isinstance(row0, (list, tuple)) and len(row0) >= 2:
                    return f"{row0[0]} @ {row0[1]}"
                return "—"

            ax_line = f"AX {ax}: bid {top(adb, 'bids')} ask {top(adb, 'asks')}"
            ref_line = ""
            if ref:
                rdb = (
                    st.ref_all_books.get(ref)
                    or st.ref_all_books.get(ref.upper())
                    or {}
                )
                ref_line = f" | Ref {ref}: bid {top(rdb, 'bids')} ask {top(rdb, 'asks')}"
            lines.append(ax_line + ref_line)
    return "\n".join(lines) if lines else "(no book snapshot yet — reload to refresh)"


def persist_mm_instruments_multi(
    st: DeskSharedState | None,
    instruments_in: list[object],
) -> tuple[bool, str, str]:
    """Write ``market_maker.instruments``, merge FIX md_symbols, cancel AX for removed legs."""
    try:
        if not DEFAULT_CONFIG_PATH.is_file():
            return False, f"missing {DEFAULT_CONFIG_PATH.name}", ""
        dc = json.loads(DEFAULT_CONFIG_PATH.read_text(encoding="utf-8"))
        if not isinstance(dc, dict):
            return False, "default_config.json root must be an object", ""
        old_axes = set(_mm_axes_from_config_mm(dc))
        new_rows: list[dict] = []
        if isinstance(instruments_in, list):
            for el in instruments_in:
                if isinstance(el, dict):
                    sym = str(el.get("symbol") or el.get("ax_symbol") or "").strip()
                    ref = str(el.get("reference_fix_symbol") or el.get("theo_symbol") or "").strip()
                    osym = str(el.get("order_symbol") or "").strip()
                    if sym:
                        new_rows.append(
                            {"symbol": sym, "reference_fix_symbol": ref, "order_symbol": osym}
                        )
        if not new_rows:
            return False, "need at least one instrument with symbol", ""
        new_axes = {r["symbol"] for r in new_rows}
        if st is not None:
            for ax in sorted(old_axes - new_axes):
                desk_cancel_all_orders(st, symbol=ax)
        mm = dc.setdefault("market_maker", {})
        mm["instruments"] = new_rows
        mm["symbol"] = new_rows[0]["symbol"]
        mm["order_symbol"] = new_rows[0].get("order_symbol") or new_rows[0]["symbol"]
        ref0 = str(new_rows[0].get("reference_fix_symbol") or "").strip()
        if ref0:
            dc.setdefault("external_feed", {})["symbol"] = ref0
        refs = [str(r.get("reference_fix_symbol") or "").strip() for r in new_rows]
        _merge_fix_md_symbols_for_refs(dc, refs)
        desk = dc.setdefault("mm_desk", {})
        extras = [r["symbol"] for r in new_rows[1:]]
        desk["ax_orderbook_symbols"] = extras
        desk["ax_architect_depth_multiselect_explicit"] = False
        _atomic_write_json(DEFAULT_CONFIG_PATH, dc)
        preview = _mm_book_preview_text(st, new_rows) if st is not None else ""
        msg = f"{DEFAULT_CONFIG_PATH.name} market_maker.instruments ({len(new_rows)} legs)"
        if st is not None:
            with st.lock:
                st.config_last_write = msg
        return True, msg, preview
    except Exception as e:
        return False, str(e), ""


def _normalize_ax_key(sym: str) -> str:
    return (sym or "").strip().upper()


def _find_mm_instrument_row(
    inst: list[object], ax: str
) -> tuple[int | None, dict | None]:
    want = _normalize_ax_key(ax)
    if not inst or not want:
        return None, None
    for i, row in enumerate(inst):
        if not isinstance(row, dict):
            continue
        s0 = str(row.get("symbol") or row.get("ax_symbol") or "").strip()
        if _normalize_ax_key(s0) == want:
            return i, row
    return None, None


def _theo_leg_choices_from_ax_master(
    ax_book_symbols: list[str],
    ax_grid_symbols: list[str],
    mm_instruments: list[dict],
    ref_symbol: str,
) -> list[dict]:
    """
    Single product ordering for MM leg pickers / modals: **Architect AX depth list** (persisted
    ``mm_desk.ax_orderbook_symbols`` → ``ax_book_symbols``), then ``ax_grid_symbols``, else every
    ``market_maker.instruments`` row. Per-symbol ref/theo_source come from the matching instrument row
    when present.
    """
    master = [str(x).strip() for x in (ax_book_symbols or []) if str(x).strip()]
    if not master:
        master = [str(x).strip() for x in (ax_grid_symbols or []) if str(x).strip()]
    ref0 = (ref_symbol or "").strip()
    mm_rows = [r for r in (mm_instruments or []) if isinstance(r, dict)]
    if not master:
        return [
            {
                "ax": str(r.get("symbol") or r.get("ax_symbol") or ""),
                "ref": str(r.get("reference_fix_symbol") or r.get("theo_symbol") or ""),
                "theo_source": str(r.get("theo_source") or ""),
            }
            for r in mm_rows
            if str(r.get("symbol") or r.get("ax_symbol") or "").strip()
        ]
    out: list[dict] = []
    seen: set[str] = set()
    for ax in master:
        nk = _normalize_ax_key(ax)
        if not nk or nk in seen:
            continue
        seen.add(nk)
        _, row = _find_mm_instrument_row(mm_rows, ax)
        if row:
            out.append(
                {
                    "ax": str(row.get("symbol") or row.get("ax_symbol") or ax),
                    "ref": str(row.get("reference_fix_symbol") or row.get("theo_symbol") or ""),
                    "theo_source": str(row.get("theo_source") or ""),
                }
            )
        else:
            out.append({"ax": ax, "ref": ref0, "theo_source": ""})
    return out


def desk_append_manual_stack(
    st: DeskSharedState | None,
    *,
    symbol: str,
    stack: dict,
) -> tuple[bool, str, dict | None]:
    """
    Append one entry to ``market_maker.instruments[leg].manual_stacks`` (C++ registers one MakeMarketStrategy per
    non-empty *manual_stacks* list after **restart**).
    """
    if not DEFAULT_CONFIG_PATH.is_file():
        return False, f"missing {DEFAULT_CONFIG_PATH.name}", None
    try:
        dc = json.loads(DEFAULT_CONFIG_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as e:
        return False, str(e), None
    if not isinstance(dc, dict):
        return False, "default_config root must be object", None
    mm = dc.get("market_maker")
    if not isinstance(mm, dict):
        return False, "no market_maker in config", None
    inst = mm.get("instruments")
    if not isinstance(inst, list) or not inst:
        return False, "market_maker.instruments must be a non-empty array", None
    idx, row = _find_mm_instrument_row(inst, symbol)
    if row is None:
        return False, f"symbol {symbol!r} not in market_maker.instruments (edit default_config.json)", None
    st_row = row
    mst = st_row.setdefault("manual_stacks", [])
    if not isinstance(mst, list):
        mst = []
        st_row["manual_stacks"] = mst
    if not str(stack.get("id") or "").strip():
        stack = {**stack, "id": __import__("uuid").uuid4().hex[:16]}
    w_bps_dc = 0
    for _wk in ("width_bps", "width_ticks", "width"):
        _wv = stack.get(_wk)
        if _wv is None or isinstance(_wv, bool):
            continue
        try:
            w_bps_dc = int(float(_wv))
        except (TypeError, ValueError):
            continue
        if w_bps_dc != 0:
            break
    mst.append(
        {
            "id": str(stack.get("id", "")).strip(),
            "width_bps": w_bps_dc,
            "order_size": int(stack.get("order_size", 0) or 0),
            # Persist per-stack order_size_step alongside order_size. Without this the
            # C++ engine falls back to the legacy global `market_maker.order_size_step`
            # (the EURUSD default of 100), which silently rounds smaller per-product
            # order_size values down to 0 and breaks every reload cycle. Default to 1
            # so any caller that omits the field is rounded to itself (i.e. no-op).
            "order_size_step": max(1, int(stack.get("order_size_step", 1) or 1)),
            "max_position": int(stack.get("max_position", 0) or 0),
            "adjust_position": int(stack.get("adjust_position", 0) or 0),
            "adjust_ticks": int(stack.get("adjust_ticks", 0) or 0),
            "min_theo_move_ticks_to_requote": int(
                stack.get("min_theo_move_ticks_to_requote")
                or stack.get("min_drift_ticks", 0)
                or 0
            ),
            "max_reload_cycles": int(stack.get("max_reload_cycles", 0) or 0),
        }
    )
    inst[idx] = st_row
    mm["instruments"] = inst
    _atomic_write_json(DEFAULT_CONFIG_PATH, dc)
    msg = f"{DEFAULT_CONFIG_PATH.name}: appended manual_stacks[…] for {symbol}"
    if st is not None:
        with st.lock:
            st.config_last_write = msg
    return True, msg, mst[-1]


def desk_remove_manual_stack(
    st: DeskSharedState | None,
    *,
    symbol: str,
    stack_id: str,
) -> tuple[bool, str]:
    if not DEFAULT_CONFIG_PATH.is_file():
        return False, f"missing {DEFAULT_CONFIG_PATH.name}"
    try:
        dc = json.loads(DEFAULT_CONFIG_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as e:
        return False, str(e)
    mm = dc.get("market_maker")
    if not isinstance(mm, dict):
        return False, "no market_maker"
    inst = mm.get("instruments")
    if not isinstance(inst, list):
        return False, "no instruments"
    idx, row = _find_mm_instrument_row(inst, symbol)
    if row is None:
        return False, "symbol not in instruments"
    mst = row.get("manual_stacks")
    if not isinstance(mst, list):
        return False, "no manual_stacks on leg"
    want = str(stack_id or "").strip()
    newm = [x for x in mst if not (isinstance(x, dict) and str(x.get("id", "")).strip() == want)]
    if len(newm) == len(mst):
        return False, "stack_id not found"
    row["manual_stacks"] = newm
    inst[idx] = row
    mm["instruments"] = inst
    _atomic_write_json(DEFAULT_CONFIG_PATH, dc)
    msg = f"Removed manual_stacks id={want!r} for {symbol}"
    if st is not None:
        with st.lock:
            st.config_last_write = msg
    return True, msg


def _desk_stack_id_matches_row(row: object, want: str) -> bool:
    w = str(want or "").strip()
    if not w or not isinstance(row, dict):
        return False
    for k in ("id", "stack_id", "request_id"):
        v = row.get(k)
        if v is not None and str(v).strip() == w:
            return True
    return False


def _orders_json_resolve_product_key(products: dict, ax: str) -> str | None:
    """Return ``products`` dict key for AX symbol (case-insensitive match)."""
    if not isinstance(products, dict) or not str(ax or "").strip():
        return None
    a = str(ax).strip()
    if a in products:
        return a
    au = a.upper()
    for k in products:
        if str(k).strip().upper() == au:
            return str(k)
    return None


def _instrument_max_position_floor_from_orders_doc(
    doc: dict,
    ax: str,
    *,
    exclude_stack_id: str = "",
) -> int:
    """
    Highest ``max_position`` already configured for *ax* across ``orders.json``.

    Product-level cap is the max of:
      * ``products[ax].max_position``
      * every ``stacks[]`` / ``products[ax].stacks[]`` row on that AX

    New desk stacks must use a value >= this floor (monotonic instrument cap).
    """
    axu = str(ax or "").strip().upper()
    if not axu or not isinstance(doc, dict):
        return 0
    exclude = str(exclude_stack_id or "").strip()
    floor = 0

    def _bump(mp_raw: object) -> None:
        nonlocal floor
        try:
            mp = int(mp_raw or 0)
        except (TypeError, ValueError):
            return
        if mp > floor:
            floor = mp

    products = doc.get("products")
    if isinstance(products, dict):
        prod = products.get(ax) or products.get(axu)
        if isinstance(prod, dict):
            _bump(prod.get("max_position"))
            prod_stacks = prod.get("stacks")
            if isinstance(prod_stacks, list):
                for row in prod_stacks:
                    if not isinstance(row, dict):
                        continue
                    sid = str(row.get("stack_id") or row.get("id") or "").strip()
                    if exclude and sid == exclude:
                        continue
                    if str(row.get("ax_symbol") or "").strip().upper() != axu:
                        continue
                    _bump(row.get("max_position"))

    root = doc.get("stacks")
    if isinstance(root, list):
        for row in root:
            if not isinstance(row, dict):
                continue
            sid = str(row.get("stack_id") or row.get("id") or "").strip()
            if exclude and sid == exclude:
                continue
            if str(row.get("ax_symbol") or "").strip().upper() != axu:
                continue
            _bump(row.get("max_position"))
    return floor


def _validate_instrument_max_position_monotonic(
    merged_cfg: dict,
    ax: str,
    requested_max_position: int,
    *,
    exclude_stack_id: str = "",
) -> tuple[bool, str, int]:
    """
    Reject lowering the instrument cap when adding another stack on the same AX.

    Returns (ok, error_message, floor). ``requested_max_position`` must be >= floor when
    floor > 0.
    """
    try:
        req = int(requested_max_position)
    except (TypeError, ValueError):
        return False, "max_position must be an integer", 0
    with _MM_ORDERS_CONFIG_LOCK:
        doc = load_mm_orders_config(merged_cfg)
        floor = _instrument_max_position_floor_from_orders_doc(
            doc, ax, exclude_stack_id=exclude_stack_id
        )
    if floor > 0 and req < floor:
        return (
            False,
            (
                f"Instrument {ax}: max_position is already {floor} across existing order "
                f"pairs. You entered {req}, which is lower. Use {floor} or higher."
            ),
            floor,
        )
    return True, "", floor


def _instrument_max_position_and_reload_from_merged(merged_cfg: dict, ax: str) -> tuple[int, int]:
    """Latest instrument-level ``max_position`` and ``max_reload_cycles`` from merged default_config."""
    axn = str(ax or "").strip()
    mm = merged_cfg.get("market_maker") if isinstance(merged_cfg.get("market_maker"), dict) else {}
    glob_po = int(mm.get("max_position") or 0)
    glob_rl = int(mm.get("max_reload_cycles") or 0)
    inst_list = mm.get("instruments") if isinstance(mm.get("instruments"), list) else []
    _, row = _find_mm_instrument_row(inst_list, axn)
    if not isinstance(row, dict):
        eff_po = glob_po if glob_po > 0 else 1
        eff_rl = max(0, glob_rl)
        return eff_po, eff_rl
    try:
        leg_po = int(row.get("max_position") or 0)
    except (TypeError, ValueError):
        leg_po = 0
    try:
        leg_rl = int(row.get("max_reload_cycles") or 0)
    except (TypeError, ValueError):
        leg_rl = 0
    eff_po = leg_po if leg_po > 0 else (glob_po if glob_po > 0 else 1)
    eff_rl = leg_rl if leg_rl > 0 else max(0, glob_rl)
    return eff_po, eff_rl


def _validate_mm_desk_stack_row_for_cpp(row: object) -> tuple[bool, str]:
    """
    Fields C++ needs for a desk-seeded stack must be present and consistent.

    **Pair mode:** both bid and ask active (qty>=1, positive price, gateway oid).

    **Reduce-only / max_position mode:** one leg may be inactive — that side uses
    ``*_qty==0``, empty ``*_exchange_oid``, and ``*_price`` 0 or omitted (matches
    ``MakeMarketStrategy::applyMmDeskSeededJsonLocked``, which adopts only non-empty oids).
    """
    if not isinstance(row, dict):
        return False, "stack row must be an object"
    sid = str(row.get("id") or "").strip()
    if not sid:
        return False, "stack.id is required"
    req_int = (
        ("width_bps", "width_bps"),
        ("order_size", "order_size"),
        ("adjust_position", "adjust_position"),
        ("adjust_ticks", "adjust_ticks"),
        ("min_theo_move_ticks_to_requote", "min_theo_move_ticks_to_requote"),
    )
    for json_key, label in req_int:
        if row.get(json_key) is None:
            return False, f"missing integer {label}"
        try:
            v = int(row[json_key])
        except (TypeError, ValueError):
            return False, f"invalid integer {label}"
        if v < 1:
            return False, f"{label} must be >= 1"
    if row.get("max_reload_cycles") is None:
        return False, "missing max_reload_cycles"
    try:
        mrc = int(row["max_reload_cycles"])
    except (TypeError, ValueError):
        return False, "invalid max_reload_cycles"
    if mrc < 0:
        return False, "max_reload_cycles must be >= 0"
    if not isinstance(row.get("desk_seeded"), bool):
        return False, "desk_seeded must be a boolean"
    if not row["desk_seeded"]:
        return False, "desk_seeded must be true for gateway-seeded stacks"
    if not isinstance(row.get("mm_move_enabled"), bool):
        return False, "mm_move_enabled must be a boolean"

    active_legs = 0
    for px_key, qty_key, oid_key, label in (
        ("bid_price", "bid_qty", "bid_exchange_oid", "bid"),
        ("ask_price", "ask_qty", "ask_exchange_oid", "ask"),
    ):
        if row.get(qty_key) is None:
            return False, f"missing {label}_qty"
        try:
            q = int(row[qty_key])
        except (TypeError, ValueError):
            return False, f"invalid {label}_qty"
        oid = str(row.get(oid_key) or "").strip()

        if q == 0:
            if oid:
                return False, f"{label}_exchange_oid must be empty when {label}_qty is 0"
            if row.get(px_key) is not None:
                try:
                    px0 = float(row[px_key])
                except (TypeError, ValueError):
                    return False, f"invalid {label}_price"
                if math.isfinite(px0) and px0 > 0.0:
                    return False, f"{label}_price must be 0 (or omitted) when {label}_qty is 0"
            continue

        if q < 1:
            return False, f"{label}_qty must be >= 1 when quoting {label}"
        active_legs += 1
        if row.get(px_key) is None:
            return False, f"missing {label}_price"
        try:
            px = float(row[px_key])
        except (TypeError, ValueError):
            return False, f"invalid {label}_price"
        if not math.isfinite(px) or px <= 0.0:
            return False, f"{label}_price must be a positive finite number"
        if not oid:
            return False, f"missing non-empty {label}_exchange_oid (gateway ack required before persist)"

    if active_legs < 1:
        return False, "at least one of bid_qty or ask_qty must be >= 1 (reduce-only single-leg stack)"
    return True, ""


def _cancel_stack_gateway_legs_best_effort(st: DeskSharedState | None, stack: dict) -> None:
    """Cancel the stack's known seed OIDs at the gateway (best-effort, idempotent).

    Multi-stack-safe by design: this function cancels ONLY the specific
    bid/ask exchange OIDs recorded on this stack row — never the whole symbol.
    Issuing a per-symbol ``/cancel-all-orders`` here would cancel sibling
    stacks on the same AX product as collateral; while pair-enforce would
    eventually restore them, the brief downtime + re-quote at moved theo is
    unnecessary risk when the targeted stack can be torn down precisely.

    Live-OID coverage after the seed:
      * For place_order rollback callers (line ~9597/9617/9669), the seed
        OIDs ARE the live OIDs — no theo_move has happened yet, so this
        function is sufficient.
      * For the per-stack X cancel (line ~9762), the seed OIDs may be stale
        after C++ moves/reloads. The caller removes the stack row from
        orders.json after this call; the C++ reconcile (~1s) then calls
        ``MakeMarketStrategy::mmDeskShutdownLogAndCancelTrackedLegs`` which
        reads the LIVE OIDs from ``tracked_bids_`` / ``tracked_asks_`` and
        cancels them by exact OID via the gateway. Multi-stack isolation is
        preserved end-to-end because the C++ teardown is per-strategy.

    The desk-wide "Cancel All" button uses ``desk_clear_orders_json_cancel_gateway``
    which DOES issue per-symbol + unscoped ``/cancel-all-orders`` sweeps —
    that's correct there because the user intent is "wipe everything", not
    "remove one stack".
    """
    if st is None or not isinstance(stack, dict):
        return
    for k in ("bid_exchange_oid", "ask_exchange_oid"):
        oid = str(stack.get(k) or "").strip()
        if oid:
            try:
                desk_cancel_one_order(st, oid)
            except Exception:
                pass


def _cancel_all_mm_stack_gateway_legs_for_ax_symbol(
    st: DeskSharedState | None, merged_cfg: dict, ax_symbol: str
) -> int:
    """Cancel every stack leg for ``ax_symbol`` listed in ``orders.json`` (best-effort).

    ``api_place_order`` seeds the gateway directly; if C++ still tracks *different* OIDs in
    OrderManager, a theo-driven cancel-replace cancels only those and leaves this venue pair
    orphaned — producing two live bid/ask pairs on the same product.
    """
    if st is None or not str(ax_symbol or "").strip():
        return 0
    doc = load_mm_orders_config(merged_cfg)
    stale_oids: list[str] = []
    root = doc.get("stacks")
    if isinstance(root, list):
        for s in root:
            if not isinstance(s, dict):
                continue
            axr = str(s.get("ax_symbol") or "").strip()
            if _norm_orders_ax(axr) != _norm_orders_ax(str(ax_symbol or "")):
                continue
            for k in ("bid_exchange_oid", "ask_exchange_oid"):
                oid = str(s.get(k) or "").strip()
                if oid:
                    stale_oids.append(oid)
    products = doc.get("products") if isinstance(doc.get("products"), dict) else {}
    pk = _orders_json_resolve_product_key(products, ax_symbol)
    if not pk:
        pk = str(ax_symbol or "").strip()
    prod = products.get(pk) if pk else None
    if isinstance(prod, dict):
        stacks = prod.get("stacks")
        if isinstance(stacks, list):
            for s in stacks:
                if not isinstance(s, dict):
                    continue
                for k in ("bid_exchange_oid", "ask_exchange_oid"):
                    oid = str(s.get(k) or "").strip()
                    if oid:
                        stale_oids.append(oid)
    if not stale_oids:
        return 0

    # Guardrail: skip oldest rows when orders.json accumulates many stale legs.
    if len(stale_oids) > 30:
        stale_oids = stale_oids[-30:]

    seen: set[str] = set()
    uniq_oids: list[str] = []
    for oid in stale_oids:
        if oid in seen:
            continue
        seen.add(oid)
        uniq_oids.append(oid)

    def _cancel_single_order(oid: str) -> None:
        try:
            desk_cancel_one_order(st, oid)
        except Exception:
            # Best-effort cancel: individual failures must not block place.
            pass

    with ThreadPoolExecutor(max_workers=20) as ex:
        futs = [ex.submit(_cancel_single_order, oid) for oid in uniq_oids]
        for fut in as_completed(futs):
            try:
                fut.result()
            except Exception:
                pass
    return len(uniq_oids)


def _find_stack_row_in_orders_doc(
    doc: dict, stack_id: str, ax_hint: str
) -> tuple[str | None, dict | None]:
    """Return (product_key, stack_row) for *stack_id*."""
    want = str(stack_id or "").strip()
    if not want:
        return None, None
    root = doc.get("stacks")
    if isinstance(root, list):
        for s in root:
            if isinstance(s, dict) and _desk_stack_id_matches_row(s, want):
                return None, s
    products = doc.get("products") if isinstance(doc.get("products"), dict) else {}
    axes: list[str] = []
    if str(ax_hint or "").strip():
        pk = _orders_json_resolve_product_key(products, ax_hint)
        if pk:
            axes.append(pk)
    if not axes:
        axes = list(products.keys())
    for ax in axes:
        prod = products.get(ax)
        if not isinstance(prod, dict):
            continue
        stacks = prod.get("stacks")
        if not isinstance(stacks, list):
            continue
        for s in stacks:
            if isinstance(s, dict) and _desk_stack_id_matches_row(s, want):
                return str(ax), s
    for ax, prod in products.items():
        if not isinstance(prod, dict):
            continue
        stacks = prod.get("stacks")
        if not isinstance(stacks, list):
            continue
        for s in stacks:
            if isinstance(s, dict) and _desk_stack_id_matches_row(s, want):
                return str(ax), s
    return None, None


def desk_clear_orders_json_cancel_gateway(
    st: DeskSharedState | None, merged_cfg: dict
) -> tuple[bool, str, int]:
    """
    Cancel every live AX order tied to the desk and replace orders.json with an
    empty document. Returns ``(ok, message, stacks_cleared_count)``.

    Sweep strategy (belt-and-suspenders) — see ``_cancel_stack_gateway_legs_best_effort``
    for the underlying bug. Past versions of this function only cancelled the
    seed OIDs recorded in orders.json; once C++ moved or reloaded a stack the
    live OIDs no longer matched and "Cancel All" silently left orders on the
    book. This version:

      1) Cancels the known seed OIDs (fast path for unmoved stacks; cheap).
      2) Issues a per-symbol ``/cancel-all-orders`` for every unique
         ``ax_symbol`` referenced in orders.json. This catches every live OID
         on those symbols, including the ones C++ created via theo_move /
         fill-reload that Python never saw.
      3) Issues an unscoped ``/cancel-all-orders`` (no ``symbol`` filter) so
         orders the desk doesn't track (base ``make_market_*`` strategies,
         hand-placed orders, orphans from prior crashes) also get cancelled.
      4) Truncates orders.json so the C++ engine stops re-placing desk stacks
         on its next reconcile.
      5) Issues one final unscoped ``/cancel-all-orders`` after the truncate
         to clean up anything any background C++ thread placed in the small
         window between (3) and (4).
    """
    seed_oid_cancels = 0
    ax_symbols: set[str] = set()

    with _MM_ORDERS_CONFIG_LOCK:
        doc = load_mm_orders_config(merged_cfg)
        n = 0
        root0 = doc.get("stacks")
        if isinstance(root0, list):
            for s in root0:
                if isinstance(s, dict):
                    for k in ("bid_exchange_oid", "ask_exchange_oid"):
                        oid = str(s.get(k) or "").strip()
                        if oid:
                            try:
                                desk_cancel_one_order(st, oid)
                                seed_oid_cancels += 1
                            except Exception:
                                pass
                    ax_raw = str(s.get("ax_symbol") or "").strip()
                    if ax_raw:
                        ax_symbols.add(ax_raw)
                    n += 1
        products = doc.get("products") if isinstance(doc.get("products"), dict) else {}
        for _pk, prod in list(products.items()):
            if not isinstance(prod, dict):
                continue
            ax_raw = str(prod.get("ax_symbol") or _pk or "").strip()
            if ax_raw:
                ax_symbols.add(ax_raw)
            stacks = prod.get("stacks")
            if not isinstance(stacks, list):
                continue
            for s in stacks:
                if isinstance(s, dict):
                    for k in ("bid_exchange_oid", "ask_exchange_oid"):
                        oid = str(s.get(k) or "").strip()
                        if oid:
                            try:
                                desk_cancel_one_order(st, oid)
                                seed_oid_cancels += 1
                            except Exception:
                                pass
                    ax_inner = str(s.get("ax_symbol") or "").strip()
                    if ax_inner:
                        ax_symbols.add(ax_inner)
                    n += 1

        # (2) per-symbol venue cancel-all — catches OIDs C++ replaced.
        per_sym_ok = 0
        per_sym_fail = 0
        for ax in sorted(ax_symbols):
            try:
                r = desk_cancel_all_orders(st, symbol=ax)
                if r.get("ok"):
                    per_sym_ok += 1
                else:
                    per_sym_fail += 1
            except Exception:
                per_sym_fail += 1

        # (3) unscoped venue cancel-all — catches everything else (base MM,
        # manual orders, orphans).
        unscoped1_ok = False
        try:
            r = desk_cancel_all_orders(st, symbol=None)
            unscoped1_ok = bool(r.get("ok"))
        except Exception:
            unscoped1_ok = False

        # (4) Truncate orders.json so the C++ reconcile stops re-placing.
        # Intentional clear-all → authorise the shrink past the mass-shrink guard, and
        # stamp intent=remove_all so the C++ mass-teardown guardrail tears down
        # immediately instead of deferring for a confirming read (emergency pull-all is
        # never delayed). The venue orders were already cancelled directly in steps 1-3.
        empty = _empty_mm_orders_config()
        empty["intent"] = "remove_all"
        ok_w, err_w = save_mm_orders_config(merged_cfg, empty, allow_shrink=True)

    if not ok_w:
        return False, err_w, n

    # (5) Second unscoped sweep AFTER the truncate, outside the lock, to mop
    # up anything any C++ thread placed in the gap between (3) and (4).
    unscoped2_ok = False
    try:
        r = desk_cancel_all_orders(st, symbol=None)
        unscoped2_ok = bool(r.get("ok"))
    except Exception:
        unscoped2_ok = False

    msg = (
        f"orders.json cleared — {n} stack row(s) removed; "
        f"seed_oid_cancels={seed_oid_cancels} "
        f"per_symbol_cancel_all ok={per_sym_ok}/{len(ax_symbols)} fail={per_sym_fail} "
        f"unscoped_cancel_all_pre={int(unscoped1_ok)} unscoped_cancel_all_post={int(unscoped2_ok)}"
    )
    if st is not None:
        try:
            _set_desk_action(st, "Cancel All sweep: " + msg)
        except Exception:
            pass
    return True, msg, n


def desk_cancel_one_order(st: DeskSharedState, oid: str) -> dict:
    """
    Order gateway: POST /cancel_order with body ``{ "oid": "<id>" }`` (see mm_live_desk.py / Architect).
    """
    o = (oid or "").strip()
    if not o:
        return {"ok": False, "error": "oid required"}
    with st.lock:
        tok = st.token
    if not tok:
        return {"ok": False, "error": "not logged in"}
    with st.lock:
        base = st.rest_endpoint.strip().rstrip("/")
    ob = architect_orders_base(base)
    hdr = {
        "Authorization": f"Bearer {tok}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    last: tuple[int, object] = (0, {})
    for pth in ("/cancel_order", "/cancel-order"):
        code, body = http_json("POST", f"{ob}{pth}", {"oid": o}, hdr, timeout=30.0)
        last = (code, body)
        if code != 404:
            break
    return {"ok": 200 <= last[0] < 300, "http": last[0], "body": last[1]}


def _neon_product_catalog_path(dc: dict) -> Path:
    desk = dc.get("mm_desk") if isinstance(dc.get("mm_desk"), dict) else {}
    raw = str(desk.get("neon_product_catalog_path", "logs/neon_products_catalog.json") or "").strip()
    p = Path(raw).expanduser()
    if not p.is_absolute():
        p = Path.cwd() / p
    return p


def _read_neon_catalog_file(path: Path) -> list[str]:
    if not path.is_file():
        return []
    try:
        j = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []
    if not isinstance(j, list):
        return []
    out: list[str] = []
    seen: set[str] = set()
    for x in j:
        s = str(x).strip()
        if not s:
            continue
        k = neon_fix_feed.fix_symbol_canonical(s)
        if k in seen:
            continue
        seen.add(k)
        out.append(s)
    out.sort(key=lambda z: z.upper())
    return out


def merge_neon_catalog_file(path: Path, symbols: list[str]) -> list[str]:
    cur = list(_read_neon_catalog_file(path))
    seen = {neon_fix_feed.fix_symbol_canonical(x) for x in cur}
    for s in symbols:
        t = str(s).strip()
        if not t:
            continue
        k = neon_fix_feed.fix_symbol_canonical(t)
        if k in seen:
            continue
        seen.add(k)
        cur.append(t)
    cur.sort(key=lambda z: z.upper())
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(cur, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    except OSError:
        pass
    return cur


def _neon_md_symbols_from_merged(dc: dict) -> list[str]:
    ext = dc.get("external_feed") if isinstance(dc.get("external_feed"), dict) else {}
    fix = ext.get("fix") if isinstance(ext.get("fix"), dict) else {}
    raw = fix.get("md_symbols")
    out: list[str] = []
    if isinstance(raw, list):
        for x in raw:
            if x is not None and str(x).strip():
                out.append(str(x).strip())
    return out


def neon_product_dropdown_union(dc: dict, neon_catalog_live: list[str]) -> list[str]:
    """Union of on-disk catalog, config md_symbols, and in-memory SL snapshot (sorted)."""
    path = _neon_product_catalog_path(dc)
    on_disk = _read_neon_catalog_file(path)
    md = _neon_md_symbols_from_merged(dc)
    seen: set[str] = set()
    merged: list[str] = []
    for block in (on_disk, md, neon_catalog_live):
        for s in block:
            t = str(s).strip()
            if not t:
                continue
            k = neon_fix_feed.fix_symbol_canonical(t)
            if k in seen:
                continue
            seen.add(k)
            merged.append(t)
    merged.sort(key=lambda z: z.upper())
    return merged


def desk_apply_neon_security_list(st: DeskSharedState, symbols: list[str]) -> list[str]:
    """Append Security List (35=y) tag-55 symbols to catalog file; optional auto-merge into fix.md_symbols.
    Returns FIX 55 strings newly appended to external_feed.fix.md_symbols (for incremental MDR)."""
    dc = get_merged_config_dict()
    path = _neon_product_catalog_path(dc)
    merged = merge_neon_catalog_file(path, symbols)
    desk = dc.get("mm_desk") if isinstance(dc.get("mm_desk"), dict) else {}
    try:
        poll_sec = float(desk.get("neon_security_list_poll_sec") or 0.0)
    except (TypeError, ValueError):
        poll_sec = 0.0
    auto_raw = desk.get("neon_auto_merge_catalog_to_md_symbols")
    if auto_raw is None and poll_sec > 0.0:
        auto = True
    elif isinstance(auto_raw, str):
        auto = auto_raw.strip().lower() in ("1", "true", "yes", "on")
    else:
        auto = bool(auto_raw)
    new_md_syms: list[str] = []
    if auto and symbols:
        try:
            if DEFAULT_CONFIG_PATH.is_file():
                disk = json.loads(DEFAULT_CONFIG_PATH.read_text(encoding="utf-8"))
            else:
                disk = {}
            if isinstance(disk, dict):
                ext = disk.setdefault("external_feed", {})
                fix = ext.setdefault("fix", {})
                cur = fix.get("md_symbols")
                base = [str(x).strip() for x in cur if isinstance(cur, list) and str(x).strip()]
                before_c = {neon_fix_feed.fix_symbol_canonical(x) for x in base}
                for s in symbols:
                    t = str(s).strip()
                    if not t:
                        continue
                    k = neon_fix_feed.fix_symbol_canonical(t)
                    if k in before_c:
                        continue
                    before_c.add(k)
                    base.append(t)
                    new_md_syms.append(t)
                fix["md_symbols"] = base
                _atomic_write_json(DEFAULT_CONFIG_PATH, disk)
                if new_md_syms:
                    desk_log(
                        st,
                        f"Neon auto-merge: external_feed.fix.md_symbols → {len(base)} symbol(s) (+{new_md_syms!r})",
                        verbose_only=False,
                    )
        except (OSError, json.JSONDecodeError, TypeError) as e:
            desk_log(st, f"Neon auto-merge md_symbols failed: {e}", verbose_only=False)
    with st.lock:
        st.neon_catalog_symbols = merged
        st.neon_security_list_updated_ms = int(time.time() * 1000)
    return new_md_syms


def persist_neon_fix_md_symbols(
    st: DeskSharedState | None,
    md_symbols: list[str],
) -> tuple[bool, str]:
    try:
        if not DEFAULT_CONFIG_PATH.is_file():
            return False, f"missing {DEFAULT_CONFIG_PATH.name}"
        disk = json.loads(DEFAULT_CONFIG_PATH.read_text(encoding="utf-8"))
        if not isinstance(disk, dict):
            return False, "invalid config"
        ext = disk.setdefault("external_feed", {})
        fix = ext.setdefault("fix", {})
        seen: set[str] = set()
        out: list[str] = []
        for x in md_symbols:
            t = str(x).strip()
            if not t:
                continue
            k = neon_fix_feed.fix_symbol_canonical(t)
            if k in seen:
                continue
            seen.add(k)
            out.append(t)
        sym0 = str(ext.get("symbol", "") or "").strip()
        if sym0 and not any(
            neon_fix_feed.fix_symbol_canonical(sym0) == neon_fix_feed.fix_symbol_canonical(x) for x in out
        ):
            out.insert(0, sym0)
        fix["md_symbols"] = out
        _atomic_write_json(DEFAULT_CONFIG_PATH, disk)
        msg = f"{DEFAULT_CONFIG_PATH.name} external_feed.fix.md_symbols ({len(out)} syms)"
        if st is not None:
            with st.lock:
                st.config_last_write = msg
        return True, msg
    except Exception as e:
        return False, str(e)


def desk_reload_config_into_state(st: DeskSharedState) -> None:
    """Reload disk config into in-memory desk (symbols, book lists, MM params). Does not clear Bearer token."""
    cfg = load_app_config()
    with st.lock:
        st.rest_endpoint = str(cfg["rest_endpoint"]).strip().rstrip("/")
        st.ws_endpoint = str(cfg.get("ws_endpoint") or "").strip()
        st.ax_symbol = str(cfg.get("ax_symbol") or "").strip()
        st.ref_symbol = str(cfg.get("ref_symbol") or "").strip()
        st.ref_display = str(cfg.get("ref_display_symbol") or "").strip()
        bu = str(cfg.get("ref_rest_url") or "").strip()
        st.ref_rest_url = bu if bu.startswith("http") else ""
        st.ref_rest_uses_spot_path = bool(cfg.get("ref_rest_uses_spot_path", True))
        st.api_key = str(cfg.get("api_key") or "")
        st.api_secret = str(cfg.get("api_secret") or "")
        absyms = cfg.get("ax_book_symbols")
        if isinstance(absyms, list) and absyms:
            st.ax_book_symbols = [str(x).strip() for x in absyms if str(x).strip()]
        else:
            st.ax_book_symbols = [st.ax_symbol] if st.ax_symbol else []
        st.mm_width = int(cfg.get("mm_width", st.mm_width))
        st.mm_order_size = int(cfg.get("mm_order_size", st.mm_order_size))
        st.mm_order_size_step = max(1, int(cfg.get("mm_order_size_step", st.mm_order_size_step)))
        st.mm_max_position = int(cfg.get("mm_max_position", st.mm_max_position))
        st.mm_adjust_position = int(cfg.get("mm_adjust_position", st.mm_adjust_position))
        st.mm_adjust_ticks = int(cfg.get("mm_adjust_ticks", st.mm_adjust_ticks))
        st.price_tick = float(cfg.get("price_tick", st.price_tick))
        st.mm_bid_width = int(cfg.get("mm_bid_width", st.mm_width))
        st.mm_ask_width = int(cfg.get("mm_ask_width", st.mm_width))
        st.mm_resting_ticks = int(cfg.get("mm_resting_ticks", st.mm_resting_ticks))
        st.mm_basis = float(cfg.get("mm_basis", st.mm_basis))
        st.mm_max_reload_cycles = int(cfg.get("mm_max_reload_cycles", st.mm_max_reload_cycles))
        st.mm_current_reload_count = int(cfg.get("mm_current_reload_count", st.mm_current_reload_count))
        st.mm_reload_cycles_count_fill_only = bool(
            cfg.get("mm_reload_cycles_count_fill_only", st.mm_reload_cycles_count_fill_only)
        )
        st.mm_reload_limit_reset_nonce = str(cfg.get("mm_reload_limit_reset_nonce") or st.mm_reload_limit_reset_nonce)
        st.mm_requote_on_theo_move = bool(cfg.get("mm_requote_on_theo_move", st.mm_requote_on_theo_move))
        st.mm_orders_enabled = bool(cfg.get("mm_orders_enabled", st.mm_orders_enabled))
        st.mm_min_theo_move_ticks_to_requote = _clamp_min_theo_move_ticks_to_requote(
            int(cfg.get("mm_min_theo_move_ticks_to_requote", st.mm_min_theo_move_ticks_to_requote))
        )
        st.mm_pricing_tick = float(cfg.get("mm_pricing_tick", st.mm_pricing_tick))
        st.reference_provider = str(cfg.get("reference_provider") or st.reference_provider)
        st.multi_theo = bool(cfg.get("multi_theo", st.multi_theo))
        st.cme_feed_rest_url = str(cfg.get("cme_feed_rest_url") or st.cme_feed_rest_url)
        st.config_external_provider = str(
            cfg.get("config_external_provider") or st.config_external_provider
        )
        st.external_feed_name = str(cfg.get("external_feed_name") or st.external_feed_name)
        st.neon_fix_host = str(cfg.get("neon_fix_host") or st.neon_fix_host)
        st.neon_fix_port = int(cfg.get("neon_fix_port", st.neon_fix_port) or 0)
        bns = cfg.get("ref_book_symbols")
        if isinstance(bns, list) and bns:
            if st.reference_provider in ("neon_fix", "multi"):
                st.ref_book_symbols = [str(x).strip() for x in bns if str(x).strip()]
            else:
                st.ref_book_symbols = [str(x).strip().upper() for x in bns if str(x).strip()]
        else:
            if st.reference_provider in ("neon_fix", "multi"):
                st.ref_book_symbols = [st.ref_symbol] if st.ref_symbol else []
            else:
                st.ref_book_symbols = [st.ref_symbol.upper()] if st.ref_symbol else []
        st.ax_grid_symbols = list(st.ax_book_symbols)
        st.mm_instruments = list(cfg.get("mm_instruments") or [])
        try:
            _dc_r = get_merged_config_dict()
            st.neon_catalog_symbols = _read_neon_catalog_file(_neon_product_catalog_path(_dc_r))
        except Exception:
            pass
        st.web_desk_run_reference_feed = _web_desk_run_reference_feed_from_env(st.reference_provider)


def _ax_auth_headers(st: DeskSharedState) -> dict | None:
    with st.lock:
        tok = st.token
    if not tok:
        return None
    return {"Authorization": f"Bearer {tok}", "Accept": "application/json"}


def _ax_api_get(
    st: DeskSharedState,
    path: str,
    params: dict,
    *,
    timeout: float | None = None,
) -> tuple[int, object]:
    with st.lock:
        base = st.rest_endpoint.rstrip("/")
    h = _ax_auth_headers(st)
    if not h:
        return 401, {"error": "not logged in"}
    q = urllib.parse.urlencode(params)
    url = f"{base}{path}?{q}"
    to = AX_HTTP_TIMEOUT if timeout is None else timeout
    return http_json("GET", url, None, h, timeout=to)


def _orders_gateway_get(st: DeskSharedState, path: str, params: dict | None = None) -> tuple[int, object]:
    with st.lock:
        rest = st.rest_endpoint.rstrip("/")
        ob = architect_orders_base(rest)
    h = _ax_auth_headers(st)
    if not h:
        return 401, {"error": "not logged in"}
    q = ("?" + urllib.parse.urlencode(params)) if params else ""
    url = f"{ob.rstrip('/')}{path}{q}"
    return http_json("GET", url, None, h, timeout=ORDERS_HTTP_TIMEOUT)


def _orders_gateway_post(
    st: DeskSharedState,
    path: str,
    body: dict | None,
    *,
    timeout: float | None = None,
) -> tuple[int, object]:
    with st.lock:
        rest = st.rest_endpoint.rstrip("/")
        ob = architect_orders_base(rest)
        tok = st.token
    if not tok:
        return 401, {"error": "not logged in"}
    url = f"{ob.rstrip('/')}{path}"
    hdr = {"Authorization": f"Bearer {tok}", "Accept": "application/json", "Content-Type": "application/json"}
    to = ORDERS_HTTP_TIMEOUT if timeout is None else timeout
    return http_json("POST", url, body if body is not None else {}, hdr, timeout=to)


def _set_desk_action(st: DeskSharedState, msg: str) -> None:
    with st.lock:
        st.desk_last_action = msg[:500]
        st.desk_last_action_ms = int(time.time() * 1000)


def desk_cancel_all_orders(st: DeskSharedState, *, symbol: str | None) -> dict:
    """
    Order gateway: POST /cancel-all-orders (Architect docs) with optional {"symbol": "..."}.
    Falls back to /cancel_all_orders if the gateway returns 404.
    """
    with st.lock:
        if not st.token:
            return {"ok": False, "error": "not logged in"}
    sym = (symbol or "").strip() or None
    payload: dict = {"symbol": sym} if sym else {}
    for path in ("/cancel-all-orders", "/cancel_all_orders"):
        code, resp = _orders_gateway_post(st, path, payload, timeout=max(ORDERS_HTTP_TIMEOUT, 20.0))
        if code == 404:
            continue
        if code == 0:
            detail = resp if isinstance(resp, dict) else str(resp)
            return {"ok": False, "error": "network or timeout", "detail": detail}
        ok_http = 200 <= code < 300
        msg = f"Cancel all AX orders HTTP {code}" + (f" ({sym})" if sym else " (all symbols)")
        _set_desk_action(st, msg + (" — ok" if ok_http else " — failed"))
        body_snip = ""
        if isinstance(resp, dict):
            body_snip = str(resp.get("message") or resp.get("error") or "")[:120]
        return {
            "ok": ok_http,
            "http": code,
            "path": path,
            "response": resp,
            "error": None if ok_http else (body_snip or f"HTTP {code}"),
        }
    return {"ok": False, "error": "cancel-all endpoint not found (tried cancel-all-orders)"}


def _ax_mid_primary_st(st: DeskSharedState) -> float | None:
    """Architect mid from cached /book (primary AX symbol); used when web desk does not poll external reference."""
    with st.lock:
        sym = (st.ax_symbol or "").strip()
    if not sym:
        return None
    bd, ad = _best_bid_ask_ax(st, sym)
    if bd is None or ad is None or not math.isfinite(bd) or not math.isfinite(ad):
        return None
    if ad <= bd:
        return None
    return (bd + ad) / 2.0


def _best_bid_ask_ax(st: DeskSharedState, sym: str) -> tuple[float | None, float | None]:
    s = (sym or "").strip()
    if not s:
        return None, None
    with st.lock:
        d = st.ax_all_books.get(s)
        if d and not d.get("_err"):
            bids, asks = d.get("bids") or [], d.get("asks") or []
            if isinstance(bids, list) and isinstance(asks, list) and bids and asks:
                try:
                    return float(bids[0][0]), float(asks[0][0])
                except (TypeError, ValueError, IndexError):
                    pass
        if st.ax_symbol.strip() == s and st.last_ax_book:
            b, a = st.last_ax_book
            if b and a:
                try:
                    return float(b[0][0]), float(a[0][0])
                except (TypeError, ValueError, IndexError):
                    pass
    return None, None


def _reference_rest_book_ticker_mid_st(st: DeskSharedState) -> float | None:
    """REST reference mid: bookTicker-style bid/ask average (matches C++ ExternalFeedManager paths)."""
    with st.lock:
        sym = st.ref_symbol.strip()
        base = st.ref_rest_url.rstrip("/")
        use_spot = st.ref_rest_uses_spot_path
        provider = st.reference_provider
    if not sym:
        return None
    if _is_hyperliquid_provider(provider):
        if not base.startswith("http"):
            base = "https://api.hyperliquid.xyz"
        try:
            mids, _dt = _hyperliquid_fetch_all_mids(base, min(REFERENCE_HTTP_TIMEOUT, 20.0))
            if not mids:
                return None
            mv, _used = _hyperliquid_pick_mid_for_symbol(mids, sym)
            return mv
        except Exception:
            return None

    if not base.startswith("http"):
        return None
    url = _reference_book_ticker_http_url(base, sym.strip().upper(), use_spot)
    try:
        req = urllib.request.Request(url, method="GET")
        with urllib.request.urlopen(req, context=_ssl_ctx(), timeout=min(REFERENCE_HTTP_TIMEOUT, 20.0)) as resp:
            j = json.loads(resp.read().decode("utf-8"))
        if not isinstance(j, dict):
            return None
        bf = _to_float(j.get("bidPrice"))
        af = _to_float(j.get("askPrice"))
        if bf is None or af is None or bf <= 0 or af <= 0:
            return None
        return (bf + af) / 2.0
    except Exception:
        return None


def _cpp_theo_mid_from_cache() -> float | None:
    c = read_cpp_theo_cache_for_desk()
    if not c:
        return None
    mid = _to_float(c.get("mid"))
    if mid is not None and math.isfinite(mid) and mid > 0:
        return float(mid)
    return None


def _theo_mid_for_leg_ax(st: DeskSharedState, ax0: str) -> float | None:
    """
    C++ theo for one MM leg: quotes_by_canonical (mm_external_theo.json), then per-ref book from the desk.
    """
    th_ref = _theo_ref_for_ax_symbol(st, (ax0 or "").strip()) if (ax0 or "").strip() else ""
    if th_ref:
        c = read_cpp_theo_cache_for_desk()
        if c and isinstance(c, dict):
            want = fix_symbol_canonical_py(th_ref)
            qbc = c.get("quotes_by_canonical")
            if isinstance(qbc, dict):
                for k, v in qbc.items():
                    if fix_symbol_canonical_py(str(k)) == want and isinstance(v, dict):
                        m = _to_float(v.get("mid"))
                        if m is not None and math.isfinite(m) and m > 0:
                            return float(m)
        with st.lock:
            rab = dict(st.ref_all_books or {})
        bd = rab.get(th_ref) or rab.get((th_ref or "").upper()) or {}
        if isinstance(bd, dict) and not bd.get("_err"):
            bbs, aas = bd.get("bids") or [], bd.get("asks") or []
            if bbs and aas:
                try:
                    b0, a0 = bbs[0], aas[0]
                    bf, af = float(b0[0]), float(a0[0])
                    if (
                        math.isfinite(bf)
                        and math.isfinite(af)
                        and bf > 0
                        and af > 0
                        and af > bf
                    ):
                        return (bf + af) / 2.0
                except (TypeError, ValueError, IndexError):
                    pass
    return None


def _reference_theo_mid_st(
    st: DeskSharedState, *, theo_work_ax: str | None = None
) -> float | None:
    """External theo: Neon TOB or REST bookTicker when the desk polls reference; else AX mid if web ref feed off."""
    with st.lock:
        prov = st.reference_provider
        wrf = st.web_desk_run_reference_feed
        ax_st = (st.ax_symbol or "").strip()
    ax0 = (theo_work_ax or "").strip() if theo_work_ax is not None else ax_st
    if ax0:
        m_leg = _theo_mid_for_leg_ax(st, ax0)
        if m_leg is not None:
            return m_leg
    if not ax0 and (prov == "multi" or getattr(st, "multi_theo", False)) and ax_st:
        m2 = _theo_mid_for_leg_ax(st, ax_st)
        if m2 is not None:
            return m2
    if prov == "neon_fix":
        if not wrf:
            cm = _cpp_theo_mid_from_cache()
            if cm is not None:
                return cm
        with st.lock:
            lb = st.last_ref_book
        if lb:
            b0, a0 = lb[0], lb[1]
            if b0 and a0:
                try:
                    bf, af = float(b0[0][0]), float(a0[0][0])
                    if math.isfinite(bf) and math.isfinite(af) and bf > 0 and af > 0 and af > bf:
                        return (bf + af) / 2.0
                except (TypeError, ValueError, IndexError):
                    pass
        if wrf:
            return None
        return _ax_mid_primary_st(st)
    rest_mid = _reference_rest_book_ticker_mid_st(st)
    if rest_mid is not None:
        return rest_mid
    if wrf:
        return None
    return _ax_mid_primary_st(st)


def _format_gateway_price(price: float) -> str:
    return f"{price:.10f}".rstrip("0").rstrip(".")


def _desk_place_limit_gtc(
    st: DeskSharedState,
    *,
    symbol: str,
    side: str,
    qty: int,
    price: float,
    trace_tag: str = "",
) -> tuple[int, object]:
    """POST order-gateway place_order / place-order (GTC), side B or S."""
    with st.lock:
        ob = architect_orders_base(st.rest_endpoint.strip().rstrip("/"))
        hdr = {
            "Authorization": f"Bearer {st.token}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
    place_body = {
        "s": symbol,
        "d": side,
        "q": int(qty),
        "p": _format_gateway_price(price),
        "tif": "GTC",
        "po": False,
    }
    tag = trace_tag or "place_order"
    desk_log(
        st,
        f"{tag}: POST gateway place_body s={place_body['s']!r} d={place_body['d']!r} "
        f"q={place_body['q']!r} p={place_body['p']!r}",
    )
    last: tuple[int, object] = (0, {})
    for ppath in ("/place_order", "/place-order"):
        code, resp = http_json("POST", f"{ob.rstrip('/')}{ppath}", place_body, hdr, timeout=30.0)
        last = (code, resp)
        desk_log(st, f"{tag}: HTTP {code} path={ppath!r} response_snip={str(resp)[:220]!r}")
        if code != 404:
            break
    return last


def desk_requote_mm_pair_after_cancel(
    st: DeskSharedState,
    *,
    symbol: str | None = None,
    log_context: str = "desk_requote",
    persist_reload_count_zero: bool = True,
) -> dict:
    """
    After cancel-all on the MM AX symbol (or additive manual place): refresh net from GET /positions, compute
    bid/ask like MakeMarketStrategy::runFullMmQuoteCycle (skew + max_position side pull + non-cross vs AX book),
    place up to two GTC limits.

    *persist_reload_count_zero*: if True, also reset C++ `current_reload_count` on success (default for requote).
    Set False for additive "manual" placements so a one-off add does not clear the reload counter.
    """
    with st.lock:
        tok = st.token
        sym = (symbol or st.ax_symbol or "").strip()
        basis = float(st.mm_basis)
    tick, ostep = _leg_tick_and_ostep_for_ax_st(st, sym)
    (
        bid_w0,
        ask_w0,
        qty,
        max_po,
        adj_po,
        adj_tk,
        ostep_ov,
        stk_qs,
        stk_ps,
        stk_sl,
    ) = _mm_leg_stack_quote_params_from_config(st, sym)
    if ostep_ov is not None and ostep_ov >= 1:
        ostep = ostep_ov
    if not tok:
        desk_log(st, f"{log_context}: skip — not logged in")
        return {"ok": False, "error": "not logged in"}
    if not sym:
        desk_log(st, f"{log_context}: skip — no ax_symbol")
        return {"ok": False, "error": "no symbol"}
    if tick <= 0 or not math.isfinite(tick):
        desk_log(
            st,
            f"{log_context}: skip — no gateway tick_size for {sym!r} (GET /instruments); "
            "use POST /api/desk/sync_instrument_ticks or wait for periodic refresh",
        )
        return {
            "ok": False,
            "error": f"no gateway tick_size for {sym} — GET /instruments",
        }

    if adj_po <= 0:
        adj_po = 1
    if adj_tk <= 0:
        adj_tk = 1
    if max_po <= 0:
        max_po = 1
    if bid_w0 < 1:
        bid_w0 = 1
    if ask_w0 < 1:
        ask_w0 = 1

    # Client mandate: desk uses bid_width/ask_width only (no resting_depth_extra_ticks). C++ may
    # still read resting_depth_extra_ticks from JSON — keep that key at 0 for client-facing width.
    net = _exchange_net_st_for_symbol(st, sym)
    if net is None:
        desk_log(st, f"{log_context}: skip — cannot read position (GET /positions)")
        return {"ok": False, "error": "cannot read position (GET /positions)"}

    mid = _desk_mm_theo_mid_for_symbol(
        st, sym, quote_snapshot=stk_qs, pricer_snapshot=stk_ps
    )
    if mid is None or not math.isfinite(mid) or mid <= 0:
        desk_log(
            st,
            f"{log_context}: skip — theo mid unavailable (need mm_external_theo.json, AX book, or web reference feed)",
        )
        return {
            "ok": False,
            "error": "Theo mid unavailable — wait for C++ mm_external_theo.json, Architect /book, or enable MM_LIVE_DESK_WEB_REFERENCE_FEED",
        }

    bid_spread_bps = bid_w0
    ask_spread_bps = ask_w0
    new_mid = apply_pricer_snapshot_transform(mid, stk_qs, stk_ps, stk_sl)
    adjusted_theo = new_mid + basis
    raw_bid, raw_ask = compute_skewed_raw_bid_ask(
        adjusted_theo,
        bid_spread_bps,
        ask_spread_bps,
        tick,
        net,
        adj_po,
        adj_tk,
        max_po,
    )
    bid_px, ask_px = finalize_mm_pair_on_tick_grid(
        raw_bid, raw_ask, bid_spread_bps, ask_spread_bps, tick
    )
    skew_log = _mm_skew_intermediate_for_log(
        adjusted_theo, bid_spread_bps, ask_spread_bps, tick, net, adj_po, adj_tk, max_po
    )

    m_bid, m_ask = _best_bid_ask_ax(st, sym)
    if m_bid is None or m_ask is None or m_ask < m_bid:
        try:
            bb, aa = fetch_ax_book_st(st, sym, 5)
            if bb and aa:
                m_bid, m_ask = float(bb[0][0]), float(aa[0][0])
        except Exception:
            pass

    ok_v, vreason = validate_mm_quotes_non_cross(bid_px, ask_px, m_bid, m_ask)
    bid_px_adj, ask_px_adj, auto_non_cross_ok = _adjust_mm_pair_to_non_cross(
        bid_px,
        ask_px,
        bid_spread_bps=bid_spread_bps,
        ask_spread_bps=ask_spread_bps,
        quote_tick=tick,
        market_bid=m_bid,
        market_ask=m_ask,
    )
    if auto_non_cross_ok:
        bid_px = bid_px_adj
        ask_px = ask_px_adj
        ok_v, vreason = validate_mm_quotes_non_cross(bid_px, ask_px, m_bid, m_ask)
    trace_pre: list[tuple[str, object]] = [
        ("log_context", log_context),
        ("ax_symbol", sym),
        ("theo_mid_raw", mid),
        # Pricer-snapshot anchors that the live re-quote loop just used. If both snapshots are 0 the
        # transform is intentionally disabled (raw theo passes through). If you submitted snapshots
        # via the desk UI but see them as 0 here, the orders.json row for this AX is missing them
        # and the per-stack apply did not propagate — see _latest_active_stack_row_for_ax.
        ("pricer_xform_quote_snapshot", stk_qs),
        ("pricer_xform_pricer_snapshot", stk_ps),
        ("pricer_xform_slope", stk_sl),
        ("pricer_xform_active", bool(stk_qs > 0.0 and stk_ps > 0.0)),
        ("new_mid_after_pricer_xform", new_mid),
        ("mm_basis", basis),
        ("adjusted_theo", adjusted_theo),
        ("price_tick", tick),
        ("bid_width_bps", bid_spread_bps),
        ("ask_width_bps", ask_spread_bps),
        ("net_position", net),
        ("max_position", max_po),
        ("adjust_position", adj_po),
        ("adjust_ticks", adj_tk),
        ("order_size", qty),
        ("order_size_step", ostep),
        ("skew_gate_abs_net_lt_max", skew_log["skew_gate_abs_net_lt_max"]),
        ("skew_tick_units", skew_log["skew_tick_units"]),
        ("skew_price", skew_log["skew_price"]),
        ("raw_bid", raw_bid),
        ("raw_ask", raw_ask),
        ("bid_px_final_grid", bid_px),
        ("ask_px_final_grid", ask_px),
        ("ax_market_bid", m_bid),
        ("ax_market_ask", m_ask),
        ("auto_non_cross_adjusted", auto_non_cross_ok),
        ("non_cross_ok", ok_v),
        ("non_cross_reason", vreason if not ok_v else ""),
    ]
    if not ok_v:
        log_mm_desk_order_trace(st, f"{log_context} (validation failed, no orders sent)", trace_pre)
        msg = (
            f"MM requote {sym}: validation failed ({vreason}) bid={bid_px} ask={ask_px} ax_top={m_bid}/{m_ask}"
        )
        _set_desk_action(st, msg)
        return {
            "ok": False,
            "error": vreason,
            "bid": bid_px,
            "ask": ask_px,
            "ax_bid": m_bid,
            "ax_ask": m_ask,
            "net_position": net,
            "theo_mid": mid,
            "adjusted_theo": adjusted_theo,
        }

    leg_gate = _mm_round_qty_to_step_down_int(int(qty), ostep)
    if ostep > 1 and int(qty) > 0 and int(qty) % ostep != 0:
        desk_log(
            st,
            f"{log_context}: order_size={qty} is not a multiple of order_size_step={ostep}; "
            f"using leg_qty={leg_gate} for gates and placement (align order_size to the lot step)",
        )
    want_bid = should_quote_mm_side(net, max_po, buy_side=True)
    want_ask = should_quote_mm_side(net, max_po, buy_side=False)
    if qty <= 0:
        return {"ok": False, "error": "order_size <= 0", "net_position": net}

    qty_bid, qty_ask = mm_desk_pair_leg_quantities(want_bid, want_ask, int(qty), step=ostep)

    trace_send = trace_pre + [
        ("want_bid_should_quote", want_bid),
        ("want_ask_should_quote", want_ask),
        ("qty_bid_leg", qty_bid),
        ("qty_ask_leg", qty_ask),
    ]
    log_mm_desk_order_trace(st, f"{log_context} (inputs → prices → gates; about to POST)", trace_send)

    out: dict = {
        "net_position": net,
        "theo_mid": mid,
        "adjusted_theo": adjusted_theo,
        "bid": bid_px,
        "ask": ask_px,
        "bid_spread_bps": bid_spread_bps,
        "ask_spread_bps": ask_spread_bps,
        "want_bid": want_bid,
        "want_ask": want_ask,
        "qty_bid": qty_bid,
        "qty_ask": qty_ask,
        "order_size_step": ostep,
    }

    ok_all = True
    c_b = 0
    c_a = 0
    if want_bid and qty_bid > 0:
        c_b, r_b = _desk_place_limit_gtc(
            st, symbol=sym, side="B", qty=qty_bid, price=bid_px, trace_tag=f"{log_context}:BUY"
        )
        out["bid_http"] = c_b
        out["bid_response"] = r_b
        ok_all = ok_all and 200 <= c_b < 300
    elif want_bid:
        out["bid_skipped"] = "max_position_qty_headroom"
    else:
        out["bid_skipped"] = "max_position_side_pull"

    if want_ask and qty_ask > 0:
        c_a, r_a = _desk_place_limit_gtc(
            st, symbol=sym, side="S", qty=qty_ask, price=ask_px, trace_tag=f"{log_context}:SELL"
        )
        out["ask_http"] = c_a
        out["ask_response"] = r_a
        ok_all = ok_all and 200 <= c_a < 300
    elif want_ask:
        out["ask_skipped"] = "max_position_qty_headroom"
    else:
        out["ask_skipped"] = "max_position_side_pull"

    if not (want_bid and qty_bid > 0) and not (want_ask and qty_ask > 0):
        # Mirror the per-cause breakdown from desk_mm_stack_pair_place_on_gateway_then_tell_cpp so
        # the live re-quote loop's freeze reason is just as actionable as the new-stack path.
        reasons: list[str] = []
        if not want_bid and not want_ask:
            reasons.append(
                f"both sides pulled by max_position gate "
                f"(net={net}, max_position={max_po}); raise max_position or wait for inventory to drop"
            )
        else:
            if want_bid and qty_bid <= 0:
                reasons.append(
                    f"BUY qty=0 after step rounding (order_size={qty}, order_size_step={ostep})"
                )
            if want_ask and qty_ask <= 0:
                reasons.append(
                    f"SELL qty=0 after step rounding (order_size={qty}, order_size_step={ostep})"
                )
            if qty < ostep:
                reasons.append(
                    f"order_size ({qty}) < order_size_step ({ostep}) — increase order_size or lower step"
                )
        if not reasons:
            reasons.append("both sides pulled or zero headroom (max_position)")
        return {**out, "ok": False, "error": "; ".join(reasons)}

    placed_ok = (1 if (want_bid and qty_bid > 0 and 200 <= c_b < 300) else 0) + (
        1 if (want_ask and qty_ask > 0 and 200 <= c_a < 300) else 0
    )
    _ = placed_ok

    # Register the bid/ask pair for margin-reject paired cancellation, but only
    # when BOTH legs were placed together and accepted (server-layer tracking;
    # see tools/mm_paired_cancel.py).
    if (
        want_bid and qty_bid > 0 and 200 <= c_b < 300
        and want_ask and qty_ask > 0 and 200 <= c_a < 300
    ):
        try:
            _b_oid = _place_order_oid_from_gateway(out.get("bid_response"))
            _a_oid = _place_order_oid_from_gateway(out.get("ask_response"))
            if _b_oid and _a_oid:
                _MM_PAIR_REGISTRY.register_pair(
                    _b_oid, _a_oid, log_fn=lambda m: desk_log(st, m)
                )
        except Exception as _re:
            desk_log(
                st, f"paired-cancel register error (non-fatal): {type(_re).__name__}: {_re}"
            )

    summary = (
        f"MM requote {sym}: net={net} mid={mid:.6f} bid={bid_px} ask={ask_px} "
        f"placed B={want_bid} S={want_ask} ok={ok_all}"
    )
    _set_desk_action(st, summary)
    out["ok"] = ok_all
    out["message"] = summary
    if not ok_all:
        out["error"] = "one or more place_order calls failed"
    else:
        if persist_reload_count_zero:
            ok_z, msg_z = persist_market_maker_current_reload_count_zero(st)
            desk_log(
                st,
                "[MM DESK] Reset current_reload_count to 0 after manual requote."
                if ok_z
                else f"[MM DESK] Failed to reset current_reload_count: {msg_z}",
            )
    return out


def _place_order_oid_from_gateway(resp: object) -> str:
    """Parse exchange / gateway id from a place_order response body (dict or string)."""
    if isinstance(resp, str):
        try:
            o = json.loads(resp)
        except (json.JSONDecodeError, TypeError, ValueError):
            return ""
        resp = o
    if not isinstance(resp, dict):
        return ""
    for k in ("oid", "o", "order_id", "id", "exchange_order_id", "eoid"):
        v = resp.get(k)
        if v is not None and str(v).strip() and not isinstance(v, (list, dict)):
            return str(v).strip()
    d = resp.get("data")
    if isinstance(d, dict):
        for k in ("oid", "o", "order_id", "id"):
            v2 = d.get(k)
            if v2 is not None and str(v2).strip():
                return str(v2).strip()
    return ""


def _leg_tick_and_ostep_for_ax_st(st: DeskSharedState, ax: str) -> tuple[float, int]:
    """Tick + order size step for an AX product.

    **Quote tick** comes **only** from the Architect gateway ``GET /instruments`` map cached on
    ``DeskSharedState.ax_tick_by_symbol`` (refreshed on feed start and periodically). There is no
    fallback to ``market_maker.price_tick`` or ``PRICE_TICK``.

    **order_size_step** precedence (per AX product, NOT per tick):
      1. Leg-row ``order_size_step`` / ``order_step``  (forward-compat; not used in current configs)
      2. ``manual_stacks[0].order_size_step``          (current default_config convention — see
         instruments[].manual_stacks[0].order_size_step for every non-EUR symbol)
      3. ``1``                                         (safe AX min-lot for any product whose leg
         has no step declared anywhere)

    The legacy global ``market_maker.order_size_step`` was intentionally REMOVED from the fallback
    chain because it is the EURUSD-PERP default (``100``) and was incorrectly flooring new-stack
    placements for every other symbol — e.g. an XAG-PERP form submit with ``order_size=5`` rounded
    DOWN to ``0`` and tripped the ``mm_desk_pair_leg_quantities`` guard, surfacing a misleading
    ``"both sides pulled or zero headroom (max_position)"`` alert even with no position and a
    healthy ``max_position`` cap. The live re-quote loop was unaffected because it already reads
    ``manual_stacks[0].order_size_step`` via :func:`_mm_leg_stack_quote_params_from_config`; this
    function now matches that behavior so the new-stack-create path agrees with steady-state.
    """
    a = (ax or "").strip()
    gt = _gateway_tick_for_ax(st, a)
    t = float(gt) if gt is not None and gt > 0.0 and math.isfinite(float(gt)) else 0.0
    step = 0
    with st.lock:
        ins = list(st.mm_instruments or [])
    for row in ins:
        if not isinstance(row, dict):
            continue
        s0 = str(row.get("symbol") or row.get("ax_symbol") or "").strip()
        if s0 and _normalize_ax_symbol(s0) == _normalize_ax_symbol(a):
            stp = int(row.get("order_size_step") or row.get("order_step") or 0)
            if stp > 0:
                step = stp
            if step < 1:
                stk_list = row.get("manual_stacks")
                if isinstance(stk_list, list) and stk_list:
                    stk0 = stk_list[0] if isinstance(stk_list[0], dict) else None
                    if stk0:
                        try:
                            stp2 = int(stk0.get("order_size_step") or stk0.get("order_step") or 0)
                        except (TypeError, ValueError):
                            stp2 = 0
                        if stp2 > 0:
                            step = stp2
            break
    if step < 1:
        step = 1
    return t, step


_MM_XFORM_WARN_LOCK = threading.Lock()
_MM_XFORM_WARN_LAST_MS: dict[str, float] = {}


def _mm_xform_warn_if_throttled(sym: str, theo_scale: float,
                                qs: float, ps: float) -> None:
    """Mirror of the C++ throttled MM_XFORM warning (see mmTransformTheo in
    src/strategy/MakeMarketStrategy.cpp). Emits at most once per 5s per symbol.
    """
    now_ms = time.monotonic() * 1000.0
    key = (sym or "").upper()
    with _MM_XFORM_WARN_LOCK:
        last = _MM_XFORM_WARN_LAST_MS.get(key, 0.0)
        if last and (now_ms - last) < 5000.0:
            return
        _MM_XFORM_WARN_LAST_MS[key] = now_ms
    try:
        desk_log(
            None,
            "[MM_XFORM] sym={} theo_scale={:.6f} qs={:.6f} ps={:.6f} "
            "— theo_scale IGNORED while pricer-snapshot xform is active".format(
                key, float(theo_scale), float(qs), float(ps)))
    except Exception:
        pass


def _desk_mm_theo_mid_for_symbol(
    st: DeskSharedState,
    sym: str,
    *,
    quote_snapshot: float = 0.0,
    pricer_snapshot: float = 0.0,
) -> float | None:
    """Per-leg theo mid with theo_scale from mm_instruments — mirrors GUI previews.

    When the desk pricer-snapshot transform is active (quote_snapshot > 0 AND pricer_snapshot > 0),
    the legacy per-leg theo_scale is bypassed because the snapshot anchors already carry the
    scale relationship between pricer and quote markets. Mirrors C++
    MakeMarketStrategy::mmTransformTheo, which has the same short-circuit.

    invert_theo was REMOVED (2026-05-12) — use the pricer-snapshot transform for cross-scale legs.
    """
    s = (sym or "").strip()
    if not s:
        return None
    xform_active = float(quote_snapshot) > 0.0 and float(pricer_snapshot) > 0.0
    with st.lock:
        rows = list(st.mm_instruments or [])
    _, row = _find_mm_instrument_row(rows, s)
    if xform_active:
        try:
            legacy_scale = float(row.get("theo_scale") or 0.0) if row else 0.0
        except (TypeError, ValueError):
            legacy_scale = 0.0
        if legacy_scale > 0.0:
            _mm_xform_warn_if_throttled(s, legacy_scale,
                                        float(quote_snapshot), float(pricer_snapshot))
        theo_scale = 0.0
    else:
        try:
            theo_scale = float(row.get("theo_scale") or 0.0) if row else 0.0
        except (TypeError, ValueError):
            theo_scale = 0.0

    mid = _theo_mid_for_leg_ax(st, s)
    if mid is not None and math.isfinite(mid) and mid > 0:
        if theo_scale > 0.0:
            mid = mid * theo_scale
        if math.isfinite(mid) and mid > 0:
            return float(mid)
    return _reference_theo_mid_st(st, theo_work_ax=s)


def _latest_active_stack_row_for_ax(merged_cfg: dict, ax: str) -> dict | None:
    """Most recently created (by ``created_ms``) stack row in ``orders.json`` for ``ax``.

    Why this exists: per-stack runtime values that the operator submits via the desk UI
    (``quote_snapshot`` / ``pricer_snapshot`` / ``slope`` and the per-stack overrides for width,
    order_size, max_position, …) are persisted to ``orders.json`` — NOT to ``default_config.json``.
    The base-config `mm_instruments[].manual_stacks[0]` block is only the **boot-time default**; once
    the user submits a stack via ``/api/desk/place_order`` the truth lives in orders.json. The Python
    re-quote loop (``desk_requote_mm_pair_after_cancel``, used by Apply)
    was previously reading those values from ``mm_instruments`` only, which made it
    quote around RAW theo whenever the desk had submitted snapshot anchors (because the default-
    config values are 0/0/1 → transform disabled). Symptom: first place sat at the AX-snap mid, the
    very next requote jumped to the raw-theo mid and crossed the AX market. The C++ side already
    reads orders.json correctly via ``MarketMakerManualStack`` → ``mmApplyPricerSnapshotTransform``;
    this helper brings the Python path in line.
    """
    a = (ax or "").strip()
    if not a:
        return None
    try:
        doc = load_mm_orders_config(merged_cfg)
    except Exception:
        return None
    if not isinstance(doc, dict):
        return None
    a_norm = _normalize_ax_symbol(a)
    candidates: list[dict] = []
    products = doc.get("products") if isinstance(doc.get("products"), dict) else {}
    for pk, prod in products.items():
        if _normalize_ax_symbol(str(pk)) != a_norm:
            continue
        if not isinstance(prod, dict):
            continue
        stks = prod.get("stacks") if isinstance(prod.get("stacks"), list) else []
        for s in stks:
            if isinstance(s, dict) and _normalize_ax_symbol(str(s.get("ax_symbol") or pk)) == a_norm:
                candidates.append(s)
    root = doc.get("stacks") if isinstance(doc.get("stacks"), list) else []
    for s in root:
        if isinstance(s, dict) and _normalize_ax_symbol(str(s.get("ax_symbol") or "")) == a_norm:
            candidates.append(s)
    if not candidates:
        return None

    def _row_sort_key(row: dict) -> tuple[int, int]:
        try:
            ms = int(row.get("created_ms") or 0)
        except (TypeError, ValueError):
            ms = 0
        active = 1 if (bool(row.get("desk_seeded")) and str(row.get("submit_status") or "") == "active") else 0
        return (active, ms)

    candidates.sort(key=_row_sort_key)
    return candidates[-1]


def _mm_leg_stack_quote_params_from_config(
    st: DeskSharedState, sym: str
) -> tuple[int, int, int, int, int, int, int | None, float, float, float]:
    """bid/ask width ticks, qty, max_po, adj_po, adj_tk — global desk + first manual_stack override.

    Returns optional ``order_size_step`` from the first stack (else ``None`` — caller keeps
    :func:`_leg_tick_and_ostep_for_ax_st` step).

    Also returns the pricer-snapshot transform anchors (``quote_snapshot``, ``pricer_snapshot``,
    ``slope``) so every requote/preview path can call :func:`apply_pricer_snapshot_transform`
    consistently with the C++ side. Resolution precedence:
      1. orders.json — most recently created (preferring active+desk_seeded) stack for this AX.
         This is what the operator most-recently submitted via the desk UI and what C++ already
         reads on its side; reading it here keeps Python's re-quote loop and C++'s cancel-replace
         math identical for the same product so a desk-submitted stack does not get re-priced
         around raw theo on the very next cycle.
      2. ``default_config.json`` ``mm_instruments[ax].manual_stacks[0]`` — boot-time default; only
         used if orders.json has no row for this AX (fresh install, never-submitted product, or
         the user wiped orders.json).
      3. Defaults: snapshots = 0.0 (transform disabled, raw theo passes through), slope = 1.0
         (linear pass-through).
    """
    ostep_ov: int | None = None
    with st.lock:
        bid_w0 = int(st.mm_bid_width)
        ask_w0 = int(st.mm_ask_width)
        qty = int(st.mm_order_size)
        max_po = int(st.mm_max_position)
        adj_po = int(st.mm_adjust_position)
        adj_tk = int(st.mm_adjust_ticks)
        rows = list(st.mm_instruments or [])
    quote_snapshot = 0.0
    pricer_snapshot = 0.0
    slope = 1.0
    _, row = _find_mm_instrument_row(rows, sym)
    if row:
        stk_list = row.get("manual_stacks")
        if isinstance(stk_list, list) and stk_list and isinstance(stk_list[0], dict):
            stk0 = stk_list[0]
            try:
                w_val = stk0.get("width_bps")
                if w_val is None:
                    w_val = stk0.get("width")
                if w_val is not None:
                    bid_w0 = int(w_val)
                    ask_w0 = int(w_val)
                if stk0.get("order_size") is not None:
                    qty = int(stk0["order_size"])
                if stk0.get("order_size_step") is not None:
                    ostep_ov = max(1, int(stk0["order_size_step"]))
                if stk0.get("max_position") is not None:
                    max_po = int(stk0["max_position"])
                if stk0.get("adjust_position") is not None:
                    adj_po = int(stk0["adjust_position"])
                if stk0.get("adjust_ticks") is not None:
                    adj_tk = int(stk0["adjust_ticks"])
            except (TypeError, ValueError):
                pass
            try:
                if stk0.get("quote_snapshot") is not None:
                    quote_snapshot = float(stk0["quote_snapshot"])
                if stk0.get("pricer_snapshot") is not None:
                    pricer_snapshot = float(stk0["pricer_snapshot"])
                if stk0.get("slope") is not None:
                    slope = float(stk0["slope"])
            except (TypeError, ValueError):
                pass
    # orders.json overrides — runtime truth from the latest operator submission for this AX.
    # Carries the same per-stack overrides the C++ side reads from MarketMakerManualStack.
    try:
        merged_cfg_for_orders = get_merged_config_dict()
    except Exception:
        merged_cfg_for_orders = {}
    latest = _latest_active_stack_row_for_ax(merged_cfg_for_orders, sym) if merged_cfg_for_orders else None
    if isinstance(latest, dict):
        try:
            w_val = latest.get("width_bps")
            if w_val is None:
                w_val = latest.get("width_ticks")
            if w_val is None:
                w_val = latest.get("width")
            if w_val is not None:
                bid_w0 = int(w_val)
                ask_w0 = int(w_val)
            if latest.get("order_size") is not None:
                qty = int(latest["order_size"])
            if latest.get("order_size_step") is not None:
                ostep_ov = max(1, int(latest["order_size_step"]))
            if latest.get("max_position") is not None:
                max_po = int(latest["max_position"])
            if latest.get("adjust_position") is not None:
                adj_po = int(latest["adjust_position"])
            if latest.get("adjust_ticks") is not None:
                adj_tk = int(latest["adjust_ticks"])
        except (TypeError, ValueError):
            pass
        try:
            if latest.get("quote_snapshot") is not None:
                quote_snapshot = float(latest["quote_snapshot"])
            if latest.get("pricer_snapshot") is not None:
                pricer_snapshot = float(latest["pricer_snapshot"])
            if latest.get("slope") is not None:
                slope = float(latest["slope"])
        except (TypeError, ValueError):
            pass
    return bid_w0, ask_w0, qty, max_po, adj_po, adj_tk, ostep_ov, quote_snapshot, pricer_snapshot, slope


def desk_mm_stack_pair_place_on_gateway_then_tell_cpp(
    st: DeskSharedState,
    *,
    ax: str,
    width_ticks: int,
    order_size: int,
    max_position: int,
    adjust_position: int,
    adjust_ticks: int,
    quote_snapshot: float = 0.0,
    pricer_snapshot: float = 0.0,
    slope: float = 1.0,
    log_context: str = "desk_stack_place",
) -> dict:
    """
    Up to one bid and one ask on the order gateway (same non-cross + skew path as
    :func:`desk_requote_mm_pair_after_cancel` but with explicit stack params, **without**
    writing ``orders.json``). At ``|net| == max_position`` only the inventory-reducing
    side is placed — see :func:`should_quote_mm_side`. Used so we only persist a stack
    to disk after the gateway has accepted the placement(s).
    """
    sym = (ax or "").strip()
    with st.lock:
        tok = st.token
        basis = float(st.mm_basis)
    if not tok:
        desk_log(st, f"{log_context}: not logged in")
        return {"ok": False, "error": "not logged in"}
    if not sym:
        return {"ok": False, "error": "no ax_symbol"}
    if not _symbol_in_mm_instruments_st(st, sym):
        with st.lock:
            inst = st.mm_instruments
        if inst:
            return {
                "ok": False,
                "error": f"symbol {sym!r} is not in market_maker.instruments — edit default_config.json or fix config",
            }
    tick, ostep = _leg_tick_and_ostep_for_ax_st(st, sym)
    if adjust_position <= 0:
        adjust_position = 1
    if adjust_ticks <= 0:
        adjust_ticks = 1
    if max_position <= 0:
        max_position = 1

    net = _exchange_net_st_for_symbol(st, sym)
    if net is None:
        desk_log(st, f"{log_context}: cannot read position (GET /positions)")
        return {"ok": False, "error": "cannot read position (GET /positions)"}

    mid = _desk_mm_theo_mid_for_symbol(
        st, sym, quote_snapshot=quote_snapshot, pricer_snapshot=pricer_snapshot
    )
    if mid is None or not math.isfinite(mid) or mid <= 0:
        desk_log(st, f"{log_context}: theo mid unavailable")
        return {
            "ok": False,
            "error": "Theo mid unavailable — wait for C++ mm_external_theo.json, Architect /book, or web reference feed",
        }

    bid_w0 = int(width_ticks)
    ask_w0 = int(width_ticks)
    if bid_w0 < 1:
        bid_w0 = 1
    if ask_w0 < 1:
        ask_w0 = 1
    qty = int(order_size)
    max_po = int(max_position)
    adj_po = int(adjust_position)
    adj_tk = int(adjust_ticks)

    # Apply the desk pricer-snapshot transform BEFORE adding `basis`, then feed the result into
    # the same skew/finalize pipeline as before. This MUST match C++ — every cancel-replace the
    # strategy issues after C++ adopts these OIDs goes through MakeMarketStrategy::
    # mmApplyPricerSnapshotTransform with the same qs/ps/slope persisted in orders.json.
    new_mid = apply_pricer_snapshot_transform(mid, quote_snapshot, pricer_snapshot, slope)
    adjusted_theo = new_mid + basis
    raw_bid, raw_ask = compute_skewed_raw_bid_ask(
        adjusted_theo, bid_w0, ask_w0, tick, net, adj_po, adj_tk, max_po
    )
    bid_px, ask_px = finalize_mm_pair_on_tick_grid(
        raw_bid, raw_ask, bid_w0, ask_w0, tick
    )

    # This check applies only to the first manual/template placement.
    # Automatic requotes after placement are intentionally unchanged.
    m_bid, m_ask = _best_bid_ask_ax(st, sym)

    # If the cached AX book is missing or invalid, request the current book.
    if (
        m_bid is None
        or m_ask is None
        or not math.isfinite(m_bid)
        or not math.isfinite(m_ask)
        or m_bid <= 0
        or m_ask <= 0
        or m_bid >= m_ask
    ):
        try:
            bb, aa = fetch_ax_book_st(st, sym, 5)
            if bb and aa:
                m_bid = float(bb[0][0])
                m_ask = float(aa[0][0])
        except Exception:
            m_bid, m_ask = None, None

    # Position limits may disable one side. Only validate sides that would
    # actually be submitted.
    want_bid = should_quote_mm_side(net, max_po, buy_side=True)
    want_ask = should_quote_mm_side(net, max_po, buy_side=False)

    # Do not submit when the AX top of book cannot be established.
    if (
        m_bid is None
        or m_ask is None
        or not math.isfinite(m_bid)
        or not math.isfinite(m_ask)
        or m_bid <= 0
        or m_ask <= 0
        or m_bid >= m_ask
    ):
        error_message = (
            f"Cannot validate the {sym} quote because the current Architect "
            "best bid and best ask are unavailable. Orders not sent. "
            "Please wait for a valid order book and submit again."
        )

        desk_log(
            st,
            f"{log_context}: top of book unavailable; no orders sent",
        )

        return {
            "ok": False,
            "error_code": "TOP_OF_BOOK_UNAVAILABLE",
            "error": error_message,
            "ax_symbol": sym,
            "bid": bid_px,
            "ask": ask_px,
            "best_bid": m_bid,
            "best_ask": m_ask,
        }

    # A BUY at or above the current best ask is immediately marketable.
    if want_bid and bid_px >= m_ask:
        error_message = (
            f"BUY quote {bid_px} will trade immediately against the current "
            f"best ask {m_ask}. Orders not sent. Please put a valid quote "
            "for this level."
        )

        desk_log(
            st,
            f"{log_context}: marketable BUY rejected; no orders sent",
        )

        return {
            "ok": False,
            "error_code": "QUOTE_WOULD_TRADE_IMMEDIATELY",
            "error": error_message,
            "ax_symbol": sym,
            "side": "BUY",
            "quote_price": bid_px,
            "best_bid": m_bid,
            "best_ask": m_ask,
            "bid": bid_px,
            "ask": ask_px,
        }

    # A SELL at or below the current best bid is immediately marketable.
    if want_ask and ask_px <= m_bid:
        error_message = (
            f"SELL quote {ask_px} will trade immediately against the current "
            f"best bid {m_bid}. Orders not sent. Please put a valid quote "
            "for this level."
        )

        desk_log(
            st,
            f"{log_context}: marketable SELL rejected; no orders sent",
        )

        return {
            "ok": False,
            "error_code": "QUOTE_WOULD_TRADE_IMMEDIATELY",
            "error": error_message,
            "ax_symbol": sym,
            "side": "SELL",
            "quote_price": ask_px,
            "best_bid": m_bid,
            "best_ask": m_ask,
            "bid": bid_px,
            "ask": ask_px,
        }

    if qty <= 0:
        return {"ok": False, "error": "order_size <= 0", "net_position": net}
    qty_bid, qty_ask = mm_desk_pair_leg_quantities(want_bid, want_ask, int(qty), step=ostep)
    ostep0 = ostep
    if ostep0 > 1 and int(qty) > 0 and int(qty) % ostep0 != 0:
        desk_log(
            st,
            f"{log_context}: order_size={qty} is not a multiple of order_size_step={ostep0}; "
            f"using mm_desk_pair_leg gate step={ostep0}",
        )

    place_b = want_bid and qty_bid > 0
    place_a = want_ask and qty_ask > 0
    if not place_b and not place_a:
        # Surface the actual cause instead of a single bundled message. Two distinct ways this
        # condition can fire and the operator-facing fix differs for each:
        #   - SIDE_PULL: |net| >= max_position so should_quote_mm_side turned the side off.
        #     Mathematically the BUY-side cannot be off at the same time as the SELL-side, so
        #     hitting this for both legs simultaneously implies max_position <= 0 (clamped to 1
        #     internally) — config bug, not a runtime gating issue.
        #   - QTY_FLOOR: order_size rounded DOWN to 0 against order_size_step (e.g. order_size=5
        #     vs step=100, which is what the XAG-PERP "no position, max=100" alert turned out to
        #     be: _leg_tick_and_ostep_for_ax_st was falling back to the EURUSD global step). The
        #     step lookup itself is now fixed but this message stays in case a future config /
        #     stack-row mismatch reintroduces the same shape.
        reasons: list[str] = []
        if not want_bid and not want_ask:
            reasons.append(
                f"both sides pulled by max_position gate "
                f"(net={net}, max_position={max_po}); raise max_position or wait for inventory to drop"
            )
        else:
            if want_bid and qty_bid <= 0:
                reasons.append(
                    f"BUY qty=0 after step rounding (order_size={qty}, order_size_step={ostep})"
                )
            if want_ask and qty_ask <= 0:
                reasons.append(
                    f"SELL qty=0 after step rounding (order_size={qty}, order_size_step={ostep})"
                )
            if qty < ostep:
                reasons.append(
                    f"order_size ({qty}) < order_size_step ({ostep}) — increase order_size or lower step"
                )
        if not reasons:
            reasons.append("both sides pulled or zero headroom (max_position)")
        return {
            "ok": False,
            "error": "; ".join(reasons),
            "net_position": net,
            "max_position": max_po,
            "order_size": qty,
            "order_size_step": ostep,
            "want_bid": want_bid,
            "want_ask": want_ask,
            "qty_bid": qty_bid,
            "qty_ask": qty_ask,
            "bid": bid_px,
            "ask": ask_px,
            "theo_mid": mid,
        }

    out: dict = {
        "ok": True,
        "theo_mid": mid,
        "adjusted_theo": adjusted_theo,
        "bid": bid_px,
        "ask": ask_px,
        "want_bid": want_bid,
        "want_ask": want_ask,
        "qty_bid": qty_bid,
        "qty_ask": qty_ask,
        "order_size_step": ostep,
        "price_tick": tick,
    }

    bid_oid = ""
    ask_oid = ""
    c_b, c_a = 0, 0
    r_b: object = {}
    r_a: object = {}

    if place_b and place_a:
        with ThreadPoolExecutor(max_workers=2) as ex:
            fut_b = ex.submit(
                lambda: _desk_place_limit_gtc(
                    st,
                    symbol=sym,
                    side="B",
                    qty=qty_bid,
                    price=bid_px,
                    trace_tag=f"{log_context}:BUY",
                )
            )
            fut_a = ex.submit(
                lambda: _desk_place_limit_gtc(
                    st,
                    symbol=sym,
                    side="S",
                    qty=qty_ask,
                    price=ask_px,
                    trace_tag=f"{log_context}:SELL",
                )
            )
            c_b, r_b = fut_b.result()
            c_a, r_a = fut_a.result()
        out["bid_http"] = c_b
        out["bid_response"] = r_b
        out["ask_http"] = c_a
        out["ask_response"] = r_a
        if 200 <= c_b < 300:
            bid_oid = _place_order_oid_from_gateway(r_b)
        if 200 <= c_a < 300:
            ask_oid = _place_order_oid_from_gateway(r_a)
        if not (200 <= c_b < 300):
            return {**out, "ok": False, "error": "place_order bid failed (HTTP " + str(c_b) + ")"}
        if not (200 <= c_a < 300):
            if bid_oid:
                _ = desk_cancel_one_order(st, bid_oid)
            return {**out, "ok": False, "error": "place_order ask failed (HTTP " + str(c_a) + "); bid rolled back"}
    elif place_b:
        c_b, r_b = _desk_place_limit_gtc(
            st, symbol=sym, side="B", qty=qty_bid, price=bid_px, trace_tag=f"{log_context}:BUY"
        )
        out["bid_http"] = c_b
        out["bid_response"] = r_b
        if 200 <= c_b < 300:
            bid_oid = _place_order_oid_from_gateway(r_b)
        if not (200 <= c_b < 300):
            return {**out, "ok": False, "error": "place_order bid failed (HTTP " + str(c_b) + ")"}
    elif place_a:
        c_a, r_a = _desk_place_limit_gtc(
            st, symbol=sym, side="S", qty=qty_ask, price=ask_px, trace_tag=f"{log_context}:SELL"
        )
        out["ask_http"] = c_a
        out["ask_response"] = r_a
        if 200 <= c_a < 300:
            ask_oid = _place_order_oid_from_gateway(r_a)
        if not (200 <= c_a < 300):
            return {**out, "ok": False, "error": "place_order ask failed (HTTP " + str(c_a) + ")"}

    out["bid_exchange_oid"] = bid_oid
    out["ask_exchange_oid"] = ask_oid
    out["ok"] = True
    out["message"] = f"place stack {sym} ok"
    # Register the bid/ask pair so a later margin reject on one leg can pull the
    # other (server-layer only; see tools/mm_paired_cancel.py). Only when BOTH
    # legs were placed together and accepted (both exchange OIDs present).
    if bid_oid and ask_oid:
        try:
            _MM_PAIR_REGISTRY.register_pair(
                bid_oid, ask_oid, log_fn=lambda m: desk_log(st, m)
            )
        except Exception as _re:
            desk_log(
                st, f"paired-cancel register error (non-fatal): {type(_re).__name__}: {_re}"
            )
    desk_log(
        st,
        f"[DESK] place pair: ax={sym} bid_px={bid_px} ask_px={ask_px} qty={order_size}",
    )
    _set_desk_action(
        st, f"MM place stack {sym}: bid={bid_px} ask={ask_px} bid_oid={bid_oid!r} ask_oid={ask_oid!r}"
    )
    return out


def _symbol_in_mm_instruments_st(st: DeskSharedState, sym: str) -> bool:
    """If ``market_maker.instruments`` is non-empty, *sym* must be one of the listed AX legs."""
    ax = (sym or "").strip()
    if not ax:
        return False
    with st.lock:
        inst = list(st.mm_instruments or [])
    if not inst:
        return True
    want = _normalize_ax_symbol(ax)
    for r in inst:
        if not isinstance(r, dict):
            continue
        s0 = str(r.get("symbol") or r.get("ax_symbol") or "").strip()
        if s0 and _normalize_ax_symbol(s0) == want:
            return True
    return False


def desk_place_manual_mm_pair(st: DeskSharedState, *, symbol: str) -> dict:
    """
    Place a bid+ask GTC pair for *symbol* from current theo, net, and desk MM fields — **without**
    canceling any existing orders (additive; duplicate prices may be rejected or stack per venue rules).
    """
    sym = (symbol or "").strip()
    if not sym:
        return {"ok": False, "error": "no symbol"}
    if not _symbol_in_mm_instruments_st(st, sym):
        with st.lock:
            inst = st.mm_instruments
        if inst:
            return {
                "ok": False,
                "error": f"symbol {sym!r} is not in market_maker.instruments — edit default_config.json or fix config",
            }
    rq = desk_requote_mm_pair_after_cancel(
        st,
        symbol=sym,
        log_context="place_manual_mm",
        persist_reload_count_zero=False,
    )
    merged_ok = bool(rq.get("ok"))
    _write_mm_desk_signal_for_cpp(st, sym, rq, merged_ok)
    return rq


def _place_manual_persist_excluding_instruments(
    st: DeskSharedState, j: dict, *, persist_config: bool
) -> tuple[bool, str, dict | None]:
    """
    apply_persist–style body but **omits** ``ax_symbol``/``ref_symbol`` so a manual place for one leg
    does not repoint global ``market_maker.symbol`` / reference (multi-instrument).
    In-memory: ``desk_apply_form_to_state`` with the same (no-ax) body when *persist_config* is false.
    """
    skip = frozenset({"symbol", "persist_config"})
    pre: dict = {k: v for k, v in j.items() if k not in skip}
    for d in ("ax_symbol", "ref_symbol", "bn_symbol"):
        pre.pop(d, None)
    if not pre:
        return True, "no config keys", None
    ok_v, err_v = validate_mm_strategy_payload(pre)
    if not ok_v:
        return False, err_v, None
    if not persist_config:
        desk_apply_form_to_state(st, pre)
        return True, "in-memory only", pre
    ok_s, msg_s = persist_desk_config_to_repo(st, pre)
    if ok_s:
        desk_reload_config_into_state(st)
    return ok_s, msg_s, pre


def _price_decimals_for_tick(tick: float) -> int:
    if not math.isfinite(tick) or tick <= 0:
        return 5
    return max(2, min(8, int(-math.floor(math.log10(tick)))))


def _cpp_theo_cache_search_paths() -> list[Path]:
    paths: list[Path] = []
    raw = os.environ.get("MM_CPP_THEO_CACHE", "").strip()
    if raw:
        paths.append(Path(raw).expanduser())
    paths.append(REPO_ROOT / "logs" / "mm_external_theo.json")
    paths.append(Path.cwd() / "logs" / "mm_external_theo.json")
    seen: set[str] = set()
    out: list[Path] = []
    for p in paths:
        try:
            key = str(p.resolve())
        except OSError:
            key = str(p)
        if key not in seen:
            seen.add(key)
            out.append(p)
    return out


def _cpp_depth_cache_search_paths() -> list[Path]:
    paths: list[Path] = []
    raw = os.environ.get("MM_CPP_DEPTH_CACHE", "").strip()
    if raw:
        paths.append(Path(raw).expanduser())
    paths.append(REPO_ROOT / "logs" / "mm_external_depth.json")
    paths.append(Path.cwd() / "logs" / "mm_external_depth.json")
    seen: set[str] = set()
    out: list[Path] = []
    for p in paths:
        try:
            key = str(p.resolve())
        except OSError:
            key = str(p)
        if key not in seen:
            seen.add(key)
            out.append(p)
    return out


def _normalize_fix_depth_ladder(raw_bids: object, raw_asks: object) -> tuple[list[list[float]], list[list[float]]]:
    bids: list[list[float]] = []
    asks: list[list[float]] = []
    if isinstance(raw_bids, list):
        for row in raw_bids:
            if isinstance(row, (list, tuple)) and len(row) >= 2:
                p = _to_float(row[0])
                s = _to_float(row[1])
                if (
                    p is not None
                    and s is not None
                    and math.isfinite(p)
                    and math.isfinite(s)
                    and p > 0
                ):
                    bids.append([float(p), float(s)])
    if isinstance(raw_asks, list):
        for row in raw_asks:
            if isinstance(row, (list, tuple)) and len(row) >= 2:
                p = _to_float(row[0])
                s = _to_float(row[1])
                if (
                    p is not None
                    and s is not None
                    and math.isfinite(p)
                    and math.isfinite(s)
                    and p > 0
                ):
                    asks.append([float(p), float(s)])
    return bids, asks


def read_cpp_neon_depth_cache_for_desk() -> dict | None:
    """JSON written by trading_client ExternalFeedManager::maybeWriteDeskDepthCache (Neon FIX ladder)."""
    for p in _cpp_depth_cache_search_paths():
        try:
            if not p.is_file():
                continue
            data = json.loads(p.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError, UnicodeDecodeError):
            continue
        if not isinstance(data, dict):
            continue
        bids, asks = _normalize_fix_depth_ladder(data.get("bids"), data.get("asks"))
        if not bids or not asks:
            continue
        return data
    return None


def read_cpp_theo_cache_for_desk() -> dict | None:
    """JSON written by trading_client ExternalFeedManager::writeDeskTheoCache (Neon/REST theo)."""
    for p in _cpp_theo_cache_search_paths():
        try:
            if not p.is_file():
                continue
            data = json.loads(p.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError, UnicodeDecodeError):
            continue
        if not isinstance(data, dict):
            continue
        qbc = data.get("quotes_by_canonical")
        if isinstance(qbc, dict) and qbc:
            return data
        mid = _to_float(data.get("mid"))
        if mid is not None and math.isfinite(mid) and mid > 0:
            return data
    return None


def _theo_ref_for_ax_symbol(st: "DeskSharedState", ax_sym: str) -> str:
    ax = (ax_sym or "").strip()
    with st.lock:
        rows = list(st.mm_instruments or [])
    for row in rows:
        s = str(row.get("symbol") or row.get("ax_symbol") or "").strip()
        if s and s.upper() == ax.upper():
            r = str(row.get("reference_fix_symbol") or row.get("theo_symbol") or "").strip()
            if r:
                return r
    with st.lock:
        r0 = (st.ref_symbol or "").strip()
    return r0


def _theo_source_for_ref_in_mm(st: "DeskSharedState", ref: str) -> str:
    want = fix_symbol_canonical_py(ref)
    ddef = str(getattr(st, "config_external_provider", "neon_fix") or "neon_fix")
    for row in st.mm_instruments or []:
        r = str(row.get("reference_fix_symbol") or row.get("theo_symbol") or "").strip()
        if fix_symbol_canonical_py(r) == want:
            t = (row.get("theo_source") or "").strip()
            return _normalize_theo_source(t, ddef)
    return _normalize_theo_source("", ddef)


def build_mm_order_preview_for_desk(st: DeskSharedState) -> dict:
    """
    Display-only bid/ask: C++ theo cache (same machine) + MM width/size/basis + position,
    same math as desk requote (no orders sent).
    """
    cache = read_cpp_theo_cache_for_desk()
    theo_source = "cpp_file"
    mid: float | None = None
    ext_bid, ext_ask = None, None
    cache_age_sec: float | None = None
    theo_updated_ms: int | None = None
    if cache:
        mid = _to_float(cache.get("mid"))
        ext_bid = _to_float(cache.get("bid"))
        ext_ask = _to_float(cache.get("ask"))
        ums = cache.get("updated_ms")
        try:
            if ums is not None:
                cache_age_sec = max(0.0, (time.time() * 1000.0 - float(ums)) / 1000.0)
                if mid is not None and math.isfinite(mid) and mid > 0:
                    theo_updated_ms = int(float(ums))
        except (TypeError, ValueError):
            pass
    if mid is None or not math.isfinite(mid) or mid <= 0:
        mid = _reference_theo_mid_st(st)
        theo_source = "desk_reference"
        theo_updated_ms = None
    if mid is None or not math.isfinite(mid) or mid <= 0:
        mid = _ax_mid_primary_st(st)
        theo_source = "ax_book"
        theo_updated_ms = None
    if mid is None or not math.isfinite(mid) or mid <= 0:
        return {
            "ok": False,
            "error": "No theo — run trading_client with Neon, or wait for Architect /book",
            "theo_source": "none",
            "theo_updated_ms": None,
        }

    with st.lock:
        sym = (st.ax_symbol or "").strip()
        qty = int(st.mm_order_size)
        bid_w0 = int(st.mm_bid_width)
        ask_w0 = int(st.mm_ask_width)
        basis = float(st.mm_basis)
        max_po = int(st.mm_max_position)
        adj_po = int(st.mm_adjust_position)
        adj_tk = int(st.mm_adjust_ticks)

    if not sym:
        return {
            "ok": False,
            "error": "no AX symbol in config",
            "theo_source": theo_source,
            "theo_mid": mid,
            "theo_updated_ms": theo_updated_ms if theo_source == "cpp_file" else None,
        }

    tick, ostep = _leg_tick_and_ostep_for_ax_st(st, sym)
    if tick <= 0 or not math.isfinite(tick):
        return {
            "ok": False,
            "error": f"no gateway tick_size for {sym!r} — GET /instruments (feed start or POST /api/desk/sync_instrument_ticks)",
            "theo_source": theo_source,
            "theo_mid": mid,
            "theo_updated_ms": theo_updated_ms if theo_source == "cpp_file" else None,
            "ax_symbol": sym,
        }
    if adj_po <= 0:
        adj_po = 1
    if adj_tk <= 0:
        adj_tk = 1
    if max_po <= 0:
        max_po = 1

    net, net_unknown = _exchange_net_from_position_snapshot(st)
    if net is None:
        net = 0

    # Same as desk_requote: bid_width/ask_width only (no resting_depth_extra_ticks).
    bid_spread_bps = bid_w0
    ask_spread_bps = ask_w0
    adjusted_theo = mid + basis
    raw_bid, raw_ask = compute_skewed_raw_bid_ask(
        adjusted_theo,
        bid_spread_bps,
        ask_spread_bps,
        tick,
        net,
        adj_po,
        adj_tk,
        max_po,
    )
    bid_px, ask_px = finalize_mm_pair_on_tick_grid(
        raw_bid, raw_ask, bid_spread_bps, ask_spread_bps, tick
    )

    leg_gate = _mm_round_qty_to_step_down_int(int(qty), ostep)
    want_bid = should_quote_mm_side(net, max_po, buy_side=True)
    want_ask = should_quote_mm_side(net, max_po, buy_side=False)
    qty_bid, qty_ask = mm_desk_pair_leg_quantities(want_bid, want_ask, int(qty), step=ostep)

    m_bid, m_ask = _best_bid_ask_ax(st, sym)
    ok_v, vreason = validate_mm_quotes_non_cross(bid_px, ask_px, m_bid, m_ask)

    dec = _price_decimals_for_tick(tick)
    return {
        "ok": True,
        "ax_symbol": sym,
        "theo_mid": mid,
        "theo_source": theo_source,
        "external_bid": ext_bid,
        "external_ask": ext_ask,
        "cache_age_sec": cache_age_sec,
        "adjusted_theo": adjusted_theo,
        "basis": basis,
        "net_position": net,
        "net_position_unknown": net_unknown,
        "qty": qty,
        "qty_bid": qty_bid,
        "qty_ask": qty_ask,
        "order_size_step": ostep,
        "mm_max_position": max_po,
        "bid_px": bid_px,
        "ask_px": ask_px,
        "bid_px_str": f"{bid_px:.{dec}f}".rstrip("0").rstrip("."),
        "ask_px_str": f"{ask_px:.{dec}f}".rstrip("0").rstrip("."),
        "want_bid": want_bid,
        "want_ask": want_ask,
        "bid_spread_bps": bid_spread_bps,
        "ask_spread_bps": ask_spread_bps,
        "crosses": not ok_v,
        "cross_reason": vreason if not ok_v else "",
        "ax_market_bid": m_bid,
        "ax_market_ask": m_ask,
        "theo_updated_ms": theo_updated_ms if theo_source == "cpp_file" else None,
    }


def build_mm_order_previews_for_all_legs(st: DeskSharedState) -> list[dict]:
    """Per-leg MM price guide for every configured instrument with a live theo.

    Returns a list of preview dicts (same schema as `build_mm_order_preview_for_desk`)
    — one per `market_maker.instruments[]` row whose theo is currently available
    (HL ``xyz:`` / CME / Neon FIX). Display-only — no orders are sent.
    """
    with st.lock:
        rows = list(st.mm_instruments or [])
        global_qty = int(st.mm_order_size)
        global_ostep = max(1, int(st.mm_order_size_step))
        global_bid_w = int(st.mm_bid_width)
        global_ask_w = int(st.mm_ask_width)
        global_basis = float(st.mm_basis)
        global_max_po = int(st.mm_max_position)
        global_adj_po = int(st.mm_adjust_position)
        global_adj_tk = int(st.mm_adjust_ticks)
    if not rows:
        return []

    out: list[dict] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        ax_sym = str(row.get("symbol") or row.get("ax_symbol") or "").strip()
        if not ax_sym:
            continue
        ref = str(row.get("reference_fix_symbol") or row.get("theo_symbol") or "").strip()
        ts_raw = str(row.get("theo_source") or "").strip()
        theo_source_resolved = _normalize_theo_source(
            ts_raw, str(getattr(st, "config_external_provider", "neon_fix") or "neon_fix")
        )

        # Per-leg width/size/etc: prefer first manual_stack, else top-level desk values.
        bid_w = global_bid_w
        ask_w = global_ask_w
        qty = global_qty
        max_po = global_max_po
        adj_po = global_adj_po
        adj_tk = global_adj_tk
        ostep = global_ostep
        stk_qs = 0.0
        stk_ps = 0.0
        stk_sl = 1.0
        stack_id = ""
        stk_list = row.get("manual_stacks")
        if isinstance(stk_list, list) and stk_list and isinstance(stk_list[0], dict):
            stk0 = stk_list[0]
            try:
                w_val = stk0.get("width_bps")
                if w_val is None:
                    w_val = stk0.get("width")
                if w_val is not None:
                    bid_w = int(w_val)
                    ask_w = int(w_val)
                if stk0.get("order_size") is not None:
                    qty = int(stk0["order_size"])
                if stk0.get("order_size_step") is not None:
                    ostep = max(1, int(stk0["order_size_step"]))
                if stk0.get("max_position") is not None:
                    max_po = int(stk0["max_position"])
                if stk0.get("adjust_position") is not None:
                    adj_po = int(stk0["adjust_position"])
                if stk0.get("adjust_ticks") is not None:
                    adj_tk = int(stk0["adjust_ticks"])
            except (TypeError, ValueError):
                pass
            try:
                if stk0.get("quote_snapshot") is not None:
                    stk_qs = float(stk0["quote_snapshot"])
                if stk0.get("pricer_snapshot") is not None:
                    stk_ps = float(stk0["pricer_snapshot"])
                if stk0.get("slope") is not None:
                    stk_sl = float(stk0["slope"])
            except (TypeError, ValueError):
                pass
            stack_id = str(stk0.get("id") or "")

        gt = _gateway_tick_for_ax(st, ax_sym)
        try:
            tick = float(gt) if gt is not None else 0.0
        except (TypeError, ValueError):
            tick = 0.0
        if tick <= 0.0 or not math.isfinite(tick):
            out.append({
                "ok": False,
                "ax_symbol": ax_sym,
                "reference_fix_symbol": ref,
                "theo_source": theo_source_resolved,
                "theo_mid": None,
                "manual_stack_id": stack_id,
                "error": f"no gateway tick_size for {ax_sym!r} — GET /instruments",
            })
            continue

        # Per-leg theo transform (mirrors MakeMarketStrategy::mmTransformTheo): legacy
        # `theo_scale` multiplier used for AX `SPY-PERP` vs Hyperliquid `xyz:SP500`
        # (theo_scale=0.1). When the desk pricer-snapshot transform is active (qs>0 AND ps>0)
        # the snapshot anchors already carry the scale relationship, so we skip scale here —
        # same short-circuit as MakeMarketStrategy::mmTransformTheo.
        # invert_theo was REMOVED (2026-05-12) — use the pricer-snapshot transform for
        # cross-scale legs.
        snap_active_leg = float(stk_qs) > 0.0 and float(stk_ps) > 0.0
        if snap_active_leg:
            try:
                cfg_scale = float(row.get("theo_scale") or 0.0)
            except (TypeError, ValueError):
                cfg_scale = 0.0
            if cfg_scale > 0.0:
                _mm_xform_warn_if_throttled(ax_sym, cfg_scale,
                                            float(stk_qs), float(stk_ps))
            theo_scale_leg = 0.0
        else:
            try:
                theo_scale_leg = float(row.get("theo_scale") or 0.0)
            except (TypeError, ValueError):
                theo_scale_leg = 0.0

        if adj_po <= 0:
            adj_po = 1
        if adj_tk <= 0:
            adj_tk = 1
        if max_po <= 0:
            max_po = 1

        mid = _theo_mid_for_leg_ax(st, ax_sym)
        if mid is not None and math.isfinite(mid) and mid > 0:
            if theo_scale_leg > 0.0:
                mid = mid * theo_scale_leg
        if mid is None or not math.isfinite(mid) or mid <= 0:
            out.append({
                "ok": False,
                "ax_symbol": ax_sym,
                "reference_fix_symbol": ref,
                "theo_source": theo_source_resolved,
                "theo_mid": None,
                "manual_stack_id": stack_id,
                "error": (
                    f"no theo for {ref or ax_sym} from {theo_source_resolved} "
                    f"(check market_maker.instruments[].theo_venue_symbol mapping)"
                ),
            })
            continue

        net, net_unknown = _net_for_ax_symbol_from_snapshot(st, ax_sym)
        bid_spread_bps = bid_w
        ask_spread_bps = ask_w
        new_mid = apply_pricer_snapshot_transform(mid, stk_qs, stk_ps, stk_sl)
        adjusted_theo = new_mid + global_basis
        raw_bid, raw_ask = compute_skewed_raw_bid_ask(
            adjusted_theo,
            bid_spread_bps,
            ask_spread_bps,
            tick,
            net,
            adj_po,
            adj_tk,
            max_po,
        )
        bid_px, ask_px = finalize_mm_pair_on_tick_grid(
            raw_bid, raw_ask, bid_spread_bps, ask_spread_bps, tick
        )
        want_bid = should_quote_mm_side(net, max_po, buy_side=True)
        want_ask = should_quote_mm_side(net, max_po, buy_side=False)
        qty_bid, qty_ask = mm_desk_pair_leg_quantities(want_bid, want_ask, int(qty), step=ostep)
        m_bid, m_ask = _best_bid_ask_ax(st, ax_sym)
        ok_v, vreason = validate_mm_quotes_non_cross(bid_px, ask_px, m_bid, m_ask)
        dec = _price_decimals_for_tick(tick)
        out.append({
            "ok": True,
            "ax_symbol": ax_sym,
            "reference_fix_symbol": ref,
            "theo_source": theo_source_resolved,
            "manual_stack_id": stack_id,
            "theo_mid": mid,
            "adjusted_theo": adjusted_theo,
            "basis": global_basis,
            "net_position": net,
            "net_position_unknown": net_unknown,
            "qty": qty,
            "qty_bid": qty_bid,
            "qty_ask": qty_ask,
            "order_size_step": ostep,
            "mm_max_position": max_po,
            "bid_px": bid_px,
            "ask_px": ask_px,
            "bid_px_str": f"{bid_px:.{dec}f}".rstrip("0").rstrip("."),
            "ask_px_str": f"{ask_px:.{dec}f}".rstrip("0").rstrip("."),
            "want_bid": want_bid,
            "want_ask": want_ask,
            "bid_spread_bps": bid_spread_bps,
            "ask_spread_bps": ask_spread_bps,
            "crosses": not ok_v,
            "cross_reason": vreason if not ok_v else "",
            "ax_market_bid": m_bid,
            "ax_market_ask": m_ask,
        })
    return out


def _open_orders_fetch(st: DeskSharedState) -> tuple[list[dict], bool, str]:
    """Order gateway GET /open-orders. Returns (rows, ok, error_hint)."""
    try:
        code, body = _orders_gateway_get(st, "/open-orders", None)
    except Exception as e:
        desk_log(st, f"orders /open-orders exception: {type(e).__name__}: {e}")
        return [], False, str(e)[:120]
    if code == 0:
        if isinstance(body, dict) and body.get("_network_error"):
            detail = str(body.get("detail", body))[:180]
            desk_log(st, f"orders /open-orders network: {detail}")
            return [], False, detail
        return [], False, "timeout/network"
    if code < 200 or code >= 300:
        hint = ""
        if isinstance(body, dict):
            hint = str(body.get("error") or body.get("message") or body.get("detail") or "")[:160]
        return [], False, (f"HTTP {code} {hint}".strip())[:200]
    rows: list[dict] = []
    if isinstance(body, list):
        rows = [x for x in body if isinstance(x, dict)]
    elif isinstance(body, dict):
        for k in ("orders", "open_orders", "data", "items"):
            v = body.get(k)
            if isinstance(v, list):
                rows = [x for x in v if isinstance(x, dict)]
                break
    return rows[:ORDERS_LIMIT], True, ""


# =============================================================================
# Per-Instrument Cancel-All + HOLD  (Tim, Basecamp 2026-07-23)
# =============================================================================
# ONE control that (D1) cancels every venue order on an instrument AND (D2) HOLDS
# it — market_maker.instruments[].mm_orders_enabled=false — so the ~72ms re-quote
# cycle cannot immediately re-place. orders.json is NEVER touched (D3): the stacks
# stay defined/visible and re-enable resumes them (this deliberately avoids the
# mass-shrink guard / gen-CAS / July-15 blast radius). The cancel is ONE venue
# /cancel-all-orders for the symbol (D4) — one RTT, no 2N burst, and it covers by
# construction every local-enumeration gap (adoption gaps, zombie re-adoption,
# PLACE_PENDING, CANCEL_SENT_UNCONFIRMED, base make_market_* strategies, and
# hand-placed orders). Strict ordering + mandatory verify loop (D5):
#   1. write hold                                   (persist_market_maker_gate_flags)
#   2. WAIT until the engine has OBSERVED the hold  (mm_orders.json projection ack)
#   3. venue cancelAllOrders(symbol)                (desk_cancel_all_orders)
#   4. venue open-orders GET filtered to symbol     (verify)
#   5. residual? -> back to 3 (bounded retries)     else DONE
#   6. report every step to the UI
# Step-2-before-3 is the whole safety argument: it closes the §4.5 in-flight-place
# orphan race (the JPY self-doubling shape). Steps 4/5 exist because no current
# cancel control ever *proves* it worked.
#
# VENUE GETs: the desk is NOT a mover thread, so the "no venue GETs on the order
# path" directive does NOT apply here. The verify-GET (step 4) is deliberate and
# LOAD-BEARING — do not delete it. (per D5)

_INSTR_HOLD_MAX_CANCEL_ATTEMPTS = 5
# Backoff between sweeps. Observed cancel RTT is well under 1s; 5 sweeps spanning
# ~4.5s comfortably outlasts a single missed venue ack while staying bounded so a
# genuinely non-convergent venue reports NOT CLEAN instead of spinning forever.
_INSTR_HOLD_CANCEL_BACKOFF_SEC = (0.25, 0.5, 0.8, 1.2, 1.8)
_INSTR_HOLD_ACK_TIMEOUT_SEC = 6.0
_INSTR_HOLD_ACK_POLL_SEC = 0.2


def _open_order_row_symbol(row: dict) -> str:
    """Normalized AX symbol from a venue open-order row (field name varies by gateway)."""
    for k in ("symbol", "ax_symbol", "s", "Symbol", "order_symbol"):
        v = row.get(k)
        if v:
            return _norm_orders_ax(str(v))
    return ""


def _open_order_row_oid(row: dict) -> str:
    for k in ("id", "order_id", "oid", "orderId", "exchange_order_id"):
        v = row.get(k)
        if v not in (None, ""):
            return str(v)
    return ""


def _open_orders_for_symbol(st: DeskSharedState, ax_norm: str) -> tuple[list[str], bool, str]:
    """Fresh venue /open-orders filtered to ``ax_norm`` -> (oids, ok, err). No caching."""
    rows, ok, err = _open_orders_fetch(st)
    if not ok:
        return [], False, err
    oids: list[str] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        if _open_order_row_symbol(row) == ax_norm:
            oid = _open_order_row_oid(row)
            oids.append(oid if oid else "?")
    return oids, True, ""


def _configured_ax_symbols_from_default_config() -> tuple[set[str], dict[str, bool]]:
    """(configured ax symbols, {ax_norm: held}) from default_config.json.

    ``held`` is True when the instrument's effective ``mm_orders_enabled`` is False —
    i.e. the global flag is off OR the per-instrument row flag is off. This is the
    desk's *desired* (written) state; the engine's *observed* state is separately
    surfaced from the mm_orders.json projection.
    """
    configured: set[str] = set()
    held: dict[str, bool] = {}
    try:
        dc = json.loads(DEFAULT_CONFIG_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return configured, held
    if not isinstance(dc, dict):
        return configured, held
    mm = dc.get("market_maker")
    if not isinstance(mm, dict):
        return configured, held

    def _b(v, default=True) -> bool:
        if v is None:
            return default
        if isinstance(v, str):
            return v.strip().lower() in ("1", "true", "yes", "on")
        return bool(v)

    global_enabled = _b(mm.get("mm_orders_enabled", True))
    inst = mm.get("instruments")
    if isinstance(inst, list):
        for row in inst:
            if not isinstance(row, dict):
                continue
            ax = _norm_orders_ax(str(row.get("symbol") or row.get("ax_symbol") or ""))
            if not ax:
                continue
            configured.add(ax)
            row_enabled = global_enabled and _b(row.get("mm_orders_enabled", True))
            held[ax] = not row_enabled
    return configured, held


def _engine_effective_gate_by_ax(merged_cfg: dict) -> tuple[dict[str, bool | None], bool]:
    """Read the engine's *observed* effective gate per instrument from the C++-owned
    ``mm_orders.json`` projection.

    Returns ``({ax_norm: effective_enabled_or_None}, projection_stale)``. A value is
    ``True``/``False`` when the projection row carries ``mm_orders_enabled_effective``,
    or ``None`` when there is no row for that instrument (e.g. no live stack). This is
    the engine's *truth* — distinct from the desk's *desired* state in default_config —
    and is what the dropdown label must reflect so it can never claim "quoting stopped"
    while the engine is actually placing (the confusion Tim hit, 26 Jul).
    """
    eff: dict[str, bool | None] = {}
    proj = load_mm_active_orders(merged_cfg)
    stale = bool(proj.get("stale"))
    for o in (proj.get("orders") or []):
        if not isinstance(o, dict):
            continue
        ax = _norm_orders_ax(str(o.get("ax_symbol") or ""))
        if not ax:
            continue
        if "mm_orders_enabled_effective" in o:
            eff[ax] = bool(o.get("mm_orders_enabled_effective"))
        elif ax not in eff:
            eff[ax] = None
    return eff, stale


def instrument_hold_options(st: DeskSharedState) -> dict:
    """D6 dropdown source: union of (a) configured instruments (``mm_instruments``) and
    (b) any instrument with LIVE open orders at the venue. Each entry carries a fresh
    venue open-order count, whether it is configured, and whether it is currently held.
    Entries in (b)-but-not-(a) are marked ``unconfigured`` — a live-but-unconfigured
    instrument is exactly the case this feature exists to clean up, so it must not be
    silently excluded.

    The ``held`` flag (drives the "quoting stopped" label) is sourced from the ENGINE's
    effective gate in the mm_orders.json projection whenever that instrument has a
    projection row that is not stale; otherwise it falls back to the desk's desired state
    from default_config. ``held_source`` records which was used so the UI (and tests) can
    tell engine-observed truth from desk-desired belief.
    """
    configured, held_desired = _configured_ax_symbols_from_default_config()
    merged_cfg = get_merged_config_dict()
    eff_by_ax, proj_stale = _engine_effective_gate_by_ax(merged_cfg)
    rows, ok, err = _open_orders_fetch(st)
    live_counts: dict[str, int] = {}
    if ok:
        for row in rows:
            if isinstance(row, dict):
                sym = _open_order_row_symbol(row)
                if sym:
                    live_counts[sym] = live_counts.get(sym, 0) + 1
    union = sorted(configured | set(live_counts.keys()))
    items: list[dict] = []
    for ax in union:
        eff = eff_by_ax.get(ax)
        if eff is not None and not proj_stale:
            held = (eff is False)
            held_source = "engine"
        else:
            held = bool(held_desired.get(ax, False))
            held_source = "config_stale" if proj_stale else "config"
        items.append({
            "ax_symbol": ax,
            "configured": ax in configured,
            "unconfigured": ax not in configured,
            "held": held,
            "held_source": held_source,
            "effective_enabled": eff,
            "live_open_count": int(live_counts.get(ax, 0)),
        })
    return {
        "ok": True,
        "venue_open_orders_ok": ok,
        "venue_error": None if ok else err,
        "projection_stale": proj_stale,
        "instruments": items,
    }


def _wait_engine_observed_hold(
    merged_cfg: dict,
    ax_norm: str,
    hold_write_mtime_ms: int,
    timeout_sec: float = _INSTR_HOLD_ACK_TIMEOUT_SEC,
) -> tuple[bool, dict]:
    """D5 step 2: block (bounded) until the engine's mm_orders.json projection proves it
    reloaded a config at least as new as our hold write (``config_mtime_ms``) AND, if the
    instrument has a projection row, that row reads ``mm_orders_enabled_effective``==False.
    Returns (observed, last_ack_snapshot).
    """
    deadline = time.time() + max(0.5, float(timeout_sec))
    last: dict = {"config_mtime_ms": 0, "row_effective": None, "stale": True}
    while True:
        proj = load_mm_active_orders(merged_cfg)
        cfg_mtime = int(proj.get("config_mtime_ms") or 0)
        stale = bool(proj.get("stale"))
        row_eff: bool | None = None
        for o in (proj.get("orders") or []):
            if not isinstance(o, dict):
                continue
            if _norm_orders_ax(str(o.get("ax_symbol") or "")) == ax_norm:
                if "mm_orders_enabled_effective" in o:
                    row_eff = bool(o.get("mm_orders_enabled_effective"))
                break
        last = {"config_mtime_ms": cfg_mtime, "row_effective": row_eff, "stale": stale}
        config_fresh = (cfg_mtime >= hold_write_mtime_ms) and not stale
        row_ok = (row_eff is None) or (row_eff is False)
        if config_fresh and row_ok:
            return True, last
        if time.time() >= deadline:
            return False, last
        time.sleep(_INSTR_HOLD_ACK_POLL_SEC)


def desk_instrument_cancel_all(st: DeskSharedState, ax_symbol: str, *, hold: bool = True) -> dict:
    """Per-instrument cancel-all + HOLD. Implements the D5 sequence exactly and returns a
    structured per-step report for the UI. Never reports success on an unverified state.

    hold=True  : cancel-AND-HOLD (write hold -> await observe -> cancelAll -> verify -> retry).
    hold=False : RE-ENABLE (flip mm_orders_enabled back on; no cancel).
    """
    ax_norm = _norm_orders_ax(ax_symbol)
    if not ax_norm:
        return {"ok": False, "error": "ax_symbol required"}
    merged_cfg = get_merged_config_dict()

    # RE-ENABLE path: flip the gate back on, no cancel.
    if not hold:
        ok_g, msg_g = persist_market_maker_gate_flags({"mm_orders_enabled": True}, ax_norm)
        if ok_g:
            desk_reload_config_into_state(st)
        desk_log(st, f"INSTRUMENT_HOLD re_enable ax={ax_norm} ok={ok_g} :: {msg_g}")
        return {
            "ok": bool(ok_g),
            "action": "re_enable",
            "ax_symbol": ax_norm,
            "held": (not ok_g),
            "message": msg_g,
        }

    # D6 validation: symbol MUST be in the union (configured OR live at venue).
    opts = instrument_hold_options(st)
    known = {i["ax_symbol"] for i in opts.get("instruments", [])}
    if ax_norm not in known:
        return {
            "ok": False,
            "error": f"{ax_norm!r} is not a configured or live instrument",
            "known": sorted(known),
        }
    pre_live = next(
        (i["live_open_count"] for i in opts["instruments"] if i["ax_symbol"] == ax_norm),
        0,
    )
    steps: list[dict] = []

    # STEP 1 — write hold.
    ok_g, msg_g = persist_market_maker_gate_flags({"mm_orders_enabled": False}, ax_norm)
    steps.append({"step": "write_hold", "ok": bool(ok_g), "detail": msg_g})
    if not ok_g:
        desk_log(st, f"INSTRUMENT_CANCEL_ALL ax={ax_norm} ABORT write_hold_failed :: {msg_g}")
        return {
            "ok": False, "clean": False, "ax_symbol": ax_norm, "held": False,
            "steps": steps, "error": f"hold write failed: {msg_g}",
        }
    desk_reload_config_into_state(st)
    hw_mtime, _ = _primary_config_stat()
    hold_write_mtime_ms = int(hw_mtime or 0)

    # STEP 2 — WAIT until the engine has demonstrably OBSERVED the hold. This is the whole
    # safety argument: no cancel is issued until placement is provably suppressed, closing
    # the in-flight-place orphan race (§4.5, the JPY doubling shape).
    observed, ack = _wait_engine_observed_hold(merged_cfg, ax_norm, hold_write_mtime_ms)
    steps.append({"step": "await_hold_observed", "ok": bool(observed), "detail": ack})
    # If not observed within the window we still proceed — the hold IS written and the
    # verify loop is the ultimate proof — but the report flags it so the UI can warn.

    # STEPS 3-5 — cancel + verify + bounded retry.
    residual: list[str] = []
    clean = False
    attempts = 0
    for attempt in range(_INSTR_HOLD_MAX_CANCEL_ATTEMPTS):
        attempts = attempt + 1
        can = desk_cancel_all_orders(st, symbol=ax_norm)
        oids, gok, gerr = _open_orders_for_symbol(st, ax_norm)
        residual = oids
        steps.append({
            "step": "cancel_and_verify",
            "attempt": attempts,
            "cancel_ok": bool(can.get("ok")),
            "cancel_http": can.get("http"),
            "verify_ok": bool(gok),
            "residual_open": len(oids),
            "residual_oids": oids[:20],
            "verify_error": (None if gok else gerr),
        })
        if gok and not oids:
            clean = True
            break
        idx = min(attempt, len(_INSTR_HOLD_CANCEL_BACKOFF_SEC) - 1)
        time.sleep(_INSTR_HOLD_CANCEL_BACKOFF_SEC[idx])

    result = {
        "ok": clean,
        "clean": clean,
        "ax_symbol": ax_norm,
        "held": True,
        "observed_hold": observed,
        "pre_live_open": pre_live,
        "attempts": attempts,
        "residual_open": len(residual),
        "residual_oids": residual[:50],
        "steps": steps,
    }
    if not clean:
        result["error"] = (
            f"NOT CLEAN: {len(residual)} order(s) still open on {ax_norm} after "
            f"{attempts} sweep(s); instrument left HELD"
        )
    desk_log(
        st,
        f"INSTRUMENT_CANCEL_ALL ax={ax_norm} pre_live_open={pre_live} observed={observed} "
        f"attempts={attempts} clean={clean} residual={len(residual)} held=True",
    )
    return result


def _ax_effective_enabled_in_config(ax_symbol: str) -> bool:
    """Effective ``mm_orders_enabled`` for ``ax_symbol`` as written in default_config.json
    (the desk's *desired* state — the exact flag a resume flips). Returns True (fail-open)
    on any read/parse problem so a transient hiccup never blocks a legitimate place."""
    ax_norm = _norm_orders_ax(ax_symbol)
    if not ax_norm:
        return True
    try:
        dc = json.loads(DEFAULT_CONFIG_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return True
    if not isinstance(dc, dict):
        return True
    mm = dc.get("market_maker")
    if not isinstance(mm, dict):
        return True

    def _b(v, default=True) -> bool:
        if v is None:
            return default
        if isinstance(v, str):
            return v.strip().lower() in ("1", "true", "yes", "on")
        return bool(v)

    g = _b(mm.get("mm_orders_enabled", True))
    inst = mm.get("instruments")
    if isinstance(inst, list):
        for row in inst:
            if not isinstance(row, dict):
                continue
            if _norm_orders_ax(str(row.get("symbol") or row.get("ax_symbol") or "")) == ax_norm:
                return g and _b(row.get("mm_orders_enabled", True))
    # Not configured: only the global gate applies.
    return g


def _ax_stopped_in_config(ax_symbol: str) -> bool:
    """True iff the instrument is currently stopped (effective mm_orders_enabled == false)
    per default_config.json. This is the trigger for the Item 3/4 resume-confirm."""
    return not _ax_effective_enabled_in_config(ax_symbol)


def desk_instrument_resume(st: DeskSharedState, ax_symbol: str) -> dict:
    """RESUME (Item 3) — FLAG-ONLY. Sets ``mm_orders_enabled=true`` for the instrument via
    the hardened ``_default_config_mutate_write`` (through ``persist_market_maker_gate_flags``)
    and nothing else.

    It MUST NOT place, re-place, or touch a single order, and MUST NOT touch orders.json —
    that is the whole reason it is safe where the old re-enable button was not (the old button
    flipped the flag as a standalone action and the engine re-quoted stale orders.json stacks,
    doubling against any freshly-placed pair). Here the flag flip is bundled with an explicit,
    user-confirmed add/place: the only new orders are the ones the operator actually asked for.
    The engine resumes quoting the already-defined stacks on its next mtime-gated config reload.
    """
    ax_norm = _norm_orders_ax(ax_symbol)
    if not ax_norm:
        return {"ok": False, "error": "ax_symbol required", "placed": 0}
    ok_g, msg_g = persist_market_maker_gate_flags({"mm_orders_enabled": True}, ax_norm)
    if ok_g:
        desk_reload_config_into_state(st)
    desk_log(st, f"INSTRUMENT_RESUME ax={ax_norm} flag_only=true placed=0 ok={ok_g} :: {msg_g}")
    return {
        "ok": bool(ok_g),
        "action": "resume",
        "ax_symbol": ax_norm,
        "placed": 0,
        "message": msg_g,
    }


def _place_resume_decision(st: DeskSharedState, ax_symbol: str, body: dict) -> dict:
    """Item 3/4 server-side gate for every desk->venue place path. Decides whether a place
    into ``ax_symbol`` may proceed when the instrument is stopped. The server is authoritative
    so a direct API call can NEVER silently place into a stopped instrument, and can NEVER
    silently resume without the operator's explicit ``resume:true`` (the UI confirm).

    Returns a dict with ``action`` in:
      - "proceed"       : not stopped -> place normally.
      - "resumed"       : was stopped AND body carried resume:true -> flag flipped (flag-only),
                          place may proceed. Carries ``resume`` (the flag-only result, placed=0).
      - "needs_confirm" : stopped AND no resume:true -> caller must 409 and place NOTHING.
      - "error"         : resume was requested but the flag write failed -> caller must 400.
    """
    ax_norm = _norm_orders_ax(ax_symbol)
    if not _ax_stopped_in_config(ax_norm):
        return {"action": "proceed"}
    resume_req = bool(body.get("resume") or body.get("resume_confirmed"))
    if not resume_req:
        return {
            "action": "needs_confirm",
            "ax_symbol": ax_norm,
            "error": f"{ax_norm} is stopped — resume quoting?",
        }
    r = desk_instrument_resume(st, ax_norm)
    if not r.get("ok"):
        return {"action": "error", "error": f"resume failed: {r.get('message') or r.get('error')}"}
    return {"action": "resumed", "resume": r}


def _orders_history_fetch(st: DeskSharedState) -> tuple[list[dict], bool, str]:
    """
    GET …/orders/orders on the order-gateway (AX OpenAPI: historical orders, incl. REJECTED).
    Requires a pagination object — not GET {api}/orders (wrong host / missing pagination).
    """
    with st.lock:
        sym = st.ax_symbol.strip()
    lim = int(ORDERS_LIMIT)
    last_err = "no order-gateway /orders response"
    base_qp: dict[str, object] = {"symbol": sym} if sym else {}
    # OpenAPI: query param `pagination` = LimitOffsetPagination; servers encode this differently.
    try_q: list[dict[str, object]] = [
        {**base_qp, "limit": lim, "offset": 0},
        {**base_qp, "pagination.limit": lim, "pagination.offset": 0},
        {**base_qp, "pagination[limit]": lim, "pagination[offset]": 0},
        {**base_qp, "pagination": json.dumps({"limit": lim, "offset": 0})},
    ]
    for qp in try_q:
        try:
            code, body = _orders_gateway_get(st, "/orders", qp)
        except Exception as e:
            last_err = f"{type(e).__name__}: {e}"[:200]
            continue
        if code == 0:
            if isinstance(body, dict) and body.get("_network_error"):
                return [], False, str(body.get("detail", "network"))[:200]
            last_err = "timeout/network"
            continue
        if code < 200 or code >= 300:
            last_err = f"HTTP {code}"
            if isinstance(body, dict):
                last_err += " " + str(body.get("error") or body.get("message") or body.get("detail") or "")[:160]
            continue
        rows: list[dict] = []
        if isinstance(body, list):
            rows = [x for x in body if isinstance(x, dict)]
        elif isinstance(body, dict):
            for k in ("orders", "data", "items", "results"):
                v = body.get(k)
                if isinstance(v, list):
                    rows = [x for x in v if isinstance(x, dict)]
                    break
        rows.sort(key=_fill_sort_ts, reverse=True)
        desk_log(
            st,
            f"order history OK ({len(rows)} rows) via /orders query keys: {list(qp.keys())[:6]}",
            verbose_only=True,
        )
        return rows[:ORDERS_LIMIT], True, ""
    # Legacy api-gateway path (older deployments)
    for qp in ({**base_qp, "limit": lim}, {"limit": lim}, {}):
        code, body = _ax_api_get(st, "/orders", qp)
        if code < 200 or code >= 300:
            continue
        rows = []
        if isinstance(body, list):
            rows = [x for x in body if isinstance(x, dict)]
        elif isinstance(body, dict):
            for k in ("orders", "data", "items", "results"):
                v = body.get(k)
                if isinstance(v, list):
                    rows = [x for x in v if isinstance(x, dict)]
                    break
        if rows:
            rows.sort(key=_fill_sort_ts, reverse=True)
            desk_log(st, f"order history OK via api /orders ({len(rows)} rows)", verbose_only=True)
            return rows[:ORDERS_LIMIT], True, ""
    return [], False, last_err[:200]


def _list_orders_st(st: DeskSharedState) -> list[dict]:
    rows, ok, _ = _open_orders_fetch(st)
    return rows if ok else []


# --- Paired-order cancellation (server layer) -------------------------------
# Statuses (lower-cased) treated as a still-live order for is_live checks.
_MM_PAIRED_CANCEL_LIVE_STATUSES = {
    "open", "new", "accepted", "working", "pending",
    "partially_filled", "partial", "ack", "acked", "",
}
# Statuses (lower-cased, non-reject) that resolve an order so its pair entry is dropped.
_MM_PAIRED_CANCEL_TERMINAL_STATUSES = {
    "canceled", "cancelled", "filled", "expired", "done", "closed", "out",
}
# Reject statuses are routed through the registry's reject handler.
_MM_PAIRED_CANCEL_REJECT_STATUSES = {"rejected", "reject"}

# De-dupe: a rejected order stays in /orders history for many polls; only act once.
_MM_PAIRED_CANCEL_SEEN_REJECTS: set[str] = set()
_MM_PAIRED_CANCEL_SEEN_LOCK = threading.Lock()
_MM_PAIRED_CANCEL_SEEN_MAX = 4000


def _mm_order_field(o: dict, keys: tuple[str, ...]) -> str:
    for k in keys:
        v = o.get(k)
        if v is None:
            continue
        if isinstance(v, (dict, list)):
            continue
        s = str(v).strip()
        if s:
            return s
    return ""


def _mm_order_oid(o: dict) -> str:
    return _mm_order_field(o, ("oid", "id", "order_id", "exchange_order_id", "eoid"))


def _mm_order_status(o: dict) -> str:
    return _mm_order_field(o, ("status", "state", "o")).lower()


def _mm_order_reject_reason(o: dict) -> str:
    # Mirror the client's orderRejectReason() field precedence.
    for k in ("txt", "reject_reason", "rejectReason", "reason", "error_message", "error", "message"):
        v = o.get(k)
        if v is None:
            continue
        if isinstance(v, dict):
            inner = v.get("error") or v.get("message") or v.get("detail")
            if inner:
                return str(inner).strip()
            continue
        s = str(v).strip()
        if s:
            return s
    return ""


def _mm_paired_cancel_seen_reject(oid: str) -> bool:
    """Return True if this rejected oid was already processed (so skip it)."""
    with _MM_PAIRED_CANCEL_SEEN_LOCK:
        if oid in _MM_PAIRED_CANCEL_SEEN_REJECTS:
            return True
        if len(_MM_PAIRED_CANCEL_SEEN_REJECTS) >= _MM_PAIRED_CANCEL_SEEN_MAX:
            _MM_PAIRED_CANCEL_SEEN_REJECTS.clear()
        _MM_PAIRED_CANCEL_SEEN_REJECTS.add(oid)
        return False


def _mm_scan_orders_for_paired_cancel(st: DeskSharedState, orders: list[dict]) -> None:
    """Server-layer reject observer: scan an order snapshot and drive paired cancels.

    For each REJECTED order with a margin-related reason, cancel the paired leg
    (if still live) via the existing cancel endpoint. For any other terminal
    order (filled/cancelled), drop its pair entry so the map stays bounded.

    Best-effort and isolated — any failure here must never disrupt the feed loop.
    """
    if not orders:
        return

    live_oids: set[str] = set()
    for o in orders:
        if not isinstance(o, dict):
            continue
        oid = _mm_order_oid(o)
        if not oid:
            continue
        if _mm_order_status(o) in _MM_PAIRED_CANCEL_LIVE_STATUSES:
            live_oids.add(oid)

    def _is_live(target_oid: str) -> bool:
        return target_oid in live_oids

    def _log(msg: str) -> None:
        desk_log(st, msg)

    for o in orders:
        if not isinstance(o, dict):
            continue
        oid = _mm_order_oid(o)
        if not oid:
            continue
        status = _mm_order_status(o)
        if status in _MM_PAIRED_CANCEL_REJECT_STATUSES:
            if _mm_paired_cancel_seen_reject(oid):
                continue
            reason = _mm_order_reject_reason(o)
            _MM_PAIR_REGISTRY.handle_reject(
                oid,
                reason,
                cancel_fn=lambda target: desk_cancel_one_order(st, target),
                log_fn=_log,
                is_live_fn=_is_live,
            )
        elif status in _MM_PAIRED_CANCEL_TERMINAL_STATUSES:
            _MM_PAIR_REGISTRY.note_resolution(oid, log_fn=_log)


def _fill_sort_ts(f: dict) -> float:
    for k in ("timestamp", "time", "ts", "created_at", "t", "fill_time", "transaction_time"):
        v = f.get(k)
        if v is None:
            continue
        if isinstance(v, (int, float)):
            x = float(v)
            while x > 1e13:
                x /= 1000.0
            return x
        if isinstance(v, str) and v.strip():
            s = v.strip().replace("Z", "+00:00")
            try:
                return datetime.fromisoformat(s).timestamp()
            except ValueError:
                try:
                    return float(s)
                except ValueError:
                    continue
    return 0.0


def _fills_fetch(st: DeskSharedState) -> tuple[list[dict], bool, str]:
    """GET /fills (api-gateway). Newest first. Returns (rows, ok, err)."""
    with st.lock:
        sym = st.ax_symbol.strip()
    last_err = "no fills response"
    try_q = (
        {"symbol": sym, "limit": FILLS_LIMIT},
        {"limit": FILLS_LIMIT},
        {},
    )
    for qp in try_q:
        code, body = _ax_api_get(st, "/fills", qp)
        if code == 0:
            if isinstance(body, dict) and body.get("_network_error"):
                return [], False, str(body.get("detail", "network"))[:200]
            last_err = "timeout/network"
            continue
        if code < 200 or code >= 300:
            last_err = f"HTTP {code}"
            continue
        raw: list | None = None
        if isinstance(body, dict) and isinstance(body.get("fills"), list):
            raw = body["fills"]
        elif isinstance(body, list):
            raw = body
        elif isinstance(body, dict):
            for k in ("fills", "data", "items", "results"):
                v = body.get(k)
                if isinstance(v, list):
                    raw = v
                    break
        if raw is None:
            continue
        rows = [x for x in raw if isinstance(x, dict)]
        rows.sort(key=_fill_sort_ts, reverse=True)
        return rows[:FILLS_LIMIT], True, ""
    return [], False, last_err


def _list_fills_st(st: DeskSharedState) -> list[dict]:
    rows, ok, _ = _fills_fetch(st)
    return rows if ok else []


def _positions_array_st(st: DeskSharedState) -> tuple[list[dict] | None, int, str]:
    """GET /positions (api-gateway). Returns (rows_or_none, http_code, err_hint)."""
    code, body = _ax_api_get(st, "/positions", {})
    if code == 0:
        d = str((body or {}).get("detail", "timeout/network"))[:100] if isinstance(body, dict) else "network"
        return None, code, d
    if code < 200 or code >= 300:
        return None, code, f"HTTP {code}"
    arr = body if isinstance(body, list) else (body.get("positions") if isinstance(body, dict) else []) or []
    if not isinstance(arr, list):
        return None, code, "parse error"
    rows = [x for x in arr if isinstance(x, dict)]
    return rows, code, ""


def _norm_ax_position_row(p: dict) -> dict | None:
    sym = str(p.get("symbol", p.get("s", ""))).strip()
    if not sym:
        return None
    sq = _signed_qty_ax_row(p)
    if sq is None:
        return None
    qv = float(sq)
    avgp = _to_float(p.get("entry_price") or p.get("average_price") or p.get("avg_price"))
    if (avgp is None or abs(float(avgp)) < 1e-15) and abs(qv) > 1e-12:
        sn = _to_float(p.get("signed_notional") or p.get("signedNotional"))
        if sn is not None and abs(float(sn)) > 1e-12:
            avgp = abs(float(sn)) / abs(qv)
    upnl = _to_float(p.get("unrealized_pnl") or p.get("unrealizedPnl"))
    return {
        "symbol": sym,
        "qty": float(qv),
        "entry_price": avgp,
        "unrealized_pnl": upnl,
    }


def _position_line_from_rows(rows: list[dict] | None, code: int, primary_sym: str, err: str) -> str:
    if code == 0 or rows is None:
        return f"Position: unreachable ({err or 'network'})"
    if code < 200 or code >= 300:
        return f"Position: ({err})"
    sym = (primary_sym or "").strip()
    sym_n = _normalize_ax_symbol(sym)
    for p in rows:
        ps = str(p.get("symbol", p.get("s", ""))).strip()
        if _normalize_ax_symbol(ps) != sym_n:
            continue
        sq = _signed_qty_ax_row(p)
        qv = float(sq) if sq is not None else None
        if qv is None:
            continue
        avgp = _to_float(p.get("entry_price") or p.get("average_price") or p.get("avg_price"))
        upnl = _to_float(p.get("unrealized_pnl") or p.get("unrealizedPnl"))
        side = "LONG" if (qv or 0) >= 0 else "SHORT"
        qa = abs(qv or 0)
        return f"Position: {sym}  {side}  qty={qa:g}  entry≈{avgp or 0:.5f}  uPnL={upnl or 0:.2f}"
    return f"Position: {sym}  FLAT"


def _position_snapshot_from_rows(
    rows: list[dict] | None, code: int, primary_sym: str, err: str
) -> dict:
    sym0 = (primary_sym or "").strip()
    snap: dict = {
        "ok": False,
        "error": None,
        "primary_symbol": sym0,
        "primary": None,
        "all_positions": [],
        "non_flat_count": 0,
        "total_abs_qty": 0.0,
        "total_unrealized_pnl": None,
    }
    if code == 0 or rows is None:
        snap["error"] = err or "network"
        return snap
    if code < 200 or code >= 300:
        snap["error"] = err or f"HTTP {code}"
        return snap
    snap["ok"] = True
    normed: list[dict] = []
    for p in rows:
        r = _norm_ax_position_row(p)
        if r and abs(r["qty"]) > 1e-12:
            normed.append(r)
    snap["all_positions"] = normed
    snap["non_flat_count"] = len(normed)
    snap["total_abs_qty"] = float(sum(abs(r["qty"]) for r in normed))
    upnls = [r["unrealized_pnl"] for r in normed if r.get("unrealized_pnl") is not None]
    if upnls:
        snap["total_unrealized_pnl"] = float(sum(float(x) for x in upnls))
    sym0_n = _normalize_ax_symbol(sym0)
    prim = next((r for r in normed if _normalize_ax_symbol(r["symbol"]) == sym0_n), None)
    if prim is None:
        snap["primary"] = {
            "flat": True,
            "qty": 0.0,
            "qty_abs": 0.0,
            "side": "",
            "entry_price": None,
            "unrealized_pnl": 0.0,
        }
    else:
        q = float(prim["qty"])
        snap["primary"] = {
            "flat": False,
            "qty": q,
            "qty_abs": abs(q),
            "side": "LONG" if q >= 0 else "SHORT",
            "entry_price": prim.get("entry_price"),
            "unrealized_pnl": prim.get("unrealized_pnl"),
        }
    return snap


def _position_row_st(st: DeskSharedState) -> str:
    with st.lock:
        sym = st.ax_symbol.strip()
    rows, code, err = _positions_array_st(st)
    return _position_line_from_rows(rows, code, sym, err)


def _exchange_net_from_position_snapshot(st: DeskSharedState) -> tuple[int | None, bool]:
    """
    Primary-symbol net qty from the last account poll (no extra HTTP).
    Returns (net, unknown): unknown True when not authenticated or snapshot missing.
    """
    with st.lock:
        snap = st.position_snapshot
    if not isinstance(snap, dict) or not snap.get("ok"):
        return None, True
    prim = snap.get("primary")
    if not isinstance(prim, dict):
        return None, True
    if prim.get("flat"):
        return 0, False
    try:
        q = float(prim["qty"])
    except (TypeError, ValueError, KeyError):
        return None, True
    return int(round(q)), False


def _net_for_ax_symbol_from_snapshot(st: DeskSharedState, ax_sym: str) -> tuple[int, bool]:
    """Net signed qty for an AX symbol from the cached position snapshot (no extra HTTP).

    Returns ``(net, unknown)``. ``unknown`` is True when the snapshot itself is
    not authenticated/loaded yet; the per-symbol row missing simply means flat
    (net=0, unknown=False) once the snapshot has loaded.
    """
    ax = (ax_sym or "").strip()
    if not ax:
        return 0, True
    with st.lock:
        snap = st.position_snapshot
    if not isinstance(snap, dict) or not snap.get("ok"):
        return 0, True
    rows = snap.get("all_positions") or []
    target = _normalize_ax_symbol(ax)
    for r in rows:
        if not isinstance(r, dict):
            continue
        if _normalize_ax_symbol(str(r.get("symbol") or "")) == target:
            try:
                return int(round(float(r["qty"]))), False
            except (TypeError, ValueError, KeyError):
                return 0, True
    return 0, False


def _exchange_net_st_for_symbol(st: DeskSharedState, sym: str) -> int | None:
    """
    Net signed qty for one Architect symbol (GET /positions). Other legs are ignored.
    """
    sym = (sym or "").strip()
    if not sym:
        return None
    code, body = _ax_api_get(st, "/positions", {})
    if code < 200 or code >= 300:
        return None
    arr = body if isinstance(body, list) else (body.get("positions") if isinstance(body, dict) else []) or []
    if not isinstance(arr, list):
        return None
    sym_n = _normalize_ax_symbol(sym)
    for p in arr:
        if not isinstance(p, dict):
            continue
        ps = str(p.get("symbol", p.get("s", "")))
        if _normalize_ax_symbol(ps) != sym_n:
            continue
        sq = _signed_qty_ax_row(p)
        if sq is None:
            continue
        return int(round(float(sq)))
    return 0


def _exchange_net_st(st: DeskSharedState) -> int | None:
    with st.lock:
        sym = st.ax_symbol.strip()
    return _exchange_net_st_for_symbol(st, sym)


def _tape_lines_st(st: DeskSharedState) -> list[str]:
    with st.lock:
        sym = st.ax_symbol.strip()
        base = st.rest_endpoint.strip().rstrip("/")
    q = urllib.parse.urlencode({"symbol": sym, "limit": 20})
    url = f"{base}/trades?{q}"
    lines: list[str] = []
    try:
        req = urllib.request.Request(url, method="GET")
        with urllib.request.urlopen(req, context=_ssl_ctx(), timeout=6.0) as resp:
            raw = json.loads(resp.read().decode("utf-8"))
        rows = raw if isinstance(raw, list) else raw.get("trades", []) if isinstance(raw, dict) else []
        if isinstance(rows, list):
            for tr in reversed(rows[-20:]):
                if not isinstance(tr, dict):
                    continue
                px = _to_float(tr.get("price") or tr.get("p"))
                sz = _to_float(tr.get("quantity") or tr.get("q"))
                sd = str(tr.get("side", tr.get("d", ""))).upper()
                if px is None or sz is None:
                    continue
                lines.append(f"{sd:4} {px:.5f} x {sz:g}")
    except Exception:
        pass
    return lines


def _sleep_polling(stop: threading.Event) -> None:
    t_rem = REST_POLL_INTERVAL_SEC
    while t_rem > 0 and not stop.is_set():
        chunk = min(0.5, t_rem)
        time.sleep(chunk)
        t_rem -= chunk


def _ax_ticker_l1_book(st: DeskSharedState, symbol: str) -> tuple[list[tuple[float, float]], list[tuple[float, float]]]:
    """
    When GET /book returns an empty snapshot (sandbox MdPub can be empty while quotes still exist),
    build a 1-level book from GET /ticker best bid/ask (bp/ap). Same Bearer as /book.
    """
    code, body = _ax_api_get(
        st,
        "/ticker",
        {"symbol": symbol},
        timeout=min(AX_BOOK_HTTP_TIMEOUT, 12.0),
    )
    if code != 200 or not isinstance(body, dict):
        return [], []
    tick = body.get("ticker")
    if not isinstance(tick, dict):
        tick = body
    bp = _to_float(tick.get("bp"))
    ap = _to_float(tick.get("ap"))
    if bp is None or ap is None or ap < bp:
        return [], []
    qv = _to_float(tick.get("q"))
    sz = qv if qv is not None and qv > 0 else 1.0
    return [(bp, sz)], [(ap, sz)]


def fetch_ax_markets_list(base: str) -> tuple[list[str], str | None]:
    """Public GET /instruments (AX api-gateway). Legacy /markets is removed (404)."""
    url = f"{base.rstrip('/')}/instruments"
    try:
        req = urllib.request.Request(url, method="GET")
        with urllib.request.urlopen(req, context=_ssl_ctx(), timeout=12.0) as resp:
            raw = json.loads(resp.read().decode("utf-8"))
        rows = raw.get("instruments") if isinstance(raw, dict) else []
        if not isinstance(rows, list):
            return [], "instruments response not a list"
        syms: list[str] = []
        for item in rows:
            if not isinstance(item, dict):
                continue
            if item.get("is_tradeable") is False:
                continue
            sym = str(item.get("symbol") or item.get("s") or "").strip()
            if sym:
                syms.append(sym)
        out = sorted(set(syms))
        return out, None
    except Exception as e:
        return [], f"{type(e).__name__}: {e}"


def _mm_tick_sizes_equal(a: float, b: float) -> bool:
    if not (math.isfinite(a) and math.isfinite(b)):
        return False
    scale = max(1.0, abs(a), abs(b))
    return abs(a - b) <= 1e-9 * scale


def fetch_ax_instrument_tick_map(base: str) -> tuple[dict[str, float], str | None]:
    """
    Public GET ``{base}/instruments`` → tick map for tradeable rows with positive ``tick_size``.

    Keys include both ``SYMBOL.UPPER()`` and ``_normalize_ax_symbol(symbol)`` so desk lookups
    match gateway symbols regardless of hyphen/spacing conventions.
    """
    url = f"{base.rstrip('/')}/instruments"
    try:
        req = urllib.request.Request(url, method="GET")
        with urllib.request.urlopen(req, context=_ssl_ctx(), timeout=12.0) as resp:
            raw = json.loads(resp.read().decode("utf-8"))
        rows = raw.get("instruments") if isinstance(raw, dict) else []
        if not isinstance(rows, list):
            return {}, "instruments response not a list"
        out: dict[str, float] = {}
        for item in rows:
            if not isinstance(item, dict):
                continue
            if item.get("is_tradeable") is False:
                continue
            sym = str(item.get("symbol") or item.get("s") or "").strip()
            if not sym:
                continue
            try:
                ts = float(item.get("tick_size") or item.get("tick") or item.get("price_tick") or 0.0)
            except (TypeError, ValueError):
                ts = 0.0
            if ts <= 0.0:
                continue
            out[sym.upper()] = ts
            nk = _normalize_ax_symbol(sym)
            if nk:
                out[nk] = ts
        return out, None
    except Exception as e:
        return {}, f"{type(e).__name__}: {e}"


def apply_gateway_tick_map_to_default_config(tick_map: dict[str, float]) -> tuple[bool, str, int]:
    """
    Persist gateway ticks into ``default_config.json`` for C++ / reload (``tick_size`` per leg;
    legacy single-symbol: ``price_tick`` from gateway when symbol matches).
    """
    if not tick_map:
        return False, "empty tick map", 0
    if not DEFAULT_CONFIG_PATH.is_file():
        return False, f"missing {DEFAULT_CONFIG_PATH.name}", 0
    try:
        dc = json.loads(DEFAULT_CONFIG_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as e:
        return False, str(e), 0
    if not isinstance(dc, dict):
        return False, "default_config root must be object", 0
    mm = dc.get("market_maker")
    if not isinstance(mm, dict):
        return True, "no market_maker object — nothing to update", 0
    inst = mm.get("instruments")
    updated = 0
    details: list[str] = []
    if isinstance(inst, list) and inst:
        for row in inst:
            if not isinstance(row, dict):
                continue
            sym = str(row.get("symbol") or row.get("ax_symbol") or "").strip()
            if not sym:
                continue
            new_t = _tick_from_gateway_map(tick_map, sym)
            if new_t is None:
                continue
            try:
                old_t = float(row.get("tick_size") or row.get("tick") or row.get("price_tick") or 0.0)
            except (TypeError, ValueError):
                old_t = 0.0
            if _mm_tick_sizes_equal(old_t, new_t):
                continue
            row["tick_size"] = new_t
            updated += 1
            if len(details) < 48:
                details.append(f"{sym}→{new_t:g}")
    if updated == 0 and (not isinstance(inst, list) or not inst):
        sym1 = str(mm.get("symbol") or mm.get("order_symbol") or "").strip()
        new_t = _tick_from_gateway_map(tick_map, sym1) if sym1 else None
        if new_t is not None:
            try:
                old_g = float(mm.get("price_tick") or 0.0)
            except (TypeError, ValueError):
                old_g = 0.0
            if not _mm_tick_sizes_equal(old_g, new_t):
                mm["price_tick"] = new_t
                updated = 1
                details.append(f"price_tick({sym1})→{new_t:g}")
    if updated == 0:
        return True, "0 tick fields changed (already in sync or no matching symbol on gateway)", 0
    try:
        _atomic_write_json(DEFAULT_CONFIG_PATH, dc)
    except OSError as e:
        return False, str(e), 0
    msg = f"updated {updated} tick field(s): " + ", ".join(details[:24])
    if len(details) > 24:
        msg += f" (+{len(details) - 24} more)"
    return True, msg, updated


def desk_refresh_gateway_instrument_ticks(
    st: DeskSharedState, rest_base: str | None = None
) -> tuple[bool, str, int]:
    """
    GET /instruments → ``st.ax_tick_by_symbol``, persist to ``default_config.json``, reload desk,
    merge ticks into ``st.mm_instruments``. **Authoritative** for MM quote ticks (no config fallbacks).

    Returns ``(ok, message, default_config_tick_fields_updated)``. The third value is the number of
    ``tick_size`` / ``price_tick`` fields written by :func:`apply_gateway_tick_map_to_default_config`
    (``0`` when already in sync or on failure before persistence).
    """
    try:
        base = (rest_base or "").strip().rstrip("/")
        if not base:
            with st.lock:
                base = str(st.rest_endpoint or "").strip().rstrip("/")
        if not base:
            return False, "empty rest_base", 0
        tick_map, err = fetch_ax_instrument_tick_map(base)
        if err:
            with st.lock:
                st.ax_tick_map_error = err
            return False, err, 0
        if not tick_map:
            with st.lock:
                st.ax_tick_map_error = "no positive tick_size in /instruments"
            return False, st.ax_tick_map_error, 0
        with st.lock:
            st.ax_tick_by_symbol = dict(tick_map)
            st.ax_tick_map_error = ""
            st.ax_tick_map_updated_ms = int(time.time() * 1000)
        ok_f, msg_f, n = apply_gateway_tick_map_to_default_config(tick_map)
        desk_reload_config_into_state(st)
        _merge_gateway_ticks_into_st_mm_instruments(st, tick_map)
        if ok_f:
            if n > 0:
                desk_log(st, f"[MM_DESK] gateway /instruments → {msg_f}")
            return (
                True,
                msg_f if n > 0 else f"gateway OK ({len(tick_map)} symbols); config already in sync",
                int(n),
            )
        desk_log(st, f"[MM_DESK] gateway ticks OK but default_config write failed: {msg_f}")
        return False, msg_f, 0
    finally:
        with st.lock:
            st._ax_tick_map_refresh_mono = time.monotonic()


def fetch_ax_book_st(st: DeskSharedState, symbol: str, max_levels: int) -> tuple[list, list]:
    """
    GET /book (AX api-gateway). Tries default (no level), then level=2 and 3 if still empty.
    If the book snapshot has no rows (common when MdPub is quiet on sandbox), falls back to
    GET /ticker bp/ap as a 1-deep synthetic book so the desk is not blank while orders/ticker work.
    """
    cap = max(1, int(max_levels))
    last_http = 0
    last_detail = ""
    for level in (None, 2, 3):
        params: dict[str, object] = {"symbol": symbol}
        if level is not None:
            params["level"] = level
        code, body = _ax_api_get(
            st,
            "/book",
            params,
            timeout=AX_BOOK_HTTP_TIMEOUT,
        )
        last_http = code
        if code < 200 or code >= 300:
            if isinstance(body, dict):
                last_detail = str(body.get("error") or body.get("message") or body)[:200]
            elif isinstance(body, str):
                last_detail = body[:200]
            else:
                last_detail = type(body).__name__
            if level in (None, 2) and code == 400:
                continue
            raise RuntimeError(f"HTTP {code}: {last_detail}")
        if not isinstance(body, dict):
            continue
        b, a = parse_orderbook_payload(body)
        if b or a:
            return b[:cap], a[:cap]
    if last_http >= 400:
        raise RuntimeError(f"HTTP {last_http}: {last_detail}")
    tb, ta = _ax_ticker_l1_book(st, symbol)
    if tb or ta:
        if symbol not in _AX_TICKER_L1_NOTE_SYMS:
            _AX_TICKER_L1_NOTE_SYMS.add(symbol)
            desk_log(
                st,
                f"AX /book returned no rows for {symbol!r}; desk shows GET /ticker best bid/ask as 1-level depth "
                "(full L2/L3 book can be empty on sandbox while ticker still updates).",
            )
        return tb[:cap], ta[:cap]
    return [], []


def _ax_desk_log_no_bearer_throttled(st: DeskSharedState, message: str, *, interval_sec: float = 90.0) -> None:
    """Avoid spamming logs every AX poll when credentials or session token are missing."""
    now = time.time()
    with st.lock:
        if now - st._ax_no_bearer_log_ts < interval_sec:
            return
        st._ax_no_bearer_log_ts = now
    desk_log(st, message)


def refresh_ax_all_market_books(st: DeskSharedState, _base: str, stop: threading.Event) -> None:
    if not MM_DESK_AX_ALL_BOOKS:
        return
    if not _ax_auth_headers(st):
        _ax_desk_log_no_bearer_throttled(
            st, "AX multi-book: skipping (no Bearer — set api_key/api_secret for /book)"
        )
        with st.lock:
            st.ax_markets_error = "login required for /book snapshots"
            st.ax_markets_count = 0
            st.ax_grid_symbols = []
        return
    with st.lock:
        syms = [str(x).strip() for x in st.ax_book_symbols if str(x).strip()]
        if not syms:
            one = st.ax_symbol.strip()
            syms = [one] if one else []
    if not syms:
        desk_log(st, "AX multi-book: no symbols in config (market_maker.symbol / mm_desk.ax_orderbook_symbols)")
        with st.lock:
            st.ax_markets_error = "no symbols in config list"
            st.ax_markets_count = 0
            st.ax_grid_symbols = []
        return
    tradeable, inv_err = fetch_ax_markets_list(_base)
    valid = set(tradeable) if (not inv_err and tradeable) else set()
    instruments_fallback = False
    if valid:
        canon_by_upper = {str(t).strip().upper(): str(t).strip() for t in tradeable if str(t).strip()}
        skipped: list[str] = []
        matched: list[str] = []
        seen_m: set[str] = set()
        for x in syms:
            raw = str(x).strip()
            if not raw:
                continue
            c = canon_by_upper.get(raw.upper())
            if c:
                if c not in seen_m:
                    seen_m.add(c)
                    matched.append(c)
            else:
                skipped.append(raw)
        if skipped:
            key_t = tuple(skipped[:12])
            now_l = time.time()
            should_log = False
            with st.lock:
                if key_t != st._ax_skip_log_key or (now_l - st._ax_skip_log_ts) >= 120.0:
                    st._ax_skip_log_key = key_t
                    st._ax_skip_log_ts = now_l
                    should_log = True
            if should_log:
                desk_log(
                    st,
                    "AX desk: not on /instruments (wrong venue or typo): " + ", ".join(skipped[:12]),
                )
        syms = matched
    if not syms:
        with st.lock:
            prim = st.ax_symbol.strip()
        if prim:
            instruments_fallback = True
            desk_log(
                st,
                "AX multi-book: no desk symbols matched GET /instruments — fetching primary "
                f"{prim!r} anyway (list may lag; /book often still works)",
            )
            syms = [prim]
        else:
            desk_log(
                st,
                "AX multi-book: every config symbol missing from GET /instruments — check "
                "market_maker.symbol / mm_desk.ax_orderbook_symbols",
            )
            with st.lock:
                st.ax_markets_error = "no config symbols listed on exchange"
                st.ax_markets_count = 0
                st.ax_grid_symbols = []
            return
    desk_log(
        st,
        f"AX config symbols {len(syms)} — fetching {min(len(syms), AX_MAX_SYMBOLS_ALL)} /book (level 2, top {AX_ALL_BOOK_DEPTH})",
    )
    with st.lock:
        st.ax_markets_error = (
            "Primary symbol not on latest /instruments list; /book was still requested"
            if instruments_fallback
            else None
        )
        st.ax_markets_count = len(syms)
    lim = syms[:AX_MAX_SYMBOLS_ALL]
    all_books: dict[str, dict] = {}
    n_ok = 0
    for s_sym in lim:
        if stop.is_set():
            break
        try:
            bb, aa = fetch_ax_book_st(st, s_sym, AX_ALL_BOOK_DEPTH)
            all_books[s_sym] = {
                "bids": [[float(p), float(s)] for p, s in bb],
                "asks": [[float(p), float(s)] for p, s in aa],
            }
            n_ok += 1
        except Exception as ex:
            all_books[s_sym] = {"bids": [], "asks": [], "_err": str(ex)[:100]}
    desk_log(st, f"AX multi orderbook: {n_ok}/{len(lim)} symbols OK")
    with st.lock:
        st.ax_all_books = all_books
        st.ax_grid_symbols = list(lim)
        st.ax_all_books_updated_ms = int(time.time() * 1000)
        st.ax_latency_text = (
            f"AX grid /book {n_ok}/{len(lim)} OK · books ~{MM_DESK_AX_BOOK_POLL_SEC:.0f}s · "
            f"acct ~{MM_DESK_ACCOUNT_POLL_SEC:.0f}s (HTTP {AX_BOOK_HTTP_TIMEOUT:.0f}s/read)"
        )


def _reference_multi_theo_poll_once(st: DeskSharedState, stop: threading.Event | None = None) -> tuple[int, int]:
    """Fill ref_all_books for each reference symbol from HL allMids, cme_feed URL, and/or C++ theo file (Neon)."""
    with st.lock:
        syms = [str(s).strip() for s in st.ref_book_symbols if str(s).strip()]
        base = (st.ref_rest_url or "").rstrip("/")
        primary = (st.ref_symbol or "").strip()
    if not base.startswith("http"):
        base = "https://api.hyperliquid.xyz"
    lim = syms[: max(1, MM_DESK_REF_MAX_SYMBOLS)] or ([primary] if primary else [])
    all_b: dict[str, dict] = {}
    n_ok = 0
    last_pair: tuple[list, list] | None = None
    mids: dict[str, float] = {}
    mids_err = ""
    try:
        mids, _dt = _hyperliquid_fetch_all_mids(base, REFERENCE_HTTP_TIMEOUT)
    except Exception as e:
        mids_err = str(e)[:120]
    # Legacy `cme_feed.rest_url` REST mid (kept as a no-config-required
    # fallback for the mettraders branch — when the C++ trading_client is
    # not the one writing `quotes_by_canonical`, this REST poll can still
    # supply the gold mid). Pre-2026-05 layouts relied on this path; the
    # C++ Mettraders WS now publishes into `quotes_by_canonical` directly,
    # so the REST poll is just belt-and-suspenders.
    cme_url = (getattr(st, "cme_feed_rest_url", "") or "").strip()
    cme_mid: float | None = None
    if cme_url.startswith("http"):
        try:
            req = urllib.request.Request(cme_url, method="GET")
            with urllib.request.urlopen(req, context=_ssl_ctx(), timeout=REFERENCE_HTTP_TIMEOUT) as resp:
                j = json.loads(resp.read().decode("utf-8"))
            cme_mid = _to_float((j or {}).get("mid")) or _to_float((j or {}).get("price"))
        except Exception:
            cme_mid = None
    th_cache = read_cpp_theo_cache_for_desk() or {}
    qbc = th_cache.get("quotes_by_canonical") if isinstance(th_cache, dict) else None

    def _qbc_mid(want_canon: str) -> float | None:
        """Canonical-key lookup in the C++ theo cache (`quotes_by_canonical`)."""
        if not want_canon or not isinstance(qbc, dict):
            return None
        for k, v in qbc.items():
            if fix_symbol_canonical_py(str(k)) == want_canon and isinstance(v, dict):
                m = _to_float(v.get("mid"))
                if m is not None and math.isfinite(m) and m > 0:
                    return float(m)
        return None

    for sym in lim:
        if stop is not None and stop.is_set():
            break
        src = _theo_source_for_ref_in_mm(st, sym)
        if src == "hyperliquid":
            mid, _used = _hyperliquid_pick_mid_for_symbol(mids, sym)
            if mid is not None:
                m = float(mid)
                all_b[sym] = {
                    "bids": [[m, 1.0]],
                    "asks": [[m, 1.0]],
                }
                n_ok += 1
                if (sym or "").upper() == (primary or "").upper() or not last_pair:
                    last_pair = ([(m, 1.0)], [(m, 1.0)])
            else:
                err_msg = "symbol unavailable on Hyperliquid allMids"
                if mids_err:
                    err_msg = f"allMids: {mids_err}"
                all_b[sym] = {"bids": [], "asks": [], "_err": err_msg}
            continue
        # Both neon_fix and mettraders (incl. legacy `cme`) publish quotes
        # into the C++ trading_client's `quotes_by_canonical` map. Look up
        # the canonical key first; mettraders also falls back to the legacy
        # `cme_feed.rest_url` REST mid when the C++ cache is empty so a
        # stand-alone desk (no trading_client) can still display the gold
        # mid if the operator configures cme_feed.rest_url.
        if src in ("neon_fix", "mettraders", "cme"):
            want = fix_symbol_canonical_py(sym)
            found = _qbc_mid(want)
            if found is None and src in ("mettraders", "cme") and cme_mid is not None and \
                    math.isfinite(cme_mid) and cme_mid > 0:
                found = float(cme_mid)
            if found is not None and math.isfinite(found) and found > 0:
                m = float(found)
                all_b[sym] = {
                    "bids": [[m, 1.0]],
                    "asks": [[m, 1.0]],
                }
                n_ok += 1
                if (sym or "").upper() == (primary or "").upper() or not last_pair:
                    last_pair = ([(m, 1.0)], [(m, 1.0)])
            else:
                if src == "neon_fix":
                    err = "Neon: no mid in C++ theo cache for this ref (run trading_client)"
                else:
                    err = (
                        "Mettraders: no mid in C++ theo cache for this ref "
                        "(run trading_client with mettraders_feed.enabled and "
                        "a leg using theo_source=mettraders, or set cme_feed.rest_url)"
                    )
                all_b[sym] = {"bids": [], "asks": [], "_err": err}
            continue
        all_b[sym] = {
            "bids": [],
            "asks": [],
            "_err": (
                f"theo_source {src!r} not wired in multi-theo desk poll for {sym!r} "
                f"(expected hyperliquid|neon_fix|mettraders)"
            ),
        }
    if last_pair is None:
        for s2 in lim:
            d0 = all_b.get(s2) or {}
            if d0.get("_err"):
                continue
            rb, ra = d0.get("bids") or [], d0.get("asks") or []
            if rb and ra:
                last_pair = (
                    [(float(p), float(s)) for p, s in rb],
                    [(float(p), float(s)) for p, s in ra],
                )
                break
    with st.lock:
        st.ref_all_books = all_b
        st.ref_all_books_updated_ms = int(time.time() * 1000)
        if last_pair is not None:
            st.last_ref_book = last_pair
        st.ref_latency_text = (
            f"Multi theo: {n_ok}/{len(lim)} refs OK · C++ + HL + cme_feed (poll ~{REST_POLL_INTERVAL_SEC:.0f}s)"
        )
    return n_ok, len(lim)


def reference_rest_poll_once(st: DeskSharedState, stop: threading.Event | None = None) -> tuple[int, int]:
    """
    Single REST sweep over ref_book_symbols. Updates ref_all_books / last_ref_book / ref_latency_text.
    Returns (symbols_ok_count, len(lim)).
    """
    with st.lock:
        is_multi = st.reference_provider == "multi" or getattr(st, "multi_theo", False)
    if is_multi:
        return _reference_multi_theo_poll_once(st, stop)
    with st.lock:
        st.ref_feed_mode = "rest"
        syms = [str(s).strip() for s in st.ref_book_symbols if str(s).strip()]
        base = st.ref_rest_url.rstrip("/")
        primary = st.ref_symbol.strip()
        use_spot = st.ref_rest_uses_spot_path
        provider = st.reference_provider
    if not base.startswith("http"):
        if _is_hyperliquid_provider(provider):
            base = "https://api.hyperliquid.xyz"
        else:
            with st.lock:
                st.ref_latency_text = "REST: set external_feed.rest_url"
            return 0, 0
    lim = syms[: max(1, MM_DESK_REF_MAX_SYMBOLS)] or ([primary] if primary else [])
    all_b: dict[str, dict] = {}
    n_ok = 0
    last_pair: tuple[list, list] | None = None
    rtt_sum = 0.0
    if _is_hyperliquid_provider(provider):
        mids: dict[str, float] = {}
        mids_err = ""
        try:
            mids, dtm = _hyperliquid_fetch_all_mids(base, REFERENCE_HTTP_TIMEOUT)
            rtt_sum += dtm
        except Exception as e:
            mids_err = str(e)[:120]
        for sym in lim:
            if stop is not None and stop.is_set():
                break
            url = _hyperliquid_l2_book_url(base)
            req_payload = None
            try:
                matched_l2 = False
                for cand in _hyperliquid_symbol_candidates(sym):
                    req = urllib.request.Request(url, method="POST")
                    req.add_header("Content-Type", "application/json")
                    req_payload = {"type": "l2Book", "coin": cand}
                    data = json.dumps(req_payload).encode("utf-8")
                    t0 = time.monotonic()
                    with urllib.request.urlopen(req, data=data, context=_ssl_ctx(), timeout=REFERENCE_HTTP_TIMEOUT) as resp:
                        j = json.loads(resp.read().decode("utf-8"))
                    b, a = _hyperliquid_parse_l2_book(j)
                    if b or a:
                        matched_l2 = True
                        dt = (time.monotonic() - t0) * 1000
                        rtt_sum += dt
                        all_b[sym] = {
                            "bids": [[float(p), float(s)] for p, s in b[:BOOK_DEPTH]],
                            "asks": [[float(p), float(s)] for p, s in a[:BOOK_DEPTH]],
                        }
                        n_ok += 1
                        if sym.upper() == primary.upper() and (b or a):
                            last_pair = (list(b[:BOOK_DEPTH]), list(a[:BOOK_DEPTH]))
                        desk_log(
                            st,
                            f"Hyperliquid l2Book OK {sym}<-{cand} bid={len(b)} ask={len(a)} {dt:.0f}ms",
                            verbose_only=True,
                        )
                        break
                if matched_l2:
                    continue
                mid, used = _hyperliquid_pick_mid_for_symbol(mids, sym)
                if mid is not None:
                    all_b[sym] = {
                        "bids": [[float(mid), 1.0]],
                        "asks": [[float(mid), 1.0]],
                    }
                    n_ok += 1
                    if sym.upper() == primary.upper():
                        last_pair = ([(float(mid), 1.0)], [(float(mid), 1.0)])
                    desk_log(
                        st,
                        f"Hyperliquid allMids fallback {sym}<-{used} mid={mid:.6f} (no l2Book)",
                        verbose_only=True,
                    )
                else:
                    err_msg = "symbol unavailable on Hyperliquid allMids/l2Book"
                    if mids_err:
                        err_msg = f"allMids unavailable: {mids_err}"
                    all_b[sym] = {"bids": [], "asks": [], "_err": err_msg}
            except Exception as e:
                detail = str(e)[:120]
                if req_payload is not None:
                    detail = f"{detail} payload={req_payload}"
                desk_log(st, f"Hyperliquid l2Book FAIL {sym}: {type(e).__name__}: {detail}")
                mid, used = _hyperliquid_pick_mid_for_symbol(mids, sym)
                if mid is not None:
                    all_b[sym] = {
                        "bids": [[float(mid), 1.0]],
                        "asks": [[float(mid), 1.0]],
                    }
                    n_ok += 1
                    if sym.upper() == primary.upper():
                        last_pair = ([(float(mid), 1.0)], [(float(mid), 1.0)])
                    desk_log(
                        st,
                        f"Hyperliquid fallback after l2Book error {sym}<-{used} mid={mid:.6f}",
                        verbose_only=True,
                    )
                else:
                    all_b[sym] = {"bids": [], "asks": [], "_err": detail}
    else:
        for sym in lim:
            if stop is not None and stop.is_set():
                break
            url = _reference_depth_http_url(base, sym.upper(), use_spot, depth_limit=None)
            try:
                req = urllib.request.Request(url, method="GET")
                t0 = time.monotonic()
                with urllib.request.urlopen(req, context=_ssl_ctx(), timeout=REFERENCE_HTTP_TIMEOUT) as resp:
                    j = json.loads(resp.read().decode("utf-8"))
                dt = (time.monotonic() - t0) * 1000
                rtt_sum += dt
                b, a = parse_orderbook_payload(j)
                all_b[sym] = {
                    "bids": [[float(p), float(s)] for p, s in b[:BOOK_DEPTH]],
                    "asks": [[float(p), float(s)] for p, s in a[:BOOK_DEPTH]],
                }
                n_ok += 1
                if sym.upper() == primary.upper() and (b or a):
                    last_pair = (list(b[:BOOK_DEPTH]), list(a[:BOOK_DEPTH]))
                desk_log(
                    st,
                    f"Reference depth OK {sym} bid={len(b)} ask={len(a)} {dt:.0f}ms",
                    verbose_only=True,
                )
            except Exception as e:
                desk_log(st, f"Reference depth FAIL {sym}: {type(e).__name__}: {e}")
                all_b[sym] = {"bids": [], "asks": [], "_err": str(e)[:120]}
    if last_pair is None:
        for sym in lim:
            d = all_b.get(sym) or {}
            if d.get("_err"):
                continue
            rb, ra = d.get("bids") or [], d.get("asks") or []
            if rb or ra:
                last_pair = (
                    [(float(p), float(s)) for p, s in rb],
                    [(float(p), float(s)) for p, s in ra],
                )
                break
    avg = rtt_sum / max(1, len(lim))
    with st.lock:
        st.ref_all_books = all_b
        st.ref_all_books_updated_ms = int(time.time() * 1000)
        if last_pair is not None:
            st.last_ref_book = last_pair
        if _is_hyperliquid_provider(provider):
            st.ref_latency_text = (
                f"Hyperliquid l2Book: {n_ok}/{len(lim)} symbols OK · RTT ~{avg:.0f}ms · poll ~{REST_POLL_INTERVAL_SEC:.0f}s"
            )
        else:
            st.ref_latency_text = (
                f"Reference REST: {n_ok}/{len(lim)} symbols OK · RTT ~{avg:.0f}ms · poll ~{REST_POLL_INTERVAL_SEC:.0f}s"
            )
    if _is_hyperliquid_provider(provider):
        desk_log(st, f"Hyperliquid multi depth {n_ok}/{len(lim)} symbols OK", verbose_only=True)
    else:
        desk_log(st, f"Reference multi depth {n_ok}/{len(lim)} symbols OK", verbose_only=True)
    return n_ok, len(lim)


def reference_rest_feed_loop(st: DeskSharedState, stop: threading.Event) -> None:
    while not stop.is_set():
        reference_rest_poll_once(st, stop)
        _sleep_polling(stop)


def reference_market_feed_loop(st: DeskSharedState, stop: threading.Event) -> None:
    """Reference column: Neon FIX (C++-compatible) or generic REST depth polling."""
    with st.lock:
        refp = st.reference_provider
    if refp == "neon_fix":
        dc = get_merged_config_dict()
        settings = neon_fix_feed.settings_from_merged_config(dc)
        if not settings:
            desk_log(
                st,
                "Neon FIX: incomplete external_feed.fix (sender/target comp id, username, md_symbols; "
                "and either host:port for stunnel or direct_tls + tls_remote_host + tls_remote_port).",
                verbose_only=False,
            )
            with st.lock:
                st.ref_latency_text = "Neon FIX: bad config"
            return
        def _neon_reload_md_after_sl() -> None:
            dc = get_merged_config_dict()
            nfs = neon_fix_feed.settings_from_merged_config(dc)
            if nfs and nfs.md_symbols:
                settings.md_symbols = list(nfs.md_symbols)

        neon_fix_feed.run_neon_fix_desk_loop(
            st,
            settings,
            stop,
            desk_log,
            register_sock=lambda s: desk_register_neon_fix_socket(st, s),
            on_security_list=lambda sy: desk_apply_neon_security_list(st, sy),
            reload_settings_after_sl=_neon_reload_md_after_sl,
        )
        return
    reference_rest_feed_loop(st, stop)


def _norm_open_order_price_for_ax(st: DeskSharedState, ax_canon: str, price: float) -> float:
    """
    Round gateway order limit to the AX leg's quote tick from ``GET /instruments`` (cached on
    ``DeskSharedState.ax_tick_by_symbol``). If the gateway has not published a tick for this symbol
    yet, the price is returned unchanged (no ``PRICE_TICK`` / config fallback).
    """
    gt = _gateway_tick_for_ax(st, ax_canon)
    if gt is None or float(gt) <= 0.0 or not math.isfinite(float(gt)):
        desk_log(
            st,
            f"_norm_open_order_price_for_ax: no gateway tick for {ax_canon!r}; limit price not rounded to grid",
            verbose_only=True,
        )
        return float(price)
    tick = float(gt)
    return round(round(price / tick) * tick, 10)


def ax_feed_loop(st: DeskSharedState, stop: threading.Event) -> None:
    last_book = -1e9
    last_acct = -1e9
    while not stop.is_set():
        now = time.monotonic()
        do_book = (now - last_book) >= MM_DESK_AX_BOOK_POLL_SEC
        do_acct = (now - last_acct) >= MM_DESK_ACCOUNT_POLL_SEC

        with st.lock:
            last_tr = float(getattr(st, "_ax_tick_map_refresh_mono", 0.0) or 0.0)
            rb_tr = str(st.rest_endpoint or "").strip().rstrip("/")
        if (
            MM_DESK_AX_INSTRUMENT_TICK_REFRESH_SEC > 0
            and rb_tr
            and (now - last_tr) >= MM_DESK_AX_INSTRUMENT_TICK_REFRESH_SEC
        ):
            try:
                desk_refresh_gateway_instrument_ticks(st, rb_tr)
            except Exception as e:
                desk_log(st, f"gateway /instruments tick refresh: {type(e).__name__}: {e}")

        with st.lock:
            sym = st.ax_symbol.strip()
            base = st.rest_endpoint.strip().rstrip("/")
            raw_list = [str(x).strip() for x in st.ax_book_symbols if str(x).strip()]
            raw_up = {x.upper() for x in raw_list}
            if sym and sym.upper() not in raw_up:
                raw_list.append(sym)
                raw_up.add(sym.upper())
            grid_syms: set[str] = set(raw_list)
            _order_sym_canon: dict[str, str] = {}
            for gx in raw_list:
                if not gx:
                    continue
                u = gx.upper()
                if u not in _order_sym_canon:
                    _order_sym_canon[u] = gx

        if do_book:
            last_book = time.monotonic()
            try:
                if not _ax_auth_headers(st):
                    _ax_desk_log_no_bearer_throttled(
                        st,
                        "AX primary /book skipped: no Bearer token (api_key/api_secret in config?)",
                    )
                    with st.lock:
                        st.ax_latency_text = "AX: /book needs auth — see config credentials"
                else:
                    # When multi-book is on, it already GET /book for every config symbol (incl. primary).
                    # A separate primary fetch doubled traffic and could block ~AX_HTTP_TIMEOUT before the
                    # grid refreshed — UI looked "frozen" while waiting on a redundant call.
                    if not MM_DESK_AX_ALL_BOOKS:
                        t0 = time.monotonic()
                        b, a = fetch_ax_book_st(st, sym, BOOK_DEPTH)
                        dt = (time.monotonic() - t0) * 1000
                        with st.lock:
                            st.last_ax_book = (b, a)
                            st.ax_latency_text = (
                                f"AX /book RTT ~{dt:.0f}ms · books ~{MM_DESK_AX_BOOK_POLL_SEC:.0f}s · "
                                f"acct ~{MM_DESK_ACCOUNT_POLL_SEC:.0f}s"
                            )
                        desk_log(
                            st,
                            f"AX primary book OK {sym} {dt:.0f}ms b/a={len(b)}/{len(a)}",
                            verbose_only=True,
                        )
            except Exception as e:
                desk_log(st, f"AX primary /book FAIL {sym}: {type(e).__name__}: {e}")

            try:
                refresh_ax_all_market_books(st, base, stop)
            except Exception as e:
                desk_log(st, f"AX all-books refresh error: {e}")

            with st.lock:
                sym0 = st.ax_symbol.strip()
                if MM_DESK_AX_ALL_BOOKS and sym0:
                    d = st.ax_all_books.get(sym0) or {}
                    if not d.get("_err"):
                        rb, ra = d.get("bids") or [], d.get("asks") or []

                        def _lvls(levels: list) -> list[tuple[float, float]]:
                            out: list[tuple[float, float]] = []
                            for r in levels or []:
                                if isinstance(r, (list, tuple)) and len(r) >= 2:
                                    pf = _to_float(r[0])
                                    qf = _to_float(r[1])
                                    if pf is not None and qf is not None:
                                        out.append((pf, qf))
                            return out

                        if rb or ra:
                            lb = _lvls(rb)[:BOOK_DEPTH]
                            la = _lvls(ra)[:BOOK_DEPTH]
                            st.last_ax_book = (lb, la)

            if not MM_DESK_AX_ALL_BOOKS:
                with st.lock:
                    s_grid = st.ax_symbol.strip()
                    st.ax_grid_symbols = [s_grid] if s_grid else []

            with st.lock:
                no_tok = st.token is None
            if no_tok:
                try:
                    tape = _tape_lines_st(st)
                    with st.lock:
                        st.tape_lines = tape
                except Exception:
                    pass

        if do_acct:
            last_acct = time.monotonic()
            with st.lock:
                tok = st.token
            if tok:
                try:
                    orders_all, o_ok, o_err = _open_orders_fetch(st)
                    orders_hist, h_ok, h_err = _orders_history_fetch(st)
                    fills, f_ok, f_err = _fills_fetch(st)
                    mine_by: dict[str, set[float]] = {}
                    for o in orders_all:
                        osym = str(o.get("s", o.get("symbol", o.get("Symbol", "")))).strip()
                        canon = _order_sym_canon.get(osym.upper()) if osym else None
                        if not canon:
                            continue
                        st_l = str(o.get("status", o.get("state", o.get("o", "")))).lower()
                        if st_l and st_l not in (
                            "open",
                            "new",
                            "accepted",
                            "partially_filled",
                            "partial",
                            "working",
                            "",
                            "pending",
                        ):
                            continue
                        op = _to_float(o.get("price") or o.get("p") or o.get("limit_price"))
                        if op is not None:
                            np = _norm_open_order_price_for_ax(st, canon, float(op))
                            mine_by.setdefault(canon, set()).add(np)
                    rows_pos, pcode, perr = _positions_array_st(st)
                    pos_line = _position_line_from_rows(rows_pos, pcode, sym, perr)
                    pos_snap = _position_snapshot_from_rows(rows_pos, pcode, sym, perr)
                    tape = _tape_lines_st(st)
                    acct_parts: list[str] = []
                    if o_ok:
                        acct_parts.append(f"open orders ({len(orders_all)})")
                    else:
                        acct_parts.append(f"open orders: {o_err}")
                    if h_ok:
                        acct_parts.append(f"history ({len(orders_hist)})")
                    else:
                        acct_parts.append(f"history: {h_err}")
                    if f_ok:
                        acct_parts.append(f"fills OK ({len(fills)})")
                    else:
                        acct_parts.append(f"fills: {f_err}")
                    detail = " · ".join(acct_parts)
                    with st.lock:
                        st.orders_api_last_ok = o_ok or h_ok
                        st.fills_api_last_ok = f_ok
                        st.account_api_detail = detail
                        prim_canon = _order_sym_canon.get(sym.upper(), sym) if sym else sym
                        st.my_prices = set(mine_by.get(prim_canon, ()))
                        st.my_prices_by_symbol = {k: set(v) for k, v in mine_by.items()}
                        st.fills_snapshot = fills
                        if h_ok and orders_hist:
                            st.orders_snapshot = orders_hist[:ORDERS_LIMIT]
                        elif o_ok:
                            st.orders_snapshot = orders_all[:ORDERS_LIMIT]
                        else:
                            st.orders_snapshot = []
                        st.position_line = pos_line
                        st.position_snapshot = pos_snap
                        st.tape_lines = tape
                        st.stats_line = (
                            f"Open orders (loaded): {len(orders_all)}  |  Fills (loaded): {len(fills)}"
                            + (f"  |  {detail}" if detail else "")
                        )
                    # Paired-order cancellation: observe rejects/resolutions on the
                    # freshest snapshot. Done OUTSIDE st.lock so desk_cancel_one_order
                    # can take the lock. Best-effort — never break the feed loop.
                    try:
                        scan_src = (
                            orders_hist if (h_ok and orders_hist)
                            else (orders_all if o_ok else [])
                        )
                        _mm_scan_orders_for_paired_cancel(st, scan_src)
                    except Exception as _pce:
                        desk_log(
                            st,
                            f"paired-cancel scan error (non-fatal): {type(_pce).__name__}: {_pce}",
                        )
                except Exception as e:
                    desk_log(st, f"AX account snapshot error (feeds continue): {type(e).__name__}: {e}")
                    with st.lock:
                        st.orders_api_last_ok = False
                        st.fills_api_last_ok = False
                        st.account_api_detail = str(e)[:160]
                        st.stats_line = (
                            f"Account APIs error: {type(e).__name__} — check order-gateway reachability"
                        )
                        st.position_snapshot = {
                            "ok": False,
                            "error": str(e)[:200],
                            "primary_symbol": st.ax_symbol.strip(),
                            "primary": None,
                            "all_positions": [],
                            "non_flat_count": 0,
                            "total_abs_qty": 0.0,
                            "total_unrealized_pnl": None,
                        }
            else:
                tape = _tape_lines_st(st)
                with st.lock:
                    sym_u = st.ax_symbol.strip()
                    st.my_prices = set()
                    st.my_prices_by_symbol = {}
                    st.orders_api_last_ok = False
                    st.fills_api_last_ok = False
                    st.account_api_detail = "not authenticated"
                    st.fills_snapshot = []
                    st.orders_snapshot = []
                    st.position_line = (
                        "Architect: not logged in — /book needs Bearer; add api_key/api_secret "
                        "or session_token to config/credentials.local.json"
                    )
                    st.stats_line = "Fills / orders / position require Architect authentication"
                    st.tape_lines = tape
                    st.position_snapshot = {
                        "ok": False,
                        "error": "not authenticated",
                        "primary_symbol": sym_u,
                        "primary": None,
                        "all_positions": [],
                        "non_flat_count": 0,
                        "total_abs_qty": 0.0,
                        "total_unrealized_pnl": None,
                    }

        next_b = MM_DESK_AX_BOOK_POLL_SEC - (time.monotonic() - last_book)
        next_a = MM_DESK_ACCOUNT_POLL_SEC - (time.monotonic() - last_acct)
        wait = min(next_b, next_a, 0.5)
        if wait < 0.05:
            wait = 0.05
        t_rem = wait
        while t_rem > 0 and not stop.is_set():
            chunk = min(0.1, t_rem)
            time.sleep(chunk)
            t_rem -= chunk


def _order_oid(o: dict) -> str:
    for k in ("oid", "exchange_order_id", "id", "order_id", "orderId"):
        v = o.get(k)
        if v is not None and str(v).strip():
            return str(v).strip()
    return ""


def _desk_response_exchange_oid(resp: object) -> str:
    if not isinstance(resp, dict):
        return ""
    o = _order_oid(resp)
    if o:
        return o
    for nest in (resp.get("data"), resp.get("order"), resp.get("result")):
        if isinstance(nest, dict):
            o = _order_oid(nest)
            if o:
                return o
    return ""


def _desk_signal_path_for_cpp() -> Path:
    raw = os.environ.get("MM_DESK_SIGNAL_PATH", "").strip()
    if raw:
        return Path(raw).expanduser()
    try:
        if DEFAULT_CONFIG_PATH.is_file():
            dc = json.loads(DEFAULT_CONFIG_PATH.read_text(encoding="utf-8"))
            mm = dc.get("market_maker") or {}
            p = str(mm.get("desk_signal_path") or "").strip()
            if p:
                pp = Path(p)
                return pp if pp.is_absolute() else (REPO_ROOT / pp)
    except OSError:
        pass
    except json.JSONDecodeError:
        pass
    return REPO_ROOT / "logs" / "mm_desk_signal.json"


def _write_mm_desk_signal_for_cpp(st: DeskSharedState, sym_ax: str, rq: dict, merged_ok: bool) -> None:
    """Notify the C++ trading loop (when market_maker.desk_sync_enabled) to reload config and adopt desk OIDs."""
    if not merged_ok:
        return
    with st.lock:
        cfg_qty = int(st.mm_order_size)
    qb = int(rq.get("qty_bid") or 0)
    qa = int(rq.get("qty_ask") or 0)
    qty_line = cfg_qty if (qb <= 0 and qa <= 0) else max(qb, qa)
    path = _desk_signal_path_for_cpp()
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
    except OSError:
        return
    bid_oid = _desk_response_exchange_oid(rq.get("bid_response")) if rq.get("bid_response") is not None else ""
    ask_oid = _desk_response_exchange_oid(rq.get("ask_response")) if rq.get("ask_response") is not None else ""
    obj: dict[str, object] = {
        "seq": int(time.time_ns()),
        "ts_ms": int(time.time() * 1000),
        "mm_desk_active": True,
        "symbol": sym_ax,
        "bid_exchange_oid": bid_oid,
        "ask_exchange_oid": ask_oid,
        "bid_price": rq.get("bid"),
        "ask_price": rq.get("ask"),
        "quantity": qty_line,
        "bid_quantity": qb,
        "ask_quantity": qa,
        "theo_mid": rq.get("theo_mid"),
    }
    try:
        _atomic_write_json(path, obj)
    except OSError:
        pass


def web_applyQuotes(st: DeskSharedState, body: dict) -> dict:
    """
    Cancel open AX orders for the MM symbol, then place bid/ask from external theo + exchange net,
    matching C++ MakeMarketStrategy skew / max-position rules.
    """
    ax_req = str((body or {}).get("ax_symbol", "") or (body or {}).get("symbol", "") or "").strip()
    with st.lock:
        sym_ax = ax_req or st.ax_symbol.strip()
    if not sym_ax:
        return {"ok": False, "error": "no ax_symbol"}
    out = desk_cancel_all_orders(st, symbol=sym_ax)
    if not out.get("ok"):
        return {**out, "hint": "Fix auth or order-gateway; requote not attempted"}
    rq = desk_requote_mm_pair_after_cancel(st, symbol=sym_ax, log_context="web_apply_persist")
    merged_ok = bool(out.get("ok")) and bool(rq.get("ok"))
    _write_mm_desk_signal_for_cpp(st, sym_ax, rq, merged_ok)
    desk_log(
        st,
        "web_apply_persist: desk_signal merged_ok="
        f"{merged_ok!r} path={str(_desk_signal_path_for_cpp())} "
        "(C++ trading_client polls this when market_maker.desk_sync_enabled)",
    )
    err_top = None if merged_ok else (rq.get("error") or "requote failed")
    return {
        "ok": merged_ok,
        "symbol": sym_ax,
        "cancel_all": out,
        "requote": rq,
        "quotes_from_python": True,
        "hint": rq.get("message") or rq.get("error"),
        **({"error": err_top} if err_top else {}),
    }


def ensure_ax_session(st: DeskSharedState) -> tuple[bool, str]:
    """Use session_token from config, or authenticate with api_key/api_secret (same sources as C++)."""
    with st.lock:
        if st.token:
            return True, "using session_token or prior login"
        key = (st.api_key or "").strip()
        sec = (st.api_secret or "").strip()
        base = st.rest_endpoint.rstrip("/")
    if not key or not sec:
        return (
            False,
            "no Architect api_key/api_secret — GET /instruments works; /book and trading need credentials "
            "(config/credentials.local.json or login in UI)",
        )
    code, body = http_json(
        "POST",
        base + "/authenticate",
        {"api_key": key, "api_secret": sec, "expiration_seconds": 86400},
    )
    if code < 200 or code >= 300 or not isinstance(body, dict):
        return False, f"Architect /authenticate HTTP {code}"
    tok = extract_bearer_token(body)
    if not tok:
        keys = list(body.keys()) if isinstance(body, dict) else []
        return False, f"no token in /authenticate body (got keys: {keys[:12]})"
    with st.lock:
        st.token = tok
    return True, "Architect authenticated with config credentials"


def _read_json_body(handler: BaseHTTPRequestHandler, limit: int = 1_000_000) -> dict:
    ln = int(handler.headers.get("Content-Length", "0") or 0)
    if ln <= 0 or ln > limit:
        return {}
    raw = handler.rfile.read(ln).decode("utf-8", errors="replace")
    try:
        j = json.loads(raw)
        return j if isinstance(j, dict) else {}
    except json.JSONDecodeError:
        return {}


# Injected into _INDEX_HTML_TEMPLATE for the Reference depth column (Neon or REST reference feed).
_REF_DEPTH_COL_BLOCK = """<div class="depth-col depth-ref-books-col" id="refDepthCol">
<div class="card depth-card"><div class="card-h"><span id="bnDepthCardTitle">Reference</span> <span class="ob-instrument" id="bnDepthInstrument">—</span> <span class="feed-tag" id="bnDepthTag">…</span> <span id="bnMarketsMeta"></span></div><div id="bnAllWrap"><p class="hint" id="hintBnAll" style="display:block;padding:12px">Waiting for reference feed…</p><div class="ax-books-scroll-host"><div class="ax-all-grid ref-depth-grid" id="bnAllGrid"></div></div></div></div>
</div>
"""

# Trading UI logic is in mm_live_desk_client.js (same directory); served at /mm_live_desk_client.js to avoid huge inline scripts.
_INDEX_HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en"><head>
<meta charset="utf-8"/>
<meta http-equiv="Cache-Control" content="no-store, no-cache, must-revalidate, max-age=0"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<!-- mm_desk_core build __MM_DESK_BUILD__ — if this comment is missing, you are not loading this server's HTML -->
<title>MM Live Desk</title>
<style>
:root { --bg:#070a0f; --panel:#0c1017; --card:#121820; --line:rgba(255,255,255,.07); --fg:#e8edf4; --muted:#8b9cb3; --acc:#5ea3ff; --bid:#3ecf8e; --ask:#ff6b6b; --mine:#ffeb3b; --go:#238636; --danger:#c92a2a; --fs:15px; --fs-sm:13px; --fs-lg:17px; }
* { box-sizing: border-box; }
body { margin:0; height:100%; min-height:100%; font-family: ui-sans-serif, system-ui, -apple-system, "Segoe UI", sans-serif; background: var(--bg); color: var(--fg); font-size: var(--fs); line-height: 1.45; -webkit-font-smoothing: antialiased; }
#loadOverlay {
  position: fixed; inset: 0; z-index: 9999; background: var(--bg); display: flex; align-items: center; justify-content: center;
  pointer-events: auto;
}
#loadOverlay.mm-desk-overlay-done { display: none !important; pointer-events: none !important; }
.load-card { text-align: center; color: var(--muted); font-size: 14px; }
.spinner {
  width: 36px; height: 36px; margin: 0 auto 14px; border: 3px solid #1e2530; border-top-color: var(--acc);
  border-radius: 50%; animation: spin 0.65s linear infinite;
}
@keyframes spin { to { transform: rotate(360deg); } }
html, body { height: 100%; margin: 0; }
#app {
  height: 100%;
  min-height: 100%;
  display: flex;
  flex-direction: column;
  overflow: hidden;
}
#deskTabPanels { flex: 1 1 0; min-height: 0; display: flex; flex-direction: column; overflow: hidden; }
#deskTabPanels > .tab-panel:not(.hidden) { flex: 1; min-height: 0; overflow: auto; }
.bar {
  display: flex; flex-wrap: wrap; align-items: center; gap: 12px 18px; padding: 14px 32px;
  background: #0d1118; border-bottom: 1px solid var(--line);
}
.logo { font-weight: 700; font-size: 1.15rem; color: var(--acc); letter-spacing: -0.02em; margin-right: 10px; }
.pill {
  font-size: 11px; font-weight: 700; letter-spacing: 0.06em; text-transform: uppercase;
  padding: 6px 12px; border-radius: 999px; border: 1px solid var(--line); color: var(--muted);
}
.pill.ok { border-color: rgba(62,207,142,.35); background: rgba(62,207,142,.12); color: #3ecf8e; }
.pill.warn { border-color: rgba(234,197,79,.35); background: rgba(234,197,79,.1); color: var(--mine); }
.bar-tail {
  margin-left: auto;
  display: flex;
  align-items: center;
  gap: 14px;
  flex-wrap: wrap;
  justify-content: flex-end;
  flex: 1 1 320px;
  min-width: 200px;
  max-width: 100%;
}
.mm-desk-leg-wrap {
  display: none;
  align-items: center;
  gap: 8px;
  flex: 0 1 auto;
  max-width: min(440px, 48vw);
}
.mm-desk-leg-wrap.mm-desk-leg-visible { display: flex; }
.mm-desk-leg-lab {
  font-size: 10px;
  font-weight: 700;
  letter-spacing: 0.06em;
  text-transform: uppercase;
  color: var(--muted);
  white-space: nowrap;
}
.mm-desk-leg-wrap select#theoLegViewSelect {
  font-size: 12px;
  padding: 6px 10px;
  border-radius: 8px;
  background: #121820;
  border: 1px solid var(--line);
  color: var(--fg);
  min-width: 180px;
  max-width: min(380px, 42vw);
  font-family: ui-monospace, Menlo, monospace;
}
.bar-tail .meta { margin-left: 0; font-size: var(--fs-sm); color: var(--muted); font-family: ui-monospace, Menlo, monospace; flex: 1 1 220px; min-width: 160px; max-width: none; text-align: right; word-break: break-all; }
.body { flex: 1; display: grid; grid-template-columns: 1fr 372px; min-height: 0; }
@media (max-width: 1080px) { .body { grid-template-columns: 1fr; } }
main { padding: 14px 16px; overflow: auto; }
.side {
  border-left: 1px solid var(--line); background: var(--panel); padding: 16px 18px 28px;
  overflow: auto; font-size: var(--fs);
}
.side h2 { margin: 0 0 6px; font-size: 12px; text-transform: uppercase; letter-spacing: 0.08em; color: var(--muted); font-weight: 600; }
.side .sub { color: var(--muted); font-size: var(--fs-sm); line-height: 1.5; margin-bottom: 14px; }
.fld { margin-bottom: 13px; }
.ax-ob-multi-wrap #axOrderbookMulti {
  width: 100%; box-sizing: border-box; margin-top: 6px;
  font-size: 12px; font-family: ui-monospace, Menlo, monospace;
  background: #0d1117; color: var(--fg); border: 1px solid var(--line); border-radius: 6px; padding: 6px;
}
.ax-ob-multi-wrap #axOrderbookMulti:focus { outline: 2px solid rgba(94, 163, 255, 0.45); outline-offset: 1px; }
.ax-ob-multi-wrap #axOrderbookMulti option {
  padding: 5px 6px;
  background: #0d1117;
  color: #c9d1d9;
}
/* Native <select multiple> selected rows — many themes default to invisible; force contrast. */
.ax-ob-multi-wrap #axOrderbookMulti option:checked {
  background: rgba(62, 207, 142, 0.35);
  color: #f6fffa;
  font-weight: 600;
}
.ax-ob-multi-wrap kbd { font-size: 10px; background: #1a222c; padding: 1px 5px; border-radius: 4px; border: 1px solid var(--line); }
.neon-md-multi-wrap #neonMdMulti {
  width: 100%;
  min-height: 7rem;
  max-height: 14rem;
  font-size: 12px;
  box-sizing: border-box;
  padding: 4px 6px;
}
.neon-md-multi-wrap #neonMdMulti option { padding: 4px 6px; }
.fld label { display: block; font-size: var(--fs-sm); font-weight: 500; color: var(--muted); margin-bottom: 5px; }
.fld label.chk-inline { display: flex; align-items: center; gap: 8px; font-weight: 400; color: var(--fg); margin-bottom: 0; }
.fld label.chk-inline input { width: auto; margin: 0; }
.fld .desk-ro-val {
  display: block; min-height: 38px; padding: 8px 10px; border-radius: 6px;
  border: 1px solid var(--line); background: #0d1117; color: var(--fg);
  font-family: ui-monospace, Menlo, monospace; font-size: 12px; line-height: 1.35;
  word-break: break-word; user-select: text;
}
.fld .desk-ro-note { font-size: 11px; color: #8b949e; margin: 0 0 10px; line-height: 1.45; }
.fld input {
  width: 100%; padding: 10px 12px; border: 1px solid #2a3140; border-radius: 8px; background: #080b10; color: var(--fg); font-size: var(--fs);
}
.fld input:focus { outline: none; border-color: var(--acc); box-shadow: 0 0 0 2px rgba(94,163,255,.15); }
.grid-mm { display: grid; grid-template-columns: 1fr 1fr; gap: 8px; }
.btn-row { display: flex; flex-wrap: wrap; gap: 8px; margin: 14px 0 10px; }
.btn {
  padding: 9px 14px; border-radius: 8px; border: 1px solid #30363d; background: #1a222c; color: var(--fg); font-size: var(--fs-sm); font-weight: 500; cursor: pointer;
}
.btn:hover { border-color: #4a5568; }
.btn-primary { background: var(--go); border-color: #2ea043; font-weight: 600; color: #fff; }
.btn-primary:hover { filter: brightness(1.1); }
.btn-danger {
  background: var(--danger); border-color: #ff6b6b; color: #fff; font-weight: 600;
}
.btn-danger:hover { filter: brightness(1.12); }
.mm-feed-control {
  margin-top: 14px; padding: 12px; border-radius: 10px; border: 1px solid rgba(201, 42, 42, 0.45);
  background: rgba(201, 42, 42, 0.07);
}
.side .desk-edit-entry-panel {
  margin-top: 0;
  margin-bottom: 16px;
  padding: 12px 14px;
  border-radius: 10px;
  border: 1px solid rgba(63, 185, 80, 0.38);
  background: rgba(46, 160, 67, 0.09);
}
.side .desk-edit-entry-panel .dee-title {
  font-size: 11px;
  font-weight: 700;
  letter-spacing: 0.07em;
  text-transform: uppercase;
  color: #56d364;
  margin: 0 0 8px;
}
.btn-desk-edit-green, .btn-desk-save-green {
  background: linear-gradient(180deg, #2ea043 0%, #238636 100%);
  border: 1px solid #3fb950;
  color: #fff;
  font-weight: 600;
}
.btn-desk-edit-green:hover, .btn-desk-save-green:hover {
  filter: brightness(1.08);
  border-color: #56d364;
}
.desk-edit-overlay {
  position: fixed;
  inset: 0;
  z-index: 10050;
  background: rgba(0, 10, 20, 0.62);
  backdrop-filter: blur(5px);
  -webkit-backdrop-filter: blur(5px);
  display: flex;
  align-items: center;
  justify-content: center;
  padding: 20px;
}
.desk-edit-overlay.hidden {
  display: none !important;
}
.desk-edit-overlay-card {
  max-width: min(920px, 98vw);
  width: 100%;
  max-height: 92vh;
  display: flex;
  flex-direction: column;
  background: rgba(22, 27, 34, 0.94);
  border: 1px solid rgba(63, 185, 80, 0.5);
  border-radius: 12px;
  padding: 18px 20px 16px;
  box-shadow: 0 16px 48px rgba(0, 0, 0, 0.5);
}
.desk-edit-tabnav {
  display: flex;
  flex-wrap: wrap;
  gap: 6px;
  margin: 0 0 10px;
}
.desk-edit-tabnav button {
  font-size: 11px;
  padding: 6px 10px;
  border-radius: 8px;
  border: 1px solid #30363d;
  background: #21262d;
  color: #c9d1d9;
  cursor: pointer;
  font-weight: 600;
}
.desk-edit-tabnav button:hover { border-color: #484f58; }
.desk-edit-tabnav button.active {
  border-color: #3fb950;
  background: rgba(46, 160, 67, 0.22);
  color: #e6edf3;
}
.desk-edit-tab-panel.hidden { display: none !important; }
.desk-edit-tab-panel h4 {
  margin: 12px 0 8px;
  font-size: 12px;
  font-weight: 700;
  letter-spacing: 0.05em;
  text-transform: uppercase;
  color: #8b949e;
}
.desk-edit-tab-panel .ce-toggle-wrap {
  display: flex;
  align-items: center;
  gap: 10px;
  margin-top: 4px;
  flex-wrap: wrap;
}
.desk-edit-tab-panel .ce-tog-lab {
  font-size: 11px;
  color: #6e7681;
  min-width: 58px;
  user-select: none;
}
.desk-edit-tab-panel .ce-tog-lab-right { text-align: right; min-width: 58px; }
.desk-edit-tab-panel .ce-toggle {
  position: relative;
  display: inline-block;
  width: 44px;
  height: 24px;
  flex-shrink: 0;
}
.desk-edit-tab-panel .ce-toggle input.ce-bool-cb {
  opacity: 0;
  width: 0;
  height: 0;
  position: absolute;
  margin: 0;
}
.desk-edit-tab-panel .ce-toggle .ce-toggle-slider {
  position: absolute;
  cursor: pointer;
  inset: 0;
  background: #30363d;
  border-radius: 999px;
  transition: background 0.2s;
  border: 1px solid #484f58;
}
.desk-edit-tab-panel .ce-toggle .ce-toggle-slider:before {
  position: absolute;
  content: "";
  height: 18px;
  width: 18px;
  left: 2px;
  bottom: 2px;
  background: #8b949e;
  border-radius: 50%;
  transition: transform 0.2s, background 0.2s;
}
/* Unchecked = thumb left = Disabled; checked = thumb right = Enabled */
.desk-edit-tab-panel .ce-toggle input.ce-bool-cb:checked + .ce-toggle-slider {
  background: rgba(46, 160, 67, 0.35);
  border-color: #3fb950;
}
.desk-edit-tab-panel .ce-toggle input.ce-bool-cb:checked + .ce-toggle-slider:before {
  transform: translateX(20px);
  background: #3fb950;
}
.desk-edit-tab-panel .ce-toggle input.ce-bool-cb:focus-visible + .ce-toggle-slider {
  outline: 2px solid #58a6ff;
  outline-offset: 2px;
}
.desk-edit-tab-panel textarea.ce-json {
  width: 100%;
  min-height: 120px;
  font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
  font-size: 11px;
  line-height: 1.35;
  padding: 8px 10px;
  border-radius: 8px;
  border: 1px solid #30363d;
  background: #0d1117;
  color: #e6edf3;
  resize: vertical;
}
.desk-edit-modal-scroll {
  flex: 1 1 auto;
  min-height: 0;
  max-height: min(72vh, 640px);
  overflow-y: auto;
  margin: 0 -4px 0 0;
  padding-right: 6px;
}
.desk-edit-modal-scroll .fld {
  margin-bottom: 9px;
}
.desk-edit-modal-scroll .fld label {
  font-size: 11px;
}
.desk-edit-modal-scroll .grid-mm {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 8px;
}
.desk-edit-docs-inner dt {
  font-weight: 600;
  color: #e6edf3;
  margin-top: 8px;
  font-size: 11px;
}
.desk-edit-docs-inner dd {
  margin: 2px 0 0 0;
  padding-left: 0;
  color: #9da7b8;
}
.desk-edit-docs-inner h5 {
  margin: 12px 0 6px;
  font-size: 11px;
  font-weight: 700;
  letter-spacing: 0.06em;
  text-transform: uppercase;
  color: #8b949e;
}
.desk-edit-modal-actions {
  display: flex;
  flex-direction: column;
  gap: 8px;
  margin-top: 12px;
  flex-shrink: 0;
}
.btn-desk-edit-muted {
  background: #21262d;
  border: 1px solid #30363d;
  color: #c9d1d9;
  font-weight: 500;
}
.btn-desk-edit-muted:hover {
  border-color: #484f58;
}
.desk-edit-toast {
  position: fixed;
  bottom: 22px;
  right: 22px;
  z-index: 10060;
  max-width: 380px;
  padding: 14px 18px;
  font-size: 13px;
  line-height: 1.45;
  color: #e6edf3;
  background: rgba(22, 27, 34, 0.96);
  border: 1px solid #3fb950;
  border-radius: 10px;
  box-shadow: 0 10px 32px rgba(0, 0, 0, 0.45);
}
.desk-edit-toast.hidden {
  display: none !important;
}
.mm-feed-control .mmc-title {
  font-size: 11px; font-weight: 700; letter-spacing: 0.06em; text-transform: uppercase;
  color: #ff8a8a; margin: 0 0 8px;
}
.mm-feed-control .mmc-sub {
  font-size: 11px; font-weight: 600; color: #c9d1d9; margin: 0 0 6px;
}
.btn-mm-theo-disable {
  background: #9b2335; border-color: #c92a2a; color: #fff; font-weight: 600; width: 100%; margin-top: 6px;
}
.btn-mm-theo-enable {
  background: #1a472a; border-color: #2ea043; color: #fff; font-weight: 600; width: 100%; margin-top: 6px;
}
.btn-mm-orders-pause {
  background: #8b2942; border-color: #c92a2a; color: #fff; font-weight: 600; width: 100%; margin-top: 6px;
}
.btn-mm-orders-resume {
  background: #1a472a; border-color: #2ea043; color: #fff; font-weight: 600; width: 100%; margin-top: 6px;
}
.desk-action { font-size: 12px; color: #c5d0e0; margin-top: 10px; line-height: 1.45; min-height: 2.8em; }
details.adv { margin-top: 12px; font-size: 12px; color: var(--muted); }
details.adv summary { cursor: pointer; font-weight: 500; color: var(--fg); }
.pos { font-weight: 600; font-size: var(--fs-lg); margin-bottom: 6px; line-height: 1.35; }
.stat { font-size: var(--fs-sm); color: var(--muted); line-height: 1.45; margin-bottom: 12px; }
.status { font-size: var(--fs-sm); color: var(--muted); line-height: 1.45; margin-top: 10px; padding-top: 10px; border-top: 1px solid var(--line); }
.lat { font-size: var(--fs-sm); color: #6b7788; margin-top: 6px; font-family: ui-monospace, Menlo, monospace; }
.out { font-size: var(--fs-sm); color: var(--muted); margin-top: 8px; word-break: break-word; }
.card { background: var(--card); border: 1px solid var(--line); border-radius: 10px; overflow: hidden; margin-bottom: 10px; }
.card-h { padding: 10px 14px; font-size: 12px; font-weight: 700; text-transform: uppercase; letter-spacing: 0.06em; color: var(--muted); border-bottom: 1px solid var(--line); }
.card-h span { color: var(--acc); font-weight: 600; margin-left: 6px; }
.card-h .ob-instrument {
  display: inline-block; margin: 0 8px 0 6px; padding: 2px 8px; border-radius: 6px;
  background: rgba(94,163,255,.12); border: 1px solid rgba(94,163,255,.28);
  color: #9ec5ff; font-weight: 700; font-size: 11px; text-transform: none;
  letter-spacing: 0.02em; font-family: ui-monospace, Menlo, monospace;
}
.card-h .hdr-hint { color: #6b7788; font-weight: 500; font-size: 10px; text-transform: none; letter-spacing: 0; margin-left: 8px; }
.card-b { padding: 0; max-height: 360px; overflow: auto; }
.tbl-wrap { max-height: 280px; overflow: auto; }
.activity-half { min-width: 0; }
.tbl-wrap--activity { max-height: min(58vh, 560px); }
table.data { width: 100%; border-collapse: collapse; font-size: var(--fs-sm); font-variant-numeric: tabular-nums; }
.data th { text-align: left; padding: 8px 12px; color: #6b7788; font-weight: 600; font-size: 11px; text-transform: uppercase; letter-spacing: 0.05em; position: sticky; top: 0; background: var(--card); }
.data td { padding: 8px 12px; border-top: 1px solid var(--line); }
.bid { color: var(--bid); font-weight: 600; } .ask { color: var(--ask); font-weight: 600; } .mine { color: var(--mine); font-weight: 700; }
/* Depth ladders: keep bid/ask price colors fixed (no px-up/px-down on non-mine cells — those classes are mine-only for flash). */
.depth-card td.bid.ob-px:not(.mine-ax),
.micro.ax-book-card td.bid.ob-px:not(.mine-ax) { color: var(--bid) !important; }
.depth-card td.ask.ob-px:not(.mine-ax),
.micro.ax-book-card td.ask.ob-px:not(.mine-ax) { color: var(--ask) !important; }
.hint { display: none; font-size: var(--fs-sm); color: #5c6573; padding: 14px 16px; }
.ax-all-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(220px, 1fr)); gap: 10px; padding: 10px; }
.micro { max-height: 320px; overflow: auto; }
.micro table.data { font-size: 10px; }
.micro .card-h { font-size: 9px; }
.trading-main { max-width: min(1760px, 100%); margin: 0 auto; padding: 10px 16px 24px; }
.desk-section { margin-bottom: 24px; }
.desk-section-depth { margin-bottom: 36px; padding: 8px 0 12px; }
.desk-section-depth .sec-title { margin: 0 0 16px 4px; letter-spacing: 0.1em; }
.desk-section-depth .depth-cols { gap: 20px; margin-top: 4px; }
.sec-title {
  font-size: 12px; font-weight: 700; letter-spacing: 0.08em; text-transform: uppercase;
  color: var(--muted); margin: 0 0 8px 3px;
}
.feed-tag {
  font-size: 10px; font-weight: 700; letter-spacing: 0.04em;
  color: #7dd3a0; background: rgba(62,207,142,.12); border: 1px solid rgba(62,207,142,.35);
  padding: 3px 8px; border-radius: 6px; margin-left: 8px; vertical-align: middle;
}
.feed-tag.rest {
  color: #9aa7b8; background: rgba(139,156,179,.1); border-color: rgba(139,156,179,.25);
}
/* Top-of-sidebar feed-health pill row: CME / Hyperliquid / Neon. */
.feed-pill-row {
  display: flex; gap: 6px; margin: 4px 0 12px 0; flex-wrap: wrap;
}
.feed-pill {
  display: inline-flex; align-items: center; gap: 6px;
  font-size: 11px; font-weight: 700; letter-spacing: 0.03em;
  padding: 4px 9px; border-radius: 999px;
  border: 1px solid rgba(139,156,179,.28);
  background: rgba(139,156,179,.08); color: #9aa7b8;
  cursor: default; user-select: none;
}
.feed-pill .feed-dot {
  width: 8px; height: 8px; border-radius: 50%;
  background: #6b7788; box-shadow: 0 0 0 1px rgba(0,0,0,0.18) inset;
}
.feed-pill.up    { color: #7dd3a0; background: rgba(62,207,142,.12);  border-color: rgba(62,207,142,.45); }
.feed-pill.up    .feed-dot { background: #3ecf8e; box-shadow: 0 0 6px rgba(62,207,142,.55); }
.feed-pill.down  { color: #ff8585; background: rgba(255,80,80,.12);   border-color: rgba(255,80,80,.5); }
.feed-pill.down  .feed-dot { background: #ff5050; box-shadow: 0 0 6px rgba(255,80,80,.6); }
.feed-pill.stale { opacity: 0.55; }
.depth-col { min-width: 0; }
.depth-cols {
  display: grid;
  /* Narrower metrics strip; extra width flows to Architect (or Neon + Architect when ref on) */
  grid-template-columns: minmax(176px, 16rem) minmax(0, 1fr);
  gap: 18px; align-items: start;
}
.depth-ref-books-col .card.depth-card,
.depth-ax-books-col .card.depth-card { width: 100%; box-sizing: border-box; }
/* Reference column narrower; Architect orderbooks get more horizontal space */
.depth-cols.depth-cols--with-ref {
  grid-template-columns: minmax(176px, 16rem) minmax(200px, 0.68fr) minmax(280px, 1.32fr);
}
@media (max-width: 1200px) {
  .depth-cols.depth-cols--with-ref {
    grid-template-columns: 1fr;
  }
}
@media (max-width: 900px) {
  .depth-cols { grid-template-columns: 1fr; }
}
.depth-metrics-card .card-h { font-size: 10px; }
.depth-metrics-inner { padding: 4px 12px 12px; font-size: 12px; max-height: min(72vh, 640px); overflow-y: auto; }
.depth-metric-block { border-bottom: 1px solid var(--line); padding: 12px 0; }
.depth-metric-block:last-child { border-bottom: none; }
.depth-metric-block .sym { font-weight: 700; color: var(--acc); margin-bottom: 8px; font-size: 12px; }
.metric-row { display: flex; justify-content: space-between; align-items: baseline; gap: 10px; margin: 5px 0; font-variant-numeric: tabular-nums; }
.metric-row .k { color: var(--muted); font-size: 11px; flex-shrink: 0; }
.metric-row .v { text-align: right; font-size: 12px; }
.docs-page { max-width: 820px; margin: 0 auto; padding: 28px 24px 56px; }
.docs-page h2 { margin: 0 0 8px; font-size: 1.65rem; }
.docs-page h3 { margin: 28px 0 10px; font-size: 1.05rem; color: var(--acc); font-weight: 600; }
.docs-page p, .docs-page li { line-height: 1.6; color: #c5d0e0; }
.docs-page ul { padding-left: 1.25rem; margin: 8px 0; }
.docs-page code { font-size: 0.88em; background: #1a222c; padding: 2px 6px; border-radius: 4px; border: 1px solid var(--line); }
.docs-callout {
  margin: 16px 0; padding: 14px 16px; border-radius: 10px; border: 1px solid rgba(94,163,255,0.25);
  background: rgba(94, 163, 255, 0.07); font-size: 13px; color: #b8cce8;
}
.desk-banner-warn {
  margin: 0 0 14px 4px; padding: 10px 14px; border-radius: 8px;
  border: 1px solid rgba(234, 197, 79, 0.42); background: rgba(234, 197, 79, 0.07);
  color: #e8dcc4; font-size: 12px; line-height: 1.45;
}
.desk-banner-warn strong { color: #ffe082; font-weight: 600; }
/* Two order books per row; extra products scroll inside .ax-books-scroll-host */
.ax-books-scroll-host {
  max-height: min(calc(100vh - 200px), 900px);
  overflow-y: auto;
  overflow-x: hidden;
  overscroll-behavior: contain;
  -webkit-overflow-scrolling: touch;
}
.ax-books-scroll-host::-webkit-scrollbar { width: 8px; }
.ax-books-scroll-host::-webkit-scrollbar-track { background: rgba(0,0,0,.2); border-radius: 4px; }
.ax-books-scroll-host::-webkit-scrollbar-thumb { background: rgba(255,255,255,.16); border-radius: 4px; }
/* Architect: two order books per row */
.depth-card .ax-all-grid.ax-book-pair-grid {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 12px;
  padding: 12px;
  align-content: start;
}
/* Reference / Neon: one book per row full width (pair grid left half empty when only one symbol) */
.depth-card .ax-all-grid.ref-depth-grid {
  display: grid;
  grid-template-columns: minmax(0, 1fr);
  gap: 12px;
  padding: 12px;
  align-content: start;
}
.depth-card .ax-all-grid { gap: 12px; padding: 12px; }
.depth-card .card-h { font-size: 11px; }
.ax-book-hdr { display: flex; align-items: center; justify-content: space-between; gap: 8px; flex-wrap: wrap; }
.ax-book-hdr > span:first-child { font-weight: 700; min-width: 0; }
.ax-book-hide-btn {
  flex-shrink: 0;
  font-size: 10px;
  font-weight: 600;
  letter-spacing: 0.04em;
  text-transform: uppercase;
  padding: 3px 8px;
  border-radius: 6px;
  border: 1px solid var(--line);
  background: #161b22;
  color: var(--muted);
  cursor: pointer;
}
.ax-book-hide-btn:hover { color: #ffb4b4; border-color: rgba(255, 107, 107, 0.45); }
.ax-depth-restore-btn { margin: 8px 12px 0; font-size: 11px; padding: 5px 12px; }
.micro.ax-book-card { max-height: none; overflow: visible; }
/* Ladder matches a ticked row in Architect instruments multiselect (or server depth list if none ticked yet). */
.micro.ax-book-card.ax-book-card--selected {
  outline: 2px solid rgba(110, 203, 140, 0.55);
  outline-offset: 1px;
  box-shadow: inset 0 0 0 1px rgba(46, 160, 67, 0.28);
}
.micro.ax-book-card table.data { font-size: 11px; }
.micro.ax-book-card .data th { padding: 6px 8px; font-size: 10px; }
.micro.ax-book-card .data td { padding: 4px 8px; line-height: 1.35; }
/* Full ladder visible per card (20 levels); outer .ax-books-scroll-host scrolls extra products */
.depth-book-table-wrap {
  max-height: none;
  overflow: visible;
}
.depth-book-table-wrap table.data { width: 100%; margin: 0; }
.mm-preview-card { margin-bottom: 8px; }
.mm-preview-card table.data { margin: 0; }
/* MM guide tables: span full card width; give Note column the slack so text doesn’t wrap in a narrow strip */
#mmPreviewTable.data,
.neon-mm-preview-table.data {
  width: 100%;
  max-width: none;
  table-layout: fixed;
}
#mmPreviewTable.data thead th:nth-child(1),
#mmPreviewTable.data tbody td:nth-child(1),
.neon-mm-preview-table.data thead th:nth-child(1),
.neon-mm-preview-table.data tbody td:nth-child(1) { width: 12%; }
#mmPreviewTable.data thead th:nth-child(2),
#mmPreviewTable.data tbody td:nth-child(2),
.neon-mm-preview-table.data thead th:nth-child(2),
.neon-mm-preview-table.data tbody td:nth-child(2) { width: 22%; }
#mmPreviewTable.data thead th:nth-child(3),
#mmPreviewTable.data tbody td:nth-child(3),
.neon-mm-preview-table.data thead th:nth-child(3),
.neon-mm-preview-table.data tbody td:nth-child(3) { width: 16%; }
#mmPreviewTable.data thead th:nth-child(4),
#mmPreviewTable.data tbody td:nth-child(4),
.neon-mm-preview-table.data thead th:nth-child(4),
.neon-mm-preview-table.data tbody td:nth-child(4) { width: 50%; text-align: left; }
.neon-tob-strip { width: 100%; box-sizing: border-box; padding: 10px 12px 12px; font-size: 13px; font-variant-numeric: tabular-nums; }
.neon-tob-row { display: flex; align-items: baseline; gap: 10px; margin: 6px 0; flex-wrap: wrap; }
.neon-tob-row .k { color: var(--muted); font-size: 11px; min-width: 2.2rem; }
.neon-tob-row .v { font-weight: 600; }
.neon-tob-row .v.bid { color: #7dd3a0; }
.neon-tob-row .v.ask { color: #ff9b9b; }
.neon-tob-row .q { color: var(--muted); font-size: 11px; }
.neon-mm-preview-slot { margin-top: 10px; border-top: 1px solid var(--line); padding-top: 10px; width: 100%; min-width: 0; box-sizing: border-box; }
.neon-mm-preview-title { font-size: 11px; font-weight: 600; color: var(--muted); text-transform: uppercase; letter-spacing: 0.04em; margin-bottom: 8px; }
.neon-mm-preview-table { font-size: 12px; }
.neon-mm-preview-meta { font-size: 10px !important; padding: 8px 0 0 !important; margin: 0 !important; border-top: none !important; }
.ob-px { transition: background-color 0.35s ease, box-shadow 0.35s ease; }
@keyframes mine-flash {
  0% { background-color: rgba(255,235,59,0.5); }
  100% { background-color: transparent; }
}
td.mine-ax {
  background: linear-gradient(90deg, rgba(255,235,59,0.42), rgba(255,235,59,0.1)) !important;
  box-shadow: inset 4px 0 0 #ffeb3b;
  font-weight: 700;
  color: #fff8e1 !important;
}
/* Architect multi-card grid: keep yellow “my order” strip visible over bid/ask color rules. */
.micro.ax-book-card td.bid.ob-px.mine-ax,
.micro.ax-book-card td.ask.ob-px.mine-ax {
  background: linear-gradient(90deg, rgba(255,235,59,0.48), rgba(255,235,59,0.14)) !important;
  box-shadow: inset 4px 0 0 #ffeb3b !important;
}
td.bid.mine-ax.px-up, td.ask.mine-ax.px-up,
td.bid.mine-ax.px-down, td.ask.mine-ax.px-down {
  animation: mine-flash 0.7s ease;
}
td.bid.mine-ax.px-up { color: #fffde7 !important; text-shadow: 0 0 12px rgba(255,235,59,0.65); }
td.bid.mine-ax.px-down { color: #ffe082 !important; text-shadow: 0 0 10px rgba(255,193,7,0.45); }
td.ask.mine-ax.px-up { color: #fffde7 !important; text-shadow: 0 0 12px rgba(255,235,59,0.65); }
td.ask.mine-ax.px-down { color: #ffe082 !important; text-shadow: 0 0 10px rgba(255,193,7,0.45); }
.activity-cols { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; align-items: start; }
@media (max-width: 1100px) { .activity-cols { grid-template-columns: 1fr; } }
.tabnav-wrap {
  display: flex; flex-wrap: wrap; align-items: center; justify-content: space-between; gap: 8px;
  padding: 12px 28px; background: #0a0e14; border-bottom: 1px solid var(--line);
}
.tabnav-center {
  flex: 1; display: flex; justify-content: center; align-items: center; flex-wrap: wrap; gap: 10px;
  min-width: 0;
}
.tabnav-center button {
  padding: 9px 20px; border-radius: 8px; border: 1px solid var(--line); background: #121820; color: var(--muted);
  font-size: var(--fs-sm); font-weight: 600; cursor: pointer;
}
.tabnav-center button:hover { border-color: #4a5568; color: var(--fg); }
.tabnav-center button.active { border-color: rgba(94,163,255,.45); background: rgba(94,163,255,.12); color: var(--acc); }
.tab-panel.hidden { display: none !important; }
.trading-layout { flex: 1; display: grid; grid-template-columns: 1fr 372px; min-height: 0; }
.tab-panel.trading-layout.hidden { display: none !important; }
@media (max-width: 1080px) { .trading-layout { grid-template-columns: 1fr; } }
#bootBanner {
  display: none; margin: 0 18px 12px; padding: 10px 14px; border-radius: 8px; border: 1px solid rgba(255,107,107,.4);
  background: rgba(255,107,107,.1); color: #ffb4b4; font-size: 13px;
}
.panel-page { padding: 20px 22px 40px; max-width: 960px; }
.panel-page h2 { margin: 0 0 10px; font-size: 22px; font-weight: 600; }
.muted { color: var(--muted); line-height: 1.5; }
.mm-desk-wire { font-size: 11px; color: #8ba3b8; padding: 6px 32px; background: #080d14; border-bottom: 1px solid var(--line); font-family: ui-monospace, Menlo, monospace; word-break: break-all; }
.mm-desk-api-err {
  margin: 0 18px 8px; padding: 8px 12px; border-radius: 8px; border: 1px solid rgba(255,107,107,.55);
  background: rgba(255,60,60,.12); color: #ffb4b4; font-size: 12px; font-family: ui-monospace, Menlo, monospace;
}
</style></head><body>
<noscript><div style="padding:24px;background:#4a1515;color:#fff;font-family:sans-serif">JavaScript must be enabled for MM Live Desk.</div></noscript>
<div id="loadOverlay"><div class="load-card"><div class="spinner"></div>Loading desk…</div></div>
<script>
(function(){
  window.mmDeskDismissBoot=function(){
    var o=document.getElementById("loadOverlay");
    if(o){
      o.classList.add("mm-desk-overlay-done");
      o.style.display="none";
      o.setAttribute("aria-hidden","true");
    }
  };
  setTimeout(window.mmDeskDismissBoot,4000);
})();
</script>
<div id="app">
<header class="bar">
<span class="logo">MM Live Desk</span>
__HEADER_REF_PILL__
<span class="pill" id="pillAx">Architect</span>
<div class="bar-tail">
<div class="mm-desk-leg-wrap" id="theoLegMultiWrap" title="Master MM product list (market_maker.instruments). Matches persist/cancel/gate prompts. Architect depth columns are chosen separately (sidebar multiselect + mm_desk.ax_orderbook_symbols).">
<span class="mm-desk-leg-lab" id="theoLegMultiLab">MM legs</span>
<select id="theoLegViewSelect" aria-label="Active MM instrument"></select>
</div>
<span class="meta" id="topMeta"></span>
</div>
</header>
<div id="mmDeskWire" class="mm-desk-wire">Starting wire check…</div>
<script>
(function(){
  function refreshDeskWire(){
    var el = document.getElementById("mmDeskWire");
    if (!el) return;
    var ctrl = new AbortController();
    var tid = setTimeout(function(){ try { ctrl.abort(); } catch (e) {} }, 12000);
    fetch((window.location.origin || "") + "/api/desk_meta", { cache: "no-store", signal: ctrl.signal })
      .finally(function() { clearTimeout(tid); })
      .then(function(r) {
        if (!r.ok) throw new Error("HTTP " + r.status);
        return r.json();
      })
      .then(function(d) {
        el = document.getElementById("mmDeskWire");
        if (!el || !d) return;
        if (!d.ok) {
          el.style.color = "#ff8a8a";
          el.textContent = "Wire: bad meta — " + JSON.stringify(d);
          return;
        }
        el.style.color = "#9ec5b8";
        el.textContent =
          "Desk connected · " + String(d.ax_symbol || "—") + " / " + String(d.ref_symbol || "—") +
          " · feeds " + (d.feeds_running ? "on" : "off") +
          (d.default_config_exists ? "" : " · NO default_config") +
          (d.credentials_exists ? "" : " · NO credentials file");
      })
      .catch(function(e) {
        el = document.getElementById("mmDeskWire");
        if (!el) return;
        el.style.color = "#ff8a8a";
        var msg = (e && e.name === "AbortError") ? "request timed out (12s)" : String((e && e.message) || e);
        el.textContent = "Wire FAILED — " + msg;
      });
  }
  window.mmDeskRefreshWire = refreshDeskWire;
  refreshDeskWire();
  setInterval(refreshDeskWire, 4000);
})();
</script>
<div class="tabnav-wrap" id="tabnav">
<div class="tabnav-center">
<button type="button" class="active" data-tab="trading" id="tabBtnTrading">Trading</button>
<button type="button" data-tab="docs" id="tabBtnDocs">Documentation</button>
</div>
</div>
<div id="bootBanner" role="alert"></div>
<div id="mmDeskApiErr" class="mm-desk-api-err" style="display:none" role="alert"></div>
<div id="deskTabPanels">
<div id="panel-trading" class="tab-panel trading-layout">
<main class="trading-main">
<section class="desk-section desk-section-depth">
<h3 class="sec-title">Live depth</h3>
<p class="desk-banner-warn" role="note"><strong>Yellow</strong> = best-effort match to your open orders on this poll. <strong>MM guide</strong> = intent math, not live acks.</p>
<div class="depth-cols" id="depthColsGrid">
<div class="depth-col depth-metrics-col">
<div class="card depth-card depth-metrics-card"><div class="card-h">Book metrics <span class="hdr-hint">mid · spread · pressure (AX top 20)</span></div><div id="depthMetrics" class="depth-metrics-inner"></div></div>
</div>
__REF_DEPTH_COL_BLOCK__
<div class="depth-col depth-ax-books-col">
<div class="card depth-card"><div class="card-h">Architect <span class="ob-instrument" id="axDepthInstrument">—</span> <span class="feed-tag rest" id="axDepthTag">REST</span> <span id="axMarketsMeta"></span></div><div id="axAllWrap"><button type="button" class="btn ax-depth-restore-btn" id="axDepthShowAllBtn" style="display:none" title="Clears this browser’s hidden-ladder list (does not change config or stop server polling)">Show all hidden ladders</button><p class="hint" id="hintAxAll" style="display:block;padding:12px">Waiting for Architect /book…</p><div class="ax-books-scroll-host"><div class="ax-all-grid ax-book-pair-grid" id="axAllGrid"></div></div></div></div>
</div>
</div>
</section>
<section class="desk-section" id="mmPreviewSection">
<h3 class="sec-title">MM guide</h3>
<p class="muted" id="mmPreviewSectionBlurb" style="font-size:12px;margin:-6px 0 12px 4px;line-height:1.45">Theo + sidebar rules (width, basis, skew, position)—same intent as C++. Not live exchange orders.</p>
<div class="card mm-preview-card" id="mmPreviewMainBlock"><div class="card-h">Prices (guide)</div>
<div id="mmPositionGateBanner" class="mm-gate-banner" style="display:none;margin:0;padding:10px 14px;font-size:12px;line-height:1.45;color:#ffd4d4;background:#3a1a1a;border-bottom:1px solid #5c2020"></div>
<table class="data" id="mmPreviewTable"><thead><tr><th>Side</th><th>Price</th><th>Qty</th><th>Note</th></tr></thead><tbody id="mmPreviewBody"></tbody></table>
<p class="muted" id="mmPreviewMeta" style="font-size:11px;padding:10px 14px 12px;margin:0;border-top:1px solid var(--line)"></p>
</div>
<p class="muted" id="mmPreviewNeonNote" style="display:none;font-size:11px;margin:6px 0 0 4px;line-height:1.4">Neon (C++): same note under <strong>reference</strong>.</p>
</section>
<section class="desk-section">
<h3 class="sec-title">Activity</h3>
<div class="activity-cols">
<div class="card activity-half"><div class="card-h">Fills</div><div class="tbl-wrap tbl-wrap--activity"><table class="data" id="tf"></table></div></div>
<div class="card activity-half"><div class="card-h">Orders</div><div class="tbl-wrap tbl-wrap--activity"><table class="data" id="to"></table></div></div>
</div>
</section>
</main>
<aside class="side">
<div id="deskSideGhostRo" style="position:absolute;left:-9999px;top:0;width:1px;height:1px;overflow:hidden;clip-path:inset(50%)" aria-hidden="true">
<div class="desk-ro-val" id="axSym"></div>
<div class="desk-ro-val" id="refSym"></div>
<div class="desk-ro-val" id="apiKey"></div>
<div class="desk-ro-val" id="apiSec"></div>
<div class="desk-ro-val" id="w"></div>
<div class="desk-ro-val" id="q"></div>
<div class="desk-ro-val" id="mx"></div>
<div class="desk-ro-val" id="ap"></div>
<div class="desk-ro-val" id="at"></div>
<div class="desk-ro-val" id="mmRequoteTheo"></div>
<div class="desk-ro-val" id="mmMinTheoTicks"></div>
<div class="desk-ro-val" id="mmPriceTick"></div>
<div class="desk-ro-val" id="mmPricingTick"></div>
<div class="desk-ro-val" id="mmCurReload"></div>
<div class="desk-ro-val" id="mrc"></div>
<div class="desk-ro-val" id="mrcFillOnly"></div>
<div class="desk-ro-val" id="mmReloadNonce"></div>
<div class="desk-ro-val" id="rest"></div>
<p class="sub" id="feedNote" style="display:none"></p>
</div>
<div class="fld" id="feedHealthBlock">
<label>External feeds <span class="muted" style="font-weight:400">(theo / leg)</span></label>
<div class="feed-pill-row" id="feedPillRow" role="status" aria-label="External feed health">
<span class="feed-pill" id="feedPillMettraders"  title="Mettraders websocket feed"><span class="feed-dot"></span>Mettraders</span>
<span class="feed-pill" id="feedPillHl"   title="Hyperliquid allMids + xyz HIP-3 (FX, metals, energy, indices)"><span class="feed-dot"></span>Hyperliquid</span>
<span class="feed-pill" id="feedPillNeon" title="Neon FIX (USD/MXN)"><span class="feed-dot"></span>Neon</span>
</div>
</div>
<div class="fld" id="manualOrderQuickBlock">
<button type="button" class="btn btn-primary" id="btnAddManualOrder" style="width:100%;padding:10px" title="Manual stack → POST /api/desk/place_order">Place manual order</button>
<button type="button" class="btn" id="btnTemplates" style="width:100%;margin-top:6px" title="Open saved order templates (templates.json) → edit & place">Templates</button>
<button type="button" class="btn btn-danger" id="btnCancelOrder" style="width:100%;margin-top:6px;background:#b91c1c;border-color:#7f1d1d;color:#fff" title="Remove one stack from orders.json (venue cancel first)">Cancel Order</button>
<button type="button" class="btn" id="btnOrdersReset" style="width:100%;margin-top:6px" title="POST /api/desk/orders_reset — cancel all stack OIDs from current orders.json then wipe file">Clear all stacks (clean slate)</button>
</div>
<div class="fld ax-ob-multi-wrap">
<label for="axOrderbookMulti">AX depth <span class="muted" style="font-weight:400">(ladders · leg menus · /instruments)</span></label>
<p class="muted" id="axOrderbookMmLegsNote" style="font-size:11px;margin:4px 0 6px;line-height:1.5;color:#e8dcc4;border-left:3px solid rgba(234,197,79,.45);padding-left:10px"></p>
<select id="axOrderbookMulti" multiple size="10" aria-label="AX depth products"></select>
<p class="muted" id="axOrderbookMultiSummary" style="font-size:11px;margin:6px 0 4px;line-height:1.45">—</p>
<p class="muted" style="font-size:11px;margin:0 0 6px;line-height:1.4">Tick rows → <strong>Apply</strong> saves <code>mm_desk.ax_orderbook_symbols</code>. Green = ticked. Double‑click ladder toggles. Cmd/Ctrl‑click, Shift range. Untick all + Apply → MM legs only.</p>
<button type="button" class="btn btn-primary" id="btnAxOrderbookApply" style="width:100%" title="Save multiselect to config and reload">Apply instruments · reload</button>
</div>
<div class="fld neon-md-multi-wrap" id="neonMdWrap" style="display:none">
<label for="neonMdMulti">Neon FIX <span class="muted" style="font-weight:400">(55 · md_symbols)</span></label>
<select id="neonMdMulti" multiple size="8" aria-label="Neon FIX md_symbols"></select>
<p class="muted" style="font-size:11px;margin:6px 0 6px;line-height:1.4">Saves <code>external_feed.fix.md_symbols</code> (reference column). Python Neon: <code>MM_LIVE_DESK_WEB_REFERENCE_FEED=1</code> + Start. Else depth from C++ <code>mm_external_depth.json</code>. Details → Documentation.</p>
<p class="muted" id="neonMdCatalogNote" style="font-size:11px;margin:0 0 8px;display:none;line-height:1.45"></p>
<p class="muted" id="neonSlHint" style="font-size:10px;margin:0 0 8px;display:none;line-height:1.4"></p>
<button type="button" class="btn btn-primary" id="btnNeonMdApply" style="width:100%" title="Persist md_symbols and reload">Apply Neon instruments · reload</button>
</div>
<details class="adv" style="margin-top:8px"><summary>MM reload limit (C++ market maker)</summary>
<div class="fld"><label>Current reload count <span class="muted" style="font-weight:400">integer 0–1,000,000</span></label><div class="desk-ro-val" id="mmCurReload"></div></div>
<div class="fld"><label>Max reload cycles <span class="muted" style="font-weight:400">integer 0–1,000,000 · 0 = unlimited</span></label><div class="desk-ro-val" id="mrc"></div></div>
<div class="fld"><label>Reload cycles (fill-only)</label><div class="desk-ro-val" id="mrcFillOnly"></div></div>
<div class="fld"><label>Reload limit reset nonce</label><div class="desk-ro-val" id="mmReloadNonce"></div></div>
<button type="button" class="btn" id="btnMmReloadBump" style="width:100%;margin-top:4px" title="Writes new reload_limit_reset_nonce; C++ can clear reload halt">Clear reload halt (new nonce)</button>
</details>
<div class="mm-feed-control">
<p class="mmc-title">MM placement</p>
<p class="mmc-sub"><code>mm_orders_enabled</code> (config) · <code>mm_move_*</code> (<code>orders.json</code>)</p>
<button type="button" class="btn btn-mm-orders-pause" id="btnMmOrdersGate" title="Pause/resume theo-driven MM moves per stack via orders.json (POST /api/desk/orders_mm_move). Config master: POST /api/desk/mm_gate.">Pause/Resume MM placement</button>
</div>
<div class="mm-feed-control" id="instrHoldPanel">
<p class="mmc-title">Cancel all &amp; stop instrument</p>
<p class="mmc-sub">Cancels every open order on one instrument (account-wide, incl. hand-placed) and stops it quoting.</p>
<select id="instrHoldSelect" aria-label="Instrument to cancel" style="width:100%;margin-bottom:6px"><option value="">— select instrument —</option></select>
<button type="button" class="btn btn-danger" id="btnInstrCancelHold" style="width:100%;background:#b91c1c;border-color:#7f1d1d;color:#fff" title="POST /api/desk/instrument_cancel_all — cancel every open order on this instrument (account-wide) and stop it quoting">Cancel all orders</button>
<div class="out" id="instrHoldOut" style="margin-top:6px"></div>
</div>
<div class="mm-feed-control" id="fastMktPanel">
<p class="mmc-title">Fast-market breaker</p>
<p class="mmc-sub">When the SPX symbol moves N ticks within T seconds, pull ALL orders on every instrument for P seconds, then resume. Global.</p>
<div id="fastMktView" style="font-size:12px;line-height:1.7">
<div>Status: <strong id="fmEnabledView">—</strong></div>
<div>Symbol: <strong id="fmSymbolView">—</strong></div>
<div>Move size: <strong id="fmSizeView">—</strong> ticks</div>
<div>Within: <strong id="fmWindowView">—</strong> s</div>
<div>Pull for: <strong id="fmPullView">—</strong> s</div>
<div>Tick size: <strong id="fmTickView">—</strong></div>
<button type="button" class="btn" id="btnFastMktEdit" style="width:100%;margin-top:8px" title="Edit market_maker.fast_market.* (writes via the hardened mutate-write)">Edit</button>
</div>
<div id="fastMktEdit" style="display:none;font-size:12px">
<label style="display:flex;align-items:center;gap:6px;margin-bottom:6px"><input type="checkbox" id="fmEnabled"> Enabled <span style="color:#8a8a8a">(off keeps the symbol)</span></label>
<input id="fmSymbol" placeholder="SPX symbol (e.g. HL SPX)" style="width:100%;margin-bottom:4px">
<input id="fmSize" type="number" min="1" step="1" placeholder="Move size (ticks)" style="width:100%;margin-bottom:4px">
<input id="fmWindow" type="number" min="1" step="1" placeholder="Within (seconds)" style="width:100%;margin-bottom:4px">
<input id="fmPull" type="number" min="1" step="1" placeholder="Pull duration (seconds)" style="width:100%;margin-bottom:4px">
<input id="fmTick" type="number" min="0" step="any" placeholder="Tick size" style="width:100%;margin-bottom:6px">
<div style="display:flex;gap:6px">
<button type="button" class="btn" id="btnFastMktCancel" style="flex:1">Cancel</button>
<button type="button" class="btn btn-primary" id="btnFastMktSave" style="flex:1" title="POST /api/desk/fast_market_save">Save</button>
</div>
</div>
<div class="out" id="fastMktOut" style="margin-top:6px"></div>
</div>
<p class="desk-action" id="deskActionLine"></p>
<div class="pos" id="pos"></div>
<div class="stat" id="stats"></div>
<div class="status" id="statusLine"></div>
<div class="lat" id="latLine"></div>
<div class="out" id="applyOut"></div>
</aside>
</div>
<div id="panel-docs" class="tab-panel hidden">
<div class="docs-page">
<h2>Documentation</h2>
<p class="desk-banner-warn" style="margin-bottom:14px;font-size:12px;line-height:1.45">Ladders / “mine” rows are best-effort vs the exchange. MM preview is indicative — confirm on AX when exact.</p>

<h3>What you see</h3>
<ul style="font-size:12px;line-height:1.45">
<li><strong>Reference</strong> — C++ cache or desk feed (product may differ from perp).</li>
<li><strong>AX depth</strong> — <code>/book</code> poll. Explicit mode: <code>mm_desk.ax_orderbook_symbols</code> (or all MM legs if empty).</li>
<li><strong>Products</strong> — Depth multiselect order drives ladders and leg menus (∩ <code>market_maker.instruments</code> when set).</li>
<li><strong>Metrics / Activity</strong> — Mid, spread, pressure; fills and orders from gateway.</li>
</ul>

<h3>Strategy</h3>
<ul>
<li><strong>Integers on save</strong> — Width 1–1e9; order size, max position, adj position 1–2,000,000,000 (no product cap; exchange margin governs); adj ticks 1–100. Symmetric <code>W</code> → <code>2W</code> ticks between bid and ask (desk path).</li>
<li><strong>Apply persist</strong> — <code>POST /api/apply_persist</code>: config + cancel-all symbol + re-quote from theo + position (no desk button; curl/API).</li>
</ul>

<h3>Setup</h3>
<ul>
<li>Python 3.10+; Neon via stunnel if using FIX.</li>
<li>Architect API credentials for books, positions, trading.</li>
</ul>

<h3><code>orders.json</code> freeze (Add order)</h3>
<p class="muted" style="font-size:12px;line-height:1.55">Opening <strong>Place manual order</strong> calls <code>POST /api/desk/orders_config_freeze</code> with <code>active:true</code>. C++ copies live <code>mm_desk.mm_orders_config_path</code> to <code>orders_config_frozen_copy_path</code> and reconciles strategies from that snapshot until <code>active:false</code> (after Submit writes the new stack, or if you Close the form). Paths are under <code>mm_desk</code> in config (see <code>config.example.json</code>).</p>

<h3>Parameters (<code>default_config.json</code>)</h3>
<p class="muted" style="font-size:11px;margin-bottom:12px">Each row is the JSON path and what it affects in the trading stack. Popup fields use the same paths.</p>

<h4 style="font-size:13px;color:#e6edf3;margin:14px 0 6px"><code>api</code></h4>
<dl style="font-size:12px;line-height:1.48;color:#c9d1d9;margin:0 0 8px 0">
<dt style="font-weight:600;margin-top:6px"><code>api_key</code> / <code>api_secret</code></dt><dd>Architect gateway credentials (often duplicated in <code>credentials.local.json</code>).</dd>
<dt style="font-weight:600;margin-top:6px"><code>rest_endpoint</code></dt><dd>HTTPS base for REST (…/api). Drives auth, books, orders.</dd>
<dt style="font-weight:600;margin-top:6px"><code>ws_endpoint</code></dt><dd>Architect WebSocket URL for private order stream.</dd>
<dt style="font-weight:600;margin-top:6px"><code>session_token</code></dt><dd>Optional long-lived session string if your flow uses it.</dd>
<dt style="font-weight:600;margin-top:6px"><code>timeout_connect_ms</code> / <code>timeout_read_ms</code> / <code>timeout_write_ms</code></dt><dd>HTTP client timeouts for gateway calls.</dd>
<dt style="font-weight:600;margin-top:6px"><code>fill_poll_cursor_file</code></dt><dd>Persists last seen fill id so restarts do not replay old trades as new orphans.</dd>
</dl>

<h4 style="font-size:13px;color:#e6edf3;margin:14px 0 6px"><code>market_maker</code></h4>
<dl style="font-size:12px;line-height:1.48;color:#c9d1d9;margin:0 0 8px 0">
<dt style="font-weight:600;margin-top:6px"><code>symbol</code> / <code>order_symbol</code></dt><dd>AX perpetual to quote (both set together on save).</dd>
<dt style="font-weight:600;margin-top:6px"><code>theo_symbol</code></dt><dd>Label for theo lookup; align with <code>external_feed.display_symbol</code>.</dd>
<dt style="font-weight:600;margin-top:6px"><code>enabled</code></dt><dd>Master MM strategy enable in config (see also <code>mm_orders_enabled</code>).</dd>
<dt style="font-weight:600;margin-top:6px"><code>mm_orders_enabled</code></dt><dd>When false, C++ does not send MM orders.</dd>
<dt style="font-weight:600;margin-top:6px"><code>width</code> / <code>bid_width</code> / <code>ask_width</code> / <code>spread_ticks</code></dt><dd>Half-spread in ticks per side; symmetric width updates all of these on sidebar save.</dd>
<dt style="font-weight:600;margin-top:6px"><code>order_size</code> / <code>quantity</code></dt><dd>Per-side contract size (kept in sync on save).</dd>
<dt style="font-weight:600;margin-top:6px"><code>order_size_step</code></dt><dd>Size must be a multiple (e.g. 100 for SYMBOL-PERP).</dd>
<dt style="font-weight:600;margin-top:6px"><code>max_position</code></dt><dd>Inventory cap for skew / gating.</dd>
<dt style="font-weight:600;margin-top:6px"><code>adjust_position</code> / <code>adjust_ticks</code></dt><dd>Skew parameters vs inventory.</dd>
<dt style="font-weight:600;margin-top:6px"><code>basis</code></dt><dd>Price offset vs theo mid.</dd>
<dt style="font-weight:600;margin-top:6px"><code>price_tick</code></dt><dd>Quoting tick on AX.</dd>
<dt style="font-weight:600;margin-top:6px"><code>pricing_tick</code></dt><dd>External feed tick (may differ from <code>price_tick</code>).</dd>
<dt style="font-weight:600;margin-top:6px"><code>requote_on_theo_move</code></dt><dd>Cancel/replace when theo drifts past the drift gate.</dd>
<dt style="font-weight:600;margin-top:6px"><code>min_theo_move_ticks_to_requote</code></dt><dd>Minimum |Δtheo| in pricing-tick steps before requote (minimum allowed value 1).</dd>
<dt style="font-weight:600;margin-top:6px"><code>requote_on_timer</code></dt><dd>Periodic MM cycle in addition to feed-driven updates.</dd>
<dt style="font-weight:600;margin-top:6px"><code>update_interval_sec</code></dt><dd>Timer interval for MM logic.</dd>
<dt style="font-weight:600;margin-top:6px"><code>max_theo_age_ms</code></dt><dd>Staleness gate on external theo.</dd>
<dt style="font-weight:600;margin-top:6px"><code>validate_vs_market</code></dt><dd>Validate quotes against market / BBO rules in C++.</dd>
<dt style="font-weight:600;margin-top:6px"><code>cancel_on_disconnect</code></dt><dd>Cancel MM orders when the private WS is down.</dd>
<dt style="font-weight:600;margin-top:6px"><code>cancel_on_external_feed_invalid</code></dt><dd>Cancel when external theo is invalidated after errors.</dd>
<dt style="font-weight:600;margin-top:6px"><code>desk_sync_enabled</code> / <code>desk_signal_path</code></dt><dd>Python desk can write a signal file; C++ picks up OIDs when enabled.</dd>
<dt style="font-weight:600;margin-top:6px"><code>exchange_position_reconcile_sec</code></dt><dd>How often to reconcile position from REST; 0 disables.</dd>
<dt style="font-weight:600;margin-top:6px"><code>current_reload_count</code> / <code>max_reload_cycles</code> / <code>reload_limit_reset_nonce</code> / <code>reload_cycles_count_fill_only</code></dt><dd>Reload cap after exchange accepts; nonce bump resets counter.</dd>
<dt style="font-weight:600;margin-top:6px"><code>reload_on_fill</code> / <code>reload_qty</code> / <code>paper_simulate_fills</code></dt><dd>Fill-driven reload and simulation flags.</dd>
<dt style="font-weight:600;margin-top:6px"><code>post_fill_extra_ticks</code> / <code>resting_depth_extra_ticks</code></dt><dd>Extra ticks after fill / resting depth shaping.</dd>
<dt style="font-weight:600;margin-top:6px"><code>suppress_periodic_theo_requote_when_at_inventory_cap</code></dt><dd>Reduce churn at max position.</dd>
<dt style="font-weight:600;margin-top:6px"><code>feed_log_verbose</code> / <code>feed_log_banner_min_interval_ms</code></dt><dd>Logging verbosity and banner debounce.</dd>
</dl>

<h4 style="font-size:13px;color:#e6edf3;margin:14px 0 6px"><code>external_feed</code> (+ <code>fix</code> JSON)</h4>
<dl style="font-size:12px;line-height:1.48;color:#c9d1d9;margin:0 0 8px 0">
<dt style="font-weight:600;margin-top:6px"><code>enabled</code></dt><dd>Use external reference for theo/depth.</dd>
<dt style="font-weight:600;margin-top:6px"><code>provider</code> / <code>name</code></dt><dd>Feed type label (e.g. <code>neon_fix</code>).</dd>
<dt style="font-weight:600;margin-top:6px"><code>symbol</code> / <code>display_symbol</code></dt><dd>Venue symbol (e.g. USD/JPY) vs display label for desk/theo.</dd>
<dt style="font-weight:600;margin-top:6px"><code>poll_interval_ms</code></dt><dd>REST poll period when not on FIX.</dd>
<dt style="font-weight:600;margin-top:6px"><code>rest_url</code> / <code>rest_uses_spot_ticker_path</code></dt><dd>REST bookTicker base and path style.</dd>
<dt style="font-weight:600;margin-top:6px"><code>consecutive_errors_before_invalidate</code></dt><dd>After N errors, mark theo invalid (0 = disable).</dd>
<dt style="font-weight:600;margin-top:6px"><code>desk_theo_cache_path</code> / <code>desk_depth_cache_path</code></dt><dd>Files C++/Python write for the desk preview.</dd>
<dt style="font-weight:600;margin-top:6px"><code>desk_depth_levels</code> / <code>desk_depth_write_interval_ms</code></dt><dd>Depth rows and write throttle.</dd>
<dt style="font-weight:600;margin-top:6px"><code>write_desk_theo_cache</code> / <code>write_desk_depth_cache</code></dt><dd>Enable writing those caches.</dd>
<dt style="font-weight:600;margin-top:6px"><code>fix</code></dt><dd>Full Neon FIX session: hosts, TLS, comp IDs, <code>md_symbols</code>, market depth, stunnel, etc. (edit as JSON in the popup).</dd>
</dl>

<h4 style="font-size:13px;color:#e6edf3;margin:14px 0 6px"><code>risk</code></h4>
<dl style="font-size:12px;line-height:1.48;color:#c9d1d9;margin:0 0 8px 0">
<dt style="font-weight:600;margin-top:6px"><code>margin_call_threshold</code></dt><dd>Fraction of margin used before risk actions.</dd>
<dt style="font-weight:600;margin-top:6px"><code>max_drawdown_percent</code></dt><dd>Drawdown limit for risk.</dd>
<dt style="font-weight:600;margin-top:6px"><code>max_position_size</code> / <code>max_total_exposure</code></dt><dd>Caps (0 may mean “unset” depending on engine).</dd>
</dl>

<h4 style="font-size:13px;color:#e6edf3;margin:14px 0 6px"><code>trading</code></h4>
<dl style="font-size:12px;line-height:1.48;color:#c9d1d9;margin:0 0 8px 0">
<dt style="font-weight:600;margin-top:6px"><code>default_slippage</code></dt><dd>Default slippage for non-MM orders if applicable.</dd>
<dt style="font-weight:600;margin-top:6px"><code>feed_guardian_enabled</code> / <code>feed_guardian_require_external</code> / <code>feed_guardian_require_ws</code></dt><dd>Guard rails before trading on feed health.</dd>
<dt style="font-weight:600;margin-top:6px"><code>max_orders_per_second</code> / <code>max_requests_per_second</code></dt><dd>Client-side rate limits.</dd>
<dt style="font-weight:600;margin-top:6px"><code>price_precision</code> / <code>quantity_precision</code></dt><dd>Decimal places for prices/quantities.</dd>
<dt style="font-weight:600;margin-top:6px"><code>fees.maker_fee_bps</code> / <code>fees.taker_fee_bps</code></dt><dd>Fee model in basis points for PnL / validation.</dd>
<dt style="font-weight:600;margin-top:6px"><code>watchlist</code></dt><dd>JSON array of symbols (advanced).</dd>
</dl>

<h4 style="font-size:13px;color:#e6edf3;margin:14px 0 6px"><code>startup</code> · <code>performance</code> · <code>websocket</code> · <code>feed</code> · <code>features</code> · <code>logging</code> · <code>hedge</code> · <code>orderbook</code> · <code>mm_desk</code></h4>
<dl style="font-size:12px;line-height:1.48;color:#c9d1d9;margin:0 0 8px 0">
<dt style="font-weight:600;margin-top:6px"><code>startup.*</code></dt><dd>Strict startup: fail on auth/feed, require API/external feed/WS, verify timeouts, log steps.</dd>
<dt style="font-weight:600;margin-top:6px"><code>performance.*</code></dt><dd>Internal queue sizes, HTTP/WS buffer sizes, worker thread count.</dd>
<dt style="font-weight:600;margin-top:6px"><code>websocket.*</code></dt><dd>Reconnect delays, heartbeat, ping/pong; <code>channels</code> JSON controls orderbook/ticker/trades subscriptions.</dd>
<dt style="font-weight:600;margin-top:6px"><code>feed.*</code></dt><dd>Generic MD mode, L2 depth, snapshot interval, subscribe flags, throttle; <code>historical</code> JSON for replay.</dd>
<dt style="font-weight:600;margin-top:6px"><code>features.*</code></dt><dd>Reconnect, validation, PnL, position tracking, rate limiting toggles.</dd>
<dt style="font-weight:600;margin-top:6px"><code>logging.*</code></dt><dd>Log level/format/dir and per-category switches (connectivity, fills, strategy, theo, …).</dd>
<dt style="font-weight:600;margin-top:6px"><code>hedge.*</code></dt><dd>When to hedge (symbol, multiplier, threshold_ratio, webhook).</dd>
<dt style="font-weight:600;margin-top:6px"><code>orderbook.*</code></dt><dd>Depth limits for book structures in the client.</dd>
<dt style="font-weight:600;margin-top:6px"><code>mm_desk.ax_orderbook_symbols</code></dt><dd>AX symbols for Architect depth columns (sidebar multiselect). With <code>mm_desk.ax_architect_depth_multiselect_explicit</code> true, this list is the exact poll set (empty = all MM legs).</dd>
<dt style="font-weight:600;margin-top:6px"><code>mm_desk.ax_architect_depth_multiselect_explicit</code></dt><dd>When true, <code>ax_orderbook_symbols</code> is the full explicit depth selection (including MM legs if ticked). When false, legacy merge applies.</dd>
<dt style="font-weight:600;margin-top:6px"><code>mm_desk.reference_orderbook_symbols</code></dt><dd>Extra reference REST books when not using Neon FIX md_symbols.</dd>
</dl>
</div>
</div>
</div>
</div>
<div id="instrHoldModal" role="dialog" aria-modal="true" aria-labelledby="instrHoldModalTitle" style="display:none;position:fixed;inset:0;z-index:9999;background:rgba(0,0,0,.6);align-items:center;justify-content:center">
<div style="background:#1a1a1a;border:1px solid #7f1d1d;border-radius:10px;max-width:520px;width:92%;padding:20px;color:#eee;box-shadow:0 10px 40px rgba(0,0,0,.6)">
<h3 id="instrHoldModalTitle" style="margin:0 0 10px;color:#f87171">Cancel all orders?</h3>
<p style="margin:0 0 12px;font-size:13px;line-height:1.5" id="instrHoldModalBody">—</p>
<div style="background:#3f1d1d;border:1px solid #7f1d1d;border-radius:6px;padding:10px;font-size:12px;line-height:1.45;margin-bottom:14px">
<strong>Account-wide.</strong> Cancels <em>all</em> open orders on this instrument for the whole account — including hand-placed orders — and stops it quoting.
</div>
<div style="display:flex;gap:10px;justify-content:flex-end">
<button type="button" class="btn" id="btnInstrHoldCancelModal" style="min-width:90px">Cancel</button>
<button type="button" class="btn btn-danger" id="btnInstrHoldConfirm" style="min-width:150px;background:#b91c1c;border-color:#7f1d1d;color:#fff">Cancel all orders</button>
</div>
</div>
</div>
<script defer src="/mm_live_desk_client.js?v=__MM_DESK_BUILD__"></script>
<script>
window.addEventListener("load", function() {
  if (typeof window.mmDeskRefresh === "function") return;
  var w = document.getElementById("mmDeskWire");
  var msg = "Desk UI script missing — open DevTools → Network, reload, and confirm GET /mm_live_desk_client.js is 200 (not blocked).";
  if (w) { w.style.color = "#ff8a8a"; w.textContent = msg; }
  else { console.error(msg); }
});
</script>
</body></html>
"""


def _finalize_mm_desk_index_html(tpl: str) -> str:
    tpl = tpl.replace("__HEADER_REF_PILL__", '<span class="pill" id="pillBn">Reference</span>\n')
    tpl = tpl.replace("__REF_DEPTH_COL_BLOCK__", _REF_DEPTH_COL_BLOCK.rstrip() + "\n")
    return tpl


MM_DESK_INDEX_HTML = _finalize_mm_desk_index_html(_INDEX_HTML_TEMPLATE)


def run_connectivity_preflight(st: DeskSharedState) -> tuple[bool, bool, bool]:
    """
    One-shot probes before the browser opens. Prints [mm_live_desk] PREFLIGHT lines.
    Returns (reference_feed_ok, ax_book_ok, order_gateway_ok) — first flag is reference REST depth or Neon TCP.
    """
    with st.lock:
        refp = st.reference_provider
        bn_sym = st.ref_symbol.strip().upper()
        bn_base = st.ref_rest_url.rstrip("/")
        use_spot = st.ref_rest_uses_spot_path
        ax_sym = st.ax_symbol.strip()
        rest = st.rest_endpoint.rstrip("/")
        tok = st.token
        neon_h = st.neon_fix_host
        neon_p = st.neon_fix_port
        wrf = st.web_desk_run_reference_feed
    bn_ok = False
    ax_ok = False
    og_ok = False
    if not wrf:
        bn_ok = True
        if refp == "neon_fix":
            print(
                "[mm_live_desk] PREFLIGHT reference feed: Neon via C++ JSON (logs/mm_external_depth.json); "
                "set MM_LIVE_DESK_WEB_REFERENCE_FEED=1 to probe TCP/TLS from Python (not with C++ Neon on same creds).",
                flush=True,
            )
        else:
            print(
                "[mm_live_desk] PREFLIGHT reference feed: SKIP (no reference column for non-Neon; "
                "set MM_LIVE_DESK_WEB_REFERENCE_FEED=1 for REST depth in desk)",
                flush=True,
            )
    elif refp == "neon_fix":
        dc_pf = get_merged_config_dict()
        nfs_pf = neon_fix_feed.settings_from_merged_config(dc_pf)
        if nfs_pf and nfs_pf.direct_tls:
            sni = (nfs_pf.tls_server_name or nfs_pf.target_comp_id or "").strip()
            bn_ok, err = neon_fix_feed.neon_transport_preflight(nfs_pf, timeout=5.0)
            print(
                f"[mm_live_desk] PREFLIGHT Neon FIX direct TLS SNI={sni!r} → {nfs_pf.tls_remote_host}:{nfs_pf.tls_remote_port}: "
                f"{'OK' if bn_ok else err}",
                flush=True,
            )
            if not bn_ok:
                print(
                    "[mm_live_desk] PREFLIGHT hint: check tls_remote_host/port and tls_server_name in external_feed.fix "
                    "(or set direct_tls false and use stunnel).",
                    flush=True,
                )
        else:
            h, p = neon_h or "127.0.0.1", neon_p or 14507
            bn_ok, err = neon_fix_feed.tcp_preflight(h, p, timeout=4.0)
            print(
                f"[mm_live_desk] PREFLIGHT Neon FIX TCP {h}:{p}: {'OK' if bn_ok else err}",
                flush=True,
            )
            if not bn_ok:
                print(
                    "[mm_live_desk] PREFLIGHT hint: start stunnel on external_feed.fix.port, or set direct_tls + tls_remote_*.",
                    flush=True,
                )
    elif _is_hyperliquid_provider(refp):
        t_bn = min(REFERENCE_HTTP_TIMEOUT, 14.0)
        if not bn_base.startswith("http"):
            bn_base = "https://api.hyperliquid.xyz"
        try:
            mids, dtm = _hyperliquid_fetch_all_mids(bn_base, t_bn)
            mid, used_mid = _hyperliquid_pick_mid_for_symbol(mids, bn_sym)
            if mid is not None:
                bn_ok = True
                print(
                    f"[mm_live_desk] PREFLIGHT Hyperliquid allMids {bn_sym}: OK (coin={used_mid}, mid={mid:.6f}, {dtm:.0f}ms)",
                    flush=True,
                )
            else:
                print(
                    f"[mm_live_desk] PREFLIGHT Hyperliquid allMids {bn_sym}: MISSING (checked {len(_hyperliquid_symbol_candidates(bn_sym))} aliases)",
                    flush=True,
                )
            # Optional L2 probe (diagnostic only; feed can run on allMids without l2Book).
            url = _hyperliquid_l2_book_url(bn_base)
            l2_picked = None
            l2_n = 0
            for cand in _hyperliquid_symbol_candidates(bn_sym):
                req = urllib.request.Request(url, method="POST")
                req.add_header("Content-Type", "application/json")
                payload = json.dumps({"type": "l2Book", "coin": cand}).encode("utf-8")
                with urllib.request.urlopen(req, data=payload, context=_ssl_ctx(), timeout=t_bn) as r:
                    raw = json.loads(r.read().decode("utf-8"))
                b, _a = _hyperliquid_parse_l2_book(raw)
                if b:
                    l2_picked = cand
                    l2_n = len(b)
                    break
            print(
                f"[mm_live_desk] PREFLIGHT Hyperliquid l2Book {bn_sym}: "
                f"{'OK' if l2_n > 0 else 'EMPTY'} ({l2_n} bid levels"
                + (f", coin={l2_picked}" if l2_picked else "")
                + ")",
                flush=True,
            )
        except Exception as e:
            print(f"[mm_live_desk] PREFLIGHT Hyperliquid: FAIL {type(e).__name__}: {e}", flush=True)
    else:
        t_bn = min(REFERENCE_HTTP_TIMEOUT, 14.0)
        if not bn_base.startswith("http"):
            print(
                "[mm_live_desk] PREFLIGHT Reference REST: SKIP (external_feed.rest_url empty — set HTTPS base for REST theo)",
                flush=True,
            )
        else:
            try:
                u = _reference_depth_http_url(bn_base, bn_sym, use_spot, depth_limit=5)
                rq = urllib.request.Request(u, method="GET")
                with urllib.request.urlopen(rq, context=_ssl_ctx(), timeout=t_bn) as r:
                    j = json.loads(r.read().decode("utf-8"))
                n = len(j.get("bids") or [])
                bn_ok = n > 0
                print(
                    f"[mm_live_desk] PREFLIGHT Reference REST depth {bn_sym}: {'OK' if bn_ok else 'EMPTY'} ({n} bid levels)",
                    flush=True,
                )
            except Exception as e:
                print(f"[mm_live_desk] PREFLIGHT Reference REST: FAIL {type(e).__name__}: {e}", flush=True)
                print(
                    "[mm_live_desk] PREFLIGHT hint: VPN/firewall or slow network — try MM_DESK_REFERENCE_TIMEOUT_SEC=25",
                    flush=True,
                )
    if not tok:
        print("[mm_live_desk] PREFLIGHT AX /book: SKIP (no bearer token)", flush=True)
    else:
        try:
            hdr = {"Authorization": f"Bearer {tok}", "Accept": "application/json"}
            qsp = urllib.parse.urlencode({"symbol": ax_sym, "level": 2})
            u = f"{rest}/book?{qsp}"
            rq = urllib.request.Request(u, method="GET", headers=hdr)
            with urllib.request.urlopen(rq, context=_ssl_ctx(), timeout=min(AX_HTTP_TIMEOUT, 14.0)) as r:
                raw = json.loads(r.read().decode("utf-8"))
            b, a = parse_orderbook_payload(raw)
            ax_ok = len(b) + len(a) > 0
            print(
                f"[mm_live_desk] PREFLIGHT AX /book {ax_sym}: {'OK' if ax_ok else 'EMPTY'} (bids={len(b)} asks={len(a)})",
                flush=True,
            )
        except Exception as e:
            print(f"[mm_live_desk] PREFLIGHT AX /book: FAIL {type(e).__name__}: {e}", flush=True)
    if tok:
        try:
            ob = architect_orders_base(rest)
            hdr = {"Authorization": f"Bearer {tok}", "Accept": "application/json"}
            u = f"{ob.rstrip('/')}/open-orders"
            rq = urllib.request.Request(u, method="GET", headers=hdr)
            to = min(ORDERS_HTTP_TIMEOUT, 12.0)
            with urllib.request.urlopen(rq, context=_ssl_ctx(), timeout=to) as r:
                raw = r.read()
            og_ok = len(raw) >= 0
            print(f"[mm_live_desk] PREFLIGHT order-gateway /open-orders: OK ({len(raw)} bytes)", flush=True)
        except Exception as e:
            print(f"[mm_live_desk] PREFLIGHT order-gateway /open-orders: FAIL {type(e).__name__}: {e}", flush=True)
            print(
                "[mm_live_desk] PREFLIGHT hint: orders/fills may be empty until this responds; "
                "try MM_DESK_ORDERS_TIMEOUT_SEC=15 or check routing to …/orders host",
                flush=True,
            )
    return bn_ok, ax_ok, og_ok


def run_web_main(port: int = 8765) -> None:
    cfg = load_app_config()
    st = DeskSharedState(cfg)
    stop = threading.Event()
    threads: list[threading.Thread] = []
    index_html_bytes = MM_DESK_INDEX_HTML.replace("__MM_DESK_BUILD__", str(int(time.time()))).encode("utf-8")
    index_html_sha16 = hashlib.sha256(index_html_bytes).hexdigest()[:16]
    try:
        client_js_bytes = MM_LIVE_DESK_CLIENT_JS_FILE.read_bytes()
    except OSError as e:
        raise RuntimeError(
            f"MM Live Desk UI requires {MM_LIVE_DESK_CLIENT_JS_FILE} (same directory as {MM_LIVE_DESK_CORE_FILE.name}): {e}"
        ) from e
    client_js_sha16 = hashlib.sha256(client_js_bytes).hexdigest()[:16]
    http_bind = (os.environ.get("MM_DESK_HTTP_BIND") or "127.0.0.1").strip() or "127.0.0.1"
    browser_host = "127.0.0.1" if http_bind in ("0.0.0.0", "::") else http_bind

    desk_log(
        None,
        "Desk config: "
        f"default_config exists={DEFAULT_CONFIG_PATH.is_file()} path={DEFAULT_CONFIG_PATH} | "
        f"credentials exists={CREDENTIALS_PATH.is_file()} | "
        f"ax={st.ax_symbol!r} ref={st.ref_symbol!r} provider={st.reference_provider!r} | core={MM_LIVE_DESK_CORE_FILE.name}",
    )

    def start_feeds() -> None:
        stop.clear()
        sync_base = ""
        with st.lock:
            st.feeds_running = True
            if not st.web_desk_run_reference_feed:
                st.ref_latency_text = (
                    "Neon: C++ logs/mm_external_depth.json + mm_external_theo.json (same cwd)"
                    if st.reference_provider == "neon_fix"
                    else "External reference: not polled (web desk)"
                )
                st.reference_error = ""
            sync_base = str(st.rest_endpoint or "").strip().rstrip("/")
        ok_ticks, tick_msg, _n_cfg = desk_refresh_gateway_instrument_ticks(st, sync_base or None)
        if not ok_ticks:
            desk_log(st, f"[MM_DESK] gateway /instruments tick refresh at feed start: {tick_msg}", verbose_only=False)
        threads.clear()
        if st.web_desk_run_reference_feed:
            threads.append(
                threading.Thread(target=reference_market_feed_loop, args=(st, stop), daemon=True)
            )
        threads.append(threading.Thread(target=ax_feed_loop, args=(st, stop), daemon=True))
        for t in threads:
            t.start()

    def stop_feeds() -> None:
        stop.set()
        desk_interrupt_neon_fix_socket(st)
        for t in threads:
            t.join(timeout=1.5)
        threads.clear()
        with st.lock:
            st.feeds_running = False
            st.status_line = "Feeds stopped"

    place_order_jobs: dict[str, dict] = {}
    place_order_jobs_lock = threading.Lock()
    local_http_host = "127.0.0.1" if http_bind in ("0.0.0.0", "::") else http_bind

    def _parse_place_order_payload(payload: dict) -> tuple[bool, dict]:
        j = payload if isinstance(payload, dict) else {}
        ax = str(j.get("ax_symbol", "") or j.get("symbol", "") or "").strip()
        if not ax:
            return False, {"error": "ax_symbol is required"}

        theo_source = _normalize_theo_source(
            j.get("theo_source"),
            str(((get_merged_config_dict().get("external_feed") or {}).get("provider")) or "neon_fix"),
        )
        if theo_source not in ("mettraders", "hyperliquid", "neon_fix"):
            return False, {"error": f"unsupported theo_source: {theo_source!r}"}

        theo_venue = str(j.get("theo_venue_symbol", "") or "").strip()
        ref_fix = str(j.get("reference_fix_symbol", "") or theo_venue or "").strip()
        ord_sym = str(j.get("order_symbol", "") or ax).strip()
        params_in = j.get("params") if isinstance(j.get("params"), dict) else j
        try:
            width = int(
                params_in.get(
                    "width_bps",
                    params_in.get("width_ticks", params_in.get("width", 0)),
                )
            )
            order_size = int(params_in.get("order_size", 0))
            adjust_position = int(params_in.get("adjust_position", 0))
            adjust_ticks = int(params_in.get("adjust_ticks", 0))
            min_drift = int(
                params_in.get(
                    "min_theo_move_ticks_to_requote",
                    params_in.get("min_drift_ticks", 0),
                )
            )
        except (TypeError, ValueError):
            return False, {"error": "all params must be integers"}
        # Pricer-snapshot linear transform (per-stack, optional). See the C++
        # MarketMakerManualStack docstring in include/config/Config.h for the full formula.
        # Defaults match the JS form: snapshots=0 disables the transform, slope=1 is
        # linear pass-through. We accept any number type and clamp to safe values; negative
        # snapshots are rejected because the C++ failsafe already disables-on-zero, but a
        # negative would silently disable too and that's confusing.
        try:
            quote_snapshot = float(params_in.get("quote_snapshot", 0) or 0)
            pricer_snapshot = float(params_in.get("pricer_snapshot", 0) or 0)
            slope = float(params_in.get("slope", 1) if params_in.get("slope", 1) != "" else 1)
        except (TypeError, ValueError):
            return False, {"error": "quote_snapshot, pricer_snapshot, slope must be numbers"}
        if quote_snapshot < 0 or pricer_snapshot < 0:
            return False, {"error": "quote_snapshot and pricer_snapshot must be >= 0 (use 0 to disable transform)"}

        merged_cfg = get_merged_config_dict()
        inst_po, inst_rl = _instrument_max_position_and_reload_from_merged(merged_cfg, ax)
        user_inst_mx = j.get("instrument_max_position", j.get("max_position"))
        if user_inst_mx is not None and user_inst_mx != "":
            try:
                max_position = int(user_inst_mx)
            except (TypeError, ValueError):
                return False, {"error": "instrument_max_position must be an integer"}
        else:
            max_position = inst_po
        if max_position < _MM_MAX_POSITION_LO or max_position > _MM_MAX_POSITION_HI:
            return False, {
                "error": f"instrument_max_position must be in [{_MM_MAX_POSITION_LO}, {_MM_MAX_POSITION_HI}]"
            }
        exclude_sid = ""
        if bool(j.get("replace_existing_stack")):
            exclude_sid = str(j.get("stack_id", "") or "").strip()
        ok_mx, err_mx, _floor_mx = _validate_instrument_max_position_monotonic(
            merged_cfg, ax, max_position, exclude_stack_id=exclude_sid
        )
        if not ok_mx:
            return False, {"error": err_mx}
        # Per-field validation: report each offending field with its observed value and the rule
        # it violated. Replaces a single bundled "params out of range" message that named only one
        # of the seven fields and made the actual culprit invisible to the user.
        out_of_range: list[str] = []
        if width < 1:
            out_of_range.append(f"width_bps={width} (need >= 1)")
        if order_size < 1:
            out_of_range.append(f"order_size={order_size} (need >= 1)")
        if max_position < 1:
            out_of_range.append(f"max_position={max_position} (need >= 1)")
        if adjust_position < 1:
            out_of_range.append(f"adjust_position={adjust_position} (need >= 1)")
        if adjust_ticks < 1:
            out_of_range.append(f"adjust_ticks={adjust_ticks} (need >= 1)")
        if min_drift < 1:
            out_of_range.append(f"min_theo_move_ticks_to_requote={min_drift} (need >= 1)")
        if inst_rl < 0:
            out_of_range.append(
                f"max_reload_cycles={inst_rl} (need >= 0; use 0 for unlimited)"
            )
        if out_of_range:
            return False, {"error": "params out of range: " + "; ".join(out_of_range)}

        return True, {
            "ax": ax,
            "theo_source": theo_source,
            "theo_venue": theo_venue,
            "reference_fix_symbol": ref_fix,
            "order_symbol": ord_sym,
            "width_bps": width,
            "order_size": order_size,
            "adjust_position": adjust_position,
            "adjust_ticks": adjust_ticks,
            "min_drift": min_drift,
            "max_position": max_position,
            "inst_reload_cycles": inst_rl,
            "quote_snapshot": quote_snapshot,
            "pricer_snapshot": pricer_snapshot,
            "slope": slope,
            "merged_cfg": merged_cfg,
        }

    def _seed_pending_stack_for_async_submit(payload: dict, stack_id: str) -> tuple[bool, dict]:
        ok, parsed = _parse_place_order_payload(payload)
        if not ok:
            return False, parsed

        ax = str(parsed["ax"])
        merged_cfg = parsed["merged_cfg"]
        pending_entry = {
            "id": stack_id,
            "stack_id": stack_id,
            "ax_symbol": ax,
            "theo_source": parsed["theo_source"],
            "theo_venue_symbol": parsed["theo_venue"],
            "reference_fix_symbol": parsed["reference_fix_symbol"],
            "order_symbol": parsed["order_symbol"],
            "max_position": int(parsed["max_position"]),
            "width_bps": int(parsed["width_bps"]),
            "order_size": int(parsed["order_size"]),
            "adjust_position": int(parsed["adjust_position"]),
            "adjust_ticks": int(parsed["adjust_ticks"]),
            "min_theo_move_ticks_to_requote": int(parsed["min_drift"]),
            "max_reload_cycles": int(parsed["inst_reload_cycles"]),
            # Pricer-snapshot transform anchors (per-stack). Persisted as floats; C++ reads
            # them via MarketMakerManualStack::{quote_snapshot,pricer_snapshot,slope} and applies
            # the transform inside MakeMarketStrategy::mmApplyPricerSnapshotTransform().
            "quote_snapshot": float(parsed["quote_snapshot"]),
            "pricer_snapshot": float(parsed["pricer_snapshot"]),
            "slope": float(parsed["slope"]),
            "created_ms": int(time.time() * 1000),
            "desk_seeded": False,
            "mm_move_enabled": False,
            "submit_status": "pending",
            "bid_price": 0.0,
            "bid_qty": 0,
            "bid_exchange_oid": "",
            "ask_price": 0.0,
            "ask_qty": 0,
            "ask_exchange_oid": "",
        }

        with _MM_ORDERS_CONFIG_LOCK:
            doc = load_mm_orders_config(merged_cfg)
            if _find_stack_row_in_orders_doc(doc, stack_id, ax)[1] is not None:
                return False, {"error": f"stack id {stack_id} already exists for {ax}"}
        ok_w, err_w = orders_json_apply_stack_row_dual_view(merged_cfg, pending_entry)
        if not ok_w:
            return False, {"error": err_w}
        return True, {"ax_symbol": ax, "stack_id": stack_id}

    def _run_place_order_job(job_id: str, payload: dict, stack_id: str) -> None:
        url = f"http://{local_http_host}:{port}/api/desk/place_order"
        payload2 = dict(payload) if isinstance(payload, dict) else {}
        payload2["stack_id"] = stack_id
        payload2["replace_existing_stack"] = True
        req = urllib.request.Request(
            url,
            data=json.dumps(payload2).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=300.0) as resp:
                code = int(resp.getcode())
                raw = resp.read().decode("utf-8", errors="replace")
            try:
                body = json.loads(raw) if raw else {}
            except Exception:
                body = {"ok": False, "error": raw[:400] or "invalid JSON response"}
        except urllib.error.HTTPError as e:
            code = int(getattr(e, "code", 500) or 500)
            raw = ""
            try:
                raw = e.read().decode("utf-8", errors="replace")
            except Exception:
                raw = str(e)
            try:
                body = json.loads(raw) if raw else {}
            except Exception:
                body = {"ok": False, "error": raw[:400] or str(e)}
        except Exception as e:
            code = 500
            body = {"ok": False, "error": f"{type(e).__name__}: {e}"}

        # The async route created a pending orders.json row before starting this
        # job. If the first manual quote was rejected as marketable, remove that
        # pending row because no order was submitted to the venue.
        if (
            isinstance(body, dict)
            and not body.get("ok")
            and body.get("error_code")
            in (
                "QUOTE_WOULD_TRADE_IMMEDIATELY",
                "TOP_OF_BOOK_UNAVAILABLE",
            )
        ):
            try:
                merged_cfg = get_merged_config_dict()
                ax_symbol = str(
                    body.get("ax_symbol")
                    or payload2.get("ax_symbol")
                    or payload2.get("symbol")
                    or ""
                ).strip()

                with _MM_ORDERS_CONFIG_LOCK:
                    doc = load_mm_orders_config(merged_cfg)
                    removed_count = _orders_json_remove_stack_id_everywhere(
                        doc,
                        stack_id,
                        ax_symbol,
                    )

                    if removed_count == 0 and ax_symbol:
                        removed_count = _orders_json_remove_stack_id_everywhere(
                            doc,
                            stack_id,
                            "",
                        )

                    if removed_count > 0:
                        ok_cleanup, cleanup_error = save_mm_orders_config(
                            merged_cfg,
                            doc,
                        )

                        if not ok_cleanup:
                            desk_log(
                                st,
                                f"manual quote rejection cleanup failed: "
                                f"stack_id={stack_id} error={cleanup_error}",
                            )
            except Exception as cleanup_exc:
                desk_log(
                    st,
                    f"manual quote rejection cleanup exception: "
                    f"stack_id={stack_id} "
                    f"error={type(cleanup_exc).__name__}: {cleanup_exc}",
                )

        with place_order_jobs_lock:
            place_order_jobs[job_id] = {
                "done": True,
                "http_code": code,
                "result": body,
            }

    class H(BaseHTTPRequestHandler):
        def log_message(self, fmt: str, *args) -> None:
            return

        def do_GET(self) -> None:
            path = urlparse(self.path).path
            if path == "/api/desk_meta":
                try:
                    mtime = int(MM_LIVE_DESK_CORE_FILE.stat().st_mtime)
                except OSError:
                    mtime = 0
                with st.lock:
                    meta = {
                        "ok": True,
                        "core_file": str(MM_LIVE_DESK_CORE_FILE),
                        "core_tag": f"{MM_LIVE_DESK_CORE_FILE.name}@{mtime}",
                        "cwd": os.getcwd(),
                        "repo_root": str(REPO_ROOT),
                        "default_config": str(DEFAULT_CONFIG_PATH),
                        "default_config_exists": DEFAULT_CONFIG_PATH.is_file(),
                        "credentials_path": str(CREDENTIALS_PATH),
                        "credentials_exists": CREDENTIALS_PATH.is_file(),
                        "ax_symbol": st.ax_symbol,
                        "ref_symbol": st.ref_symbol,
                        "feeds_running": bool(st.feeds_running),
                        "ref_all_n": len(st.ref_all_books),
                        "ax_all_n": len(st.ax_all_books),
                        "index_html_sha16": index_html_sha16,
                        "client_js_sha16": client_js_sha16,
                        "feed_health": load_mm_feed_health(get_merged_config_dict()),
                        "live_orders": _enrich_live_orders_with_orders_json_move_flags(
                            load_mm_orders_config(get_merged_config_dict()),
                            load_mm_active_orders(get_merged_config_dict()),
                        ),
                        "mm_orders_config": load_mm_orders_config(get_merged_config_dict()),
                    }
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Cache-Control", "no-store, no-cache, must-revalidate")
                self.end_headers()
                _safe_wfile_write(self, json.dumps(meta, ensure_ascii=False).encode("utf-8"))
                return
            if path == "/mm_live_desk_client.js":
                # Serve the client JS FRESH from disk on every request so edits to
                # mm_live_desk_client.js take effect without restarting the desk
                # server (only a browser reload is needed). Falls back to the
                # snapshot read at startup if the file briefly cannot be read.
                try:
                    js_bytes = MM_LIVE_DESK_CLIENT_JS_FILE.read_bytes()
                except OSError:
                    js_bytes = client_js_bytes
                self.send_response(200)
                self.send_header("Content-Type", "application/javascript; charset=utf-8")
                self.send_header("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
                self.send_header("Pragma", "no-cache")
                self.end_headers()
                _safe_wfile_write(self, js_bytes)
                return
            if path in ("/api/state", "/api/boot"):
                try:
                    if path == "/api/boot":
                        desk_log(st, "HTTP GET /api/boot (initial page load)")
                    snap = st.snapshot_json()
                except Exception as e:
                    desk_log(st, f"snapshot_json ERROR: {type(e).__name__}: {e}")
                    self.send_response(500)
                    self.send_header("Content-Type", "application/json")
                    self.end_headers()
                    _safe_wfile_write(
                        self, json.dumps({"error": "snapshot_failed", "detail": str(e)}).encode("utf-8")
                    )
                    return
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Cache-Control", "no-store, no-cache, must-revalidate")
                self.send_header("Pragma", "no-cache")
                self.end_headers()
                _safe_wfile_write(self, _state_json_bytes(snap))
                return
            if path == "/api/desk/templates":
                try:
                    mc = get_merged_config_dict()
                    ensure_mm_templates_file(mc)
                    doc = load_mm_templates(mc)
                except Exception as e:
                    self._json(500, {"ok": False, "error": f"{type(e).__name__}: {e}"})
                    return
                self._json(200, {"ok": True, "templates": doc})
                return
            if path == "/api/instruments":
                with st.lock:
                    base = st.rest_endpoint.strip().rstrip("/")
                syms, err = fetch_ax_markets_list(base)
                if err:
                    self._json(502, {"ok": False, "instruments": syms, "error": err})
                else:
                    self._json(200, {"ok": True, "instruments": syms})
                return
            if path == "/":
                # Build the cache-bust stamp (?v=) from the CURRENT client JS file's
                # mtime+size, recomputed per request. The stamp therefore CHANGES the
                # moment mm_live_desk_client.js is edited, so the <script> URL changes
                # and the browser is FORCED to refetch the new file on the next page
                # reload — no server restart and no manual hard-refresh required.
                #
                # This is the permanent fix for "Open Template shows a stale value":
                # previously the stamp was frozen at server-startup time, so reloading
                # the page reused the identical script URL and the browser kept serving
                # the OLD cached JS (the corrected template-read code never reached the
                # browser). Falls back to the startup snapshot if the file can't be
                # stat'd for a moment.
                try:
                    js_stat = MM_LIVE_DESK_CLIENT_JS_FILE.stat()
                    build_stamp = f"{int(js_stat.st_mtime)}-{js_stat.st_size}"
                    html_bytes = MM_DESK_INDEX_HTML.replace(
                        "__MM_DESK_BUILD__", build_stamp
                    ).encode("utf-8")
                except OSError:
                    html_bytes = index_html_bytes
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
                self.send_header("Pragma", "no-cache")
                self.send_header("Expires", "0")
                self.end_headers()
                _safe_wfile_write(self, html_bytes)
                return
            self.send_error(404)

        def do_POST(self) -> None:
            path = urlparse(self.path).path
            if path == "/api/login":
                j = _read_json_body(self)
                base = str(j.get("rest_endpoint", "") or "").strip()
                key = str(j.get("api_key", "") or "")
                sec = str(j.get("api_secret", "") or "")
                if base:
                    with st.lock:
                        st.rest_endpoint = base.rstrip("/")
                if not key or not sec:
                    self._json(400, {"ok": False, "error": "missing api_key/api_secret"})
                    return
                with st.lock:
                    b = st.rest_endpoint.rstrip("/")
                code, body = http_json("POST", b + "/authenticate", {"api_key": key, "api_secret": sec, "expiration_seconds": 86400})
                if code < 200 or code >= 300 or not isinstance(body, dict):
                    self._json(code, {"ok": False, "error": f"HTTP {code}", "body": body})
                    return
                tok = extract_bearer_token(body)
                if not tok:
                    self._json(400, {"ok": False, "error": "no token"})
                    return
                with st.lock:
                    st.token = tok
                    st.api_key = key
                    st.api_secret = sec
                rest_for_save = base.rstrip("/") if base else ""
                if not rest_for_save:
                    with st.lock:
                        rest_for_save = st.rest_endpoint.rstrip("/")
                ok_s, msg_s = persist_desk_config_to_repo(
                    st,
                    {"rest_endpoint": rest_for_save, "api_key": key, "api_secret": sec},
                )
                self._json(200, {"ok": True, "config_saved": ok_s, "config_message": msg_s})
                return

            if path == "/api/feeds/start":
                j = _read_json_body(self)
                with st.lock:
                    if j.get("rest_endpoint"):
                        st.rest_endpoint = str(j["rest_endpoint"]).strip().rstrip("/")
                    if j.get("ax_symbol"):
                        st.ax_symbol = str(j["ax_symbol"]).strip()
                    _rs = j.get("ref_symbol") or j.get("bn_symbol")
                    if _rs:
                        st.ref_symbol = str(_rs).strip()
                    if j.get("api_key"):
                        st.api_key = str(j["api_key"]).strip()
                    if j.get("api_secret"):
                        st.api_secret = str(j["api_secret"]).strip()
                stop_feeds()
                ensure_ax_session(st)
                cfg_reload = load_app_config()
                base_syms = cfg_reload.get("ax_book_symbols") if isinstance(cfg_reload.get("ax_book_symbols"), list) else []
                with st.lock:
                    primary = st.ax_symbol.strip()
                merged: list[str] = [str(x).strip() for x in base_syms if str(x).strip()]
                if primary and primary not in merged:
                    merged.insert(0, primary)
                with st.lock:
                    st.ax_book_symbols = merged if merged else ([primary] if primary else [])
                bn_reload = (
                    cfg_reload.get("ref_book_symbols") if isinstance(cfg_reload.get("ref_book_symbols"), list) else []
                )
                with st.lock:
                    primary_bn = st.ref_symbol.strip().upper()
                merged_bn: list[str] = [str(x).strip().upper() for x in bn_reload if str(x).strip()]
                if primary_bn and primary_bn not in merged_bn:
                    merged_bn.insert(0, primary_bn)
                with st.lock:
                    st.ref_book_symbols = merged_bn if merged_bn else ([primary_bn] if primary_bn else [])
                start_feeds()
                with st.lock:
                    st.status_line = "Feeds restarted (public + Architect if configured)"
                self._json(200, {"ok": True})
                return

            if path == "/api/feeds/stop":
                stop_feeds()
                self._json(200, {"ok": True})
                return

            if path == "/api/desk/sync_instrument_ticks":
                j = _read_json_body(self)
                rb = str(j.get("rest_endpoint", "") or "").strip().rstrip("/")
                if not rb:
                    with st.lock:
                        rb = str(st.rest_endpoint or "").strip().rstrip("/")
                if not rb:
                    self._json(400, {"ok": False, "error": "rest_endpoint not set", "updated": 0, "message": ""})
                    return
                ok, msg, n = desk_refresh_gateway_instrument_ticks(st, rb)
                self._json(
                    200 if ok else 502,
                    {"ok": ok, "updated": int(n), "message": msg},
                )
                return

            if path == "/api/apply":
                j = _read_json_body(self)
                out = web_applyQuotes(st, j)
                code = 200 if out.get("ok") else 400
                self._json(code, out)
                return

            if path == "/api/apply_persist":
                j = _read_json_body(self)
                payload = {
                    "rest_endpoint": str(j.get("rest_endpoint", "") or ""),
                    "ax_symbol": str(j.get("ax_symbol", "") or ""),
                    "ref_symbol": str(j.get("ref_symbol") or j.get("bn_symbol", "") or ""),
                    "api_key": str(j.get("api_key", "") or ""),
                    "api_secret": str(j.get("api_secret", "") or ""),
                    "width_bps": j.get("width_bps", j.get("width")),
                    "order_size": j.get("order_size"),
                    "max_position": j.get("max_position"),
                    "adjust_position": j.get("adjust_position"),
                    "adjust_ticks": j.get("adjust_ticks"),
                }
                if "max_reload_cycles" in j:
                    payload["max_reload_cycles"] = j.get("max_reload_cycles")
                if "current_reload_count" in j:
                    payload["current_reload_count"] = j.get("current_reload_count")
                if "reload_cycles_count_fill_only" in j:
                    payload["reload_cycles_count_fill_only"] = j.get("reload_cycles_count_fill_only")
                if "reload_limit_reset_nonce" in j:
                    payload["reload_limit_reset_nonce"] = j.get("reload_limit_reset_nonce")
                if "requote_on_theo_move" in j:
                    payload["requote_on_theo_move"] = j.get("requote_on_theo_move")
                if "min_theo_move_ticks_to_requote" in j:
                    payload["min_theo_move_ticks_to_requote"] = j.get("min_theo_move_ticks_to_requote")
                if "price_tick" in j:
                    payload["price_tick"] = j.get("price_tick")
                if "pricing_tick" in j:
                    payload["pricing_tick"] = j.get("pricing_tick")
                if "mm_orders_enabled" in j:
                    payload["mm_orders_enabled"] = j.get("mm_orders_enabled")
                if "config_merge" in j:
                    payload["config_merge"] = j.get("config_merge")
                ok_s, msg_s = persist_desk_config_to_repo(st, payload)
                if not ok_s:
                    self._json(400, {"ok": False, "error": msg_s})
                    return
                desk_apply_form_to_state(st, payload)
                if payload.get("api_key") or payload.get("api_secret"):
                    with st.lock:
                        st.token = None
                ensure_ax_session(st)
                out = web_applyQuotes(st, j)
                code = 200 if out.get("ok") else 400
                self._json(code, {**out, "config_saved": True, "config_message": msg_s})
                return

            if path == "/api/desk/mm_reload_nonce_bump":
                ok_b, msg_b, nonce_b = desk_bump_mm_reload_limit_nonce(st)
                if not ok_b:
                    self._json(400, {"ok": False, "error": msg_b})
                    return
                self._json(
                    200,
                    {
                        "ok": True,
                        "config_message": msg_b,
                        "reload_limit_reset_nonce": nonce_b,
                    },
                )
                return

            if path == "/api/desk/place_order_async":
                j = _read_json_body(self)
                if not isinstance(j, dict):
                    self._json(400, {"ok": False, "error": "invalid JSON payload"})
                    return
                # Item 3/4: never seed/place into a stopped instrument without an explicit
                # resume confirm. Fires BEFORE any orders.json seed or gateway submit.
                _ax_async = str(j.get("ax_symbol", "") or j.get("symbol", "") or "").strip()
                if _ax_async:
                    _rg = _place_resume_decision(st, _ax_async, j)
                    if _rg["action"] == "needs_confirm":
                        self._json(409, {"ok": False, "needs_resume_confirm": True,
                                         "ax_symbol": _rg["ax_symbol"], "error": _rg["error"]})
                        return
                    if _rg["action"] == "error":
                        self._json(400, {"ok": False, "error": _rg["error"]})
                        return
                stack_id = str(j.get("stack_id", "") or "").strip() or uuid.uuid4().hex[:16]
                ok_seed, seed_info = _seed_pending_stack_for_async_submit(j, stack_id)
                if not ok_seed:
                    self._json(400, {"ok": False, "error": seed_info.get("error", "seed failed")})
                    return
                job_id = uuid.uuid4().hex
                with place_order_jobs_lock:
                    place_order_jobs[job_id] = {
                        "done": False,
                        "http_code": 202,
                        "result": {
                            "ok": True,
                            "status": "Submitting...",
                            "stack_id": stack_id,
                            "ax_symbol": seed_info.get("ax_symbol"),
                            "hint": "Stack written to orders.json; gateway submit running in background.",
                        },
                    }
                    if len(place_order_jobs) > 200:
                        done_keys = [k for k, v in place_order_jobs.items() if bool(v.get("done"))]
                        for k in done_keys[:80]:
                            place_order_jobs.pop(k, None)
                threading.Thread(
                    target=_run_place_order_job,
                    args=(job_id, j, stack_id),
                    daemon=True,
                ).start()
                self._json(
                    202,
                    {
                        "ok": True,
                        "job_id": job_id,
                        "stack_id": stack_id,
                        "status": "Submitting...",
                        "hint": "Stack written to orders.json; gateway submit running in background.",
                    },
                )
                return

            if path == "/api/desk/place_order_async_status":
                j = _read_json_body(self)
                job_id = str(j.get("job_id", "") or "").strip()
                if not job_id:
                    self._json(400, {"ok": False, "error": "job_id is required"})
                    return
                with place_order_jobs_lock:
                    job = place_order_jobs.get(job_id)
                if not isinstance(job, dict):
                    self._json(404, {"ok": False, "error": "unknown job_id"})
                    return
                self._json(
                    200,
                    {
                        "ok": True,
                        "job_id": job_id,
                        "done": bool(job.get("done")),
                        "http_code": int(job.get("http_code") or 200),
                        "result": job.get("result") if isinstance(job.get("result"), dict) else {},
                    },
                )
                return

            if path == "/api/desk/place_order":
                # 1) Place bid+ask on the order gateway (HTTP 2xx) using the
                #    same desk skew/non-cross path as re-quote (no C++).
                # 2) On success, append the stack to `orders.json` with
                #    `desk_seeded` + OIDs & prices so `mm_req_*` adopts on
                #    startup (no duplicate place / no canceling the new legs).
                j = _read_json_body(self)
                ax = str(j.get("ax_symbol", "") or j.get("symbol", "") or "").strip()
                if not ax:
                    self._json(400, {"ok": False, "error": "ax_symbol is required"})
                    return
                # Item 3/4: a place/add into a stopped instrument requires an explicit resume
                # confirm. Without resume:true -> 409 needs_resume_confirm, place NOTHING.
                # With resume:true -> flip the flag (flag-only) then proceed with this place.
                _rg = _place_resume_decision(st, ax, j)
                if _rg["action"] == "needs_confirm":
                    self._json(409, {"ok": False, "needs_resume_confirm": True,
                                     "ax_symbol": _rg["ax_symbol"], "error": _rg["error"]})
                    return
                if _rg["action"] == "error":
                    self._json(400, {"ok": False, "error": _rg["error"]})
                    return
                theo_source = _normalize_theo_source(
                    j.get("theo_source"),
                    str(((get_merged_config_dict().get("external_feed") or {}).get("provider")) or "neon_fix"),
                )
                if theo_source not in ("mettraders", "hyperliquid", "neon_fix"):
                    self._json(400, {"ok": False, "error": f"unsupported theo_source: {theo_source!r}"})
                    return
                theo_venue = str(j.get("theo_venue_symbol", "") or "").strip()
                ref_fix = str(
                    j.get("reference_fix_symbol", "")
                    or theo_venue
                    or ""
                ).strip()
                ord_sym = str(j.get("order_symbol", "") or ax).strip()
                params_in = j.get("params") if isinstance(j.get("params"), dict) else j
                try:
                    width = int(
                        params_in.get(
                            "width_bps",
                            params_in.get("width_ticks", params_in.get("width", 0)),
                        )
                    )
                    order_size = int(params_in.get("order_size", 0))
                    adjust_position = int(params_in.get("adjust_position", 0))
                    adjust_ticks = int(params_in.get("adjust_ticks", 0))
                    min_drift = int(params_in.get("min_theo_move_ticks_to_requote",
                                                  params_in.get("min_drift_ticks", 0)))
                except (TypeError, ValueError):
                    self._json(400, {"ok": False, "error": "all params must be integers"})
                    return
                # Pricer-snapshot transform fields (see _parse_place_order_payload for full notes).
                # Defaults match the JS form: snapshots=0 disables, slope=1 = linear pass-through.
                try:
                    quote_snapshot = float(params_in.get("quote_snapshot", 0) or 0)
                    pricer_snapshot = float(params_in.get("pricer_snapshot", 0) or 0)
                    slope_v = params_in.get("slope", 1)
                    slope = float(slope_v if slope_v != "" else 1)
                except (TypeError, ValueError):
                    self._json(400, {"ok": False,
                        "error": "quote_snapshot, pricer_snapshot, slope must be numbers"})
                    return
                if quote_snapshot < 0 or pricer_snapshot < 0:
                    self._json(400, {"ok": False,
                        "error": "quote_snapshot and pricer_snapshot must be >= 0 (use 0 to disable transform)"})
                    return
                merged_cfg = get_merged_config_dict()
                inst_po, inst_rl = _instrument_max_position_and_reload_from_merged(merged_cfg, ax)
                # Client sends instrument_max_position from "Max position (instrument)"; must win over
                # YAML merged defaults so each place_order updates products[ax].max_position to the latest UI value.
                user_inst_mx = j.get("instrument_max_position", j.get("max_position"))
                if user_inst_mx is not None and user_inst_mx != "":
                    try:
                        max_position = int(user_inst_mx)
                    except (TypeError, ValueError):
                        self._json(400, {"ok": False, "error": "instrument_max_position must be an integer"})
                        return
                else:
                    max_position = inst_po
                if max_position < _MM_MAX_POSITION_LO or max_position > _MM_MAX_POSITION_HI:
                    self._json(
                        400,
                        {
                            "ok": False,
                            "error": (
                                f"instrument_max_position must be in [{_MM_MAX_POSITION_LO}, {_MM_MAX_POSITION_HI}]"
                            ),
                        },
                    )
                    return
                # Mirror _parse_place_order_payload: per-field validation so the user sees the
                # actual offending field name + value, not a single bundled message.
                out_of_range: list[str] = []
                if width < 1:
                    out_of_range.append(f"width_bps={width} (need >= 1)")
                if order_size < 1:
                    out_of_range.append(f"order_size={order_size} (need >= 1)")
                if max_position < 1:
                    out_of_range.append(f"max_position={max_position} (need >= 1)")
                if adjust_position < 1:
                    out_of_range.append(f"adjust_position={adjust_position} (need >= 1)")
                if adjust_ticks < 1:
                    out_of_range.append(f"adjust_ticks={adjust_ticks} (need >= 1)")
                if min_drift < 1:
                    out_of_range.append(f"min_theo_move_ticks_to_requote={min_drift} (need >= 1)")
                if inst_rl < 0:
                    out_of_range.append(
                        f"max_reload_cycles={inst_rl} (need >= 0; use 0 for unlimited)"
                    )
                if out_of_range:
                    self._json(400, {
                        "ok": False,
                        "error": "params out of range: " + "; ".join(out_of_range),
                    })
                    return
                stack_id = str(j.get("stack_id", "") or "").strip() or uuid.uuid4().hex[:16]
                replace_existing_stack = bool(j.get("replace_existing_stack"))
                exclude_sid_po = stack_id if replace_existing_stack else ""
                ok_mx_po, err_mx_po, _ = _validate_instrument_max_position_monotonic(
                    merged_cfg,
                    ax,
                    max_position,
                    exclude_stack_id=exclude_sid_po,
                )
                if not ok_mx_po:
                    self._json(400, {"ok": False, "error": err_mx_po})
                    return
                with _MM_ORDERS_CONFIG_LOCK:
                    doc_pre = load_mm_orders_config(merged_cfg)
                    _, ex0 = _find_stack_row_in_orders_doc(doc_pre, stack_id, ax)
                    if ex0 is not None and not replace_existing_stack:
                        self._json(
                            409,
                            {"ok": False, "error": f"stack id {stack_id} already exists (use replace_existing_stack)"},
                        )
                        return
                    # C3 (2026-06-11): check-and-mark atomically under the lock so a concurrent
                    # request for the same stack_id is rejected (409) before any gateway placement.
                    # The marker is held through completion/failure (cleared in the finally below);
                    # the lock is released here, so the gateway REST call never serializes unrelated
                    # stacks.
                    _place_key = f"{ax}::{stack_id}"
                    if _place_key in _MM_PLACE_IN_PROGRESS:
                        self._json(
                            409,
                            {"ok": False, "error": f"stack id {stack_id} placement already in progress"},
                        )
                        return
                    _MM_PLACE_IN_PROGRESS.add(_place_key)
                try:
                    pending_skeleton = {
                        "stack_id": stack_id,
                        "id": stack_id,
                        "ax_symbol": ax,
                        "theo_source": theo_source,
                        "theo_venue_symbol": theo_venue,
                        "reference_fix_symbol": ref_fix,
                        "order_symbol": ord_sym,
                        "width_bps": width,
                        "order_size": order_size,
                        "max_position": max_position,
                        "adjust_position": adjust_position,
                        "adjust_ticks": adjust_ticks,
                        "min_theo_move_ticks_to_requote": min_drift,
                        "max_reload_cycles": inst_rl,
                        "quote_snapshot": float(quote_snapshot),
                        "pricer_snapshot": float(pricer_snapshot),
                        "slope": float(slope),
                        "created_ms": int(time.time() * 1000),
                        "desk_seeded": False,
                        "mm_move_enabled": False,
                        "submit_status": "pending",
                        "bid_price": 0.0,
                        "ask_price": 0.0,
                        "bid_qty": 0,
                        "ask_qty": 0,
                        "bid_exchange_oid": "",
                        "ask_exchange_oid": "",
                    }
                    ok0, err0 = orders_json_apply_stack_row_dual_view(merged_cfg, pending_skeleton)
                    if not ok0:
                        self._json(500, {"ok": False, "error": err0 or "orders.json write failed (skeleton)"})
                        return
                    gw = desk_mm_stack_pair_place_on_gateway_then_tell_cpp(
                        st,
                        ax=ax,
                        width_ticks=width,
                        order_size=order_size,
                        max_position=max_position,
                        adjust_position=adjust_position,
                        adjust_ticks=adjust_ticks,
                        quote_snapshot=quote_snapshot,
                        pricer_snapshot=pricer_snapshot,
                        slope=slope,
                        log_context="api_place_order",
                    )
                    if not gw.get("ok"):
                        self._json(400, {"ok": False, "error": gw.get("error") or "gateway place failed", "place": gw})
                        return
                    qb = int(gw.get("qty_bid") or 0)
                    qa = int(gw.get("qty_ask") or 0)
                    bid_oid_gw = str(gw.get("bid_exchange_oid") or "").strip() if qb > 0 else ""
                    ask_oid_gw = str(gw.get("ask_exchange_oid") or "").strip() if qa > 0 else ""
                    if qb > 0 and not bid_oid_gw:
                        _cancel_stack_gateway_legs_best_effort(
                            st,
                            {
                                "bid_exchange_oid": str(gw.get("bid_exchange_oid") or ""),
                                "ask_exchange_oid": str(gw.get("ask_exchange_oid") or ""),
                            },
                        )
                        self._json(
                            400,
                            {
                                "ok": False,
                                "error": (
                                    "Gateway did not return bid exchange OID — nothing written to orders.json "
                                    "(rolled back)."
                                ),
                                "place": gw,
                            },
                        )
                        return
                    if qa > 0 and not ask_oid_gw:
                        _cancel_stack_gateway_legs_best_effort(
                            st,
                            {
                                "bid_exchange_oid": str(gw.get("bid_exchange_oid") or ""),
                                "ask_exchange_oid": str(gw.get("ask_exchange_oid") or ""),
                            },
                        )
                        self._json(
                            400,
                            {
                                "ok": False,
                                "error": (
                                    "Gateway did not return ask exchange OID — nothing written to orders.json "
                                    "(rolled back)."
                                ),
                                "place": gw,
                            },
                        )
                        return
                    bpx = float(gw.get("bid") or 0.0) if qb > 0 else 0.0
                    apx = float(gw.get("ask") or 0.0) if qa > 0 else 0.0
                    stack_entry = {
                        "id": stack_id,
                        "stack_id": stack_id,
                        "ax_symbol": ax,
                        "theo_source": theo_source,
                        "theo_venue_symbol": theo_venue,
                        "reference_fix_symbol": ref_fix,
                        "order_symbol": ord_sym,
                        "max_position": max_position,
                        "width_bps": width,
                        "order_size": order_size,
                        "adjust_position": adjust_position,
                        "adjust_ticks": adjust_ticks,
                        "min_theo_move_ticks_to_requote": min_drift,
                        "max_reload_cycles": inst_rl,
                        "quote_snapshot": float(quote_snapshot),
                        "pricer_snapshot": float(pricer_snapshot),
                        "slope": float(slope),
                        "created_ms": int(time.time() * 1000),
                        "desk_seeded": True,
                        "mm_move_enabled": True,
                        "submit_status": "active",
                        "bid_price": bpx,
                        "bid_qty": qb,
                        "bid_exchange_oid": bid_oid_gw,
                        "ask_price": apx,
                        "ask_qty": qa,
                        "ask_exchange_oid": ask_oid_gw,
                    }
                    ok_v, err_v = _validate_mm_desk_stack_row_for_cpp(stack_entry)
                    if not ok_v:
                        _cancel_stack_gateway_legs_best_effort(st, stack_entry)
                        self._json(400, {"ok": False, "error": err_v, "place": gw})
                        return
                    ok_w, err_w = orders_json_apply_stack_row_dual_view(merged_cfg, stack_entry)
                    if not ok_w:
                        write_orders_config_freeze(merged_cfg, False)
                        self._json(500, {"ok": False, "error": err_w})
                        return
                    write_orders_config_freeze(merged_cfg, False)
                    _set_desk_action(st, f"orders.json: add ax={ax} stack_id={stack_id} (after gateway 2xx)")
                    self._json(200, {
                        "ok": True,
                        "request_id": stack_id,  # kept for JS back-compat
                        "stack_id": stack_id,
                        "ax_symbol": ax,
                        "stack": stack_entry,
                        "place": gw,
                        "hint": "Gateway place OK; stack written to orders.json. C++ mm_req_* adopts the placed OIDs on the next reconcile.",
                    })
                    return
                finally:
                    with _MM_ORDERS_CONFIG_LOCK:
                        _MM_PLACE_IN_PROGRESS.discard(_place_key)

            if path == "/api/desk/set_product_max_position":
                # Set products[ax].max_position in orders.json (instrument-level cap for all stacks on that AX).
                j = _read_json_body(self)
                ax_sp = str(j.get("ax_symbol", "") or j.get("symbol", "") or "").strip()
                try:
                    mx_po = int(j.get("max_position", j.get("max_po", 0)) or 0)
                except (TypeError, ValueError):
                    self._json(400, {"ok": False, "error": "max_position must be an integer"})
                    return
                if not ax_sp or mx_po < 1:
                    self._json(400, {"ok": False, "error": "ax_symbol and max_position>=1 required"})
                    return
                merged_cfg = get_merged_config_dict()
                with _MM_ORDERS_CONFIG_LOCK:
                    doc = load_mm_orders_config(merged_cfg)
                    products = doc.setdefault("products", {})
                    prod_w = products.setdefault(ax_sp, {})
                    prod_w["max_position"] = mx_po
                    ok_w, err_w = save_mm_orders_config(merged_cfg, doc)
                if not ok_w:
                    self._json(500, {"ok": False, "error": err_w})
                    return
                _set_desk_action(st, f"orders.json: product max_position ax={ax_sp} cap={mx_po}")
                self._json(
                    200,
                    {
                        "ok": True,
                        "ax_symbol": ax_sp,
                        "max_position": mx_po,
                        "hint": "Instrument cap saved; C++ applies to all mm_req stacks on this AX on the next reconcile (~1s).",
                    },
                )
                return

            if path == "/api/desk/manual_stack_remove":
                j = _read_json_body(self)
                sym = str(j.get("symbol", "") or "").strip()
                sid = str(j.get("stack_id", "") or j.get("id", "") or "").strip()
                if not sym or not sid:
                    self._json(400, {"ok": False, "error": "symbol and stack_id are required"})
                    return
                ok_r, msg_r = desk_remove_manual_stack(st, symbol=sym, stack_id=sid)
                if not ok_r:
                    self._json(400, {"ok": False, "error": msg_r})
                    return
                desk_reload_config_into_state(st)
                self._json(200, {"ok": True, "message": msg_r})
                return

            if path == "/api/desk/cancel_one_order":
                # Two modes:
                #   (a) stack_id (or legacy `request_id`) supplied →
                #       remove the matching stack from `orders.json`. The
                #       trading_client tears down the MakeMarketStrategy
                #       on its next ~1s reconcile pass; BaseStrategy::stop
                #       cancels the AX bid+ask before unregistering, so
                #       the user's "remove the entry from the config so
                #       cpp stops moving that order" rule is satisfied.
                #   (b) only oid supplied → legacy direct AX cancel by
                #       order id, used by the Orders tab for non-managed
                #       orders (per-leg base MM, hand-placed orders).
                j = _read_json_body(self)
                stack_id = str(j.get("stack_id", "") or j.get("request_id", "") or "").strip()
                ax_symbol = str(j.get("ax_symbol", "") or j.get("symbol", "") or "").strip()
                if stack_id:
                    merged_cfg = get_merged_config_dict()
                    with _MM_ORDERS_CONFIG_LOCK:
                        _doc_pre = load_mm_orders_config(merged_cfg)
                        _pk_snap, _row_snap = _find_stack_row_in_orders_doc(
                            _doc_pre, stack_id, ax_symbol
                        )
                    if isinstance(_row_snap, dict):
                        _cancel_stack_gateway_legs_best_effort(st, _row_snap)

                    with _MM_ORDERS_CONFIG_LOCK:
                        doc = load_mm_orders_config(merged_cfg)
                        removed_count = _orders_json_remove_stack_id_everywhere(doc, stack_id, ax_symbol)
                        if removed_count == 0 and ax_symbol:
                            removed_count = _orders_json_remove_stack_id_everywhere(doc, stack_id, "")
                        ok_w, err_w = (True, "")
                        if removed_count > 0:
                            ok_w, err_w = save_mm_orders_config(merged_cfg, doc)

                    # AX symbol for the C++ force-cancel signal: prefer the caller's, else the row we
                    # matched in orders.json (either file may omit it, so both are best-effort).
                    ax_for_signal = ax_symbol
                    if not ax_for_signal and isinstance(_row_snap, dict):
                        ax_for_signal = str(
                            _row_snap.get("ax_symbol") or _row_snap.get("symbol") or ""
                        ).strip()

                    if removed_count == 0:
                        # ORPHAN PATH: the stack is not in orders.json (desk desired-state) but may
                        # still rest on the venue and be tracked by C++ (mm_orders.json). Python cannot
                        # cancel those legs itself — mm_orders.json only carries C++ *local* order ids,
                        # not exchange OIDs — so we signal C++ to force-cancel the tracked legs it owns
                        # (it holds the local-id → exchange-OID mapping). This is exactly the "empty
                        # cancel dropdown while orders rest on the exchange" recovery case.
                        live_row = _find_stack_in_mm_active_orders(merged_cfg, stack_id, ax_symbol)
                        if isinstance(live_row, dict):
                            if not ax_for_signal:
                                ax_for_signal = str(
                                    live_row.get("ax_symbol") or live_row.get("order_symbol") or ""
                                ).strip()
                            sig_ok = write_force_cancel_signal(
                                merged_cfg, ax_for_signal, stack_id, "desk_orphan_cancel"
                            )
                            _set_desk_action(
                                st,
                                f"orphan cancel: stack_id={stack_id} not in orders.json — "
                                f"signalled C++ force-cancel (mm_orders.json venue truth)",
                            )
                            self._json(
                                200,
                                {
                                    "ok": True,
                                    "stack_id": stack_id,
                                    "request_id": stack_id,
                                    "removed": 0,
                                    "orphan_recovered": True,
                                    "cpp_signal": bool(sig_ok),
                                    "hint": (
                                        "Stack was absent from orders.json but live on the venue "
                                        "(mm_orders.json). Signalled C++ to cancel the tracked legs by "
                                        "exchange OID on the mover. If it persists after ~2s, use Cancel All."
                                    ),
                                },
                            )
                            return
                        # 200 + ok:false so fetch() does not throw; client shows error from body.
                        self._json(
                            200,
                            {
                                "ok": False,
                                "error": (
                                    f"stack_id {stack_id!r} not found in orders.json or mm_orders.json"
                                ),
                            },
                        )
                        return
                    if not ok_w:
                        self._json(500, {"ok": False, "error": err_w})
                        return
                    # Belt-and-suspenders: also signal C++ to force-cancel this stack's tracked legs
                    # immediately. Removing the row from orders.json alone only makes C++ STOP MOVING
                    # the stack (desk_orders_json_stack_missing gate) — the resting legs are cancelled
                    # by the desk's exchange-OID cancel above, but this signal covers any legs C++ moved
                    # to fresh OIDs since orders.json was last patched.
                    cpp_sig = write_force_cancel_signal(merged_cfg, ax_for_signal, stack_id, "desk_cancel")
                    _set_desk_action(st, f"orders.json: cancel stack_id={stack_id} (removed {removed_count})")
                    desk_reload_config_into_state(st)
                    self._json(200, {
                        "ok": True,
                        "stack_id": stack_id,
                        "request_id": stack_id,
                        "removed": removed_count,
                        "cpp_signal": bool(cpp_sig),
                        "hint": (
                            "Venue legs cancelled from desk (exchange OIDs), row removed from orders.json, "
                            "and C++ force-cancel signalled for any legs moved to fresh OIDs."
                        ),
                    })
                    return
                oid = str(j.get("oid", "") or j.get("order_id", "") or "").strip()
                out = desk_cancel_one_order(st, oid)
                self._json(200 if out.get("ok") else 400, out)
                return

            if path == "/api/desk/orders_reset":
                j = _read_json_body(self) or {}
                if not bool(j.get("confirm")):
                    self._json(
                        400,
                        {
                            "ok": False,
                            "error": 'Set "confirm": true to cancel all known stack OIDs and wipe orders.json',
                        },
                    )
                    return
                merged_r = get_merged_config_dict()
                ok_c, msg_c, n_st = desk_clear_orders_json_cancel_gateway(st, merged_r)
                if ok_c and st is not None:
                    desk_reload_config_into_state(st)
                    _set_desk_action(st, f"orders.json: full reset ({n_st} stack row(s) cleared, venue cancel attempted)")
                self._json(
                    200 if ok_c else 500,
                    {"ok": ok_c, "message": msg_c, "stacks_touched": n_st},
                )
                return

            if path == "/api/desk/place_manual_mm":
                j = _read_json_body(self)
                sym = str(j.get("symbol", "") or "").strip()
                if not sym:
                    self._json(400, {"ok": False, "error": "symbol is required (AX product to add bid+ask for)"})
                    return
                # Item 3/4: manual-place into a stopped instrument routes through the same
                # resume-confirm. No silent place, no silent resume.
                _rg = _place_resume_decision(st, sym, j)
                if _rg["action"] == "needs_confirm":
                    self._json(409, {"ok": False, "needs_resume_confirm": True,
                                     "ax_symbol": _rg["ax_symbol"], "error": _rg["error"]})
                    return
                if _rg["action"] == "error":
                    self._json(400, {"ok": False, "error": _rg["error"]})
                    return
                if not _symbol_in_mm_instruments_st(st, sym):
                    with st.lock:
                        have = st.mm_instruments
                    if have:
                        self._json(
                            400,
                            {
                                "ok": False,
                                "error": f"symbol {sym!r} is not listed in market_maker.instruments",
                            },
                        )
                        return
                persist_on = j.get("persist_config")
                if persist_on is None:
                    persist_on = True
                persist_b = bool(persist_on)
                ok_p, msg_p, _ = _place_manual_persist_excluding_instruments(
                    st, j, persist_config=persist_b
                )
                if not ok_p:
                    self._json(400, {"ok": False, "error": msg_p})
                    return
                out = desk_place_manual_mm_pair(st, symbol=sym)
                code = 200 if out.get("ok") else 400
                self._json(
                    code,
                    {
                        **out,
                        "symbol": sym,
                        "config_message": msg_p,
                    },
                )
                return

            if path == "/api/desk/mm_gate":
                j = _read_json_body(self)
                sym_scope = str(j.get("symbol", "") or j.get("ax_symbol", "") or "").strip()
                gate_body = {
                    k: j.get(k)
                    for k in ("mm_orders_enabled", "requote_on_theo_move")
                    if k in j
                }
                ok_g, msg_g = persist_market_maker_gate_flags(gate_body, sym_scope)
                if ok_g:
                    desk_reload_config_into_state(st)
                self._json(
                    200 if ok_g else 400,
                    {"ok": ok_g, "message": msg_g, "symbol": sym_scope},
                )
                return

            if path == "/api/desk/instrument_cancel_all":
                # Per-instrument cancel-and-stop (Tim, 2026-07-23). Cancels every venue
                # order on the instrument and stops it quoting (mm_orders_enabled=false).
                # See desk_instrument_cancel_all for the D5 sequence.
                #
                # RE-ENABLE IS INTENTIONALLY NOT EXPOSED (Tim, 2026-07-26): the re-enable
                # path flipped mm_orders_enabled back true while the old stacks were still
                # in orders.json, so the engine re-quoted them AND any freshly-placed pair
                # double-placed. The button was removed and this route now hard-forces
                # cancel (hold=True) — a `hold:false` payload can no longer re-enable.
                j = _read_json_body(self)
                ax_in = str(j.get("ax_symbol", "") or j.get("symbol", "") or "").strip()
                if not ax_in:
                    self._json(400, {"ok": False, "error": "ax_symbol required"})
                    return
                res = desk_instrument_cancel_all(st, ax_in, hold=True)
                # Any run that executed the D5 sequence (has "steps") returns 200 carrying
                # the full per-step report + residual OIDs — even when NOT CLEAN — so the
                # UI can render residuals rather than have the fetch helper throw the body
                # away. The UI reads res.clean and shows NOT CLEAN explicitly, so an
                # unverified state can never be mistaken for success. Pure input-validation
                # failures (missing/unknown symbol) return 400.
                ran_sequence = ("steps" in res) or bool(res.get("ok"))
                self._json(200 if ran_sequence else 400, res)
                return

            if path == "/api/desk/instrument_hold_options":
                # D6 dropdown: union of configured instruments and live-at-venue symbols,
                # with fresh open-order counts + held state (for the confirmation modal).
                self._json(200, instrument_hold_options(st))
                return

            if path == "/api/desk/instrument_resume":
                # Item 3 RESUME — FLAG-ONLY. Sets mm_orders_enabled=true via the hardened
                # mutate-write and places NOTHING (see desk_instrument_resume). The UI calls
                # this on an explicit "resume quoting?" confirm; the actual add/place is then
                # re-submitted by the client (or was carried inline via resume:true on the
                # place route). Placing zero orders here is the safety property.
                j = _read_json_body(self)
                ax_in = str(j.get("ax_symbol", "") or j.get("symbol", "") or "").strip()
                if not ax_in:
                    self._json(400, {"ok": False, "error": "ax_symbol required"})
                    return
                res = desk_instrument_resume(st, ax_in)
                self._json(200 if res.get("ok") else 400, res)
                return

            if path == "/api/desk/fast_market_get":
                # Item 6: current market_maker.fast_market.* (read-only render source).
                self._json(200, fast_market_config_get())
                return

            if path == "/api/desk/fast_market_save":
                # Item 6: validate + write fast_market.* via the hardened mutate-write only.
                j = _read_json_body(self)
                ok_s, msg_s = fast_market_config_save(j if isinstance(j, dict) else {})
                self._json(200 if ok_s else 400, {"ok": ok_s, "message": msg_s})
                return

            if path == "/api/desk/orders_mm_move":
                j = _read_json_body(self)
                ax_in = str(j.get("ax_symbol", "") or j.get("symbol", "") or "").strip()
                scope = str(j.get("scope", "") or "").strip().lower()
                if not ax_in or scope not in ("global", "stack"):
                    self._json(
                        400,
                        {
                            "ok": False,
                            "error": "need ax_symbol and scope (global|stack)",
                        },
                    )
                    return
                merged_om = get_merged_config_dict()
                if scope == "global":
                    if "mm_move_all_enabled" not in j:
                        self._json(
                            400,
                            {"ok": False, "error": "need mm_move_all_enabled (boolean)"},
                        )
                        return
                    val_b = _orders_json_boolish(j.get("mm_move_all_enabled"), True)
                else:
                    stid = str(j.get("stack_id", "") or j.get("id", "") or "").strip()
                    if not stid:
                        self._json(
                            400,
                            {"ok": False, "error": "need stack_id when scope=stack"},
                        )
                        return
                    if "mm_move_enabled" not in j:
                        self._json(
                            400,
                            {"ok": False, "error": "need mm_move_enabled (boolean)"},
                        )
                        return
                    val_b = _orders_json_boolish(j.get("mm_move_enabled"), True)
                with _MM_ORDERS_CONFIG_LOCK:
                    doc = load_mm_orders_config(merged_om)
                    products = doc.setdefault("products", {})
                    if not isinstance(products, dict):
                        doc["products"] = {}
                        products = doc["products"]
                    pk = _orders_json_resolve_product_key(products, ax_in)
                    if pk is None:
                        self._json(
                            404,
                            {
                                "ok": False,
                                "error": f"ax_symbol {ax_in!r} not in orders.json products",
                            },
                        )
                        return
                    prod = products.get(pk)
                    if not isinstance(prod, dict):
                        self._json(400, {"ok": False, "error": "invalid product bucket"})
                        return
                    if scope == "global":
                        prod["mm_move_all_enabled"] = val_b
                    else:
                        stacks = prod.get("stacks")
                        if not isinstance(stacks, list):
                            self._json(
                                400,
                                {"ok": False, "error": "product has no stacks array"},
                            )
                            return
                        found = False
                        for row in stacks:
                            if isinstance(row, dict) and _desk_stack_id_matches_row(row, stid):
                                row["mm_move_enabled"] = val_b
                                found = True
                                break
                        if not found:
                            self._json(
                                404,
                                {
                                    "ok": False,
                                    "error": f"stack_id {stid!r} not found under product",
                                },
                            )
                            return
                        root = doc.setdefault("stacks", [])
                        if isinstance(root, list):
                            for row in root:
                                if isinstance(row, dict) and _desk_stack_id_matches_row(row, stid):
                                    row["mm_move_enabled"] = val_b
                                    break
                    ok_w, err_w = save_mm_orders_config(merged_om, doc)
                if ok_w and st is not None:
                    detail = (
                        f"mm_move_all_enabled={val_b}"
                        if scope == "global"
                        else f"stack_id={stid} mm_move_enabled={val_b}"
                    )
                    _set_desk_action(st, f"orders.json mm_move scope={scope} ax={pk} {detail}")
                self._json(
                    200 if ok_w else 400,
                    {
                        "ok": ok_w,
                        "message": err_w or "ok",
                        "ax_symbol": pk,
                        "scope": scope,
                    },
                )
                return

            if path == "/api/desk/templates_save":
                j = _read_json_body(self)
                if not isinstance(j, dict):
                    self._json(400, {"ok": False, "error": "invalid JSON payload"})
                    return
                instrument = str(
                    j.get("instrument", "") or j.get("ax_symbol", "") or j.get("symbol", "") or ""
                ).strip()
                if not instrument:
                    self._json(400, {"ok": False, "error": "instrument is required"})
                    return
                name = str(j.get("name", "") or "").strip()
                old_name = str(j.get("old_name", "") or "").strip()
                params = j.get("params") if isinstance(j.get("params"), dict) else {}
                ok_t, info_t = mm_templates_upsert(
                    get_merged_config_dict(), instrument, name, params, old_name=old_name
                )
                if not ok_t:
                    self._json(400, {"ok": False, "error": info_t.get("error", "save failed")})
                    return
                if st is not None:
                    try:
                        _set_desk_action(
                            st,
                            f"template saved: {(info_t.get('template') or {}).get('name', name)}",
                        )
                    except Exception:
                        pass
                self._json(
                    200,
                    {
                        "ok": True,
                        "template": info_t.get("template", {}),
                        "templates": info_t.get("templates", {}),
                    },
                )
                return

            if path == "/api/desk/templates_delete":
                j = _read_json_body(self)
                instrument = str(
                    j.get("instrument", "") or j.get("ax_symbol", "") or j.get("symbol", "") or ""
                ).strip()
                name = str(j.get("name", "") or "").strip()
                ok_d, info_d = mm_templates_delete(get_merged_config_dict(), instrument, name)
                if not ok_d:
                    self._json(400, {"ok": False, "error": info_d.get("error", "delete failed")})
                    return
                self._json(200, {"ok": True, "templates": info_d.get("templates", {})})
                return

            if path == "/api/desk/orders_config_freeze":
                j = _read_json_body(self)
                raw = j.get("active")
                if raw is None:
                    self._json(400, {"ok": False, "error": "need active (boolean)"})
                    return
                if isinstance(raw, str):
                    en = raw.strip().lower() in ("1", "true", "yes", "on")
                else:
                    en = bool(raw)
                merged_f = get_merged_config_dict()
                ok_f, msg_f, seq_f = write_orders_config_freeze(merged_f, en)
                if ok_f and st is not None:
                    _set_desk_action(st, f"orders.json freeze signal active={en} seq={seq_f}")
                self._json(
                    200 if ok_f else 400,
                    {"ok": ok_f, "message": msg_f, "seq": seq_f, "active": en},
                )
                return

            if path == "/api/desk/mm_instruments":
                j = _read_json_body(self)
                inst = j.get("instruments")
                if not isinstance(inst, list):
                    self._json(400, {"ok": False, "error": "instruments must be a list"})
                    return
                ok_w, msg_w, preview = persist_mm_instruments_multi(st, inst)
                if not ok_w:
                    self._json(400, {"ok": False, "error": msg_w})
                    return
                stop_feeds()
                desk_reload_config_into_state(st)
                ensure_ax_session(st)
                start_feeds()
                with st.lock:
                    st.status_line = "MM instruments saved — reloading page"
                self._json(
                    200,
                    {
                        "ok": True,
                        "reload": True,
                        "config_message": msg_w,
                        "book_preview": preview,
                    },
                )
                return

            if path == "/api/desk/instruments":
                j = _read_json_body(self)
                ax = str(j.get("ax_symbol", "") or "").strip()
                bn = str(j.get("ref_symbol") or j.get("bn_symbol", "") or "").strip()
                er = j.get("ax_orderbook_extras")
                if isinstance(er, str):
                    extras = [x.strip() for x in er.split(",") if x.strip()]
                elif isinstance(er, list):
                    extras = [str(x).strip() for x in er if str(x).strip()]
                else:
                    extras = []
                if not ax and not bn and not extras:
                    self._json(400, {"ok": False, "error": "need ax_symbol and/or ref_symbol / extras"})
                    return
                ok_w, msg_w = persist_desk_instruments_to_repo(
                    st,
                    ax_symbol=ax,
                    bn_symbol=bn,
                    ax_orderbook_extras=extras,
                )
                if not ok_w:
                    self._json(400, {"ok": False, "error": msg_w})
                    return
                stop_feeds()
                desk_reload_config_into_state(st)
                ensure_ax_session(st)
                start_feeds()
                with st.lock:
                    st.status_line = "Instruments saved — page reload recommended"
                self._json(200, {"ok": True, "reload": True, "config_message": msg_w})
                return

            if path == "/api/desk/neon_md_symbols":
                j = _read_json_body(self)
                raw = j.get("md_symbols")
                if isinstance(raw, str):
                    syms = [x.strip() for x in raw.split(",") if x.strip()]
                elif isinstance(raw, list):
                    syms = [str(x).strip() for x in raw if str(x).strip()]
                else:
                    syms = []
                if not syms:
                    self._json(400, {"ok": False, "error": "need md_symbols (non-empty list or comma string)"})
                    return
                ok_n, msg_n = persist_neon_fix_md_symbols(st, syms)
                if not ok_n:
                    self._json(400, {"ok": False, "error": msg_n})
                    return
                stop_feeds()
                desk_reload_config_into_state(st)
                start_feeds()
                with st.lock:
                    st.status_line = "Neon FIX md_symbols saved — reload recommended"
                self._json(200, {"ok": True, "reload": True, "config_message": msg_n})
                return

            self.send_error(404)

        def _json(self, code: int, obj: dict) -> None:
            # Client may close the socket early (timeout, tab close, duplicate click) while we still
            # finish gateway work — flush_headers/end_headers can raise BrokenPipeError; body write is
            # already guarded in _safe_wfile_write.
            payload = json.dumps(obj).encode("utf-8")
            try:
                self.send_response(code)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
            except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
                return
            _safe_wfile_write(self, payload)

    print("MM Live Desk — web UI (no tkinter)", flush=True)
    print("Entry script:", SCRIPT_PATH_MAIN, flush=True)
    print("Core module:", Path(__file__).resolve(), flush=True)
    if st.web_desk_run_reference_feed:
        print(
            (
                (
                    "Reference market: Neon FIX — direct TLS from Python (external_feed.fix.direct_tls) or TCP to stunnel. "
                    "Do not run this while C++ trading_client uses the same Neon credentials (second session often fails TLS→FIX)."
                )
                if st.reference_provider == "neon_fix"
                else (
                    f"Reference market: REST depth polling every {REST_POLL_INTERVAL_SEC}s "
                    f"(external_feed.rest_url + symbol; set rest_uses_spot_ticker_path for /api/v3 vs /fapi/v1 paths)"
                )
            ),
            flush=True,
        )
    else:
        if st.reference_provider == "neon_fix":
            print(
                "Reference market: Neon depth/theo from C++ JSON (logs/mm_external_*.json, same cwd). "
                "Set MM_LIVE_DESK_WEB_REFERENCE_FEED=1 only for a dedicated Python-only Neon session (Security List / desk FIX).",
                flush=True,
            )
        else:
            print(
                "Reference market: column hidden (not neon_fix). MM_LIVE_DESK_WEB_REFERENCE_FEED=1 for REST depth in desk.",
                flush=True,
            )
    print(
        f"Architect REST: books every {MM_DESK_AX_BOOK_POLL_SEC}s · orders/fills every {MM_DESK_ACCOUNT_POLL_SEC}s "
        "(MM_DESK_AX_BOOK_POLL_SEC · MM_DESK_ACCOUNT_POLL_SEC; legacy MM_DESK_AX_POLL_SEC sets book interval)",
        flush=True,
    )

    try:
        _boot_merged = get_merged_config_dict()
        _rec_stats = orders_json_reconcile_dual_views_at_startup(_boot_merged)
        if _rec_stats.get("error"):
            print(f"[mm_live_desk] [ORDERS_JSON] startup reconcile failed: {_rec_stats.get('error')}", flush=True)
        elif _rec_stats.get("saved"):
            print(
                "[mm_live_desk] [ORDERS_JSON] startup reconcile: "
                f"removed_abandoned={_rec_stats.get('removed_abandoned', 0)} "
                f"root_deduped_extra={_rec_stats.get('root_deduped', 0)} "
                f"products_resynced={_rec_stats.get('products_resynced', 0)}",
                flush=True,
            )
    except Exception as _ex:
        print(f"[mm_live_desk] [ORDERS_JSON] startup reconcile skipped: {_ex!r}", flush=True)

    httpd = ThreadingHTTPServer((http_bind, port), H)
    srv_thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    srv_thread.start()
    time.sleep(0.12)
    desk_log(
        None,
        f"HTTP bind {http_bind}:{port} — open http://{browser_host}:{port}/ (use this URL if localhost misbehaves); "
        f"MM_DESK_HTTP_BIND to change; MM_DESK_VERBOSE=1; MM_DESK_AX_MAX_SYMBOLS={AX_MAX_SYMBOLS_ALL}",
    )

    ok_ax, auth_msg = ensure_ax_session(st)
    desk_log(None, f"Architect auth: {'OK — ' if ok_ax else ''}{auth_msg}")
    with st.lock:
        st.status_line = f"Auto-started — {auth_msg}"

    bn_pf, ax_pf, og_pf = run_connectivity_preflight(st)
    strict_pf = os.environ.get("MM_LIVE_DESK_STRICT_PREFLIGHT", "").strip().lower() in ("1", "true", "yes")
    skip_browser = strict_pf and not (bn_pf and ax_pf)

    start_feeds()

    url = f"http://{browser_host}:{port}/?desk={index_html_sha16}"
    no_br = os.environ.get("MM_LIVE_DESK_NO_BROWSER", "").strip() in ("1", "true", "yes")
    if skip_browser:
        print(
            f"[mm_live_desk] STRICT preflight failed (reference feed OK={bn_pf}, AX book OK={ax_pf}) — "
            f"not opening browser. Fix connectivity or unset MM_LIVE_DESK_STRICT_PREFLIGHT. URL: {url}",
            flush=True,
        )
    elif not no_br:

        def _open_browser() -> None:
            time.sleep(0.35)
            webbrowser.open(url)

        threading.Thread(target=_open_browser, daemon=True).start()
        print(f"Opening browser → {url}", flush=True)
        print(
            "[mm_live_desk] If the page shows OLD labels (no grey wire strip under header, wrong buttons), "
            "the browser cached another app: clear site data for 127.0.0.1 or open the URL above in a private window.",
            flush=True,
        )
        print(
            "[mm_live_desk] If Trading stays on “Waiting…” but logs show depth OK: DevTools → Network → /api/state must be 200 JSON "
            "(run curl http://127.0.0.1:PORT/api/state). Stay on the same host as the terminal URL (127.0.0.1 vs localhost).",
            flush=True,
        )
        if not og_pf and ok_ax:
            print(
                "[mm_live_desk] Note: order-gateway preflight failed — open orders may be blank until network improves.",
                flush=True,
            )
    else:
        print(f"Browser disabled (MM_LIVE_DESK_NO_BROWSER) — open {url}", flush=True)

    try:
        while True:
            time.sleep(1.0)
    except KeyboardInterrupt:
        print("\nShutting down…", flush=True)
    finally:
        try:
            stop_feeds()
        except KeyboardInterrupt:
            print("[mm_live_desk] Interrupt during shutdown — closing Neon socket and exiting.", flush=True)
            stop.set()
            desk_interrupt_neon_fix_socket(st)
        try:
            httpd.shutdown()
        except KeyboardInterrupt:
            desk_interrupt_neon_fix_socket(st)
        except Exception:
            pass
        try:
            srv_thread.join(timeout=1.0)
        except KeyboardInterrupt:
            print("[mm_live_desk] Interrupt while stopping HTTP server — exiting.", flush=True)
            desk_interrupt_neon_fix_socket(st)
        try:
            httpd.server_close()
        except KeyboardInterrupt:
            desk_interrupt_neon_fix_socket(st)
        except Exception:
            pass
