#!/usr/bin/env python3
"""Tests for the Fast-market breaker GUI config (Item 6, tools/mm_live_desk_core.py).

Dependency-free: run directly with ``python3 tools/test_fast_market_config.py``.
Exits non-zero on first failure.

These lock down the desk-side Save path for market_maker.fast_market.*:

  1. VALID EDIT ROUND-TRIPS — a valid payload writes through the hardened mutate-write
     and reads back byte-identical via fast_market_config_get; every other config key
     (top-level AND inside market_maker) is preserved.
  2. ENABLED TOGGLE — enabled=false persists WITHOUT blanking the symbol; enabled=true
     round-trips.
  3. NEGATIVE CONTROLS — an out-of-range number, a blank symbol, a non-positive tick,
     and a non-integer are each rejected AND leave the file byte-for-byte untouched.
  4. KEY PRESERVATION — the write never drops sibling keys (mutate-write only touches
     the fast_market subkeys); a save leaves all other market_maker + top-level keys.

The engine reads this block live per ~1 Hz tick after its mtime-gated primary-config
reload (FastMarketMonitor::onMoverTick), so a Save is picked up within ~1-2s without a
restart. The enabled->config_disabled mapping itself is covered by the C++ suite
(tests/mwr_breaker_test.cpp::test_fast_mkt_enabled_toggle_gates).
"""

from __future__ import annotations

import json
import sys
import tempfile
from contextlib import contextmanager
from pathlib import Path

_TOOLS_DIR = str(Path(__file__).resolve().parent)
if _TOOLS_DIR not in sys.path:
    sys.path.insert(0, _TOOLS_DIR)

import mm_live_desk_core as core  # noqa: E402

_failures = 0


def check(cond: bool, msg: str) -> None:
    global _failures
    if cond:
        print(f"  [PASS] {msg}")
    else:
        print(f"  [FAIL] {msg}")
        _failures += 1


def _seed() -> dict:
    """A full-ish config so we can prove unrelated keys survive a fast_market save."""
    return {
        "trading": {"max_orders_per_second": 50},
        "external_feed": {"provider": "neon_fix"},
        "market_maker": {
            "enabled": True,
            "mm_orders_enabled": True,
            "instruments": [{"symbol": "XAU-PERP", "mm_orders_enabled": True}],
            "fast_market": {
                "hl_spx_symbol": "HL SPX",
                "size_of_move_ticks": 5,
                "time_of_move_sec": 10,
                "pull_sec": 15,
                "tick_size": 0.25,
                "enabled": True,
            },
        },
    }


def _write(path: Path, doc: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(doc, indent=2), encoding="utf-8")


@contextmanager
def _patched(**overrides):
    saved = {}
    _missing = object()
    for k, v in overrides.items():
        saved[k] = getattr(core, k, _missing)
        setattr(core, k, v)
    try:
        yield
    finally:
        for k, v in saved.items():
            if v is _missing:
                delattr(core, k)
            else:
                setattr(core, k, v)


def test_valid_edit_round_trips() -> None:
    with tempfile.TemporaryDirectory() as d:
        p = Path(d) / "default_config.json"
        _write(p, _seed())
        with _patched(DEFAULT_CONFIG_PATH=p):
            ok, msg = core.fast_market_config_save({
                "hl_spx_symbol": "  ES1!  ",  # trims to ES1!
                "size_of_move_ticks": 8,
                "time_of_move_sec": 20,
                "pull_sec": 30,
                "tick_size": 0.5,
                "enabled": True,
            })
            check(ok is True, f"valid payload saved ok ({msg})")
            got = core.fast_market_config_get()
        check(got["hl_spx_symbol"] == "ES1!", "symbol trimmed + round-trips")
        check(got["size_of_move_ticks"] == 8, "size_of_move_ticks round-trips")
        check(got["time_of_move_sec"] == 20, "time_of_move_sec round-trips")
        check(got["pull_sec"] == 30, "pull_sec round-trips")
        check(got["tick_size"] == 0.5, "tick_size round-trips")
        check(got["enabled"] is True, "enabled round-trips")
        # Sibling keys preserved.
        dc = json.loads(p.read_text(encoding="utf-8"))
        check(dc.get("trading", {}).get("max_orders_per_second") == 50, "unrelated top-level key preserved")
        check(dc["market_maker"]["mm_orders_enabled"] is True, "sibling market_maker key preserved")
        check(dc["market_maker"]["instruments"][0]["symbol"] == "XAU-PERP", "instruments preserved")


def test_enabled_toggle_keeps_symbol() -> None:
    with tempfile.TemporaryDirectory() as d:
        p = Path(d) / "default_config.json"
        _write(p, _seed())
        with _patched(DEFAULT_CONFIG_PATH=p):
            ok, _ = core.fast_market_config_save({
                "hl_spx_symbol": "HL SPX",
                "size_of_move_ticks": 5,
                "time_of_move_sec": 10,
                "pull_sec": 15,
                "tick_size": 0.25,
                "enabled": False,  # turn OFF without blanking symbol
            })
            check(ok is True, "enabled=false save ok")
            got = core.fast_market_config_get()
        check(got["enabled"] is False, "breaker turned OFF")
        check(got["hl_spx_symbol"] == "HL SPX", "symbol PRESERVED when disabled (not blanked)")


def test_out_of_range_rejected_file_untouched() -> None:
    with tempfile.TemporaryDirectory() as d:
        p = Path(d) / "default_config.json"
        _write(p, _seed())
        before = p.read_text(encoding="utf-8")
        with _patched(DEFAULT_CONFIG_PATH=p):
            # size_of_move_ticks below bound.
            ok1, msg1 = core.fast_market_config_save({
                "hl_spx_symbol": "HL SPX", "size_of_move_ticks": 0,
                "time_of_move_sec": 10, "pull_sec": 15, "tick_size": 0.25, "enabled": True,
            })
            check(ok1 is False and "out of range" in msg1, "size_of_move_ticks=0 rejected")
            # tick_size non-positive.
            ok2, msg2 = core.fast_market_config_save({
                "hl_spx_symbol": "HL SPX", "size_of_move_ticks": 5,
                "time_of_move_sec": 10, "pull_sec": 15, "tick_size": 0.0, "enabled": True,
            })
            check(ok2 is False and "tick_size" in msg2, "tick_size=0 rejected")
            # blank symbol.
            ok3, msg3 = core.fast_market_config_save({
                "hl_spx_symbol": "  ", "size_of_move_ticks": 5,
                "time_of_move_sec": 10, "pull_sec": 15, "tick_size": 0.25, "enabled": True,
            })
            check(ok3 is False and "hl_spx_symbol" in msg3, "blank symbol rejected")
            # non-integer window.
            ok4, msg4 = core.fast_market_config_save({
                "hl_spx_symbol": "HL SPX", "size_of_move_ticks": 5,
                "time_of_move_sec": "soon", "pull_sec": 15, "tick_size": 0.25, "enabled": True,
            })
            check(ok4 is False and "integer" in msg4, "non-integer window rejected")
        after = p.read_text(encoding="utf-8")
        check(before == after, "NEGATIVE CONTROL: every rejected save left the file byte-for-byte untouched")


def test_save_never_drops_keys() -> None:
    # The mutate-write's key-drop guard protects top-level keys; the fast_market mutate
    # only touches its own subkeys, so nothing else can vanish.
    with tempfile.TemporaryDirectory() as d:
        p = Path(d) / "default_config.json"
        seed = _seed()
        seed["risk_limits"] = {"max_notional": 1_000_000}  # an extra top-level key
        _write(p, seed)
        with _patched(DEFAULT_CONFIG_PATH=p):
            ok, _ = core.fast_market_config_save({
                "hl_spx_symbol": "HL SPX", "size_of_move_ticks": 6,
                "time_of_move_sec": 12, "pull_sec": 18, "tick_size": 0.1, "enabled": True,
            })
            check(ok is True, "save ok")
        dc = json.loads(p.read_text(encoding="utf-8"))
        check(set(dc.keys()) == {"trading", "external_feed", "market_maker", "risk_limits"},
              "all top-level keys preserved after a fast_market save")
        check(dc["risk_limits"]["max_notional"] == 1_000_000, "unrelated top-level value intact")
        check(dc["market_maker"]["fast_market"]["size_of_move_ticks"] == 6, "the intended edit landed")


def main() -> int:
    print("=== fast-market breaker config tests (2026-07-27) ===")
    for fn in (
        test_valid_edit_round_trips,
        test_enabled_toggle_keeps_symbol,
        test_out_of_range_rejected_file_untouched,
        test_save_never_drops_keys,
    ):
        print(f"[RUN ] {fn.__name__}")
        fn()
    if _failures:
        print(f"[SUITE FAIL] fast_market_config — {_failures} failure(s)")
        return 1
    print("[SUITE OK] fast_market_config — all checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
