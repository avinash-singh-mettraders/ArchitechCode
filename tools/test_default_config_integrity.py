#!/usr/bin/env python3
"""Tests for default_config.json write integrity (tools/mm_live_desk_core.py).

Dependency-free: run directly with ``python3 tools/test_default_config_integrity.py``.
Exits non-zero on first failure.

``default_config.json`` holds the ENTIRE engine config and is about to become part of a
live safety control (the per-instrument ``mm_orders_enabled`` hold used by "Per-Instrument
Cancel All"). A clobber of this file is strictly worse than the July orders.json incident.
These lock down the desk-side hardening in ``_default_config_mutate_write`` /
``persist_market_maker_gate_flags``:

  1. TARGETED-KEY MUTATION preserves every other top-level key and every other instrument
     row (pure read-modify-write, never a partial in-memory view).
  2. UNREADABLE / UNPARSEABLE DISK is refused, leaving the file byte-for-byte intact
     (a torn read must never be atomically republished as a truncated document).
  3. MISSING FILE is refused.
  4. SYMBOL-NOT-FOUND is refused and writes nothing.
  5. TOP-LEVEL-KEY-DROP GUARD refuses a write that would drop a key present on disk —
     with a NEGATIVE CONTROL proving the drop actually happens when the guard is disabled
     (i.e. the guard is load-bearing, not decorative).

The engine-side half of the fix (atomic ``Config::saveToFile``) is covered in C++.
"""

from __future__ import annotations

import json
import sys
import tempfile
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


def _seed_default_config() -> dict:
    """A realistic multi-top-level-key default_config.json with two MM instruments."""
    return {
        "trading": {"max_orders_per_second": 50},
        "logging": {"level": "info"},
        "startup": {"fetch_ax_gateway_instruments": True},
        "external_feed": {"provider": "binance"},
        "market_maker": {
            "enabled": True,
            "mm_orders_enabled": True,
            "basis": 0.0,
            "instruments": [
                {"symbol": "XAU-PERP", "mm_orders_enabled": True, "requote_on_theo_move": True},
                {"symbol": "XAG-PERP", "mm_orders_enabled": True, "requote_on_theo_move": True},
            ],
        },
    }


def _write(path: Path, doc: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(doc, indent=2), encoding="utf-8")


def _ondisk(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


# 1. Targeted-key mutation preserves every other top-level key + sibling instrument.
def test_targeted_mutation_preserves_all_other_keys() -> None:
    with tempfile.TemporaryDirectory() as d:
        p = Path(d) / "default_config.json"
        seed = _seed_default_config()
        _write(p, seed)
        orig = core.DEFAULT_CONFIG_PATH
        core.DEFAULT_CONFIG_PATH = p
        try:
            ok, msg = core.persist_market_maker_gate_flags(
                {"mm_orders_enabled": False}, symbol="XAU-PERP"
            )
        finally:
            core.DEFAULT_CONFIG_PATH = orig
        check(ok, f"per-instrument gate write accepted (msg={msg!r})")
        disk = _ondisk(p)
        # Every non-market_maker top-level key is byte-identical.
        for k in ("trading", "logging", "startup", "external_feed"):
            check(disk.get(k) == seed.get(k), f"top-level key {k!r} preserved unchanged")
        insts = disk["market_maker"]["instruments"]
        xau = next(i for i in insts if i["symbol"] == "XAU-PERP")
        xag = next(i for i in insts if i["symbol"] == "XAG-PERP")
        check(xau["mm_orders_enabled"] is False, "target XAU-PERP.mm_orders_enabled -> False")
        check(xau["requote_on_theo_move"] is True, "target XAU-PERP other field untouched")
        check(
            xag == {"symbol": "XAG-PERP", "mm_orders_enabled": True, "requote_on_theo_move": True},
            "sibling XAG-PERP row completely untouched",
        )
        check(disk["market_maker"]["basis"] == 0.0, "market_maker sibling key 'basis' preserved")


# 1b. Global (no-symbol) gate write also preserves all other keys.
def test_global_gate_write_preserves_keys() -> None:
    with tempfile.TemporaryDirectory() as d:
        p = Path(d) / "default_config.json"
        seed = _seed_default_config()
        _write(p, seed)
        orig = core.DEFAULT_CONFIG_PATH
        core.DEFAULT_CONFIG_PATH = p
        try:
            ok, _ = core.persist_market_maker_gate_flags({"mm_orders_enabled": False})
        finally:
            core.DEFAULT_CONFIG_PATH = orig
        check(ok, "global gate write accepted")
        disk = _ondisk(p)
        check(disk["market_maker"]["mm_orders_enabled"] is False, "global mm_orders_enabled -> False")
        for k in ("trading", "logging", "startup", "external_feed"):
            check(disk.get(k) == seed.get(k), f"top-level key {k!r} preserved on global write")
        check(len(disk["market_maker"]["instruments"]) == 2, "instruments list preserved")


# 2. Unreadable/unparseable disk is refused and the file is left byte-for-byte intact.
def test_unparseable_disk_refused() -> None:
    with tempfile.TemporaryDirectory() as d:
        p = Path(d) / "default_config.json"
        corrupt = '{ "market_maker": {"instruments": [ }}} not json'
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(corrupt, encoding="utf-8")
        orig = core.DEFAULT_CONFIG_PATH
        core.DEFAULT_CONFIG_PATH = p
        try:
            ok, err = core.persist_market_maker_gate_flags(
                {"mm_orders_enabled": False}, symbol="XAU-PERP"
            )
        finally:
            core.DEFAULT_CONFIG_PATH = orig
        check(not ok, "write over an unparseable default_config.json is refused")
        check("unparseable" in (err or "").lower(), f"refusal cites unparseable (err={err!r})")
        check(p.read_text(encoding="utf-8") == corrupt, "corrupt file left byte-for-byte intact")


# 3. Missing file is refused.
def test_missing_file_refused() -> None:
    with tempfile.TemporaryDirectory() as d:
        p = Path(d) / "does_not_exist.json"
        orig = core.DEFAULT_CONFIG_PATH
        core.DEFAULT_CONFIG_PATH = p
        try:
            ok, err = core.persist_market_maker_gate_flags({"mm_orders_enabled": False})
        finally:
            core.DEFAULT_CONFIG_PATH = orig
        check(not ok, "write with no on-disk default_config.json is refused")
        check("missing" in (err or "").lower(), f"refusal cites missing (err={err!r})")
        check(not p.exists(), "no file created by a refused write")


# 4. Symbol not present in instruments is refused and writes nothing.
def test_symbol_not_found_refused() -> None:
    with tempfile.TemporaryDirectory() as d:
        p = Path(d) / "default_config.json"
        seed = _seed_default_config()
        _write(p, seed)
        before = p.read_text(encoding="utf-8")
        orig = core.DEFAULT_CONFIG_PATH
        core.DEFAULT_CONFIG_PATH = p
        try:
            ok, err = core.persist_market_maker_gate_flags(
                {"mm_orders_enabled": False}, symbol="NOT-A-SYMBOL"
            )
        finally:
            core.DEFAULT_CONFIG_PATH = orig
        check(not ok, "unknown symbol gate write is refused")
        check("not found" in (err or "").lower(), f"refusal cites not found (err={err!r})")
        check(p.read_text(encoding="utf-8") == before, "file unchanged after refused unknown-symbol write")


# 5. Top-level-key-drop guard + NEGATIVE CONTROL.
def test_key_drop_guard_and_negative_control() -> None:
    with tempfile.TemporaryDirectory() as d:
        p = Path(d) / "default_config.json"
        _write(p, _seed_default_config())
        orig = core.DEFAULT_CONFIG_PATH
        core.DEFAULT_CONFIG_PATH = p

        def _drop_trading(dc: dict) -> tuple[bool, str]:
            dc.pop("trading", None)  # simulate a mutate bug / partial-load republish
            return True, "dropped trading"

        try:
            # Guard ON: the drop is refused, file left intact.
            ok, err = core._default_config_mutate_write(_drop_trading)
            check(not ok, "key-drop write refused with guard enabled")
            check("drop top-level" in (err or "").lower(), f"refusal cites key drop (err={err!r})")
            check("trading" in _ondisk(p), "'trading' still present on disk after refused drop")

            # NEGATIVE CONTROL: with the guard disabled the drop actually lands. This proves
            # the guard is load-bearing — if someone deletes the guard, THIS is the regression.
            ok2, _ = core._default_config_mutate_write(_drop_trading, _skip_key_drop_guard=True)
            check(ok2, "with guard disabled the write is accepted (negative control)")
            check("trading" not in _ondisk(p), "guard disabled -> 'trading' key IS lost (regression reproduced)")
        finally:
            core.DEFAULT_CONFIG_PATH = orig


def main() -> int:
    print("=== default_config.json write-integrity tests (2026-07-23) ===")
    for fn in (
        test_targeted_mutation_preserves_all_other_keys,
        test_global_gate_write_preserves_keys,
        test_unparseable_disk_refused,
        test_missing_file_refused,
        test_symbol_not_found_refused,
        test_key_drop_guard_and_negative_control,
    ):
        print(f"[RUN ] {fn.__name__}")
        fn()
    if _failures:
        print(f"[SUITE FAIL] default_config_integrity — {_failures} failure(s)")
        return 1
    print("[SUITE OK] default_config_integrity — all checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
