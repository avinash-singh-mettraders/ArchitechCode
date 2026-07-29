#!/usr/bin/env python3
"""Tests for orders.json write integrity (tools/mm_live_desk_core.py).

Dependency-free: run directly with ``python3 tools/test_orders_json_integrity.py``.
Exits non-zero on first failure.

These lock down the desk-side half of the "orders pulled automatically" fix
(2026-07-15). The proven incident was a GUI that saved its partial in-memory
state (1 stack) over a file holding 12 live stacks; the engine then mirrored the
shrunk desired-state and mass-cancelled 24 live venue orders. The fix makes such
a write impossible at the single write chokepoint ``save_mm_orders_config``:

  1. GENERATION CAS  — a save may only land on top of the exact on-disk
     generation it was loaded from; a stale/foreign generation is refused
     (ORDERS_JSON_GEN_CAS_GUARD), so a fresh/second GUI instance can never
     overwrite a newer file.
  2. NON-CLOBBER ON UNREADABLE DISK — a file that exists but cannot be
     read/parsed is never overwritten (ORDERS_JSON_UNREADABLE_GUARD).
  3. MASS-SHRINK GUARD — a write dropping >= 2 stacks vs disk is refused
     (ORDERS_JSON_MASS_SHRINK_GUARD) unless allow_shrink (clear/remove).
  4. INTENT — an authorised clear-all stamps intent=remove_all and succeeds.
  5. MONOTONIC GEN — every accepted write bumps the on-disk generation.
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


def _cfg(tmp: Path) -> dict:
    # Absolute path → _resolve_mm_orders_config_path returns it verbatim.
    return {"mm_desk": {"mm_orders_config_path": str(tmp / "orders.json")}}


def _stack(sid: str, ax: str = "EURUSD-PERP") -> dict:
    return {"stack_id": sid, "id": sid, "ax_symbol": ax, "width_bps": 4, "order_size": 1000}


def _seed(cfg: dict, n: int, gen: int) -> dict:
    """Write an initial orders.json with n stacks at a given generation."""
    doc = core._empty_mm_orders_config()
    doc["stacks"] = [_stack(f"s{i:02d}") for i in range(n)]
    doc["gen"] = gen
    p = core._resolve_mm_orders_config_path(cfg)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(doc), encoding="utf-8")
    return doc


def _ondisk(cfg: dict) -> dict:
    return json.loads(core._resolve_mm_orders_config_path(cfg).read_text(encoding="utf-8"))


# 1+5. Clean read-modify-write: an add on top of the current generation succeeds
# and bumps the generation, and the sibling stacks are preserved.
def test_clean_cas_increments_generation() -> None:
    with tempfile.TemporaryDirectory() as d:
        cfg = _cfg(Path(d))
        _seed(cfg, n=12, gen=7)
        doc = core.load_mm_orders_config(cfg)          # loads gen=7, 12 stacks
        check(doc["gen"] == 7, "load carries on-disk generation (7)")
        check(len(doc["stacks"]) == 12, "load carries all 12 stacks")
        doc["stacks"].append(_stack("s12"))            # add a 13th
        ok, err = core.save_mm_orders_config(cfg, doc)
        check(ok, f"clean save on current generation accepted (err={err!r})")
        disk = _ondisk(cfg)
        check(disk["gen"] == 8, "accepted write bumps generation 7 -> 8")
        check(len(disk["stacks"]) == 13, "accepted write keeps all 13 stacks")


# 2. Generation CAS: a foreign writer advanced the file between load and save.
# The stale doc must be REFUSED and the good on-disk file left intact.
def test_stale_generation_refused() -> None:
    with tempfile.TemporaryDirectory() as d:
        cfg = _cfg(Path(d))
        _seed(cfg, n=12, gen=30)
        doc = core.load_mm_orders_config(cfg)          # loads gen=30, 12 stacks
        # Simulate a second GUI/instance writing gen=31 with the full 12 in between.
        _seed(cfg, n=12, gen=31)
        # This desk now tries to save its stale view (only 1 stack in memory).
        doc["stacks"] = [_stack("only_one")]
        ok, err = core.save_mm_orders_config(cfg, doc)
        check(not ok, "stale-generation save is refused")
        check("generation" in (err or "").lower(), f"refusal cites generation (err={err!r})")
        disk = _ondisk(cfg)
        check(len(disk["stacks"]) == 12, "on-disk file still holds 12 stacks (not clobbered)")
        check(disk["gen"] == 31, "on-disk generation untouched (31)")


# 3. Non-clobber on unreadable disk: a corrupt file must never be overwritten by
# a doc built on a silently-degraded (empty) load.
def test_unreadable_disk_refused() -> None:
    with tempfile.TemporaryDirectory() as d:
        cfg = _cfg(Path(d))
        p = core._resolve_mm_orders_config_path(cfg)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text("{ this is not valid json ", encoding="utf-8")  # corrupt but present
        doc = core._empty_mm_orders_config()
        doc["stacks"] = [_stack("s0")]
        ok, err = core.save_mm_orders_config(cfg, doc)
        check(not ok, "save over an unreadable/corrupt file is refused")
        check("unreadable" in (err or "").lower() or "corrupt" in (err or "").lower(),
              f"refusal cites unreadable/corrupt (err={err!r})")
        check(p.read_text(encoding="utf-8").startswith("{ this is not valid"),
              "corrupt file left byte-for-byte intact")


# 4a. Mass-shrink guard: dropping 12 -> 1 without authorisation is refused, even
# though the generation matches (belt-and-suspenders behind the CAS).
def test_mass_shrink_refused_without_intent() -> None:
    with tempfile.TemporaryDirectory() as d:
        cfg = _cfg(Path(d))
        _seed(cfg, n=12, gen=3)
        doc = core.load_mm_orders_config(cfg)          # gen matches → CAS ok
        doc["stacks"] = [_stack("survivor")]           # 12 -> 1
        ok, err = core.save_mm_orders_config(cfg, doc)
        check(not ok, "12->1 save refused without allow_shrink")
        check("shrink" in (err or "").lower(), f"refusal cites mass-shrink (err={err!r})")
        check(len(_ondisk(cfg)["stacks"]) == 12, "on-disk file still holds 12 stacks")


# 4b. Authorised clear-all: allow_shrink + intent=remove_all writes the empty file.
def test_remove_all_intent_succeeds() -> None:
    with tempfile.TemporaryDirectory() as d:
        cfg = _cfg(Path(d))
        _seed(cfg, n=12, gen=9)
        empty = core._empty_mm_orders_config()
        empty["intent"] = "remove_all"
        ok, err = core.save_mm_orders_config(cfg, empty, allow_shrink=True)
        check(ok, f"authorised clear-all accepted (err={err!r})")
        disk = _ondisk(cfg)
        check(len(disk["stacks"]) == 0, "clear-all wrote 0 stacks")
        check(disk.get("intent") == "remove_all", "clear-all stamped intent=remove_all")
        check(disk["gen"] == 10, "clear-all bumps generation 9 -> 10 (monotonic)")


# 4c. A single-stack remove (12 -> 11) is a normal edit and must NOT be gated.
def test_single_remove_allowed() -> None:
    with tempfile.TemporaryDirectory() as d:
        cfg = _cfg(Path(d))
        _seed(cfg, n=12, gen=2)
        doc = core.load_mm_orders_config(cfg)
        doc["stacks"] = doc["stacks"][:-1]             # 12 -> 11
        ok, err = core.save_mm_orders_config(cfg, doc)
        check(ok, f"single-stack remove accepted (err={err!r})")
        check(len(_ondisk(cfg)["stacks"]) == 11, "single remove landed (11 stacks)")


def main() -> int:
    print("=== orders.json write-integrity tests (2026-07-15) ===")
    for fn in (
        test_clean_cas_increments_generation,
        test_stale_generation_refused,
        test_unreadable_disk_refused,
        test_mass_shrink_refused_without_intent,
        test_remove_all_intent_succeeds,
        test_single_remove_allowed,
    ):
        print(f"[RUN ] {fn.__name__}")
        fn()
    if _failures:
        print(f"[SUITE FAIL] orders_json_integrity — {_failures} failure(s)")
        return 1
    print("[SUITE OK] orders_json_integrity — all checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
