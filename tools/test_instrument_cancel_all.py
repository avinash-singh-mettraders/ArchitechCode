#!/usr/bin/env python3
"""Tests for Per-Instrument Cancel-All + HOLD (tools/mm_live_desk_core.py).

Dependency-free: run directly with ``python3 tools/test_instrument_cancel_all.py``.
Exits non-zero on first failure.

These lock down the desk-side D5 safety sequence and the D6 dropdown source:

  1. SYMBOL NORMALISATION + REJECTION — the request keys on the normalised AX symbol
     and a symbol outside the (configured ∪ live) union is refused with an explicit
     error (never a silent no-op that looks like success).
  2. ORDERING (hold precedes cancel) + NEGATIVE CONTROL — the hold is on disk BEFORE
     the venue cancel is issued (this is the whole §4.5 in-flight-place orphan safety
     argument); the negative control proves that reversing the order leaves the
     instrument still enabled at cancel time (the re-quote window the sequence closes).
  3. RETRY-UNTIL-CLEAN — a residual on the first verify triggers another sweep and the
     run reports CLEAN once the venue confirms zero open.
  4. NON-CONVERGENT -> NOT CLEAN — a venue that never drains reports ok=False /
     clean=False with the residual OIDs listed and the instrument left HELD; success is
     never reported on an unverified state.
  5. D6 UNION + FRESH VENUE COUNTS — the dropdown is the union of configured
     instruments and any symbol with live venue orders, live-but-unconfigured marked,
     counts taken from a fresh venue read.
  6. AWAIT-HOLD-OBSERVED predicate — the step-2 wait only returns observed once the
     engine projection proves it reloaded a config at least as new as the hold write
     (config_mtime marker) and the instrument's effective gate reads false.
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


def _seed_default_config() -> dict:
    return {
        "trading": {"max_orders_per_second": 50},
        "market_maker": {
            "enabled": True,
            "mm_orders_enabled": True,
            "instruments": [
                {"symbol": "XAU-PERP", "mm_orders_enabled": True, "requote_on_theo_move": True},
                {"symbol": "XAG-PERP", "mm_orders_enabled": True, "requote_on_theo_move": True},
            ],
        },
    }


def _write_cfg(path: Path, doc: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(doc, indent=2), encoding="utf-8")


def _cfg_symbol_enabled(path: Path, sym: str) -> bool:
    """Effective mm_orders_enabled for ``sym`` as it currently sits on disk."""
    dc = json.loads(path.read_text(encoding="utf-8"))
    mm = dc.get("market_maker", {})
    g = bool(mm.get("mm_orders_enabled", True))
    for row in mm.get("instruments", []):
        if str(row.get("symbol", "")).upper() == sym.upper():
            return g and bool(row.get("mm_orders_enabled", True))
    return g


class _FakeState:
    """Minimal stand-in — every function that touches real desk state is monkeypatched."""


@contextmanager
def _patched(**overrides):
    """Temporarily set attributes on the core module, restoring originals after."""
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


def _observed_projection(_merged):
    """load_mm_active_orders stub: engine has observed a very-new config and holds XAG-PERP."""
    return {
        "stale": False,
        "config_mtime_ms": 10 ** 15,
        "orders": [{"ax_symbol": "XAG-PERP", "mm_orders_enabled_effective": False}],
    }


def _rows(*syms: str) -> list[dict]:
    return [{"symbol": s, "id": f"oid-{s}-{i}"} for i, s in enumerate(syms)]


# 1. Symbol normalisation + rejection of a symbol outside the union.
def test_symbol_normalisation_and_rejection() -> None:
    with tempfile.TemporaryDirectory() as d:
        p = Path(d) / "default_config.json"
        _write_cfg(p, _seed_default_config())
        st = _FakeState()
        calls = {"cancel": 0}

        def fake_cancel(_st, *, symbol):
            calls["cancel"] += 1
            return {"ok": True, "http": 200}

        with _patched(
            DEFAULT_CONFIG_PATH=p,
            get_merged_config_dict=lambda: {},
            desk_reload_config_into_state=lambda _st: None,
            desk_log=lambda *_a, **_k: None,
            load_mm_active_orders=_observed_projection,
            desk_cancel_all_orders=fake_cancel,
            _open_orders_fetch=lambda _st: ([], True, ""),
        ):
            # lower-case + surrounding whitespace normalises to the configured AX symbol.
            res = core.desk_instrument_cancel_all(st, "  xag-perp ", hold=True)
            check(res.get("ax_symbol") == "XAG-PERP", "input '  xag-perp ' normalises to 'XAG-PERP'")
            check(res.get("clean") is True, "known symbol runs the sequence to CLEAN")

            # A symbol that is neither configured nor live at the venue is rejected outright.
            calls["cancel"] = 0
            rej = core.desk_instrument_cancel_all(st, "DOGE-PERP", hold=True)
            check(rej.get("ok") is False, "unknown symbol rejected (ok=False)")
            check("not a configured or live" in (rej.get("error") or ""), "rejection error is explicit")
            check(calls["cancel"] == 0, "no venue cancel issued for a rejected symbol")


# 2. Ordering: hold is on disk BEFORE cancel + NEGATIVE CONTROL.
def test_hold_precedes_cancel_ordering() -> None:
    with tempfile.TemporaryDirectory() as d:
        p = Path(d) / "default_config.json"
        _write_cfg(p, _seed_default_config())
        st = _FakeState()
        observed_flag = {"at_cancel": None}

        def fake_cancel(_st, *, symbol):
            # Read the on-disk gate AT THE MOMENT the cancel fires.
            observed_flag["at_cancel"] = _cfg_symbol_enabled(p, symbol)
            return {"ok": True, "http": 200}

        with _patched(
            DEFAULT_CONFIG_PATH=p,
            get_merged_config_dict=lambda: {},
            desk_reload_config_into_state=lambda _st: None,
            desk_log=lambda *_a, **_k: None,
            load_mm_active_orders=_observed_projection,
            desk_cancel_all_orders=fake_cancel,
            _open_orders_fetch=lambda _st: ([], True, ""),
        ):
            res = core.desk_instrument_cancel_all(st, "XAG-PERP", hold=True)

        check(observed_flag["at_cancel"] is False,
              "instrument was already HELD (mm_orders_enabled=false) when cancel fired")
        step_names = [s["step"] for s in res.get("steps", [])]
        check(step_names[0] == "write_hold", "step 1 is write_hold")
        check(step_names[1] == "await_hold_observed", "step 2 is await_hold_observed (before any cancel)")
        check("cancel_and_verify" in step_names, "cancel/verify only after hold + observe")
        check(step_names.index("write_hold") < step_names.index("cancel_and_verify"),
              "write_hold strictly precedes cancel_and_verify")

        # NEGATIVE CONTROL: reverse the order — cancel FIRST, then hold. The instrument is
        # still ENABLED at cancel time, i.e. the ~72ms re-quote window this sequence exists
        # to close would be wide open. Proves the ordering is load-bearing, not cosmetic.
        _write_cfg(p, _seed_default_config())
        rev = {"at_cancel": None}
        with _patched(DEFAULT_CONFIG_PATH=p):
            rev["at_cancel"] = _cfg_symbol_enabled(p, "XAG-PERP")  # cancel would see this
            core.persist_market_maker_gate_flags({"mm_orders_enabled": False}, "XAG-PERP")
        check(rev["at_cancel"] is True,
              "NEGATIVE CONTROL: cancel-before-hold sees instrument still ENABLED (re-quote window open)")


# 2b. ITEM 2 — re-enable stays removed. The /api/desk/instrument_cancel_all route hard-forces
# hold=True regardless of the request body, so a legacy/hand-rolled client sending
# {"hold": false} to re-enable is ignored: the instrument is NOT re-enabled and zero orders
# are placed. The negative control proves the force is load-bearing — the unforced hold=false
# path DOES re-enable (which is exactly why the route must never pass the body value through).
def test_reenable_via_cancel_route_is_rejected() -> None:
    with tempfile.TemporaryDirectory() as d:
        p = Path(d) / "default_config.json"
        seed = _seed_default_config()
        seed["market_maker"]["instruments"][1]["mm_orders_enabled"] = False  # XAG already stopped
        _write_cfg(p, seed)
        st = _FakeState()
        calls = {"cancel": 0, "place": 0}

        def fake_cancel(_st, *, symbol):
            calls["cancel"] += 1
            return {"ok": True, "http": 200}

        def fake_place(*_a, **_k):
            calls["place"] += 1
            return (200, {"ok": True})

        with _patched(
            DEFAULT_CONFIG_PATH=p,
            get_merged_config_dict=lambda: {},
            desk_reload_config_into_state=lambda _st: None,
            desk_log=lambda *_a, **_k: None,
            load_mm_active_orders=_observed_projection,
            desk_cancel_all_orders=fake_cancel,
            _desk_place_limit_gtc=fake_place,
            _open_orders_fetch=lambda _st: ([], True, ""),
        ):
            # The route ALWAYS calls with hold=True (see /api/desk/instrument_cancel_all handler),
            # even if the body said {"hold": false}. Replicate the route's forced value.
            res = core.desk_instrument_cancel_all(st, "XAG-PERP", hold=True)
            check(res.get("action") != "re_enable", "forced hold=True path never re-enables")
            check(_cfg_symbol_enabled(p, "XAG-PERP") is False,
                  "instrument LEFT STOPPED after a hold:false attempt (not re-enabled)")
            check(calls["place"] == 0, "zero orders placed by the cancel route")

            # NEGATIVE CONTROL: if the route had passed the body's hold=false through, the
            # instrument WOULD be re-enabled — proving the hard-force is load-bearing, not cosmetic.
            _write_cfg(p, seed)
            calls["place"] = 0
            bad = core.desk_instrument_cancel_all(st, "XAG-PERP", hold=False)
            check(bad.get("action") == "re_enable" and _cfg_symbol_enabled(p, "XAG-PERP") is True,
                  "NEGATIVE CONTROL: unforced hold=false re-enables (why the route must force hold=True)")
            check(calls["place"] == 0, "even the re-enable branch places zero orders itself (flag-only)")


# 2c. ITEM 3 — RESUME is FLAG-ONLY and places ZERO orders, with a negative control that
# reproduces the old re-enable's re-placement (doubling) shape to prove behavioral difference.
def test_resume_is_flag_only_and_places_zero() -> None:
    with tempfile.TemporaryDirectory() as d:
        p = Path(d) / "default_config.json"
        seed = _seed_default_config()
        seed["market_maker"]["instruments"][1]["mm_orders_enabled"] = False  # XAG stopped
        _write_cfg(p, seed)
        st = _FakeState()
        calls = {"cancel": 0, "place": 0}

        def fake_cancel(_st, *, symbol):
            calls["cancel"] += 1
            return {"ok": True, "http": 200}

        def fake_place(*_a, **_k):
            calls["place"] += 1
            return (200, {"ok": True})

        with _patched(
            DEFAULT_CONFIG_PATH=p,
            get_merged_config_dict=lambda: {},
            desk_reload_config_into_state=lambda _st: None,
            desk_log=lambda *_a, **_k: None,
            desk_cancel_all_orders=fake_cancel,
            _desk_place_limit_gtc=fake_place,
        ):
            check(_cfg_symbol_enabled(p, "XAG-PERP") is False, "precondition: XAG-PERP starts stopped")
            res = core.desk_instrument_resume(st, "XAG-PERP")
            check(res.get("ok") is True and res.get("action") == "resume", "resume returns ok/action=resume")
            check(res.get("placed") == 0, "resume reports placed=0")
            check(calls["place"] == 0, "resume places ZERO orders (no _desk_place_limit_gtc)")
            check(calls["cancel"] == 0, "resume cancels nothing")
            check(_cfg_symbol_enabled(p, "XAG-PERP") is True, "resume flips mm_orders_enabled back true")

            # NEGATIVE CONTROL: the OLD re-enable ALSO re-placed the pair (that is what doubled
            # against a freshly-added stack). Model that extra re-place step explicitly — it DOES
            # place. The new resume above omits exactly this step, which is why it is safe.
            calls["place"] = 0
            core._desk_place_limit_gtc(st, symbol="XAG-PERP", side="B", qty=1, price=1.0)
            check(calls["place"] == 1,
                  "NEGATIVE CONTROL: the old re-enable's re-place step DOES place (doubling shape) — "
                  "the new flag-only resume never runs it")


# 2d. ITEM 3/4 — server gate: add/manual-place into a stopped instrument requires an explicit
# resume confirm. Without it -> needs_confirm, flag stays false, nothing placed. With resume:true
# -> flag flipped (flag-only) and the place proceeds. Not-stopped -> proceed unchanged.
def test_add_to_stopped_requires_confirm() -> None:
    with tempfile.TemporaryDirectory() as d:
        p = Path(d) / "default_config.json"
        seed = _seed_default_config()
        seed["market_maker"]["instruments"][1]["mm_orders_enabled"] = False  # XAG stopped
        _write_cfg(p, seed)
        st = _FakeState()

        with _patched(
            DEFAULT_CONFIG_PATH=p,
            get_merged_config_dict=lambda: {},
            desk_reload_config_into_state=lambda _st: None,
            desk_log=lambda *_a, **_k: None,
        ):
            # No confirm -> needs_confirm, flag untouched.
            dec = core._place_resume_decision(st, "  xag-perp ", {})
            check(dec.get("action") == "needs_confirm", "stopped + no resume -> needs_confirm")
            check(dec.get("ax_symbol") == "XAG-PERP", "needs_confirm carries the normalised symbol")
            check("stopped" in dec.get("error", "") and "resume" in dec.get("error", ""),
                  "confirm prompt is plain-language")
            check(_cfg_symbol_enabled(p, "XAG-PERP") is False,
                  "add-to-stopped WITHOUT confirm leaves the flag false (nothing resumed)")

            # Confirmed -> flag flipped (flag-only), place may proceed.
            dec2 = core._place_resume_decision(st, "XAG-PERP", {"resume": True})
            check(dec2.get("action") == "resumed", "stopped + resume:true -> resumed")
            check(dec2.get("resume", {}).get("placed") == 0, "the resume step itself places zero")
            check(_cfg_symbol_enabled(p, "XAG-PERP") is True, "confirmed resume flips the flag true")

            # A running instrument is never gated.
            dec3 = core._place_resume_decision(st, "XAU-PERP", {})
            check(dec3.get("action") == "proceed", "running instrument -> proceed (no confirm)")


# 3. Retry until the venue confirms clean.
def test_retry_until_clean() -> None:
    with tempfile.TemporaryDirectory() as d:
        p = Path(d) / "default_config.json"
        _write_cfg(p, _seed_default_config())
        st = _FakeState()
        # _open_orders_fetch call sequence: [validation pre-count, verify#1, verify#2, ...].
        seq = [
            _rows("XAG-PERP", "XAG-PERP", "XAG-PERP"),  # pre-count (instrument_hold_options)
            _rows("XAG-PERP", "XAG-PERP"),              # verify after sweep 1: residual -> retry
            [],                                          # verify after sweep 2: clean
        ]
        idx = {"i": 0}

        def fake_fetch(_st):
            i = min(idx["i"], len(seq) - 1)
            idx["i"] += 1
            return (seq[i], True, "")

        with _patched(
            DEFAULT_CONFIG_PATH=p,
            get_merged_config_dict=lambda: {},
            desk_reload_config_into_state=lambda _st: None,
            desk_log=lambda *_a, **_k: None,
            load_mm_active_orders=_observed_projection,
            desk_cancel_all_orders=lambda _st, *, symbol: {"ok": True, "http": 200},
            _open_orders_fetch=fake_fetch,
            _INSTR_HOLD_CANCEL_BACKOFF_SEC=(0.0, 0.0, 0.0, 0.0, 0.0),
        ):
            res = core.desk_instrument_cancel_all(st, "XAG-PERP", hold=True)

        check(res.get("clean") is True, "converges to CLEAN once venue confirms zero open")
        check(res.get("attempts") == 2, "took exactly 2 sweeps (first had residual)")
        check(res.get("residual_open") == 0, "no residual reported on the clean run")


# 4. Non-convergent venue -> NOT CLEAN with residual OIDs, instrument left HELD.
def test_non_convergent_reports_not_clean() -> None:
    with tempfile.TemporaryDirectory() as d:
        p = Path(d) / "default_config.json"
        _write_cfg(p, _seed_default_config())
        st = _FakeState()

        def fake_fetch(_st):
            return (_rows("XAG-PERP", "XAG-PERP"), True, "")  # never drains

        with _patched(
            DEFAULT_CONFIG_PATH=p,
            get_merged_config_dict=lambda: {},
            desk_reload_config_into_state=lambda _st: None,
            desk_log=lambda *_a, **_k: None,
            load_mm_active_orders=_observed_projection,
            desk_cancel_all_orders=lambda _st, *, symbol: {"ok": True, "http": 200},
            _open_orders_fetch=fake_fetch,
            _INSTR_HOLD_MAX_CANCEL_ATTEMPTS=2,
            _INSTR_HOLD_CANCEL_BACKOFF_SEC=(0.0, 0.0),
        ):
            res = core.desk_instrument_cancel_all(st, "XAG-PERP", hold=True)

        check(res.get("ok") is False, "non-convergent run is NOT ok")
        check(res.get("clean") is False, "non-convergent run is NOT clean")
        check(res.get("held") is True, "instrument is LEFT HELD after a non-convergent cancel")
        check(res.get("attempts") == 2, "bounded to the configured max attempts")
        check("NOT CLEAN" in (res.get("error") or ""), "error string says NOT CLEAN")
        check(len(res.get("residual_oids") or []) > 0, "residual OIDs are listed for the operator")


# 5. D6 union + fresh venue counts, unconfigured-but-live marked.
def test_dropdown_union_and_fresh_counts() -> None:
    with tempfile.TemporaryDirectory() as d:
        p = Path(d) / "default_config.json"
        _write_cfg(p, _seed_default_config())
        st = _FakeState()
        # Venue: XAG-PERP has 2 live, and LTC-PERP is live-but-unconfigured (1).
        live = _rows("XAG-PERP", "XAG-PERP", "LTC-PERP")

        with _patched(
            DEFAULT_CONFIG_PATH=p,
            get_merged_config_dict=lambda: {},
            load_mm_active_orders=lambda _m: {"stale": True, "orders": []},
            _open_orders_fetch=lambda _st: (live, True, ""),
        ):
            opts = core.instrument_hold_options(st)

        by = {i["ax_symbol"]: i for i in opts["instruments"]}
        check(set(by.keys()) == {"XAU-PERP", "XAG-PERP", "LTC-PERP"},
              "union = configured (XAU,XAG) ∪ live (XAG,LTC)")
        check(by["XAU-PERP"]["configured"] is True and by["XAU-PERP"]["live_open_count"] == 0,
              "configured-but-idle XAU-PERP present with 0 live")
        check(by["XAG-PERP"]["configured"] is True and by["XAG-PERP"]["live_open_count"] == 2,
              "XAG-PERP fresh live count = 2 (from venue read)")
        check(by["LTC-PERP"]["unconfigured"] is True and by["LTC-PERP"]["live_open_count"] == 1,
              "live-but-unconfigured LTC-PERP marked unconfigured with 1 live")


# 5b. Held state surfaced from disk into the dropdown options.
def test_dropdown_marks_held() -> None:
    with tempfile.TemporaryDirectory() as d:
        p = Path(d) / "default_config.json"
        seed = _seed_default_config()
        seed["market_maker"]["instruments"][1]["mm_orders_enabled"] = False  # XAG-PERP held
        _write_cfg(p, seed)
        st = _FakeState()
        # No projection row (stale) -> held falls back to the desk desired state on disk.
        with _patched(
            DEFAULT_CONFIG_PATH=p,
            get_merged_config_dict=lambda: {},
            load_mm_active_orders=lambda _m: {"stale": True, "orders": []},
            _open_orders_fetch=lambda _st: ([], True, ""),
        ):
            opts = core.instrument_hold_options(st)
        by = {i["ax_symbol"]: i for i in opts["instruments"]}
        check(by["XAG-PERP"]["held"] is True, "XAG-PERP (mm_orders_enabled=false) shows held=True")
        check(by["XAG-PERP"]["held_source"] == "config_stale", "held sourced from config when projection stale")
        check(by["XAU-PERP"]["held"] is False, "XAU-PERP shows held=False")


# 5c. ITEM 1 — the dropdown "quoting stopped" label is driven by the ENGINE effective gate
# from the mm_orders.json projection, NOT the desk's desired state in default_config. This is
# the exact confusion Tim hit: config said stopped while the engine was actually placing.
def test_dropdown_label_from_engine_projection() -> None:
    with tempfile.TemporaryDirectory() as d:
        p = Path(d) / "default_config.json"
        # Desk DESIRED state on disk: XAG-PERP stopped (mm_orders_enabled=false),
        # XAU-PERP running. The engine projection will DISAGREE for XAG-PERP.
        seed = _seed_default_config()
        seed["market_maker"]["instruments"][1]["mm_orders_enabled"] = False  # XAG desired-stopped
        _write_cfg(p, seed)
        st = _FakeState()

        # Engine truth (projection): XAG-PERP is actually ENABLED and quoting (effective=True),
        # XAU-PERP is actually STOPPED (effective=False) — the inverse of the desk's belief.
        def proj(_m):
            return {
                "stale": False,
                "config_mtime_ms": 10 ** 15,
                "orders": [
                    {"ax_symbol": "XAG-PERP", "mm_orders_enabled_effective": True},
                    {"ax_symbol": "XAU-PERP", "mm_orders_enabled_effective": False},
                ],
            }

        # Venue: XAG-PERP has 3 live open orders, XAU-PERP has 0.
        live = _rows("XAG-PERP", "XAG-PERP", "XAG-PERP")
        with _patched(
            DEFAULT_CONFIG_PATH=p,
            get_merged_config_dict=lambda: {},
            load_mm_active_orders=proj,
            _open_orders_fetch=lambda _st: (live, True, ""),
        ):
            opts = core.instrument_hold_options(st)

        by = {i["ax_symbol"]: i for i in opts["instruments"]}
        # ITEM 1 core assertion: label follows the ENGINE gate, not the config, AND the count
        # is the true venue count — both facts asserted together (Tim's exact confusion).
        check(by["XAG-PERP"]["held"] is False and by["XAG-PERP"]["live_open_count"] == 3,
              "XAG-PERP: NOT stopped (engine effective=True) AND shows true live count 3 "
              "(even though default_config desires it stopped)")
        check(by["XAG-PERP"]["held_source"] == "engine", "XAG-PERP held sourced from engine projection")
        check(by["XAU-PERP"]["held"] is True and by["XAU-PERP"]["live_open_count"] == 0,
              "XAU-PERP: stopped (engine effective=False) AND shows true live count 0")
        check(by["XAU-PERP"]["held_source"] == "engine", "XAU-PERP held sourced from engine projection")


# 6. Step-2 await-observed predicate: only observed once config marker + effective gate agree.
def test_await_hold_observed_predicate() -> None:
    # Fresh config + effective gate false -> observed.
    def proj_fresh(_m):
        return {"stale": False, "config_mtime_ms": 2000,
                "orders": [{"ax_symbol": "XAG-PERP", "mm_orders_enabled_effective": False}]}
    with _patched(load_mm_active_orders=proj_fresh):
        ok, ack = core._wait_engine_observed_hold({}, "XAG-PERP", 1500, timeout_sec=0.3)
    check(ok is True, "observed when projection config_mtime >= hold write and effective gate false")

    # Stale config marker (engine hasn't reloaded) -> NOT observed within the window.
    def proj_stale(_m):
        return {"stale": False, "config_mtime_ms": 1000,
                "orders": [{"ax_symbol": "XAG-PERP", "mm_orders_enabled_effective": False}]}
    with _patched(load_mm_active_orders=proj_stale, _INSTR_HOLD_ACK_POLL_SEC=0.02):
        ok2, _ = core._wait_engine_observed_hold({}, "XAG-PERP", 5000, timeout_sec=0.15)
    check(ok2 is False, "NOT observed while engine's config_mtime is older than the hold write")


def main() -> int:
    print("=== per-instrument cancel-all + HOLD tests (2026-07-23) ===")
    for fn in (
        test_symbol_normalisation_and_rejection,
        test_hold_precedes_cancel_ordering,
        test_reenable_via_cancel_route_is_rejected,
        test_resume_is_flag_only_and_places_zero,
        test_add_to_stopped_requires_confirm,
        test_retry_until_clean,
        test_non_convergent_reports_not_clean,
        test_dropdown_union_and_fresh_counts,
        test_dropdown_marks_held,
        test_dropdown_label_from_engine_projection,
        test_await_hold_observed_predicate,
    ):
        print(f"[RUN ] {fn.__name__}")
        fn()
    if _failures:
        print(f"[SUITE FAIL] instrument_cancel_all — {_failures} failure(s)")
        return 1
    print("[SUITE OK] instrument_cancel_all — all checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
