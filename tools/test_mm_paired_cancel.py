#!/usr/bin/env python3
"""Tests for paired-order cancellation (tools/mm_paired_cancel.py).

Dependency-free: run directly with ``python3 tools/test_mm_paired_cancel.py``.
Exits non-zero on first failure.

Covers the required scenarios:
  1. Reject with reason "margin breach" -> paired cancel IS triggered.
  2. Reject with a non-margin reason     -> paired cancel is NOT triggered.
  3. Margin reject where the paired order is already cancelled -> graceful,
     no cancel sent, correct log output.
  4. Pair map is empty after both orders are resolved.
Plus: log content assertions, extensibility of the trigger list, and the
bounded-map / cleanup guarantees.
"""

from __future__ import annotations

import sys
from pathlib import Path

_TOOLS_DIR = str(Path(__file__).resolve().parent)
if _TOOLS_DIR not in sys.path:
    sys.path.insert(0, _TOOLS_DIR)

import mm_paired_cancel  # noqa: E402
from mm_paired_cancel import (  # noqa: E402
    PairRegistry,
    PAIRED_CANCEL_REJECT_REASONS,
    reason_triggers_paired_cancel,
)


class FakeCancelEndpoint:
    """Stand-in for desk_cancel_one_order: records every oid it was asked to cancel."""

    def __init__(self):
        self.calls: list[str] = []

    def __call__(self, oid: str):
        self.calls.append(oid)
        return {"ok": True, "http": 200, "oid": oid}


class LogCapture:
    def __init__(self):
        self.lines: list[str] = []

    def __call__(self, msg: str):
        self.lines.append(str(msg))

    def contains(self, needle: str) -> bool:
        return any(needle in ln for ln in self.lines)

    def dump(self) -> str:
        return "\n    ".join(self.lines)


def _live_set(*oids: str):
    live = set(oids)
    return lambda oid: oid in live


_passed = 0


def check(cond: bool, label: str, log: "LogCapture | None" = None):
    global _passed
    if not cond:
        print(f"FAIL: {label}")
        if log is not None:
            print("  log was:\n    " + log.dump())
        raise SystemExit(1)
    _passed += 1
    print(f"ok: {label}")


# ---------------------------------------------------------------------------
# Reason matcher
# ---------------------------------------------------------------------------
def test_reason_matcher():
    check(reason_triggers_paired_cancel("margin breach"), "matcher: 'margin breach'")
    check(reason_triggers_paired_cancel("MARGIN BREACH"), "matcher: case-insensitive")
    check(
        reason_triggers_paired_cancel(
            "initial margin required for order (15069.43) is greater than "
            "available initial margin (5260.72)"
        ),
        "matcher: real gateway margin string (substring)",
    )
    check(reason_triggers_paired_cancel("insufficient funds"), "matcher: 'insufficient funds'")
    check(reason_triggers_paired_cancel("account balance too low"), "matcher: 'account balance'")
    check(not reason_triggers_paired_cancel("post only would cross"), "matcher: non-margin -> False")
    check(not reason_triggers_paired_cancel(""), "matcher: empty -> False")
    check(not reason_triggers_paired_cancel(None), "matcher: None -> False")


# ---------------------------------------------------------------------------
# Scenario 1: margin-breach reject triggers paired cancel
# ---------------------------------------------------------------------------
def test_margin_reject_triggers_cancel():
    reg = PairRegistry()
    cancel = FakeCancelEndpoint()
    log = LogCapture()
    reg.register_pair("BID1", "ASK1", log_fn=log)
    check(len(reg) == 1, "scenario1: pair registered")

    res = reg.handle_reject(
        "BID1", "margin breach",
        cancel_fn=cancel, log_fn=log, is_live_fn=_live_set("ASK1"),
    )
    check(res["matched"] is True, "scenario1: reason matched", log)
    check(res["cancel_sent"] is True, "scenario1: cancel_sent flag", log)
    check(cancel.calls == ["ASK1"], "scenario1: paired ASK1 cancelled", log)
    check(
        log.contains("margin breach reject received for order BID1, cancelling paired order ASK1"),
        "scenario1: reject+intent log line",
        log,
    )
    check(log.contains("cancel sent for paired order ASK1"), "scenario1: cancel-sent log line", log)
    check(len(reg) == 0, "scenario1: pair removed after handling", log)


# ---------------------------------------------------------------------------
# Scenario 2: non-margin reject does NOT trigger paired cancel
# ---------------------------------------------------------------------------
def test_non_margin_reject_no_cancel():
    reg = PairRegistry()
    cancel = FakeCancelEndpoint()
    log = LogCapture()
    reg.register_pair("BID2", "ASK2", log_fn=log)

    res = reg.handle_reject(
        "BID2", "post only would cross the book",
        cancel_fn=cancel, log_fn=log, is_live_fn=_live_set("ASK2"),
    )
    check(res["matched"] is False, "scenario2: reason not matched", log)
    check(res["cancel_sent"] is False, "scenario2: no cancel_sent", log)
    check(cancel.calls == [], "scenario2: cancel endpoint NOT called", log)
    check(
        log.contains("did not match any paired-cancel trigger"),
        "scenario2: no-trigger log line",
        log,
    )
    # Step 4: a reject for any reason resolves the order -> pair entry cleaned up.
    check(len(reg) == 0, "scenario2: pair entry cleaned up after non-margin reject", log)


# ---------------------------------------------------------------------------
# Scenario 3: margin reject where paired order is already cancelled/inactive
# ---------------------------------------------------------------------------
def test_margin_reject_paired_already_dead():
    reg = PairRegistry()
    cancel = FakeCancelEndpoint()
    log = LogCapture()
    reg.register_pair("BID3", "ASK3", log_fn=log)

    # ASK3 is NOT in the live set -> already cancelled/filled/etc.
    res = reg.handle_reject(
        "BID3", "insufficient funds",
        cancel_fn=cancel, log_fn=log, is_live_fn=_live_set(),  # nothing live
    )
    check(res["matched"] is True, "scenario3: margin reason matched", log)
    check(res["cancel_sent"] is False, "scenario3: no cancel sent (paired dead)", log)
    check(cancel.calls == [], "scenario3: cancel endpoint NOT called", log)
    check(
        log.contains("paired order ASK3 not found or already inactive, no action taken"),
        "scenario3: graceful-skip log line",
        log,
    )
    check(len(reg) == 0, "scenario3: pair removed after graceful skip", log)


# ---------------------------------------------------------------------------
# Scenario 4: pair map empty after both orders resolved (cleanup / no leak)
# ---------------------------------------------------------------------------
def test_pair_map_empty_after_resolution():
    reg = PairRegistry()
    log = LogCapture()
    reg.register_pair("BID4", "ASK4", log_fn=log)
    check(len(reg) == 1, "scenario4: one pair tracked")

    # One leg fills, the other gets cancelled -> both resolutions clean the map.
    reg.note_resolution("BID4", log_fn=log)
    check(len(reg) == 0, "scenario4: map empty after first resolution removes the pair", log)
    # Resolving the partner again is a harmless no-op.
    reg.note_resolution("ASK4", log_fn=log)
    check(len(reg) == 0, "scenario4: still empty after second resolution", log)
    check(reg.paired_oid("BID4") is None, "scenario4: no stale BID4 link", log)
    check(reg.paired_oid("ASK4") is None, "scenario4: no stale ASK4 link", log)


# ---------------------------------------------------------------------------
# Extra: extensibility, idempotency, bounded growth
# ---------------------------------------------------------------------------
def test_extensibility_config_list():
    reg = PairRegistry()
    cancel = FakeCancelEndpoint()
    log = LogCapture()
    reg.register_pair("BIDx", "ASKx", log_fn=log)
    custom = list(PAIRED_CANCEL_REJECT_REASONS) + ["risk limit exceeded"]
    res = reg.handle_reject(
        "BIDx", "RISK LIMIT EXCEEDED on account",
        cancel_fn=cancel, log_fn=log, is_live_fn=_live_set("ASKx"), triggers=custom,
    )
    check(res["cancel_sent"] is True, "extensibility: new trigger keyword fires paired cancel", log)
    check(cancel.calls == ["ASKx"], "extensibility: paired leg cancelled via new keyword", log)


def test_reject_on_either_leg():
    # Reject can arrive on the ASK leg; the BID leg must be cancelled.
    reg = PairRegistry()
    cancel = FakeCancelEndpoint()
    log = LogCapture()
    reg.register_pair("BID5", "ASK5", log_fn=log)
    reg.handle_reject(
        "ASK5", "margin call", cancel_fn=cancel, log_fn=log, is_live_fn=_live_set("BID5")
    )
    check(cancel.calls == ["BID5"], "either-leg: ASK reject cancels BID", log)


def test_no_pair_tracked_is_graceful():
    reg = PairRegistry()
    cancel = FakeCancelEndpoint()
    log = LogCapture()
    res = reg.handle_reject(
        "UNKNOWN", "margin breach", cancel_fn=cancel, log_fn=log, is_live_fn=_live_set()
    )
    check(res["matched"] is True, "no-pair: reason matched", log)
    check(cancel.calls == [], "no-pair: nothing cancelled", log)
    check(res["skipped"] == "no_pair_tracked", "no-pair: skipped reason recorded", log)


def test_bounded_growth():
    reg = PairRegistry(max_pairs=10)
    for i in range(100):
        reg.register_pair(f"B{i}", f"A{i}")
    check(len(reg) == 10, f"bounded: capped at 10 pairs (got {len(reg)})")
    # Oldest evicted, newest retained.
    check(reg.paired_oid("B0") is None, "bounded: oldest pair evicted")
    check(reg.paired_oid("B99") == "A99", "bounded: newest pair retained")


def main():
    test_reason_matcher()
    test_margin_reject_triggers_cancel()
    test_non_margin_reject_no_cancel()
    test_margin_reject_paired_already_dead()
    test_pair_map_empty_after_resolution()
    test_extensibility_config_list()
    test_reject_on_either_leg()
    test_no_pair_tracked_is_graceful()
    test_bounded_growth()
    print(f"\nAll paired-cancel tests passed ({_passed} checks).")


if __name__ == "__main__":
    main()
