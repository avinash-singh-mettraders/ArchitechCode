"""Paired-order cancellation for the MM Live Desk (server/GUI layer only).

Orders are placed in bid+ask pairs for the same instrument. When one leg is
rejected for a margin-related reason there is no reason to keep showing the
other (one-sided) leg, so it must be cancelled immediately.

This module is intentionally self-contained and dependency-free (stdlib only):

  * It does NOT touch the C++ order-submission path.
  * It does NOT change any existing reject handler signature or cancel endpoint.
  * It only maintains a lightweight, bounded ``order_id -> paired_order_id`` map
    and decides, on a reject, whether the paired leg should be cancelled.

Extensibility (Tim's note): the reject-reason -> paired-cancel mapping is a plain
config list, :data:`PAIRED_CANCEL_REJECT_REASONS`. To make a new reject reason
trigger the same paired-cancel behaviour, add a keyword to that list — no logic
change required. Matching is case-insensitive substring matching.
"""

from __future__ import annotations

import threading
from collections import OrderedDict
from typing import Callable, Optional


# ---------------------------------------------------------------------------
# Config — reject reasons that trigger a paired cancellation.
# Add keywords here to extend the behaviour to new reject reasons. Matching is
# case-insensitive and substring-based, so "margin" also matches
# "initial margin required for order ... greater than available initial margin".
# ---------------------------------------------------------------------------
PAIRED_CANCEL_REJECT_REASONS: list[str] = [
    "margin",
    "insufficient funds",
    "margin breach",
    "margin call",
    "account balance",
]


def _norm_oid(oid: object) -> str:
    if oid is None:
        return ""
    return str(oid).strip()


def reason_triggers_paired_cancel(
    reason: object, *, triggers: Optional[list[str]] = None
) -> bool:
    """Return True iff *reason* matches any configured paired-cancel keyword.

    Case-insensitive substring match against :data:`PAIRED_CANCEL_REJECT_REASONS`
    (or *triggers* when supplied, used by tests).
    """
    if reason is None:
        return False
    text = str(reason).strip().lower()
    if not text:
        return False
    keywords = triggers if triggers is not None else PAIRED_CANCEL_REJECT_REASONS
    for kw in keywords:
        k = str(kw).strip().lower()
        if k and k in text:
            return True
    return False


# Type alias for readability.
LogFn = Callable[[str], None]


def _noop_log(_msg: str) -> None:
    return None


class PairRegistry:
    """Thread-safe, bounded ``order_id -> paired_order_id`` map.

    Both directions of a pair are stored so a reject on *either* leg can find
    its partner. The map is capped at ``max_pairs`` pairs and evicts the oldest
    entries first, so it can never grow unbounded even if some resolution event
    is missed.
    """

    def __init__(self, *, max_pairs: int = 5000) -> None:
        self._lock = threading.RLock()
        # oid -> partner oid (one entry per side, so a pair occupies two keys).
        self._partner: "OrderedDict[str, str]" = OrderedDict()
        self._max_pairs = int(max_pairs) if max_pairs and max_pairs > 0 else 5000

    # -- registration -------------------------------------------------------
    def register_pair(
        self, oid_a: object, oid_b: object, *, log_fn: LogFn = _noop_log
    ) -> bool:
        """Register a bid/ask pair by their exchange order ids.

        Called at placement time when both sides of a pair are submitted
        together. Returns True when a pair was stored.
        """
        a = _norm_oid(oid_a)
        b = _norm_oid(oid_b)
        if not a or not b or a == b:
            return False
        with self._lock:
            # Drop any stale links for these ids before re-linking them.
            self._forget_locked(a)
            self._forget_locked(b)
            self._partner[a] = b
            self._partner[b] = a
            self._evict_if_needed_locked()
        log_fn(f"Paired-cancel: registered pair order_ids {a} <-> {b}")
        return True

    # -- lookup -------------------------------------------------------------
    def paired_oid(self, oid: object) -> Optional[str]:
        o = _norm_oid(oid)
        if not o:
            return None
        with self._lock:
            return self._partner.get(o)

    def pair_count(self) -> int:
        with self._lock:
            return len(self._partner) // 2

    def __len__(self) -> int:  # number of live pairs
        return self.pair_count()

    # -- cleanup ------------------------------------------------------------
    def forget(self, oid: object) -> bool:
        """Remove *oid* and its partner from the map. Returns True if removed."""
        o = _norm_oid(oid)
        if not o:
            return False
        with self._lock:
            return self._forget_locked(o)

    def note_resolution(self, oid: object, *, log_fn: LogFn = _noop_log) -> bool:
        """Step 4: a leg was filled/cancelled/rejected (any reason) — drop the pair."""
        o = _norm_oid(oid)
        if not o:
            return False
        with self._lock:
            removed = self._forget_locked(o)
        if removed:
            log_fn(f"Paired-cancel: order {o} resolved; removed pair entry")
        return removed

    def _forget_locked(self, oid: str) -> bool:
        if oid not in self._partner:
            return False
        partner = self._partner.pop(oid, None)
        if partner is not None and self._partner.get(partner) == oid:
            self._partner.pop(partner, None)
        return True

    def _evict_if_needed_locked(self) -> None:
        cap = 2 * self._max_pairs
        while len(self._partner) > cap:
            old_oid, partner = self._partner.popitem(last=False)
            if partner is not None and self._partner.get(partner) == old_oid:
                self._partner.pop(partner, None)

    # -- core decision ------------------------------------------------------
    def handle_reject(
        self,
        order_id: object,
        reason: object,
        *,
        cancel_fn: Callable[[str], object],
        log_fn: LogFn = _noop_log,
        is_live_fn: Optional[Callable[[str], bool]] = None,
        triggers: Optional[list[str]] = None,
    ) -> dict:
        """Handle one order reject.

        On a margin-related reject, look up the paired order and (if it is still
        live) cancel it via *cancel_fn*. The pair entry is always removed after a
        reject — step 4/5 of the spec — so the map never accumulates stale rows.

        Every decision point is logged via *log_fn*. *is_live_fn* (optional)
        returns whether a given order id is still live; when omitted the paired
        order is assumed live. The return dict is informational (used by tests).
        """
        oid = _norm_oid(order_id)
        result: dict = {
            "order_id": oid,
            "reason": "" if reason is None else str(reason),
            "matched": False,
            "paired_order_id": None,
            "cancel_sent": False,
            "skipped": None,
        }
        if not oid:
            result["skipped"] = "empty_order_id"
            return result

        log_fn(f"Paired-cancel: reject received for order {oid} reason={result['reason']!r}")

        matched = reason_triggers_paired_cancel(reason, triggers=triggers)
        result["matched"] = matched
        if not matched:
            log_fn(
                f"Paired-cancel: reject reason for order {oid} did not match any "
                f"paired-cancel trigger; leaving paired order untouched"
            )
            # A non-margin reject still resolves this order — clean up its pair entry.
            self.forget(oid)
            result["skipped"] = "reason_not_matched"
            return result

        paired = self.paired_oid(oid)
        result["paired_order_id"] = paired
        if not paired:
            log_fn(
                f"Paired-cancel: margin breach reject for order {oid}, but no paired "
                f"order tracked or already inactive, no action taken"
            )
            self.forget(oid)
            result["skipped"] = "no_pair_tracked"
            return result

        log_fn(
            f"Paired-cancel: margin breach reject received for order {oid}, "
            f"cancelling paired order {paired}"
        )

        live = True
        if is_live_fn is not None:
            try:
                live = bool(is_live_fn(paired))
            except Exception as exc:  # pragma: no cover - defensive
                log_fn(
                    f"Paired-cancel: is_live check for paired order {paired} raised "
                    f"{type(exc).__name__}: {exc}; assuming live"
                )
                live = True

        if not live:
            log_fn(
                f"Paired-cancel: paired order {paired} not found or already "
                f"inactive, no action taken"
            )
            self.forget(oid)
            result["skipped"] = "paired_inactive"
            return result

        try:
            resp = cancel_fn(paired)
            result["cancel_response"] = resp
        except Exception as exc:
            log_fn(
                f"Paired-cancel: cancel request for paired order {paired} raised "
                f"{type(exc).__name__}: {exc}"
            )
            self.forget(oid)
            result["skipped"] = "cancel_exception"
            return result

        log_fn(f"Paired-cancel: cancel sent for paired order {paired}")
        result["cancel_sent"] = True
        # Step 5: remove both orders from the pair map.
        self.forget(oid)
        return result
