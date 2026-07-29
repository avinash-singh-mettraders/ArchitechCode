// =============================================================================
// REGRESSION TEST — product placement lease quiet-gate bypass (2026-07)
// =============================================================================
//
// Bug: under per-instrument sharded ENFORCE + place_before_cancel_ack
// (fire-and-track), a mover job cancels its own legs (state ->
// CANCEL_SENT_UNCONFIRMED) and then tries to acquire the per-AX product
// placement lease to place the replacement pair WITHOUT waiting for the cancel
// acks. The lease's "product quiet" requirement counted those still-in-flight
// CANCEL_SENT_UNCONFIRMED legs as activity, so the product was never quiet and
// the immediate place was denied → slow DESK_RECOVERY fallback.
//
// Fix: when the caller proves strict single-thread serialization (ENFORCE +
// sharding resolved ON + running on the AX's own mover worker) the quiet gate
// is bypassed. off/shadow, single-global-mover, and non-mover callers keep the
// legacy quiet||degraded gate byte-for-byte.
//
// This exercises the exact production predicates used by
// MakeMarketStrategy::mmAcquireProductPlacementLease /
// mmTryAtomicClaimProductPlacementLeaseLocked (see MmProductLeasePolicy.h).

#include "test_helpers.h"

#include "strategy/MmProductLeasePolicy.h"

#include <string>
#include <vector>

namespace {

using architect::strategy::mmProductLeaseClaimGranted;
using architect::strategy::mmSerializedSingleThreadLeaseBypassEligible;

// Minimal model of the per-AX product "quiet" computation. Mirrors the
// production predicate: quiet == (pending_accepts == 0) && no in-flight
// placement activity, where CANCEL_SENT_UNCONFIRMED counts as activity (it is
// treated via mmLegStateIsCancelInFlight()).
enum class LegState { IDLE, PLACE_PENDING, CANCEL_PENDING, CANCEL_SENT_UNCONFIRMED, FILLED };

bool legIsCancelInFlight(LegState s) {
    return s == LegState::CANCEL_PENDING || s == LegState::CANCEL_SENT_UNCONFIRMED;
}

struct ProductBook {
    int pending_accepts{0};
    std::vector<LegState> legs;

    bool anyPlacementActivity() const {
        for (LegState s : legs) {
            if (legIsCancelInFlight(s) || s == LegState::PLACE_PENDING) {
                return true;
            }
        }
        return false;
    }
    bool quiet() const { return pending_accepts == 0 && !anyPlacementActivity(); }
};

// -----------------------------------------------------------------------------
// (a) fire-and-track acquires the lease instantly while its OWN legs are
//     CANCEL_SENT_UNCONFIRMED — the bug's direct regression test.
// -----------------------------------------------------------------------------
void fire_and_track_grants_while_own_legs_unconfirmed() {
    // Fire-and-track just sent both cancels: legs are CANCEL_SENT_UNCONFIRMED.
    ProductBook book;
    book.legs = {LegState::CANCEL_SENT_UNCONFIRMED, LegState::CANCEL_SENT_UNCONFIRMED};
    TX_REQUIRE(!book.quiet());  // never quiet during the fire-and-track window

    // ENFORCE + sharding ON + on the AX's own mover thread -> bypass eligible.
    const bool bypass = mmSerializedSingleThreadLeaseBypassEligible(
        /*enforce_mode=*/true, /*sharding_resolved_on=*/true,
        /*current_thread_is_mover_for_ax=*/true);
    TX_REQUIRE(bypass);

    // Lease is free (owner empty -> available), product NOT quiet, not degraded.
    const bool granted = mmProductLeaseClaimGranted(
        /*owner_available=*/true, bypass, book.quiet(), /*degraded_bypass=*/false);
    TX_REQUIRE(granted);  // BUG regression: must grant instantly despite !quiet
}

// -----------------------------------------------------------------------------
// (b) a DIFFERENT stack owning the lease is still denied under the bypass.
// -----------------------------------------------------------------------------
void different_owner_denies_even_under_bypass() {
    const bool bypass = mmSerializedSingleThreadLeaseBypassEligible(true, true, true);
    TX_REQUIRE(bypass);

    // Another live stack owns the lease -> owner_available=false. Never steal it,
    // even though the single-thread bypass is otherwise eligible.
    const bool granted_while_owned = mmProductLeaseClaimGranted(
        /*owner_available=*/false, bypass, /*product_quiet=*/false, /*degraded_bypass=*/false);
    TX_REQUIRE(!granted_while_owned);

    // Once that owner releases (owner_available becomes true), the same waiter
    // is granted under the bypass.
    const bool granted_after_release = mmProductLeaseClaimGranted(
        /*owner_available=*/true, bypass, /*product_quiet=*/false, /*degraded_bypass=*/false);
    TX_REQUIRE(granted_after_release);
}

// -----------------------------------------------------------------------------
// (c) off/shadow quiet-gate behavior is byte-identical to before the fix.
//     In these modes the bypass is never eligible, so the grant reduces exactly
//     to the legacy (product_quiet || degraded_bypass) gate.
// -----------------------------------------------------------------------------
void off_shadow_quiet_gate_unchanged() {
    // Neither off nor shadow are ENFORCE -> never eligible for the bypass, even
    // with sharding on and running on the mover thread.
    TX_REQUIRE(!mmSerializedSingleThreadLeaseBypassEligible(
        /*enforce_mode=*/false, /*sharding_resolved_on=*/true,
        /*current_thread_is_mover_for_ax=*/true));

    // Legacy gate table, reproduced exactly via the pure predicate with
    // serialized_single_thread_bypass == false:
    ProductBook quiet_book;
    quiet_book.legs = {LegState::IDLE, LegState::FILLED};
    TX_REQUIRE(quiet_book.quiet());

    ProductBook busy_book;  // same CANCEL_SENT_UNCONFIRMED scenario as (a)
    busy_book.legs = {LegState::CANCEL_SENT_UNCONFIRMED};
    TX_REQUIRE(!busy_book.quiet());

    // quiet -> granted
    TX_REQUIRE(mmProductLeaseClaimGranted(true, false, quiet_book.quiet(), false));
    // NOT quiet, not degraded -> DENIED (this is the exact pre-fix behavior that
    // (a) deliberately changes only under the bypass; shadow/off keep denying).
    TX_REQUIRE(!mmProductLeaseClaimGranted(true, false, busy_book.quiet(), false));
    // NOT quiet, degraded -> granted (legacy fully-degraded 0/0 bypass).
    TX_REQUIRE(mmProductLeaseClaimGranted(true, false, busy_book.quiet(), true));
    // even quiet is denied if another stack owns the lease.
    TX_REQUIRE(!mmProductLeaseClaimGranted(false, false, quiet_book.quiet(), false));
}

// -----------------------------------------------------------------------------
// (d) a placement attempt from a non-mover thread does NOT get the bypass.
//     Timer / feed / recovery threads (not the AX's own mover) fall back to the
//     legacy quiet gate no matter the mode or sharding state.
// -----------------------------------------------------------------------------
void non_mover_thread_does_not_get_bypass() {
    // ENFORCE + sharding on, but NOT on the AX's mover thread -> not eligible.
    TX_REQUIRE(!mmSerializedSingleThreadLeaseBypassEligible(
        /*enforce_mode=*/true, /*sharding_resolved_on=*/true,
        /*current_thread_is_mover_for_ax=*/false));

    // Sharding not resolved on (single global mover) -> not eligible either.
    TX_REQUIRE(!mmSerializedSingleThreadLeaseBypassEligible(
        /*enforce_mode=*/true, /*sharding_resolved_on=*/false,
        /*current_thread_is_mover_for_ax=*/true));

    // A non-mover caller with CANCEL_SENT_UNCONFIRMED legs (not quiet) is denied,
    // exactly like the legacy path — no bypass leaks off the mover thread.
    ProductBook book;
    book.legs = {LegState::CANCEL_SENT_UNCONFIRMED};
    const bool bypass = mmSerializedSingleThreadLeaseBypassEligible(true, true, false);
    TX_REQUIRE(!bypass);
    TX_REQUIRE(!mmProductLeaseClaimGranted(true, bypass, book.quiet(), false));
}

}  // namespace

int main() {
    std::printf("=== product placement lease quiet-gate bypass ===\n");
    TX_RUN(fire_and_track_grants_while_own_legs_unconfirmed);
    TX_RUN(different_owner_denies_even_under_bypass);
    TX_RUN(off_shadow_quiet_gate_unchanged);
    TX_RUN(non_mover_thread_does_not_get_bypass);
    return tx::finish("test_product_lease_bypass");
}
