#pragma once

// =============================================================================
// Pure decision predicates for the per-AX product placement lease.
//
// These are intentionally dependency-free (plain bools) so the exact grant
// logic used in production (MakeMarketStrategy::mmAcquireProductPlacementLease /
// mmTryAtomicClaimProductPlacementLeaseLocked) can be unit-tested in isolation
// without spinning up a full MakeMarketStrategy, config, gateway, or mover.
// =============================================================================

namespace architect {
namespace strategy {

// Is the per-instrument-sharded ENFORCE neutralization ("quiet-gate bypass")
// active for THIS claim attempt?
//
// True ONLY when all three hold:
//   * position-book ENFORCE mode is resolved for the symbol, AND
//   * mover sharding is resolved ON (one worker per instrument), AND
//   * we are executing on the AX's OWN mover worker thread.
//
// Under those conditions every stack on the AX is strictly serialized on the
// same thread, so the cross-stack "product quiet" requirement is redundant.
// off/shadow, the single global mover, and any non-mover caller (timers,
// feed threads) are NOT eligible and keep the legacy quiet gate.
inline bool mmSerializedSingleThreadLeaseBypassEligible(bool enforce_mode,
                                                        bool sharding_resolved_on,
                                                        bool current_thread_is_mover_for_ax) {
    return enforce_mode && sharding_resolved_on && current_thread_is_mover_for_ax;
}

// Pure product-lease grant decision, evaluated AFTER any stale-owner expiry has
// been resolved by the caller.
//
//   owner_available: the lease owner is empty OR already equals this waiter.
//   serialized_single_thread_bypass: result of the predicate above.
//   product_quiet: no pending accepts and no in-flight placement activity on AX.
//   degraded_bypass: this stack is fully degraded (legacy 0/0 quiet-bypass).
//
// Under the single-thread bypass the "product quiet" requirement is skipped
// (thread serialization already guarantees mutual exclusion), which is what lets
// fire-and-track place while its OWN just-cancelled legs are still
// CANCEL_SENT_UNCONFIRMED (product is never "quiet" during that window).
// off/shadow keep the exact legacy behavior: quiet OR degraded.
inline bool mmProductLeaseClaimGranted(bool owner_available,
                                       bool serialized_single_thread_bypass,
                                       bool product_quiet,
                                       bool degraded_bypass) {
    if (!owner_available) {
        return false;  // another live stack owns the lease — never steal it.
    }
    if (serialized_single_thread_bypass) {
        return true;  // serialized on one mover thread — quiet gate is redundant.
    }
    return product_quiet || degraded_bypass;  // legacy off/shadow gate, unchanged.
}

}  // namespace strategy
}  // namespace architect
