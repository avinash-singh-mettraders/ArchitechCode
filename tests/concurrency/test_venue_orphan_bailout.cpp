// =============================================================================
// REGRESSION TEST — Venue source-of-truth guard, requirement 6 (2026-06-11)
// =============================================================================
//
// Problem: when orders are cancelled externally (exchange GUI cancel-all,
// margin breach, any external cancellation), the engine's local state becomes
// stale and mover-thread workers keep operating on orders that no longer exist
// at the venue.
//
// Fix modeled here (production: MakeMarketStrategy):
//   - A stack state machine (ACTIVE / VENUE_ORPHANED / TEARDOWN / DEAD) is the
//     single source of truth for whether a worker may proceed. Only ACTIVE
//     proceeds; anything else bails out cleanly  (mmWorkerMayProceed).
//   - A cached "last known venue order state" (mm_venue_truth_oids_) gated by a
//     single mutex (mm_venue_truth_mu_). Before any outbound cancel a worker
//     checks the cache; an OID the venue no longer reports is skipped and an
//     immediate reconcile is triggered (mmVenueTruthKnowsOid /
//     mmTriggerImmediateVenueReconcile).
//   - When the venue evicts ALL legs (live_oids=0) the poll path immediately
//     transitions the stack to VENUE_ORPHANED under that same mutex, so the
//     cache-empty and the state-flip are observed atomically together.
//
// This test models that contract exactly and asserts, under N concurrent
// workers mid-operation while an external cancel-all empties the venue:
//   - VENUE_ORPHANED is set immediately (not on the next poll cycle).
//   - Every worker bails out cleanly afterwards.
//   - No stale request is ever sent to the (now empty) venue.
//   - A reconcile was triggered.
//
// SCOPE (2026-06-15) — what this test does and does NOT cover:
//   This suite models the STACK STATE-MACHINE orphan-bailout guard: when the
//   venue is GENUINELY emptied (external cancel-all / margin breach) the stack
//   flips to VENUE_ORPHANED and every worker bails by STATE. That contract is
//   unchanged and still correct.
//
//   It does NOT model the per-leg CANCEL-SEND decision. The venue-truth
//   "skip the cancel when the oid is absent from a fresh snapshot" behavior was
//   REMOVED from the production cancel path on 2026-06-15 (it conflated "not yet
//   propagated into /open-orders" with "already gone" and leaked orphans — see
//   mmCancelTrackedLegThisStackGateway / mmSendRestCancelByOrderId). The cancel
//   path now ALWAYS sends the real cancel. This file must NOT be cited to
//   justify reintroducing skip-on-absent for the cancel path; the authoritative
//   cancel-path contract lives in test_venue_truth_always_cancel.cpp.

#include "test_helpers.h"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>

namespace {

// Mirror of production strategy::MmStackState.
enum class StackState : int { ACTIVE = 0, VENUE_ORPHANED, TEARDOWN, DEAD };

// Faithful model of the per-stack venue source-of-truth guard. The send-decision
// (state + venue-cache read) and the venue-poll update (cache replace + orphan
// flip) are both serialized on `venue_mu_`, exactly as the production code routes
// both through mm_venue_truth_mu_.
struct GuardedStack {
    std::atomic<StackState> state_{StackState::ACTIVE};
    std::atomic<int>        reconcile_triggers_{0};

    mutable std::mutex            venue_mu_;
    std::unordered_set<std::string> venue_oids_;   // last-known venue order state
    bool                         venue_emptied_{false};  // external cancel-all happened

    // Production: mmTransitionStackState (monotonic toward DEAD).
    bool transition(StackState to) {
        StackState from = state_.load(std::memory_order_acquire);
        for (;;) {
            if (from == to) return false;
            if (from == StackState::DEAD) return false;
            if (from == StackState::TEARDOWN && to != StackState::DEAD) return false;
            if (state_.compare_exchange_weak(from, to, std::memory_order_acq_rel,
                                             std::memory_order_acquire)) {
                return true;
            }
        }
    }

    // Production: mmWorkerMayProceed.
    bool worker_may_proceed() const {
        return state_.load(std::memory_order_acquire) == StackState::ACTIVE;
    }

    void trigger_reconcile() { reconcile_triggers_.fetch_add(1, std::memory_order_relaxed); }

    // ---- H2 (2026-06-11): bounded VENUE_ORPHANED->ACTIVE auto-recovery -------------------
    // Mirrors the production gate in mmReconcileTrackedAgainstVenue: recovery requires BOTH a
    // minimum spacing (kRecoverMinIntervalMs) AND a consecutive-attempt cap
    // (kRecoverMaxConsecutive). Recovery is attempted whenever the stack is orphaned + desired
    // (not just on the ACTIVE->ORPHANED edge). The consecutive counter resets on a healthy cycle.
    static constexpr std::int64_t kRecoverMinIntervalMs = 2000;
    static constexpr int          kRecoverMaxConsecutive = 5;

    std::atomic<std::int64_t> last_recover_ms_{0};
    std::atomic<int>          consec_recoveries_{0};

    // Returns true iff this call transitioned back to ACTIVE (a recovery happened).
    bool attempt_orphan_recovery(std::int64_t now_ms, bool still_desired) {
        const std::int64_t last  = last_recover_ms_.load(std::memory_order_acquire);
        const int          consec = consec_recoveries_.load(std::memory_order_acquire);
        transition(StackState::VENUE_ORPHANED);
        if (state_.load(std::memory_order_acquire) != StackState::VENUE_ORPHANED) {
            return false;
        }
        const bool interval_ok = (last == 0) || (now_ms - last >= kRecoverMinIntervalMs);
        const bool under_cap   = consec < kRecoverMaxConsecutive;
        if (still_desired && interval_ok && under_cap) {
            last_recover_ms_.store(now_ms, std::memory_order_release);
            consec_recoveries_.fetch_add(1, std::memory_order_acq_rel);
            transition(StackState::ACTIVE);
            return true;
        }
        return false;
    }

    void healthy_cycle() { consec_recoveries_.store(0, std::memory_order_release); }
};

// External cancel-all (exchange GUI / margin breach): empty the venue cache AND
// flip to VENUE_ORPHANED under the same lock so a worker can never observe an
// empty venue while still ACTIVE.
void external_cancel_all(GuardedStack& s, std::vector<std::string>& evicted_out) {
    std::lock_guard<std::mutex> lk(s.venue_mu_);
    for (const auto& oid : s.venue_oids_) evicted_out.push_back(oid);
    s.venue_oids_.clear();
    s.venue_emptied_ = true;
    // Immediate orphan transition (do NOT wait for the next scheduled poll).
    s.transition(StackState::VENUE_ORPHANED);
    // The poll that detected live_oids=0 schedules an immediate reconcile to confirm
    // (production: mmTriggerImmediateVenueReconcile on the orphan/recovery path).
    s.trigger_reconcile();
}

// One worker "outbound cancel" attempt for `oid`. Returns true iff it actually
// sent to the venue. `bad_send` is set true iff it sent for an oid the venue no
// longer has (a stale request — must never happen).
bool worker_try_cancel(GuardedStack& s, const std::string& oid, bool& bad_send) {
    bad_send = false;
    // Gate 1: state machine — only ACTIVE proceeds.
    if (!s.worker_may_proceed()) {
        return false;
    }
    // Gate 2: under the cache mutex so the decision and the venue state are consistent.
    // NOTE: this models bailing on an EXTERNALLY-EMPTIED venue (the oid is GENUINELY gone
    // because of a cancel-all / margin breach), which pairs with the immediate
    // VENUE_ORPHANED flip in external_cancel_all() below. It is NOT the per-leg cancel-send
    // decision: production's cancel path no longer skips on a merely-absent oid (that was the
    // propagation-lag leak; see the SCOPE note up top and test_venue_truth_always_cancel.cpp).
    std::lock_guard<std::mutex> lk(s.venue_mu_);
    if (s.venue_oids_.count(oid) == 0) {
        s.trigger_reconcile();
        return false;
    }
    // Send. If the venue was already emptied this would be a stale request.
    if (s.venue_emptied_) {
        bad_send = true;
    }
    return true;
}

void venue_orphan_sets_state_immediately_and_workers_bail() {
    GuardedStack s;
    {
        std::lock_guard<std::mutex> lk(s.venue_mu_);
        s.venue_oids_ = {"OID_BID", "OID_ASK"};
    }

    constexpr int kWorkers = 8;
    std::atomic<bool> stop{false};
    std::atomic<int>  stale_sends{0};
    std::atomic<int>  successful_sends{0};
    std::atomic<int>  bailouts{0};

    std::vector<std::thread> workers;
    workers.reserve(kWorkers);
    for (int w = 0; w < kWorkers; ++w) {
        workers.emplace_back([&, w] {
            const std::string oid = (w % 2 == 0) ? "OID_BID" : "OID_ASK";
            while (!stop.load(std::memory_order_acquire)) {
                bool bad = false;
                if (worker_try_cancel(s, oid, bad)) {
                    successful_sends.fetch_add(1, std::memory_order_relaxed);
                } else {
                    bailouts.fetch_add(1, std::memory_order_relaxed);
                }
                if (bad) stale_sends.fetch_add(1, std::memory_order_relaxed);
                std::this_thread::sleep_for(std::chrono::microseconds(50));
            }
        });
    }

    // Let workers run mid-operation, then fire the external cancel-all.
    std::this_thread::sleep_for(std::chrono::milliseconds(40));
    std::vector<std::string> evicted;
    external_cancel_all(s, evicted);

    // VENUE_ORPHANED must be set immediately by the cancel-all itself.
    TX_REQUIRE(s.state_.load() == StackState::VENUE_ORPHANED);

    // Let workers keep spinning after the orphan; they must all bail now.
    std::this_thread::sleep_for(std::chrono::milliseconds(80));
    stop.store(true, std::memory_order_release);
    for (auto& t : workers) t.join();

    std::printf("    evicted_oids=%zu successful_sends=%d bailouts=%d stale_sends=%d reconciles=%d\n",
                evicted.size(), successful_sends.load(), bailouts.load(),
                stale_sends.load(), s.reconcile_triggers_.load());

    // No stale request was ever sent to the emptied venue.
    TX_EQ(stale_sends.load(), 0);
    // The eviction reported the legs the venue lost.
    TX_EQ(static_cast<int>(evicted.size()), 2);
    // A reconcile was triggered (workers that raced past gate 1 hit gate 2's skip).
    TX_REQUIRE(s.reconcile_triggers_.load() >= 1);
    // State stays orphaned (this scenario has no auto-recovery driver running).
    TX_REQUIRE(s.state_.load() == StackState::VENUE_ORPHANED);
}

// Cancel-path contract AFTER the 2026-06-15 order-leak fix (replaces the old
// "skip-on-absent" assertion that encoded the bug). A still-ACTIVE worker whose oid is
// ABSENT from even a FRESH venue snapshot must ALWAYS send the real cancel — never skip —
// because a just-placed order may simply not have propagated into /open-orders yet.
// We only refresh venue truth for the NEXT decision. Mirrors the post-fix decision in
// mmCancelTrackedLegThisStackGateway / mmSendRestCancelByOrderId.
enum class CancelAction { kSendRealCancel, kSkip };

// Post-fix cancel decision: NO skip branch — refresh truth if absent, then ALWAYS send.
CancelAction cancel_decision_after_fix(GuardedStack& s, const std::string& oid) {
    std::lock_guard<std::mutex> lk(s.venue_mu_);
    if (s.venue_oids_.count(oid) == 0) {
        s.trigger_reconcile();  // refresh for the NEXT decision...
    }
    return CancelAction::kSendRealCancel;  // ...then ALWAYS send. Never skip-on-absent.
}

void cancel_path_always_sends_real_cancel_even_when_oid_absent() {
    GuardedStack s;
    {
        std::lock_guard<std::mutex> lk(s.venue_mu_);
        s.venue_oids_ = {"OID_FROM_PRIOR_CYCLE"};  // snapshot is FRESH but lacks the new oid
    }
    // The just-placed leg has not propagated into /open-orders yet — state still ACTIVE.
    TX_REQUIRE(s.worker_may_proceed());
    const CancelAction a = cancel_decision_after_fix(s, "JUST_PLACED_OID");
    TX_REQUIRE(a == CancelAction::kSendRealCancel);  // MUST send — skipping here was the leak
    TX_EQ(s.reconcile_triggers_.load(), 1);          // truth refreshed for the next decision
}

// Once VENUE_ORPHANED, mmWorkerMayProceed must refuse every operation type.
void orphaned_state_blocks_all_workers() {
    GuardedStack s;
    {
        std::lock_guard<std::mutex> lk(s.venue_mu_);
        s.venue_oids_ = {"OID_BID"};
    }
    TX_REQUIRE(s.worker_may_proceed());
    s.transition(StackState::VENUE_ORPHANED);
    TX_REQUIRE(!s.worker_may_proceed());
    s.transition(StackState::TEARDOWN);
    TX_REQUIRE(!s.worker_may_proceed());
    s.transition(StackState::DEAD);
    TX_REQUIRE(!s.worker_may_proceed());
}

// Monotonic transition guard: TEARDOWN may only advance to DEAD; DEAD is terminal;
// ACTIVE<->VENUE_ORPHANED is reversible (auto-recovery edge).
void transition_monotonicity() {
    GuardedStack s;
    TX_REQUIRE(s.transition(StackState::VENUE_ORPHANED));
    TX_REQUIRE(s.transition(StackState::ACTIVE));            // recover
    TX_REQUIRE(s.transition(StackState::VENUE_ORPHANED));
    TX_REQUIRE(s.transition(StackState::TEARDOWN));
    TX_REQUIRE(!s.transition(StackState::ACTIVE));           // teardown can't go back
    TX_REQUIRE(!s.transition(StackState::VENUE_ORPHANED));   // nor orphaned
    TX_REQUIRE(s.transition(StackState::DEAD));
    TX_REQUIRE(!s.transition(StackState::ACTIVE));           // dead is terminal
    TX_REQUIRE(!s.transition(StackState::TEARDOWN));
}

// H2: under PERSISTENT venue emptiness, VENUE_ORPHANED->ACTIVE auto-recovery must not exceed the
// consecutive cap within the throttle window. After the cap is reached the stack stays
// VENUE_ORPHANED (no more cancel/replace churn); a healthy cycle resets the allowance.
void orphan_recovery_is_bounded_by_consecutive_cap() {
    GuardedStack s;
    int recoveries = 0;
    std::int64_t now = 1000;
    // Space every attempt by >= the min interval so ONLY the consecutive cap can stop recovery.
    for (int i = 0; i < 50; ++i) {
        if (s.attempt_orphan_recovery(now, /*still_desired=*/true)) ++recoveries;
        now += GuardedStack::kRecoverMinIntervalMs;
    }
    std::printf("    persistent-empty recoveries=%d cap=%d\n",
                recoveries, GuardedStack::kRecoverMaxConsecutive);
    TX_EQ(recoveries, GuardedStack::kRecoverMaxConsecutive);  // capped, not unbounded thrash
    TX_REQUIRE(s.state_.load() == StackState::VENUE_ORPHANED);  // stays bailed once capped

    // A healthy cycle (venue reports live legs again) resets the allowance.
    s.healthy_cycle();
    TX_REQUIRE(s.attempt_orphan_recovery(now + GuardedStack::kRecoverMinIntervalMs, true));
    TX_REQUIRE(s.state_.load() == StackState::ACTIVE);
}

// H2: recovery also respects the minimum interval — a retry inside the window must NOT recover,
// but once the interval elapses recovery is allowed again (so a transient orphaning still heals).
void orphan_recovery_respects_min_interval() {
    GuardedStack s;
    TX_REQUIRE(s.attempt_orphan_recovery(1000, true));            // first recovery (last==0) allowed
    TX_REQUIRE(s.state_.load() == StackState::ACTIVE);
    TX_REQUIRE(!s.attempt_orphan_recovery(1000 + 10, true));      // within interval -> throttled
    TX_REQUIRE(s.state_.load() == StackState::VENUE_ORPHANED);    // stays orphaned, workers bail
    TX_REQUIRE(s.attempt_orphan_recovery(1000 + GuardedStack::kRecoverMinIntervalMs, true));
    TX_REQUIRE(s.state_.load() == StackState::ACTIVE);            // interval elapsed -> recovers
}

// H2: a stack that is no longer desired (teardown-bound, not running) must never auto-recover.
void orphan_recovery_skips_when_not_desired() {
    GuardedStack s;
    TX_REQUIRE(!s.attempt_orphan_recovery(1000, /*still_desired=*/false));
    TX_REQUIRE(s.state_.load() == StackState::VENUE_ORPHANED);
}

}  // namespace

int main() {
    std::printf("=== Venue source-of-truth guard: external cancel-all worker bail-out (req 6) ===\n");
    TX_RUN(venue_orphan_sets_state_immediately_and_workers_bail);
    TX_RUN(cancel_path_always_sends_real_cancel_even_when_oid_absent);
    TX_RUN(orphaned_state_blocks_all_workers);
    TX_RUN(transition_monotonicity);
    TX_RUN(orphan_recovery_is_bounded_by_consecutive_cap);
    TX_RUN(orphan_recovery_respects_min_interval);
    TX_RUN(orphan_recovery_skips_when_not_desired);
    return tx::finish("test_venue_orphan_bailout");
}
