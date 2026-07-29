// =============================================================================
// REGRESSION TEST — Tim Izzo's Issue #1 (2026-05-19, silver flatten)
// =============================================================================
//
// "Joe manually flattened a silver short of 1. The 1-lot on the offer side
//  reloaded. There was a 1-lot bid reload which should not have happened.
//  Final result: 3 offers and 4 bids when he only loaded 3 pairs."
//
// Root cause: `mm_pair_enforce` runs on the desk bg thread every ~1s and
// checks if a desk-managed stack is "stuck" (no live bid_order_id_ and no
// live ask_order_id_, but JSON still owns the stack). It then triggered
// DESK_RECOVERY which clears tracked vectors and enqueues a new full quote
// cycle. But during a NORMAL cancel-replace the mover thread produces a
// transient `bid_order_id_=0 && ask_order_id_=0` window between cancel-ack
// and place-submit. mm_pair_enforce saw that and added an extra place that
// did not have a matching cancel → extra bid visible on the desk.
//
// Fix (ensureStartupQuotePair, src/strategy/MakeMarketStrategy.cpp ~2898):
//   desk_recovery_eligible now requires ALL of:
//     1. bid_order_id_ == 0 && ask_order_id_ == 0          (stale check)
//     2. mm_manual_stack_.desk_seeded                       (we own the stack)
//     3. mmDeskOrdersJsonContainsThisStack()                (JSON still has it)
//     4. !mm_cycle_running_                                  (NEW — RAII guard
//                                                             set inside
//                                                             runFullMmQuoteCycle)
//     5. !mm_quote_cycle_pending_                            (NEW — no enqueued
//                                                             cycle waiting)
//     6. mm_pending_accepts_ == 0                            (NEW — no place ack
//                                                             in flight)
//     7. now - mm_last_cycle_activity_ms_ >= quiet_ms        (NEW — 1.5s grace)
//
// This test models the guard predicate exactly and shows:
//   - It correctly INHIBITS recovery during a normal cancel-replace.
//   - It correctly INHIBITS recovery when an accept is in flight.
//   - It correctly INHIBITS recovery during the quiet window after activity.
//   - It correctly ADMITS recovery for a genuinely stuck stack.
//
// Plus a concurrency stress test: spawn a "mover" thread that cycles the
// flags as production does, and an "enforcer" thread that polls the
// predicate. Assert that no false-positive recovery ever fires during an
// in-flight cycle.

#include "test_helpers.h"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <thread>

namespace {

// Exact mirror of the production state used by ensureStartupQuotePair to
// decide DESK_RECOVERY eligibility.
struct DeskRecoveryState {
    std::atomic<uint64_t> bid_order_id_{0};
    std::atomic<uint64_t> ask_order_id_{0};

    bool desk_seeded{true};                                 // mm_manual_stack_->desk_seeded
    bool orders_json_owns{true};                            // mmDeskOrdersJsonContainsThisStack()
    std::atomic<bool>     cycle_running_{false};            // mm_cycle_running_
    std::atomic<bool>     quote_cycle_pending_{false};      // mm_quote_cycle_pending_
    std::atomic<int>      pending_accepts_{0};              // mm_pending_accepts_ (now atomic — fix C2)
    std::atomic<int64_t>  last_cycle_activity_ms_{0};       // mm_last_cycle_activity_ms_

    int quiet_ms{1500};                                     // market_maker.desk_recovery_quiet_ms

    static int64_t now_ms() {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
                   std::chrono::steady_clock::now().time_since_epoch())
            .count();
    }

    // Reproduces the boolean expression in ensureStartupQuotePair line ~2941.
    bool eligible() const {
        const int64_t activity = last_cycle_activity_ms_.load(std::memory_order_relaxed);
        const bool cycle_quiet =
            (activity == 0) || ((now_ms() - activity) >= quiet_ms);
        return bid_order_id_.load(std::memory_order_acquire) == 0 &&
               ask_order_id_.load(std::memory_order_acquire) == 0 &&
               desk_seeded &&
               orders_json_owns &&
               !cycle_running_.load(std::memory_order_acquire) &&
               !quote_cycle_pending_.load(std::memory_order_acquire) &&
               pending_accepts_.load(std::memory_order_acquire) == 0 &&
               cycle_quiet;
    }
};

void inhibits_during_in_flight_cycle() {
    DeskRecoveryState s;
    s.cycle_running_.store(true, std::memory_order_release);
    // Cancel just happened, place not yet submitted — exact race window.
    s.bid_order_id_.store(0, std::memory_order_release);
    s.ask_order_id_.store(0, std::memory_order_release);
    TX_REQUIRE(!s.eligible());  // PRE-FIX: this returned true → phantom recovery.
}

void inhibits_during_pending_accept() {
    DeskRecoveryState s;
    s.cycle_running_.store(false, std::memory_order_release);
    s.pending_accepts_.store(1, std::memory_order_release);
    s.last_cycle_activity_ms_.store(0, std::memory_order_relaxed);  // bypass quiet gate
    TX_REQUIRE(!s.eligible());
}

void inhibits_during_quote_cycle_pending() {
    DeskRecoveryState s;
    s.quote_cycle_pending_.store(true, std::memory_order_release);
    TX_REQUIRE(!s.eligible());
}

void inhibits_during_quiet_window() {
    DeskRecoveryState s;
    s.last_cycle_activity_ms_.store(DeskRecoveryState::now_ms(), std::memory_order_relaxed);
    TX_REQUIRE(!s.eligible());  // Activity right now → must wait 1.5s.
}

void admits_after_quiet_window_with_clean_state() {
    DeskRecoveryState s;
    s.last_cycle_activity_ms_.store(
        DeskRecoveryState::now_ms() - 2000, std::memory_order_relaxed);  // 2s ago
    TX_REQUIRE(s.eligible());
}

void admits_with_zero_activity_marker() {
    // last_cycle_activity_ms_ == 0 means "never ran" → quiet-by-definition,
    // recovery may fire (this is the startup case after a process restart).
    DeskRecoveryState s;
    s.last_cycle_activity_ms_.store(0, std::memory_order_relaxed);
    TX_REQUIRE(s.eligible());
}

void inhibits_when_legs_are_live() {
    DeskRecoveryState s;
    s.bid_order_id_.store(42, std::memory_order_release);
    s.ask_order_id_.store(43, std::memory_order_release);
    TX_REQUIRE(!s.eligible());
}

void inhibits_when_json_does_not_own() {
    DeskRecoveryState s;
    s.orders_json_owns = false;
    TX_REQUIRE(!s.eligible());
}

// ---------------------------------------------------------------------------
// Concurrency stress: a "mover" thread runs simulated quote cycles that
// briefly null both ids, place new ids, then null again, etc. An
// "enforcer" thread polls eligible() at high frequency. With the fix in
// place, no eligible==true reading should ever happen DURING an in-flight
// cycle (i.e. while cycle_running_ is true).
// ---------------------------------------------------------------------------
void no_false_eligibility_during_cycle_under_load() {
    DeskRecoveryState s;
    // De-flake: this stress test models STEADY STATE (a stack that has already
    // quoted and is now cycling), not process startup. Leaving the default
    // last_cycle_activity_ms_==0 opens a startup-only window where eligible()
    // takes the "quiet-by-definition" branch (activity==0) before the mover
    // writes its first activity stamp; the enforcer's second, separate read of
    // cycle_running_ (below) could then race the mover setting it true and log a
    // spurious bad observation. The activity==0 / startup-recovery path is
    // covered independently by admits_with_zero_activity_marker(). Seed a fresh
    // stamp so the quiet gate reflects an actively-cycling stack from the start.
    s.last_cycle_activity_ms_.store(DeskRecoveryState::now_ms(), std::memory_order_relaxed);
    std::atomic<bool> stop{false};
    std::atomic<int>  bad_observations{0};
    std::atomic<int>  total_observations{0};

    std::thread mover([&] {
        uint64_t next_oid = 1000;
        while (!stop.load(std::memory_order_acquire)) {
            // Start of cycle
            s.cycle_running_.store(true, std::memory_order_release);
            s.last_cycle_activity_ms_.store(
                DeskRecoveryState::now_ms(), std::memory_order_relaxed);

            // Cancel both legs (production: mmCancelTrackedLegThisStackGateway)
            s.bid_order_id_.store(0, std::memory_order_release);
            s.ask_order_id_.store(0, std::memory_order_release);

            // Place ack expected
            s.pending_accepts_.fetch_add(2, std::memory_order_acq_rel);
            std::this_thread::sleep_for(std::chrono::microseconds(200));

            // Place complete (oids assigned, accepts decrement once OM dispatches)
            s.bid_order_id_.store(next_oid++, std::memory_order_release);
            s.ask_order_id_.store(next_oid++, std::memory_order_release);
            s.last_cycle_activity_ms_.store(
                DeskRecoveryState::now_ms(), std::memory_order_relaxed);
            s.cycle_running_.store(false, std::memory_order_release);

            // OM dispatches accept on another thread; simulate decrement
            std::this_thread::sleep_for(std::chrono::microseconds(100));
            s.pending_accepts_.fetch_sub(2, std::memory_order_acq_rel);

            std::this_thread::sleep_for(std::chrono::microseconds(500));
        }
    });

    std::thread enforcer([&] {
        while (!stop.load(std::memory_order_acquire)) {
            const bool e = s.eligible();
            total_observations.fetch_add(1, std::memory_order_relaxed);
            // INVARIANT: it is NEVER safe to fire DESK_RECOVERY while the
            // mover thread reports cycle_running_=true. Production violated
            // this; the fix gates eligible() on !cycle_running_.
            if (e && s.cycle_running_.load(std::memory_order_acquire)) {
                bad_observations.fetch_add(1, std::memory_order_relaxed);
            }
        }
    });

    std::this_thread::sleep_for(std::chrono::seconds(2));
    stop.store(true, std::memory_order_release);
    mover.join();
    enforcer.join();

    std::printf("    polled eligible() %d times; bad observations: %d\n",
                total_observations.load(), bad_observations.load());
    TX_EQ(bad_observations.load(), 0);
}

}  // namespace

int main() {
    std::printf("=== Tim Issue #1: phantom 1-lot bid reload after manual flatten ===\n");
    TX_RUN(inhibits_during_in_flight_cycle);
    TX_RUN(inhibits_during_pending_accept);
    TX_RUN(inhibits_during_quote_cycle_pending);
    TX_RUN(inhibits_during_quiet_window);
    TX_RUN(admits_after_quiet_window_with_clean_state);
    TX_RUN(admits_with_zero_activity_marker);
    TX_RUN(inhibits_when_legs_are_live);
    TX_RUN(inhibits_when_json_does_not_own);
    TX_RUN(no_false_eligibility_during_cycle_under_load);
    return tx::finish("test_desk_recovery_guard");
}
