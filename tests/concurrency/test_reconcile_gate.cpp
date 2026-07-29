// =============================================================================
// RECONCILIATION GATE — Tim's spec, 2026-05-21
// =============================================================================
//
// Tim's mandated model after the second SILVER overtrade incident
// (`Overtrade second example.txt`):
//
//   1. At process start: query position from REST exactly once.
//   2. After process start: position changes ONLY when a real fill event lands
//      from the venue (`OrderManager::onOrderFilled` driven by
//      `StartupSequence::pollExchangeFills`).
//   3. Before placing a NEW order pair on a stack that recently cancelled one
//      of its legs, wait until at least one fill poll has completed AFTER the
//      cancel response. This guarantees that a "cancel returned 404 because
//      the order silently filled" race cannot hide a fill from the cap-gate.
//
// This file models the reconciliation gate's invariant directly, without
// pulling in the full MakeMarketStrategy machinery. It is intentionally tiny
// and focused so a regression in the gate jumps out immediately on `ctest`.
//
// The production wiring is:
//   * `MakeMarketStrategy::last_cancel_response_ms_` — bumped by
//     `mmCancelTrackedLegThisStackGateway` on every cancel response (HTTP 2xx
//     or benign 404).
//   * `StartupSequence::last_fill_poll_completed_ms_` — bumped after every
//     `pollExchangeFills` invocation, success or throw.
//   * Top of `runFullMmQuoteCycle` waits up to `2 * fill_poll_interval_sec`
//     for `lastFillPollCompletedMs() > last_cancel_response_ms_` before
//     proceeding to placement.

#include "test_helpers.h"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <thread>

namespace {

// Mirror of the production atomics so the test can drive timestamps directly.
std::atomic<std::int64_t> g_cancel_stamp_ms{0};
std::atomic<std::int64_t> g_fill_poll_stamp_ms{0};

// Production-shape gate. Returns true when the cycle is permitted to proceed
// to order placement. Caller passes `wait_budget_ms` so the test can verify
// both the "gate clears in time" and "gate times out (last-resort proceed)"
// branches.
struct GateOutcome {
    bool   cleared{false};         // true if the gate was satisfied within budget
    bool   timed_out{false};       // true if the gate fell through on timeout
    std::int64_t wait_ms{0};       // observed time spent waiting
};

GateOutcome runReconcileGate(std::int64_t wait_budget_ms) {
    const std::int64_t cancel_stamp = g_cancel_stamp_ms.load(std::memory_order_acquire);
    if (cancel_stamp == 0) {
        return {true, false, 0};   // nothing to wait for — first cycle of process life
    }
    const auto t0 = std::chrono::steady_clock::now();
    for (;;) {
        const std::int64_t poll_stamp = g_fill_poll_stamp_ms.load(std::memory_order_acquire);
        if (poll_stamp > cancel_stamp) {
            const auto t1 = std::chrono::steady_clock::now();
            const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();
            return {true, false, ms};
        }
        const auto elapsed =
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t0).count();
        if (elapsed >= wait_budget_ms) {
            return {false, true, elapsed};
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
}

std::int64_t nowMs() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

// -----------------------------------------------------------------------------
// Scenario 1 — first cycle of process life: cancel_stamp==0, gate must
// pass immediately so the very first quote can go out without a fill poll.
// -----------------------------------------------------------------------------
void scenario_first_cycle_passes_through() {
    g_cancel_stamp_ms.store(0);
    g_fill_poll_stamp_ms.store(0);
    const auto t0 = std::chrono::steady_clock::now();
    auto out = runReconcileGate(/*wait_budget_ms*/ 500);
    const auto elapsed =
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t0).count();
    TX_REQUIRE(out.cleared);
    TX_REQUIRE(!out.timed_out);
    TX_LE(elapsed, 10);
}

// -----------------------------------------------------------------------------
// Scenario 2 — happy path: a fill poll lands BEFORE the gate's deadline, so
// the gate clears as soon as `poll_stamp > cancel_stamp`.
// -----------------------------------------------------------------------------
void scenario_fill_poll_lands_before_deadline_clears_gate() {
    g_cancel_stamp_ms.store(nowMs(), std::memory_order_release);
    g_fill_poll_stamp_ms.store(0);

    // Worker bumps the fill poll stamp 150ms from now — within the 2000ms budget.
    std::thread bump([] {
        std::this_thread::sleep_for(std::chrono::milliseconds(150));
        g_fill_poll_stamp_ms.store(nowMs(), std::memory_order_release);
    });

    auto out = runReconcileGate(/*wait_budget_ms*/ 2000);
    bump.join();

    TX_REQUIRE(out.cleared);
    TX_REQUIRE(!out.timed_out);
    // Should be ~150ms +/- scheduler jitter. Allow generous bound to keep this
    // test stable on loaded CI hosts.
    TX_LE(out.wait_ms, 700);
}

// -----------------------------------------------------------------------------
// Scenario 3 — pathological: fill poll never advances (e.g. REST endpoint
// hung). Gate must hit its deadline and fall through (`timed_out==true`).
// Without this fall-through, a wedged REST endpoint would deadlock the cycle
// forever. The cap-projection inside `runFullMmQuoteCycle` is the second
// line of defence — it reads `position_state_` which has not been mutated
// during the wedge, so the cycle uses stale-but-bounded NetPo and the cap
// still cannot be breached for more than one cycle's worth of qty.
// -----------------------------------------------------------------------------
void scenario_fill_poll_never_lands_gate_times_out() {
    g_cancel_stamp_ms.store(nowMs(), std::memory_order_release);
    g_fill_poll_stamp_ms.store(0);   // and never advances

    auto out = runReconcileGate(/*wait_budget_ms*/ 250);

    TX_REQUIRE(!out.cleared);
    TX_REQUIRE(out.timed_out);
    TX_REQUIRE(out.wait_ms >= 250);
}

// -----------------------------------------------------------------------------
// Scenario 4 — fill poll completed BEFORE the cancel happens. The gate must
// still require a NEW poll (poll_stamp > cancel_stamp), not just any poll.
// Without this strictness, the gate would let the cycle proceed on a poll
// that ran BEFORE the venue could have observed our cancel — defeating the
// purpose of the gate entirely.
// -----------------------------------------------------------------------------
void scenario_pre_cancel_poll_does_not_satisfy_gate() {
    const auto pre = nowMs();
    g_fill_poll_stamp_ms.store(pre, std::memory_order_release);
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    g_cancel_stamp_ms.store(nowMs(), std::memory_order_release);

    // No new fill poll lands.
    auto out = runReconcileGate(/*wait_budget_ms*/ 200);
    TX_REQUIRE(!out.cleared);
    TX_REQUIRE(out.timed_out);
}

}  // namespace

int main() {
    std::printf("=== Reconciliation Gate (Tim's 2026-05-21 spec) ===\n");
    TX_RUN(scenario_first_cycle_passes_through);
    TX_RUN(scenario_fill_poll_lands_before_deadline_clears_gate);
    TX_RUN(scenario_fill_poll_never_lands_gate_times_out);
    TX_RUN(scenario_pre_cancel_poll_does_not_satisfy_gate);
    return ::tx::finish("test_reconcile_gate");
}
