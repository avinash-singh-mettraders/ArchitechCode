// =============================================================================
// MWR (Moving Window Range) volatility-breaker tests.
// =============================================================================
// These exercise the SAME pure logic the production strategy uses
// (include/strategy/MwrBreaker.h) — no duplicated model. The strategy wiring
// maps onto this logic as follows:
//
//   production sampler  (mmDrainPendingQuoteCycle): 1 Hz throttle + MwrBreaker::push
//   production gate      (runFullMmQuoteCycle):
//        Action::kPausedSkip  -> log debug, early-return (suppress quoting)
//        Action::kBreachPause -> cancel BUY leg + cancel SELL leg + early-return
//        Action::kProceed/... -> run the cycle normally
//        just_paused          -> one warn log; just_resumed -> one info log
//
// Style mirrors the project's other self-contained suites: a tiny CHECK macro,
// each case in its own function, nonzero exit on failure.

#include "strategy/MwrBreaker.h"
#include "strategy/FastMarketMonitor.h"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <limits>
#include <string>
#include <thread>
#include <vector>

using architect::strategy::MwrBreaker;
using architect::strategy::FastMarketCore;
using clock_t_ = MwrBreaker::clock;

namespace {

int g_failures = 0;
int g_checks = 0;

#define CHECK(cond, msg)                                                       \
    do {                                                                       \
        ++g_checks;                                                            \
        if (!(cond)) {                                                         \
            ++g_failures;                                                      \
            std::printf("  [FAIL] %s  (%s:%d)\n", (msg), __FILE__, __LINE__);  \
        }                                                                      \
    } while (0)

// Deterministic synthetic clock: 1 hour past epoch + N seconds, so time_points
// are comfortably positive (default-constructed paused_until_ == epoch == "never").
clock_t_::time_point at(double seconds) {
    return clock_t_::time_point{} + std::chrono::hours(1) +
           std::chrono::milliseconds(static_cast<long long>(seconds * 1000.0));
}

// -----------------------------------------------------------------------------
// 1. mwr_disabled_is_noop: window<=0 (disabled) → push never grows the deque.
// -----------------------------------------------------------------------------
void test_disabled_is_noop() {
    std::printf("test_disabled_is_noop\n");
    MwrBreaker b;
    for (int i = 0; i < 50; ++i) {
        b.push(at(i), 100.0 + i, /*window_sec=*/0);  // disabled
    }
    CHECK(b.sampleCount() == 0, "disabled push must not grow the window");
    CHECK(b.empty(), "disabled breaker stays empty");
    CHECK(!b.isPaused(), "disabled breaker is never paused");
}

// Mirrors the production 1 Hz sampler gate (mmDrainPendingQuoteCycle) so the
// throttle behaviour is tested exactly as wired.
struct ThrottledSampler {
    bool have_last{false};
    clock_t_::time_point last_ts{};
    int window_sec{0};
    void feed(MwrBreaker& b, clock_t_::time_point now, double mid) {
        if (window_sec <= 0) return;  // disabled → zero-cost no-op
        if (!have_last || (now - last_ts) >= std::chrono::milliseconds(1000)) {
            have_last = true;
            last_ts = now;
            b.push(now, mid, window_sec);
        }
    }
};

// -----------------------------------------------------------------------------
// 2. mwr_breach_triggers_pull_and_pause: range/tick > size → kBreachPause once,
//    then kPausedSkip on subsequent cycles within the pause window.
// -----------------------------------------------------------------------------
void test_breach_triggers_pull_and_pause() {
    std::printf("test_breach_triggers_pull_and_pause\n");
    MwrBreaker b;
    const int size = 10, window = 5, pull = 0;  // pull=0 → falls back to window
    const double tick = 1.0;

    b.push(at(0), 100.0, window);
    b.push(at(1), 120.0, window);  // range = 20 ticks > 10

    auto r1 = b.evaluate(at(1), tick, size, window, pull);
    CHECK(r1.action == MwrBreaker::Action::kBreachPause, "first breach → kBreachPause");
    CHECK(r1.just_paused, "breach is an unpaused→paused edge (one warn log)");
    CHECK(r1.mwr_ticks > 10.0, "mwr_ticks exceeds size threshold");
    CHECK(b.isPaused(), "breaker is paused after breach");

    // Next cycle, still inside the pause window → must skip (this is the suppression
    // that maps to early-return; no second cancel, no re-log).
    auto r2 = b.evaluate(at(2), tick, size, window, pull);
    CHECK(r2.action == MwrBreaker::Action::kPausedSkip, "still-paused cycle → kPausedSkip");
    CHECK(!r2.just_paused, "no repeated pause edge while already paused");
    CHECK(!r2.just_resumed, "not resumed yet");
}

// -----------------------------------------------------------------------------
// 3. mwr_auto_resume_after_window: advance past paused_until → cycle proceeds,
//    just_resumed edge fires exactly once.
// -----------------------------------------------------------------------------
void test_auto_resume_after_window() {
    std::printf("test_auto_resume_after_window\n");
    MwrBreaker b;
    const int size = 10, window = 5, pull = 0;
    const double tick = 1.0;

    b.push(at(0), 100.0, window);
    b.push(at(1), 120.0, window);
    auto r1 = b.evaluate(at(1), tick, size, window, pull);
    CHECK(r1.action == MwrBreaker::Action::kBreachPause, "breach");
    // pause = max(pull||window, window) = 5s → expires at at(1)+5 = at(6).

    // Just before expiry: still paused.
    auto rmid = b.evaluate(at(5), tick, size, window, pull);
    CHECK(rmid.action == MwrBreaker::Action::kPausedSkip, "before expiry still paused");

    // Let the range fall back inside threshold by the time we resume: only recent,
    // calm samples remain in the window. (Production: reconcile-against-venue runs at
    // drain start BEFORE this evaluate, so re-quote is venue-truth-safe.)
    b.push(at(7), 130.0, window);
    b.push(at(8), 130.0, window);  // window now [at(7),at(8)] → range 0

    auto r2 = b.evaluate(at(8), tick, size, window, pull);
    CHECK(r2.action != MwrBreaker::Action::kPausedSkip, "past expiry → not paused");
    CHECK(r2.just_resumed, "paused→unpaused edge fires once (one info log)");
    CHECK(!b.isPaused(), "resumed");

    auto r3 = b.evaluate(at(9), tick, size, window, pull);
    CHECK(!r3.just_resumed, "resume edge does not repeat");
}

// -----------------------------------------------------------------------------
// 4. mwr_sampling_is_1hz: many sub-second updates → deque grows ~1/sec, not 1/tick.
// -----------------------------------------------------------------------------
void test_sampling_is_1hz() {
    std::printf("test_sampling_is_1hz\n");
    MwrBreaker b;
    ThrottledSampler s;
    s.window_sec = 600;  // big window so nothing evicts during the test

    // 1000 updates spaced 10 ms across 10 seconds.
    for (int i = 0; i < 1000; ++i) {
        s.feed(b, at(i * 0.010), 100.0 + 0.001 * i);
    }
    // ~10s of 1 Hz sampling → ~10 or 11 samples, definitely not ~1000.
    CHECK(b.sampleCount() >= 9 && b.sampleCount() <= 12,
          "≈1 sample/sec over 10s (not 1/tick)");
}

// -----------------------------------------------------------------------------
// 5. mwr_window_eviction: samples older than window_sec are dropped; range
//    reflects only the surviving window.
// -----------------------------------------------------------------------------
void test_window_eviction() {
    std::printf("test_window_eviction\n");
    MwrBreaker b;
    const int window = 5;
    b.push(at(0), 100.0, window);
    b.push(at(1), 101.0, window);
    b.push(at(2), 102.0, window);
    b.push(at(6), 200.0, window);  // cutoff = at(1); at(0) (strictly older) evicted

    CHECK(b.sampleCount() == 3, "sample at t=0 evicted, three remain");
    // Surviving mids: 101,102,200 → range 99 (the t=0 value 100 is gone).
    CHECK(b.windowRange() == 99.0, "range reflects only the surviving window");
}

// -----------------------------------------------------------------------------
// 6. mwr_state_is_mover_only: the owner-thread guard identifies the binding
//    ("mover") thread and flags any other thread — without cross-thread MWR data.
// -----------------------------------------------------------------------------
void test_state_is_mover_only() {
    std::printf("test_state_is_mover_only\n");
    MwrBreaker b;
    // Bind owner by touching on THIS (the "mover") thread.
    b.push(at(0), 100.0, /*window=*/10);
    CHECK(b.ownerBound(), "owner bound on first touch");
    CHECK(b.calledFromOwnerThread(), "owner thread recognized as owner");

    // A different thread must NOT be recognized as the owner. We only READ the
    // (publish-before-spawn) owner id here — no MWR mutation off-thread, no atomic.
    bool seen_owner_on_other_thread = true;
    std::thread other([&]() { seen_owner_on_other_thread = b.calledFromOwnerThread(); });
    other.join();
    CHECK(!seen_owner_on_other_thread, "foreign thread is detected as non-owner");
    // Back on the mover thread, still the owner.
    CHECK(b.calledFromOwnerThread(), "still owner on the mover thread");
}

// -----------------------------------------------------------------------------
// 7. mwr_invalid_tick_skips: quote_tick<=0 / NaN / inf → kInvalidTick, no divide.
// -----------------------------------------------------------------------------
void test_invalid_tick_skips() {
    std::printf("test_invalid_tick_skips\n");
    MwrBreaker b;
    const int size = 10, window = 5, pull = 0;
    b.push(at(0), 100.0, window);
    b.push(at(1), 999.0, window);  // huge range — would breach IF tick were valid

    const double nan_v = std::numeric_limits<double>::quiet_NaN();
    const double inf_v = std::numeric_limits<double>::infinity();

    auto r0 = b.evaluate(at(1), 0.0, size, window, pull);
    CHECK(r0.action == MwrBreaker::Action::kInvalidTick, "tick=0 → kInvalidTick");
    auto rneg = b.evaluate(at(1), -1.0, size, window, pull);
    CHECK(rneg.action == MwrBreaker::Action::kInvalidTick, "tick<0 → kInvalidTick");
    auto rnan = b.evaluate(at(1), nan_v, size, window, pull);
    CHECK(rnan.action == MwrBreaker::Action::kInvalidTick, "tick=NaN → kInvalidTick");
    auto rinf = b.evaluate(at(1), inf_v, size, window, pull);
    CHECK(rinf.action == MwrBreaker::Action::kInvalidTick, "tick=inf → kInvalidTick");
    CHECK(!b.isPaused(), "invalid tick never pauses");
}

// Bonus: <2 samples → kInsufficient (the "need ≥2 spanning the window" guard).
void test_insufficient_samples() {
    std::printf("test_insufficient_samples\n");
    MwrBreaker b;
    b.push(at(0), 100.0, 5);
    auto r = b.evaluate(at(0), 1.0, 10, 5, 0);
    CHECK(r.action == MwrBreaker::Action::kInsufficient, "single sample → kInsufficient");
}

// -----------------------------------------------------------------------------
// Placement-gate model (Item G fix).
//
// The production fix adds a pure mover-only predicate:
//
//     bool MakeMarketStrategy::mmMwrIsPausedNow() const {
//         return std::chrono::steady_clock::now() < mm_mwr_.pausedUntil();
//     }
//
// and early-returns on it at the top of the two placement paths that bypass the
// runFullMmQuoteCycle MWR gate (tryDeskFillRequoteF4PlaceFreshPair,
// deskTheoMoveAfterCancelAckPlaceOneLeg). Cancels / reconcile are NOT gated.
//
// FakeStrategy mirrors that wiring exactly, with the clock injected so the suite
// is deterministic — the gate expression below is byte-for-byte the predicate body
// with steady_clock::now() replaced by the test's synthetic `now`.
// -----------------------------------------------------------------------------
struct FakeStrategy {
    MwrBreaker mwr;
    int placements{0};
    int cancels{0};
    int reconciles{0};
    int predicate_calls{0};

    // == mmMwrIsPausedNow() — pure deadline read (now injected for determinism). ==
    bool mwrIsPausedNow(clock_t_::time_point now) {
        ++predicate_calls;
        return now < mwr.pausedUntil();
    }

    // tryDeskFillRequoteF4PlaceFreshPair top-gate: suppress the fresh pair while paused.
    void fillRequote(clock_t_::time_point now) {
        if (mwrIsPausedNow(now)) return;
        placements += 2;  // fresh skewed pair (bid + ask)
    }
    // deskTheoMoveAfterCancelAckPlaceOneLeg top-gate: suppress the one-leg re-peg while paused.
    void afterCancelAckRepeg(clock_t_::time_point now) {
        if (mwrIsPausedNow(now)) return;
        placements += 1;  // single survivor leg
    }
    // Cancels / reconcile are intentionally NOT gated by the pause.
    void cancelLeg() { ++cancels; }
    void reconcile() { ++reconciles; }

    // Drive the breaker into a breach → paused until at(6) (size=10, window=5, pull=0).
    void breachAndPause() {
        mwr.push(at(0), 100.0, 5);
        mwr.push(at(1), 130.0, 5);  // 30 ticks > size 10
        const auto r = mwr.evaluate(at(1), 1.0, 10, 5, 0);
        if (r.action != MwrBreaker::Action::kBreachPause) {
            std::printf("  [FAIL] breachAndPause precondition: expected kBreachPause\n");
            ++g_failures;
        }
    }
};

// 8. fill_requote_during_pause_places_nothing
void test_fill_requote_during_pause_places_nothing() {
    std::printf("test_fill_requote_during_pause_places_nothing\n");
    FakeStrategy s;
    s.breachAndPause();
    s.cancelLeg();         // the cancel that pulled the legs still runs
    s.fillRequote(at(2));  // inside pause (at(2) < at(6))
    CHECK(s.placements == 0, "fill-requote places nothing while MWR-paused");
    CHECK(s.cancels == 1, "cancel is still allowed while paused");
}

// 9. after_cancel_ack_repeg_during_pause_places_nothing
void test_after_cancel_ack_repeg_during_pause_places_nothing() {
    std::printf("test_after_cancel_ack_repeg_during_pause_places_nothing\n");
    FakeStrategy s;
    s.breachAndPause();
    s.afterCancelAckRepeg(at(3));  // inside pause
    CHECK(s.placements == 0, "after-cancel-ack re-peg places nothing while MWR-paused");
}

// 10. resume_after_deadline_allows_placement
void test_resume_after_deadline_allows_placement() {
    std::printf("test_resume_after_deadline_allows_placement\n");
    FakeStrategy s;
    s.breachAndPause();             // paused until at(6)
    s.fillRequote(at(5));           // still paused → nothing
    s.afterCancelAckRepeg(at(5));   // still paused → nothing
    CHECK(s.placements == 0, "no placement before the deadline");
    s.fillRequote(at(7));           // past deadline → +2
    s.afterCancelAckRepeg(at(7));   // past deadline → +1
    CHECK(s.placements == 3, "both paths place again once now >= paused_until");
}

// 11. cancel_still_works_while_paused
void test_cancel_still_works_while_paused() {
    std::printf("test_cancel_still_works_while_paused\n");
    FakeStrategy s;
    s.breachAndPause();
    CHECK(s.mwrIsPausedNow(at(2)), "precondition: paused at at(2)");
    s.cancelLeg();
    s.cancelLeg();
    s.reconcile();
    CHECK(s.cancels == 2, "cancels are not blocked by the pause");
    CHECK(s.reconciles == 1, "reconcile is not blocked by the pause");
    CHECK(s.placements == 0, "and still no placement leaked through");
}

// 12. predicate_is_cheap: mmMwrIsPausedNow() is a pure deadline read — it must not
//     evaluate(), push(), or otherwise mutate breaker state (and by construction in
//     the real code calls no mmEffMwr*/config getter).
void test_predicate_is_cheap() {
    std::printf("test_predicate_is_cheap\n");
    FakeStrategy s;
    s.breachAndPause();
    const auto before_samples = s.mwr.sampleCount();
    const bool before_paused = s.mwr.isPaused();
    const auto before_until = s.mwr.pausedUntil();
    for (int i = 0; i < 1000; ++i) {
        (void)s.mwrIsPausedNow(at(2));
    }
    CHECK(s.mwr.sampleCount() == before_samples, "predicate does not push samples");
    CHECK(s.mwr.isPaused() == before_paused, "predicate does not change pause state");
    CHECK(s.mwr.pausedUntil() == before_until, "predicate does not move the deadline");
    CHECK(s.predicate_calls == 1000, "predicate ran exactly N times (no hidden recursion)");
}

// =============================================================================
// GLOBAL fast-market breaker (HL SPX) — FastMarketCore.
//
// These exercise the REAL production decision core (FastMarketCore::evaluateTick,
// reusing MwrBreaker) and the REAL single shared atomic deadline
// (FastMarketCore::pausedUntilMs()). Only the engine wiring (Config/EFM reads,
// getAllStrategies fan-out) lives in FastMarketMonitor.cpp and is modelled here
// with a counter — the trip/suppression DECISIONS below are production code.
// =============================================================================

// steady_clock ms for the synthetic `at()` instants (deterministic deadline checks).
long long toMsAt(double seconds) {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               at(seconds).time_since_epoch())
        .count();
}
// The global deadline is a process-wide static atomic; reset between cases.
void resetGlobalDeadline() {
    FastMarketCore::pausedUntilMs().store(0, std::memory_order_relaxed);
}

// Item 6: use the SAME production helper onMoverTick calls, so a regression in the
// disable decision (dropping the `enabled` term) fails this test AND breaks production.
bool onMoverConfigDisabled(const std::string& hl_spx_symbol, bool enabled) {
    return FastMarketCore::configDisabledFromConfig(hl_spx_symbol.empty(), enabled);
}

using FA = FastMarketCore::TickResult::Action;

// 1. fast_mkt_breach_pulls_all_markets: HL breach sets the global deadline and (modelled)
//    fans a cancel job to every live AX.
void test_fast_mkt_breach_pulls_all_markets() {
    std::printf("test_fast_mkt_breach_pulls_all_markets\n");
    resetGlobalDeadline();
    FastMarketCore core;
    const int size = 10, window = 5, pull = 10;
    const double tick = 1.0;

    auto t1 = core.evaluateTick(at(0), false, false, true, 100.0, tick, size, window, pull);
    CHECK(t1.action == FA::kInsufficient, "single HL sample → insufficient (no trip yet)");
    auto t2 = core.evaluateTick(at(1), false, false, true, 120.0, tick, size, window, pull);
    CHECK(t2.action == FA::kBreachPause, "20-tick HL move > size → breach");
    CHECK(t2.just_paused, "breach is the unpaused→paused edge (one [FAST_MKT_BREAKER] log)");

    // Global deadline is set (pull=10 > window=5 → paused 10s, expires at(11)).
    CHECK(FastMarketCore::isGloballyPausedAt(toMsAt(2)), "global deadline set after breach");

    // Fan-out model: mmFastMarketPullAllTrackedLegs enqueues a cancel per live AX.
    std::vector<std::string> live_axs = {"EURUSD", "USDJPY", "XAGUSD"};
    int cancel_jobs = 0;
    if (t2.action == FA::kBreachPause && t2.just_paused) {
        for (const auto& ax : live_axs) {
            (void)ax;
            ++cancel_jobs;  // enqueueForAx(ax, cancel BUY+SELL)
        }
    }
    CHECK(cancel_jobs == static_cast<int>(live_axs.size()),
          "a cancel job is enqueued for every live AX");
}

// 2. fast_mkt_suppresses_all_placement: while globally paused, cycle + fill-requote +
//    after-cancel-ack all place nothing (each gated on isGloballyPausedNow()).
void test_fast_mkt_suppresses_all_placement() {
    std::printf("test_fast_mkt_suppresses_all_placement\n");
    resetGlobalDeadline();
    FastMarketCore core;
    core.evaluateTick(at(0), false, false, true, 100.0, 1.0, 10, 5, 10);
    auto br = core.evaluateTick(at(1), false, false, true, 120.0, 1.0, 10, 5, 10);
    CHECK(br.action == FA::kBreachPause, "precondition: global breach");

    int placements = 0;
    auto gated_place = [&](double when_sec, int n) {
        // == the production gate: `if (FastMarketMonitor::isGloballyPausedNow()) return;` ==
        if (!FastMarketCore::isGloballyPausedAt(toMsAt(when_sec))) {
            placements += n;
        }
    };
    gated_place(2, 2);  // runFullMmQuoteCycle fresh pair
    gated_place(2, 2);  // tryDeskFillRequoteF4PlaceFreshPair
    gated_place(2, 1);  // deskTheoMoveAfterCancelAckPlaceOneLeg
    CHECK(placements == 0, "all three placement paths suppressed while globally paused");
}

// 3. fast_mkt_auto_resume: past the deadline, placement resumes (no timer, no clear).
void test_fast_mkt_auto_resume() {
    std::printf("test_fast_mkt_auto_resume\n");
    resetGlobalDeadline();
    FastMarketCore core;
    core.evaluateTick(at(0), false, false, true, 100.0, 1.0, 10, 5, 10);
    core.evaluateTick(at(1), false, false, true, 120.0, 1.0, 10, 5, 10);  // paused until at(11)
    CHECK(FastMarketCore::isGloballyPausedAt(toMsAt(5)), "still paused before deadline");
    CHECK(!FastMarketCore::isGloballyPausedAt(toMsAt(12)), "auto-resumed past the deadline");

    int placements = 0;
    if (!FastMarketCore::isGloballyPausedAt(toMsAt(12))) {
        placements += 2;  // cycle quotes again
    }
    CHECK(placements == 2, "placement resumes automatically once now >= deadline");
}

// 4. fast_mkt_stale_hl_feed_fail_open: HL feed stale → no trip even on a huge move; window cleared.
void test_fast_mkt_stale_hl_feed_fail_open() {
    std::printf("test_fast_mkt_stale_hl_feed_fail_open\n");
    resetGlobalDeadline();
    FastMarketCore core;
    core.evaluateTick(at(0), false, /*feed_stale=*/false, true, 100.0, 1.0, 10, 5, 10);
    auto fo = core.evaluateTick(at(1), false, /*feed_stale=*/true, true, 999.0, 1.0, 10, 5, 10);
    CHECK(fo.action == FA::kFailOpen, "stale HL feed → FAIL-OPEN (does not trip)");
    CHECK(!FastMarketCore::isGloballyPausedAt(toMsAt(2)), "no global pause while feed is stale");
    CHECK(core.sampleCount() == 0, "fail-open clears the window so recovery cannot straddle");
}

// 5. fast_mkt_no_trip_insufficient_samples: <2 fresh samples → no trip.
void test_fast_mkt_no_trip_insufficient_samples() {
    std::printf("test_fast_mkt_no_trip_insufficient_samples\n");
    resetGlobalDeadline();
    FastMarketCore core;
    auto r = core.evaluateTick(at(0), false, false, true, 100.0, 1.0, 10, 5, 10);
    CHECK(r.action == FA::kInsufficient, "single sample → insufficient");
    CHECK(!FastMarketCore::isGloballyPausedAt(toMsAt(1)), "no trip on insufficient samples");
}

// 6. fast_mkt_gap_does_not_manufacture_range (finding F): a SAMPLE gap (ticks continue, but
//    mids are invalid for a stretch — so NOT self-stale) must drop the pre-gap sample so a
//    2-sample straddle can't fabricate an oversized range.
void test_fast_mkt_gap_does_not_manufacture_range() {
    std::printf("test_fast_mkt_gap_does_not_manufacture_range\n");
    resetGlobalDeadline();
    FastMarketCore core;
    const int size = 10, window = 30, pull = 10;
    const double tick = 1.0;
    core.evaluateTick(at(0), false, false, /*valid=*/true, 100.0, tick, size, window, pull);
    // Ticks keep coming at 1 Hz (no self-staleness), but the HL mid is invalid → samples drop.
    core.evaluateTick(at(1), false, false, /*valid=*/false, 0.0, tick, size, window, pull);
    core.evaluateTick(at(2), false, false, /*valid=*/false, 0.0, tick, size, window, pull);
    core.evaluateTick(at(3), false, false, /*valid=*/false, 0.0, tick, size, window, pull);
    // Mid returns 30 ticks away after a 4s SAMPLE gap (> 2x interval) → clear-on-gap drops the
    // pre-gap 100, leaving a single fresh sample.
    auto post = core.evaluateTick(at(4), false, false, /*valid=*/true, 130.0, tick, size, window, pull);
    CHECK(post.action == FA::kInsufficient, "clear-on-gap drops the pre-gap sample → insufficient");
    CHECK(!FastMarketCore::isGloballyPausedAt(toMsAt(5)),
          "a sampling gap did NOT manufacture a spurious oversized-range breach");
}

// 7. fast_mkt_monitor_self_staleness: if the monitor itself isn't ticked for > kSelfStaleMs,
//    it must NOT evaluate the stale window as a valid range.
void test_fast_mkt_monitor_self_staleness() {
    std::printf("test_fast_mkt_monitor_self_staleness\n");
    resetGlobalDeadline();
    FastMarketCore core;
    core.evaluateTick(at(0), false, false, true, 100.0, 1.0, 10, 30, 10);
    // Next tick is 4s later (> kSelfStaleMs=3s): the mover backed up / loop stalled.
    auto stale = core.evaluateTick(at(4), false, false, true, 999.0, 1.0, 10, 30, 10);
    CHECK(stale.self_stale, "monitor detects its own >3s tick gap");
    CHECK(stale.action == FA::kFailOpen, "self-stale tick does not evaluate a stale window");
    CHECK(!FastMarketCore::isGloballyPausedAt(toMsAt(5)), "no trip on a self-stale window");
    CHECK(core.sampleCount() == 0, "self-staleness clears the window (treated like feed staleness)");
}

// 8. fast_mkt_missing_tick_disables: tick_size<=0 (or symbol unset) → disabled, no divide, no trip.
void test_fast_mkt_missing_tick_disables() {
    std::printf("test_fast_mkt_missing_tick_disables\n");
    resetGlobalDeadline();
    FastMarketCore core;
    core.evaluateTick(at(0), false, false, true, 100.0, /*tick=*/0.0, 10, 5, 10);
    auto dis = core.evaluateTick(at(1), false, false, true, 999.0, /*tick=*/0.0, 10, 5, 10);
    CHECK(dis.action == FA::kDisabled, "tick_size<=0 → disabled (never divides)");
    CHECK(!FastMarketCore::isGloballyPausedAt(toMsAt(2)), "disabled breaker never trips");

    // symbol unset (config_disabled) is also a hard disable, regardless of a valid tick/move.
    FastMarketCore core2;
    core2.evaluateTick(at(0), /*config_disabled=*/true, false, true, 100.0, 1.0, 10, 5, 10);
    auto dis2 = core2.evaluateTick(at(1), /*config_disabled=*/true, false, true, 200.0, 1.0, 10, 5, 10);
    CHECK(dis2.action == FA::kDisabled, "unset hl_spx_symbol → disabled");
}

// 8b. fast_mkt_enabled_toggle_gates (Item 6, 2026-07-27): the desk on/off switch maps to
//     config_disabled exactly as onMoverTick computes it — enabled=false disables the breaker
//     even with a valid symbol + big move (symbol preserved), enabled=true (the getBool default)
//     keeps it watching. Modelled the same way the suite models the rest of the Config wiring.
void test_fast_mkt_enabled_toggle_gates() {
    std::printf("test_fast_mkt_enabled_toggle_gates\n");

    // enabled=false with a REAL symbol -> disabled: no trip on a 20-tick move, no global pause.
    resetGlobalDeadline();
    FastMarketCore off;
    const bool cd_off = onMoverConfigDisabled("HL-SPX", /*enabled=*/false);
    CHECK(cd_off, "enabled=false -> config_disabled true even with the symbol set (not blanked)");
    off.evaluateTick(at(0), cd_off, false, true, 100.0, 1.0, 10, 5, 10);
    auto r_off = off.evaluateTick(at(1), cd_off, false, true, 120.0, 1.0, 10, 5, 10);
    CHECK(r_off.action == FA::kDisabled, "breaker OFF via enabled=false does not trip on a 20-tick move");
    CHECK(!FastMarketCore::isGloballyPausedAt(toMsAt(2)), "no global pause while enabled=false");

    // NEGATIVE CONTROL: enabled=true with the SAME symbol + move -> trips (toggle is load-bearing).
    resetGlobalDeadline();
    FastMarketCore on;
    const bool cd_on = onMoverConfigDisabled("HL-SPX", /*enabled=*/true);
    CHECK(!cd_on, "enabled=true -> config_disabled false (symbol set)");
    on.evaluateTick(at(0), cd_on, false, true, 100.0, 1.0, 10, 5, 10);
    auto r_on = on.evaluateTick(at(1), cd_on, false, true, 120.0, 1.0, 10, 5, 10);
    CHECK(r_on.action == FA::kBreachPause, "same move DOES trip when enabled=true (proves the toggle gates)");

    // Absent key defaults to ON (Config.getBool(..., /*default=*/true)).
    CHECK(!onMoverConfigDisabled("HL-SPX", /*enabled=*/true),
          "absent enabled key defaults to ON (getBool default=true)");
}

// 9. global_deadline_is_single_atomic: the only shared MWR state is the one process-wide atomic;
//    a breach on one core is visible to a different core, and each core's window stays independent.
void test_fast_mkt_global_deadline_is_single_atomic() {
    std::printf("test_fast_mkt_global_deadline_is_single_atomic\n");
    resetGlobalDeadline();
    FastMarketCore a;
    FastMarketCore b;  // a separate "instrument" core; its window is mover-only & independent
    a.evaluateTick(at(0), false, false, true, 100.0, 1.0, 10, 5, 10);
    auto br = a.evaluateTick(at(1), false, false, true, 120.0, 1.0, 10, 5, 10);
    CHECK(br.action == FA::kBreachPause, "core a breaches");

    // The deadline is GLOBAL: core b (and the strategy predicate) read the SAME atomic.
    CHECK(FastMarketCore::isGloballyPausedAt(toMsAt(2)),
          "breach on one core is visible globally via the single shared atomic");
    CHECK(b.sampleCount() == 0,
          "the second core's sample window is independent (mover-only, not shared)");
    // The shared primitive is exactly one int64 atomic.
    CHECK(sizeof(FastMarketCore::pausedUntilMs()) == sizeof(std::atomic<std::int64_t>),
          "the single shared primitive is one std::atomic<int64_t>");
}

}  // namespace

int main() {
    std::printf("=== MWR breaker tests ===\n");
    test_disabled_is_noop();
    test_breach_triggers_pull_and_pause();
    test_auto_resume_after_window();
    test_sampling_is_1hz();
    test_window_eviction();
    test_state_is_mover_only();
    test_invalid_tick_skips();
    test_insufficient_samples();
    test_fill_requote_during_pause_places_nothing();
    test_after_cancel_ack_repeg_during_pause_places_nothing();
    test_resume_after_deadline_allows_placement();
    test_cancel_still_works_while_paused();
    test_predicate_is_cheap();

    // --- Global fast-market breaker (HL SPX) ---
    test_fast_mkt_breach_pulls_all_markets();
    test_fast_mkt_suppresses_all_placement();
    test_fast_mkt_auto_resume();
    test_fast_mkt_stale_hl_feed_fail_open();
    test_fast_mkt_no_trip_insufficient_samples();
    test_fast_mkt_gap_does_not_manufacture_range();
    test_fast_mkt_monitor_self_staleness();
    test_fast_mkt_missing_tick_disables();
    test_fast_mkt_enabled_toggle_gates();
    test_fast_mkt_global_deadline_is_single_atomic();

    std::printf("=== %d checks, %d failures ===\n", g_checks, g_failures);
    return g_failures == 0 ? 0 : 1;
}
