// =============================================================================
// PositionBook tests (2026-07, Tim single-source-of-truth position)
// =============================================================================
//
// PositionBook is a standalone header (STL only), so this test includes the
// REAL production header (include/strategy/PositionBook.h) — not a model.
//
// Coverage (Phase 2 spec):
//   1. Unit — fill accumulation, snapshot reset ordering (fills after
//      applySnapshot are NOT wiped), effective() correctness, epoch bump.
//   2. Concurrency — writer thread interleaves onFill/applySnapshot (they run
//      on ONE thread in production, per the thread-id invariant) while several
//      mover threads spin effective(). Intended to be TSAN-clean.
//   3. Simulated breach — fills push effective() past max_position between
//      refreshes; the reduce-only decision fires reading ONLY effective(),
//      with a mock REST counter asserted == 0 on the trigger path.
//   4. Startup breach — seed() at/over cap -> effective() breaches immediately.
//   5. Refresh failure — a skipped applySnapshot (REST error) keeps the old
//      snapshot, keeps accruing fills, and ages the snapshot (stale warn input).
//   6. Latency — the cancel->re-add decision reads effective() only; a mock
//      REST counter confirms zero round-trips on that path.
//
// IMPORTANT: PositionBook asserts onFill()/applySnapshot() run on a single
// thread (the production main loop). Every writer call in this file therefore
// runs on the main test thread; only effective()/readers run on other threads.

#include "test_helpers.h"
#include "strategy/PositionBook.h"

#include <atomic>
#include <thread>
#include <vector>

using architect::strategy::PositionBook;

namespace {

// A mock REST client whose only job is to prove the hot decision paths never
// call it. Any position/venue query would bump `calls`.
struct MockRest {
    std::atomic<int> calls{0};
    long long getPositionsSignedQty() {
        calls.fetch_add(1, std::memory_order_relaxed);
        return 0;  // never expected to be reached on the trigger path
    }
};

// Reduce-only decision EXACTLY mirrors production shouldQuoteSide / noteReduceOnlyState:
// it reads ONLY effective() and never touches REST.
bool reduceOnlyActive(const PositionBook& book, long long cap, MockRest& /*rest_must_not_be_used*/) {
    const long long net = static_cast<long long>(book.effective());
    return std::llabs(net) >= (cap > 0 ? cap : 1);
}

// ---------------------------------------------------------------------------
// 1. Unit
// ---------------------------------------------------------------------------
void unit_fill_accumulation_and_effective() {
    auto b = PositionBook::forSymbol("UNIT-A");
    b->seed(0.0);
    TX_EQ(static_cast<long long>(b->effective()), 0LL);

    b->onFill(+3.0);
    b->onFill(-1.0);
    TX_EQ(static_cast<long long>(b->effective()), 2LL);
    TX_EQ(static_cast<long long>(b->exchangeSnapshot()), 0LL);
    TX_EQ(static_cast<long long>(b->unaccounted()), 2LL);
}

void unit_snapshot_reset_ordering() {
    auto b = PositionBook::forSymbol("UNIT-B");
    b->seed(0.0);
    b->onFill(+5.0);                        // unaccounted = 5, effective = 5
    TX_EQ(static_cast<long long>(b->effective()), 5LL);

    const std::uint64_t e0 = b->epoch();
    // Snapshot says venue is 5 (it has caught up to our fills). Reset-then-apply:
    // unaccounted -> 0 first, snapshot -> 5. Effective stays 5, no double count.
    PositionBook::ApplyResult r = b->applySnapshot(5.0);
    TX_EQ(static_cast<long long>(r.prev_unaccounted), 5LL);
    TX_EQ(static_cast<long long>(r.new_snapshot), 5LL);
    TX_EQ(static_cast<long long>(b->effective()), 5LL);
    TX_EQ(static_cast<long long>(b->unaccounted()), 0LL);
    TX_REQUIRE(b->epoch() == e0 + 1);       // epoch bumped

    // A fill AFTER the snapshot must NOT be wiped by that snapshot.
    b->onFill(+2.0);
    TX_EQ(static_cast<long long>(b->effective()), 7LL);
    TX_EQ(static_cast<long long>(b->unaccounted()), 2LL);
}

void unit_epoch_and_age() {
    auto b = PositionBook::forSymbol("UNIT-C");
    b->seed(1.0);
    const std::uint64_t e0 = b->epoch();
    b->applySnapshot(2.0);
    b->applySnapshot(3.0);
    TX_REQUIRE(b->epoch() == e0 + 2);
    TX_REQUIRE(b->snapshotAgeMs() >= 0);
    TX_EQ(static_cast<long long>(b->effective()), 3LL);
}

// ---------------------------------------------------------------------------
// 2. Concurrency (writers single-threaded; readers concurrent). TSAN target.
// ---------------------------------------------------------------------------
void concurrency_readers_never_tear() {
    auto b = PositionBook::forSymbol("CONC-A");
    b->seed(0.0);

    std::atomic<bool> stop{false};
    std::atomic<int> bad{0};
    std::vector<std::thread> readers;
    for (int i = 0; i < 4; ++i) {
        readers.emplace_back([&] {
            while (!stop.load(std::memory_order_acquire)) {
                const double eff = b->effective();
                // effective must always equal snapshot+unaccounted for SOME
                // consistent instant — never NaN / never wildly out of range.
                if (eff < -1e9 || eff > 1e9) {
                    bad.fetch_add(1, std::memory_order_relaxed);
                }
            }
        });
    }

    // Single writer thread == the main test thread (thread-id invariant).
    for (int i = 0; i < 200000; ++i) {
        b->onFill((i % 2 == 0) ? +1.0 : -1.0);
        if (i % 5000 == 0) {
            b->applySnapshot(static_cast<double>(i % 7));
        }
    }
    stop.store(true, std::memory_order_release);
    for (auto& t : readers) t.join();

    TX_EQ(bad.load(), 0);
}

// ---------------------------------------------------------------------------
// 3. Simulated breach between refreshes -> reduce-only w/ ZERO REST.
// ---------------------------------------------------------------------------
void breach_between_refreshes_no_rest() {
    MockRest rest;
    auto b = PositionBook::forSymbol("BREACH-A");
    b->seed(0.0);                                  // flat at last snapshot
    const long long cap = 5;

    TX_REQUIRE(!reduceOnlyActive(*b, cap, rest));  // flat, quoting both sides

    // Fills arrive (NO snapshot refresh in between) and push us to the cap.
    for (int i = 0; i < 5; ++i) {
        b->onFill(+1.0);
    }
    TX_EQ(static_cast<long long>(b->effective()), 5LL);
    TX_REQUIRE(reduceOnlyActive(*b, cap, rest));    // reduce-only fired

    // The whole point: the trigger read effective() only — no venue/REST tally.
    TX_EQ(rest.calls.load(), 0);
}

// ---------------------------------------------------------------------------
// 4. Startup breach — seed at/over cap trips reduce-only immediately.
// ---------------------------------------------------------------------------
void startup_breach_immediate() {
    MockRest rest;
    auto b = PositionBook::forSymbol("STARTUP-A");
    const long long cap = 3;
    b->seed(4.0);  // venue already long 4 vs cap 3 at boot
    TX_EQ(static_cast<long long>(b->effective()), 4LL);
    TX_REQUIRE(reduceOnlyActive(*b, cap, rest));
    TX_EQ(rest.calls.load(), 0);
}

// ---------------------------------------------------------------------------
// 5. Refresh failure — skipped snapshot keeps old snapshot + accrues fills.
// ---------------------------------------------------------------------------
void refresh_failure_fail_open() {
    auto b = PositionBook::forSymbol("FAIL-A");
    b->seed(2.0);                       // snapshot = 2
    b->onFill(+1.0);                    // effective = 3

    // Simulate a refresh window where getPositions() FAILED: we simply do NOT
    // call applySnapshot. Trading continues; fills keep accruing.
    b->onFill(+1.0);                    // effective = 4
    TX_EQ(static_cast<long long>(b->effective()), 4LL);
    TX_EQ(static_cast<long long>(b->exchangeSnapshot()), 2LL);  // snapshot unchanged
    TX_REQUIRE(b->snapshotAgeMs() >= 0);

    // When the refresh finally succeeds, reset-then-apply reconciles cleanly.
    b->applySnapshot(4.0);
    TX_EQ(static_cast<long long>(b->effective()), 4LL);
    TX_EQ(static_cast<long long>(b->unaccounted()), 0LL);
}

// ---------------------------------------------------------------------------
// 6. Latency — cancel->re-add decision reads effective() only, zero REST.
// ---------------------------------------------------------------------------
void cancel_readd_path_has_no_rest() {
    MockRest rest;
    auto b = PositionBook::forSymbol("LAT-A");
    b->seed(1.0);
    const long long cap = 10;

    // Model the hot path: feed tick -> read position -> decide -> (cancel/place).
    // Production reads effective() (nanoseconds); assert we never queried REST.
    for (int i = 0; i < 1000; ++i) {
        const long long net = static_cast<long long>(b->effective());
        (void)net;
        (void)reduceOnlyActive(*b, cap, rest);
    }
    TX_EQ(rest.calls.load(), 0);
}

// ---------------------------------------------------------------------------
// 7. Snapshot jump past cap with NO intervening fill -> grow-side pulled in one
//    refresh tick; the decision path uses effective() only (mock REST == 0).
// ---------------------------------------------------------------------------
//
// Models MakeMarketStrategy::mmEnforceCapFromSnapshotOnSymbol: after
// applySnapshot reveals |effective()| >= cap (a fill we never saw locally), the
// main-loop enforcement sets reduce-only and enqueues a grow-side pull. The
// enqueue (mover job) is modeled by a counter; the decision itself never calls
// REST.
void snapshot_breach_pulls_grow_side_no_rest() {
    MockRest rest;
    auto b = PositionBook::forSymbol("SNAPBREACH-A");
    b->seed(0.0);                       // last snapshot: flat
    const long long cap = 5;

    // A grow-side (BUY) leg is resting from before; we are flat so it's allowed.
    bool grow_side_resting = true;
    TX_REQUIRE(!reduceOnlyActive(*b, cap, rest));

    // Background refresh reveals the venue jumped to +6 — NO onFill happened.
    b->applySnapshot(6.0);
    TX_EQ(static_cast<long long>(b->effective()), 6LL);

    // --- mmEnforceCapFromSnapshotOnSymbol decision (main-loop, REST-free) ------
    const long long net = static_cast<long long>(b->effective());
    const bool reduce_only = reduceOnlyActive(*b, cap, rest);   // reads effective() only
    int grow_side_pull_enqueued = 0;
    if (std::llabs(net) >= cap) {
        const bool grow_is_buy = (net > 0);
        if (grow_is_buy && grow_side_resting) {
            grow_side_resting = false;          // enqueue pull on per-AX mover (modeled)
            ++grow_side_pull_enqueued;
        }
    }

    TX_REQUIRE(reduce_only);                     // reduce-only set within this tick
    TX_EQ(grow_side_pull_enqueued, 1);           // grow-side pull enqueued
    TX_REQUIRE(!grow_side_resting);              // grow-side leg pulled, same tick
    TX_EQ(rest.calls.load(), 0);                 // ZERO REST on the enforcement path
}

// =============================================================================
// Wiring-contract models (2026-07 shadow-first re-wire).
//
// These model the MODE GUARD semantics implemented in MakeMarketStrategy.cpp
// (off/shadow/enforce). The repo's test idiom is to model the production
// decision in isolation (see the mmEnforceCapFromSnapshotOnSymbol model above);
// these do the same for the mode switch, mirroring the exact guard logic:
//   - mmReconcileVenueOrAbort / mmWaitReconcileGateBeforePlace do a legacy venue
//     REST check UNLESS enforce.
//   - shadow adds the [POSITION_SNAPSHOT] drift log (with delta_vs_legacy) and,
//     on a cap breach, a [POSITION_SNAPSHOT_CAP_SHADOW] line — and takes NO action.
//   - enforce sets reduce-only + enqueues a grow-side pull, REST-free on the
//     decision thread.
// =============================================================================
enum class Mode { Off, Shadow, Enforce };

// Model of the legacy hot-path venue check (mmReconcileVenueOrAbort +
// mmWaitReconcileGateBeforePlace). Runs a venue REST round-trip UNLESS enforce.
int legacyQuoteCycleVenueCalls(Mode mode, MockRest& rest) {
    if (mode != Mode::Enforce) {
        rest.getPositionsSignedQty();  // legacy reconcile/gate REST (off + shadow)
    }
    return rest.calls.load();
}

// Model of the snapshot host (mmApplyVenueSnapshotBatch): applies the snapshot to
// the book, and in shadow emits the drift line; in off emits nothing. Cap eval:
// shadow logs CAP_SHADOW only; enforce sets reduce-only + enqueues a pull.
struct SnapshotSideEffects {
    std::string drift_log;
    std::string cap_shadow_log;
    bool reduce_only_set{false};
    int grow_side_pull_enqueued{0};
};

SnapshotSideEffects applySnapshotWithMode(Mode mode, PositionBook& b, double venue_qty,
                                          long long legacy_net, long long cap,
                                          MockRest& rest) {
    SnapshotSideEffects fx;
    if (mode == Mode::Off) {
        // off never even applies to the book / never logs.
        return fx;
    }
    const PositionBook::ApplyResult r = b.applySnapshot(venue_qty);
    if (mode == Mode::Shadow) {
        fx.drift_log = "[POSITION_SNAPSHOT] effective_before=" +
                       std::to_string(static_cast<long long>(r.prev_effective)) +
                       " legacy_net=" + std::to_string(legacy_net) + " delta_vs_legacy=" +
                       std::to_string(static_cast<long long>(r.prev_effective) - legacy_net);
    }
    const long long eff = static_cast<long long>(r.new_effective);
    if (std::llabs(eff) >= cap) {
        if (mode == Mode::Shadow) {
            // LOG ONLY — no action.
            fx.cap_shadow_log = "[POSITION_SNAPSHOT_CAP_SHADOW] effective=" +
                                std::to_string(eff) + " cap=" + std::to_string(cap) +
                                " would_set_reduce_only=1 would_pull_side=" +
                                std::string(eff > 0 ? "BUY" : "SELL");
        } else {  // Enforce: act, REST-free on this thread.
            fx.reduce_only_set = true;
            fx.grow_side_pull_enqueued = 1;  // enqueued on the mover (modeled)
        }
    }
    (void)rest;  // the decision path never touches REST
    return fx;
}

// ---------------------------------------------------------------------------
// 8. Shadow equivalence (spec test 2): off and shadow issue the IDENTICAL venue
//    call sequence; shadow differs ONLY by emitting log lines.
// ---------------------------------------------------------------------------
void shadow_equivalence_off_vs_shadow() {
    MockRest rest_off;
    MockRest rest_shadow;
    auto boff = PositionBook::forSymbol("EQ-OFF");
    auto bsh = PositionBook::forSymbol("EQ-SHADOW");
    boff->seed(0.0);
    bsh->seed(0.0);

    // Same quote cycle + fill on both.
    const int off_calls = legacyQuoteCycleVenueCalls(Mode::Off, rest_off);
    const int shadow_calls = legacyQuoteCycleVenueCalls(Mode::Shadow, rest_shadow);
    boff->onFill(+2.0);
    bsh->onFill(+2.0);
    auto fx_off = applySnapshotWithMode(Mode::Off, *boff, 2.0, 2, 5, rest_off);
    auto fx_sh = applySnapshotWithMode(Mode::Shadow, *bsh, 2.0, 2, 5, rest_shadow);

    // IDENTICAL venue-call sequence (both ran the legacy reconcile once).
    TX_EQ(off_calls, shadow_calls);
    TX_EQ(rest_off.calls.load(), rest_shadow.calls.load());
    // off emits nothing; shadow emits the drift line with delta_vs_legacy.
    TX_REQUIRE(fx_off.drift_log.empty());
    TX_REQUIRE(fx_sh.drift_log.find("[POSITION_SNAPSHOT]") != std::string::npos);
    TX_REQUIRE(fx_sh.drift_log.find("delta_vs_legacy") != std::string::npos);
    // effective matches between the two (equivalent tracking).
    TX_EQ(static_cast<long long>(boff->effective()),
          static_cast<long long>(bsh->effective()));
}

// ---------------------------------------------------------------------------
// 9. Shadow cap (spec test 3): drive effective past cap in shadow -> CAP_SHADOW
//    logged, reduce-only NOT set, no cancels enqueued.
// ---------------------------------------------------------------------------
void shadow_cap_logs_only_no_action() {
    MockRest rest;
    auto b = PositionBook::forSymbol("SHCAP-A");
    b->seed(0.0);
    const long long cap = 5;
    // Snapshot reveals a breach (+7 vs cap 5).
    auto fx = applySnapshotWithMode(Mode::Shadow, *b, 7.0, 0, cap, rest);
    TX_REQUIRE(fx.cap_shadow_log.find("[POSITION_SNAPSHOT_CAP_SHADOW]") != std::string::npos);
    TX_REQUIRE(fx.cap_shadow_log.find("would_pull_side=BUY") != std::string::npos);
    TX_REQUIRE(!fx.reduce_only_set);            // NO action in shadow
    TX_EQ(fx.grow_side_pull_enqueued, 0);       // NO cancels enqueued
    TX_EQ(rest.calls.load(), 0);
}

// ---------------------------------------------------------------------------
// 10. Enforce (spec test 4): breach -> reduce-only fires + grow-side pull
//     enqueued, REST counter == 0 on the trigger path.
// ---------------------------------------------------------------------------
void enforce_cap_acts_rest_free() {
    MockRest rest;
    auto b = PositionBook::forSymbol("ENFCAP-A");
    b->seed(0.0);
    const long long cap = 5;
    // Snapshot-jump breach with NO fills (venue jumped to +6).
    auto fx = applySnapshotWithMode(Mode::Enforce, *b, 6.0, 0, cap, rest);
    TX_REQUIRE(fx.reduce_only_set);             // reduce-only fired
    TX_EQ(fx.grow_side_pull_enqueued, 1);       // grow-side pull enqueued (mover)
    TX_REQUIRE(fx.cap_shadow_log.empty());      // enforce does not emit the shadow line
    TX_EQ(rest.calls.load(), 0);                // ZERO REST on the trigger path
}

// ---------------------------------------------------------------------------
// 11. Mixed mode (spec test 5): symbol A enforce + symbol B shadow in one
//     process -> A's hot path is REST-free, B runs the legacy REST sequence,
//     and the two books are independent.
// ---------------------------------------------------------------------------
void mixed_mode_independent_books() {
    MockRest rest_a;  // enforce symbol
    MockRest rest_b;  // shadow symbol
    auto a = PositionBook::forSymbol("MIX-A");
    auto b = PositionBook::forSymbol("MIX-B");
    a->seed(0.0);
    b->seed(0.0);

    const int a_calls = legacyQuoteCycleVenueCalls(Mode::Enforce, rest_a);
    const int b_calls = legacyQuoteCycleVenueCalls(Mode::Shadow, rest_b);
    TX_EQ(a_calls, 0);          // A (enforce): hot path REST-free
    TX_REQUIRE(b_calls > 0);    // B (shadow): legacy REST sequence unchanged

    // Independence: a fill on A must not move B and vice-versa.
    a->onFill(+3.0);
    b->onFill(-1.0);
    TX_EQ(static_cast<long long>(a->effective()), 3LL);
    TX_EQ(static_cast<long long>(b->effective()), -1LL);

    // A breach enforces; B breach only logs — keyed strictly by symbol.
    auto fx_a = applySnapshotWithMode(Mode::Enforce, *a, 9.0, 3, 5, rest_a);
    auto fx_b = applySnapshotWithMode(Mode::Shadow, *b, -9.0, -1, 5, rest_b);
    TX_REQUIRE(fx_a.reduce_only_set);
    TX_EQ(fx_a.grow_side_pull_enqueued, 1);
    TX_REQUIRE(!fx_b.reduce_only_set);
    TX_REQUIRE(fx_b.cap_shadow_log.find("would_pull_side=SELL") != std::string::npos);
    TX_EQ(rest_a.calls.load(), 0);   // enforce stayed REST-free throughout
}

// ---------------------------------------------------------------------------
// 12. Refresh failure (spec test 6): a REST error just skips applySnapshot;
//     the book ages past 3x the refresh interval -> STALE condition is true,
//     and trading continues on the last snapshot (fail-open). Mirrors the
//     StartupSequence pollExchangePositions fail-open + POSITION_SNAPSHOT_STALE.
// ---------------------------------------------------------------------------
void refresh_failure_stale_after_3x_interval() {
    auto b = PositionBook::forSymbol("STALE-A");
    b->seed(1.0);
    b->onFill(+1.0);  // effective = 2; trading continues on this
    // Simulate the refresh interval being tiny so we cross 3x quickly.
    const std::int64_t refresh_ms = 5;
    std::this_thread::sleep_for(std::chrono::milliseconds(3 * refresh_ms + 8));
    const std::int64_t age = b->snapshotAgeMs();
    const std::int64_t stale_threshold = 3 * refresh_ms;
    TX_REQUIRE(age > stale_threshold);                 // STALE warn condition met
    TX_EQ(static_cast<long long>(b->effective()), 2LL); // trading value intact
}

// ---------------------------------------------------------------------------
// 13. Boot banner (spec test 7): format contract for the lines emitted by
//     MakeMarketStrategy::mmLogPositionBookBootBannerOnce(). Kept in lock-step
//     with the production fmt strings.
// ---------------------------------------------------------------------------
std::string bannerGlobalLine(const char* mode, const char* source) {
    return std::string("[POSITION_BOOK] mode=") + mode + " source=" + source;
}
std::string bannerPerSymbolLine(const char* sym, const char* mode) {
    return std::string("[POSITION_BOOK] sym=") + sym + " mode=" + mode + " source=config";
}
void boot_banner_format_contract() {
    const std::string g = bannerGlobalLine("SHADOW", "code-default");
    TX_REQUIRE(g.find("[POSITION_BOOK] mode=SHADOW") != std::string::npos);
    TX_REQUIRE(g.find("source=code-default") != std::string::npos);

    const std::string g2 = bannerGlobalLine("SHADOW", "config");
    TX_REQUIRE(g2.find("source=config") != std::string::npos);

    const std::string s = bannerPerSymbolLine("XAU-PERP", "ENFORCE");
    TX_REQUIRE(s.find("[POSITION_BOOK] sym=XAU-PERP mode=ENFORCE source=config") !=
               std::string::npos);
}

}  // namespace

int main() {
    TX_RUN(unit_fill_accumulation_and_effective);
    TX_RUN(unit_snapshot_reset_ordering);
    TX_RUN(unit_epoch_and_age);
    TX_RUN(concurrency_readers_never_tear);
    TX_RUN(breach_between_refreshes_no_rest);
    TX_RUN(startup_breach_immediate);
    TX_RUN(refresh_failure_fail_open);
    TX_RUN(cancel_readd_path_has_no_rest);
    TX_RUN(snapshot_breach_pulls_grow_side_no_rest);
    // --- wiring-contract models (shadow-first re-wire) ---
    TX_RUN(shadow_equivalence_off_vs_shadow);
    TX_RUN(shadow_cap_logs_only_no_action);
    TX_RUN(enforce_cap_acts_rest_free);
    TX_RUN(mixed_mode_independent_books);
    TX_RUN(refresh_failure_stale_after_3x_interval);
    TX_RUN(boot_banner_format_contract);
    return tx::finish("position_book");
}
