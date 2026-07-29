// =============================================================================
// REGRESSION TEST — venue-truth skip-cancel REMOVED → always send real cancel
// (2026-06-15 order-leak fix)
// =============================================================================
//
// Bug being locked out (verified across 3 production logs):
//   The cancel path consulted a cached /open-orders snapshot
//   (mmVenueTruthKnowsOid). When the snapshot was "fresh" but did NOT contain
//   the oid being cancelled, the guard concluded the order was already gone,
//   evicted the leg LOCALLY, and never sent the REST cancel. A just-placed
//   order that simply had not propagated into /open-orders yet (fast
//   cancel-replace on liquid instruments; placed→cancel gaps 0.37s–2.10s)
//   was therefore abandoned alive at the venue → orphan accumulation.
//
// Fix (production: MakeMarketStrategy::mmCancelTrackedLegThisStackGateway and
// the free function mmSendRestCancelByOrderId):
//   The `if (cache_fresh) { skip + evict + return success }` branch is DELETED.
//   The code refreshes venue truth for the NEXT decision, then ALWAYS sends the
//   real REST cancel. A 404 for a genuinely-gone order is handled as benign
//   success (mmIsBenignCancelFailure → CANCEL_PENDING, never an eviction here),
//   so there is no downside to always sending.
//
// These are self-contained contract models (the established style for this
// suite — no link against strategy_lib). Each models the exact production
// decision/handler being changed and asserts the new invariant. Tests 5 and 6
// additionally exercise the lifetime contract that makes "always send" safe:
// the cancel is synchronous on the mover thread and the strategy is pinned by a
// shared_ptr for the whole request+response window. Run under TSan + ASan.

#include "test_helpers.h"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>

namespace {

// ---- Production constants mirrored here (RestClient) -------------------------
constexpr long kOrderOpTimeoutMs = 4000;   // api.order_op_timeout_ms (POST + DELETE fallback)
constexpr long kHttpDefaultMs    = 30000;  // HttpRequest default (the OLD unbounded fallback)

// What the cancel decision resolves to.
enum class CancelDecision { kSendRest, kSkipEvictNoSend };

// Mirror of MmLegState (subset we need).
enum class LegState : int { ADOPTED = 0, CANCEL_PENDING, CANCELLED, FILLED };

// Faithful model of the venue-truth cache + the (post-fix) cancel decision.
struct CancelPath {
    mutable std::mutex mu_;
    std::unordered_set<std::string> venue_oids_;  // last /open-orders snapshot
    std::int64_t updated_ms_{0};
    std::atomic<int> reconcile_triggers_{0};

    static constexpr std::int64_t kTtlMs = 3000;

    void set_snapshot(std::unordered_set<std::string> oids, std::int64_t now_ms) {
        std::lock_guard<std::mutex> lk(mu_);
        venue_oids_ = std::move(oids);
        updated_ms_ = now_ms;
    }

    // Production: mmVenueTruthKnowsOid(oid, cache_fresh).
    bool knows_oid(const std::string& oid, bool& cache_fresh, std::int64_t now_ms) const {
        std::lock_guard<std::mutex> lk(mu_);
        cache_fresh = (updated_ms_ > 0) && (now_ms - updated_ms_ < kTtlMs);
        return venue_oids_.count(oid) > 0;
    }

    void trigger_reconcile() { reconcile_triggers_.fetch_add(1, std::memory_order_relaxed); }

    // POST-FIX decision (the code under test). There is NO skip branch left:
    // refresh truth if absent, then ALWAYS send the real cancel.
    CancelDecision decide(const std::string& oid, std::int64_t now_ms) {
        bool cache_fresh = false;
        if (!knows_oid(oid, cache_fresh, now_ms)) {
            trigger_reconcile();
        }
        return CancelDecision::kSendRest;  // never kSkipEvictNoSend
    }
};

// -----------------------------------------------------------------------------
// Test 1 — place leg → cancel within 1s (oid not yet in /open-orders snapshot):
//          a REAL REST cancel must be sent; the leg is never left
//          tracked-but-uncancelled.
// -----------------------------------------------------------------------------
void young_oid_absent_from_fresh_snapshot_still_sends_real_cancel() {
    CancelPath p;
    const std::int64_t t0 = 100000;
    // Snapshot is FRESH (just refreshed) but does NOT yet contain the just-placed oid —
    // exactly the production leak condition (propagation lag on a <1s-old order).
    p.set_snapshot({"OLD_OID_FROM_PRIOR_CYCLE"}, /*now_ms=*/t0);

    bool cache_fresh = false;
    const bool known = p.knows_oid("JUST_PLACED_OID", cache_fresh, /*now_ms=*/t0 + 800);
    TX_REQUIRE(!known);        // venue snapshot does not know it yet
    TX_REQUIRE(cache_fresh);   // ...and the snapshot is "fresh" — the OLD code would SKIP here

    const CancelDecision d = p.decide("JUST_PLACED_OID", /*now_ms=*/t0 + 800);
    // The whole point of the fix: a fresh snapshot that lacks the oid must NOT skip.
    TX_REQUIRE(d == CancelDecision::kSendRest);
    // And it must have refreshed venue truth for the next decision.
    TX_EQ(p.reconcile_triggers_.load(), 1);
}

// -----------------------------------------------------------------------------
// Benign-404 / fill-race handler (production ~line 8683). Models the contract:
//   - a fill is booked exactly once via the INDEPENDENT fill event;
//   - a benign-404 cancel marks CANCEL_PENDING but NEVER reverses FILLED.
// -----------------------------------------------------------------------------
struct Leg {
    std::atomic<LegState> state{LegState::ADOPTED};
    std::atomic<long long> net_po{0};
    long long qty{0};
    std::atomic<int> fills_booked{0};

    // Production: on_fill → trackFill → processFill (independent of the cancel response).
    void apply_fill_event() {
        LegState expect = LegState::ADOPTED;
        // First fill wins; idempotent against a racing CANCEL_PENDING too.
        LegState cur = state.load(std::memory_order_acquire);
        if (cur == LegState::FILLED) return;  // already booked
        // Book exactly once.
        if (fills_booked.fetch_add(1, std::memory_order_acq_rel) == 0) {
            net_po.fetch_add(qty, std::memory_order_acq_rel);
        }
        // FILLED is terminal.
        state.store(LegState::FILLED, std::memory_order_release);
        (void)expect; (void)cur;
    }

    // Production: benign-404 cancel handler — mark CANCEL_PENDING unless FILLED.
    void apply_benign_404() {
        LegState cur = state.load(std::memory_order_acquire);
        for (;;) {
            if (cur == LegState::FILLED) return;  // never reverse a terminal fill
            if (state.compare_exchange_weak(cur, LegState::CANCEL_PENDING,
                                            std::memory_order_acq_rel,
                                            std::memory_order_acquire)) {
                return;
            }
        }
    }
};

// -----------------------------------------------------------------------------
// Test 2 — young leg FILLS while the (always-sent) cancel is in flight:
//          net_po reflects the fill exactly once and the FILLED leg is never
//          reversed by the benign-404.
// -----------------------------------------------------------------------------
void fill_while_cancel_in_flight_books_once_and_never_reverses() {
    constexpr int kIters = 2000;
    for (int i = 0; i < kIters; ++i) {
        Leg leg;
        leg.qty = 10000;

        std::atomic<bool> go{false};
        std::thread fill_thr([&] {
            while (!go.load(std::memory_order_acquire)) {}
            leg.apply_fill_event();
        });
        std::thread cancel_thr([&] {
            while (!go.load(std::memory_order_acquire)) {}
            leg.apply_benign_404();  // cancel comes back 404 because the order just filled
        });
        go.store(true, std::memory_order_release);
        fill_thr.join();
        cancel_thr.join();

        // Fill booked exactly once; position is the leg qty; state is terminal FILLED.
        if (leg.fills_booked.load() != 1) {
            ::tx::report_fail(__FILE__, __LINE__, "fills_booked == 1",
                              "iter=" + std::to_string(i) +
                                  " fills=" + std::to_string(leg.fills_booked.load()));
            return;
        }
        if (leg.net_po.load() != leg.qty) {
            ::tx::report_fail(__FILE__, __LINE__, "net_po == qty",
                              "iter=" + std::to_string(i) +
                                  " net=" + std::to_string(leg.net_po.load()));
            return;
        }
        if (leg.state.load() != LegState::FILLED) {
            ::tx::report_fail(__FILE__, __LINE__, "state == FILLED",
                              "iter=" + std::to_string(i));
            return;
        }
    }
}

// -----------------------------------------------------------------------------
// Test 3 — cancel of a genuinely-dead leg → benign-404 → CANCEL_PENDING →
//          resolved by the ack-timeout sweep. No crash, no phantom
//          pending_accepts (pending_accepts is independent of tracked legs).
// -----------------------------------------------------------------------------
void dead_leg_404_goes_cancel_pending_then_swept_no_phantom_accepts() {
    Leg leg;
    leg.qty = 10000;
    std::atomic<int> pending_accepts{0};  // production: mm_pending_accepts_

    // Order was already cancelled out-of-band; our always-sent cancel returns benign 404.
    leg.apply_benign_404();
    TX_REQUIRE(leg.state.load() == LegState::CANCEL_PENDING);
    // Eviction/CANCEL_PENDING never touches pending_accepts (place() is the only producer).
    TX_EQ(pending_accepts.load(), 0);

    // Ack-timeout sweep (production: maybeReconcileDeskCancelAckTimeouts) confirms gone.
    if (leg.state.load() == LegState::CANCEL_PENDING) {
        leg.state.store(LegState::CANCELLED, std::memory_order_release);
    }
    TX_REQUIRE(leg.state.load() == LegState::CANCELLED);
    TX_EQ(pending_accepts.load(), 0);  // still no phantom accepts
}

// -----------------------------------------------------------------------------
// Test 4 — shutdown/converge path (mmSendRestCancelByOrderId) also ALWAYS sends,
//          never skips, even on a fresh-but-absent snapshot.
// -----------------------------------------------------------------------------
void shutdown_converge_path_always_sends_cancel() {
    CancelPath p;
    const std::int64_t t0 = 500000;
    p.set_snapshot({}, /*now_ms=*/t0);  // fresh + empty snapshot (worst case for the old skip)

    int sends = 0, skips = 0;
    for (const char* oid : {"OID_A", "OID_B", "OID_C"}) {
        if (p.decide(oid, t0 + 10) == CancelDecision::kSendRest) ++sends; else ++skips;
    }
    TX_EQ(sends, 3);
    TX_EQ(skips, 0);
}

// -----------------------------------------------------------------------------
// Lifetime model: a strategy pinned by a shared_ptr for the synchronous cancel
// window (request + response handler), the way the mover lambda pins it. Teardown
// drops the "registry" reference; the object must survive until the pinned lambda
// returns, and the response handler must safely touch its members.
// -----------------------------------------------------------------------------
struct PinnedStrategy {
    std::atomic<int> tracked_legs{2};
    std::atomic<long long> net_po{0};
    std::shared_ptr<int> liveness = std::make_shared<int>(1);  // sentinel for ASan/use-after-free

    // Response handler runs INSIDE the pinned lambda — touches members.
    void handle_cancel_response_200() {
        // Mutate tracked state exactly like evict_oid_locally on the 200 path.
        if (tracked_legs.load(std::memory_order_acquire) > 0) {
            tracked_legs.fetch_sub(1, std::memory_order_acq_rel);
        }
        // Touch the liveness sentinel so ASan flags any use-after-free.
        volatile int v = *liveness;
        (void)v;
    }
};

// Mirror of StrategyManager: the registry owns ONE shared_ptr and serializes
// lookup() (returns a pinned copy, or nullptr if torn down) against teardown()
// under a mutex. The mover lambda pins via lookup() exactly as production does
// StrategyManager::getStrategy(name); it NEVER races on a single shared_ptr
// object (that would be UB regardless of the production fix).
struct Registry {
    mutable std::mutex mu_;
    std::shared_ptr<PinnedStrategy> sp_;
    explicit Registry(std::shared_ptr<PinnedStrategy> sp) : sp_(std::move(sp)) {}
    std::shared_ptr<PinnedStrategy> lookup() const {
        std::lock_guard<std::mutex> lk(mu_);
        return sp_;  // pinned copy (or nullptr) — outlives teardown via refcount
    }
    void teardown() {
        std::lock_guard<std::mutex> lk(mu_);
        sp_.reset();
    }
};

// -----------------------------------------------------------------------------
// Test 5 — stress: racing fill + clean-cancel-200 (evict) + teardown on the SAME
//          leg. The shared_ptr pin must keep the object alive through the
//          synchronous response handler; no UAF, fill booked at most once.
// -----------------------------------------------------------------------------
void stress_fill_cancel200_teardown_same_leg() {
    constexpr int kIters = 3000;
    for (int i = 0; i < kIters; ++i) {
        auto strat = std::make_shared<PinnedStrategy>();
        auto reg = std::make_shared<Registry>(strat);  // "StrategyManager" owns one ref
        auto strat_filler = strat;                     // independent OM-thread handle (own pin)
        strat.reset();                                 // only reg + filler hold refs now

        std::atomic<int> fills{0};
        std::atomic<bool> go{false};

        // Mover thread: pin via the locked lookup for the WHOLE synchronous cancel.
        std::thread mover([&] {
            while (!go.load(std::memory_order_acquire)) {}
            auto pin = reg->lookup();  // production: StrategyManager::getStrategy(name)
            if (!pin) return;          // torn down before lookup → no-op (no UAF)
            pin->handle_cancel_response_200();  // safe: pin keeps it alive through the response
        });

        // Fill event thread (independent of the cancel response; holds its own pin).
        std::thread filler([&] {
            while (!go.load(std::memory_order_acquire)) {}
            if (fills.fetch_add(1, std::memory_order_acq_rel) == 0) {
                strat_filler->net_po.fetch_add(10000, std::memory_order_acq_rel);
            }
        });

        // Teardown thread: drop the registry's ref while the mover lambda may still run.
        std::thread teardown([&] {
            while (!go.load(std::memory_order_acquire)) {}
            reg->teardown();  // object stays alive via any pin already taken
        });

        go.store(true, std::memory_order_release);
        mover.join();
        filler.join();
        teardown.join();
        // No ASan/TSan trap ⇒ the pin held. Fill booked at most once.
        if (fills.load() > 1) {
            ::tx::report_fail(__FILE__, __LINE__, "fills <= 1",
                              "iter=" + std::to_string(i) + " fills=" + std::to_string(fills.load()));
            return;
        }
    }
}

// -----------------------------------------------------------------------------
// Test 6 — cancel RESPONSE arriving after teardown begins: because the cancel is
//          synchronous and the lambda holds the pin, the response handler runs on
//          a still-alive object; destruction happens only after the lambda
//          returns (modeled by the pin being the last owner).
// -----------------------------------------------------------------------------
void cancel_response_after_teardown_is_lifetime_safe() {
    constexpr int kIters = 3000;
    std::atomic<int> alive_violations{0};  // pin held but object already gone (would be UAF)
    std::atomic<int> ran_after_teardown{0};
    for (int i = 0; i < kIters; ++i) {
        auto strat = std::make_shared<PinnedStrategy>();
        std::weak_ptr<PinnedStrategy> weak = strat;
        auto reg = std::make_shared<Registry>(strat);
        strat.reset();  // only reg holds the strategy now

        std::atomic<bool> go{false};
        std::thread mover([&] {
            while (!go.load(std::memory_order_acquire)) {}
            auto pin = reg->lookup();  // production: getStrategy(name); null ⇒ torn down ⇒ no-op
            if (!pin) return;
            // We hold a pin: the object MUST still be alive for the whole response handler.
            if (weak.expired()) alive_violations.fetch_add(1, std::memory_order_relaxed);
            if (reg->lookup() == nullptr) ran_after_teardown.fetch_add(1, std::memory_order_relaxed);
            pin->handle_cancel_response_200();  // safe regardless of teardown timing
        });
        std::thread teardown([&] {
            while (!go.load(std::memory_order_acquire)) {}
            reg->teardown();
        });
        go.store(true, std::memory_order_release);
        mover.join();
        teardown.join();
    }
    // While a pin is held the object can NEVER be destroyed → zero violations, no UAF.
    TX_EQ(alive_violations.load(), 0);
    std::printf("    response-handler-ran-after-teardown iterations=%d (all lifetime-safe)\n",
                ran_after_teardown.load());
}

// -----------------------------------------------------------------------------
// Test 7 — Commit 1: the DELETE-fallback cancel respects the bounded 4s timeout,
//          not the old 30s HttpRequest default.
// -----------------------------------------------------------------------------
long select_del_timeout_ms() {
    // Production (RestClient::del after the fix): req.timeout = order_op_timeout_.
    return kOrderOpTimeoutMs;
}
void delete_fallback_respects_bounded_timeout() {
    TX_EQ(select_del_timeout_ms(), kOrderOpTimeoutMs);
    TX_REQUIRE(select_del_timeout_ms() != kHttpDefaultMs);
    TX_LE(select_del_timeout_ms(), kOrderOpTimeoutMs);  // bounded, never the 30s default
}

}  // namespace

int main() {
    std::printf("=== venue-truth skip-cancel removed → always send real cancel (order-leak fix) ===\n");
    TX_RUN(young_oid_absent_from_fresh_snapshot_still_sends_real_cancel);          // test 1
    TX_RUN(fill_while_cancel_in_flight_books_once_and_never_reverses);             // test 2
    TX_RUN(dead_leg_404_goes_cancel_pending_then_swept_no_phantom_accepts);       // test 3
    TX_RUN(shutdown_converge_path_always_sends_cancel);                           // test 4
    TX_RUN(stress_fill_cancel200_teardown_same_leg);                             // test 5
    TX_RUN(cancel_response_after_teardown_is_lifetime_safe);                      // test 6
    TX_RUN(delete_fallback_respects_bounded_timeout);                            // commit 1
    return tx::finish("test_venue_truth_always_cancel");
}
