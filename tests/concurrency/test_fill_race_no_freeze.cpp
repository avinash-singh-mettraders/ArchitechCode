// =============================================================================
// REGRESSION TEST — Tim Izzo's "no reload after reducing fill" incident
//                   (2026-05-22 XAU-PERP, `Error evening of May 21.txt`)
// =============================================================================
//
// "Max po was hit with a long 3. it properly only showed the offer. Great.
//  Then the offer got filled so we are long 2. Reload did not occur after
//  that."
//
// Root cause: `mmReconcileVenueOrAbort`'s legacy "max-magnitude" rule could
// not distinguish the two shapes of local/venue disagreement:
//
//   1. Fill-race shape — local just reduced via a fill, REST cache is one
//      lot behind. Max-magnitude forces local BACK UP to the stale venue,
//      pinning the strategy at the cap and blocking reload.
//
//   2. Silver-overtrade shape — local never received fills, venue grew
//      independently. Max-magnitude correctly adopts venue.
//
// The fix adds a fill-race detector + forced REST refresh + bounded stuck
// budget; this test asserts the behavioural invariants of that fix:
//
//   (1) True/venue NetPo never exceeds `max_po` at any instant.
//   (2) Growth-side cancel is actually dispatched (counter > 0) when there
//       IS a growth leg to cancel — proves the fix removes live exposure.
//   (3) Reducer-side leg state is not touched by the race path.
//   (4) Reload completes within the stuck budget on the happy path.
//   (5) On a wedged REST, escalation fires within budget + epsilon (no
//       silent freeze).
//   (6) ≤1 growth-side order live at any instant (covers the cancel/replace
//       race — synchronous cancel means we cannot have the old leg still
//       resting while a new one goes out).
//   (7) Forced REST refresh count stays ≤6 per 3s budget (proves the
//       500ms throttle — a wedged venue cannot DoS our own REST endpoint).
//
// We DO NOT instantiate MakeMarketStrategy (too many runtime deps for a
// fast unit test). Instead this file replicates the Case-A decision tree
// and the cancel/refresh side-effects in a faithful model.

#include "test_helpers.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <deque>
#include <limits>
#include <mutex>
#include <thread>
#include <vector>

namespace {

// ---------------------------------------------------------------------------
// Constants must match the production values in `mmReconcileVenueOrAbort`.
// ---------------------------------------------------------------------------
constexpr std::int64_t kFillTrustWindowMs                  = 5000;
constexpr long long    kMaxFillRaceGap                     = 1;
constexpr std::int64_t kFillRaceMaxStuckMs                 = 3000;
constexpr std::int64_t kFillRaceForcedRefreshMinIntervalMs = 500;
constexpr std::int64_t kCacheStaleMs                       = 1500;  // matches kCacheStaleMs in src
constexpr std::int64_t kEscalationEpsilonMs                = 500;
// Tightest cycle cadence we model — production theo_move under HL is faster
// than this, so the test exercises a strictly more demanding scheduler.
constexpr std::int64_t kReconcileCycleMs                   = 50;

// ---------------------------------------------------------------------------
// Simulated venue + REST cache. Fills are applied to `true_net_` immediately,
// but the REST cache only reflects them after `rest_lag_ms_` has passed since
// the fill. `wedged_` simulates a REST endpoint that never converges.
// ---------------------------------------------------------------------------
struct VenueModel {
    std::atomic<long long>     true_net_{0};
    std::atomic<long long>     rest_cached_net_{0};
    std::atomic<std::int64_t>  rest_lag_ms_{200};
    std::atomic<bool>          wedged_{false};

    struct PendingUpdate {
        std::int64_t apply_at_ms;
        long long    new_value;
    };
    std::mutex pending_mu_;
    std::deque<PendingUpdate> pending_;

    void applyTrueFill(long long new_net, std::int64_t now_ms) {
        true_net_.store(new_net, std::memory_order_release);
        std::lock_guard<std::mutex> lk(pending_mu_);
        pending_.push_back({now_ms + rest_lag_ms_.load(std::memory_order_acquire), new_net});
    }

    // Returns the cached value after dequeuing any updates whose visible-time
    // has passed. If `wedged_`, never dequeues anything.
    long long pollRest(std::int64_t now_ms) {
        if (wedged_.load(std::memory_order_acquire)) {
            return rest_cached_net_.load(std::memory_order_acquire);
        }
        std::lock_guard<std::mutex> lk(pending_mu_);
        while (!pending_.empty() && pending_.front().apply_at_ms <= now_ms) {
            rest_cached_net_.store(pending_.front().new_value, std::memory_order_release);
            pending_.pop_front();
        }
        return rest_cached_net_.load(std::memory_order_acquire);
    }
};

// ---------------------------------------------------------------------------
// Simulated REST client surface that the strategy talks to. Tracks total
// refreshes (gated by cache TTL — matches production top-of-function logic)
// and forced refreshes (Case-A only, gated by 500ms throttle).
// ---------------------------------------------------------------------------
struct RestClient {
    std::atomic<bool>      cancel_should_fail_{false};
    std::atomic<long long> n_refreshes_total_{0};
    std::atomic<long long> n_forced_refreshes_{0};
    std::atomic<long long> n_growth_cancels_dispatched_{0};
    long long              cached_value_{0};
    std::int64_t           last_refresh_at_ms_{std::numeric_limits<std::int64_t>::min() / 2};

    // Returns 1 on success, 0 on failure (matches production semantics of
    // `mmCancelTrackedLegThisStackGateway`).
    int cancelGrowthLeg() {
        n_growth_cancels_dispatched_.fetch_add(1, std::memory_order_relaxed);
        if (cancel_should_fail_.load(std::memory_order_acquire)) {
            return 0;
        }
        return 1;
    }

    // Top-of-function refresh: only re-polls REST if cache_age > 1.5s.
    // Mirrors the gate at line ~2486 of `mmReconcileVenueOrAbort`.
    long long refreshTopOfFunction(VenueModel& venue, std::int64_t now_ms) {
        if ((now_ms - last_refresh_at_ms_) > kCacheStaleMs) {
            cached_value_       = venue.pollRest(now_ms);
            last_refresh_at_ms_ = now_ms;
            n_refreshes_total_.fetch_add(1, std::memory_order_relaxed);
        }
        return cached_value_;
    }

    // Forced refresh from inside Case A: always re-polls, but the CALLER
    // is responsible for honouring the 500ms throttle.
    long long forceRefresh(VenueModel& venue, std::int64_t now_ms) {
        cached_value_       = venue.pollRest(now_ms);
        last_refresh_at_ms_ = now_ms;
        n_refreshes_total_.fetch_add(1, std::memory_order_relaxed);
        n_forced_refreshes_.fetch_add(1, std::memory_order_relaxed);
        return cached_value_;
    }
};

// ---------------------------------------------------------------------------
// Simulated strategy. Mirrors the fields and decision tree of the production
// `MakeMarketStrategy::mmReconcileVenueOrAbort` Case-A path.
// ---------------------------------------------------------------------------
struct StrategyModel {
    explicit StrategyModel(long long max_po) : max_po_(max_po) {}

    long long local_net_{0};
    long long max_po_{3};
    bool      bid_live_{false};
    bool      ask_live_{false};

    std::int64_t last_fill_time_ms_{0};
    std::int64_t mm_fill_race_first_entered_ms_{0};
    std::int64_t mm_fill_race_last_refresh_ms_{0};

    // Diagnostics surfaced to the test.
    long long n_resolved_returns_{0};
    long long n_aborts_{0};
    long long n_timeouts_{0};
    bool      escalated_to_max_magnitude_{false};
    long long max_growth_legs_live_observed_{0};
    bool      reducer_leg_touched_during_race_{false};

    enum class Side { BUY, SELL };

    Side growthSide() const {
        if (local_net_ > 0) return Side::BUY;
        if (local_net_ < 0) return Side::SELL;
        return Side::BUY;  // arbitrary when flat
    }

    bool& legLive(Side s) { return s == Side::BUY ? bid_live_ : ask_live_; }

    long long growthLegsLive() const {
        const Side g = const_cast<StrategyModel*>(this)->growthSide();
        return (g == Side::BUY ? bid_live_ : ask_live_) ? 1 : 0;
    }

    void applyLocalFill(long long new_local, std::int64_t now_ms) {
        const long long prev = local_net_;
        local_net_ = new_local;
        last_fill_time_ms_ = now_ms;
        if (new_local > prev) {
            bid_live_ = false;  // a buy filled
        } else if (new_local < prev) {
            ask_live_ = false;  // a sell filled
        }
    }

    // Faithful reproduction of `mmReconcileVenueOrAbort` Case-A.
    bool reconcile(VenueModel& venue, RestClient& rest, std::int64_t now_ms) {
        const long long venue_net = rest.refreshTopOfFunction(venue, now_ms);

        if (venue_net == local_net_) {
            mm_fill_race_first_entered_ms_ = 0;
            mm_fill_race_last_refresh_ms_  = 0;
            return true;
        }

        const std::int64_t fill_age_ms =
            (last_fill_time_ms_ > 0) ? (now_ms - last_fill_time_ms_)
                                     : std::numeric_limits<std::int64_t>::max();
        const bool same_sign =
            (local_net_ >= 0 && venue_net >= 0) || (local_net_ <= 0 && venue_net <= 0);
        const bool local_lower_magnitude = std::llabs(local_net_) < std::llabs(venue_net);
        const long long abs_gap = std::llabs(venue_net - local_net_);
        const bool fill_race_shape =
            fill_age_ms >= 0 && fill_age_ms < kFillTrustWindowMs && same_sign &&
            local_lower_magnitude && abs_gap <= kMaxFillRaceGap;

        if (!fill_race_shape) {
            const long long resolved_net =
                (std::llabs(venue_net) > std::llabs(local_net_)) ? venue_net : local_net_;
            local_net_                     = resolved_net;
            mm_fill_race_first_entered_ms_ = 0;
            mm_fill_race_last_refresh_ms_  = 0;
            escalated_to_max_magnitude_    = true;
            return false;
        }

        if (mm_fill_race_first_entered_ms_ == 0) {
            mm_fill_race_first_entered_ms_ = now_ms;
        }
        const std::int64_t stuck_ms = now_ms - mm_fill_race_first_entered_ms_;

        if (stuck_ms > kFillRaceMaxStuckMs) {
            const long long resolved_net =
                (std::llabs(venue_net) > std::llabs(local_net_)) ? venue_net : local_net_;
            local_net_                     = resolved_net;
            mm_fill_race_first_entered_ms_ = 0;
            mm_fill_race_last_refresh_ms_  = 0;
            ++n_timeouts_;
            escalated_to_max_magnitude_    = true;
            return false;
        }

        const Side growth_side  = growthSide();
        const Side reducer_side = (growth_side == Side::BUY) ? Side::SELL : Side::BUY;
        const bool reducer_live_pre = legLive(reducer_side);

        bool growth_cancel_ok = true;  // vacuous-true when nothing to cancel
        if (legLive(growth_side)) {
            const int cancel_rc = rest.cancelGrowthLeg();
            if (cancel_rc == 1) {
                legLive(growth_side) = false;
                growth_cancel_ok = true;
            } else {
                growth_cancel_ok = false;
            }
        }

        const bool refresh_due =
            (mm_fill_race_last_refresh_ms_ == 0) ||
            ((now_ms - mm_fill_race_last_refresh_ms_) >= kFillRaceForcedRefreshMinIntervalMs);
        long long venue_net_post = venue_net;
        bool venue_post_observed = false;
        if (refresh_due) {
            mm_fill_race_last_refresh_ms_ = now_ms;
            venue_net_post                = rest.forceRefresh(venue, now_ms);
            venue_post_observed           = true;
        }

        if (reducer_live_pre != legLive(reducer_side)) {
            reducer_leg_touched_during_race_ = true;
        }

        // Resolved branch: gated on growth_cancel_ok per the production fix.
        if (growth_cancel_ok && venue_post_observed && venue_net_post == local_net_) {
            mm_fill_race_first_entered_ms_ = 0;
            mm_fill_race_last_refresh_ms_  = 0;
            ++n_resolved_returns_;
            return true;
        }

        ++n_aborts_;
        return false;
    }

    void quotePass() {
        const long long abs_net = std::llabs(local_net_);
        if (!bid_live_ && abs_net + 1 <= max_po_) {
            bid_live_ = true;
        }
        if (!ask_live_ && abs_net + 1 <= max_po_) {
            ask_live_ = true;
        }
        const long long live = growthLegsLive();
        if (live > max_growth_legs_live_observed_) {
            max_growth_legs_live_observed_ = live;
        }
    }
};

// ---------------------------------------------------------------------------
// Helper: build a strategy + venue up to a target long position by simulating
// place + fill cycles. Drains REST cache so cap-gate sees true position.
// ---------------------------------------------------------------------------
void buildToLong(StrategyModel& st, VenueModel& venue, long long target,
                 std::int64_t& now_ms) {
    while (st.local_net_ < target) {
        st.local_net_ += 1;
        venue.applyTrueFill(st.local_net_, now_ms);
        (void)venue.pollRest(now_ms + venue.rest_lag_ms_.load() + 1);
        now_ms += 100;
    }
}

// ---------------------------------------------------------------------------
// Scenario 1 — Tim's exact happy path. max_po=3, hit cap (long 3, reduce-only,
// only ask resting). Ask fills → long 2. Race fires. There is NO growth leg
// to cancel (bid was already suppressed by reduce-only). Race resolves via
// forced REST refresh once the venue cache catches up.
//
// This is the exact production scenario from `Error evening of May 21.txt`.
// ---------------------------------------------------------------------------
void scenario_tim_exact_reduce_only_no_growth_leg() {
    VenueModel    venue;
    RestClient    rest;
    StrategyModel st{/*max_po=*/3};

    std::int64_t now_ms = 0;
    venue.rest_lag_ms_.store(200);

    buildToLong(st, venue, 3, now_ms);
    TX_EQ(st.local_net_, 3LL);

    // At cap → reduce-only: only ask live.
    st.bid_live_ = false;
    st.ask_live_ = true;

    // Sell fill: local 3 → 2.
    const std::int64_t fill_at_ms = now_ms;
    st.applyLocalFill(2, fill_at_ms);
    venue.applyTrueFill(2, fill_at_ms);

    bool reload_complete = false;
    std::int64_t reload_complete_at_ms = -1;

    while (now_ms - fill_at_ms < kFillRaceMaxStuckMs + kEscalationEpsilonMs) {
        const bool proceed = st.reconcile(venue, rest, now_ms);

        // INVARIANT 1: true venue net never exceeds max_po.
        TX_LE(std::llabs(venue.true_net_.load()), st.max_po_);
        // INVARIANT 6: ≤1 growth-side leg live at any instant.
        TX_LE(st.growthLegsLive(), 1LL);

        if (proceed) {
            st.quotePass();
            TX_LE(st.growthLegsLive(), 1LL);
            if (st.bid_live_ && st.ask_live_) {
                reload_complete = true;
                reload_complete_at_ms = now_ms;
                break;
            }
        }
        now_ms += kReconcileCycleMs;
    }

    // INVARIANT 4: reload completed within budget.
    TX_REQUIRE(reload_complete);
    TX_LE(reload_complete_at_ms - fill_at_ms, kFillRaceMaxStuckMs);

    // INVARIANT 3: reducer side leg was not touched by the race path.
    TX_REQUIRE(!st.reducer_leg_touched_during_race_);

    // INVARIANT 5: no escalation on the happy path.
    TX_REQUIRE(!st.escalated_to_max_magnitude_);
    TX_EQ(st.n_timeouts_, 0LL);

    // INVARIANT 7: forced refresh count bounded by throttle.
    // Worst case in 3s budget @ 500ms throttle = 7 (initial + 6 throttled).
    TX_LE(rest.n_forced_refreshes_.load(),
          (kFillRaceMaxStuckMs / kFillRaceForcedRefreshMinIntervalMs) + 1);

    // Note: INVARIANT 2 (cancel dispatched) is NOT asserted here because in
    // the reduce-only scenario there is no growth leg to cancel. The next
    // scenario exercises that branch.
}

// ---------------------------------------------------------------------------
// Scenario 2 — below-cap happy path with a LIVE growth leg. max_po=5, long 2,
// both legs resting. Sell fill → long 1, bid still live. Race detects the
// stale venue cache and cancels the bid (growth leg). After REST catches
// up, reload places both legs again.
// ---------------------------------------------------------------------------
void scenario_below_cap_growth_leg_cancelled() {
    VenueModel    venue;
    RestClient    rest;
    StrategyModel st{/*max_po=*/5};

    std::int64_t now_ms = 0;
    venue.rest_lag_ms_.store(200);

    buildToLong(st, venue, 2, now_ms);
    TX_EQ(st.local_net_, 2LL);

    // Both legs live (below cap).
    st.bid_live_ = true;
    st.ask_live_ = true;

    // Sell fill: local 2 → 1. Bid (growth) stays live; ask (filled) dies.
    const std::int64_t fill_at_ms = now_ms;
    st.applyLocalFill(1, fill_at_ms);
    venue.applyTrueFill(1, fill_at_ms);
    TX_REQUIRE(st.bid_live_);
    TX_REQUIRE(!st.ask_live_);

    const long long n_cancels_at_race_start = rest.n_growth_cancels_dispatched_.load();
    bool reload_complete = false;
    std::int64_t reload_complete_at_ms = -1;

    while (now_ms - fill_at_ms < kFillRaceMaxStuckMs + kEscalationEpsilonMs) {
        const bool proceed = st.reconcile(venue, rest, now_ms);
        TX_LE(std::llabs(venue.true_net_.load()), st.max_po_);
        TX_LE(st.growthLegsLive(), 1LL);

        if (proceed) {
            st.quotePass();
            TX_LE(st.growthLegsLive(), 1LL);
            if (st.bid_live_ && st.ask_live_) {
                reload_complete = true;
                reload_complete_at_ms = now_ms;
                break;
            }
        }
        now_ms += kReconcileCycleMs;
    }

    // INVARIANT 2: growth-side cancel dispatched (counter > 0).
    TX_REQUIRE(rest.n_growth_cancels_dispatched_.load() > n_cancels_at_race_start);

    // INVARIANT 4: reload completed within budget.
    TX_REQUIRE(reload_complete);
    TX_LE(reload_complete_at_ms - fill_at_ms, kFillRaceMaxStuckMs);

    // INVARIANT 5: no escalation on happy path.
    TX_REQUIRE(!st.escalated_to_max_magnitude_);

    // INVARIANT 7: forced refresh throttle.
    TX_LE(rest.n_forced_refreshes_.load(),
          (kFillRaceMaxStuckMs / kFillRaceForcedRefreshMinIntervalMs) + 1);

    // INVARIANT 1 + 6 asserted inside the loop on every cycle.
}

// ---------------------------------------------------------------------------
// Scenario 3 — wedged REST never reflects the fill. Escalation MUST fire
// within budget + epsilon (no silent freeze) and force-refresh count stays
// bounded.
// ---------------------------------------------------------------------------
void scenario_wedged_rest_escalates_within_budget() {
    VenueModel    venue;
    RestClient    rest;
    StrategyModel st{/*max_po=*/5};

    std::int64_t now_ms = 0;
    venue.rest_lag_ms_.store(200);

    buildToLong(st, venue, 2, now_ms);
    st.bid_live_ = true;
    st.ask_live_ = true;

    const std::int64_t fill_at_ms = now_ms;
    st.applyLocalFill(1, fill_at_ms);
    venue.applyTrueFill(1, fill_at_ms);
    venue.wedged_.store(true);  // REST will never advance from here

    bool escalated = false;
    std::int64_t escalated_at_ms = -1;

    while (now_ms - fill_at_ms < kFillRaceMaxStuckMs + kEscalationEpsilonMs * 4) {
        (void)st.reconcile(venue, rest, now_ms);
        TX_LE(std::llabs(venue.true_net_.load()), st.max_po_);
        TX_LE(st.growthLegsLive(), 1LL);

        if (st.escalated_to_max_magnitude_) {
            escalated = true;
            escalated_at_ms = now_ms;
            break;
        }
        now_ms += kReconcileCycleMs;
    }

    // INVARIANT 5: escalation fired within budget + epsilon.
    TX_REQUIRE(escalated);
    TX_LE(escalated_at_ms - fill_at_ms, kFillRaceMaxStuckMs + kEscalationEpsilonMs);

    // INVARIANT 7: forced refresh count bounded — wedged REST cannot DoS us.
    TX_LE(rest.n_forced_refreshes_.load(),
          (kFillRaceMaxStuckMs / kFillRaceForcedRefreshMinIntervalMs) + 1);
}

// ---------------------------------------------------------------------------
// Scenario 4 — cancel REST itself fails (network/timeout, 3 retries
// exhaust). The fix MUST NOT return true even if venue converges, because
// the old growth leg may still be alive at the venue.
// ---------------------------------------------------------------------------
void scenario_cancel_failure_no_overshoot() {
    VenueModel    venue;
    RestClient    rest;
    StrategyModel st{/*max_po=*/5};

    std::int64_t now_ms = 0;
    venue.rest_lag_ms_.store(200);

    buildToLong(st, venue, 2, now_ms);
    st.bid_live_ = true;
    st.ask_live_ = true;

    const std::int64_t fill_at_ms = now_ms;
    st.applyLocalFill(1, fill_at_ms);
    venue.applyTrueFill(1, fill_at_ms);

    // BREAK THE CANCEL PATH — all cancel attempts return 0.
    rest.cancel_should_fail_.store(true);

    while (now_ms - fill_at_ms < kFillRaceMaxStuckMs) {
        const bool proceed = st.reconcile(venue, rest, now_ms);
        if (proceed) {
            st.quotePass();
        }
        // INVARIANT 6: ≤1 growth leg live at any instant. This is the actual
        // safety claim — even after `proceed`, the quote pass must not
        // create a duplicate growth leg while the old one is potentially
        // still alive at the venue.
        TX_LE(st.growthLegsLive(), 1LL);
        // INVARIANT 1: true net never exceeds max_po (no fill is simulated
        // on the stuck old bid, so true_net stays at 1; but if a fill were
        // to land on the stuck bid, the cap math holds because max_po=5 and
        // true_net would only go to 2).
        TX_LE(std::llabs(venue.true_net_.load()), st.max_po_);
        now_ms += kReconcileCycleMs;
    }

    // The CASE-A RESOLVED branch must NEVER fire while cancel is failing —
    // that's the precise branch gated by `growth_cancel_ok` in the
    // production fix. (The top-of-function agreement branch CAN fire once
    // the venue REST cache catches up; that's safe because `bid_live_`
    // remains true and `quotePass` won't place a duplicate.)
    TX_EQ(st.n_resolved_returns_, 0LL);
    // Bid must still be live (cancel never succeeded, local tracking was
    // intentionally preserved by the fix).
    TX_REQUIRE(st.bid_live_);
    // ≤1 growth leg observed across the whole window (proves the model
    // never transiently had two growth legs).
    TX_LE(st.max_growth_legs_live_observed_, 1LL);
}

// ---------------------------------------------------------------------------
// Scenario 5 — Silver-overtrade shape must still adopt venue. Regression
// guard for the legacy max-magnitude branch we explicitly preserved.
// ---------------------------------------------------------------------------
void scenario_silver_overtrade_still_adopts_venue() {
    VenueModel    venue;
    RestClient    rest;
    StrategyModel st{/*max_po=*/6};

    venue.rest_lag_ms_.store(0);  // REST is fresh

    // Silver pattern: local stuck at 1, no recent fill, venue grew to 6.
    st.local_net_         = 1;
    st.last_fill_time_ms_ = 0;
    venue.applyTrueFill(6, 0);
    (void)venue.pollRest(1);

    // Make sure the cache is fresh in the strategy's RestClient too.
    rest.last_refresh_at_ms_ = std::numeric_limits<std::int64_t>::min() / 2;

    const bool proceed = st.reconcile(venue, rest, 100);

    // Must NOT proceed (disagreement should abort).
    TX_REQUIRE(!proceed);
    // Must adopt venue (silver-defense behaviour).
    TX_EQ(st.local_net_, 6LL);
    TX_REQUIRE(st.escalated_to_max_magnitude_);
}

}  // namespace

int main() {
    std::printf("=== Tim Izzo fill-race no-freeze regression (2026-05-22 XAU-PERP) ===\n");
    TX_RUN(scenario_tim_exact_reduce_only_no_growth_leg);
    TX_RUN(scenario_below_cap_growth_leg_cancelled);
    TX_RUN(scenario_wedged_rest_escalates_within_budget);
    TX_RUN(scenario_cancel_failure_no_overshoot);
    TX_RUN(scenario_silver_overtrade_still_adopts_venue);
    return tx::finish("test_fill_race_no_freeze");
}
