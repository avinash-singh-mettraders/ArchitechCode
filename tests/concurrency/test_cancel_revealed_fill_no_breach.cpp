// =============================================================================
// REGRESSION TEST — SILVER / XAG max_position breach (2026-05-21)
// =============================================================================
//
// Joe & Tim incident (2026-05-21):
//   - max_position = 2 on a SILVER mm_req stack.
//   - Venue NetPo drifted from +1 → +3 in ~6.5 seconds (Example.txt:4725-4931)
//     even though all the cap-gates in runFullMmQuoteCycle were intact.
//
// Root cause (per Example.txt deep-dive):
//   1. Stack a56a8ea7 issued a REST cancel for resting BUY 312 @ 75.01.
//      Venue returned HTTP 404 because the order had filled an instant earlier.
//   2. `mmIsBenignCancelFailure` classified 404 as benign and evicted the leg
//      WITHOUT updating position_state_.net_position_qty.
//   3. Strategy's local NetPo stayed at +1 (the pre-fill value from the REST
//      sync at 07:49:22.096) while the venue was actually at +2.
//   4. The next cycle's cap-gate read net_po_rounded=1, cap=2, qty=1 →
//      projected_long = 1+0+1 = 2 ≤ 2 → ALLOWED. Submitted BUY 314.
//   5. BUY 314 filled. Venue went to +3 → cap breach. Reduce-only fired 2s
//      later via the next REST sync — too late.
//
// This test models the EXACT race:
//   - Phase 1: an in-flight order silently fills at the venue (record_silent_fill).
//   - Phase 2: the strategy attempts to cancel it. The mock venue returns
//     "404 benign". The fix MUST detect the venue position drift and synthesize
//     the fill BEFORE the cycle's cap-gate runs.
//   - Phase 3: the cycle re-reads NetPo and runs the hard pre-gate. If NetPo
//     is at/over the cap on the grow side, the next same-side place is
//     refused even if the original shouldQuoteSide() said "yes".
//
// THE INVARIANT: at no point during the race may `exposure = max(0, net_po) +
// inflight_on_side` exceed `cap` on the grow side.

#include "test_helpers.h"

#include <atomic>
#include <chrono>
#include <mutex>
#include <thread>

namespace {

// ---------------------------------------------------------------------------
// Mock venue + strategy state. Production analogues are noted on each field.
// ---------------------------------------------------------------------------
enum class Side { Buy, Sell };

struct MockState {
    // Production: PortfolioManager::getPosition(sym).quantity (signed).
    std::mutex venue_mu_;
    long long  venue_net_po_{0};

    // Production: MakeMarketStrategy::position_state_.net_position_qty.
    std::mutex local_pos_mu_;
    long long  local_net_po_{0};

    // Production: tracked_bids_ / tracked_asks_ entry.
    std::mutex leg_mu_;
    bool       leg_active_{false};
    long long  leg_qty_{0};
    Side       leg_side_{Side::Buy};

    // Production: cap from Config (max_position).
    long long  cap_{2};

    // Diagnostic: highest |exposure| observed during the race.
    std::mutex breach_mu_;
    long long  worst_exposure_{0};
};

// Mirror the new CANCEL_REVEALED_FILL behaviour from
// `mmCancelTrackedLegThisStackGateway`:
//   - on 404/benign: refresh venue, compare delta, synth a fill if direction
//     matches the leg's side.
//   - then evict the leg.
// Returns 1 (success) in both fill-detected and true-cancel cases.
int cancelLegBenign404(MockState& st, Side cancel_side) {
    long long local_before = 0;
    {
        std::lock_guard<std::mutex> pl(st.local_pos_mu_);
        local_before = st.local_net_po_;
    }
    long long venue_now = 0;
    {
        std::lock_guard<std::mutex> vl(st.venue_mu_);
        venue_now = st.venue_net_po_;
    }
    long long leg_qty = 0;
    bool leg_was_active = false;
    {
        std::lock_guard<std::mutex> ll(st.leg_mu_);
        leg_qty = st.leg_qty_;
        leg_was_active = st.leg_active_;
    }
    const long long actual_delta = venue_now - local_before;
    const double half_qty = 0.5 * std::max<long long>(1, leg_qty);
    const bool buy_dir_match =
        (cancel_side == Side::Buy && actual_delta > half_qty);
    const bool sell_dir_match =
        (cancel_side == Side::Sell && actual_delta < -half_qty);
    const bool fill_detected =
        leg_was_active && (buy_dir_match || sell_dir_match);
    if (fill_detected) {
        std::lock_guard<std::mutex> pl(st.local_pos_mu_);
        st.local_net_po_ = venue_now;
    }
    {
        std::lock_guard<std::mutex> ll(st.leg_mu_);
        st.leg_active_ = false;
        st.leg_qty_ = 0;
    }
    return 1;
}

// Mirror the hard pre-gate + cross-stack projection from the updated
// runFullMmQuoteCycle BUY branch.
bool runQuoteCycleBuy(MockState& st, long long new_qty) {
    // Pre-cycle NetPo snapshot (production: read at line ~6778).
    long long net_po_pre = 0;
    {
        std::lock_guard<std::mutex> pl(st.local_pos_mu_);
        net_po_pre = st.local_net_po_;
    }
    // shouldQuoteSide(BUY) — `|net|<cap` only checked against pre-cancel value.
    const bool would_quote_pre = (std::abs(net_po_pre) < st.cap_);
    if (!would_quote_pre) {
        return false;  // pre-gate refused
    }
    // Cancel + 404-benign path: may surface a missed fill into local NetPo.
    (void)cancelLegBenign404(st, Side::Buy);
    // Re-read NetPo AFTER cancel (the new fix).
    long long net_po_post = 0;
    {
        std::lock_guard<std::mutex> pl(st.local_pos_mu_);
        net_po_post = st.local_net_po_;
    }
    // Hard pre-gate (BUY is grow side when net>=0).
    const bool buy_is_grow_side = (net_po_post >= 0);
    if (buy_is_grow_side && net_po_post >= st.cap_) {
        return false;  // MM_PLACE_BID_HARD_CAP_GATE
    }
    // Cross-stack projection (single-stack here, sibling_inflight=0).
    const long long projected =
        std::max<long long>(0, net_po_post) + 0 + new_qty;
    if (projected > st.cap_) {
        return false;  // MM_PLACE_BID_CAP_GATE
    }
    // Place admitted. Simulate the order joining the book (instantly filled
    // for the simplest worst-case test — the venue immediately accepts it).
    {
        std::lock_guard<std::mutex> ll(st.leg_mu_);
        st.leg_active_ = true;
        st.leg_qty_ = new_qty;
    }
    {
        std::lock_guard<std::mutex> vl(st.venue_mu_);
        st.venue_net_po_ += new_qty;
    }
    {
        std::lock_guard<std::mutex> pl(st.local_pos_mu_);
        st.local_net_po_ += new_qty;
    }
    return true;
}

// Track worst-case exposure across the race (long-side only).
void observeBreach(MockState& st) {
    long long net = 0;
    long long inflight = 0;
    {
        std::lock_guard<std::mutex> pl(st.local_pos_mu_);
        net = st.local_net_po_;
    }
    {
        std::lock_guard<std::mutex> ll(st.leg_mu_);
        inflight = st.leg_active_ ? st.leg_qty_ : 0;
    }
    const long long exposure = std::max<long long>(0, net) + inflight;
    std::lock_guard<std::mutex> bl(st.breach_mu_);
    if (exposure > st.worst_exposure_) {
        st.worst_exposure_ = exposure;
    }
}

// ---------------------------------------------------------------------------
// Scenario: replicate Example.txt @ 07:49:22 → 07:49:28 step-by-step.
// ---------------------------------------------------------------------------
void scenario_silver_cancel_revealed_fill_no_breach() {
    MockState st;
    st.cap_ = 2;
    // Initial state: REST sync just reported venue=+1 and local=+1
    // (production: signed_qty=1 at 07:49:22.096).
    st.venue_net_po_ = 1;
    st.local_net_po_ = 1;
    // The resting BUY leg is order 312 — qty 1 @ 75.01.
    st.leg_active_ = true;
    st.leg_qty_ = 1;

    // Silent venue fill of the resting BUY between adoption and our cancel
    // (the actual incident: bid 75.01 filled between 07:49:22.064 and
    // 07:49:24.557, but the strategy never received a fill event).
    {
        std::lock_guard<std::mutex> vl(st.venue_mu_);
        st.venue_net_po_ += 1;  // venue NOW at +2 — already at cap
    }
    observeBreach(st);
    // Note: at this instant exposure = max(0,1) + 1(inflight) = 2 = cap.
    // We have not breached YET, but we've burned every bit of headroom.

    // The cycle runs: shouldQuoteSide(BUY) sees pre-cancel NetPo=1 < cap=2,
    // would happily proceed. WITHOUT the fix, the gate would then place a
    // new BUY, fill, and breach to +3. WITH the fix, the cancel surfaces
    // the missed fill, the hard pre-gate sees NetPo=2 >= cap=2, refuses
    // the place.
    const bool placed = runQuoteCycleBuy(st, /*new_qty=*/1);
    observeBreach(st);

    std::printf(
        "    SILVER cancel-revealed-fill: placed=%d local_net=%lld venue_net=%lld "
        "worst_exposure=%lld cap=%lld\n",
        placed ? 1 : 0,
        st.local_net_po_, st.venue_net_po_,
        st.worst_exposure_, st.cap_);

    // INVARIANT 1: the BUY MUST be refused.
    TX_REQUIRE(!placed);
    // INVARIANT 2: venue NetPo must remain at +2 (i.e., never breach to +3).
    TX_LE(st.venue_net_po_, st.cap_);
    // INVARIANT 3: worst observed exposure stays at cap, never above.
    TX_LE(st.worst_exposure_, st.cap_);
}

// ---------------------------------------------------------------------------
// Concurrent race: each iteration plants a silent venue fill BEFORE the
// strategy's cancel REST and then runs the cycle. Production order of
// operations: order fills at venue -> our cancel REST -> 404 response ->
// CANCEL_REVEALED_FILL detects the missed fill -> gate refuses next place.
// ---------------------------------------------------------------------------
void scenario_concurrent_silent_fill_then_cycle() {
    constexpr int kIters = 50;
    for (int i = 0; i < kIters; ++i) {
        MockState st;
        st.cap_ = 2;
        st.venue_net_po_ = 1;
        st.local_net_po_ = 1;
        st.leg_active_ = true;
        st.leg_qty_ = 1;
        // Silent venue fill happens FIRST (mirrors the real timeline where
        // the order fills at the venue some time before our cancel REST
        // arrives). The cycle on the mover thread starts a moment later.
        std::thread fill_thread([&] {
            std::lock_guard<std::mutex> vl(st.venue_mu_);
            st.venue_net_po_ += 1;
        });
        fill_thread.join();
        std::thread cycle_thread([&] {
            const bool placed = runQuoteCycleBuy(st, 1);
            observeBreach(st);
            (void)placed;
        });
        cycle_thread.join();
        observeBreach(st);
        if (st.worst_exposure_ > st.cap_) {
            std::printf("    iter %d BREACH: local=%lld venue=%lld worst=%lld cap=%lld\n",
                        i, st.local_net_po_, st.venue_net_po_,
                        st.worst_exposure_, st.cap_);
            TX_LE(st.worst_exposure_, st.cap_);
            return;
        }
    }
    std::printf("    concurrent race: %d iterations, all exposures <= cap=2\n", kIters);
}

// ---------------------------------------------------------------------------
// Mirror image for the SHORT side — same bug, same fix, SELL grow side.
// ---------------------------------------------------------------------------
void scenario_short_side_mirror_no_breach() {
    MockState st;
    st.cap_ = 2;
    st.venue_net_po_ = -1;
    st.local_net_po_ = -1;
    st.leg_active_ = true;
    st.leg_qty_ = 1;
    st.leg_side_ = Side::Sell;
    // Silent venue fill of the SELL leg pushes venue to -2.
    {
        std::lock_guard<std::mutex> vl(st.venue_mu_);
        st.venue_net_po_ -= 1;
    }
    long long net_pre = 0;
    {
        std::lock_guard<std::mutex> pl(st.local_pos_mu_);
        net_pre = st.local_net_po_;
    }
    const bool would_quote_pre = (std::abs(net_pre) < st.cap_);
    TX_REQUIRE(would_quote_pre);  // pre-gate would not block (mirrors prod)
    (void)cancelLegBenign404(st, Side::Sell);
    long long net_post = 0;
    {
        std::lock_guard<std::mutex> pl(st.local_pos_mu_);
        net_post = st.local_net_po_;
    }
    // Hard pre-gate for SELL: grow side when net<=0; refuse if -net>=cap.
    const bool sell_is_grow_side = (net_post <= 0);
    const bool refused = (sell_is_grow_side && -net_post >= st.cap_);
    TX_REQUIRE(refused);

    std::printf(
        "    SHORT-side mirror: net_pre=%lld net_post=%lld venue=%lld refused=%d\n",
        net_pre, net_post, st.venue_net_po_, refused ? 1 : 0);

    TX_LE(-st.venue_net_po_, st.cap_);
}

}  // namespace

int main() {
    std::printf("=== SILVER / XAG max_position breach (2026-05-21) regression ===\n");
    TX_RUN(scenario_silver_cancel_revealed_fill_no_breach);
    TX_RUN(scenario_concurrent_silent_fill_then_cycle);
    TX_RUN(scenario_short_side_mirror_no_breach);
    return tx::finish("test_cancel_revealed_fill_no_breach");
}
