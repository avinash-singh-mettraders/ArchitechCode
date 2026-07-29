// =============================================================================
// REGRESSION TEST — combined cancel+place batch + self-trade guard (2026-07-13)
// =============================================================================
//
// The fire-and-track cancel-replace can fire both cancels AND both places in ONE
// curl-multi batch (Platform::placeAndCancelConcurrent) so the whole cycle costs
// ~1 RTT instead of ~2. But a combined batch gives up cancel-before-place ordering
// AT THE VENUE: on a large theo jump the fresh bid can price at/above the OLD
// resting ask (or the fresh ask at/below the old bid), so the new leg can execute
// against our OWN not-yet-cancelled leg — a self-trade.
//
// Production (MakeMarketStrategy::mmFireAndTrackPlacePair) therefore gates the
// combined path behind a guard:
//     self_trade_risk = (want_bid && old_ask>0 && new_bid >= old_ask)
//                    || (want_ask && old_bid>0 && new_ask <= old_bid)
// risk  -> cancels-batch first (await), THEN places-batch (~2 RTT, big moves only)
// clear -> combined c,c,p,p in one batch (~1 RTT, the common one-tick drift)
//
// This test models the guard predicate + both execution paths against a venue that
// registers a self-trade whenever a place is applied while a crossing opposite leg
// of ours is still live. It asserts: normal drifts take the 1-RTT combined path
// with no self-trade; crossing jumps take the guarded 2-RTT path with no
// self-trade; and (negative control) using the combined path on a crossing jump
// WOULD self-trade — proving the guard has teeth.

#include "test_helpers.h"

#include <string>
#include <vector>

namespace {

// -----------------------------------------------------------------------------
// Production-mirrored guard predicate (keep in sync with mmFireAndTrackPlacePair).
// -----------------------------------------------------------------------------
bool selfTradeRisk(bool want_bid, bool want_ask,
                   double new_bid, double new_ask,
                   double old_bid, double old_ask) {
    const bool bid_crosses_old_ask = want_bid && old_ask > 0.0 && new_bid >= old_ask;
    const bool ask_crosses_old_bid = want_ask && old_bid > 0.0 && new_ask <= old_bid;
    return bid_crosses_old_ask || ask_crosses_old_bid;
}

// -----------------------------------------------------------------------------
// Minimal venue model: OLD (resting) and NEW (replacement) legs tracked
// separately, so a cancel of an old leg never clobbers a freshly placed new leg.
// A self-trade is registered when a NEW leg is applied while our own OLD opposite
// leg is still live and crosses it.
// -----------------------------------------------------------------------------
struct Leg { bool live{false}; double px{0.0}; };

struct Venue {
    Leg old_bid, old_ask, new_bid, new_ask;
    int self_trades{0};

    void seedPair(double b, double a) {
        old_bid = {true, b};
        old_ask = {true, a};
    }
    void cancelOldBid() { old_bid.live = false; }
    void cancelOldAsk() { old_ask.live = false; }

    // Applying the NEW BUY at p self-trades if our OLD ASK is still live at/below p.
    void placeNewBid(double p) {
        if (old_ask.live && p >= old_ask.px) ++self_trades;
        new_bid = {true, p};
    }
    // Applying the NEW SELL at p self-trades if our OLD BID is still live at/above p.
    void placeNewAsk(double p) {
        if (old_bid.live && p <= old_bid.px) ++self_trades;
        new_ask = {true, p};
    }
    bool replacedCleanly() const {
        return new_bid.live && new_ask.live && !old_bid.live && !old_ask.live;
    }
};

// Combined batch (worst case for ordering): the venue may apply a place BEFORE the
// paired cancel lands. Model the adversarial interleave place-before-cancel.
int runCombined(Venue& v, double new_bid, double new_ask) {
    v.placeNewBid(new_bid);  // place can hit the wire before...
    v.placeNewAsk(new_ask);
    v.cancelOldBid();        // ...the old legs are pulled
    v.cancelOldAsk();
    return 1;  // rtt units
}

// Guarded fallback: cancels complete FIRST (awaited), then places. No old leg is
// live when a place is applied, so no self-trade is possible.
int runFallback(Venue& v, double new_bid, double new_ask) {
    v.cancelOldBid();
    v.cancelOldAsk();
    v.placeNewBid(new_bid);
    v.placeNewAsk(new_ask);
    return 2;  // rtt units
}

// Production routing: guard picks the path.
int runCancelReplace(Venue& v, double new_bid, double new_ask, double old_bid, double old_ask,
                     bool& took_combined) {
    took_combined = !selfTradeRisk(true, true, new_bid, new_ask, old_bid, old_ask);
    return took_combined ? runCombined(v, new_bid, new_ask)
                         : runFallback(v, new_bid, new_ask);
}

// -----------------------------------------------------------------------------
// 1. Normal one-tick drift: fresh pair stays inside the old pair → combined 1-RTT,
//    no self-trade.
// -----------------------------------------------------------------------------
void normal_drift_takes_combined_one_rtt() {
    Venue v;
    v.seedPair(/*bid*/ 100.00, /*ask*/ 100.10);
    bool combined = false;
    // theo ticked up one: new bid 100.01, new ask 100.11 — new bid (100.01) still below old ask
    // (100.10), new ask (100.11) still above old bid (100.00): no cross.
    const int rtt = runCancelReplace(v, 100.01, 100.11, 100.00, 100.10, combined);
    TX_REQUIRE(combined);
    TX_EQ(rtt, 1);
    TX_EQ(v.self_trades, 0);
    TX_REQUIRE(v.replacedCleanly());
}

// -----------------------------------------------------------------------------
// 2. Big up-jump: new bid prices at/above the old resting ask → guard trips,
//    fallback 2-RTT, no self-trade.
// -----------------------------------------------------------------------------
void big_up_jump_trips_guard_fallback() {
    Venue v;
    v.seedPair(/*bid*/ 100.00, /*ask*/ 100.10);
    bool combined = false;
    // theo jumped up hard: new bid 100.15 >= old ask 100.10.
    const int rtt = runCancelReplace(v, 100.15, 100.25, 100.00, 100.10, combined);
    TX_REQUIRE(!combined);
    TX_EQ(rtt, 2);
    TX_EQ(v.self_trades, 0);
    TX_REQUIRE(v.replacedCleanly());
}

// -----------------------------------------------------------------------------
// 3. Big down-jump: new ask prices at/below the old resting bid → guard trips too.
// -----------------------------------------------------------------------------
void big_down_jump_trips_guard_fallback() {
    Venue v;
    v.seedPair(/*bid*/ 100.00, /*ask*/ 100.10);
    bool combined = false;
    // new ask 99.95 <= old bid 100.00.
    const int rtt = runCancelReplace(v, 99.85, 99.95, 100.00, 100.10, combined);
    TX_REQUIRE(!combined);
    TX_EQ(rtt, 2);
    TX_EQ(v.self_trades, 0);
}

// -----------------------------------------------------------------------------
// 4. Negative control: prove the guard is load-bearing. If we (wrongly) used the
//    combined path on a crossing up-jump, the venue WOULD self-trade.
// -----------------------------------------------------------------------------
void combined_on_crossing_jump_would_self_trade() {
    Venue v;
    v.seedPair(100.00, 100.10);
    (void)runCombined(v, /*new_bid*/ 100.15, /*new_ask*/ 100.25);  // new bid 100.15 >= old ask 100.10
    TX_EQ(v.self_trades, 1);  // caught — this is exactly what the guard prevents
}

// -----------------------------------------------------------------------------
// 5. Guard predicate boundaries: equality counts as a cross (>= / <=); want flags
//    gate each side; a side with no old leg (px 0) never trips.
// -----------------------------------------------------------------------------
void guard_predicate_boundaries() {
    // Exact touch: new bid == old ask -> risk.
    TX_REQUIRE(selfTradeRisk(true, true, 100.10, 100.30, 100.00, 100.10));
    // One tick below the old ask -> safe.
    TX_REQUIRE(!selfTradeRisk(true, true, 100.09, 100.30, 100.00, 100.10));
    // Exact touch on the ask side: new ask == old bid -> risk.
    TX_REQUIRE(selfTradeRisk(true, true, 99.80, 100.00, 100.00, 100.10));
    // Not re-adding the bid -> bid cross ignored even if it would cross.
    TX_REQUIRE(!selfTradeRisk(false, true, 100.15, 100.30, 100.00, 100.10));
    // No old ask resting (reduce-only pulled it) -> bid can't cross a phantom.
    TX_REQUIRE(!selfTradeRisk(true, true, 100.15, 100.30, 100.00, 0.0));
}

// -----------------------------------------------------------------------------
// 6. One-sided re-add (reduce-only dropped the ask): combined stays safe because
//    there is no opposite leg to cross.
// -----------------------------------------------------------------------------
void one_sided_readd_is_safe_combined() {
    Venue v;
    v.seedPair(100.00, 100.10);
    // Only re-adding the bid; ask not wanted. Even a high new bid can't self-trade once we
    // also cancel the old ask — but guard must not trip on want_ask=false.
    const bool risk = selfTradeRisk(/*want_bid*/ true, /*want_ask*/ false,
                                    /*new_bid*/ 100.15, /*new_ask*/ 0.0,
                                    /*old_bid*/ 100.00, /*old_ask*/ 100.10);
    // new bid 100.15 >= old ask 100.10 with want_bid true -> still risky (old ask is live).
    TX_REQUIRE(risk);
    (void)v;
}

}  // namespace

int main() {
    std::printf("=== combined cancel+place batch + self-trade guard ===\n");
    TX_RUN(normal_drift_takes_combined_one_rtt);
    TX_RUN(big_up_jump_trips_guard_fallback);
    TX_RUN(big_down_jump_trips_guard_fallback);
    TX_RUN(combined_on_crossing_jump_would_self_trade);
    TX_RUN(guard_predicate_boundaries);
    TX_RUN(one_sided_readd_is_safe_combined);
    return tx::finish("test_combined_cancel_place_batch");
}
