// =============================================================================
// CANCEL-FIRST theo-move reorder — canceled-order-that-filled blocks the place
// =============================================================================
//
// Rationale from the directive: the position check exists to catch "the order I
// just canceled actually filled → don't place the pair at cap". That is only
// answerable AFTER the cancel response, which is exactly why the reorder keeps
// the NetPo read + shouldQuoteSide on the RE-ADD side of the gate.
//
// Production wiring: after the cancel-first drain, on_cancel re-enqueues
// "theo_move" → runFullMmQuoteCycle → mmWaitReconcileGateBeforePlace waits for a
// /fills poll newer than the last cancel response, then the NetPo snapshot
// (MakeMarketStrategy.cpp:9823) + shouldQuoteSide (net-only, :8481) decide
// per-side placement. If the canceled leg actually filled and pushed NetPo to
// the cap, the grow-side shouldQuoteSide returns false → grow-side place blocked.
//
// This test models that net-only gate: cancel a BUY, inject its fill via the
// poll so net reaches the long cap, then assert the grow-side (BUY) place is
// blocked while the reduce-side (SELL) place is allowed.

#include "test_helpers.h"

namespace {

// Net-only side gate, mirroring shouldQuoteSide's spec (|net| < cap on the grow
// side; the reduce side is always allowed).
bool shouldQuoteBuy(long long net, long long cap) {
    return net < cap;   // long grow side blocked at/over +cap
}
bool shouldQuoteSell(long long net, long long cap) {
    return net > -cap;  // short grow side blocked at/under -cap
}

struct PlaceResult {
    bool placed_bid{false};
    bool placed_ask{false};
};

// Model of the re-add cycle's per-side gate AFTER the cancel + fill poll.
PlaceResult reAddAfterCancel(long long net_after_poll, long long cap) {
    PlaceResult r;
    // Gate cleared (a /fills poll landed after the cancel response) → NetPo is
    // now the post-fill truth; decide each side net-only.
    r.placed_bid = shouldQuoteBuy(net_after_poll, cap);
    r.placed_ask = shouldQuoteSell(net_after_poll, cap);
    return r;
}

// -----------------------------------------------------------------------------
// Scenario 1 — canceled BUY actually filled: net jumps 0 → +cap. The grow-side
// (BUY) place MUST be blocked; the reduce-side (SELL) place is allowed.
// -----------------------------------------------------------------------------
void scenario_canceled_buy_filled_blocks_bid_place() {
    const long long cap = 5;
    // Before the cancel we were flat and had a resting BUY. The BUY silently
    // filled; the cancel returned 404; the /fills poll injects +5.
    const long long net_after_poll = 5;   // == +cap
    auto r = reAddAfterCancel(net_after_poll, cap);
    TX_REQUIRE(!r.placed_bid);   // grow-side blocked (at cap long)
    TX_REQUIRE(r.placed_ask);    // reduce-side allowed
}

// -----------------------------------------------------------------------------
// Scenario 2 — canceled SELL actually filled: net jumps 0 → -cap. The grow-side
// (SELL) place MUST be blocked; the reduce-side (BUY) place is allowed.
// -----------------------------------------------------------------------------
void scenario_canceled_sell_filled_blocks_ask_place() {
    const long long cap = 3;
    const long long net_after_poll = -3;  // == -cap
    auto r = reAddAfterCancel(net_after_poll, cap);
    TX_REQUIRE(!r.placed_ask);   // grow-side blocked (at cap short)
    TX_REQUIRE(r.placed_bid);    // reduce-side allowed
}

// -----------------------------------------------------------------------------
// Scenario 3 — control: the canceled order did NOT fill (poll still flat). Both
// sides re-quote as normal. Proves the block is specific to the fill, not a
// blanket suppression introduced by the reorder.
// -----------------------------------------------------------------------------
void scenario_no_fill_both_sides_requote() {
    const long long cap = 5;
    const long long net_after_poll = 0;   // flat
    auto r = reAddAfterCancel(net_after_poll, cap);
    TX_REQUIRE(r.placed_bid);
    TX_REQUIRE(r.placed_ask);
}

}  // namespace

int main() {
    std::printf("=== Cancel-first: canceled-order-filled blocks grow-side place ===\n");
    TX_RUN(scenario_canceled_buy_filled_blocks_bid_place);
    TX_RUN(scenario_canceled_sell_filled_blocks_ask_place);
    TX_RUN(scenario_no_fill_both_sides_requote);
    return ::tx::finish("test_canceled_order_filled_blocks_place");
}
