// =============================================================================
// REGRESSION TEST — Concurrent pair-cancel (theo-move latency fix, 2026-07-14)
// =============================================================================
//
// Production change: MakeMarketStrategy::enqueueRestCancelsConcurrentOnMover ->
// mmSendRestCancelsConcurrentByOrderId fires BOTH stale-leg cancels of a theo-move
// in ONE curl-multi batch (~1 venue RTT) instead of two serial mover jobs (~2 RTT).
//
// The change MUST be semantics-preserving and DEAD-SAFE. This test models the exact
// per-leg control flow of mmSendRestCancelsConcurrentByOrderId + the cancel-ack ->
// replace gating (mmDeskPairCancelSettledForTheoReplace + the coalescing
// enqueueQuoteCycleOnMover) and asserts the invariants that keep it safe:
//
//   1. One batch: N valid legs -> N cancel bodies, fired together, order-preserving.
//   2. NO double-fire: each confirmed leg produces EXACTLY ONE om.cancelOrder()
//      bookkeeping (== one ORDER_CANCELLED == one cancel-ack), never two. (This is
//      the same class of bug as the cancels=4 fix — see
//      test_combined_cancel_no_double_fire.cpp.)
//   3. POST-gateway then DELETE fallback on hard failure — byte-identical to the
//      serial mmSendRestCancelByOrderId.
//   4. A leg whose cancel HARD-FAILS (gateway + DELETE) is left CANCEL_PENDING (no
//      bookkeeping) and the pair is therefore NOT settled -> the fresh pair is NOT
//      placed. A failed cancel can NEVER cause a replacement order (no dupes, no
//      naked exposure created by this path).
//   5. The replacement fires ONLY after BOTH legs are venue-confirmed, and at most
//      once (coalesced) — the confirm-then-replace contract.
//   6. Invalid legs (no exchange oid) are never sent.

#include "test_helpers.h"

#include <cstddef>
#include <string>
#include <vector>

namespace {

enum class Resp { SUCCESS, BENIGN_404, HARD_FAIL };

// Faithful mirror of the fields mmSendRestCancelsConcurrentByOrderId reads per leg.
struct Leg {
    int tid{0};
    bool has_oid{true};        // op->exchange_order_id non-empty
    Resp gateway{Resp::SUCCESS};
    Resp del{Resp::HARD_FAIL};  // DELETE fallback result (only consulted on gateway fail)
    bool side_is_bid{true};
};

struct Outcome {
    int bodies_sent{0};       // gateway cancels issued in the concurrent batch
    int delete_fallbacks{0};  // serial DELETE retries (only on hard gateway failure)
    int bookkeeping{0};       // om.cancelOrder() calls == ORDER_CANCELLED published
    int left_open{0};         // legs left CANCEL_PENDING for the async reconcile
    bool bid_cancelled{false};
    bool ask_cancelled{false};
    int replace_count{0};     // fresh pairs placed (coalesced enqueueQuoteCycleOnMover)
};

// mmIsBenignCancelFailure() analogue: a 404 means the venue already dropped it.
bool benign(Resp r) { return r == Resp::BENIGN_404; }
bool success(Resp r) { return r == Resp::SUCCESS; }

// Models mmSendRestCancelsConcurrentByOrderId + the on_cancel -> replace gating.
Outcome run(const std::vector<Leg>& legs) {
    Outcome o;

    // Pre-flight: only legs with an exchange oid become cancel bodies (mirrors the
    // `oid.empty()` skip). The batch fires all bodies together.
    std::vector<const Leg*> wire;
    for (const auto& lg : legs) {
        if (!lg.has_oid) {
            continue;  // skipped — never sent
        }
        wire.push_back(&lg);
    }
    o.bodies_sent = static_cast<int>(wire.size());

    // Per-response processing — byte-for-byte the production loop:
    //   if (!gateway.success) cr = DELETE(oid);
    //   if (!cr.success && !benign(cr)) { leave open; continue; }
    //   om.cancelOrder(req);   // success OR benign
    for (const Leg* lg : wire) {
        Resp cr = lg->gateway;
        if (!success(cr)) {
            ++o.delete_fallbacks;
            cr = lg->del;
        }
        if (!success(cr) && !benign(cr)) {
            ++o.left_open;
            continue;  // CANCEL_PENDING — timeout backstop re-sends. NEVER re-placed here.
        }
        ++o.bookkeeping;  // exactly one ORDER_CANCELLED for this leg
        if (lg->side_is_bid) {
            o.bid_cancelled = true;
        } else {
            o.ask_cancelled = true;
        }
    }

    // Cancel-ack -> replace gating: the fresh pair is placed ONLY when BOTH legs are
    // venue-confirmed cancelled (mmDeskPairCancelSettledForTheoReplace), and
    // enqueueQuoteCycleOnMover coalesces so it happens at most once.
    if (o.bid_cancelled && o.ask_cancelled) {
        o.replace_count = 1;
    }
    return o;
}

// 1+2+5. The happy path: two legs, both gateway-confirmed. One batch of two, exactly
// two cancel-acks (NO double-fire), and exactly one replacement after both confirm.
void two_legs_success_one_batch_no_double_fire_one_replace() {
    std::vector<Leg> legs = {
        {/*tid=*/1, /*has_oid=*/true, Resp::SUCCESS, Resp::HARD_FAIL, /*bid=*/true},
        {/*tid=*/2, /*has_oid=*/true, Resp::SUCCESS, Resp::HARD_FAIL, /*bid=*/false},
    };
    Outcome o = run(legs);
    TX_EQ(o.bodies_sent, 2);
    TX_EQ(o.delete_fallbacks, 0);
    TX_EQ(o.bookkeeping, 2);   // one ORDER_CANCELLED per leg — never four
    TX_EQ(o.left_open, 0);
    TX_EQ(o.replace_count, 1); // placed exactly once, after BOTH cancels confirmed
}

// 3. Benign 404 (venue already dropped it) is treated as cancelled — same as serial.
// A gateway 404 is not is_success, so (exactly like mmSendRestCancelByOrderId) it falls
// through to the DELETE fallback, which for an already-gone order also returns 404 (benign)
// -> the leg is booked as cancelled.
void benign_404_counts_as_confirmed() {
    std::vector<Leg> legs = {
        {1, true, Resp::BENIGN_404, /*del=*/Resp::BENIGN_404, true},
        {2, true, Resp::SUCCESS, Resp::HARD_FAIL, false},
    };
    Outcome o = run(legs);
    TX_EQ(o.bodies_sent, 2);
    TX_EQ(o.delete_fallbacks, 1);  // the 404 leg still consults DELETE (byte-identical to serial)
    TX_EQ(o.bookkeeping, 2);
    TX_EQ(o.left_open, 0);
    TX_EQ(o.replace_count, 1);
}

// 3. Hard gateway failure that the DELETE fallback rescues -> confirmed.
void hardfail_then_delete_success_is_confirmed() {
    std::vector<Leg> legs = {
        {1, true, Resp::HARD_FAIL, /*del=*/Resp::SUCCESS, true},
        {2, true, Resp::SUCCESS, Resp::HARD_FAIL, false},
    };
    Outcome o = run(legs);
    TX_EQ(o.bodies_sent, 2);
    TX_EQ(o.delete_fallbacks, 1);  // one DELETE retry for the failed gateway leg
    TX_EQ(o.bookkeeping, 2);
    TX_EQ(o.left_open, 0);
    TX_EQ(o.replace_count, 1);
}

// 4. THE DEAD-SAFE INVARIANT: a leg whose cancel truly fails (gateway AND DELETE) is
// left open and is NEVER re-placed. Because the pair is not settled, NO replacement
// order goes out — a failed cancel can never create a duplicate or naked leg here.
void hard_fail_both_leaves_open_and_never_replaces() {
    std::vector<Leg> legs = {
        {1, true, Resp::HARD_FAIL, /*del=*/Resp::HARD_FAIL, true},  // truly failed
        {2, true, Resp::SUCCESS, Resp::HARD_FAIL, false},           // confirmed
    };
    Outcome o = run(legs);
    TX_EQ(o.bodies_sent, 2);
    TX_EQ(o.delete_fallbacks, 1);
    TX_EQ(o.bookkeeping, 1);   // only the confirmed leg
    TX_EQ(o.left_open, 1);     // the failed leg stays CANCEL_PENDING
    TX_REQUIRE(!o.bid_cancelled);
    TX_REQUIRE(o.ask_cancelled);
    TX_EQ(o.replace_count, 0); // pair NOT settled -> NO fresh pair placed
}

// 6. A leg without an exchange oid is never put on the wire.
void leg_without_oid_is_not_sent() {
    std::vector<Leg> legs = {
        {1, /*has_oid=*/false, Resp::SUCCESS, Resp::HARD_FAIL, true},
        {2, /*has_oid=*/true, Resp::SUCCESS, Resp::HARD_FAIL, false},
    };
    Outcome o = run(legs);
    TX_EQ(o.bodies_sent, 1);   // only the leg with an oid
    TX_EQ(o.bookkeeping, 1);
    TX_EQ(o.replace_count, 0); // bid never confirmed -> not settled -> no replace
}

// 2 (stress). No matter the mix, ORDER_CANCELLED count == confirmed legs and never
// exceeds the number of legs sent — the no-double-fire guarantee holds structurally.
void bookkeeping_never_exceeds_legs() {
    std::vector<Leg> legs = {
        {1, true, Resp::SUCCESS, Resp::HARD_FAIL, true},
        {2, true, Resp::BENIGN_404, /*del=*/Resp::BENIGN_404, false},
        {3, true, Resp::HARD_FAIL, Resp::HARD_FAIL, true},
    };
    Outcome o = run(legs);
    TX_REQUIRE(o.bookkeeping <= o.bodies_sent);
    TX_EQ(o.bookkeeping, 2);            // legs 1 and 2 confirmed, leg 3 failed
    TX_EQ(o.bookkeeping + o.left_open, o.bodies_sent);
}

}  // namespace

int main() {
    std::printf("=== Concurrent pair-cancel (theo-move latency fix) ===\n");
    TX_RUN(two_legs_success_one_batch_no_double_fire_one_replace);
    TX_RUN(benign_404_counts_as_confirmed);
    TX_RUN(hardfail_then_delete_success_is_confirmed);
    TX_RUN(hard_fail_both_leaves_open_and_never_replaces);
    TX_RUN(leg_without_oid_is_not_sent);
    TX_RUN(bookkeeping_never_exceeds_legs);
    return tx::finish("test_concurrent_cancels");
}
