// =============================================================================
// Combined cancel-replace: the wire cancel must fire EXACTLY ONCE per leg
// (the cancels=4-vs-2 double-fire fix, 2026-07-13).
//
// Bug: a combined pair job (cancelThenPlaceOrdersConcurrent) sends 2 cancels +
// 2 places on the wire and counts them once each (correct). But
// Platform::dispatchCancelResponses then called orders()->cancelOrder(id) to
// sync local state, which published ORDER_CANCELLED — and the Platform
// wire-cancel handler treats every ORDER_CANCELLED as a fresh "please cancel"
// and re-sends cancelOrderGateway(). That re-send both re-hit the venue AND
// re-bumped MM_JOB_VENUE_CALLS cancels → the job reported cancels=4 instead of 2.
//
// Fix: onOrderCancelled() publishes ORDER_CANCELLED with venue_confirmed=true,
// and the Platform handler skips the wire send for venue_confirmed cancels.
// dispatchCancelResponses now calls onOrderCancelled() (a "venue already did it"
// notification), not cancelOrder() (a "please cancel" request).
//
// This test models that exact flow using the REAL per-job counter header
// (core/MmJobVenueCalls.h) — the same idiom as test_enforce_zero_venue_reads —
// so the counting logic under test is production's. The negative control drives
// the OLD (venue_confirmed=false) behavior and proves it WOULD double-fire.
// =============================================================================

#include "test_helpers.h"

#include "core/MmJobVenueCalls.h"

#include <string>
#include <vector>

using architect::core::mmJobVenueBegin;
using architect::core::mmJobVenueEnd;
using architect::core::mmJobVenueSnapshot;
using architect::core::mmJobVenueNoteCancel;
using architect::core::mmJobVenueNotePost;

namespace {

// Models api::RestClient at the venue-call sites. Each call bumps the REAL
// thread-local job counter exactly where production's RestClient does.
struct ModelRest {
    // cancelThenPlaceOrdersConcurrent(): one noteCancel per cancel body, one
    // notePost per place body — mirrors RestClient.cpp lines 1024 / 1029.
    void cancelThenPlaceOrdersConcurrent(std::size_t n_cancel, std::size_t n_place) {
        for (std::size_t i = 0; i < n_cancel; ++i) mmJobVenueNoteCancel();
        for (std::size_t i = 0; i < n_place; ++i) mmJobVenueNotePost();
    }
    // cancelOrderGateway(): one venue cancel — mirrors RestClient.cpp line 892.
    void cancelOrderGateway() { mmJobVenueNoteCancel(); }
};

// Models the Platform ORDER_CANCELLED wire-cancel handler. It re-sends a wire
// cancel ONLY for a fresh "please cancel" (venue_confirmed=false). For a
// venue-confirmed notification it must return early (the fix).
void wireCancelHandler(ModelRest& rest, bool venue_confirmed) {
    if (venue_confirmed) {
        return;  // venue already cancelled — nothing to send (the fix)
    }
    rest.cancelOrderGateway();
}

// Models Platform::dispatchCancelResponses: for each cancelled leg, sync local
// state by publishing ORDER_CANCELLED with the given venue_confirmed flag, which
// synchronously drives wireCancelHandler (ORDER_CANCELLED is dispatched sync).
void dispatchCancelResponses(ModelRest& rest, std::size_t n_legs, bool venue_confirmed) {
    for (std::size_t i = 0; i < n_legs; ++i) {
        wireCancelHandler(rest, venue_confirmed);
    }
}

// One combined pair mover job: the batch (2 cancels + 2 places) followed by the
// local-state sync for the 2 cancelled legs.
architect::core::MmJobVenueCounters combinedPairJob(bool sync_venue_confirmed) {
    ModelRest rest;
    mmJobVenueBegin();
    // Combined c,c,p,p batch — the one true wire send for the cancels + places.
    rest.cancelThenPlaceOrdersConcurrent(/*n_cancel=*/2, /*n_place=*/2);
    // Local-state sync for the two cancelled legs (venue already did it).
    dispatchCancelResponses(rest, /*n_legs=*/2, sync_venue_confirmed);
    const auto c = mmJobVenueSnapshot();
    mmJobVenueEnd();
    return c;
}

// ---------------------------------------------------------------------------
// 1. FIXED path: dispatchCancelResponses syncs via venue_confirmed=true, so the
//    wire handler skips the re-send. A combined pair job reports gets=0,
//    posts=2, cancels=2 — the spec contract.
// ---------------------------------------------------------------------------
void combined_pair_job_reports_two_cancels() {
    const auto c = combinedPairJob(/*sync_venue_confirmed=*/true);
    TX_EQ(c.gets, 0L);
    TX_EQ(c.posts, 2L);
    TX_EQ(c.cancels, 2L);  // exactly two wire cancels — no double-fire
}

// ---------------------------------------------------------------------------
// 2. Negative control: the OLD behavior (venue_confirmed=false → handler
//    re-sends per leg) WOULD report cancels=4. This proves the guard has teeth
//    and that the fix is what collapses 4 → 2.
// ---------------------------------------------------------------------------
void unguarded_sync_double_fires_to_four() {
    const auto c = combinedPairJob(/*sync_venue_confirmed=*/false);
    TX_EQ(c.gets, 0L);
    TX_EQ(c.posts, 2L);
    TX_EQ(c.cancels, 4L);  // the bug: 2 (batch) + 2 (re-sent by handler)
}

// ---------------------------------------------------------------------------
// 3. The legacy "please cancel" path (a strategy-initiated cancel with NO prior
//    wire send) MUST still fire the wire cancel — the fix must not silence it.
// ---------------------------------------------------------------------------
void legacy_please_cancel_still_fires() {
    ModelRest rest;
    mmJobVenueBegin();
    // cancelOrder() publishes ORDER_CANCELLED with venue_confirmed=false.
    wireCancelHandler(rest, /*venue_confirmed=*/false);
    const auto c = mmJobVenueSnapshot();
    mmJobVenueEnd();
    TX_EQ(c.cancels, 1L);  // the wire send happened — legacy path preserved
}

}  // namespace

int main() {
    TX_RUN(combined_pair_job_reports_two_cancels);
    TX_RUN(unguarded_sync_double_fires_to_four);
    TX_RUN(legacy_please_cancel_still_fires);
    return tx::finish("combined_cancel_no_double_fire");
}
