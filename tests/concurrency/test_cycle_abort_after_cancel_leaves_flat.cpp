// =============================================================================
// CANCEL-FIRST theo-move reorder — reconcile abort after cancel leaves flat
// =============================================================================
//
// Directive: cancels must not be conditioned on any REST result. If the
// reconcile gate later times out or the venue reconcile aborts, we are simply
// not quoting — the legs are already canceled, which is the intended safe state.
// The re-add cycle logs MM_CYCLE_ABORT_AFTER_CANCEL when this happens.
//
// Production wiring (MakeMarketStrategy.cpp): the cancel-first drain sets
// mm_tt_cancel_sent_ms_ when the cancels are enqueued. The re-add cycle
// (runFullMmQuoteCycle) calls mmReconcileVenueOrAbort; if it returns false and
// mm_tt_cancel_sent_ms_ > 0, it logs [MM_CYCLE_ABORT_AFTER_CANCEL], clears the
// MM_TT stamps, and returns WITHOUT placing.
//
// This test models that control flow and asserts: (a) the cancels already went
// out, (b) no place op is emitted on abort, (c) the abort is logged exactly once.

#include "test_helpers.h"

#include <string>
#include <vector>

namespace {

std::vector<std::string> g_tape;
long long g_cancel_sent_stamp = 0;   // mirrors mm_tt_cancel_sent_ms_

// Model of processTrackedOrdersTheoMove: cancels both legs, stamps cancel_sent.
int cancelFirst(int adopted_legs) {
    int sent = 0;
    for (int i = 0; i < adopted_legs; ++i) {
        g_tape.push_back("CANCEL");
        ++sent;
    }
    g_cancel_sent_stamp = 1;  // non-zero → "we cancelled this cycle-group"
    return sent;
}

// Model of runFullMmQuoteCycle's reconcile-abort branch.
// Returns true if it proceeded to placement, false if it aborted after cancel.
bool reAddCycle(bool venue_reconcile_ok) {
    g_tape.push_back("GATE_WAIT");
    if (!venue_reconcile_ok) {
        if (g_cancel_sent_stamp > 0) {
            g_tape.push_back("MM_CYCLE_ABORT_AFTER_CANCEL");
        }
        g_cancel_sent_stamp = 0;  // clear stamps on abort
        return false;             // NO place
    }
    g_tape.push_back("PLACE");
    g_tape.push_back("PLACE");
    g_cancel_sent_stamp = 0;
    return true;
}

int countOf(const std::string& what) {
    int n = 0;
    for (const auto& e : g_tape) {
        if (e == what) {
            ++n;
        }
    }
    return n;
}

// -----------------------------------------------------------------------------
// Scenario 1 — venue reconcile aborts the re-add: cancels stay done, no place,
// abort logged once.
// -----------------------------------------------------------------------------
void scenario_abort_after_cancel_no_place() {
    g_tape.clear();
    g_cancel_sent_stamp = 0;

    const int sent = cancelFirst(/*adopted_legs*/ 2);
    const bool placed = reAddCycle(/*venue_reconcile_ok*/ false);

    TX_EQ(sent, 2);                                 // both cancels went out
    TX_REQUIRE(!placed);                            // nothing placed
    TX_EQ(countOf("CANCEL"), 2);
    TX_EQ(countOf("PLACE"), 0);                     // left flat
    TX_EQ(countOf("MM_CYCLE_ABORT_AFTER_CANCEL"), 1);
}

// -----------------------------------------------------------------------------
// Scenario 2 — healthy reconcile: re-add places the pair; no abort log.
// -----------------------------------------------------------------------------
void scenario_reconcile_ok_places_pair() {
    g_tape.clear();
    g_cancel_sent_stamp = 0;

    (void)cancelFirst(2);
    const bool placed = reAddCycle(/*venue_reconcile_ok*/ true);

    TX_REQUIRE(placed);
    TX_EQ(countOf("PLACE"), 2);
    TX_EQ(countOf("MM_CYCLE_ABORT_AFTER_CANCEL"), 0);
}

// -----------------------------------------------------------------------------
// Scenario 3 — abort WITHOUT a preceding cancel-first (e.g. non-theo cycle):
// no MM_CYCLE_ABORT_AFTER_CANCEL noise (guarded on the cancel stamp).
// -----------------------------------------------------------------------------
void scenario_abort_without_cancel_is_silent() {
    g_tape.clear();
    g_cancel_sent_stamp = 0;   // no cancel-first happened

    const bool placed = reAddCycle(/*venue_reconcile_ok*/ false);

    TX_REQUIRE(!placed);
    TX_EQ(countOf("MM_CYCLE_ABORT_AFTER_CANCEL"), 0);
}

}  // namespace

int main() {
    std::printf("=== Cancel-first: reconcile abort after cancel leaves flat ===\n");
    TX_RUN(scenario_abort_after_cancel_no_place);
    TX_RUN(scenario_reconcile_ok_places_pair);
    TX_RUN(scenario_abort_without_cancel_is_silent);
    return ::tx::finish("test_cycle_abort_after_cancel_leaves_flat");
}
