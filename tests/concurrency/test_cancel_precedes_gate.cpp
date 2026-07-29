// =============================================================================
// CANCEL-FIRST theo-move reorder — cancel precedes the gate (2026-07-03)
// =============================================================================
//
// Client-approved directive: on a theo move the cancel request must leave the
// box IMMEDIATELY. All waits/checks (mmWaitReconcileGateBeforePlace,
// mmReconcileVenueOrAbort, NetPo snapshot, shouldQuoteSide) gate ONLY the
// re-add (place), not the cancel.
//
// Production wiring (MakeMarketStrategy.cpp):
//   * feed drift → onFeedUpdate stamps mm_tt_feed_arrival_ms_ and enqueues
//     "theo_move" on the mover.
//   * mmDrainPendingQuoteCycle routes desk "theo_move" through
//     processTrackedOrdersTheoMove FIRST → enqueueRestCancelOnMover for each
//     stale leg (no gate/reconcile/NetPo read ahead of the cancel).
//   * on_cancel (TheoMove intent) re-enqueues "theo_move"; the re-add drain now
//     finds no ADOPTED legs → declines → runFullMmQuoteCycle runs the gate +
//     reconcile + NetPo checks and THEN places.
//
// This test models the ordered sequence of side-effects a single cancel-first
// requote emits and asserts the CANCEL op is recorded strictly before the first
// gate-wait begins. It mirrors the production shape without linking the full
// MakeMarketStrategy machinery (same convention as test_reconcile_gate.cpp).

#include "test_helpers.h"

#include <string>
#include <vector>

namespace {

// Ordered event tape the model appends to as it walks a requote.
std::vector<std::string> g_tape;

enum class LegState { Adopted, CancelPending, Cancelled, PlacePending };

struct Leg {
    LegState state{LegState::Adopted};
    bool     drifted{false};
};

// --- processTrackedOrdersTheoMove model: cancel-first, no gate ---------------
// Returns true if it sent cancels (steady-state adopted+drifted pair).
bool processTrackedOrdersTheoMove(std::vector<Leg>& legs) {
    bool any_drifted = false;
    for (const auto& l : legs) {
        if (l.state == LegState::Adopted && l.drifted) {
            any_drifted = true;
            break;
        }
    }
    if (!any_drifted) {
        return false;  // decline → caller falls through to full cycle
    }
    // Pair repricing: cancel BOTH adopted legs from one theo snapshot.
    for (auto& l : legs) {
        if (l.state == LegState::Adopted) {
            l.state = LegState::CancelPending;
            g_tape.push_back("CANCEL");  // enqueueRestCancelOnMover
        }
    }
    return true;
}

// --- runFullMmQuoteCycle model: gate → reconcile → NetPo → place ------------
void runFullMmQuoteCycle(std::vector<Leg>& legs) {
    g_tape.push_back("GATE_WAIT");        // mmWaitReconcileGateBeforePlace
    g_tape.push_back("VENUE_RECONCILE");  // mmReconcileVenueOrAbort
    g_tape.push_back("NETPO_READ");       // position_state_ snapshot + shouldQuoteSide
    for (auto& l : legs) {
        if (l.state != LegState::Adopted && l.state != LegState::CancelPending) {
            l.state = LegState::PlacePending;
            g_tape.push_back("PLACE");
        }
    }
}

// --- mmDrainPendingQuoteCycle model: route desk theo_move cancel-first ------
void drainDeskTheoMove(std::vector<Leg>& legs) {
    if (processTrackedOrdersTheoMove(legs)) {
        return;  // cancels enqueued; re-add follows on cancel-ack
    }
    runFullMmQuoteCycle(legs);
}

// Simulate the cancel-ack → on_cancel re-enqueue → re-add drain.
void deliverCancelAcksAndReadd(std::vector<Leg>& legs) {
    for (auto& l : legs) {
        if (l.state == LegState::CancelPending) {
            l.state = LegState::Cancelled;  // on_cancel CANCELLED
        }
    }
    drainDeskTheoMove(legs);  // re-add: processTrackedOrdersTheoMove declines → full cycle
}

int indexOf(const std::string& what) {
    for (int i = 0; i < static_cast<int>(g_tape.size()); ++i) {
        if (g_tape[i] == what) {
            return i;
        }
    }
    return -1;
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
// Scenario 1 — steady-state theo move: both cancels are recorded before the
// first gate wait; two places happen only after both cancels + the gate.
// -----------------------------------------------------------------------------
void scenario_cancel_recorded_before_gate() {
    g_tape.clear();
    std::vector<Leg> legs = {{LegState::Adopted, true}, {LegState::Adopted, true}};

    drainDeskTheoMove(legs);           // cancel-first
    deliverCancelAcksAndReadd(legs);   // ack + gated re-add

    const int first_cancel = indexOf("CANCEL");
    const int first_gate = indexOf("GATE_WAIT");
    const int first_place = indexOf("PLACE");

    TX_REQUIRE(first_cancel >= 0);
    TX_REQUIRE(first_gate >= 0);
    TX_REQUIRE(first_place >= 0);
    // The whole point: cancel leaves before any gate wait begins.
    TX_REQUIRE(first_cancel < first_gate);
    // And the gate still gates the place (place after gate).
    TX_REQUIRE(first_gate < first_place);
    // Exactly two cancels (pair) and two places (pair).
    TX_EQ(countOf("CANCEL"), 2);
    TX_EQ(countOf("PLACE"), 2);
}

// -----------------------------------------------------------------------------
// Scenario 2 — no gate/reconcile/netpo op precedes the first cancel at all.
// -----------------------------------------------------------------------------
void scenario_no_prewait_before_first_cancel() {
    g_tape.clear();
    std::vector<Leg> legs = {{LegState::Adopted, true}, {LegState::Adopted, true}};

    drainDeskTheoMove(legs);
    deliverCancelAcksAndReadd(legs);

    const int first_cancel = indexOf("CANCEL");
    TX_REQUIRE(first_cancel >= 0);
    // Nothing gate-ish appears before the first cancel.
    for (int i = 0; i < first_cancel; ++i) {
        TX_REQUIRE(g_tape[i] != "GATE_WAIT");
        TX_REQUIRE(g_tape[i] != "VENUE_RECONCILE");
        TX_REQUIRE(g_tape[i] != "NETPO_READ");
    }
}

}  // namespace

int main() {
    std::printf("=== Cancel-first theo-move: cancel precedes gate ===\n");
    TX_RUN(scenario_cancel_recorded_before_gate);
    TX_RUN(scenario_no_prewait_before_first_cancel);
    return ::tx::finish("test_cancel_precedes_gate");
}
