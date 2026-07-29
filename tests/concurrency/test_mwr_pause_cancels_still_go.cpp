// =============================================================================
// CANCEL-FIRST theo-move reorder — MWR / fast-market pause suppresses PLACEMENT
// only; cancels still go (2026-07-03)
// =============================================================================
//
// Directive constraint: MWR / fast-market pause must still suppress PLACEMENT
// only; the cancels in the cancel-first step must run even while paused (matches
// existing Item G "pure suppression" semantics).
//
// Production wiring (MakeMarketStrategy.cpp):
//   * processTrackedOrdersTheoMove has NO MWR gate — it enqueues cancels
//     unconditionally (cancels/reconcile are never gated, only placement).
//   * The MWR breach path itself PULLS legs via mmCancelTrackedLegThisStackGateway
//     (runFullMmQuoteCycle :9525-9526) — cancels during volatility.
//   * deskTheoMoveAfterCancelAckPlaceOneLeg (:1995) and runFullMmQuoteCycle's
//     placement (FastMarketMonitor::isGloballyPausedNow :9478, MWR kPausedSkip
//     :9515) return BEFORE placing when paused → the survivor leg stays pulled.
//
// This test models a paused breaker and asserts: cancels are emitted, places are
// suppressed.

#include "test_helpers.h"

#include <string>
#include <vector>

namespace {

std::vector<std::string> g_tape;

// Cancel step — never gated by the breaker.
int cancelFirst(int adopted_legs, bool /*mwr_paused*/) {
    int sent = 0;
    for (int i = 0; i < adopted_legs; ++i) {
        g_tape.push_back("CANCEL");
        ++sent;
    }
    return sent;
}

// Place step — suppressed while paused (pure suppression).
int placeReAdd(int want_legs, bool mwr_paused) {
    if (mwr_paused) {
        return 0;  // deskTheoMoveAfterCancelAckPlaceOneLeg / cycle returns before place
    }
    int placed = 0;
    for (int i = 0; i < want_legs; ++i) {
        g_tape.push_back("PLACE");
        ++placed;
    }
    return placed;
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
// Scenario 1 — breaker paused: cancels still go, no placement.
// -----------------------------------------------------------------------------
void scenario_paused_cancels_go_no_place() {
    g_tape.clear();
    const bool paused = true;

    const int sent = cancelFirst(/*adopted_legs*/ 2, paused);
    const int placed = placeReAdd(/*want_legs*/ 2, paused);

    TX_EQ(sent, 2);
    TX_EQ(placed, 0);
    TX_EQ(countOf("CANCEL"), 2);
    TX_EQ(countOf("PLACE"), 0);
}

// -----------------------------------------------------------------------------
// Scenario 2 — breaker not paused: cancels then places (normal cancel-first).
// -----------------------------------------------------------------------------
void scenario_not_paused_cancel_then_place() {
    g_tape.clear();
    const bool paused = false;

    const int sent = cancelFirst(2, paused);
    const int placed = placeReAdd(2, paused);

    TX_EQ(sent, 2);
    TX_EQ(placed, 2);
    // Ordering: both cancels appear before the first place.
    int first_place = -1, last_cancel = -1;
    for (int i = 0; i < static_cast<int>(g_tape.size()); ++i) {
        if (g_tape[i] == "CANCEL") {
            last_cancel = i;
        }
        if (g_tape[i] == "PLACE" && first_place < 0) {
            first_place = i;
        }
    }
    TX_REQUIRE(last_cancel >= 0 && first_place >= 0);
    TX_REQUIRE(last_cancel < first_place);
}

}  // namespace

int main() {
    std::printf("=== Cancel-first: MWR pause suppresses placement only ===\n");
    TX_RUN(scenario_paused_cancels_go_no_place);
    TX_RUN(scenario_not_paused_cancel_then_place);
    return ::tx::finish("test_mwr_pause_cancels_still_go");
}
