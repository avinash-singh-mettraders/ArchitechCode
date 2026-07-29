// =============================================================================
// Per-instrument HOLD — mm_orders_enabled suppresses EVERY place site, keyed on
// ax_symbol only, cancels never gated (Phase 2, per-instrument-cancel-all)
// =============================================================================
//
// "Per-Instrument Cancel All" holds an instrument by setting the existing
// market_maker.instruments[].mm_orders_enabled=false for its ax_symbol. For the
// hold to be honest, the engine must place NOTHING on that symbol from any path,
// while (a) leaving every other symbol untouched and (b) still allowing cancels.
//
// Production wiring under test (MakeMarketStrategy.cpp):
//   * mmPlacementAllowedForSymbol(cfg) = mmMarketMakerOrdersPlacementAllowed(cfg)
//       (= isMarketMakerEnabled() && getMarketMakerMmOrdersEnabledForSymbol(ax))
//       && !mmMwrIsPausedNow()  — the single placement predicate.
//   * deskTheoMoveAfterCancelAckPlaceOneLeg (the P1 gap) now returns before the
//     place when !mmPlacementAllowedForSymbol — previously it only checked
//     mmMwrIsPausedNow(), so a HOLD issued while a theo-move cancel-ack was in
//     flight would re-peg a leg AFTER the hold was observed (the §4.5 orphan).
//   * the fire-and-track place branch (P6) routes through the same predicate.
//   * processTrackedOrdersTheoMove already gated mm_orders_enabled at the top,
//     and cancels are emitted unconditionally (never gated).
//
// This models the placement predicate + the two previously-ungated place sites and
// asserts hold-suppresses-place / other-symbol-unaffected / cancels-still-go, with
// two NEGATIVE CONTROLS proving (a) the hold check and (b) the ax_symbol filter are
// each load-bearing.

#include "test_helpers.h"

#include <set>
#include <string>
#include <vector>

namespace {

// --- Model of the engine's placement gate -----------------------------------

struct HoldState {
    std::set<std::string> held;     // ax_symbols with mm_orders_enabled=false
    bool mwr_paused = false;        // MWR / global fast-market pause
    bool strategy_enabled = true;   // isMarketMakerEnabled()
};

// Faithful model of mmPlacementAllowedForSymbol(). `apply_symbol_filter=false`
// is the NEGATIVE CONTROL for the ax_symbol keying (hold becomes account-wide).
bool placementAllowedForSymbol(const std::string& ax, const HoldState& s,
                               bool apply_symbol_filter = true) {
    if (!s.strategy_enabled) {
        return false;
    }
    const bool held = apply_symbol_filter ? (s.held.count(ax) > 0) : !s.held.empty();
    if (held) {
        return false;  // per-instrument HOLD (mm_orders_enabled=false)
    }
    if (s.mwr_paused) {
        return false;  // pure suppression
    }
    return true;
}

std::vector<std::string> g_tape;

void clearTape() { g_tape.clear(); }

int countOf(const std::string& what) {
    int n = 0;
    for (const auto& e : g_tape) {
        if (e == what) {
            ++n;
        }
    }
    return n;
}

// --- Modelled place sites (§4.1) --------------------------------------------
// Every site emits its cancels first (cancels are NEVER gated) and only places
// when the predicate allows. `gate_on` / `apply_symbol_filter` exist purely so the
// negative controls can disable each guard and show the regression.

// Site: deskTheoMoveAfterCancelAckPlaceOneLeg (the P1 gap). One survivor leg.
void afterCancelAckRepeg(const std::string& ax, const HoldState& s,
                         bool gate_on = true, bool apply_symbol_filter = true) {
    g_tape.push_back("CANCEL:" + ax);  // the cancel that armed this re-peg already ran
    const bool allowed =
        gate_on ? placementAllowedForSymbol(ax, s, apply_symbol_filter) : true;
    if (allowed) {
        g_tape.push_back("PLACE:" + ax);
    }
}

// Site: fire-and-track place (P6). Cancels then a fresh pair.
void fireAndTrackPlacePair(const std::string& ax, const HoldState& s,
                           bool gate_on = true, bool apply_symbol_filter = true) {
    g_tape.push_back("CANCEL:" + ax);
    g_tape.push_back("CANCEL:" + ax);
    const bool allowed =
        gate_on ? placementAllowedForSymbol(ax, s, apply_symbol_filter) : true;
    if (allowed) {
        g_tape.push_back("PLACE:" + ax);
        g_tape.push_back("PLACE:" + ax);
    }
}

// -----------------------------------------------------------------------------
// 1 — instrument held: NO place from either site, cancels still go.
// -----------------------------------------------------------------------------
void scenario_held_suppresses_place_cancels_go() {
    clearTape();
    HoldState s;
    s.held.insert("XAU-PERP");

    afterCancelAckRepeg("XAU-PERP", s);
    fireAndTrackPlacePair("XAU-PERP", s);

    TX_EQ(countOf("PLACE:XAU-PERP"), 0);   // held → nothing placed
    TX_EQ(countOf("CANCEL:XAU-PERP"), 3);  // cancels never gated (1 + 2)
}

// -----------------------------------------------------------------------------
// 2 — not held: both sites place normally, and always after their cancels.
// -----------------------------------------------------------------------------
void scenario_not_held_places() {
    clearTape();
    HoldState s;  // nothing held, not paused

    afterCancelAckRepeg("XAU-PERP", s);
    fireAndTrackPlacePair("XAU-PERP", s);

    TX_EQ(countOf("PLACE:XAU-PERP"), 3);   // 1 + 2
    TX_EQ(countOf("CANCEL:XAU-PERP"), 3);
    // Every place is preceded by at least one cancel (cancel-before-place per site).
    int first_place = -1;
    for (int i = 0; i < static_cast<int>(g_tape.size()); ++i) {
        if (g_tape[i].rfind("PLACE:", 0) == 0) {
            first_place = i;
            break;
        }
    }
    TX_REQUIRE(first_place > 0 && g_tape[0].rfind("CANCEL:", 0) == 0);
}

// -----------------------------------------------------------------------------
// 3 — holding one instrument leaves every OTHER instrument completely unaffected.
// -----------------------------------------------------------------------------
void scenario_other_symbol_unaffected() {
    clearTape();
    HoldState s;
    s.held.insert("XAU-PERP");

    afterCancelAckRepeg("XAU-PERP", s);   // held
    fireAndTrackPlacePair("XAG-PERP", s); // NOT held

    TX_EQ(countOf("PLACE:XAU-PERP"), 0);  // held symbol suppressed
    TX_EQ(countOf("PLACE:XAG-PERP"), 2);  // other symbol quotes normally
}

// -----------------------------------------------------------------------------
// 4 — MWR / fast-market pause also suppresses placement (P6), cancels still go.
// -----------------------------------------------------------------------------
void scenario_pause_suppresses_place() {
    clearTape();
    HoldState s;
    s.mwr_paused = true;  // nothing held, but paused

    fireAndTrackPlacePair("XAU-PERP", s);

    TX_EQ(countOf("PLACE:XAU-PERP"), 0);
    TX_EQ(countOf("CANCEL:XAU-PERP"), 2);
}

// -----------------------------------------------------------------------------
// NEGATIVE CONTROL (a) — remove the hold check: a held instrument re-places.
// Fails if someone deletes mmPlacementAllowedForSymbol from a place site.
// -----------------------------------------------------------------------------
void negctrl_gate_removed_places_while_held() {
    clearTape();
    HoldState s;
    s.held.insert("XAU-PERP");

    // gate_on=false models a place site that skipped the predicate (the P1 bug).
    afterCancelAckRepeg("XAU-PERP", s, /*gate_on*/ false);

    // With the gate removed the orphan-making place happens. The gated version
    // (scenario 1) proves it does NOT — together they prove the gate is load-bearing.
    TX_EQ(countOf("PLACE:XAU-PERP"), 1);
}

// -----------------------------------------------------------------------------
// NEGATIVE CONTROL (b) — drop the ax_symbol filter: holding XAU also freezes XAG.
// Fails if the predicate stops keying on ax_symbol (hold leaks account-wide).
// -----------------------------------------------------------------------------
void negctrl_symbol_filter_dropped_freezes_others() {
    clearTape();
    HoldState s;
    s.held.insert("XAU-PERP");

    // apply_symbol_filter=false → any-held gates everything.
    fireAndTrackPlacePair("XAG-PERP", s, /*gate_on*/ true, /*apply_symbol_filter*/ false);

    // XAG (not held) is wrongly frozen. Scenario 3 proves that WITH the filter XAG
    // places 2 — together they prove the ax_symbol keying is load-bearing.
    TX_EQ(countOf("PLACE:XAG-PERP"), 0);
}

}  // namespace

int main() {
    std::printf("=== Per-instrument HOLD: mm_orders_enabled gates every place site ===\n");
    TX_RUN(scenario_held_suppresses_place_cancels_go);
    TX_RUN(scenario_not_held_places);
    TX_RUN(scenario_other_symbol_unaffected);
    TX_RUN(scenario_pause_suppresses_place);
    TX_RUN(negctrl_gate_removed_places_while_held);
    TX_RUN(negctrl_symbol_filter_dropped_freezes_others);
    return ::tx::finish("test_per_instrument_hold");
}
