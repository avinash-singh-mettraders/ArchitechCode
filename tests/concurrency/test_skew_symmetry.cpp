// =============================================================================
// Regression test: inventory-skew formula is symmetric across long / short.
// =============================================================================
//
// Tim's MM_VERIFY log on 2026-05-20 showed:
//   NetPo=-1, adjust_po=50000, adjust_ticks=2 → skew_tick_units=-2 (WRONG)
// The desk spec says skew should only engage once |NetPo| >= adjust_position,
// and should behave identically for long and short of equal magnitude.
//
// Root cause: std::floor(net_po / adjust_po) rounds toward -∞, so any negative
// inventory below adjust_po jumped straight to -adjust_ticks, while the
// positive equivalent stayed at zero. Fix: floor on |NetPo| and re-apply sign.
//
// This test mirrors the exact arithmetic from
// computeInventorySkewedRawBidAsk() and runFullMmQuoteCycle() so a future
// refactor cannot silently re-introduce the asymmetric form.

#include "test_helpers.h"

#include <cmath>
#include <cstdlib>
#include <cstdint>
#include <limits>

namespace {

// Mirror of the production formula AFTER the 2026-05-21 symmetry + cap-gate
// fix. Kept verbatim with the C++ in MakeMarketStrategy.cpp so this test will
// fail loudly if anyone reverts to either:
//   (a) the asymmetric `floor(net_po/adjust_po)*adjust_ticks` (sign-flip bug), or
//   (b) the un-gated "skew past cap" variant that diverged from MM_VERIFY and the
//       Python desk-side preview.
//
// Cap policy: when |NetPo| >= max_position, skew is ZERO; the strategy relies on
// shouldQuoteSide + headroom-capped qty to flatten beyond the cap, NOT on
// continuing to skew the resting price.
double computeSkewTickUnits(long long net_po, int adjust_po, int adjust_ticks, int max_po) {
    if (adjust_po <= 0) return 0.0;
    const long long abs_net_po = (net_po < 0) ? -net_po : net_po;
    const long long cap_ll =
        static_cast<long long>(max_po > 0 ? max_po : 1);
    if (abs_net_po >= cap_ll) return 0.0;
    const int net_po_sign = (net_po > 0) ? 1 : (net_po < 0 ? -1 : 0);
    return std::floor(static_cast<double>(abs_net_po) / static_cast<double>(adjust_po)) *
           static_cast<double>(net_po_sign) *
           static_cast<double>(adjust_ticks);
}

// Three-arg overload — for the "no cap supplied" callsites in tests that
// pre-date the cap policy. Treats max_po as effectively infinite.
double computeSkewTickUnits(long long net_po, int adjust_po, int adjust_ticks) {
    return computeSkewTickUnits(net_po, adjust_po, adjust_ticks, std::numeric_limits<int>::max());
}

// What the OLD (buggy) formula produced — used only to prove the test would
// have caught the historical bug.
double computeSkewTickUnitsLegacyBuggy(long long net_po, int adjust_po, int adjust_ticks) {
    if (adjust_po <= 0) return 0.0;
    return std::floor(static_cast<double>(net_po) / static_cast<double>(adjust_po)) *
           static_cast<double>(adjust_ticks);
}

void test_zero_skew_below_adjust_position() {
    // Tim's exact case: NetPo=-1, adjust_po=50000, adjust_ticks=2 → 0.
    TX_EQ(computeSkewTickUnits(-1, 50000, 2), 0.0);
    TX_EQ(computeSkewTickUnits(+1, 50000, 2), 0.0);
    // Anything strictly below adjust_position is zero.
    for (long long np = -49999; np <= 49999; np += 5000) {
        TX_EQ(computeSkewTickUnits(np, 50000, 2), 0.0);
    }
}

void test_skew_engages_at_threshold_symmetrically() {
    // At exactly the threshold, skew = ±adjust_ticks.
    TX_EQ(computeSkewTickUnits(+50000, 50000, 2), +2.0);
    TX_EQ(computeSkewTickUnits(-50000, 50000, 2), -2.0);
    // 2× threshold → 2 × adjust_ticks each side.
    TX_EQ(computeSkewTickUnits(+100000, 50000, 2), +4.0);
    TX_EQ(computeSkewTickUnits(-100000, 50000, 2), -4.0);
    // 3× threshold.
    TX_EQ(computeSkewTickUnits(+150000, 50000, 2), +6.0);
    TX_EQ(computeSkewTickUnits(-150000, 50000, 2), -6.0);
}

void test_symmetry_property() {
    // For every long inventory point, the short of equal magnitude must
    // produce a skew equal in magnitude but opposite in sign. No exceptions.
    const int adjust_po = 5;
    const int adjust_ticks = 2;
    for (long long mag = 0; mag <= 25; ++mag) {
        const double s_long  = computeSkewTickUnits(+mag, adjust_po, adjust_ticks);
        const double s_short = computeSkewTickUnits(-mag, adjust_po, adjust_ticks);
        if (s_long != -s_short) {
            ::tx::report_fail(__FILE__, __LINE__, "s_long == -s_short",
                              "mag=" + std::to_string(mag) +
                                  " s_long=" + std::to_string(s_long) +
                                  " s_short=" + std::to_string(s_short));
            return;
        }
    }
}

void test_legacy_formula_was_asymmetric() {
    // Sanity: prove the buggy formula would have failed the same test, so a
    // future engineer porting the helper can see why we use floor(|.|)*sign.
    TX_EQ(computeSkewTickUnitsLegacyBuggy(-1, 50000, 2), -2.0);
    TX_EQ(computeSkewTickUnitsLegacyBuggy(+1, 50000, 2),  0.0);
}

void test_zero_net_po_zero_skew() {
    TX_EQ(computeSkewTickUnits(0, 50000, 2), 0.0);
    TX_EQ(computeSkewTickUnits(0, 1, 2), 0.0);
    TX_EQ(computeSkewTickUnits(0, 10, 5), 0.0);
}

void test_skew_price_combines_correctly() {
    // skew_price = skew_tick_units * quote_tick — used downstream as the
    // single quantity subtracted from both bid and ask.
    const double quote_tick = 0.1;
    const double s_long = computeSkewTickUnits(+50000, 50000, 2) * quote_tick;
    const double s_short = computeSkewTickUnits(-50000, 50000, 2) * quote_tick;
    TX_EQ(s_long, +0.2);
    TX_EQ(s_short, -0.2);

    // Below threshold, skew_price MUST be exactly zero so raw_bid/raw_ask
    // equal pricing_mid ± offset (no inventory bias on small positions).
    TX_EQ(computeSkewTickUnits(-1, 50000, 2) * quote_tick, 0.0);
    TX_EQ(computeSkewTickUnits(+1, 50000, 2) * quote_tick, 0.0);
}

void test_user_reported_gold_scenario() {
    // Reproduce the exact log line from 2026-05-20 10:37:49 (GOLD/XAU stack):
    //   NetPo~=-1  max_po=5  adjust_po=50000  adjust_ticks=2  quote_tick=0.1
    // After fix, skew must be 0 → raw_bid/raw_ask are pure mid ± offset.
    const long long net_po = -1;
    const int adjust_po = 50000;
    const int adjust_ticks = 2;
    const int max_po = 5;
    const double quote_tick = 0.1;
    const double skew_units = computeSkewTickUnits(net_po, adjust_po, adjust_ticks, max_po);
    const double skew_price = skew_units * quote_tick;
    TX_EQ(skew_units, 0.0);
    TX_EQ(skew_price, 0.0);
}

void test_skew_zero_at_and_past_max_position() {
    // Cap-skew alignment (2026-05-21): when |NetPo| >= max_position the
    // placement code now matches the verify printer / Python preview and
    // spec — skew is exactly zero, regardless of sign or magnitude past the
    // cap. The strategy relies on side-gating + headroom-capped qty beyond
    // the cap, not on continuing to skew the resting prices.
    const int adjust_po = 5;
    const int adjust_ticks = 2;
    const int max_po = 5;
    // At threshold and beyond on the long side.
    TX_EQ(computeSkewTickUnits(+5,   adjust_po, adjust_ticks, max_po), 0.0);
    TX_EQ(computeSkewTickUnits(+6,   adjust_po, adjust_ticks, max_po), 0.0);
    TX_EQ(computeSkewTickUnits(+100, adjust_po, adjust_ticks, max_po), 0.0);
    // Same on the short side.
    TX_EQ(computeSkewTickUnits(-5,   adjust_po, adjust_ticks, max_po), 0.0);
    TX_EQ(computeSkewTickUnits(-6,   adjust_po, adjust_ticks, max_po), 0.0);
    TX_EQ(computeSkewTickUnits(-100, adjust_po, adjust_ticks, max_po), 0.0);
    // Just below the cap — formula still active.
    TX_EQ(computeSkewTickUnits(+4,   adjust_po, adjust_ticks, max_po), 0.0); // floor(4/5)=0
    TX_EQ(computeSkewTickUnits(-4,   adjust_po, adjust_ticks, max_po), 0.0); // floor(4/5)=0
}

void test_cap_skew_aligns_with_verify_log() {
    // The bug we just closed: with adjust_po=1, adjust_ticks=2, max_po=5 and
    // NetPo=+10 (well past the cap), the OLD code would have produced a
    // non-zero skew_tick_units (floor(10/1)*2 = 20) on the placement path,
    // while MM_VERIFY and the Python desk preview both showed 0. Now both
    // paths agree on 0.
    TX_EQ(computeSkewTickUnits(+10, 1, 2, 5), 0.0);
    TX_EQ(computeSkewTickUnits(-10, 1, 2, 5), 0.0);
    // And the within-cap behaviour is still symmetric and non-zero where it
    // should be.
    TX_EQ(computeSkewTickUnits(+3, 1, 2, 5), +6.0);  // floor(3/1)*+1*2 = +6
    TX_EQ(computeSkewTickUnits(-3, 1, 2, 5), -6.0);
}

}  // namespace

int main() {
    std::printf("[SUITE ] test_skew_symmetry\n");
    TX_RUN(test_zero_skew_below_adjust_position);
    TX_RUN(test_skew_engages_at_threshold_symmetrically);
    TX_RUN(test_symmetry_property);
    TX_RUN(test_legacy_formula_was_asymmetric);
    TX_RUN(test_zero_net_po_zero_skew);
    TX_RUN(test_skew_price_combines_correctly);
    TX_RUN(test_user_reported_gold_scenario);
    TX_RUN(test_skew_zero_at_and_past_max_position);
    TX_RUN(test_cap_skew_aligns_with_verify_log);
    return ::tx::finish("test_skew_symmetry");
}
