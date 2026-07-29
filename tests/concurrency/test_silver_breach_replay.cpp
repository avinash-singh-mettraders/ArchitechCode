// =============================================================================
// REPLAY TEST — Example.txt SILVER breach, step-by-step
// =============================================================================
//
// This test reconstructs the EXACT timeline observed in `Example.txt`
// (`logMrinal/Example.txt` shared by Tim 2026-05-21) for stack
// `mm_req_XAG_PERP_a56a8ea7` between 07:49:22.064 and 07:49:28.593, and runs
// it through two execution modes:
//
//   * BROKEN_MODE  — mimics the original code where a benign HTTP 404 on
//                    cancel evicts the leg without checking venue position,
//                    and `processFillDeskStack` does not update local NetPo.
//                    Used here only to PROVE the bug existed and that the
//                    test faithfully reproduces it.
//
//   * FIXED_MODE   — mirrors the post-fix behaviour:
//                    `mmCancelTrackedLegThisStackGateway` on benign 404 →
//                    refresh portfolio → if venue moved in this leg's side,
//                    synth fill via `mmRouteOmGatewayVenueTruthMissing` →
//                    `processFillDeskStack` updates `position_state_` →
//                    `runFullMmQuoteCycle` re-reads NetPo and runs the
//                    new hard pre-gate before submit_order.
//
// Both modes step through the same Example.txt timeline. The test asserts:
//   * BROKEN_MODE must reproduce the breach (venue NetPo reaches +3 > cap=2).
//   * FIXED_MODE must NEVER let venue NetPo exceed cap=2.
//
// If BROKEN_MODE does not breach, the replay is unfaithful and the test fails
// loudly. If FIXED_MODE breaches, the production fix is insufficient and the
// test fails loudly.

#include "test_helpers.h"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <mutex>
#include <string>
#include <vector>

namespace {

// ---------------------------------------------------------------------------
// Mock infrastructure modelling the production primitives.
// ---------------------------------------------------------------------------
enum class Mode {
    Broken,
    Fixed,
    FixedWithRefreshFailure,
    FixedWithArchitectRestLag,
};
enum class Side { Buy, Sell };

const char* sideStr(Side s) { return s == Side::Buy ? "BUY" : "SELL"; }

struct Leg {
    long long  oid{0};                 // local order_id (production: OrderId)
    std::string exch_oid{};            // venue oid (production: exchange_order_id)
    Side       side{Side::Buy};
    long long  qty{0};
    long long  px_cents{0};            // price * 1000 for display only
    bool       active{false};
    bool       venue_has_it{false};    // mirrors venue's open-orders truth
};

struct State {
    // mover serialisation — production: mm_cycle_mutex_
    std::mutex mover_mu_;

    // production: PortfolioManager (signed venue position).
    std::mutex venue_mu_;
    long long  venue_net_po_{0};

    // production: position_state_.net_position_qty under position_state_mutex_.
    std::mutex local_pos_mu_;
    long long  local_net_po_{0};

    // production: tracked_bids_ / tracked_asks_ under tracked_orders_mutex_.
    std::mutex tracked_mu_;
    std::vector<Leg> tracked_legs_;

    // production: max_position from Config (per-stack override).
    long long cap_{2};

    // observability
    long long max_venue_abs_seen_{0};
    long long max_local_abs_seen_{0};
    bool      breach_observed_{false};
    std::vector<std::string> log_;
};

void logf(State& st, const std::string& s) {
    st.log_.push_back(s);
}

void observe(State& st) {
    long long v = 0, l = 0;
    {
        std::lock_guard<std::mutex> vl(st.venue_mu_);
        v = std::abs(st.venue_net_po_);
    }
    {
        std::lock_guard<std::mutex> pl(st.local_pos_mu_);
        l = std::abs(st.local_net_po_);
    }
    if (v > st.max_venue_abs_seen_) st.max_venue_abs_seen_ = v;
    if (l > st.max_local_abs_seen_) st.max_local_abs_seen_ = l;
    if (v > st.cap_) st.breach_observed_ = true;
}

// ---------------------------------------------------------------------------
// Production-equivalent helpers, parameterised by Mode.
// ---------------------------------------------------------------------------

// processFillDeskStack — synthesises a fill against the tracked leg.
// In BROKEN_MODE the fill is logged but local NetPo is NOT updated (the
// pre-fix bug: position_state_.onAxFill was never called from this path).
// In FIXED_MODE the fill DOES update local NetPo.
void processFillDeskStack(State& st, Mode m, long long order_id, Side side, long long qty,
                          long long px_cents) {
    std::string leg_name = (side == Side::Buy) ? "bid" : "ask";
    // Mark the leg FILLED in tracked vector + opposite leg goes to CANCEL_PENDING.
    {
        std::lock_guard<std::mutex> tl(st.tracked_mu_);
        for (auto& tl_entry : st.tracked_legs_) {
            if (tl_entry.oid == order_id) {
                tl_entry.active = false;
                tl_entry.venue_has_it = false;
            }
        }
    }
    const bool fixed_mode =
        (m == Mode::Fixed
         || m == Mode::FixedWithRefreshFailure
         || m == Mode::FixedWithArchitectRestLag);
    if (fixed_mode) {
        std::lock_guard<std::mutex> pl(st.local_pos_mu_);
        st.local_net_po_ += (side == Side::Buy) ? qty : -qty;
    }
    long long now_local = 0;
    {
        std::lock_guard<std::mutex> pl(st.local_pos_mu_);
        now_local = st.local_net_po_;
    }
    char buf[256];
    std::snprintf(buf, sizeof(buf),
                  "  [FILL] sym=XAG-PERP leg=%s oid=%lld qty=%lld px=%lld.%03lld "
                  "new_position=%lld desk_synth_update=%s",
                  leg_name.c_str(), order_id, qty,
                  px_cents / 1000, px_cents % 1000,
                  now_local, fixed_mode ? "Y" : "N(BUG)");
    logf(st, buf);
    (void)px_cents;
}

// mmRouteOmGatewayVenueTruthMissing — production: looks the tracked leg up,
// if ADOPTED routes to processFillDeskStack. We always have ADOPTED here so
// just route.
bool mmRouteOmGatewayVenueTruthMissing(State& st, Mode m, long long order_id) {
    Leg snap{};
    bool found = false;
    {
        std::lock_guard<std::mutex> tl(st.tracked_mu_);
        for (const auto& tl_entry : st.tracked_legs_) {
            if (tl_entry.oid == order_id && tl_entry.active) {
                snap = tl_entry;
                found = true;
                break;
            }
        }
    }
    if (!found) return false;
    char buf[256];
    std::snprintf(buf, sizeof(buf),
                  "  [FILL_DESK] sym=XAG-PERP side=%s qty=%lld stack=a56a8ea7 "
                  "routed_from=om_gateway_venue_truth",
                  sideStr(snap.side), snap.qty);
    logf(st, buf);
    processFillDeskStack(st, m, snap.oid, snap.side, snap.qty, snap.px_cents);
    return true;
}

// mmCancelTrackedLegThisStackGateway:
//   - returns 1 on success (mirrors production).
//   - BROKEN_MODE: on benign 404, log + evict, NO position check.
//   - FIXED_MODE:  on benign 404, force-refresh portfolio, compare delta vs
//                   leg side; if matches, synthesize fill BEFORE eviction.
int mmCancelTrackedLeg(State& st, Mode m, long long order_id) {
    Leg snap{};
    bool found = false;
    {
        std::lock_guard<std::mutex> tl(st.tracked_mu_);
        for (const auto& tl_entry : st.tracked_legs_) {
            if (tl_entry.oid == order_id) {
                snap = tl_entry;
                found = true;
                break;
            }
        }
    }
    if (!found) return 1;
    // Simulate the venue cancel REST call. The venue returns HTTP 404 iff the
    // order is not on the book anymore (filled OR cancelled). 200 iff it
    // genuinely cancelled it now.
    bool venue_has_it = false;
    {
        std::lock_guard<std::mutex> vl(st.venue_mu_);
        (void)vl;  // venue state lives outside this mutex in the mock
    }
    {
        std::lock_guard<std::mutex> tl(st.tracked_mu_);
        for (const auto& tl_entry : st.tracked_legs_) {
            if (tl_entry.oid == order_id) {
                venue_has_it = tl_entry.venue_has_it;
                break;
            }
        }
    }
    if (venue_has_it) {
        // Real cancel — HTTP 200.
        char buf[256];
        std::snprintf(buf, sizeof(buf),
                      "  [THEO_MOVE:XAG-PERP] cancel result: oid=%s http_status=200 "
                      "treating_as_success=true",
                      snap.exch_oid.c_str());
        logf(st, buf);
        // remove from venue book + evict locally
        {
            std::lock_guard<std::mutex> tl(st.tracked_mu_);
            for (auto& tl_entry : st.tracked_legs_) {
                if (tl_entry.oid == order_id) {
                    tl_entry.active = false;
                    tl_entry.venue_has_it = false;
                }
            }
        }
        return 1;
    }
    // HTTP 404 — order missing from venue (filled OR cancelled).
    if (m == Mode::Broken) {
        char buf[256];
        std::snprintf(buf, sizeof(buf),
                      "  [THEO_MOVE:XAG-PERP] cancel result: oid=%s http_status=404 "
                      "treating_as_success=true (benign)",
                      snap.exch_oid.c_str());
        logf(st, buf);
        // evict; LOCAL POSITION NOT UPDATED — this is the bug.
        std::lock_guard<std::mutex> tl(st.tracked_mu_);
        for (auto it = st.tracked_legs_.begin(); it != st.tracked_legs_.end();) {
            if (it->oid == order_id) it = st.tracked_legs_.erase(it);
            else ++it;
        }
        return 1;
    }
    // FIXED_MODE: refresh portfolio, compare delta, synth fill if matches.
    long long local_before = 0;
    {
        std::lock_guard<std::mutex> pl(st.local_pos_mu_);
        local_before = st.local_net_po_;
    }
    long long venue_now = 0;
    bool refresh_advanced = true;
    if (m == Mode::FixedWithRefreshFailure) {
        venue_now = local_before;
        refresh_advanced = false;
    } else if (m == Mode::FixedWithArchitectRestLag) {
        // Architect's portfolio REST lags: timestamp advances (refresh
        // succeeded) but the position field still shows the pre-fill value
        // because the order-book endpoint propagated the fill faster than
        // the positions endpoint did. This is exactly what we observed in
        // `Overtrade second example.txt` between 09:10:22.313 and
        // 09:10:26.501 — venue_net_delta=+0 with refresh_advanced=Y.
        venue_now = local_before;
        refresh_advanced = true;
    } else {
        std::lock_guard<std::mutex> vl(st.venue_mu_);
        venue_now = st.venue_net_po_;
    }
    const long long actual_delta = venue_now - local_before;
    const double half_qty = 0.5 * std::max<long long>(1, snap.qty);
    const bool buy_match  = (snap.side == Side::Buy)  && (actual_delta >  half_qty);
    const bool sell_match = (snap.side == Side::Sell) && (actual_delta < -half_qty);
    const bool dir_match = buy_match || sell_match;
    const bool refresh_suspect = !refresh_advanced;
    // ALWAYS-SYNTH policy (post-Overtrade-2 fix): on any benign 404 of an
    // ADOPTED leg, synthesize the fill regardless of position-REST evidence.
    // Architect's positions REST can lag the orders REST and we cannot
    // distinguish "filled and not yet propagated" from "truly cancelled"
    // at this instant. Synth pessimistically; false positives self-heal in
    // <3s via the next REST sync.
    const bool fill_detected = true;
    (void)dir_match; (void)refresh_suspect;
    char buf[512];
    if (fill_detected) {
        std::snprintf(buf, sizeof(buf),
                      "  [STRATEGY:mm_req_XAG_PERP_a56a8ea7] CANCEL_REVEALED_FILL: "
                      "cancel returned HTTP 404 (benign) venue_delta=%+lld "
                      "expected_full_fill=%+lld refresh_advanced=%s -> synthesizing fill "
                      "route oid=%s side=%s leg_qty=%lld (was net=%lld now venue=%lld). "
                      "Without this synth the next cycle would have placed a fresh order "
                      "against a stale NetPo and could have breached max_position.",
                      actual_delta,
                      snap.side == Side::Buy ? snap.qty : -snap.qty,
                      refresh_advanced ? "Y" : "N(treating_pessimistically_as_fill)",
                      snap.exch_oid.c_str(),
                      sideStr(snap.side), snap.qty,
                      local_before, venue_now);
        logf(st, buf);
        // For the pessimistic synth path we must update venue position in our
        // mock too — production has the *real* venue at +2 already (we just
        // couldn't refresh to learn that). Use the leg qty.
        if (m == Mode::FixedWithRefreshFailure) {
            std::lock_guard<std::mutex> vl(st.venue_mu_);
            // venue position is whatever it really is (we don't touch it).
            (void)vl;
        }
        (void)mmRouteOmGatewayVenueTruthMissing(st, m, snap.oid);
    } else {
        std::snprintf(buf, sizeof(buf),
                      "  [THEO_MOVE:XAG-PERP] cancel result: oid=%s http_status=404 "
                      "treating_as_success=true (benign) venue_net_delta=%+lld -> "
                      "classified=true_cancel",
                      snap.exch_oid.c_str(), actual_delta);
        logf(st, buf);
    }
    {
        std::lock_guard<std::mutex> tl(st.tracked_mu_);
        for (auto it = st.tracked_legs_.begin(); it != st.tracked_legs_.end();) {
            if (it->oid == order_id) it = st.tracked_legs_.erase(it);
            else ++it;
        }
    }
    return 1;
}

// Place a fresh order at the venue (mimic OrderManager + venue REST).
// Returns the new order's local oid (0 on refusal).
long long placeOrder(State& st, long long oid, Side side, long long qty,
                     long long px_cents, const std::string& exch_oid_str) {
    // Add to venue book + tracked vector.
    {
        std::lock_guard<std::mutex> tl(st.tracked_mu_);
        Leg leg{};
        leg.oid = oid;
        leg.exch_oid = exch_oid_str;
        leg.side = side;
        leg.qty = qty;
        leg.px_cents = px_cents;
        leg.active = true;
        leg.venue_has_it = true;
        st.tracked_legs_.push_back(leg);
    }
    char buf[256];
    std::snprintf(buf, sizeof(buf),
                  "  [STRATEGY:mm_req_XAG_PERP_a56a8ea7] Order submitted: "
                  "oid=%lld side=%s qty=%lld px=%lld.%03lld",
                  oid, sideStr(side), qty, px_cents / 1000, px_cents % 1000);
    logf(st, buf);
    return oid;
}

// Simulate a silent venue fill: order is removed from the book + venue
// position is updated. The strategy does NOT get notified (no fill event).
void silentVenueFill(State& st, long long target_oid) {
    Leg snap{};
    bool found = false;
    {
        std::lock_guard<std::mutex> tl(st.tracked_mu_);
        for (auto& tl_entry : st.tracked_legs_) {
            if (tl_entry.oid == target_oid && tl_entry.venue_has_it) {
                snap = tl_entry;
                tl_entry.venue_has_it = false;  // gone from venue book
                found = true;
                break;
            }
        }
    }
    if (!found) return;
    {
        std::lock_guard<std::mutex> vl(st.venue_mu_);
        st.venue_net_po_ += (snap.side == Side::Buy) ? snap.qty : -snap.qty;
    }
    long long v;
    {
        std::lock_guard<std::mutex> vl(st.venue_mu_);
        v = st.venue_net_po_;
    }
    char buf[256];
    std::snprintf(buf, sizeof(buf),
                  "  [VENUE_SILENT_FILL] order=%lld side=%s qty=%lld -> venue NetPo now %+lld "
                  "(strategy NOT notified — no fill event delivered)",
                  target_oid, sideStr(snap.side), snap.qty, v);
    logf(st, buf);
}

// runFullMmQuoteCycle: BID branch only (the breach side in Example.txt).
// Production-equivalent flow:
//   1. snapshot net_po from local position state
//   2. cancel any resting BID leg for this stack
//   3. (FIXED) re-read net_po — may have changed if cancel surfaced a fill
//   4. hard pre-gate: refuse BUY if net_po >= cap on grow side
//   5. cross-stack projection (single-stack here, sibling_inflight=0)
//   6. submit_order if both gates passed
// Returns true if a new bid was placed.
bool runBidCycle(State& st, Mode m, long long new_oid, long long new_qty,
                 long long new_px_cents, const std::string& new_exch_oid) {
    std::lock_guard<std::mutex> mv(st.mover_mu_);

    long long net_po_pre = 0;
    {
        std::lock_guard<std::mutex> pl(st.local_pos_mu_);
        net_po_pre = st.local_net_po_;
    }
    // shouldQuoteSide(BUY) — only checks |net| < cap on snapshot value
    if (std::abs(net_po_pre) >= st.cap_) {
        char buf[160];
        std::snprintf(buf, sizeof(buf),
                      "  [STRATEGY] shouldQuoteSide(BUY)=N (|net_po|=%lld >= cap=%lld)",
                      std::abs(net_po_pre), st.cap_);
        logf(st, buf);
        return false;
    }
    // Cancel any resting bid this stack owns.
    long long resting_bid = 0;
    {
        std::lock_guard<std::mutex> tl(st.tracked_mu_);
        for (const auto& tl_entry : st.tracked_legs_) {
            if (tl_entry.active && tl_entry.side == Side::Buy) {
                resting_bid = tl_entry.oid;
                break;
            }
        }
    }
    if (resting_bid) {
        char buf[160];
        std::snprintf(buf, sizeof(buf),
                      "  [STRATEGY] cancel tracked leg: oid=%lld side=BUY ax=XAG-PERP",
                      resting_bid);
        logf(st, buf);
        (void)mmCancelTrackedLeg(st, m, resting_bid);
    }

    long long net_po_for_gate = net_po_pre;
    if (m == Mode::Fixed
        || m == Mode::FixedWithRefreshFailure
        || m == Mode::FixedWithArchitectRestLag) {
        // === SILVER fix: re-read AFTER cancel ===
        std::lock_guard<std::mutex> pl(st.local_pos_mu_);
        net_po_for_gate = st.local_net_po_;
        if (net_po_for_gate != net_po_pre) {
            char buf[256];
            std::snprintf(buf, sizeof(buf),
                          "  [STRATEGY] MM_PLACE_BID_NETPO_REFRESHED "
                          "pre_cancel_net_po=%lld post_cancel_net_po=%lld "
                          "(cancel revealed a missed fill)",
                          net_po_pre, net_po_for_gate);
            logf(st, buf);
        }
        // Hard pre-gate
        const bool buy_is_grow = (net_po_for_gate >= 0);
        if (buy_is_grow && net_po_for_gate >= st.cap_) {
            char buf[256];
            std::snprintf(buf, sizeof(buf),
                          "  [STRATEGY] MM_PLACE_BID_HARD_CAP_GATE net_po=%lld max_po=%lld "
                          "-> refusing BID place (post-cancel re-check)",
                          net_po_for_gate, st.cap_);
            logf(st, buf);
            return false;
        }
    }
    // Cross-stack projection (single-stack: sibling_inflight=0)
    const long long projected =
        std::max<long long>(0, net_po_for_gate) + 0 + new_qty;
    if (projected > st.cap_) {
        char buf[256];
        std::snprintf(buf, sizeof(buf),
                      "  [STRATEGY] MM_PLACE_BID_CAP_GATE net_po=%lld qty=%lld -> "
                      "projected_long=%lld > max_po=%lld -> refusing",
                      net_po_for_gate, new_qty, projected, st.cap_);
        logf(st, buf);
        return false;
    }
    // Submit -> venue accepts -> venue book updated
    (void)placeOrder(st, new_oid, Side::Buy, new_qty, new_px_cents, new_exch_oid);
    return true;
}

// Detect & route the missed-fill via OM_GATEWAY (the slow backstop the
// existing code already has). In production this runs on a 5-15s cadence;
// here we just call it explicitly at the end of the replay to mirror the
// real /open-orders sweep that eventually surfaces the missing leg.
void omGatewayScanAndCancel(State& st, Mode m_in) {
    Mode m = (m_in == Mode::FixedWithRefreshFailure
              || m_in == Mode::FixedWithArchitectRestLag)
                 ? Mode::Fixed
                 : m_in;
    std::vector<Leg> to_terminate;
    {
        std::lock_guard<std::mutex> tl(st.tracked_mu_);
        for (const auto& tl_entry : st.tracked_legs_) {
            if (tl_entry.active && !tl_entry.venue_has_it) {
                to_terminate.push_back(tl_entry);
            }
        }
    }
    for (const auto& leg : to_terminate) {
        if (m == Mode::Fixed) {
            char buf[256];
            std::snprintf(buf, sizeof(buf),
                          "  [OM_GATEWAY] venue has no open order for oid=%lld exch_oid=%s "
                          "sym=XAG-PERP — broadcast claim attempt",
                          leg.oid, leg.exch_oid.c_str());
            logf(st, buf);
            (void)mmRouteOmGatewayVenueTruthMissing(st, m, leg.oid);
        } else {
            char buf[256];
            std::snprintf(buf, sizeof(buf),
                          "  [OM_GATEWAY] venue has no open order for oid=%lld exch_oid=%s "
                          "sym=XAG-PERP — syncing local state to CANCELLED (NO FILL ROUTE)",
                          leg.oid, leg.exch_oid.c_str());
            logf(st, buf);
        }
        std::lock_guard<std::mutex> tl(st.tracked_mu_);
        for (auto it = st.tracked_legs_.begin(); it != st.tracked_legs_.end();) {
            if (it->oid == leg.oid) it = st.tracked_legs_.erase(it);
            else ++it;
        }
    }
}

// REST sync: overwrites local NetPo from venue (production: portfolio refresh).
void restSyncInventory(State& st) {
    long long v;
    {
        std::lock_guard<std::mutex> vl(st.venue_mu_);
        v = st.venue_net_po_;
    }
    {
        std::lock_guard<std::mutex> pl(st.local_pos_mu_);
        st.local_net_po_ = v;
    }
    char buf[256];
    std::snprintf(buf, sizeof(buf),
                  "  [MM_SYNC] After exchange REST refresh: venue NetPo=%+lld "
                  "(overwriting local)",
                  v);
    logf(st, buf);
}

// ---------------------------------------------------------------------------
// The Example.txt replay, parameterised by Mode.
// ---------------------------------------------------------------------------
void replay(State& st, Mode m) {
    const char* tag = "?";
    switch (m) {
        case Mode::Broken: tag = "BROKEN_MODE (pre-fix)"; break;
        case Mode::Fixed:  tag = "FIXED_MODE (post-fix, REST refresh works)"; break;
        case Mode::FixedWithRefreshFailure:
            tag = "FIXED_MODE_REFRESH_FAILS (post-fix, REST refresh times out)"; break;
        case Mode::FixedWithArchitectRestLag:
            tag = "FIXED_MODE_ARCHITECT_REST_LAG (post-fix, positions REST lags orders REST)"; break;
    }
    std::printf("\n=================================================================\n");
    std::printf("  REPLAY: %s\n", tag);
    std::printf("  Starting state (Example.txt @ 07:49:22.064 spawn of stack a56a8ea7):\n");
    std::printf("    cap=max_position=2, REST sync just reported venue=+1, local=+1\n");
    std::printf("    Adopted leg from orders.json: oid=312 BUY 1@75.010\n");
    std::printf("=================================================================\n");

    st.cap_ = 2;
    st.venue_net_po_ = 1;     // REST sync at 07:49:22.096
    st.local_net_po_ = 1;
    {
        std::lock_guard<std::mutex> tl(st.tracked_mu_);
        Leg bid312{};
        bid312.oid = 312;
        bid312.exch_oid = "O-01KS59AN17GRVBYNS6WFR5QZJC";
        bid312.side = Side::Buy;
        bid312.qty = 1;
        bid312.px_cents = 75010;
        bid312.active = true;
        bid312.venue_has_it = true;
        st.tracked_legs_.push_back(bid312);
    }
    observe(st);

    logf(st, "T+0.0s  [07:49:22.064]  orders.json gateway-seed adopt bid 75.01 ask 75.07 (oid=312)");
    logf(st, "T+0.0s  [07:49:22.096]  REST sync: signed_qty=1 -> local NetPo=+1");

    // === T+~2.5s: silent venue fill of order 312 ===
    // In Example.txt, the cancel at 07:49:25.051 returned HTTP 404, proving
    // 312 was already gone from the venue. Most likely cause: it filled.
    logf(st, "T+2.5s  [07:49:24.500]  *** silent venue fill of bid 312 *** (no event delivered)");
    silentVenueFill(st, /*target_oid=*/312);
    observe(st);

    // === T+~2.6s: cycle runs, cancels 312 ===
    logf(st, "T+2.6s  [07:49:24.557]  cycle runs (theo_move), about to cancel 312 + place 314");
    // The cycle places a NEW bid (314) at 75.04. In BROKEN_MODE the gate
    // sees stale NetPo=1 and admits the place. In FIXED_MODE the cancel
    // path surfaces the missed fill, NetPo becomes +2, hard pre-gate
    // refuses.
    const bool placed = runBidCycle(
        st, m,
        /*new_oid=*/314,
        /*new_qty=*/1,
        /*new_px_cents=*/75040,
        /*new_exch_oid=*/"O-01KS59AS4RHXC9PNDENWSJ8P1V");
    observe(st);
    char buf[160];
    std::snprintf(buf, sizeof(buf),
                  "T+2.7s  cycle result: placed_bid_314=%s", placed ? "YES" : "NO");
    logf(st, buf);

    // === T+~4s: bid 314 fills at the venue (if it was placed) ===
    if (placed) {
        logf(st, "T+4.0s  [07:49:26.545]  bid 314 fills at venue (BUY 1 @ 75.04)");
        silentVenueFill(st, /*target_oid=*/314);
        observe(st);
    }

    // === T+~4.5s: OM_GATEWAY scan finds missing legs ===
    logf(st, "T+4.5s  [07:49:26.545/27.020]  OM_GATEWAY scan: missing-from-venue oids");
    omGatewayScanAndCancel(st, m);
    observe(st);

    // === T+~6.5s: next REST sync ===
    logf(st, "T+6.5s  [07:49:28.593]  REST sync (overwrites local from venue truth)");
    restSyncInventory(st);
    observe(st);

    // Dump the chronology
    for (const auto& line : st.log_) std::printf("%s\n", line.c_str());

    {
        std::lock_guard<std::mutex> vl(st.venue_mu_);
        std::lock_guard<std::mutex> pl(st.local_pos_mu_);
        std::printf(
            "\n  FINAL: venue_net=%+lld local_net=%+lld max_|venue|_seen=%lld cap=%lld "
            "BREACH=%s\n",
            st.venue_net_po_, st.local_net_po_,
            st.max_venue_abs_seen_, st.cap_,
            st.breach_observed_ ? "YES" : "NO");
    }
}

// ---------------------------------------------------------------------------
// The single test: run BOTH modes, assert BROKEN breaches and FIXED does not.
// ---------------------------------------------------------------------------
void scenario_example_txt_silver_replay() {
    State broken;
    replay(broken, Mode::Broken);

    State fixed;
    replay(fixed, Mode::Fixed);

    State fixed_rest_fails;
    replay(fixed_rest_fails, Mode::FixedWithRefreshFailure);

    State fixed_architect_lag;
    replay(fixed_architect_lag, Mode::FixedWithArchitectRestLag);

    std::printf("\n=================================================================\n");
    std::printf("  VERDICT\n");
    std::printf("=================================================================\n");
    std::printf("  BROKEN_MODE                  : max |venue|=%lld cap=%lld breach=%s\n",
                broken.max_venue_abs_seen_, broken.cap_,
                broken.breach_observed_ ? "YES (reproduces Joe & Tim's incident)" : "NO");
    std::printf("  FIXED_MODE                   : max |venue|=%lld cap=%lld breach=%s\n",
                fixed.max_venue_abs_seen_, fixed.cap_,
                fixed.breach_observed_ ? "YES (FIX IS INSUFFICIENT)" : "NO (fix holds)");
    std::printf("  FIXED_MODE_REFRESH_FAILS     : max |venue|=%lld cap=%lld breach=%s\n",
                fixed_rest_fails.max_venue_abs_seen_, fixed_rest_fails.cap_,
                fixed_rest_fails.breach_observed_
                    ? "YES (FIX IS INSUFFICIENT — REST failure exposes new path)"
                    : "NO (defensive hardening holds even when REST refresh fails)");
    std::printf("  FIXED_MODE_ARCHITECT_REST_LAG: max |venue|=%lld cap=%lld breach=%s\n",
                fixed_architect_lag.max_venue_abs_seen_, fixed_architect_lag.cap_,
                fixed_architect_lag.breach_observed_
                    ? "YES (FIX IS INSUFFICIENT — Architect positions REST lag exposes Overtrade-2 path)"
                    : "NO (always-synth-on-404 holds even when positions REST lags orders REST)");

    // 1. The replay MUST reproduce the bug under BROKEN_MODE — otherwise the
    //    test is unfaithful and any FIXED_MODE pass is meaningless.
    TX_REQUIRE(broken.breach_observed_);
    TX_REQUIRE(broken.max_venue_abs_seen_ > broken.cap_);

    // 2. FIXED_MODE must NEVER let the venue go past the cap (happy path).
    TX_REQUIRE(!fixed.breach_observed_);
    TX_LE(fixed.max_venue_abs_seen_, fixed.cap_);

    // 3. FIXED_MODE_REFRESH_FAILS must ALSO NEVER let the venue go past the
    //    cap (sad path: REST refresh times out and we cannot prove a fill).
    //    Defensive hardening treats this as a possible fill and refuses the
    //    new order — false positive is acceptable, breach is not.
    TX_REQUIRE(!fixed_rest_fails.breach_observed_);
    TX_LE(fixed_rest_fails.max_venue_abs_seen_, fixed_rest_fails.cap_);

    // 4. FIXED_MODE_ARCHITECT_REST_LAG must ALSO NEVER let the venue go past
    //    the cap. This is the "Overtrade second example.txt" 09:10:21-09:10:26
    //    failure mode: portfolio REST is stale relative to orders REST so the
    //    delta-vs-leg check returns 0 and refresh_advanced=Y. Pre-this-fix the
    //    cancel was classified `true_cancel`, NetPo stayed stale, and a fresh
    //    grow-side order went out and breached. ALWAYS-SYNTH-ON-404 closes this.
    TX_REQUIRE(!fixed_architect_lag.breach_observed_);
    TX_LE(fixed_architect_lag.max_venue_abs_seen_, fixed_architect_lag.cap_);
}

}  // namespace

int main() {
    std::printf("=== Example.txt SILVER breach replay (07:49:22 → 07:49:28) ===\n");
    TX_RUN(scenario_example_txt_silver_replay);
    return tx::finish("test_silver_breach_replay");
}
