// =============================================================================
// REGRESSION TEST — orders.json mass-teardown guardrail + incident replay
// (2026-07-15, "orders pulled automatically")
// =============================================================================
//
// Production change: examples/main.cpp reconcileOrdersConfig() no longer tears
// down stacks the instant the live orders.json shrinks. When a single reconcile
// pass would remove >= mass_min stacks it DEFERS on first sighting and only
// proceeds if an immediately-forced second read yields the IDENTICAL removal set
// (MM_RECONCILE_MASS_TEARDOWN_DEFERRED -> _CONFIRMED). A desk-authorised
// intent=remove_all bypasses the defer for an INSTANT emergency pull-all
// (MM_RECONCILE_MASS_TEARDOWN_INTENT). A file that recovers before the confirming
// read cancels the pending teardown (MM_RECONCILE_MASS_TEARDOWN_RECOVERED).
//
// This test models that exact decision flow in isolation (the repo's established
// idiom — the guardrail lives in main.cpp which links no test binary) and replays
// the proven incident sequence from Wednesday's logs:
//
//     [ORDERS_JSON_READ] found 12  ... healthy
//     [ORDERS_JSON_READ] found 1   -> removing=12   (the wipe)
//     [ORDERS_JSON_READ] found 0   -> removing=...   (the wipe's other face)
//
// with a NEGATIVE CONTROL proving that with the guard disabled the same sequence
// cancels 12 on the first read (i.e. the test genuinely detects the original bug).

#include "test_helpers.h"

#include <algorithm>
#include <string>
#include <vector>

namespace {

// One reconcile read: what the pass computed from the file it just read.
struct Read {
    long desired_count{0};
    int running_mm_req{0};
    std::vector<std::string> to_remove;   // ax:stack_id of stacks no longer desired
    std::string intent;                   // "" | "normal" | "remove_all" | "clear_all"
    bool guard_enabled{true};
    int mass_min{2};
};

struct Outcome {
    int torn_down{0};       // stacks actually cancelled this pass
    bool deferred{false};   // pass declined to act, awaiting a confirming read
    bool force_reread{false};
};

// Faithful mirror of the MASS-TEARDOWN GUARDRAIL block in reconcileOrdersConfig().
// `pending_fp` is the persistent static (s_pending_mass_teardown_fp) carried
// across reconcile passes.
struct Guard {
    std::string pending_fp;

    Outcome pass(const Read& r) {
        Outcome o;

        // Stable, sorted removal fingerprint == the production `removed` string,
        // prefixed by desired#running (matches fp construction in main.cpp).
        std::vector<std::string> ids = r.to_remove;
        std::sort(ids.begin(), ids.end());
        std::string removed;
        for (const auto& s : ids) {
            if (!removed.empty()) removed += ",";
            removed += s;
        }
        const std::string fp =
            std::to_string(r.desired_count) + "#" + std::to_string(r.running_mm_req) + "|" + removed;

        const bool intent_remove_all = (r.intent == "remove_all" || r.intent == "clear_all");
        const bool is_mass =
            static_cast<int>(r.to_remove.size()) >= r.mass_min && !intent_remove_all;

        if (r.guard_enabled && is_mass) {
            if (pending_fp != fp) {
                pending_fp = fp;
                o.force_reread = true;
                o.deferred = true;
                return o;  // DEFERRED — no teardown this pass
            }
            // second consecutive identical observation → CONFIRMED
            pending_fp.clear();
        } else if (!pending_fp.empty()) {
            // recovered / below threshold / intent → cancel any pending defer
            pending_fp.clear();
        }

        o.torn_down = static_cast<int>(r.to_remove.size());
        return o;
    }
};

std::vector<std::string> nStacks(int n) {
    std::vector<std::string> v;
    v.reserve(n);
    for (int i = 0; i < n; ++i) {
        v.push_back("EURUSD-PERP:s" + std::to_string(i));
    }
    return v;
}

// INCIDENT REPLAY (12 -> 1): the first read that sees the shrink must NOT cancel
// anything; only a second, identical confirming read tears the 12 down.
void incident_12_to_1_defers_then_confirms() {
    Guard g;
    Read r1;
    r1.desired_count = 1;
    r1.running_mm_req = 13;   // a 13th stack had just been added (survivor)
    r1.to_remove = nStacks(12);
    r1.intent = "normal";

    Outcome o1 = g.pass(r1);
    TX_EQ(o1.torn_down, 0);          // the fix: ZERO cancels on the wipe's first read
    TX_REQUIRE(o1.deferred);
    TX_REQUIRE(o1.force_reread);

    Outcome o2 = g.pass(r1);         // identical confirming read
    TX_EQ(o2.torn_down, 12);         // now the deliberate shrink proceeds
    TX_REQUIRE(!o2.deferred);
}

// INCIDENT REPLAY (12 -> 0): the empty-file variant defers identically.
void incident_12_to_0_defers_then_confirms() {
    Guard g;
    Read r1;
    r1.desired_count = 0;
    r1.running_mm_req = 12;
    r1.to_remove = nStacks(12);
    r1.intent = "normal";

    Outcome o1 = g.pass(r1);
    TX_EQ(o1.torn_down, 0);
    TX_REQUIRE(o1.deferred);

    Outcome o2 = g.pass(r1);
    TX_EQ(o2.torn_down, 12);
}

// A transient/partial/foreign shrink that RECOVERS before the confirming read
// must cancel the pending teardown — nothing gets torn down.
void recovered_before_confirm_cancels_teardown() {
    Guard g;
    Read bad;
    bad.desired_count = 1;
    bad.running_mm_req = 13;
    bad.to_remove = nStacks(12);
    bad.intent = "normal";
    Outcome o1 = g.pass(bad);
    TX_EQ(o1.torn_down, 0);
    TX_REQUIRE(o1.deferred);

    // Next read: the file is healthy again (nothing to remove).
    Read good;
    good.desired_count = 13;
    good.running_mm_req = 13;
    good.to_remove = {};
    good.intent = "normal";
    Outcome o2 = g.pass(good);
    TX_EQ(o2.torn_down, 0);
    TX_REQUIRE(!o2.deferred);
    TX_REQUIRE(g.pending_fp.empty());  // pending defer was cleared
}

// EMERGENCY PULL-ALL stays INSTANT: intent=remove_all tears down on the FIRST
// read with no deferral (the safety function is never delayed).
void intent_remove_all_is_instant() {
    Guard g;
    Read r;
    r.desired_count = 0;
    r.running_mm_req = 12;
    r.to_remove = nStacks(12);
    r.intent = "remove_all";

    Outcome o = g.pass(r);
    TX_EQ(o.torn_down, 12);          // immediate
    TX_REQUIRE(!o.deferred);
    TX_REQUIRE(!o.force_reread);
}

// A normal small edit (single-stack remove) is never gated.
void single_remove_not_gated() {
    Guard g;
    Read r;
    r.desired_count = 11;
    r.running_mm_req = 12;
    r.to_remove = nStacks(1);
    r.intent = "normal";

    Outcome o = g.pass(r);
    TX_EQ(o.torn_down, 1);
    TX_REQUIRE(!o.deferred);
}

// If the shrink set CHANGES between reads, the second read is a fresh first
// sighting (defer again) — we never confirm a teardown we didn't see twice.
void changed_shrink_defers_again() {
    Guard g;
    Read a;
    a.desired_count = 1;
    a.running_mm_req = 13;
    a.to_remove = nStacks(12);
    a.intent = "normal";
    Outcome o1 = g.pass(a);
    TX_EQ(o1.torn_down, 0);
    TX_REQUIRE(o1.deferred);

    Read b = a;                       // different removal set (drops to 0/11)
    b.desired_count = 0;
    b.running_mm_req = 13;
    b.to_remove = nStacks(11);
    Outcome o2 = g.pass(b);
    TX_EQ(o2.torn_down, 0);           // not the same fp → defer again, still no cancel
    TX_REQUIRE(o2.deferred);
}

// NEGATIVE CONTROL: with the guard disabled, the exact incident sequence cancels
// 12 on the FIRST read — i.e. this is the original bug, and the test above proves
// the guard is what prevents it.
void negative_control_guard_off_reproduces_bug() {
    Guard g;
    Read r;
    r.desired_count = 1;
    r.running_mm_req = 13;
    r.to_remove = nStacks(12);
    r.intent = "normal";
    r.guard_enabled = false;          // pre-fix behaviour

    Outcome o = g.pass(r);
    TX_EQ(o.torn_down, 12);           // 24 live venue orders pulled on first read
    TX_REQUIRE(!o.deferred);
}

}  // namespace

int main() {
    std::printf("=== orders.json mass-teardown guardrail + incident replay ===\n");
    TX_RUN(incident_12_to_1_defers_then_confirms);
    TX_RUN(incident_12_to_0_defers_then_confirms);
    TX_RUN(recovered_before_confirm_cancels_teardown);
    TX_RUN(intent_remove_all_is_instant);
    TX_RUN(single_remove_not_gated);
    TX_RUN(changed_shrink_defers_again);
    TX_RUN(negative_control_guard_off_reproduces_bug);
    return tx::finish("test_orders_json_mass_teardown_guard");
}
