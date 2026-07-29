// =============================================================================
// REGRESSION TEST — Venue-truth eviction propagation/adoption grace (2026-07-14)
// =============================================================================
//
// Live incident 2026-07-14 08:27:04→08:27:05 (`mm_req_EURUSD_PERP_803a7859`):
//   1. A desk stack adopted its exchange OIDs at spawn (08:27:04).
//   2. The very next venue-truth reconcile ~1s later read a snapshot whose
//      live_oids did NOT yet contain those OIDs (venue ack→/open-orders
//      visibility lag / a snapshot that predated the adoption).
//   3. `evict_dead` removed BOTH legs → the stack flipped
//      ACTIVE→VENUE_ORPHANED("venue_truth_evicted_all")→ACTIVE
//      ("venue_orphan_recovered") and re-placed a fresh pair.
//   4. The original, still-live orders were left resting at the venue as
//      untracked orphans (VENUE_COUNT_MISMATCH venue_open held at 4 for
//      minutes) — the "2 stacks / 4 orders from one submit" the desk saw.
//
// Fix (production: MakeMarketStrategy::mmApplyVenueTruthReconcile evict_dead):
//   Never evict a leg whose exchange_oid was established within
//   `venue_truth_evict_grace_ms`. This ONLY delays eviction of freshly
//   placed/adopted legs; a genuinely-dead resting leg is still evicted once the
//   grace elapses, and the guard never places or cancels anything itself. It is
//   the eviction-path analogue of the 2026-06-15 cancel-path fix (always send /
//   never skip on merely-absent oid — see test_venue_orphan_bailout.cpp scope
//   note).
//
// This test models the exact evict_dead + orphan-decision logic and asserts the
// grace contract.

#include "test_helpers.h"

#include <algorithm>
#include <cstdint>
#include <string>
#include <unordered_set>
#include <vector>

namespace {

// Faithful mirror of the production TrackedLeg fields the eviction reads.
struct Leg {
    std::string  oid;
    std::int64_t state_entered_steady_ms{0};
};

struct EvictResult {
    int evicted{0};
    int grace_protected{0};
    bool orphaned{false};  // would flip VENUE_ORPHANED (→ recover → re-place)
};

// Exact mirror of MakeMarketStrategy::mmApplyVenueTruthReconcile's evict_dead
// lambda + the (evicted>0 && live_oids.empty() && grace_protected==0) orphan gate.
EvictResult run_evict(std::vector<Leg>& legs,
                      const std::unordered_set<std::string>& live_oids,
                      std::int64_t now_ms,
                      std::int64_t grace_ms) {
    EvictResult r;
    legs.erase(std::remove_if(legs.begin(), legs.end(), [&](const Leg& t) {
        if (t.oid.empty()) {
            return false;  // pre-adoption — keep
        }
        if (live_oids.count(t.oid) > 0) {
            return false;  // venue still reports it — keep
        }
        if (grace_ms > 0 && t.state_entered_steady_ms > 0 &&
            (now_ms - t.state_entered_steady_ms) < grace_ms) {
            ++r.grace_protected;  // freshly established — keep (may not have propagated)
            return false;
        }
        ++r.evicted;
        return true;
    }), legs.end());
    r.orphaned = (r.evicted > 0) && live_oids.empty() && (r.grace_protected == 0);
    return r;
}

constexpr std::int64_t kGraceMs = 4000;

// THE BUG: a freshly adopted pair, absent from an empty snapshot, must NOT be
// evicted and must NOT orphan the stack (which would re-place = duplicate).
void fresh_pair_absent_from_snapshot_is_not_evicted() {
    const std::int64_t now = 1'000'000;
    std::vector<Leg> legs = {
        {"O-BID-NEW", now - 1000},  // adopted 1s ago (like 803a7859: 08:27:04→08:27:05)
        {"O-ASK-NEW", now - 1000},
    };
    std::unordered_set<std::string> live_oids;  // snapshot has not caught up yet

    EvictResult r = run_evict(legs, live_oids, now, kGraceMs);

    TX_EQ(r.evicted, 0);
    TX_EQ(r.grace_protected, 2);
    TX_REQUIRE(!r.orphaned);           // must NOT flip VENUE_ORPHANED → no duplicate re-place
    TX_EQ(static_cast<int>(legs.size()), 2);  // both legs retained
}

// A genuinely-dead, OLD resting pair (past the grace) must still be evicted and
// still orphan the stack — the healthy path is unchanged.
void old_pair_absent_from_snapshot_is_still_evicted() {
    const std::int64_t now = 1'000'000;
    std::vector<Leg> legs = {
        {"O-BID-OLD", now - (kGraceMs + 1)},
        {"O-ASK-OLD", now - (kGraceMs + 1)},
    };
    std::unordered_set<std::string> live_oids;  // venue genuinely empty

    EvictResult r = run_evict(legs, live_oids, now, kGraceMs);

    TX_EQ(r.evicted, 2);
    TX_EQ(r.grace_protected, 0);
    TX_REQUIRE(r.orphaned);                    // genuine orphan → recover path still fires
    TX_EQ(static_cast<int>(legs.size()), 0);
}

// Mixed ages: an old leg is evicted but a fresh leg is grace-protected. Because a
// live tracked leg remains, the stack must NOT be flagged orphaned.
void mixed_age_does_not_orphan_when_a_fresh_leg_remains() {
    const std::int64_t now = 1'000'000;
    std::vector<Leg> legs = {
        {"O-BID-OLD", now - (kGraceMs + 1)},  // evictable
        {"O-ASK-NEW", now - 500},             // grace-protected
    };
    std::unordered_set<std::string> live_oids;

    EvictResult r = run_evict(legs, live_oids, now, kGraceMs);

    TX_EQ(r.evicted, 1);
    TX_EQ(r.grace_protected, 1);
    TX_REQUIRE(!r.orphaned);                    // still owns a live leg → not orphaned
    TX_EQ(static_cast<int>(legs.size()), 1);
    TX_REQUIRE(legs[0].oid == "O-ASK-NEW");
}

// A leg the venue still reports is never evicted, regardless of age.
void leg_present_in_snapshot_is_never_evicted() {
    const std::int64_t now = 1'000'000;
    std::vector<Leg> legs = {
        {"O-BID-OLD", now - (kGraceMs * 100)},  // very old but venue confirms it
    };
    std::unordered_set<std::string> live_oids = {"O-BID-OLD"};

    EvictResult r = run_evict(legs, live_oids, now, kGraceMs);

    TX_EQ(r.evicted, 0);
    TX_EQ(r.grace_protected, 0);
    TX_REQUIRE(!r.orphaned);
    TX_EQ(static_cast<int>(legs.size()), 1);
}

// grace_ms=0 disables the guard → pre-fix behavior (fresh legs evicted). Confirms
// the escape hatch restores the old path exactly.
void grace_disabled_restores_prefix_eviction() {
    const std::int64_t now = 1'000'000;
    std::vector<Leg> legs = {
        {"O-BID-NEW", now - 1000},
        {"O-ASK-NEW", now - 1000},
    };
    std::unordered_set<std::string> live_oids;

    EvictResult r = run_evict(legs, live_oids, now, /*grace_ms=*/0);

    TX_EQ(r.evicted, 2);
    TX_EQ(r.grace_protected, 0);
    TX_REQUIRE(r.orphaned);
}

}  // namespace

int main() {
    std::printf("=== Venue-truth eviction propagation/adoption grace (JPY/EURUSD duplicate fix) ===\n");
    TX_RUN(fresh_pair_absent_from_snapshot_is_not_evicted);
    TX_RUN(old_pair_absent_from_snapshot_is_still_evicted);
    TX_RUN(mixed_age_does_not_orphan_when_a_fresh_leg_remains);
    TX_RUN(leg_present_in_snapshot_is_never_evicted);
    TX_RUN(grace_disabled_restores_prefix_eviction);
    return tx::finish("test_venue_truth_evict_grace");
}
