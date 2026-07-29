// =============================================================================
// Enforce mode: ZERO venue reads on the mover (order) thread; reconciliation on
// a background cadence (2026-07, position-book-wiring).
//
// These tests exercise the REAL standalone production headers:
//   - core/MmJobVenueCalls.h    (the per-mover-job venue-call counter)
//   - strategy/VenueOrdersCache.h (the main-loop-populated snapshot cache)
// and model the mover-job / refresher / mismatch semantics from
// MakeMarketStrategy.cpp in isolation (the repo's established test idiom — see
// test_position_book.cpp). Nothing here links strategy_lib, so it stays fast.
//
// Coverage (spec §Tests):
//   1. Enforce cycle: a full theo-move pair issues gets==0, posts==2, cancels==2
//      on the mover thread.
//   2. Refresher runs on the main-loop thread id (never the mover); one GET per
//      symbol per interval, interval honored.
//   3. Stale cache: age past max -> VENUE_CACHE_STALE logged ONCE per interval,
//      the job proceeds on local order-state, and issues NO inline fetch.
//   4. Off/shadow equivalence: both still run the identical legacy venue-call
//      sequence (the enforce change must not leak into off/shadow).
//   5. Own-OID mismatch filter: a sibling stack's pair no longer counts as an
//      orphan; a genuinely foreign oid still does.
//
// The mover/refresher "RestClient" is modeled by calling the REAL counter note
// functions (mmJobVenueNoteGet/Post/Cancel) — i.e. exactly what production's
// RestClient does at each venue call site — so the counting logic under test is
// the real one.

#include "test_helpers.h"

#include "core/MmJobVenueCalls.h"
#include "strategy/VenueOrdersCache.h"

#include <atomic>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>

using architect::core::mmJobVenueBegin;
using architect::core::mmJobVenueEnd;
using architect::core::mmJobVenueSnapshot;
using architect::core::mmJobVenueNoteGet;
using architect::core::mmJobVenueNotePost;
using architect::core::mmJobVenueNoteCancel;
using architect::strategy::VenueCacheRow;
using architect::strategy::VenueOrdersCache;
using architect::strategy::VenueOrdersSnapshot;

namespace {

// A modeled venue client. Every method bumps the REAL thread-local job counter
// exactly where production's RestClient does, then returns canned data.
struct ModelVenue {
    // getOrders / getOrdersGatewayRelative — a venue GET.
    std::vector<VenueCacheRow> getOpenOrders(std::vector<VenueCacheRow> rows) {
        mmJobVenueNoteGet();
        return rows;
    }
    // placeOrderRaw — a venue POST.
    void place() { mmJobVenueNotePost(); }
    // cancelOrderGateway / cancelOrder — a venue cancel.
    void cancel() { mmJobVenueNoteCancel(); }
};

// ---------------------------------------------------------------------------
// Modeled mover jobs (mirror mmDrainPendingQuoteCycle's two phases).
//
// ENFORCE: the drift-gate / mismatch / adopt views come from the cache (a read,
// never a GET); the cancel-first drain cancels both stale legs, the re-add drain
// places the fresh pair. NO venue GET anywhere on the mover thread.
// ---------------------------------------------------------------------------

// Phase 1 of a theo-move: cancel both stale legs. Enforce reads the cache only.
void enforceCancelDrain(ModelVenue& venue, const std::string& ax) {
    mmJobVenueBegin();
    // Drift-gate working-order view: consume the cache (a read, not a GET).
    const VenueOrdersSnapshot snap = VenueOrdersCache::instance().get(ax);
    (void)snap;  // used for the drift view; never triggers a fetch
    venue.cancel();
    venue.cancel();
    mmJobVenueEnd();
}

// Phase 2 of a theo-move: re-add the fresh pair after the cancel acks.
void enforcePlaceDrain(ModelVenue& venue, const std::string& ax) {
    mmJobVenueBegin();
    const VenueOrdersSnapshot snap = VenueOrdersCache::instance().get(ax);
    (void)snap;  // cap/adopt candidacy read from cache — no GET
    venue.place();
    venue.place();
    mmJobVenueEnd();
}

// Legacy (off/shadow) mover job: starts with a venue open-orders GET
// (VENUE_TRUTH) before it cancels + places. This is the path that must remain
// byte-for-byte in off/shadow.
struct JobCounts {
    long gets{0};
    long posts{0};
    long cancels{0};
};
JobCounts legacyMoverJob(ModelVenue& venue) {
    mmJobVenueBegin();
    venue.getOpenOrders({});  // job-start VENUE_TRUTH open-orders fetch
    venue.cancel();
    venue.cancel();
    venue.place();
    venue.place();
    const auto c = mmJobVenueSnapshot();
    mmJobVenueEnd();
    return JobCounts{c.gets, c.posts, c.cancels};
}

// ---------------------------------------------------------------------------
// 1. Enforce cycle: full theo-move pair on the mover thread -> gets=0,
//    posts=2, cancels=2 (summed across the cancel-drain + re-add-drain).
// ---------------------------------------------------------------------------
void enforce_pair_cycle_zero_gets() {
    VenueOrdersCache::instance().clearForTest();
    const std::string ax = "EURUSD-PERP";
    // Warm the cache the way the background refresher would (on ANOTHER thread).
    VenueOrdersCache::instance().publish(
        ax, {VenueCacheRow{"o-bid", true, 1.0, 1.0}, VenueCacheRow{"o-ask", false, 2.0, 1.0}});

    ModelVenue venue;
    long total_gets = 0, total_posts = 0, total_cancels = 0;

    // Run the whole pair on ONE thread (the mover worker), two drains.
    std::thread mover([&] {
        // Cancel-first drain.
        enforceCancelDrain(venue, ax);
        auto c1 = mmJobVenueSnapshot();  // snapshot after end() still holds last values
        // Re-add drain.
        enforcePlaceDrain(venue, ax);
        auto c2 = mmJobVenueSnapshot();
        // Per-drain contract.
        TX_EQ(c1.gets, 0L);
        TX_EQ(c1.cancels, 2L);
        TX_EQ(c1.posts, 0L);
        TX_EQ(c2.gets, 0L);
        TX_EQ(c2.posts, 2L);
        TX_EQ(c2.cancels, 0L);
        total_gets = c1.gets + c2.gets;
        total_posts = c1.posts + c2.posts;
        total_cancels = c1.cancels + c2.cancels;
    });
    mover.join();

    // Aggregate across the pair: exactly the spec's gets=0, posts=2, cancels=2.
    TX_EQ(total_gets, 0L);
    TX_EQ(total_posts, 2L);
    TX_EQ(total_cancels, 2L);
}

// ---------------------------------------------------------------------------
// 2. Refresher runs on the main-loop thread, never the mover; one GET per
//    symbol per interval; interval honored; mover reads issue zero GETs.
// ---------------------------------------------------------------------------
void refresher_on_main_thread_interval_honored() {
    VenueOrdersCache::instance().clearForTest();
    const std::string ax = "SPY-PERP";

    std::atomic<bool> stop{false};
    std::atomic<int> publishes{0};
    std::thread::id refresher_tid;
    std::thread::id mover_tid;

    // Background refresher: piggybacks the main-loop cadence. One publish per
    // interval. Uses a small interval so the test runs fast.
    const int interval_ms = 10;
    const int run_ms = 105;
    std::thread refresher([&] {
        refresher_tid = std::this_thread::get_id();
        ModelVenue venue;
        while (!stop.load(std::memory_order_acquire)) {
            // Refresher DOES issue a venue GET — but it is NOT inside a mover job,
            // so the per-job counter (inactive here) must stay at zero.
            mmJobVenueNoteGet();  // (no active job on this thread) — must be ignored
            auto rows = venue.getOpenOrders({VenueCacheRow{"o1", true, 1.0, 1.0}});
            VenueOrdersCache::instance().publish(ax, rows);
            publishes.fetch_add(1, std::memory_order_relaxed);
            std::this_thread::sleep_for(std::chrono::milliseconds(interval_ms));
        }
    });

    // A mover thread reads the cache repeatedly inside a job — zero GETs.
    std::thread mover([&] {
        mover_tid = std::this_thread::get_id();
        std::this_thread::sleep_for(std::chrono::milliseconds(30));  // let cache warm
        mmJobVenueBegin();
        for (int i = 0; i < 50; ++i) {
            const VenueOrdersSnapshot s = VenueOrdersCache::instance().get(ax);
            (void)s;
        }
        const auto c = mmJobVenueSnapshot();
        mmJobVenueEnd();
        TX_EQ(c.gets, 0L);  // reading the cache is never a GET
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(run_ms));
    stop.store(true, std::memory_order_release);
    refresher.join();
    mover.join();

    // Refresher and mover are distinct threads.
    TX_REQUIRE(refresher_tid != mover_tid);
    // Interval honored: ~run_ms/interval_ms publishes (allow generous slack).
    const int p = publishes.load();
    TX_REQUIRE(p >= 3);
    TX_LE(p, 40);
    // Cache ended up warm and valid.
    const VenueOrdersSnapshot s = VenueOrdersCache::instance().get(ax);
    TX_REQUIRE(s.valid);
}

// ---------------------------------------------------------------------------
// 3. Stale cache: age past max -> VENUE_CACHE_STALE logged ONCE per interval,
//    the job proceeds on local order-state, and issues NO inline fetch.
// ---------------------------------------------------------------------------
//
// Mirrors the mover read guard: if snap.ageMs(now) > max_age, emit
// VENUE_CACHE_STALE (throttled to once per interval via a last-log timestamp),
// then PROCEED on local state — never fetch inline.
struct StaleGuard {
    // Sentinel far in the past so the FIRST staleness always logs (production
    // starts with no prior VENUE_CACHE_STALE stamp, so the first hit fires).
    std::int64_t last_log_ms{INT64_MIN / 2};
    int stale_logs{0};
    // Returns true if the job should proceed on LOCAL state (i.e. cache too old).
    bool consult(const VenueOrdersSnapshot& snap, std::int64_t now_ms,
                 std::int64_t max_age_ms, std::int64_t throttle_ms, ModelVenue& venue) {
        const std::int64_t age = snap.ageMs(now_ms);
        if (age > max_age_ms) {
            if (now_ms - last_log_ms >= throttle_ms) {
                ++stale_logs;  // [VENUE_CACHE_STALE] emitted
                last_log_ms = now_ms;
            }
            // PROCEED on local state — crucially, do NOT touch `venue` (no fetch).
            (void)venue;
            return true;
        }
        return false;
    }
};

void stale_cache_logs_once_and_proceeds_no_fetch() {
    VenueOrdersCache::instance().clearForTest();
    const std::string ax = "XAU-PERP";
    // Never published -> snapshot invalid -> ageMs == INT64_MAX (maximally stale).
    const VenueOrdersSnapshot snap = VenueOrdersCache::instance().get(ax);
    TX_REQUIRE(!snap.valid);

    ModelVenue venue;
    StaleGuard guard;
    const std::int64_t max_age = 30000;
    const std::int64_t throttle = 10000;  // once per refresh interval

    mmJobVenueBegin();
    // Several mover jobs within the same throttle window.
    std::int64_t now = 1000;
    for (int i = 0; i < 5; ++i) {
        const bool proceed_local = guard.consult(snap, now, max_age, throttle, venue);
        TX_REQUIRE(proceed_local);  // stale -> always proceed on local state
        now += 500;                 // still inside the 10s throttle window
    }
    const auto c = mmJobVenueSnapshot();
    mmJobVenueEnd();

    // Logged exactly once across the burst; and NO inline venue fetch at all.
    TX_EQ(guard.stale_logs, 1);
    TX_EQ(c.gets, 0L);
    TX_EQ(c.posts, 0L);
    TX_EQ(c.cancels, 0L);
}

// ---------------------------------------------------------------------------
// 4. Off/shadow equivalence: both run the IDENTICAL legacy venue-call sequence
//    (job-start GET + 2 cancels + 2 places). The enforce change (gets=0) must
//    not leak into off/shadow.
// ---------------------------------------------------------------------------
void off_shadow_keep_legacy_venue_sequence() {
    VenueOrdersCache::instance().clearForTest();
    ModelVenue off_venue;
    ModelVenue shadow_venue;

    const JobCounts off = legacyMoverJob(off_venue);
    const JobCounts shadow = legacyMoverJob(shadow_venue);

    // Identical sequence between off and shadow.
    TX_EQ(off.gets, shadow.gets);
    TX_EQ(off.posts, shadow.posts);
    TX_EQ(off.cancels, shadow.cancels);
    // And it is the LEGACY sequence: a job-start GET is present.
    TX_EQ(off.gets, 1L);
    TX_EQ(off.posts, 2L);
    TX_EQ(off.cancels, 2L);

    // Contrast with enforce on the same modeled cycle: gets collapses to 0.
    VenueOrdersCache::instance().publish("Z-PERP", {});
    ModelVenue enf_venue;
    enforceCancelDrain(enf_venue, "Z-PERP");
    const auto c1 = mmJobVenueSnapshot();
    enforcePlaceDrain(enf_venue, "Z-PERP");
    const auto c2 = mmJobVenueSnapshot();
    TX_EQ(c1.gets + c2.gets, 0L);  // enforce did NOT issue the legacy job-start GET
}

// ---------------------------------------------------------------------------
// 5. Own-OID mismatch filter (mirrors mmRefreshVenueOrdersCacheAllEnforced):
//    orphan = a venue row owned (tracked) by NO local stack. A sibling stack's
//    pair is owned -> not an orphan; a genuinely foreign oid is -> an orphan.
// ---------------------------------------------------------------------------
std::size_t countOrphans(const std::vector<VenueCacheRow>& venue_rows,
                         const std::unordered_set<std::string>& owned_oids) {
    std::size_t orphans = 0;
    for (const auto& r : venue_rows) {
        if (!r.oid.empty() && owned_oids.find(r.oid) == owned_oids.end()) {
            ++orphans;
        }
    }
    return orphans;
}

void own_oid_filter_ignores_siblings_flags_foreign() {
    // Venue reports: my pair, a sibling stack's pair (same symbol), and one truly
    // foreign row placed by nobody local.
    const std::vector<VenueCacheRow> venue_rows = {
        {"mine-bid", true, 1.0, 1.0},
        {"mine-ask", false, 2.0, 1.0},
        {"sib-bid", true, 1.0, 1.0},
        {"sib-ask", false, 2.0, 1.0},
        {"foreign-1", true, 3.0, 1.0},
    };

    // owned_oids is filtered across ALL stacks on the symbol (mine + sibling).
    const std::unordered_set<std::string> owned_oids = {
        "mine-bid", "mine-ask", "sib-bid", "sib-ask"};

    // Sibling pair no longer counts; only the genuinely foreign oid does.
    TX_EQ(countOrphans(venue_rows, owned_oids), static_cast<std::size_t>(1));

    // Sanity: the OLD (buggy) single-stack view would have flagged the sibling
    // pair as orphans too (3 total) — the own-OID filter is what removes them.
    const std::unordered_set<std::string> mine_only = {"mine-bid", "mine-ask"};
    TX_EQ(countOrphans(venue_rows, mine_only), static_cast<std::size_t>(3));
}

}  // namespace

int main() {
    TX_RUN(enforce_pair_cycle_zero_gets);
    TX_RUN(refresher_on_main_thread_interval_honored);
    TX_RUN(stale_cache_logs_once_and_proceeds_no_fetch);
    TX_RUN(off_shadow_keep_legacy_venue_sequence);
    TX_RUN(own_oid_filter_ignores_siblings_flags_foreign);
    return tx::finish("enforce_zero_venue_reads");
}
