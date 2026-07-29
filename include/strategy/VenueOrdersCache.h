// =============================================================================
// VenueOrdersCache — per-AX open-orders snapshot, populated ONLY by the
// main-loop background refresher, consumed (read-only) by mover threads in
// enforce mode.
//
// Enforce-mode directive (2026-07): the mover (order) thread must issue ZERO
// venue GETs. Venue reconciliation still happens — on the main loop, on a
// cadence — feeding this cache. Movers read a consistent snapshot + age and
// NEVER populate it and NEVER fetch inline.
//
// Deliberately decoupled: this header pulls in no strategy/engine headers so it
// can be included by both MakeMarketStrategy and StartupSequence without cycles.
// =============================================================================
#pragma once

#include <chrono>
#include <cstdint>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace architect {
namespace strategy {

/** One resting open-order row for an AX symbol (mirror of MmVenueOpenRow + qty). */
struct VenueCacheRow {
    std::string oid;
    bool is_buy{false};
    double price{0.0};
    double qty{0.0};
};

/** Immutable snapshot handed to a reader: rows + when they were fetched + epoch. */
struct VenueOrdersSnapshot {
    std::vector<VenueCacheRow> rows;
    std::int64_t fetch_steady_ms{0};
    std::uint64_t epoch{0};
    bool valid{false};

    /** Age in ms vs a steady-clock `now`. A missing/invalid snapshot is maximally stale. */
    std::int64_t ageMs(std::int64_t now_steady_ms) const {
        return valid ? (now_steady_ms - fetch_steady_ms) : INT64_MAX;
    }

    /** Convenience: set of live oids in this snapshot. */
    std::unordered_set<std::string> oidSet() const {
        std::unordered_set<std::string> s;
        s.reserve(rows.size() * 2 + 8);
        for (const auto& r : rows) {
            if (!r.oid.empty()) s.insert(r.oid);
        }
        return s;
    }
};

class VenueOrdersCache {
public:
    static VenueOrdersCache& instance() {
        static VenueOrdersCache s_cache;
        return s_cache;
    }

    /** Steady-clock milliseconds — the single time base for all cache ages. */
    static std::int64_t nowSteadyMs() {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
                   std::chrono::steady_clock::now().time_since_epoch())
            .count();
    }

    /** Publish a fresh snapshot for `ax`. Main-loop refresher ONLY. Stamps ts + epoch. */
    void publish(const std::string& ax, std::vector<VenueCacheRow> rows) {
        std::lock_guard<std::mutex> lk(mu_);
        VenueOrdersSnapshot snap;
        snap.rows = std::move(rows);
        snap.fetch_steady_ms = nowSteadyMs();
        snap.epoch = ++epoch_ctr_;
        snap.valid = true;
        by_sym_[ax] = std::move(snap);
    }

    /** Read a consistent snapshot for `ax`. Returns an invalid snapshot if never published. */
    VenueOrdersSnapshot get(const std::string& ax) const {
        std::lock_guard<std::mutex> lk(mu_);
        auto it = by_sym_.find(ax);
        if (it == by_sym_.end()) return VenueOrdersSnapshot{};
        return it->second;
    }

    /** Test/tooling hook: wipe all state. */
    void clearForTest() {
        std::lock_guard<std::mutex> lk(mu_);
        by_sym_.clear();
        epoch_ctr_ = 0;
    }

private:
    VenueOrdersCache() = default;

    mutable std::mutex mu_;
    std::unordered_map<std::string, VenueOrdersSnapshot> by_sym_;
    std::uint64_t epoch_ctr_{0};
};

}  // namespace strategy
}  // namespace architect
