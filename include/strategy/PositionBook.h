#pragma once

#include <atomic>
#include <cassert>
#include <chrono>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

namespace architect {
namespace strategy {

/**
 * PositionBook — single, authoritative representation of net position for ONE
 * AX symbol, shared across every sibling market-making stack (L1/L2/L3) on that
 * symbol.
 *
 * Design (Tim spec, 2026-07):
 *   effective() = exchange_snapshot_qty + unaccounted_fill_qty
 *   - exchange_snapshot_qty: last venue truth, refreshed OFF the hot path on a
 *     30s background cadence (piggybacked on the existing 5s position poll).
 *   - unaccounted_fill_qty: signed sum of all fills received since the last
 *     snapshot. Driven by the ~1s fills poll -> onFill().
 *
 * `effective()` is THE position for every gate (quoting, reduce-only, skew,
 * cap). It is a nanosecond, in-process read — NEVER a syscall or REST call —
 * so it is safe on the latency-sensitive mover threads.
 *
 * Threading invariant:
 *   - onFill() and applySnapshot() are BOTH driven by the single main trading
 *     loop thread (fills poll + positions poll run there), so they are
 *     naturally serialized with respect to each other. This is asserted via a
 *     thread-id check in both methods.
 *   - effective()/readers may run concurrently on mover worker threads; they
 *     take the per-book mutex for a torn-read-free snapshot.
 *   - seed() runs once at startup (possibly on a different thread than the
 *     steady-state loop) and therefore does NOT assert the writer thread.
 */
class PositionBook {
public:
    struct ApplyResult {
        double prev_effective{0.0};
        double prev_unaccounted{0.0};
        double new_snapshot{0.0};
        double new_effective{0.0};
        std::uint64_t epoch{0};
    };

    // THE position. Lock-free-ish (short mutex hold). Hot-path safe.
    double effective() const {
        std::lock_guard<std::mutex> lk(mu_);
        return exchange_snapshot_qty_ + unaccounted_fill_qty_;
    }

    double exchangeSnapshot() const {
        std::lock_guard<std::mutex> lk(mu_);
        return exchange_snapshot_qty_;
    }

    double unaccounted() const {
        std::lock_guard<std::mutex> lk(mu_);
        return unaccounted_fill_qty_;
    }

    std::uint64_t epoch() const {
        std::lock_guard<std::mutex> lk(mu_);
        return snapshot_epoch_;
    }

    std::int64_t snapshotAgeMs() const {
        std::lock_guard<std::mutex> lk(mu_);
        if (snapshot_epoch_ == 0) {
            return -1;  // never seeded
        }
        return std::chrono::duration_cast<std::chrono::milliseconds>(
                   std::chrono::steady_clock::now() - snapshot_time_)
            .count();
    }

    // Startup seed. Sets the initial snapshot and clears unaccounted fills.
    // Not thread-asserted (init may run off the steady-state loop thread).
    void seed(double exchange_qty) {
        std::lock_guard<std::mutex> lk(mu_);
        exchange_snapshot_qty_ = exchange_qty;
        unaccounted_fill_qty_ = 0.0;
        snapshot_time_ = std::chrono::steady_clock::now();
        if (snapshot_epoch_ == 0) {
            snapshot_epoch_ = 1;
        }
    }

    // Fill handler — main-loop thread only.
    void onFill(double signed_qty) {
        assertWriterThread();
        std::lock_guard<std::mutex> lk(mu_);
        unaccounted_fill_qty_ += signed_qty;
    }

    // Background refresh — main-loop thread only.
    // Reset unaccounted FIRST, then set the new snapshot (Tim spec ordering).
    ApplyResult applySnapshot(double exchange_qty) {
        assertWriterThread();
        std::lock_guard<std::mutex> lk(mu_);
        ApplyResult r;
        r.prev_effective = exchange_snapshot_qty_ + unaccounted_fill_qty_;
        r.prev_unaccounted = unaccounted_fill_qty_;
        unaccounted_fill_qty_ = 0.0;               // reset FIRST
        exchange_snapshot_qty_ = exchange_qty;     // THEN apply snapshot
        snapshot_time_ = std::chrono::steady_clock::now();
        ++snapshot_epoch_;
        r.new_snapshot = exchange_snapshot_qty_;
        r.new_effective = exchange_snapshot_qty_ + unaccounted_fill_qty_;
        r.epoch = snapshot_epoch_;
        return r;
    }

    // ---- Per-AX-symbol registry (one shared book per symbol) -----------------

    static std::shared_ptr<PositionBook> forSymbol(const std::string& ax_symbol) {
        if (ax_symbol.empty()) {
            return nullptr;
        }
        std::lock_guard<std::mutex> lk(registryMutex());
        auto& slot = registry()[ax_symbol];
        if (!slot) {
            slot = std::make_shared<PositionBook>();
        }
        return slot;
    }

    // Oldest snapshot age across all registered books (-1 if none seeded). Used
    // by the refresh host to emit POSITION_SNAPSHOT_STALE on repeated failures.
    static std::int64_t oldestSnapshotAgeMs() {
        std::vector<std::shared_ptr<PositionBook>> books;
        {
            std::lock_guard<std::mutex> lk(registryMutex());
            books.reserve(registry().size());
            for (auto& kv : registry()) {
                books.push_back(kv.second);
            }
        }
        std::int64_t worst = -1;
        for (auto& b : books) {
            const std::int64_t age = b->snapshotAgeMs();
            if (age > worst) {
                worst = age;
            }
        }
        return worst;
    }

    static std::shared_ptr<PositionBook> lookup(const std::string& ax_symbol) {
        if (ax_symbol.empty()) {
            return nullptr;
        }
        std::lock_guard<std::mutex> lk(registryMutex());
        auto it = registry().find(ax_symbol);
        return (it == registry().end()) ? nullptr : it->second;
    }

    // Apply a full venue snapshot batch: every registered symbol is updated.
    // Symbols present in `venue_by_symbol` take that value; symbols absent from
    // the venue response are treated as flat (0). Main-loop thread only.
    static void applySnapshotBatch(
        const std::unordered_map<std::string, double>& venue_by_symbol,
        std::vector<std::pair<std::string, ApplyResult>>* out_results = nullptr) {
        std::vector<std::pair<std::string, std::shared_ptr<PositionBook>>> books;
        {
            std::lock_guard<std::mutex> lk(registryMutex());
            books.reserve(registry().size());
            for (auto& kv : registry()) {
                books.emplace_back(kv.first, kv.second);
            }
        }
        for (auto& kv : books) {
            const auto it = venue_by_symbol.find(kv.first);
            const double qty = (it == venue_by_symbol.end()) ? 0.0 : it->second;
            ApplyResult r = kv.second->applySnapshot(qty);
            if (out_results) {
                out_results->emplace_back(kv.first, r);
            }
        }
    }

private:
    static std::unordered_map<std::string, std::shared_ptr<PositionBook>>& registry() {
        static std::unordered_map<std::string, std::shared_ptr<PositionBook>> r;
        return r;
    }
    static std::mutex& registryMutex() {
        static std::mutex m;
        return m;
    }

    static void assertWriterThread() {
        static std::atomic<std::thread::id> writer_tid{};
        const std::thread::id self = std::this_thread::get_id();
        std::thread::id expected{};
        if (writer_tid.compare_exchange_strong(expected, self)) {
            return;  // first writer wins and defines the invariant thread
        }
        assert(writer_tid.load() == self &&
               "PositionBook onFill/applySnapshot must run on the single main-loop thread");
        (void)self;
    }

    mutable std::mutex mu_;
    double exchange_snapshot_qty_{0.0};
    double unaccounted_fill_qty_{0.0};
    std::chrono::steady_clock::time_point snapshot_time_{};
    std::uint64_t snapshot_epoch_{0};
};

}  // namespace strategy
}  // namespace architect
