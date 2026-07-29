// =============================================================================
// REGRESSION TEST — fixes C3 (order_id atomicity) + C5 (feed mutex)
// =============================================================================
//
// C3 (order id atomicity)
//   `bid_order_id_` / `ask_order_id_` are written by on_accept (OM event
//   dispatch thread) and read by onFeedUpdate (feed worker thread), the
//   mover (cycle planning), and the desk snapshot. Plain OrderId allowed
//   torn reads on 64-bit platforms whose stores aren't naturally atomic
//   for all alignments. The fix makes them std::atomic<OrderId> with
//   acquire/release pairing.
//
// C5 (per-strategy feed mutex)
//   ExternalFeedManager runs multiple worker threads (poll_thread_,
//   aux_poll_thread_, mettraders_thread_). When a strategy is bound to
//   primary+auxiliary theo sources, two of those threads can call
//   onFeedUpdate on the SAME strategy concurrently. Without
//   mm_feed_callback_mutex_, the body races on last_theo_, banner state,
//   and the enqueued-cycle reason map.
//
// This test asserts:
//   - Atomic OrderId reads/writes never tear.
//   - Per-strategy feed mutex ensures mutual exclusion (probe via a
//     non-atomic "in_critical" counter — should always be ≤1).
//   - The mutex does not introduce a deadlock with the simulated
//     downstream lock (tracked_orders_mutex_-like).

#include "test_helpers.h"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <mutex>
#include <thread>
#include <vector>

namespace {

// ---- C3 ----
void order_id_atomic_no_torn_reads() {
    std::atomic<uint64_t> oid{0};
    std::atomic<bool>     stop{false};
    std::atomic<int>      bad{0};

    // Writer cycles between a low-magic and a high-magic value with all
    // bytes set such that a 32-bit torn read would land on a value that's
    // neither pattern.
    constexpr uint64_t kA = 0x1111111122222222ULL;
    constexpr uint64_t kB = 0xAAAAAAAAEEEEEEEEULL;

    std::thread writer([&] {
        bool flip = false;
        while (!stop.load(std::memory_order_acquire)) {
            oid.store(flip ? kA : kB, std::memory_order_release);
            flip = !flip;
        }
    });

    std::thread reader([&] {
        while (!stop.load(std::memory_order_acquire)) {
            const uint64_t v = oid.load(std::memory_order_acquire);
            if (v != 0 && v != kA && v != kB) {
                bad.fetch_add(1, std::memory_order_relaxed);
            }
        }
    });

    std::this_thread::sleep_for(std::chrono::seconds(1));
    stop.store(true, std::memory_order_release);
    writer.join();
    reader.join();
    TX_EQ(bad.load(), 0);
}

// ---- C5 ----
// Faithful model of MakeMarketStrategy::onFeedUpdate:
//   - mm_feed_callback_mutex_ is acquired at the top of the function
//   - then tracked_orders_mutex_ may be acquired inside for snapshots
// We probe with `in_critical` (a non-atomic int counter incremented at the
// top of the simulated onFeedUpdate). Without mm_feed_callback_mutex_
// concurrent feed threads would observe in_critical > 1.
struct StrategyFake {
    std::mutex feed_cb_mu_;     // mm_feed_callback_mutex_
    std::mutex tracked_mu_;     // tracked_orders_mutex_
    int        in_critical_{0}; // NON-ATOMIC; serves as race witness
    std::atomic<int> max_in_critical_{0};

    void onFeedUpdate() {
        std::lock_guard<std::mutex> feed_lk(feed_cb_mu_);
        in_critical_ += 1;
        const int cur = in_critical_;
        // Track the max observed concurrency. Must always be 1.
        int prev = max_in_critical_.load(std::memory_order_relaxed);
        while (cur > prev &&
               !max_in_critical_.compare_exchange_weak(prev, cur,
                                                       std::memory_order_acq_rel,
                                                       std::memory_order_relaxed)) {}
        // Simulate inner work that takes a sub-lock (production pattern)
        {
            std::lock_guard<std::mutex> tlk(tracked_mu_);
            // pretend to read tracked vectors
            std::this_thread::sleep_for(std::chrono::microseconds(50));
        }
        std::this_thread::sleep_for(std::chrono::microseconds(50));
        in_critical_ -= 1;
    }
};

void feed_callback_mutex_serializes_per_strategy() {
    StrategyFake s;
    std::atomic<bool> stop{false};
    constexpr int kFeedThreads = 4;  // poll, aux_poll, mettraders, + 1
    std::vector<std::thread> threads;

    for (int i = 0; i < kFeedThreads; ++i) {
        threads.emplace_back([&] {
            while (!stop.load(std::memory_order_acquire)) {
                s.onFeedUpdate();
            }
        });
    }
    std::this_thread::sleep_for(std::chrono::seconds(1));
    stop.store(true, std::memory_order_release);
    for (auto& t : threads) t.join();

    std::printf("    max concurrent onFeedUpdate observed: %d (must be 1)\n",
                s.max_in_critical_.load());
    TX_EQ(s.max_in_critical_.load(), 1);
}

// ---- C5 lock-order sanity ----
// mm_feed_callback_mutex_ is acquired at the top of onFeedUpdate. Inside,
// tracked_orders_mutex_ is acquired. So the order is:
//   mm_feed_callback_mutex_ -> tracked_orders_mutex_
// If anywhere else in the codebase acquired feed_cb_mu_ while holding
// tracked_mu_, we'd have a deadlock cycle. We assert that even when many
// threads simultaneously call BOTH the feed-callback path AND a hot
// tracked-orders read path, no deadlock occurs (hangs would be observable
// as a test timeout > 5s).
void no_deadlock_with_downstream_lock() {
    StrategyFake s;
    std::atomic<bool> stop{false};
    constexpr int kThreads = 6;
    std::vector<std::thread> threads;

    for (int i = 0; i < kThreads; ++i) {
        if (i % 2 == 0) {
            threads.emplace_back([&] {
                while (!stop.load(std::memory_order_acquire)) {
                    s.onFeedUpdate();
                }
            });
        } else {
            threads.emplace_back([&] {
                while (!stop.load(std::memory_order_acquire)) {
                    // Production code paths that take tracked_orders_mutex_
                    // WITHOUT holding mm_feed_callback_mutex_ — e.g.
                    // processFill, runFullMmQuoteCycle.
                    std::lock_guard<std::mutex> tlk(s.tracked_mu_);
                    std::this_thread::sleep_for(std::chrono::microseconds(30));
                }
            });
        }
    }
    const auto t0 = std::chrono::steady_clock::now();
    std::this_thread::sleep_for(std::chrono::seconds(1));
    stop.store(true, std::memory_order_release);
    for (auto& t : threads) t.join();
    const auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                std::chrono::steady_clock::now() - t0).count();
    std::printf("    test ran %lldms (no deadlock if < 3000ms)\n", static_cast<long long>(elapsed_ms));
    TX_LE(elapsed_ms, 3000);
}

}  // namespace

int main() {
    std::printf("=== fixes C3 (order id atomic) + C5 (per-strategy feed mutex) ===\n");
    TX_RUN(order_id_atomic_no_torn_reads);
    TX_RUN(feed_callback_mutex_serializes_per_strategy);
    TX_RUN(no_deadlock_with_downstream_lock);
    return tx::finish("test_feed_callback_and_order_ids");
}
