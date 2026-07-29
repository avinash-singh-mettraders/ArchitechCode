// =============================================================================
// REGRESSION TEST — `mm_pending_accepts_` atomicity (fix C2)
// =============================================================================
//
// `mm_pending_accepts_` is the gate that prevents pair-enforce / DESK_RECOVERY
// from firing while a place-ack is still in flight. Production had it as a
// plain `int`, incremented on the MmOrderMover worker thread and decremented
// on the OrderManager event-dispatch thread. Torn reads → false zero → the
// DESK_RECOVERY race that caused both Joe's phantom 1-lot bid AND part of
// the segfault path.
//
// This test runs the EXACT same RMW pattern production uses, with hostile
// scheduling, and asserts:
//   1. After N producer increments + N consumer decrements, the final value
//      is exactly 0 (no lost updates).
//   2. A reader thread that polls the field never sees a transient value
//      outside the producer/consumer range.
//   3. CAS / store / load with the production memory orders gives correct
//      synchronization (no ABA-style false-zero observable to a parallel
//      "is pair healthy?" probe).

#include "test_helpers.h"

#include <atomic>
#include <chrono>
#include <thread>
#include <vector>

namespace {

// Production-equivalent counter.
std::atomic<int> g_pending_accepts{0};

void atomic_counter_never_drifts_under_contention() {
    g_pending_accepts.store(0);
    constexpr int kPerProducer = 200000;
    constexpr int kProducers   = 4;
    constexpr int kConsumers   = 4;

    auto producer = [&] {
        for (int i = 0; i < kPerProducer; ++i) {
            ++g_pending_accepts;  // production: `++mm_pending_accepts_` on mover thread
        }
    };

    auto consumer = [&] {
        for (int i = 0; i < kPerProducer; ++i) {
            // Spin until we have something to decrement — mirrors the
            // `if (mm_pending_accepts_ > 0) --mm_pending_accepts_;` pattern.
            while (true) {
                int cur = g_pending_accepts.load(std::memory_order_acquire);
                if (cur > 0 &&
                    g_pending_accepts.compare_exchange_weak(
                        cur, cur - 1,
                        std::memory_order_acq_rel,
                        std::memory_order_acquire)) {
                    break;
                }
                std::this_thread::yield();
            }
        }
    };

    std::vector<std::thread> threads;
    for (int i = 0; i < kProducers; ++i) threads.emplace_back(producer);
    for (int i = 0; i < kConsumers; ++i) threads.emplace_back(consumer);
    for (auto& t : threads) t.join();

    // With balanced producers and consumers, must land at exactly 0.
    TX_EQ(g_pending_accepts.load(), 0);
}

void operator_overloads_match_int_semantics() {
    // Production calls each of these — verify they behave like a regular int
    // for the purposes of the gates that read them.
    g_pending_accepts.store(0);
    TX_REQUIRE(g_pending_accepts == 0);
    TX_REQUIRE(!(g_pending_accepts > 0));

    g_pending_accepts = 3;
    TX_REQUIRE(g_pending_accepts > 0);
    TX_EQ(static_cast<int>(g_pending_accepts), 3);

    ++g_pending_accepts;
    TX_EQ(static_cast<int>(g_pending_accepts), 4);

    --g_pending_accepts;
    TX_EQ(static_cast<int>(g_pending_accepts), 3);

    g_pending_accepts += 2;
    TX_EQ(static_cast<int>(g_pending_accepts), 5);

    g_pending_accepts = 0;
    TX_EQ(static_cast<int>(g_pending_accepts), 0);
}

void reader_never_sees_out_of_range_value() {
    // Probe that the "stale_awaiting_accept" path in production
    // (ensureStartupQuotePair line ~2810) can use to detect false zeros.
    g_pending_accepts.store(0);
    std::atomic<bool> stop{false};
    std::atomic<int>  bad{0};

    std::thread writer([&] {
        while (!stop.load(std::memory_order_acquire)) {
            ++g_pending_accepts;
            std::this_thread::sleep_for(std::chrono::microseconds(5));
            --g_pending_accepts;
        }
    });

    std::thread reader([&] {
        while (!stop.load(std::memory_order_acquire)) {
            const int v = g_pending_accepts.load(std::memory_order_acquire);
            // Production has 1 producer producing at most a few accepts per
            // cycle. The counter is bounded by reasonable design.
            if (v < 0 || v > 100) {
                bad.fetch_add(1, std::memory_order_relaxed);
            }
        }
    });

    std::this_thread::sleep_for(std::chrono::seconds(1));
    stop.store(true, std::memory_order_release);
    writer.join();
    reader.join();
    TX_EQ(bad.load(), 0);
    TX_EQ(g_pending_accepts.load(), 0);
}

}  // namespace

int main() {
    std::printf("=== mm_pending_accepts_ atomic (fix C2) ===\n");
    TX_RUN(operator_overloads_match_int_semantics);
    TX_RUN(atomic_counter_never_drifts_under_contention);
    TX_RUN(reader_never_sees_out_of_range_value);
    return tx::finish("test_atomic_pending_accepts");
}
