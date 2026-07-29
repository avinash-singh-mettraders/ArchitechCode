// =============================================================================
// REGRESSION TEST — Tim Izzo max_position spec (2026-05-26)
// =============================================================================
//
// Tim placement: quote grow side while |net_po| < max_po (open orders excluded).
// Tim post-fill: on every fill, if |net_po| >= max_po pull all grow-side legs on AX.
// Tim acceptable breach: |net| may exceed max only up to
//   max + sum(grow-side qty on book) when resting orders fill together.
// Tim unacceptable (Joe): |net| > max + resting without that resting explanation.
//
// This test models those rules (not full MakeMarketStrategy). Production must
// keep placement net-only; breach logging uses sumWorstCaseInflightLegQtyForSymbol.

#include "test_helpers.h"

#include <atomic>
#include <chrono>
#include <mutex>
#include <random>
#include <thread>
#include <vector>

namespace {

struct TimCapModel {
    std::mutex mu_;
    long long net_po_{0};
    long long resting_grow_qty_{0};  // total bid qty on book (all stacks)

    long long cap_{500};
    long long max_abs_net_{0};
    long long max_tim_bound_{0};
    long long max_unexpected_over_bound_{0};
};

bool timMayPlaceGrowBid(TimCapModel& m, long long qty, long long cap) {
    std::lock_guard<std::mutex> lk(m.mu_);
    if (m.net_po_ >= cap) {
        return false;
    }
    m.resting_grow_qty_ += qty;
    return true;
}

void timOnFill(TimCapModel& m, long long fill_qty, long long cap) {
    std::lock_guard<std::mutex> lk(m.mu_);
    m.resting_grow_qty_ = std::max<long long>(0, m.resting_grow_qty_ - fill_qty);
    m.net_po_ += fill_qty;
    if (m.net_po_ >= cap) {
        m.resting_grow_qty_ = 0;  // pull all bids
    }
    const long long abs_net = std::llabs(m.net_po_);
    const long long tim_bound = cap + m.resting_grow_qty_;
    if (abs_net > m.max_abs_net_) {
        m.max_abs_net_ = abs_net;
    }
    if (tim_bound > m.max_tim_bound_) {
        m.max_tim_bound_ = tim_bound;
    }
    if (abs_net > tim_bound && abs_net - tim_bound > m.max_unexpected_over_bound_) {
        m.max_unexpected_over_bound_ = abs_net - tim_bound;
    }
}

void scenario_tim_multi_stack_placement() {
    TimCapModel book;
    constexpr long long kCap = 3;
    constexpr long long kLeg = 1;
    constexpr int kStacks = 4;
    constexpr int kSeconds = 2;
    std::atomic<bool> stop{false};
    std::atomic<long long> places{0};

    // TSAN fix: the loop index `s` was captured by reference and read from each
    // worker thread while the main thread mutated it (++s) — a data race, and a
    // use-after-scope once the loop exited. Hand each worker a unique seed through
    // a shared std::atomic<int> instead of the plain stack int.
    std::atomic<int> next_seed{42};
    std::vector<std::thread> stack_threads;
    for (int s = 0; s < kStacks; ++s) {
        stack_threads.emplace_back([&] {
            std::mt19937 rng(
                static_cast<unsigned>(next_seed.fetch_add(1, std::memory_order_relaxed)));
            std::uniform_int_distribution<int> jitter_us(50, 400);
            while (!stop.load(std::memory_order_acquire)) {
                if (timMayPlaceGrowBid(book, kLeg, kCap)) {
                    places.fetch_add(1, std::memory_order_relaxed);
                }
                std::this_thread::sleep_for(std::chrono::microseconds(jitter_us(rng)));
            }
        });
    }

    std::thread fill_thread([&] {
        std::mt19937 rng(99);
        std::uniform_int_distribution<int> jitter_us(100, 1200);
        while (!stop.load(std::memory_order_acquire)) {
            std::this_thread::sleep_for(std::chrono::microseconds(jitter_us(rng)));
            bool have_resting = false;
            {
                std::lock_guard<std::mutex> lk(book.mu_);
                have_resting = book.resting_grow_qty_ >= kLeg;
            }
            if (have_resting) {
                timOnFill(book, kLeg, kCap);
            }
        }
    });

    std::this_thread::sleep_for(std::chrono::seconds(kSeconds));
    stop.store(true, std::memory_order_release);
    for (auto& t : stack_threads) {
        t.join();
    }
    fill_thread.join();

    std::printf(
        "    Tim multi-stack (cap=%lld stacks=%d): places=%lld max_|net|=%lld "
        "max_tim_bound=%lld max_unexpected_over_bound=%lld\n",
        kCap,
        kStacks,
        places.load(),
        book.max_abs_net_,
        book.max_tim_bound_,
        book.max_unexpected_over_bound_);

    // With net=2 and cap=3, four 1-lot bids may rest; burst can reach cap + resting.
    TX_LE(book.max_abs_net_, kCap + kStacks);
    TX_EQ(book.max_unexpected_over_bound_, 0);
}

void tim_breach_bound_unit() {
    TimCapModel m;
    m.net_po_ = 4;
    m.resting_grow_qty_ = 1;
    const long long cap = 3;
    const long long abs_net = std::llabs(m.net_po_);
    const long long tim_bound = cap + m.resting_grow_qty_;
    TX_REQUIRE(abs_net <= tim_bound);  // acceptable (XAG-like burst)

    m.resting_grow_qty_ = 0;
    const long long tim_bound2 = cap + m.resting_grow_qty_;
    TX_REQUIRE(abs_net > tim_bound2);  // unexpected when no resting explains over-max
}

}  // namespace

int main() {
    std::printf("=== Tim max_position: net-only place, pull on fill, breach bound ===\n");
    TX_RUN(tim_breach_bound_unit);
    TX_RUN(scenario_tim_multi_stack_placement);
    return tx::finish("test_max_position_no_breach");
}
