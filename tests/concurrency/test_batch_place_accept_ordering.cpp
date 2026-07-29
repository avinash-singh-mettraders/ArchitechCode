// =============================================================================
// REGRESSION TEST — async batch place: accept-counter integrity (Option B, 2026-07-13)
// =============================================================================
//
// The async batch place seam (market_maker.async_batch_submit) changes the order
// in which work happens relative to the legacy synchronous path:
//
//   legacy (flag OFF):   for each leg:  reserve -> fire wire (blocking HTTP) ->
//                                       ORDER_ACCEPTED dispatched inline
//                                       (accept always follows *that* leg's reserve,
//                                        one leg fully done before the next starts)
//
//   batch  (flag ON):    reserve ALL legs (pending += N)  ->  fire ALL wires
//                        concurrently (Platform::placeOrdersConcurrent) ->
//                        ORDER_ACCEPTED for each leg dispatched as its HTTP
//                        completes — possibly OUT OF ORDER and overlapping.
//
// The safety property that must hold in BOTH paths: every ORDER_ACCEPTED matches a
// leg that was reserved *before* any wire fired, and the pending-accept counter
// returns to exactly zero regardless of the order/concurrency of completions. If a
// wire could fire (and thus an accept could arrive) for a leg that was never
// reserved, the counter would go negative / a leg would be adopted with no
// reservation to balance it — the exact class of bug that corrupts the DESK_RECOVERY
// gate. This test models that contract in isolation (repo idiom) and runs the
// concurrent-completion case under hostile scheduling for -DENABLE_TSAN=ON.

#include "test_helpers.h"

#include <algorithm>
#include <atomic>
#include <mutex>
#include <random>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

namespace {

// Minimal model of the strategy-side bookkeeping the batch seam touches: a
// pending-accept counter (production: mm_pending_accepts_, std::atomic<int>) and a
// per-client-id leg state map guarded by the strategy mutex.
class LegBook {
public:
    enum class State { UNKNOWN, PLACE_PENDING, ADOPTED };

    // Called by the mover BEFORE any wire fires (submit_order_no_dispatch path).
    void reserve(const std::string& cid) {
        std::lock_guard<std::mutex> lk(mu_);
        legs_[cid] = State::PLACE_PENDING;
        pending_.fetch_add(1, std::memory_order_acq_rel);
    }

    // Called when ORDER_ACCEPTED is dispatched for cid (Platform::placeOrdersConcurrent
    // -> onOrderAccepted). May run on an arbitrary thread, in an arbitrary order.
    void accept(const std::string& cid) {
        std::lock_guard<std::mutex> lk(mu_);
        auto it = legs_.find(cid);
        if (it == legs_.end() || it->second == State::UNKNOWN) {
            // Bug witness: an accept arrived for a leg that was never reserved.
            accepts_without_reservation_.fetch_add(1, std::memory_order_relaxed);
            return;
        }
        if (it->second == State::ADOPTED) {
            double_accepts_.fetch_add(1, std::memory_order_relaxed);
            return;
        }
        it->second = State::ADOPTED;
        pending_.fetch_sub(1, std::memory_order_acq_rel);
    }

    int pending() const { return pending_.load(std::memory_order_acquire); }
    int accepts_without_reservation() const {
        return accepts_without_reservation_.load(std::memory_order_relaxed);
    }
    int double_accepts() const { return double_accepts_.load(std::memory_order_relaxed); }

    State state(const std::string& cid) {
        std::lock_guard<std::mutex> lk(mu_);
        auto it = legs_.find(cid);
        return it == legs_.end() ? State::UNKNOWN : it->second;
    }

private:
    std::mutex mu_;
    std::unordered_map<std::string, State> legs_;
    std::atomic<int> pending_{0};
    std::atomic<int> accepts_without_reservation_{0};
    std::atomic<int> double_accepts_{0};
};

// -----------------------------------------------------------------------------
// 1. Batch path: reserve both legs up front, then fire concurrent overlapping
//    accepts. Counter must always settle to 0 and both legs adopt, no matter how
//    the two completions interleave.
// -----------------------------------------------------------------------------
void batch_concurrent_accepts_balance_counter() {
    constexpr int kIters = 20000;
    for (int i = 0; i < kIters; ++i) {
        LegBook book;
        const std::string bid = "bid_" + std::to_string(i);
        const std::string ask = "ask_" + std::to_string(i);

        // Reserve ALL legs before any wire fires (the batch contract).
        book.reserve(bid);
        book.reserve(ask);
        TX_EQ(book.pending(), 2);

        // Two HTTP completions land concurrently, order not guaranteed.
        std::thread ta([&] { book.accept(bid); });
        std::thread tb([&] { book.accept(ask); });
        ta.join();
        tb.join();

        TX_EQ(book.pending(), 0);
        TX_REQUIRE(book.state(bid) == LegBook::State::ADOPTED);
        TX_REQUIRE(book.state(ask) == LegBook::State::ADOPTED);
        TX_EQ(book.accepts_without_reservation(), 0);
        TX_EQ(book.double_accepts(), 0);
    }
}

// -----------------------------------------------------------------------------
// 2. Out-of-order (reverse) completion: the second-reserved leg's accept lands
//    first. Still balances — proves the counter is order-independent, not FIFO.
// -----------------------------------------------------------------------------
void batch_reverse_order_accepts_balance() {
    LegBook book;
    book.reserve("bid");
    book.reserve("ask");
    // ask completes first, then bid.
    book.accept("ask");
    book.accept("bid");
    TX_EQ(book.pending(), 0);
    TX_REQUIRE(book.state("bid") == LegBook::State::ADOPTED);
    TX_REQUIRE(book.state("ask") == LegBook::State::ADOPTED);
    TX_EQ(book.accepts_without_reservation(), 0);
}

// -----------------------------------------------------------------------------
// 3. Flag OFF (legacy sequential) vs flag ON (batch) produce identical final
//    book state: same legs adopted, same zero pending. This is the "flag off ==
//    current behavior" contract.
// -----------------------------------------------------------------------------
void sequential_and_batch_reach_identical_state() {
    // Legacy: reserve leg, accept leg, next leg. Fully serialized.
    LegBook legacy;
    legacy.reserve("bid");
    legacy.accept("bid");
    legacy.reserve("ask");
    legacy.accept("ask");

    // Batch: reserve both, then accept both.
    LegBook batch;
    batch.reserve("bid");
    batch.reserve("ask");
    batch.accept("bid");
    batch.accept("ask");

    TX_EQ(legacy.pending(), batch.pending());
    TX_EQ(legacy.pending(), 0);
    TX_REQUIRE(legacy.state("bid") == batch.state("bid"));
    TX_REQUIRE(legacy.state("ask") == batch.state("ask"));
    TX_REQUIRE(batch.state("bid") == LegBook::State::ADOPTED);
    TX_REQUIRE(batch.state("ask") == LegBook::State::ADOPTED);
}

// -----------------------------------------------------------------------------
// 4. Negative control: prove the witness has teeth. If a wire ever fired (and an
//    accept arrived) for a leg that was NOT reserved first — the failure mode the
//    reserve-before-fire ordering exists to prevent — the model must flag it.
// -----------------------------------------------------------------------------
void unreserved_accept_is_detected() {
    LegBook book;
    book.reserve("bid");
    book.accept("bid");
    book.accept("ask");  // never reserved — must be caught, must NOT go negative
    TX_EQ(book.accepts_without_reservation(), 1);
    TX_EQ(book.pending(), 0);  // counter never dipped below zero
}

// -----------------------------------------------------------------------------
// 5. Many legs / many completion threads, shuffled dispatch order under hostile
//    scheduling — stress + TSAN witness for the concurrent accept path.
// -----------------------------------------------------------------------------
void many_legs_shuffled_concurrent_accepts() {
    constexpr int kLegs = 64;
    LegBook book;
    std::vector<std::string> cids;
    cids.reserve(kLegs);
    for (int i = 0; i < kLegs; ++i) {
        cids.push_back("leg_" + std::to_string(i));
        book.reserve(cids.back());
    }
    TX_EQ(book.pending(), kLegs);

    // Shuffle so completions dispatch in a random order across threads.
    std::mt19937 rng(12345);
    std::shuffle(cids.begin(), cids.end(), rng);

    std::vector<std::thread> workers;
    workers.reserve(kLegs);
    for (const auto& cid : cids) {
        workers.emplace_back([&book, cid] { book.accept(cid); });
    }
    for (auto& w : workers) w.join();

    TX_EQ(book.pending(), 0);
    TX_EQ(book.accepts_without_reservation(), 0);
    TX_EQ(book.double_accepts(), 0);
    for (int i = 0; i < kLegs; ++i) {
        TX_REQUIRE(book.state("leg_" + std::to_string(i)) == LegBook::State::ADOPTED);
    }
}

}  // namespace

int main() {
    std::printf("=== async batch place: accept-counter integrity (Option B) ===\n");
    TX_RUN(batch_concurrent_accepts_balance_counter);
    TX_RUN(batch_reverse_order_accepts_balance);
    TX_RUN(sequential_and_batch_reach_identical_state);
    TX_RUN(unreserved_accept_is_detected);
    TX_RUN(many_legs_shuffled_concurrent_accepts);
    return tx::finish("test_batch_place_accept_ordering");
}
