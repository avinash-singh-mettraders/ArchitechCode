// =============================================================================
// REGRESSION TEST — Paired-pull on margin reject (2026-06-12)
// =============================================================================
//
// Contract (production: strategy::MakeMarketStrategy::on_reject +
// mmRejectReasonShouldPullPairedLeg + enqueueRestCancelOnMover):
//
//   The desk must never be left showing a one-sided quote after a margin-style
//   rejection (it is paid to show both sides together). So when one leg is
//   rejected for a "pull-paired" reason, on_reject also cancels the OPPOSITE
//   (paired) leg. Requirements modeled here, one-for-one with the production code:
//
//     1. The reason classifier triggers on the structured INSUFFICIENT_MARGIN
//        error code, OR a case-insensitive "margin" substring in the free-text
//        message (some venues only put the cause in text). It is a single
//        catch-all extension point: any future reason added there pulls the pair.
//     2. Unrelated rejects (rate limit, generic errors) do NOT pull the pair.
//     3. on_reject zeroes ONLY the rejected side's id, then enqueues a cancel for
//        the opposite side's still-live id. The rejected side is never re-cancelled.
//     4. If the opposite leg is already absent (id 0), nothing is enqueued.
//     5. The cancel is posted to the global mover capturing the strategy NAME,
//        not `this`. When the job runs it re-resolves the strategy by name; if the
//        strategy was torn down first, the lookup returns null and the job is a
//        no-op — never a use-after-free. This is the segfault-safety guarantee the
//        user explicitly required.
//
// As with the rest of this suite, the contract is reproduced in isolation (no
// link against the engine) so it stays fast and self-contained, and so the
// teardown-race case is a real UAF witness for ThreadSanitizer/AddressSanitizer.

#include "test_helpers.h"

#include <algorithm>
#include <atomic>
#include <cctype>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

namespace {

using OrderId = long long;

enum class Side { BUY, SELL };

// Mirror of the subset of core::ErrorCode the classifier inspects.
enum class ErrorCode { NONE, INSUFFICIENT_MARGIN, RATE_LIMITED, GENERIC };

// Mirror of the fields of events::OrderEventData that on_reject / the classifier read.
struct RejectEvent {
    OrderId order_id{0};
    Side side{Side::BUY};
    ErrorCode error_code{ErrorCode::NONE};
    std::string error_message;
};

// Faithful reproduction of MakeMarketStrategy::mmRejectReasonShouldPullPairedLeg.
bool shouldPullPairedLeg(const RejectEvent& reject) {
    if (reject.error_code == ErrorCode::INSUFFICIENT_MARGIN) {
        return true;
    }
    std::string msg = reject.error_message;
    std::transform(msg.begin(), msg.end(), msg.begin(),
                   [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    if (msg.find("margin") != std::string::npos) {
        return true;
    }
    return false;
}

// In-isolation stand-in for the global MmOrderMover: a FIFO of name-capturing jobs.
// enqueueRestCancelOnMover captures the strategy NAME (never `this`); the job
// re-resolves through the registry at run time.
class MockMover {
public:
    void enqueue(std::function<void()> fn) {
        std::lock_guard<std::mutex> lk(mu_);
        queue_.push_back(std::move(fn));
    }
    // Run all queued jobs (single-threaded drain, like the real mover worker).
    void drain() {
        for (;;) {
            std::function<void()> fn;
            {
                std::lock_guard<std::mutex> lk(mu_);
                if (queue_.empty()) {
                    return;
                }
                fn = std::move(queue_.front());
                queue_.pop_front();
            }
            fn();
        }
    }
    std::size_t size() {
        std::lock_guard<std::mutex> lk(mu_);
        return queue_.size();
    }

private:
    std::mutex mu_;
    std::deque<std::function<void()>> queue_;
};

struct CancelRecord {
    OrderId oid{0};
    Side side{Side::BUY};
    std::string reason;
};

class MockStrategy;

// Stand-in for StrategyManager: name -> live strategy. getStrategy returns null if
// the strategy was torn down — exactly the lifetime guard the production lambda uses.
class MockRegistry {
public:
    static MockRegistry& instance() {
        static MockRegistry inst;
        return inst;
    }
    void add(const std::string& name, std::shared_ptr<MockStrategy> s) {
        std::lock_guard<std::mutex> lk(mu_);
        map_[name] = std::move(s);
    }
    void remove(const std::string& name) {
        std::lock_guard<std::mutex> lk(mu_);
        map_.erase(name);
    }
    std::shared_ptr<MockStrategy> getStrategy(const std::string& name) {
        std::lock_guard<std::mutex> lk(mu_);
        auto it = map_.find(name);
        return it == map_.end() ? nullptr : it->second;
    }

private:
    std::mutex mu_;
    std::unordered_map<std::string, std::shared_ptr<MockStrategy>> map_;
};

// Mock market-making stack. bid/ask ids are atomic (as in production); cancels it
// receives are recorded. guarded_ is owned heap state so any post-teardown access
// is a real UAF witness for the sanitizers.
class MockStrategy {
public:
    MockStrategy(std::string name, MockMover& mover)
        : name_(std::move(name)), mover_(mover), guarded_(256, 0) {}

    const std::string& name() const { return name_; }

    void setLegs(OrderId bid, OrderId ask) {
        bid_oid_.store(bid, std::memory_order_release);
        ask_oid_.store(ask, std::memory_order_release);
    }
    OrderId bidOid() const { return bid_oid_.load(std::memory_order_acquire); }
    OrderId askOid() const { return ask_oid_.load(std::memory_order_acquire); }

    // Mirror of enqueueRestCancelOnMover: capture NAME + oid + side, re-resolve at run.
    void enqueueRestCancel(OrderId local_oid, Side side, std::string reason) {
        if (local_oid == 0) {
            return;
        }
        const std::string name = name_;
        mover_.enqueue([name, local_oid, side, reason = std::move(reason)]() {
            auto sp = MockRegistry::instance().getStrategy(name);
            if (!sp) {
                return;  // torn down -> no-op, never UAF.
            }
            sp->recordCancel(local_oid, side, reason);
        });
    }

    void recordCancel(OrderId oid, Side side, const std::string& reason) {
        // Touch guarded heap state, like the real strategy touching members.
        guarded_[static_cast<std::size_t>(oid) % guarded_.size()] += 1;
        std::lock_guard<std::mutex> lk(rec_mu_);
        cancels_.push_back(CancelRecord{oid, side, reason});
    }

    std::vector<CancelRecord> cancels() {
        std::lock_guard<std::mutex> lk(rec_mu_);
        return cancels_;
    }

    // Faithful reproduction of the on_reject paired-pull tail.
    void on_reject(const RejectEvent& reject) {
        // Zero ONLY the rejected side's id (production does this before the pull).
        if (reject.order_id == bid_oid_.load(std::memory_order_acquire) &&
            reject.side == Side::BUY) {
            bid_oid_.store(0, std::memory_order_release);
        }
        if (reject.order_id == ask_oid_.load(std::memory_order_acquire) &&
            reject.side == Side::SELL) {
            ask_oid_.store(0, std::memory_order_release);
        }

        if (!shouldPullPairedLeg(reject)) {
            return;
        }
        const Side opposite_side = (reject.side == Side::BUY) ? Side::SELL : Side::BUY;
        const OrderId opposite_oid =
            (opposite_side == Side::BUY) ? bid_oid_.load(std::memory_order_acquire)
                                         : ask_oid_.load(std::memory_order_acquire);
        if (opposite_oid != 0) {
            enqueueRestCancel(opposite_oid, opposite_side, "paired_pull_on_margin_reject");
        }
    }

private:
    std::string name_;
    MockMover& mover_;
    std::atomic<OrderId> bid_oid_{0};
    std::atomic<OrderId> ask_oid_{0};
    std::mutex rec_mu_;
    std::vector<CancelRecord> cancels_;
    std::vector<int> guarded_;
};

// (1) Classifier triggers on margin reasons (code or free text, case-insensitive)
// and not on unrelated reasons.
void classifier_matches_only_margin_reasons() {
    TX_REQUIRE(shouldPullPairedLeg({1, Side::BUY, ErrorCode::INSUFFICIENT_MARGIN, ""}));
    TX_REQUIRE(shouldPullPairedLeg({1, Side::BUY, ErrorCode::GENERIC, "margin breach"}));
    TX_REQUIRE(shouldPullPairedLeg({1, Side::BUY, ErrorCode::GENERIC, "Insufficient Margin"}));
    TX_REQUIRE(shouldPullPairedLeg({1, Side::BUY, ErrorCode::GENERIC, "MARGIN CALL"}));
    // Negatives.
    TX_REQUIRE(!shouldPullPairedLeg({1, Side::BUY, ErrorCode::NONE, ""}));
    TX_REQUIRE(!shouldPullPairedLeg({1, Side::BUY, ErrorCode::RATE_LIMITED, "rate limited"}));
    TX_REQUIRE(!shouldPullPairedLeg({1, Side::BUY, ErrorCode::GENERIC, "price out of bounds"}));
}

// (3) Margin reject on the BUY leg pulls ONLY the opposite (SELL) leg.
void margin_reject_pulls_opposite_leg_only() {
    MockMover mover;
    auto s = std::make_shared<MockStrategy>("mm_req_XAU_PERP_test", mover);
    MockRegistry::instance().add(s->name(), s);
    s->setLegs(/*bid*/ 111, /*ask*/ 222);

    s->on_reject({/*order_id*/ 111, Side::BUY, ErrorCode::INSUFFICIENT_MARGIN, "insufficient margin"});
    mover.drain();

    const auto cancels = s->cancels();
    TX_EQ(static_cast<int>(cancels.size()), 1);
    TX_REQUIRE(cancels[0].oid == 222);
    TX_REQUIRE(cancels[0].side == Side::SELL);
    // Rejected side id was cleared; opposite remained until its cancel was issued.
    TX_REQUIRE(s->bidOid() == 0);

    MockRegistry::instance().remove(s->name());
}

// (3, mirror) Margin reject on the SELL leg pulls ONLY the opposite (BUY) leg.
void margin_reject_pulls_opposite_leg_sell_side() {
    MockMover mover;
    auto s = std::make_shared<MockStrategy>("mm_req_XAU_PERP_test2", mover);
    MockRegistry::instance().add(s->name(), s);
    s->setLegs(/*bid*/ 333, /*ask*/ 444);

    s->on_reject({/*order_id*/ 444, Side::SELL, ErrorCode::GENERIC, "Margin breach on account"});
    mover.drain();

    const auto cancels = s->cancels();
    TX_EQ(static_cast<int>(cancels.size()), 1);
    TX_REQUIRE(cancels[0].oid == 333);
    TX_REQUIRE(cancels[0].side == Side::BUY);
    TX_REQUIRE(s->askOid() == 0);

    MockRegistry::instance().remove(s->name());
}

// (2) Unrelated reject pulls nothing.
void unrelated_reject_pulls_nothing() {
    MockMover mover;
    auto s = std::make_shared<MockStrategy>("mm_req_EURUSD_PERP_test", mover);
    MockRegistry::instance().add(s->name(), s);
    s->setLegs(/*bid*/ 11, /*ask*/ 22);

    s->on_reject({/*order_id*/ 11, Side::BUY, ErrorCode::RATE_LIMITED, "rate limited"});
    mover.drain();

    TX_EQ(static_cast<int>(s->cancels().size()), 0);
    TX_EQ(static_cast<int>(mover.size()), 0);

    MockRegistry::instance().remove(s->name());
}

// (4) Margin reject with no live opposite leg enqueues nothing (and never crashes).
void margin_reject_no_opposite_leg_is_noop() {
    MockMover mover;
    auto s = std::make_shared<MockStrategy>("mm_req_WTIOIL_PERP_test", mover);
    MockRegistry::instance().add(s->name(), s);
    s->setLegs(/*bid*/ 55, /*ask*/ 0);  // opposite (ask) already absent

    s->on_reject({/*order_id*/ 55, Side::BUY, ErrorCode::INSUFFICIENT_MARGIN, "insufficient margin"});
    mover.drain();

    TX_EQ(static_cast<int>(s->cancels().size()), 0);

    MockRegistry::instance().remove(s->name());
}

// (5) Teardown race: the strategy is destroyed BEFORE the queued paired-pull cancel
// runs. The job must re-resolve to null and be a clean no-op — no use-after-free.
void teardown_before_cancel_runs_is_safe() {
    MockMover mover;
    {
        auto s = std::make_shared<MockStrategy>("mm_req_SPY_PERP_test", mover);
        MockRegistry::instance().add(s->name(), s);
        s->setLegs(/*bid*/ 77, /*ask*/ 88);
        s->on_reject({/*order_id*/ 77, Side::BUY, ErrorCode::INSUFFICIENT_MARGIN, "insufficient margin"});
        // Job is queued but NOT yet drained. Tear the strategy down now.
        MockRegistry::instance().remove(s->name());
    }  // shared_ptr drops; strategy freed while the cancel job is still queued.

    // Draining must not touch freed memory (lookup returns null -> no-op).
    mover.drain();
    TX_EQ(static_cast<int>(mover.size()), 0);
}

// (5, stress) Many strategies rejecting + tearing down concurrently with a mover
// draining on another thread. A real UAF would be caught by ASan/TSan here.
void concurrent_reject_and_teardown_no_uaf() {
    MockMover mover;
    std::atomic<bool> stop{false};
    std::thread drainer([&mover, &stop] {
        while (!stop.load(std::memory_order_acquire)) {
            mover.drain();
            std::this_thread::yield();
        }
    });

    for (int i = 0; i < 500; ++i) {
        const std::string name = "mm_req_stress_" + std::to_string(i);
        auto s = std::make_shared<MockStrategy>(name, mover);
        MockRegistry::instance().add(name, s);
        s->setLegs(100 + i, 200 + i);
        s->on_reject({100 + i, Side::BUY, ErrorCode::INSUFFICIENT_MARGIN, "insufficient margin"});
        std::this_thread::yield();
        MockRegistry::instance().remove(name);  // teardown races the drainer
        s.reset();
    }

    stop.store(true, std::memory_order_release);
    drainer.join();
    mover.drain();
    TX_EQ(static_cast<int>(mover.size()), 0);
}

}  // namespace

int main() {
    TX_RUN(classifier_matches_only_margin_reasons);
    TX_RUN(margin_reject_pulls_opposite_leg_only);
    TX_RUN(margin_reject_pulls_opposite_leg_sell_side);
    TX_RUN(unrelated_reject_pulls_nothing);
    TX_RUN(margin_reject_no_opposite_leg_is_noop);
    TX_RUN(teardown_before_cancel_runs_is_safe);
    TX_RUN(concurrent_reject_and_teardown_no_uaf);
    return ::tx::finish("test_paired_pull_on_reject");
}
