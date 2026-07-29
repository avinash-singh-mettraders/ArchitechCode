// =============================================================================
// REGRESSION TEST — Feed-dispatch vs strategy-teardown use-after-free (2026-06-12)
// =============================================================================
//
// Problem (production: marketdata::ExternalFeedManager + strategy::MakeMarketStrategy):
//   notifyCallbacks() copied the registered callbacks under callbacks_mutex_, then
//   released the lock and invoked each callback OUTSIDE the lock. unregisterCallback()
//   (called from ~MakeMarketStrategy) only erased the entry from the live map and
//   returned immediately — it did NOT wait for any in-flight dispatch. So:
//     1. feed thread copies the [this] callback
//     2. teardown thread runs ~MakeMarketStrategy -> unregisterCallback (returns)
//     3. strategy object freed
//     4. feed thread invokes the copied callback -> onFeedUpdate on freed `this`
//     5. SIGSEGV / use-after-free
//
// Fix modeled here (Solution B — in-flight refcount):
//   - Each callback is held via a shared_ptr<CallbackEntry> carrying an atomic
//     in_flight counter.
//   - notifyCallbacks pins the entries and increments in_flight UNDER the lock,
//     then runs the callback outside the lock, then decrements + notifies a
//     condition variable when in_flight hits 0.
//   - unregisterCallback erases the entry under the lock, then BLOCKS on the cv
//     until that entry's in_flight reaches 0. The strategy destructor therefore
//     cannot return (and the object cannot be freed) while a feed thread is still
//     inside the callback.
//
// This file reproduces that exact contract in isolation (no link against the
// engine, matching the rest of the concurrency suite) and asserts under
// ThreadSanitizer/AddressSanitizer:
//   - No UAF when strategies are destroyed concurrently with rapid dispatch.
//   - The dispatch loop keeps running normally after a strategy is destroyed.
//   - unregisterCallback drains an in-flight dispatch and completes in bounded time.

#include "test_helpers.h"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>
#include <utility>
#include <vector>

namespace {

struct FeedQuote {
    double bid{0.0};
    double ask{0.0};
};

using FeedCallback = std::function<void(const FeedQuote&)>;

// Faithful in-isolation reproduction of ExternalFeedManager's callback registry
// with the in-flight-drain UAF fix. The locking/atomic protocol mirrors the
// production code one-for-one.
class FeedManager {
public:
    struct CallbackEntry {
        FeedCallback cb;
        std::atomic<int> in_flight{0};
    };

    int registerCallback(FeedCallback cb) {
        std::lock_guard<std::mutex> lk(mu_);
        const int id = next_id_++;
        auto entry = std::make_shared<CallbackEntry>();
        entry->cb = std::move(cb);
        callbacks_.emplace_back(id, std::move(entry));
        return id;
    }

    void unregisterCallback(int id) {
        std::shared_ptr<CallbackEntry> removed;
        {
            std::lock_guard<std::mutex> lk(mu_);
            for (auto it = callbacks_.begin(); it != callbacks_.end(); ++it) {
                if (it->first == id) {
                    removed = it->second;
                    callbacks_.erase(it);
                    break;
                }
            }
        }
        if (!removed) {
            return;
        }
        std::unique_lock<std::mutex> lk(mu_);
        drain_cv_.wait(lk, [&removed] {
            return removed->in_flight.load(std::memory_order_acquire) == 0;
        });
    }

    void notifyCallbacks(const FeedQuote& quote) {
        std::vector<std::shared_ptr<CallbackEntry>> entries;
        {
            std::lock_guard<std::mutex> lk(mu_);
            entries.reserve(callbacks_.size());
            for (const auto& [id, entry] : callbacks_) {
                (void)id;
                if (entry) {
                    entry->in_flight.fetch_add(1, std::memory_order_acq_rel);
                    entries.push_back(entry);
                }
            }
        }
        for (const auto& entry : entries) {
            entry->cb(quote);
            if (entry->in_flight.fetch_sub(1, std::memory_order_acq_rel) == 1) {
                std::lock_guard<std::mutex> lk(mu_);
                drain_cv_.notify_all();
            }
        }
        dispatch_count_.fetch_add(1, std::memory_order_relaxed);
    }

    long dispatch_count() const { return dispatch_count_.load(std::memory_order_relaxed); }

private:
    std::mutex mu_;
    std::condition_variable drain_cv_;
    std::vector<std::pair<int, std::shared_ptr<CallbackEntry>>> callbacks_;
    int next_id_{1};
    std::atomic<long> dispatch_count_{0};
};

// Mock strategy that registers itself as a feed callback owner and tears itself
// down in its destructor (exactly like MakeMarketStrategy). onFeedUpdate touches
// heap-owned member memory, so any dispatch racing past teardown is a real UAF
// witness for ASan/TSan.
struct MockStrategy {
    FeedManager& feed_;
    int cb_id_{0};
    std::atomic<long> updates_{0};
    std::vector<int> guarded_;  // owned heap state; freed at destruction.

    explicit MockStrategy(FeedManager& feed) : feed_(feed), guarded_(256, 0) {
        cb_id_ = feed_.registerCallback(
            [this](const FeedQuote& q) { onFeedUpdate(q); });
    }

    ~MockStrategy() {
        // Drains any in-flight dispatch BEFORE guarded_ / this are torn down.
        feed_.unregisterCallback(cb_id_);
    }

    void onFeedUpdate(const FeedQuote& q) {
        const long n = updates_.fetch_add(1, std::memory_order_relaxed);
        guarded_[static_cast<std::size_t>(n) % guarded_.size()] +=
            static_cast<int>(q.bid + q.ask);
    }
};

// (1)+(4)+(5): rapid dispatch concurrent with create/destroy. No UAF, dispatch
// keeps running, and every strategy destruction completes (no hang).
void uaf_no_crash_under_concurrent_teardown() {
    FeedManager feed;

    std::atomic<bool> stop{false};
    std::vector<std::thread> dispatchers;
    for (int i = 0; i < 4; ++i) {
        dispatchers.emplace_back([&feed, &stop] {
            FeedQuote q{1.2345, 1.2350};
            while (!stop.load(std::memory_order_acquire)) {
                feed.notifyCallbacks(q);
            }
        });
    }

    const long before = feed.dispatch_count();

    // Continuously create and destroy strategies while dispatch hammers the feed.
    for (int i = 0; i < 400; ++i) {
        auto s = std::make_unique<MockStrategy>(feed);
        std::this_thread::yield();
        s.reset();  // ~MockStrategy -> unregisterCallback drain (must not deadlock)
    }

    stop.store(true, std::memory_order_release);
    for (auto& t : dispatchers) {
        t.join();
    }

    // Dispatch loop made progress throughout (no hang / deadlock).
    TX_REQUIRE(feed.dispatch_count() > before);
}

// (5): after a strategy is destroyed, the dispatch loop continues normally and
// no callback fires for the dead object.
void dispatch_continues_after_strategy_destroyed() {
    FeedManager feed;
    {
        MockStrategy s(feed);
        FeedQuote q{2.0, 2.5};
        feed.notifyCallbacks(q);
        TX_REQUIRE(s.updates_.load(std::memory_order_relaxed) >= 1);
    }  // strategy destroyed here; unregisterCallback drained.

    const long before = feed.dispatch_count();
    FeedQuote q{3.0, 3.5};
    for (int i = 0; i < 1000; ++i) {
        feed.notifyCallbacks(q);  // no live callbacks; must be a clean no-op.
    }
    TX_LE(before + 1000, feed.dispatch_count());
}

// (6): unregisterCallback blocks until the in-flight dispatch finishes, and
// completes in bounded time (no infinite wait, no premature return).
void unregister_drains_in_flight_and_is_bounded() {
    FeedManager feed;

    std::mutex m;
    std::condition_variable entered_cv;
    bool entered = false;
    std::atomic<bool> release{false};
    std::atomic<bool> cb_returned{false};

    const int id = feed.registerCallback([&](const FeedQuote&) {
        {
            std::lock_guard<std::mutex> lk(m);
            entered = true;
        }
        entered_cv.notify_all();
        // Hold the dispatch "in flight" until the test releases it.
        while (!release.load(std::memory_order_acquire)) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        cb_returned.store(true, std::memory_order_release);
    });

    std::thread dispatcher([&feed] {
        FeedQuote q{9.0, 9.0};
        feed.notifyCallbacks(q);
    });

    // Wait until the callback is actually mid-flight.
    {
        std::unique_lock<std::mutex> lk(m);
        entered_cv.wait(lk, [&entered] { return entered; });
    }

    // unregisterCallback must NOT return while the dispatch is in flight, and must
    // return promptly once it completes. Run it on its own thread so we can both
    // assert it is still blocked and bound its completion time.
    std::atomic<bool> unregister_done{false};
    auto t0 = std::chrono::steady_clock::now();
    std::thread unreg([&] {
        feed.unregisterCallback(id);
        unregister_done.store(true, std::memory_order_release);
    });

    // Give it a moment; it must still be blocked because the callback hasn't returned.
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    TX_REQUIRE(!unregister_done.load(std::memory_order_acquire));
    TX_REQUIRE(!cb_returned.load(std::memory_order_acquire));

    // Release the in-flight dispatch; unregister should now drain and return.
    release.store(true, std::memory_order_release);

    // Bounded wait: poll for completion up to a generous ceiling.
    bool done = false;
    for (int i = 0; i < 500 && !done; ++i) {  // <= ~5s
        if (unregister_done.load(std::memory_order_acquire)) {
            done = true;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                          std::chrono::steady_clock::now() - t0)
                          .count();

    unreg.join();
    dispatcher.join();

    TX_REQUIRE(done);                                       // no infinite wait
    TX_REQUIRE(cb_returned.load(std::memory_order_acquire));// drained the dispatch
    TX_LE(elapsed_ms, static_cast<long long>(5000));        // bounded completion
}

}  // namespace

int main() {
    TX_RUN(uaf_no_crash_under_concurrent_teardown);
    TX_RUN(dispatch_continues_after_strategy_destroyed);
    TX_RUN(unregister_drains_in_flight_and_is_bounded);
    return ::tx::finish("test_feed_callback_uaf");
}
