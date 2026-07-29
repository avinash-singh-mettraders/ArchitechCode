#include "strategy/MmOrderMover.h"

#include "config/Config.h"
#include "core/LatencyTracker.h"
#include "utils/Logger.h"

#include <chrono>
#include <exception>
#include <unordered_map>
#include <vector>

namespace architect {
namespace strategy {

namespace {
constexpr std::int64_t kMoverWarnMs = 1000;
constexpr std::int64_t kMoverCircuitBreakerMs = 3000;
constexpr int kMoverCircuitBreakerConsecutive = 3;

std::mutex g_mover_circuit_mu;
std::unordered_map<std::string, int> g_mover_slow_streak_by_ax;

// Key used for the single shared worker when per-instrument sharding is OFF.
const std::string kSharedWorkerKey = "";

// Set at the top of runWorker(): identifies any mover worker thread so placement-lease
// acquisition can fail fast on it instead of blocking for seconds.
thread_local bool t_is_mover_thread = false;
// The shard key (AX symbol under sharding ON, "" under sharding OFF) this worker serves.
// Lets the place funnel assert it runs on the symbol's OWN worker.
thread_local std::string t_mover_ax_key;
}  // namespace

MmOrderMover& MmOrderMover::getInstance() {
    static MmOrderMover instance;
    return instance;
}

MmOrderMover::~MmOrderMover() {
    shutdown();
}

bool MmOrderMover::shardingEnabled() {
    int mode = sharding_mode_.load(std::memory_order_acquire);
    if (mode >= 0) {
        return mode == 1;
    }
    // Resolve once from config, then freeze for the process lifetime.
    // CODE DEFAULT = TRUE: per-instrument sharding is the intended production mode. A missing
    // config key must NEVER silently degrade to the single global worker (that reintroduces the
    // cross-symbol head-of-line blocking the July-9 log proved). Only an explicit `false` in
    // config downgrades to SINGLE_GLOBAL.
    bool enabled = true;
    bool from_config = false;
    try {
        auto& cfg = config::Config::getInstance();
        from_config = cfg.has("market_maker.mover_shard_per_instrument");
        enabled = cfg.getBool("market_maker.mover_shard_per_instrument", true);
    } catch (...) {
        enabled = true;      // fail SAFE toward sharding, not toward the degraded single worker
        from_config = false;
    }
    int expected = -1;
    const int desired = enabled ? 1 : 0;
    if (sharding_mode_.compare_exchange_strong(expected, desired, std::memory_order_acq_rel)) {
        if (utils::Logger::isInitialized()) {
            const char* source = from_config ? "config" : "code-default";
            if (enabled) {
                utils::Logger::getInstance().info(
                    "[MM_ORDER_MOVER] mode=PER_INSTRUMENT (workers created lazily per AX) "
                    "market_maker.mover_shard_per_instrument={} source={}",
                    "true",
                    source);
            } else {
                utils::Logger::getInstance().warn(
                    "[MM_ORDER_MOVER] mode=SINGLE_GLOBAL \u2014 DEGRADED, all symbols share one "
                    "worker market_maker.mover_shard_per_instrument={} source={}",
                    "false",
                    source);
            }
        }
        return desired == 1;
    }
    // Lost the race; use whatever won.
    return sharding_mode_.load(std::memory_order_acquire) == 1;
}

MmOrderMover::Worker* MmOrderMover::workerFor(const std::string& ax_symbol) {
    const std::string key = shardingEnabled() ? ax_symbol : kSharedWorkerKey;
    std::lock_guard<std::mutex> lk(workers_mutex_);
    auto it = workers_.find(key);
    if (it != workers_.end()) {
        return it->second.get();
    }
    auto w = std::make_unique<Worker>();
    w->ax_key = key;
    Worker* raw = w.get();
    workers_.emplace(key, std::move(w));
    raw->thread = std::thread([this, raw]() { runWorker(raw); });
    return raw;
}

void MmOrderMover::enqueueForAx(const std::string& ax_symbol, Action action) {
    if (!action) {
        return;
    }
    if (shutdown_called_.load(std::memory_order_acquire)) {
        return;
    }
    Worker* w = workerFor(ax_symbol);
    if (!w) {
        return;
    }
    {
        std::lock_guard<std::mutex> lk(w->mu);
        w->queue.emplace_back(QueuedAction{ax_symbol, std::move(action)});
    }
    w->cv.notify_one();
}

void MmOrderMover::shutdown() {
    bool expected = false;
    if (!shutdown_called_.compare_exchange_strong(expected, true)) {
        return;
    }
    // Snapshot the workers under the registry lock, then signal + join each without holding
    // it (a worker's runWorker never touches workers_mutex_, but joining while holding it is
    // needless contention).
    std::vector<Worker*> to_join;
    {
        std::lock_guard<std::mutex> lk(workers_mutex_);
        to_join.reserve(workers_.size());
        for (auto& kv : workers_) {
            to_join.push_back(kv.second.get());
        }
    }
    for (Worker* w : to_join) {
        {
            std::lock_guard<std::mutex> lk(w->mu);
            w->stop.store(true, std::memory_order_release);
            // Drop unprocessed actions: callers never block on results, and orders we'd
            // cancel/place during teardown race the gateway's own session cleanup anyway.
            w->queue.clear();
        }
        w->cv.notify_all();
    }
    for (Worker* w : to_join) {
        if (w->thread.joinable()) {
            w->thread.join();
        }
    }
}

std::size_t MmOrderMover::workerCount() const {
    std::lock_guard<std::mutex> lk(const_cast<std::mutex&>(workers_mutex_));
    return workers_.size();
}

bool MmOrderMover::isCurrentThreadMover() {
    return t_is_mover_thread;
}

bool MmOrderMover::isCurrentThreadMoverForAx(const std::string& ax_symbol) {
    return t_is_mover_thread && !ax_symbol.empty() && t_mover_ax_key == ax_symbol;
}

void MmOrderMover::runWorker(Worker* w) {
    t_is_mover_thread = true;
    t_mover_ax_key = w->ax_key;
    while (true) {
        QueuedAction qa;
        {
            std::unique_lock<std::mutex> lk(w->mu);
            w->cv.wait(lk, [w] {
                return w->stop.load(std::memory_order_acquire) || !w->queue.empty();
            });
            if (w->stop.load(std::memory_order_acquire) && w->queue.empty()) {
                break;
            }
            qa = std::move(w->queue.front());
            w->queue.pop_front();
        }

        // Strict-serial WITHIN this worker: this action must complete before the next item on
        // the SAME worker is dequeued. With sharding ON, that means per-instrument serial;
        // different instruments run on their own workers concurrently.
        const auto t0 = std::chrono::steady_clock::now();
        try {
            qa.fn();
        } catch (const std::exception& e) {
            if (utils::Logger::isInitialized()) {
                utils::Logger::getInstance().warn(
                    "[MM_ORDER_MOVER] ax={} action threw: {}",
                    qa.ax_label.empty() ? std::string("(unset)") : qa.ax_label,
                    e.what());
            }
        } catch (...) {
            if (utils::Logger::isInitialized()) {
                utils::Logger::getInstance().warn(
                    "[MM_ORDER_MOVER] ax={} action threw unknown exception",
                    qa.ax_label.empty() ? std::string("(unset)") : qa.ax_label);
            }
        }
        const auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                    std::chrono::steady_clock::now() - t0)
                                    .count();
        const std::string ax_key = qa.ax_label.empty() ? std::string("(unset)") : qa.ax_label;
        if (elapsed_ms >= kMoverWarnMs && utils::Logger::isInitialized()) {
            utils::Logger::getInstance().info(
                "[MM_ORDER_MOVER] ax={} action elapsed={}ms (serial within worker; queue may "
                "back up if this stays high)",
                ax_key,
                core::latencyDisplayMs(elapsed_ms));
        }
        if (elapsed_ms >= kMoverCircuitBreakerMs) {
            int streak = 0;
            {
                std::lock_guard<std::mutex> lk(g_mover_circuit_mu);
                streak = ++g_mover_slow_streak_by_ax[ax_key];
            }
            if (streak >= kMoverCircuitBreakerConsecutive && utils::Logger::isInitialized()) {
                utils::Logger::getInstance().warn(
                    "[MM_MOVER_CIRCUIT_BREAKER] ax={} consecutive_slow_actions={} "
                    "last_elapsed_ms={} warn_ms={} pause_ms={} — alert only (quoting not paused)",
                    ax_key,
                    streak,
                    core::latencyDisplayMs(elapsed_ms),
                    kMoverWarnMs,
                    kMoverCircuitBreakerMs);
            }
        } else {
            std::lock_guard<std::mutex> lk(g_mover_circuit_mu);
            g_mover_slow_streak_by_ax[ax_key] = 0;
        }
    }
}

}  // namespace strategy
}  // namespace architect
