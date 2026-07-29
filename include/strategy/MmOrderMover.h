#pragma once

/**
 * @file MmOrderMover.h
 * @brief Executor for ALL market-making cancel/replace/place ops.
 *
 * History
 * -------
 *   v1 (2026-05-07 morning) — per-AX worker thread per symbol. EURUSD-PERP and JPYUSD-PERP
 *                             could process orders concurrently. Eliminated FEED_DISPATCH_SLOW
 *                             but the user reported "still freezing" symptoms tied to libcurl
 *                             contention between AX workers (all transfers serialised on one
 *                             global CURLM/`multi_mutex_`), so multithreading was removed.
 *   v2 (2026-05-07 evening) — ONE global worker thread, ONE FIFO queue. Strictly serial.
 *                             Latency goes up under burst, correctness is absolute.
 *   v3 (2026-07-09)         — per-AX workers reintroduced, GATED behind
 *                             `market_maker.mover_shard_per_instrument` (default OFF). The v1
 *                             freeze root-cause (global `multi_mutex_`) is gone: each thread now
 *                             drives its OWN per-thread CURLM* (see CurlMultiManager), so
 *                             different instruments' REST ops run truly in parallel. When the
 *                             flag is OFF the behaviour is byte-for-byte v2 (one worker keyed by
 *                             the empty string).
 *
 * Design
 * ------
 *   - Work is posted via `enqueueForAx(ax_symbol, action)`. The post returns in microseconds.
 *   - Sharding OFF: every action runs on ONE global worker in posting order (v2).
 *   - Sharding ON:  each AX symbol gets its OWN worker thread + FIFO. All work for a given AX
 *     stays strictly serial on that AX's single thread (so every same-AX invariant — product
 *     placement lease, ordered mm_cycle_mutex_ sets, per-AX repricer mutex — is preserved), but
 *     different AXes run concurrently. Because same-AX stacks share one worker, no NEW intra-AX
 *     concurrency is introduced; the only added parallelism is BETWEEN instruments.
 *   - Lambdas are fire-and-forget; the caller keeps captures safe (weak StrategyManager lookup).
 *
 * This class does not understand strategies; it is a generic sharded executor.
 */

#include <atomic>
#include <condition_variable>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>

namespace architect {
namespace strategy {

class MmOrderMover {
public:
    using Action = std::function<void()>;

    static MmOrderMover& getInstance();

    /**
     * Post `action`. With sharding OFF, `ax_symbol` is a diagnostic label and all work shares
     * one FIFO. With sharding ON, `ax_symbol` selects the per-instrument worker (work for the
     * same AX stays serial; different AXes run in parallel). Returns immediately. Thread-safe.
     */
    void enqueueForAx(const std::string& ax_symbol, Action action);

    /**
     * Stop all worker threads and join. After shutdown, future `enqueueForAx` calls are
     * silently dropped. Idempotent. Safe to call from Platform shutdown.
     */
    void shutdown();

    /** For diagnostics: number of live worker threads (1 when sharding OFF and started). */
    std::size_t workerCount() const;

    /** True iff per-instrument sharding has resolved ON. For diagnostics/asserts only. */
    bool shardingResolvedOn() const { return sharding_mode_.load(std::memory_order_acquire) == 1; }

    /**
     * True when the calling thread IS a mover worker thread (any shard). Used by
     * placement-lease acquisition to pick a short, non-blocking wait budget on the mover.
     */
    static bool isCurrentThreadMover();

    /**
     * True when the calling thread is THIS AX symbol's mover worker (sharding ON only).
     * Under sharding OFF the shared worker's key is "" so this returns false for real
     * symbols — callers gate the check on sharding. Used by the place-funnel thread assert.
     */
    static bool isCurrentThreadMoverForAx(const std::string& ax_symbol);

private:
    MmOrderMover() = default;
    ~MmOrderMover();
    MmOrderMover(const MmOrderMover&) = delete;
    MmOrderMover& operator=(const MmOrderMover&) = delete;

    struct QueuedAction {
        std::string ax_label;  // for diagnostics only
        Action fn;
    };

    // One of these per shard (or a single one keyed by "" when sharding is OFF).
    struct Worker {
        std::string ax_key;
        std::mutex mu;
        std::condition_variable cv;
        std::deque<QueuedAction> queue;
        std::thread thread;
        std::atomic<bool> stop{false};
    };

    /** Resolve (once, cached) whether per-instrument sharding is enabled via config. */
    bool shardingEnabled();
    /** Get-or-create the worker for `ax_symbol` (or the shared worker when sharding OFF). */
    Worker* workerFor(const std::string& ax_symbol);
    void runWorker(Worker* w);

    std::mutex workers_mutex_;
    std::unordered_map<std::string, std::unique_ptr<Worker>> workers_;

    // -1 = unresolved, 0 = off, 1 = on. Resolved on first enqueue and then frozen for the
    // process lifetime so a mid-session config reload can't change the threading model under us.
    std::atomic<int> sharding_mode_{-1};
    std::atomic<bool> shutdown_called_{false};
};

}  // namespace strategy
}  // namespace architect
