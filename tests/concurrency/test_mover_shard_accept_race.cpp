// =============================================================================
// REGRESSION TEST — accept-ordering race + per-instrument mover sharding
// (2026-07-09, Path B: parallel movers + shared-state hardening)
// =============================================================================
//
// Two production contracts are modelled here in isolation (no strategy_lib link,
// matching the other concurrency tests). Both are also intended to be run under
// ThreadSanitizer (see -DENABLE_TSAN=ON) to prove they are data-race-free.
//
// PART A — reserve-before-submit (MakeMarketStrategy::mmPlaceReservedLeg)
// ----------------------------------------------------------------------
//   ORDER_ACCEPTED is dispatched SYNCHRONOUSLY, inline, on the submitting thread
//   from inside submit_order() (Strategy.cpp: "arrives before submit_order
//   returns"). The OLD code bumped mm_pending_accepts_ and registered the
//   PLACE_PENDING leg AFTER the submit call, so the inline accept saw pending==0
//   (skipped its decrement) and matched no leg → the counter/leg got stranded and
//   isPairCycleInFlight() stayed true forever (the JPY ce60e217 CYCLE_LOCK).
//
//   The fix RESERVES (register leg + ++pending) BEFORE submit. This test models
//   both an inline accept and an async accept and asserts the fixed ordering
//   always settles to {pending==0, leg==ADOPTED}, while the old ordering strands.
//
// PART B — per-AX mover sharding (MmOrderMover v3)
// -----------------------------------------------
//   With sharding ON each instrument gets its own worker thread. The invariant is:
//   work for one AX is STRICTLY serial (never two actions for the same AX at once),
//   while different AXes run concurrently, and NO work is lost or double-run.

#include "test_helpers.h"

#include <atomic>
#include <condition_variable>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

namespace {

// ---------------------------------------------------------------------------
// PART A: reserve-before-submit model
// ---------------------------------------------------------------------------

enum class LegState { IDLE, PLACE_PENDING, ADOPTED, DEAD };

struct MiniStrategy {
    std::atomic<int> pending{0};
    // Guarded leg state (production guards with tracked_orders_mutex_).
    std::mutex leg_mu;
    LegState leg{LegState::IDLE};
    std::string leg_cid;

    // Models on_accept: matches the reserved leg (if any) and balances the counter.
    // Runs either inline (same thread, during submit) or async (OM thread).
    void on_accept(const std::string& cid) {
        {
            std::lock_guard<std::mutex> lk(leg_mu);
            if (leg == LegState::PLACE_PENDING && leg_cid == cid) {
                leg = LegState::ADOPTED;      // matched_pending path
            }
            // else: production adopts as manual (still ADOPTED); modelled below when
            // reservation is absent by leaving IDLE→ we set ADOPTED to mirror adopt.
            else {
                leg = LegState::ADOPTED;
                leg_cid = cid;
            }
        }
        int cur = pending.load(std::memory_order_acquire);
        while (cur > 0 &&
               !pending.compare_exchange_weak(cur, cur - 1,
                                              std::memory_order_acq_rel,
                                              std::memory_order_acquire)) {
        }
    }
};

// FIXED path: reserve (register leg + ++pending) BEFORE submit.
void reserve_before_submit_settles(bool inline_accept) {
    for (int iter = 0; iter < 5000; ++iter) {
        MiniStrategy s;
        const std::string cid = "cid_" + std::to_string(iter);

        // Reserve BEFORE submit.
        {
            std::lock_guard<std::mutex> lk(s.leg_mu);
            s.leg = LegState::PLACE_PENDING;
            s.leg_cid = cid;
        }
        s.pending.fetch_add(1, std::memory_order_acq_rel);

        // "submit": the accept fires either inline (this thread) or async (other thread).
        std::thread async;
        if (inline_accept) {
            s.on_accept(cid);
        } else {
            async = std::thread([&] { s.on_accept(cid); });
        }
        if (async.joinable()) async.join();

        // Must have settled: counter back to 0, leg ADOPTED.
        TX_EQ(s.pending.load(), 0);
        std::lock_guard<std::mutex> lk(s.leg_mu);
        TX_REQUIRE(s.leg == LegState::ADOPTED);
    }
}

void reserve_before_submit_inline() { reserve_before_submit_settles(true); }
void reserve_before_submit_async()  { reserve_before_submit_settles(false); }

// Demonstrates WHY the old ordering stranded: bump AFTER an inline accept leaves
// pending stuck at 1 (this asserts the failure mode, so the fix is meaningful).
void old_ordering_strands_on_inline_accept() {
    MiniStrategy s;
    const std::string cid = "old";
    // Inline accept fires DURING submit, before the post-submit bump.
    s.on_accept(cid);                 // pending==0 → decrement skipped; leg ADOPTED
    s.pending.fetch_add(1);           // post-submit bump → stuck at 1
    TX_EQ(s.pending.load(), 1);       // stranded (this is the bug the fix removes)
}

// ---------------------------------------------------------------------------
// PART B: per-AX sharded mover model
// ---------------------------------------------------------------------------

class MiniShardedMover {
public:
    using Action = std::function<void()>;

    void enqueue(const std::string& ax, Action fn) {
        Worker* w = workerFor(ax);
        {
            std::lock_guard<std::mutex> lk(w->mu);
            w->q.emplace_back(std::move(fn));
        }
        w->cv.notify_one();
    }

    void shutdown() {
        std::vector<Worker*> ws;
        {
            std::lock_guard<std::mutex> lk(workers_mu_);
            for (auto& kv : workers_) ws.push_back(kv.second.get());
        }
        for (Worker* w : ws) {
            {
                std::lock_guard<std::mutex> lk(w->mu);
                w->stop = true;
            }
            w->cv.notify_all();
        }
        for (Worker* w : ws) {
            if (w->t.joinable()) w->t.join();
        }
    }

    std::size_t workerCount() {
        std::lock_guard<std::mutex> lk(workers_mu_);
        return workers_.size();
    }

private:
    struct Worker {
        std::mutex mu;
        std::condition_variable cv;
        std::deque<Action> q;
        std::thread t;
        bool stop{false};
    };

    Worker* workerFor(const std::string& ax) {
        std::lock_guard<std::mutex> lk(workers_mu_);
        auto it = workers_.find(ax);
        if (it != workers_.end()) return it->second.get();
        auto w = std::make_unique<Worker>();
        Worker* raw = w.get();
        workers_.emplace(ax, std::move(w));
        raw->t = std::thread([this, raw] { run(raw); });
        return raw;
    }

    void run(Worker* w) {
        for (;;) {
            Action fn;
            {
                std::unique_lock<std::mutex> lk(w->mu);
                w->cv.wait(lk, [w] { return w->stop || !w->q.empty(); });
                if (w->stop && w->q.empty()) return;
                fn = std::move(w->q.front());
                w->q.pop_front();
            }
            fn();
        }
    }

    std::mutex workers_mu_;
    std::unordered_map<std::string, std::unique_ptr<Worker>> workers_;
};

// Same-AX actions never overlap; different AXes may run in parallel; nothing lost.
void sharding_serialises_same_ax_no_loss() {
    constexpr int kAx = 6;
    constexpr int kPerAx = 3000;

    // Per-AX "currently executing" flag: if two actions for the same AX ever run
    // concurrently, exchange(true) will observe an already-true flag → race caught.
    std::vector<std::atomic<bool>> in_action(kAx);
    std::vector<std::atomic<int>>  processed(kAx);
    for (int i = 0; i < kAx; ++i) { in_action[i] = false; processed[i] = 0; }
    std::atomic<int> same_ax_overlap{0};

    // Per-AX shared state mutated ONLY by that AX's worker → must stay consistent
    // WITHOUT any extra lock precisely because sharding guarantees single-threading.
    std::vector<long long> per_ax_state(kAx, 0);

    {
        MiniShardedMover mover;
        for (int a = 0; a < kAx; ++a) {
            const std::string ax = "AX" + std::to_string(a);
            for (int i = 0; i < kPerAx; ++i) {
                mover.enqueue(ax, [a, &in_action, &processed, &same_ax_overlap, &per_ax_state] {
                    if (in_action[a].exchange(true)) {
                        same_ax_overlap.fetch_add(1, std::memory_order_relaxed);
                    }
                    // Unlocked RMW on per-AX state — safe iff single-threaded per AX.
                    per_ax_state[a] += 1;
                    processed[a].fetch_add(1, std::memory_order_relaxed);
                    in_action[a].store(false);
                });
            }
        }
        mover.shutdown();
    }

    TX_EQ(same_ax_overlap.load(), 0);
    for (int a = 0; a < kAx; ++a) {
        TX_EQ(processed[a].load(), kPerAx);
        TX_EQ(per_ax_state[a], static_cast<long long>(kPerAx));
    }
}

// A concurrent "fill stream" touches per-AX state guarded by a mutex while the
// mover runs — models processFill (OM thread) vs mover cross-thread access. With
// the guard, the running total is always exact (no torn/lost updates).
void fill_stream_concurrent_with_mover_is_consistent() {
    constexpr int kAx = 4;
    constexpr int kPerAx = 2000;
    constexpr int kFills = 4000;

    struct AxState { std::mutex mu; long long net = 0; };
    std::vector<std::unique_ptr<AxState>> st;
    for (int i = 0; i < kAx; ++i) st.emplace_back(std::make_unique<AxState>());

    std::atomic<bool> stop_fills{false};
    std::thread filler([&] {
        for (int i = 0; i < kFills && !stop_fills.load(); ++i) {
            AxState& a = *st[i % kAx];
            std::lock_guard<std::mutex> lk(a.mu);
            a.net += 1;
        }
    });

    {
        MiniShardedMover mover;
        for (int a = 0; a < kAx; ++a) {
            for (int i = 0; i < kPerAx; ++i) {
                mover.enqueue("AX" + std::to_string(a), [a, &st] {
                    AxState& s = *st[a];
                    std::lock_guard<std::mutex> lk(s.mu);
                    s.net += 1;
                });
            }
        }
        mover.shutdown();
    }
    stop_fills.store(true);
    filler.join();

    long long total = 0;
    for (auto& a : st) total += a->net;
    // mover: kAx*kPerAx, filler: exactly kFills.
    TX_EQ(total, static_cast<long long>(kAx) * kPerAx + kFills);
}

}  // namespace

int main() {
    std::printf("=== mover sharding + accept-ordering race ===\n");
    TX_RUN(reserve_before_submit_inline);
    TX_RUN(reserve_before_submit_async);
    TX_RUN(old_ordering_strands_on_inline_accept);
    TX_RUN(sharding_serialises_same_ax_no_loss);
    TX_RUN(fill_stream_concurrent_with_mover_is_consistent);
    return tx::finish("test_mover_shard_accept_race");
}
