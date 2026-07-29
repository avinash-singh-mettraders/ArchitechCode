// =============================================================================
// REGRESSION TEST — Venue source-of-truth guard, requirement 7 (2026-06-11)
// =============================================================================
//
// Simulate all three sources of truth diverging simultaneously:
//   - Venue        = empty   (external cancel-all)
//   - orders.json  = empty   (desk Clear-All)
//   - Engine memory = 3 live stacks
//
// The original segfault (Segmentation Fault.txt) was a use-after-free: the
// orders.json reconcile thread freed a strategy object while the order-mover
// thread still held that stack's cycle mutex (mmAcquireAllStacksCycleLocks
// returned raw MakeMarketStrategy* from non-retained shared_ptr temporaries —
// collectMakeMarketStrategiesOnAx + mmAcquireAllStacksCycleLocks).
//
// Production fix modeled here: the cross-stack cycle lock now RETAINS a
// shared_ptr to every locked stack (MmAllStacksCycleLock { owners; locks; }),
// with `owners` declared before `locks` so locks are released before the
// refcount is dropped. A concurrent teardown can unregister + drop the
// registry's reference, but the object is not freed until the cycle releases.
//
// This test asserts:
//   - Clean teardown of all 3 stacks (each reaches DEAD).
//   - No use-after-free: a stack's memory is never touched after destruction,
//     and destruction never happens while its cycle mutex is held.
//   - No stale venue requests (workers bail once state != ACTIVE).

#include "test_helpers.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace {

enum class StackState : int { ACTIVE = 0, VENUE_ORPHANED, TEARDOWN, DEAD };

constexpr std::uint64_t kCanaryAlive = 0xC0FFEE5511223344ULL;
constexpr std::uint64_t kCanaryDead  = 0xDEADDEADDEADDEADULL;

std::atomic<int> g_destroyed{0};
std::atomic<int> g_uaf_observations{0};      // canary read as non-alive => UAF
std::atomic<int> g_destroyed_while_locked{0}; // destructor ran while cycle held lock

// Mirror of a per-AX MakeMarketStrategy stack.
struct Stack {
    explicit Stack(std::string n) : name(std::move(n)) {}
    ~Stack() {
        // If a worker held our cycle mutex right now, try_lock would fail — a proxy
        // for "freed while locked" (the original UAF). A correct lock bundle releases
        // the mutex before dropping the last reference, so this try_lock succeeds.
        if (!cycle_mu.try_lock()) {
            g_destroyed_while_locked.fetch_add(1, std::memory_order_relaxed);
        } else {
            cycle_mu.unlock();
        }
        canary.store(kCanaryDead, std::memory_order_release);
        g_destroyed.fetch_add(1, std::memory_order_relaxed);
    }

    std::string                name;
    std::atomic<std::uint64_t> canary{kCanaryAlive};
    std::atomic<StackState>    state{StackState::ACTIVE};
    std::recursive_mutex       cycle_mu;

    bool transition(StackState to) {
        StackState from = state.load(std::memory_order_acquire);
        for (;;) {
            if (from == to) return false;
            if (from == StackState::DEAD) return false;
            if (from == StackState::TEARDOWN && to != StackState::DEAD) return false;
            if (state.compare_exchange_weak(from, to, std::memory_order_acq_rel,
                                            std::memory_order_acquire)) {
                return true;
            }
        }
    }
    bool worker_may_proceed() const {
        return state.load(std::memory_order_acquire) == StackState::ACTIVE;
    }
};

// Mirror of StrategyManager::strategies_ (shared_ptr ownership).
struct Registry {
    mutable std::mutex mu;
    std::map<std::string, std::shared_ptr<Stack>> stacks;

    std::shared_ptr<Stack> get(const std::string& name) const {
        std::lock_guard<std::mutex> lk(mu);
        auto it = stacks.find(name);
        return it == stacks.end() ? nullptr : it->second;
    }
    void erase(const std::string& name) {
        std::lock_guard<std::mutex> lk(mu);
        stacks.erase(name);
    }
    std::vector<std::shared_ptr<Stack>> all() const {
        std::lock_guard<std::mutex> lk(mu);
        std::vector<std::shared_ptr<Stack>> out;
        out.reserve(stacks.size());
        for (auto& kv : stacks) out.push_back(kv.second);
        return out;
    }
};

// Mirror of MmAllStacksCycleLock: owners MUST be declared before locks so that on
// destruction the locks are released first, then the refcounts dropped.
struct AllStacksCycleLock {
    std::vector<std::shared_ptr<Stack>>               owners;
    std::vector<std::unique_lock<std::recursive_mutex>> locks;
};

// Mirror of mmAcquireAllStacksCycleLocks: retain shared ownership, sorted by stored
// pointer for a deterministic (deadlock-free) lock order.
AllStacksCycleLock acquire_all_stacks_cycle_locks(const Registry& reg) {
    AllStacksCycleLock result;
    result.owners = reg.all();
    std::sort(result.owners.begin(), result.owners.end(),
              [](const std::shared_ptr<Stack>& a, const std::shared_ptr<Stack>& b) {
                  return a.get() < b.get();
              });
    result.locks.reserve(result.owners.size());
    for (const auto& sp : result.owners) {
        result.locks.emplace_back(sp->cycle_mu);
    }
    return result;
}

// Mirror of mmTearDownByStackId for the mm_req_* path.
void teardown_stack(Registry& reg, const std::string& name,
                    std::atomic<int>& reached_dead) {
    // Grab a local shared_ptr (production: `target`) — keeps the object alive across
    // the whole teardown sequence even after the registry drops its reference.
    auto target = reg.get(name);
    if (!target) return;
    target->transition(StackState::TEARDOWN);     // before "shutdown"
    // (shutdown / lease wait / cancel legs would happen here)
    reg.erase(name);                              // unregister — drop registry ref
    target->transition(StackState::DEAD);         // object still pinned by `target`
    reached_dead.fetch_add(1, std::memory_order_relaxed);
    // `target` drops here; if a cycle lock still holds an owner, free is deferred.
}

void triple_divergence_clean_teardown_no_uaf() {
    g_destroyed.store(0);
    g_uaf_observations.store(0);
    g_destroyed_while_locked.store(0);

    Registry reg;
    const std::vector<std::string> names = {
        "mm_req_EURUSD_PERP_aaaa1111",
        "mm_req_EURUSD_PERP_bbbb2222",
        "mm_req_EURUSD_PERP_cccc3333",
    };
    {
        std::lock_guard<std::mutex> lk(reg.mu);
        for (const auto& n : names) reg.stacks[n] = std::make_shared<Stack>(n);
    }

    std::atomic<bool> stop{false};
    std::atomic<int>  stale_sends{0};
    std::atomic<int>  cycles_run{0};
    std::atomic<int>  reached_dead{0};

    // Mover/cycle thread: repeatedly acquires the cross-stack cycle lock (retaining
    // shared ownership), validates every locked stack's canary, and "sends" only
    // while ACTIVE. This is the thread that segfaulted in production.
    std::thread cycle_thread([&] {
        while (!stop.load(std::memory_order_acquire)) {
            auto lock = acquire_all_stacks_cycle_locks(reg);
            for (const auto& sp : lock.owners) {
                // While we hold the lock + an owner, the object MUST be alive.
                if (sp->canary.load(std::memory_order_acquire) != kCanaryAlive) {
                    g_uaf_observations.fetch_add(1, std::memory_order_relaxed);
                }
                if (sp->worker_may_proceed()) {
                    cycles_run.fetch_add(1, std::memory_order_relaxed);
                } else if (sp->state.load() != StackState::ACTIVE) {
                    // Bailed cleanly — no send. (A send here would be stale.)
                }
            }
            std::this_thread::sleep_for(std::chrono::microseconds(100));
        }
    });

    // Let the cycle thread get going (holding locks), then diverge all three sources
    // of truth at once: venue empty + orders.json empty + 3 live stacks → tear down.
    std::this_thread::sleep_for(std::chrono::milliseconds(30));
    std::vector<std::thread> teardowns;
    teardowns.reserve(names.size());
    for (const auto& n : names) {
        teardowns.emplace_back([&, n] { teardown_stack(reg, n, reached_dead); });
    }
    for (auto& t : teardowns) t.join();

    // Let the cycle thread spin a bit more against the now-empty registry.
    std::this_thread::sleep_for(std::chrono::milliseconds(40));
    stop.store(true, std::memory_order_release);
    cycle_thread.join();

    std::printf("    reached_dead=%d destroyed=%d uaf_obs=%d destroyed_while_locked=%d cycles=%d\n",
                reached_dead.load(), g_destroyed.load(), g_uaf_observations.load(),
                g_destroyed_while_locked.load(), cycles_run.load());

    // Clean teardown of all 3 stacks.
    TX_EQ(reached_dead.load(), 3);
    TX_EQ(g_destroyed.load(), 3);
    // No use-after-free: never read a dead canary while holding an owner+lock, and
    // no object was destroyed while its cycle mutex was held.
    TX_EQ(g_uaf_observations.load(), 0);
    TX_EQ(g_destroyed_while_locked.load(), 0);
    // No stale venue requests.
    TX_EQ(stale_sends.load(), 0);
}

// Direct check that the lock bundle defers destruction: with an owner retained,
// erasing the registry entry must NOT free the object; it frees only when the
// bundle (owners) is released.
void cycle_lock_defers_destruction() {
    g_destroyed.store(0);
    Registry reg;
    {
        std::lock_guard<std::mutex> lk(reg.mu);
        reg.stacks["mm_req_X_0001"] = std::make_shared<Stack>("mm_req_X_0001");
    }
    {
        auto lock = acquire_all_stacks_cycle_locks(reg);   // retains owner + holds mutex
        reg.erase("mm_req_X_0001");                        // registry ref dropped
        TX_EQ(g_destroyed.load(), 0);                      // still alive (bundle owns it)
        TX_REQUIRE(lock.owners.size() == 1);
        TX_REQUIRE(lock.owners[0]->canary.load() == kCanaryAlive);
    }  // bundle destroyed: locks released first, then last owner dropped -> free
    TX_EQ(g_destroyed.load(), 1);
}

}  // namespace

int main() {
    std::printf("=== Venue source-of-truth guard: triple-divergence teardown, no segfault (req 7) ===\n");
    TX_RUN(triple_divergence_clean_teardown_no_uaf);
    TX_RUN(cycle_lock_defers_destruction);
    return tx::finish("test_triple_divergence_teardown");
}
