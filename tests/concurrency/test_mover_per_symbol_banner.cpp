// =============================================================================
// REAL MmOrderMover — per-symbol isolation + boot-mode banner (2026-07-12)
// =============================================================================
//
// Unlike the isolated mover *model* in test_mover_shard_accept_race.cpp, this
// test links the REAL production singleton (src/strategy/MmOrderMover.cpp) plus
// the real Config + Logger. It proves, on the actual code path:
//
//   1. With market_maker.mover_shard_per_instrument=true, jobs enqueued for two
//      different AX symbols run on DIFFERENT std::thread::ids (one worker per AX).
//   2. A 500ms job on symbol A does NOT delay symbol B's job by more than 50ms
//      (no cross-symbol head-of-line blocking — the July-9 production bug).
//   3. The one-shot boot-mode banner is emitted:
//        [MM_ORDER_MOVER] mode=PER_INSTRUMENT (workers created lazily per AX) ...
//      with source=config (because the key is present in config here), and the
//      DEGRADED SINGLE_GLOBAL line is NOT emitted.
//
// The banner is written from shardingEnabled(), which resolves ONCE on the first
// enqueue, so the Logger must be initialized BEFORE the first enqueue.

#include "test_helpers.h"

#include "config/Config.h"
#include "strategy/MmOrderMover.h"
#include "utils/Logger.h"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <sstream>
#include <string>
#include <thread>

using architect::config::Config;
using architect::strategy::MmOrderMover;
using architect::utils::Logger;

namespace {

constexpr const char* kBaseLogDir = "mover_banner_logs";
constexpr const char* kDate = "20260712";

std::string readPlatformLog() {
    const std::string path =
        std::string(kBaseLogDir) + "/" + kDate + "/" + kDate + ".platform.log";
    std::ifstream f(path);
    std::stringstream ss;
    ss << f.rdbuf();
    return ss.str();
}

void per_symbol_isolation_and_boot_banner() {
    using clock = std::chrono::steady_clock;

    // (1) Sharding ON, sourced from config so the banner reports source=config.
    Config::getInstance().set<bool>("market_maker.mover_shard_per_instrument", true);

    // (2) Logger live BEFORE the first enqueue so the one-shot boot banner lands
    //     in the file sink we read back below.
    Logger::Config lc;
    lc.base_log_dir = kBaseLogDir;
    lc.simulation_date = kDate;
    lc.console_enabled = false;
    lc.file_enabled = true;
    lc.csv_enabled = false;
    lc.crash_on_init_failure = true;
    Logger::initialize(lc);

    const std::thread::id main_tid = std::this_thread::get_id();

    std::atomic<bool> a_done{false};
    std::atomic<bool> b_done{false};
    std::thread::id a_tid{};
    std::thread::id b_tid{};
    std::atomic<long long> b_dispatch_delay_ms{-1};

    auto& mover = MmOrderMover::getInstance();

    // Symbol A: deliberately SLOW (500ms). On a single shared worker this would
    // block B behind it.
    mover.enqueueForAx("AX-A", [&] {
        a_tid = std::this_thread::get_id();
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        a_done.store(true, std::memory_order_release);
    });

    // Symbol B: enqueued right after A's slow job; measure enqueue -> dispatch.
    const auto t_enqueue_b = clock::now();
    mover.enqueueForAx("AX-B", [&, t_enqueue_b] {
        const auto now = clock::now();
        b_dispatch_delay_ms.store(
            std::chrono::duration_cast<std::chrono::milliseconds>(now - t_enqueue_b).count(),
            std::memory_order_relaxed);
        b_tid = std::this_thread::get_id();
        b_done.store(true, std::memory_order_release);
    });

    // Bounded wait for BOTH. B should finish well before A's 500ms elapses.
    const auto deadline = clock::now() + std::chrono::seconds(3);
    while ((!a_done.load(std::memory_order_acquire) ||
            !b_done.load(std::memory_order_acquire)) &&
           clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    TX_REQUIRE(a_done.load(std::memory_order_acquire));
    TX_REQUIRE(b_done.load(std::memory_order_acquire));

    // (4a) Different worker threads per symbol; neither is the enqueuing thread.
    TX_REQUIRE(a_tid != b_tid);
    TX_REQUIRE(a_tid != main_tid);
    TX_REQUIRE(b_tid != main_tid);

    // (4b) B not stuck behind A's 500ms job.
    const long long delay = b_dispatch_delay_ms.load(std::memory_order_relaxed);
    TX_REQUIRE(delay >= 0);
    TX_LE(delay, 50LL);

    // (4c) Boot banner: PER_INSTRUMENT with source=config, no DEGRADED line.
    Logger::getInstance().flush();
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    const std::string log = readPlatformLog();
    TX_REQUIRE(log.find("[MM_ORDER_MOVER] mode=PER_INSTRUMENT") != std::string::npos);
    TX_REQUIRE(log.find("source=config") != std::string::npos);
    TX_REQUIRE(log.find("mode=SINGLE_GLOBAL") == std::string::npos);

    mover.shutdown();
}

}  // namespace

int main() {
    std::printf("=== real mover: per-symbol isolation + boot banner ===\n");
    TX_RUN(per_symbol_isolation_and_boot_banner);
    const int rc = tx::finish("test_mover_per_symbol_banner");
    // The real Logger/spdlog + Config singletons have a known static-destruction-
    // order fiasco at process exit (a global mutex is torn down in one TU before
    // another TU's atexit handler locks it -> libc++ "mutex lock failed"). It is
    // unrelated to the mover contract under test and only fires during global
    // teardown, AFTER every mover worker has already been joined. Report the real
    // test result deterministically by skipping global destructors.
    std::fflush(stdout);
    std::_Exit(rc);
}
