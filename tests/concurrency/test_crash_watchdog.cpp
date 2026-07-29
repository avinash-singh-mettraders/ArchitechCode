// =============================================================================
// REGRESSION TEST — Tim Izzo's Issue #2 (2026-05-19)
// =============================================================================
//
// "Confusing message in CPP logging. The CPP logging stopped printing. The
//  orders were left in the book unmanaged. Joe used Cancel All from
//  MMLiveFeed and the orders did pull, but ideally orders should always
//  pull automatically, if possible."
//
// Root cause: SIGSEGV (and SIGABRT/SIGFPE/SIGBUS/SIGILL) had no installed
// handler in the C++ binary. When the process crashed, executeShutdown was
// never reached, so the venue-wide cancel-all REST sweep never ran. Joe had
// to manually click Cancel All in MMLiveFeed.
//
// Fix (examples/main.cpp installCrashCancelHandler): pre-armed watchdog
// thread sits on an atomic flag. Fatal-signal handler raises the flag
// (async-signal-safe), the watchdog runs `rest()->cancelAllOrders(nullopt)`
// best-effort, then the handler re-raises the signal so the OS still
// produces a core dump.
//
// This test verifies the watchdog state machine itself — it cannot SIGSEGV
// a unit-test binary (that would kill the test runner), so we model the
// signal handler with a direct flag set. The thread / flag / cleanup
// machinery is identical to production.
//
// Invariants checked:
//   1. Watchdog thread wakes up when the flag is raised.
//   2. Cleanup callback fires before the timeout.
//   3. Multiple flag raises don't re-fire cleanup (CAS guard).
//   4. Watchdog exits cleanly on graceful teardown (no flag).
//   5. The 5s handler wait is bounded.

#include "test_helpers.h"

#include <atomic>
#include <chrono>
#include <thread>

namespace {

// Mirror of the production watchdog (examples/main.cpp).
struct Watchdog {
    std::atomic<bool> cleanup_requested{false};
    std::atomic<bool> cleanup_done{false};
    std::atomic<int>  signum{0};
    std::atomic<int>  cleanup_call_count{0};   // for invariant #3
    std::atomic<bool> stop{false};
    std::thread       thread;

    void start() {
        thread = std::thread([this] { loop(); });
    }

    void loop() {
        while (!stop.load(std::memory_order_acquire)) {
            if (cleanup_requested.load(std::memory_order_acquire)) {
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }
        if (stop.load(std::memory_order_acquire) &&
            !cleanup_requested.load(std::memory_order_acquire)) {
            return;  // graceful teardown path — no cleanup needed
        }
        // === Production analogue: Main().rest()->cancelAllOrders(nullopt) ===
        cleanup_call_count.fetch_add(1, std::memory_order_acq_rel);
        std::this_thread::sleep_for(std::chrono::milliseconds(10));  // simulate REST
        cleanup_done.store(true, std::memory_order_release);
    }

    void stop_and_join() {
        stop.store(true, std::memory_order_release);
        if (thread.joinable()) thread.join();
    }

    // Mirror of fatalSignalHandler — CAS guards re-entry, then waits up to
    // `max_wait_ms` for the watchdog to set cleanup_done.
    void simulate_fatal_signal(int sig, int max_wait_ms) {
        int expected = 0;
        if (!signum.compare_exchange_strong(expected, sig,
                                            std::memory_order_acq_rel,
                                            std::memory_order_relaxed)) {
            return;  // re-entry — production immediately re-raises
        }
        cleanup_requested.store(true, std::memory_order_release);
        const int slices = max_wait_ms / 10;
        for (int i = 0; i < slices; ++i) {
            if (cleanup_done.load(std::memory_order_acquire)) break;
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    }
};

void watchdog_fires_cleanup_on_signal() {
    Watchdog wd;
    wd.start();
    wd.simulate_fatal_signal(/*SIGSEGV*/ 11, /*wait_ms*/ 5000);
    TX_REQUIRE(wd.cleanup_done.load());
    TX_EQ(wd.cleanup_call_count.load(), 1);
    wd.stop_and_join();
}

void watchdog_cleanup_runs_before_5s_bound() {
    Watchdog wd;
    wd.start();
    const auto t0 = std::chrono::steady_clock::now();
    wd.simulate_fatal_signal(/*SIGABRT*/ 6, /*wait_ms*/ 5000);
    const auto t1 = std::chrono::steady_clock::now();
    const auto elapsed_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();
    std::printf("    handler wait elapsed: %lldms (bound 5000)\n", static_cast<long long>(elapsed_ms));
    TX_LE(elapsed_ms, 5000);
    TX_REQUIRE(wd.cleanup_done.load());
    wd.stop_and_join();
}

void repeated_signals_do_not_re_run_cleanup() {
    Watchdog wd;
    wd.start();
    wd.simulate_fatal_signal(11, 5000);
    TX_REQUIRE(wd.cleanup_done.load());
    // Second and third signals (e.g. SIGABRT from inside the cleanup REST)
    // must NOT trigger another cancel-all storm. The CAS guard short-circuits.
    wd.simulate_fatal_signal(6, 200);
    wd.simulate_fatal_signal(8, 200);
    TX_EQ(wd.cleanup_call_count.load(), 1);
    wd.stop_and_join();
}

void graceful_teardown_does_not_fire_cleanup() {
    Watchdog wd;
    wd.start();
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    wd.stop_and_join();  // graceful — no flag was raised
    TX_EQ(wd.cleanup_call_count.load(), 0);
    TX_REQUIRE(!wd.cleanup_done.load());
}

void watchdog_does_not_leak_thread() {
    // Stop must always join. If join() is missed the thread keeps spinning
    // and gets reaped at process exit on the test runner — undetectable in
    // CI. Assert thread::joinable() returns false after stop_and_join.
    Watchdog wd;
    wd.start();
    TX_REQUIRE(wd.thread.joinable());
    wd.stop_and_join();
    TX_REQUIRE(!wd.thread.joinable());
}

}  // namespace

int main() {
    std::printf("=== Tim Issue #2: crash watchdog (orders left unmanaged after segfault) ===\n");
    TX_RUN(watchdog_fires_cleanup_on_signal);
    TX_RUN(watchdog_cleanup_runs_before_5s_bound);
    TX_RUN(repeated_signals_do_not_re_run_cleanup);
    TX_RUN(graceful_teardown_does_not_fire_cleanup);
    TX_RUN(watchdog_does_not_leak_thread);
    return tx::finish("test_crash_watchdog");
}
