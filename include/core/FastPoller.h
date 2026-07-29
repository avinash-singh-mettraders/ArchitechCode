#pragma once

/**
 * @file FastPoller.h
 * @brief Pillar C — dedicated continuous fills+positions poller thread.
 *
 * A single background thread performs the HTTP GET /fills and GET /positions
 * (the ~RTT) continuously and hands the RAW response bodies back to the main
 * trading loop via a push callback. The main loop parses+applies them, so the
 * PositionBook single-writer invariant is preserved (fetch here, apply there).
 *
 * The poller is rate-governed (429/Retry-After exponential backoff, RTT-
 * degradation adaptive halving) and self-healing (respawn on death up to N
 * times, then signal fall-back to legacy inline polling). Its REST budget is
 * sacrificial — order traffic always wins because it shares the same
 * CurlMultiManager easy-handle pool (a µs pop/push, never held across an RTT).
 *
 * The class is deliberately decoupled from RestClient: the caller supplies
 * fetch callbacks that return a small FetchResult POD, so this header pulls in
 * no API types and is trivially unit-testable with a mock venue.
 */

#include <atomic>
#include <cstdint>
#include <functional>
#include <string>
#include <thread>

namespace architect {
namespace core {

class FastPoller {
public:
    /** Result of one fetch callback (decoupled from api::HttpResponse). */
    struct FetchResult {
        int status_code{0};
        bool ok{false};              // 2xx AND transport success
        std::string body;
        std::int64_t rtt_ms{0};      // measured by the callback (or the poller)
        std::int64_t retry_after_ms{0};  // parsed Retry-After header; 0 if absent
        bool rate_limited{false};    // status==429 OR caller-detected rate limit
    };

    struct Config {
        bool enabled{true};
        // Default cadence: fills 300ms, positions 1000ms (2026-07-13). 0 is still
        // allowed (back-to-back == venue RTT) but is no longer the default because
        // constant back-to-back polling soft-throttles the whole account. Floors,
        // clamped to >= 0.
        int fills_interval_ms{300};
        int positions_interval_ms{1000};
        int poll_min_interval_ms{0};      // extra floor between full cycles
        // Governor
        int backoff_base_ms{500};
        int backoff_cap_ms{10000};
        double rtt_degrade_factor{2.0};   // legacy median-relative check (unused by self-throttle)
        int max_freq_divisor{16};
        // Self-throttle (RTT-degradation) governor. The reference RTT is anchored to
        // min(expected_rtt_ms, session-min observed) — the session minimum is robust
        // to self-inflicted congestion, whereas a rolling median baselines at the
        // already-throttled value and goes blind. If the rolling median exceeds
        // self_throttle_hi_factor * ref for self_throttle_min_samples consecutive
        // samples, halve frequency; keep halving until the median recovers below
        // self_throttle_lo_factor * ref.
        int expected_rtt_ms{100};
        double self_throttle_hi_factor{1.8};
        double self_throttle_lo_factor{1.3};
        int self_throttle_min_samples{10};
        // Lifecycle
        int heartbeat_ms{20000};
        int max_respawns{3};
        int respawn_spacing_ms{1000};
    };

    // fetch_fills(out_start_ts_ns) -> result; the callback also reports the cursor
    // (start_timestamp_ns) it used so the applier can advance it correctly.
    using FetchFills = std::function<FetchResult(std::int64_t& out_start_ts_ns)>;
    using FetchPositions = std::function<FetchResult()>;
    // push(is_fills, body, start_ts_ns, fetch_steady_ms)
    using PushFn = std::function<void(bool, std::string, std::int64_t, std::int64_t)>;
    using LogFn = std::function<void(const std::string&)>;

    FastPoller(Config cfg,
               FetchFills fetch_fills,
               FetchPositions fetch_positions,
               PushFn push,
               LogFn log_info,
               LogFn log_warn);
    ~FastPoller();

    FastPoller(const FastPoller&) = delete;
    FastPoller& operator=(const FastPoller&) = delete;

    void start();
    void stop();  // idempotent; joins the thread

    [[nodiscard]] bool fellBack() const { return fell_back_.load(std::memory_order_acquire); }
    [[nodiscard]] bool running() const { return running_.load(std::memory_order_acquire); }

    // --- stats (for heartbeat + tests) ---
    [[nodiscard]] std::uint64_t fillsPolls() const { return fills_polls_.load(std::memory_order_relaxed); }
    [[nodiscard]] std::uint64_t positionsPolls() const { return positions_polls_.load(std::memory_order_relaxed); }
    [[nodiscard]] std::uint64_t backoffs() const { return backoffs_.load(std::memory_order_relaxed); }
    [[nodiscard]] std::uint64_t respawns() const { return respawns_.load(std::memory_order_relaxed); }
    [[nodiscard]] std::int64_t medianRttMs() const { return median_rtt_ms_.load(std::memory_order_relaxed); }
    [[nodiscard]] std::int64_t minRttMs() const { return min_rtt_ms_.load(std::memory_order_relaxed); }
    [[nodiscard]] std::int64_t refRttMs() const { return ref_rtt_ms_.load(std::memory_order_relaxed); }
    [[nodiscard]] bool throttled() const { return throttled_.load(std::memory_order_relaxed); }
    [[nodiscard]] bool selfThrottled() const { return self_throttled_.load(std::memory_order_relaxed); }

    static std::int64_t nowSteadyMs();

private:
    void supervisor();  // thread entry: runs workLoop with respawn/fallback
    void workLoop();    // the actual fetch loop; may throw (caught by supervisor)
    void sleepInterruptibleMs(std::int64_t ms);

    Config cfg_;
    FetchFills fetch_fills_;
    FetchPositions fetch_positions_;
    PushFn push_;
    LogFn log_info_;
    LogFn log_warn_;

    std::thread thread_;
    std::atomic<bool> running_{false};
    std::atomic<bool> fell_back_{false};

    std::atomic<std::uint64_t> fills_polls_{0};
    std::atomic<std::uint64_t> positions_polls_{0};
    std::atomic<std::uint64_t> backoffs_{0};
    std::atomic<std::uint64_t> respawns_{0};
    std::atomic<std::int64_t> median_rtt_ms_{0};
    std::atomic<std::int64_t> min_rtt_ms_{0};
    std::atomic<std::int64_t> ref_rtt_ms_{0};
    std::atomic<bool> throttled_{false};
    std::atomic<bool> self_throttled_{false};
};

}  // namespace core
}  // namespace architect
