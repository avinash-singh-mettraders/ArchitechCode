#include "core/FastPoller.h"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <deque>
#include <string>

namespace architect {
namespace core {

std::int64_t FastPoller::nowSteadyMs() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

FastPoller::FastPoller(Config cfg,
                       FetchFills fetch_fills,
                       FetchPositions fetch_positions,
                       PushFn push,
                       LogFn log_info,
                       LogFn log_warn)
    : cfg_(cfg),
      fetch_fills_(std::move(fetch_fills)),
      fetch_positions_(std::move(fetch_positions)),
      push_(std::move(push)),
      log_info_(std::move(log_info)),
      log_warn_(std::move(log_warn)) {
    cfg_.fills_interval_ms = std::max(0, cfg_.fills_interval_ms);
    cfg_.positions_interval_ms = std::max(0, cfg_.positions_interval_ms);
    cfg_.poll_min_interval_ms = std::max(0, cfg_.poll_min_interval_ms);
    cfg_.backoff_base_ms = std::max(1, cfg_.backoff_base_ms);
    cfg_.backoff_cap_ms = std::max(cfg_.backoff_base_ms, cfg_.backoff_cap_ms);
    cfg_.max_freq_divisor = std::max(1, cfg_.max_freq_divisor);
    if (cfg_.rtt_degrade_factor < 1.0) cfg_.rtt_degrade_factor = 2.0;
    cfg_.expected_rtt_ms = std::max(1, cfg_.expected_rtt_ms);
    if (cfg_.self_throttle_hi_factor < 1.0) cfg_.self_throttle_hi_factor = 1.8;
    if (cfg_.self_throttle_lo_factor < 1.0) cfg_.self_throttle_lo_factor = 1.3;
    // lo must be < hi so recovery has hysteresis.
    if (cfg_.self_throttle_lo_factor >= cfg_.self_throttle_hi_factor) {
        cfg_.self_throttle_lo_factor = cfg_.self_throttle_hi_factor * 0.75;
    }
    cfg_.self_throttle_min_samples = std::max(1, cfg_.self_throttle_min_samples);
    // Seed the reference to expected so it is meaningful before the first sample.
    min_rtt_ms_.store(cfg_.expected_rtt_ms, std::memory_order_relaxed);
    ref_rtt_ms_.store(cfg_.expected_rtt_ms, std::memory_order_relaxed);
}

FastPoller::~FastPoller() { stop(); }

void FastPoller::start() {
    if (!cfg_.enabled) {
        return;
    }
    if (running_.exchange(true)) {
        return;  // already running
    }
    fell_back_.store(false, std::memory_order_release);
    thread_ = std::thread(&FastPoller::supervisor, this);
}

void FastPoller::stop() {
    running_.store(false, std::memory_order_release);
    if (thread_.joinable()) {
        thread_.join();
    }
}

void FastPoller::sleepInterruptibleMs(std::int64_t ms) {
    // Sleep in small slices so stop() is observed promptly.
    const std::int64_t deadline = nowSteadyMs() + std::max<std::int64_t>(0, ms);
    while (running_.load(std::memory_order_acquire) && nowSteadyMs() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(
            std::min<std::int64_t>(10, deadline - nowSteadyMs())));
    }
}

void FastPoller::supervisor() {
    int respawns = 0;
    while (running_.load(std::memory_order_acquire)) {
        try {
            workLoop();
            break;  // clean stop (running_ went false inside workLoop)
        } catch (const std::exception& e) {
            if (log_warn_) {
                log_warn_(std::string("[FAST_POLLER] FATAL poller thread threw: ") + e.what());
            }
        } catch (...) {
            if (log_warn_) {
                log_warn_("[FAST_POLLER] FATAL poller thread threw: unknown exception");
            }
        }
        if (!running_.load(std::memory_order_acquire)) {
            break;
        }
        if (++respawns > cfg_.max_respawns) {
            fell_back_.store(true, std::memory_order_release);
            if (log_warn_) {
                log_warn_("[FAST_POLLER] respawn limit exceeded — falling back to legacy inline polling");
            }
            break;
        }
        respawns_.fetch_add(1, std::memory_order_relaxed);
        if (log_warn_) {
            log_warn_("[FAST_POLLER] respawning poller thread (attempt " + std::to_string(respawns) +
                      "/" + std::to_string(cfg_.max_respawns) + ")");
        }
        sleepInterruptibleMs(cfg_.respawn_spacing_ms);
    }
    running_.store(false, std::memory_order_release);
}

void FastPoller::workLoop() {
    std::int64_t last_fills_ms = 0;
    std::int64_t last_pos_ms = 0;
    std::int64_t last_hb_ms = nowSteadyMs();
    std::int64_t backoff_until_ms = 0;
    int consec_backoffs = 0;
    double freq_div = 1.0;  // >1 halves frequency (adaptive RTT governor)

    std::deque<std::int64_t> rtt_win;
    std::int64_t session_min_rtt = cfg_.expected_rtt_ms;  // robust degradation anchor
    int degraded_samples = 0;                             // consecutive median > hi*ref
    auto record_rtt = [&](std::int64_t rtt) {
        if (rtt < 0) rtt = 0;
        rtt_win.push_back(rtt);
        if (rtt_win.size() > 64) rtt_win.pop_front();
        std::deque<std::int64_t> sorted(rtt_win.begin(), rtt_win.end());
        std::sort(sorted.begin(), sorted.end());
        const std::int64_t med = sorted.empty() ? 0 : sorted[sorted.size() / 2];
        median_rtt_ms_.store(med, std::memory_order_relaxed);
        // Session-minimum baseline (ignore 0s from instant/mock returns so the ref
        // does not collapse to 0). Reference = min(expected, session-min observed).
        if (rtt > 0 && rtt < session_min_rtt) {
            session_min_rtt = rtt;
            min_rtt_ms_.store(session_min_rtt, std::memory_order_relaxed);
        }
        ref_rtt_ms_.store(session_min_rtt, std::memory_order_relaxed);
        return med;
    };

    auto governed_fetch = [&](bool is_fills) {
        std::int64_t start_ts_ns = 0;
        FetchResult r;
        const std::int64_t t0 = nowSteadyMs();
        if (is_fills) {
            r = fetch_fills_ ? fetch_fills_(start_ts_ns) : FetchResult{};
            fills_polls_.fetch_add(1, std::memory_order_relaxed);
        } else {
            r = fetch_positions_ ? fetch_positions_() : FetchResult{};
            positions_polls_.fetch_add(1, std::memory_order_relaxed);
        }
        const std::int64_t rtt = (r.rtt_ms > 0) ? r.rtt_ms : (nowSteadyMs() - t0);
        const std::int64_t med = record_rtt(rtt);

        // --- Rate governor: 429 / Retry-After exponential backoff (sacrificial) ---
        if (r.status_code == 429 || r.rate_limited) {
            const int shift = std::min(consec_backoffs, 20);
            std::int64_t base = cfg_.backoff_base_ms * (std::int64_t{1} << shift);
            base = std::min<std::int64_t>(base, cfg_.backoff_cap_ms);
            const std::int64_t wait = (r.retry_after_ms > 0)
                                          ? std::min<std::int64_t>(r.retry_after_ms, cfg_.backoff_cap_ms)
                                          : base;
            backoff_until_ms = nowSteadyMs() + wait;
            ++consec_backoffs;
            backoffs_.fetch_add(1, std::memory_order_relaxed);
            throttled_.store(true, std::memory_order_relaxed);
            if (log_warn_) {
                log_warn_("[POLLER_BACKOFF] endpoint=" + std::string(is_fills ? "fills" : "positions") +
                          " status=" + std::to_string(r.status_code) + " wait_ms=" + std::to_string(wait) +
                          " retry_after_ms=" + std::to_string(r.retry_after_ms) +
                          " consec=" + std::to_string(consec_backoffs) + " (sacrificial; order traffic wins)");
            }
            return;
        }

        // Success (or non-429 error we simply skip): recover throttle state.
        consec_backoffs = 0;
        throttled_.store(false, std::memory_order_relaxed);
        if (r.ok && !r.body.empty() && push_) {
            push_(is_fills, std::move(r.body), start_ts_ns, nowSteadyMs());
        }

        // --- Self-throttle governor (RTT-degradation vs a robust reference) ---
        // Baseline against min(expected_rtt_ms, session-min) — NOT the rolling median,
        // which would baseline at the already-throttled value and never fire. If the
        // median stays above hi*ref for >= min_samples consecutive samples, halve the
        // frequency; recover (restore frequency) once the median drops below lo*ref.
        const double ref = static_cast<double>(session_min_rtt);
        if (med > 0 && ref > 0.0) {
            const double hi = cfg_.self_throttle_hi_factor * ref;
            const double lo = cfg_.self_throttle_lo_factor * ref;
            if (static_cast<double>(med) > hi) {
                if (degraded_samples < 1000000) ++degraded_samples;
                if (degraded_samples >= cfg_.self_throttle_min_samples &&
                    freq_div < static_cast<double>(cfg_.max_freq_divisor)) {
                    freq_div = std::min(freq_div * 2.0, static_cast<double>(cfg_.max_freq_divisor));
                    self_throttled_.store(true, std::memory_order_relaxed);
                    if (log_warn_) {
                        log_warn_("[POLLER_SELF_THROTTLE] median_rtt_ms=" + std::to_string(med) +
                                  " ref_rtt_ms=" + std::to_string(static_cast<std::int64_t>(ref)) +
                                  " min_rtt_ms=" + std::to_string(session_min_rtt) +
                                  " hi_factor=" + std::to_string(cfg_.self_throttle_hi_factor) +
                                  " -> freq_div=" + std::to_string(freq_div) +
                                  " (halving; order traffic wins)");
                    }
                    // Require another full window of degradation before the next halve.
                    degraded_samples = 0;
                }
            } else if (static_cast<double>(med) < lo) {
                degraded_samples = 0;
                if (freq_div > 1.0) {
                    freq_div = std::max(1.0, freq_div / 2.0);
                    if (freq_div <= 1.0) {
                        self_throttled_.store(false, std::memory_order_relaxed);
                    }
                }
            } else {
                // Between lo and hi: hold. Decay the degradation streak slowly so a
                // one-off spike does not accumulate toward a halve.
                if (degraded_samples > 0) --degraded_samples;
            }
        }
    };

    // When self-throttled at a 0 (back-to-back) interval, freq_div*0 is still 0, so
    // the halving would have no teeth. Floor the effective interval to the reference
    // RTT in that case so throttling actually spaces requests out (~freq_div * RTT).
    auto effective_gap = [&](int interval) -> std::int64_t {
        double base = static_cast<double>(interval);
        if (base <= 0.0 && freq_div > 1.0) {
            base = static_cast<double>(std::max<std::int64_t>(1, ref_rtt_ms_.load(std::memory_order_relaxed)));
        }
        return static_cast<std::int64_t>(base * freq_div);
    };

    while (running_.load(std::memory_order_acquire)) {
        std::int64_t now = nowSteadyMs();
        if (now < backoff_until_ms) {
            sleepInterruptibleMs(std::min<std::int64_t>(backoff_until_ms - now, 50));
            continue;
        }
        std::int64_t fills_gap = effective_gap(cfg_.fills_interval_ms);
        if (now - last_fills_ms >= fills_gap) {
            governed_fetch(true);
            last_fills_ms = nowSteadyMs();
        }
        if (!running_.load(std::memory_order_acquire)) break;

        now = nowSteadyMs();
        std::int64_t pos_gap = effective_gap(cfg_.positions_interval_ms);
        if (now >= backoff_until_ms) {
            if (now - last_pos_ms >= pos_gap) {
                governed_fetch(false);
                last_pos_ms = nowSteadyMs();
            }
        }

        // Heartbeat
        if (nowSteadyMs() - last_hb_ms >= cfg_.heartbeat_ms) {
            last_hb_ms = nowSteadyMs();
            if (log_info_) {
                log_info_("[FAST_POLLER] fills_polls=" + std::to_string(fills_polls_.load()) +
                          " positions_polls=" + std::to_string(positions_polls_.load()) +
                          " median_rtt_ms=" + std::to_string(median_rtt_ms_.load()) +
                          " ref_rtt_ms=" + std::to_string(ref_rtt_ms_.load()) +
                          " min_rtt_ms=" + std::to_string(min_rtt_ms_.load()) +
                          " backoffs=" + std::to_string(backoffs_.load()) +
                          " respawns=" + std::to_string(respawns_.load()) +
                          " throttled=" + std::string(throttled_.load() ? "1" : "0") +
                          " self_throttled=" + std::string(self_throttled_.load() ? "1" : "0") +
                          " freq_div=" + std::to_string(freq_div));
            }
        }

        // Sleep until the next fetch is due so a non-zero interval does not hot-spin.
        // Recompute gaps (freq_div / ref may have changed inside governed_fetch).
        fills_gap = effective_gap(cfg_.fills_interval_ms);
        pos_gap = effective_gap(cfg_.positions_interval_ms);
        now = nowSteadyMs();
        std::int64_t next_due = std::min(last_fills_ms + fills_gap, last_pos_ms + pos_gap);
        std::int64_t sleep_ms = next_due - now;
        if (cfg_.poll_min_interval_ms > 0) {
            sleep_ms = std::max<std::int64_t>(sleep_ms, cfg_.poll_min_interval_ms);
        }
        if (sleep_ms > 0) {
            // Cap each slice so stop()/backoff are observed promptly.
            sleepInterruptibleMs(std::min<std::int64_t>(sleep_ms, 50));
        } else if (fills_gap == 0 && pos_gap == 0) {
            // Back-to-back cadence: real fetches block ~RTT so this is not a busy-spin;
            // yield to avoid pathologically hot looping if a fetch ever returns instantly.
            std::this_thread::yield();
        }
    }
}

}  // namespace core
}  // namespace architect
