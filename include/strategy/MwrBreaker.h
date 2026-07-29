#pragma once

/**
 * @file MwrBreaker.h
 * @brief Moving Window Range (MWR) volatility breaker — pure, dependency-free logic.
 *
 * Tracks the rolling range of a sampled midpoint over a lookback window. When
 * (px_max - px_min) / tick exceeds a per-instrument threshold, the breaker enters a
 * fixed-duration "paused" state; while paused it tells the caller to pull/skip quoting.
 * Resume is automatic: once steady_clock passes paused_until, the next evaluate() proceeds.
 *
 * CONCURRENCY CONTRACT (non-negotiable):
 *   This object is MOVER-THREAD-ONLY. All state below is plain (no mutex, no atomic).
 *   It is touched exclusively from the MmOrderMover worker thread:
 *     - push()     from MakeMarketStrategy::mmDrainPendingQuoteCycle (1 Hz sampling)
 *     - evaluate() from MakeMarketStrategy::runFullMmQuoteCycle (gate, before any place/cancel)
 *   A debug-only owner-thread guard (see touchGuard()) asserts this invariant; in release it
 *   still records the owning thread so a test can verify off-thread access is detectable
 *   WITHOUT introducing any cross-thread synchronization of MWR data.
 *
 *   There is intentionally NO feed->mover ring buffer, SPSC queue, or shared atomic here.
 *   The single permitted atomic (a read-only paused_until_ms mirror for the web desk) lives
 *   on MakeMarketStrategy, NOT in this class, and is never read by the decision logic.
 */

#include <chrono>
#include <cstdint>
#include <deque>
#include <limits>
#include <thread>
#include <utility>
#include <cassert>

namespace architect {
namespace strategy {

class MwrBreaker {
public:
    using clock = std::chrono::steady_clock;

    enum class Action {
        kProceed,       ///< not paused, not breaching — run the cycle normally
        kPausedSkip,    ///< currently paused — caller must early-return (suppress quoting)
        kBreachPause,   ///< range breached this cycle — caller must cancel legs + early-return
        kInvalidTick,   ///< quote_tick <= 0 / non-finite — caller must skip MWR (do not divide)
        kInsufficient   ///< fewer than 2 samples in window — skip MWR this cycle
    };

    struct Result {
        Action       action{Action::kProceed};
        double       mwr_ticks{0.0};   ///< range / quote_tick (valid for kBreachPause / kProceed)
        bool         just_paused{false};   ///< unpaused -> paused edge (caller logs warn once)
        bool         just_resumed{false};  ///< paused -> unpaused edge (caller logs info once)
        std::int64_t paused_until_ms{0};   ///< steady_clock ms of pause expiry (for desk mirror)
    };

    /**
     * Record one midpoint sample. 1 Hz cadence is the CALLER's responsibility (it owns the
     * throttle timestamp so a disabled breaker costs nothing per feed tick). This only pushes
     * and evicts the window. window_sec<=0 is treated as disabled (no-op, no growth).
     */
    void push(clock::time_point now, double mid, int window_sec) {
        touchGuard();
        if (window_sec <= 0) {
            return;
        }
        samples_.emplace_back(now, mid);
        const clock::time_point cutoff = now - std::chrono::seconds(window_sec);
        while (!samples_.empty() && samples_.front().first < cutoff) {
            samples_.pop_front();
        }
    }

    /**
     * Evaluate the breaker for this cycle. Pause check is FIRST (so a sustained move stays
     * pulled). On breach, sets paused_until = now + max(pull_sec>0?pull_sec:window_sec, window_sec).
     * Caller (runFullMmQuoteCycle) must act on the returned Action.
     */
    Result evaluate(clock::time_point now, double quote_tick,
                    int size_ticks, int window_sec, int pull_sec) {
        touchGuard();
        Result r;

        // PAUSE CHECK FIRST — while paused, do not even look at the range.
        if (now < paused_until_) {
            r.action = Action::kPausedSkip;
            r.paused_until_ms = toMs(paused_until_);
            return r;
        }
        // Not paused now. Detect the paused -> unpaused transition exactly once.
        if (paused_) {
            paused_ = false;
            r.just_resumed = true;
        }

        if (!(quote_tick > 0.0) || !(quote_tick == quote_tick) /* NaN */ ||
            quote_tick == std::numeric_limits<double>::infinity()) {
            r.action = Action::kInvalidTick;
            return r;
        }
        if (samples_.size() < 2) {
            r.action = Action::kInsufficient;
            return r;
        }

        double mn = samples_.front().second;
        double mx = mn;
        for (const auto& s : samples_) {
            if (s.second < mn) mn = s.second;
            if (s.second > mx) mx = s.second;
        }
        r.mwr_ticks = (mx - mn) / quote_tick;

        if (r.mwr_ticks > static_cast<double>(size_ticks)) {
            const int pull = (pull_sec > 0) ? pull_sec : window_sec;
            const int dur = (pull > window_sec) ? pull : window_sec;
            paused_until_ = now + std::chrono::seconds(dur);
            paused_ = true;
            r.just_paused = true;
            r.paused_until_ms = toMs(paused_until_);
            r.action = Action::kBreachPause;
            return r;
        }

        r.action = Action::kProceed;
        return r;
    }

    // ---- Introspection (tests / desk mirror) ----
    std::size_t sampleCount() const { return samples_.size(); }
    bool isPaused() const { return paused_; }
    clock::time_point pausedUntil() const { return paused_until_; }
    std::int64_t pausedUntilMs() const { return toMs(paused_until_); }
    void clear() { samples_.clear(); }
    bool empty() const { return samples_.empty(); }

    /**
     * Range over the current window in price units (max-min), or 0 if <2 samples.
     * Provided for tests asserting window eviction; not used by production decision logic.
     */
    double windowRange() const {
        if (samples_.size() < 2) return 0.0;
        double mn = samples_.front().second, mx = mn;
        for (const auto& s : samples_) {
            if (s.second < mn) mn = s.second;
            if (s.second > mx) mx = s.second;
        }
        return mx - mn;
    }

    /**
     * Owner-thread guard for tests: returns true iff called from the thread that first
     * touched this breaker. Reads only the (single-write-before-publish) owner id, so a
     * test can spawn a thread and assert this returns false there — no data race, no atomic.
     */
    bool calledFromOwnerThread() const {
        return owner_bound_ && owner_ == std::this_thread::get_id();
    }
    bool ownerBound() const { return owner_bound_; }

private:
    static std::int64_t toMs(clock::time_point tp) {
        return std::chrono::duration_cast<std::chrono::milliseconds>(tp.time_since_epoch()).count();
    }

    // Records the first-touching thread; asserts same-thread access in debug builds.
    void touchGuard() {
        const std::thread::id tid = std::this_thread::get_id();
        if (!owner_bound_) {
            owner_ = tid;
            owner_bound_ = true;
        }
#ifndef NDEBUG
        else {
            assert(owner_ == tid && "MwrBreaker accessed off the mover thread");
        }
#endif
    }

    std::deque<std::pair<clock::time_point, double>> samples_;
    clock::time_point paused_until_{};
    bool paused_{false};

    std::thread::id owner_{};
    bool owner_bound_{false};
};

}  // namespace strategy
}  // namespace architect
