#pragma once

/**
 * @file FastMarketMonitor.h
 * @brief GLOBAL "fast market" volatility breaker driven by a single watched instrument
 *        (HyperLiquid S&P500 perp, "HL SPX").
 *
 * Scope (supersedes the per-instrument MWR trigger as the primary breaker; the per-instrument
 * MWR code in MakeMarketStrategy is kept intact and ADDITIVE — this is a second, independent
 * trigger feeding the SAME suppression gates):
 *   - Sample the HL SPX midpoint at ~1 Hz.
 *   - Track its rolling range with the already-committed MwrBreaker (range/deque/pause logic).
 *   - If (px_max - px_min)/tick exceeds SizeOfMove, set a GLOBAL pause deadline and pull ALL
 *     orders across ALL markets, suppressing all placement everywhere until resume.
 *
 * CONCURRENCY CONTRACT (mirrors the committed MmOrderMover / MwrBreaker design):
 *   - All sampling/evaluation runs on the ONE MmOrderMover worker thread (see MmOrderMover.h —
 *     a single global FIFO worker). FastMarketCore's deque/breaker state is therefore mover-only,
 *     plain (no mutex, no per-object atomic).
 *   - The ONLY shared primitive is a single process-wide std::atomic<int64_t> pause deadline
 *     (steady_clock ms), justified because it is genuinely global and read by every strategy's
 *     suppression predicate from multiple threads. It is a function-local static so this header
 *     is link-clean for the self-contained logic test (no .cpp needed to exercise the core).
 *
 * SPLIT:
 *   - FastMarketCore  : pure, header-only, dependency-free decision logic (reuses MwrBreaker).
 *                       Unit-tested directly (no engine link).
 *   - FastMarketMonitor: singleton that wires Config + ExternalFeedManager + StrategyManager +
 *                       MmOrderMover to the core, and performs the breach fan-out. Engine-only.
 */

#include "strategy/MwrBreaker.h"

#include <atomic>
#include <chrono>
#include <cstdint>

namespace architect {
namespace strategy {

/**
 * Pure decision core for the global fast-market breaker. Mover-thread-only state (plain members);
 * the single shared pause deadline is a function-local static atomic shared process-wide.
 */
class FastMarketCore {
public:
    using clock = std::chrono::steady_clock;

    /** Monitor self-staleness: if it hasn't been ticked in this long, treat like a stale feed. */
    static constexpr std::int64_t kSelfStaleMs = 3000;
    /** Clear-on-gap (finding F): a sampling gap longer than this drops the window so a range can
     *  never be computed straddling the gap. ~2x the 1 Hz sample interval. */
    static constexpr std::int64_t kGapMs = 2000;
    /** HL feed staleness threshold (consumed by FastMarketMonitor; centralized here). */
    static constexpr std::int64_t kHlStaleMs = 3000;

    struct TickResult {
        enum class Action {
            kDisabled,      ///< symbol unset / tick<=0 / size<=0 / window<=0 — never trips
            kFailOpen,      ///< HL feed stale OR monitor self-stale — FAIL-OPEN, never trips
            kDropInvalid,   ///< no valid mid this tick — sample dropped
            kInsufficient,  ///< <2 fresh samples in the window
            kProceed,       ///< evaluated, no breach
            kBreachPause,   ///< breached: global deadline set this tick
            kPausedSkip     ///< already globally paused (deadline still in the future)
        };
        Action       action{Action::kProceed};
        double       mwr_ticks{0.0};
        bool         just_paused{false};    ///< unpaused->paused edge (caller logs once + fans out)
        std::int64_t paused_until_ms{0};
        bool         self_stale{false};     ///< exposed for logging/tests
    };

    /** The single shared primitive: global pause deadline in steady_clock ms. */
    static std::atomic<std::int64_t>& pausedUntilMs() {
        static std::atomic<std::int64_t> v{0};
        return v;
    }
    static std::int64_t nowMs() {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
                   clock::now().time_since_epoch())
            .count();
    }
    /** Deterministic predicate (test-friendly): is a given steady-ms instant within the pause? */
    static bool isGloballyPausedAt(std::int64_t now_ms) {
        return now_ms < pausedUntilMs().load(std::memory_order_relaxed);
    }
    /** Production predicate: reads the real steady clock vs the shared deadline. */
    static bool isGloballyPausedNow() { return isGloballyPausedAt(nowMs()); }

    /**
     * The config-based disable decision, shared by FastMarketMonitor::onMoverTick and its tests.
     * The breaker is disabled when the SPX symbol is unset OR the explicit on/off switch
     * (market_maker.fast_market.enabled, default true) is off — so it can be turned off from the
     * desk WITHOUT blanking the symbol (the symbol is preserved for the next re-enable).
     */
    static bool configDisabledFromConfig(bool symbol_empty, bool enabled) {
        return symbol_empty || !enabled;
    }

    /**
     * Drive one evaluation from already-gathered inputs (config/feed reads live in the caller).
     * Updates the internal MwrBreaker window and, on breach, the global atomic deadline.
     *   - config_disabled : symbol unset (the caller's only config-dependent disable input).
     *   - feed_stale      : HL feed down/stale (FAIL-OPEN decision made by the caller).
     *   - have_valid_mid  : a finite, >0 midpoint was read this tick.
     * Self-staleness is computed here from this object's own tick cadence.
     */
    TickResult evaluateTick(clock::time_point now,
                            bool config_disabled, bool feed_stale,
                            bool have_valid_mid, double mid,
                            double tick, int size_ticks, int window_sec, int pull_sec) {
        TickResult tr;

        // Self-staleness from our OWN tick cadence (mover backed up / loop stalled).
        const bool self_stale =
            have_last_tick_ &&
            (now - last_tick_ts_) > std::chrono::milliseconds(kSelfStaleMs);
        last_tick_ts_ = now;
        have_last_tick_ = true;
        tr.self_stale = self_stale;

        const bool disabled = config_disabled || !(tick > 0.0) || (tick != tick) /*NaN*/ ||
                              size_ticks <= 0 || window_sec <= 0;
        if (disabled) {
            tr.action = TickResult::Action::kDisabled;
            return tr;
        }

        if (feed_stale || self_stale) {
            // FAIL-OPEN: never trip on stale/sparse data. Drop the window so that when data
            // returns we cannot manufacture a range straddling the outage.
            mwr_.clear();
            have_last_sample_ = false;
            tr.action = TickResult::Action::kFailOpen;
            return tr;
        }

        if (!have_valid_mid || !(mid > 0.0) || mid != mid /*NaN*/) {
            tr.action = TickResult::Action::kDropInvalid;
            return tr;
        }

        // Finding F (clear-on-gap): a sampling gap must not yield a 2-sample straddle range.
        if (have_last_sample_ &&
            (now - last_sample_ts_) > std::chrono::milliseconds(kGapMs)) {
            mwr_.clear();
            have_last_sample_ = false;
        }
        mwr_.push(now, mid, window_sec);
        last_sample_ts_ = now;
        have_last_sample_ = true;

        const auto res = mwr_.evaluate(now, tick, size_ticks, window_sec, pull_sec);
        tr.mwr_ticks = res.mwr_ticks;
        tr.paused_until_ms = res.paused_until_ms;
        switch (res.action) {
            case MwrBreaker::Action::kPausedSkip:
                tr.action = TickResult::Action::kPausedSkip;
                break;
            case MwrBreaker::Action::kInsufficient:
                tr.action = TickResult::Action::kInsufficient;
                break;
            case MwrBreaker::Action::kInvalidTick:
                tr.action = TickResult::Action::kDropInvalid;
                break;
            case MwrBreaker::Action::kProceed:
                tr.action = TickResult::Action::kProceed;
                break;
            case MwrBreaker::Action::kBreachPause:
                pausedUntilMs().store(res.paused_until_ms, std::memory_order_relaxed);
                tr.action = TickResult::Action::kBreachPause;
                tr.just_paused = res.just_paused;
                break;
        }
        return tr;
    }

    // ---- Introspection (tests) ----
    std::size_t sampleCount() const { return mwr_.sampleCount(); }
    bool haveLastSample() const { return have_last_sample_; }

private:
    MwrBreaker mwr_;
    bool have_last_sample_{false};
    clock::time_point last_sample_ts_{};
    bool have_last_tick_{false};
    clock::time_point last_tick_ts_{};
};

/**
 * Singleton engine wiring for the global fast-market breaker. Posts a ~1 Hz sample+evaluate job
 * onto the single MmOrderMover worker, reads HL SPX from ExternalFeedManager, and on breach fans
 * out a cancel-only job to every live MM strategy. Suppression is read by every strategy's pause
 * predicate via isGloballyPausedNow().
 */
class FastMarketMonitor {
public:
    static FastMarketMonitor& getInstance();

    /** Post one monitor tick to the single mover worker. Thread-safe; called ~1 Hz from the
     *  StartupSequence main loop (UNCONDITIONAL — see StartupSequence). Returns immediately. */
    void enqueueTick();

    /** Suppression predicate read by the cycle gate + Item G placement gates (any thread). */
    static bool isGloballyPausedNow() { return FastMarketCore::isGloballyPausedNow(); }
    static std::int64_t pausedUntilMs() {
        return FastMarketCore::pausedUntilMs().load(std::memory_order_relaxed);
    }

private:
    FastMarketMonitor() = default;
    FastMarketMonitor(const FastMarketMonitor&) = delete;
    FastMarketMonitor& operator=(const FastMarketMonitor&) = delete;

    void onMoverTick();  // runs on the single mover worker

    FastMarketCore core_;
    bool disabled_log_edge_{false};
    std::int64_t fail_open_warn_last_ms_{0};
    // Throttled liveness heartbeat (wall-clock ms of last emission). The breaker is silent in
    // its steady non-breach states (kProceed/kInsufficient/kDropInvalid/kPausedSkip), which makes
    // an armed-but-quiet monitor indistinguishable from a dead one. The heartbeat proves it is
    // watching. LOG-ONLY; never affects the breach decision.
    std::int64_t heartbeat_last_ms_{0};
};

}  // namespace strategy
}  // namespace architect
