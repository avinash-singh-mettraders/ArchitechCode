#pragma once

/**
 * @file ExternalFeedManager.h
 * @brief External price feed for theo pricing (Neon FIX or generic REST bookTicker)
 *
 * Used to price orders from an external source. Default provider: neon_fix.
 * Replace per client (CME, FX ECN, LSEG, etc.).
 */

#include "marketdata/ExternalFeedTypes.h"
#include <string>
#include <optional>
#include <atomic>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <memory>
#include <chrono>
#include <functional>
#include <vector>
#include <utility>
#include <unordered_map>
#include <unordered_set>

namespace architect {
namespace marketdata {

/**
 * @brief Callback signature for feed update notifications
 * @param quote The new quote data
 */
using FeedUpdateCallback = std::function<void(const ExternalFeedQuote&)>;

/**
 * @brief Manages one external feed: REST bookTicker polling, or FIX neon_fix (top of book).
 */
class ExternalFeedManager {
public:
    static ExternalFeedManager& getInstance();

    ExternalFeedManager(const ExternalFeedManager&) = delete;
    ExternalFeedManager& operator=(const ExternalFeedManager&) = delete;

    void start();
    void stop();
    [[nodiscard]] bool isRunning() const { return running_.load(); }
    [[nodiscard]] bool isEnabled() const { return enabled_.load(); }

    /**
     * @brief Get theo mid for a reference symbol (market_maker.theo_symbol / reference_fix_symbol).
     * Matches neon FIX 55 using fixSymbolCanonical (slash form vs compact form).
     */
    std::optional<core::Price> getTheoPrice(const std::string& theo_symbol) const;

    /**
     * Bid/ask/mid for a specific reference / venue symbol (multi-theo); same keying as getTheoPrice.
     * Prefer this over getLastQuote() when correlating MM_VERIFY to one leg (getLastQuote is global order).
     */
    std::optional<ExternalFeedQuote> getQuoteForTheoSymbol(const std::string& theo_symbol) const;

    /**
     * @brief Get last quote (bid/ask/mid) for the feed symbol
     */
    std::optional<ExternalFeedQuote> getLastQuote() const;

    /**
     * @brief Register a callback for immediate feed update notifications
     * @param callback Function to call when new quote arrives
     * @return Callback ID for unregistering
     */
    int registerCallback(FeedUpdateCallback callback);
    
    /**
     * @brief Unregister a previously registered callback
     * @param callback_id The ID returned from registerCallback
     */
    void unregisterCallback(int callback_id);

    struct Stats {
        std::uint64_t poll_count{0};
        std::uint64_t success_count{0};
        std::uint64_t error_count{0};
        std::chrono::steady_clock::time_point last_success_at;
    };
    [[nodiscard]] Stats getStats() const;

    /** Last fetch failure reason (for diagnostics when theo is missing). */
    [[nodiscard]] std::string getLastFetchError() const;

    /**
     * True when pricing transport is up: neon_fix = FIX Logon accepted (session); REST = last poll HTTP succeeded.
     * When external_feed.enabled is false, returns true (nothing to monitor).
     */
    [[nodiscard]] bool isPricingTransportConnected() const;

    /**
     * @brief Per-feed liveness flags.
     *
     * Each `is<Feed>Up()` reports whether that feed has produced at least one
     * valid quote and has not since errored. They are independent — a partial
     * outage on (say) Neon must not pull down the CME or Hyperliquid signals.
     * `last<Feed>OkMs()` returns the ms-since-epoch wall clock of the most
     * recent successful publish for that feed (0 if none yet). `last<Feed>Error()`
     * returns the last user-visible diagnostic for that feed (empty when ok).
     */
    [[nodiscard]] bool isMettradersUp() const { return mettraders_up_.load(); }
    [[nodiscard]] bool isMettradersWsConnected() const { return mettraders_ws_connected_.load(); }
    [[nodiscard]] bool isHlUp() const { return hl_up_.load(); }
    [[nodiscard]] bool isNeonUp() const { return neon_up_.load(); }
    [[nodiscard]] std::int64_t lastMettradersOkMs() const { return last_mettraders_ok_ms_.load(); }
    [[nodiscard]] std::int64_t lastHlOkMs() const { return last_hl_ok_ms_.load(); }
    [[nodiscard]] std::int64_t lastNeonOkMs() const { return last_neon_ok_ms_.load(); }
    [[nodiscard]] std::string lastMettradersError() const;
    [[nodiscard]] std::string lastHlError() const;
    [[nodiscard]] std::string lastNeonError() const;

    /**
     * @brief Resolve a leg's theo_source ("mettraders"|"hyperliquid"|"neon_fix") to liveness.
     *
     * Returns true when the named feed is up, false when down. Unknown feed
     * names return true (conservative — does not block legs whose source we
     * cannot classify so existing behavior is preserved).
     */
    [[nodiscard]] bool isFeedUpForSource(const std::string& source) const;

    /** Increment error streak; when threshold reached, mark quote invalid and notify callbacks (MM pulls quotes). */
    void recordQuoteErrorForProtectiveInvalidate();
    /** Reset error streak after a successful quote publish. */
    void resetQuoteErrorStreak();

private:
    ExternalFeedManager();
    ~ExternalFeedManager();

    void pollLoop();
    void fixFeedLoop();
    bool fetchAndUpdate();
    void publishQuote(const ExternalFeedQuote& quote);
    void invalidateQuoteAndNotifyCallbacks();
    void notifyCallbacks(const ExternalFeedQuote& quote);
    /** JSON file for MM Live Desk: primary quote + per-symbol map (caller holds quote_mutex_). */
    void writeDeskTheoCacheLocked() const;
    /** Throttled JSON ladder (top N bid/ask rows) from Neon FIX for the web desk. */
    void maybeWriteDeskDepthCache(const std::vector<std::pair<double, double>>& bids,
                                  const std::vector<std::pair<double, double>>& asks) const;

    static bool isNeonFixProvider(const std::string& provider);
    static bool isHyperliquidProvider(const std::string& provider);
    bool fetchAndUpdateHyperliquid();
    bool fetchAndUpdateHyperliquidMulti();
    bool fetchAndUpdateMettraders();
    void mettradersFeedLoop();
    void auxRestPollLoop();
    void ensureStunnelOrThrow();
    void stopManagedStunnel();

    std::atomic<bool> enabled_{false};
    std::atomic<bool> running_{false};
    /** neon_fix: set true after FIX Logon (35=A), false on disconnect / reconnect / error. */
    std::atomic<bool> fix_session_logged_on_{false};
    /** REST external: last bookTicker poll succeeded. */
    std::atomic<bool> rest_last_fetch_ok_{true};
    std::atomic<bool> rest_hl_last_ok_{true};
    std::atomic<bool> rest_cme_last_ok_{true};

    /**
     * Per-feed public liveness — set true on first valid publish, false on
     * subsequent error. Independent of legs/multi_theo so the desk and the
     * trade gate can read each feed in isolation.
     */
    /**
     * `mettraders_up_` is the public health flag and now means "websocket
     * connected AND at least one quote successfully parsed + published".
     * `mettraders_ws_connected_` is the transport-only flag (handshake ok)
     * exposed for diagnostics; do NOT use it for trade-gating because a
     * connected-but-silent feed used to be reported as up (false green pill).
     */
    std::atomic<bool> mettraders_up_{false};
    std::atomic<bool> mettraders_ws_connected_{false};
    std::atomic<bool> hl_up_{false};
    std::atomic<bool> neon_up_{false};
    std::atomic<std::int64_t> last_mettraders_ok_ms_{0};
    std::atomic<std::int64_t> last_hl_ok_ms_{0};
    std::atomic<std::int64_t> last_neon_ok_ms_{0};
    /** First-quote sanity log emitted at most once per feed per process. */
    std::atomic<bool> mettraders_sanity_logged_{false};
    std::atomic<bool> hl_sanity_logged_{false};
    // One-shot info log: "xyz HIP-3 DEX merged keys=…"
    std::atomic<bool> hl_xyz_logged_{false};
    std::atomic<bool> neon_sanity_logged_{false};
    /** Per-leg HL match diagnostic logged at most once per leg per process. */
    std::unordered_set<std::string> hl_leg_match_logged_;
    mutable std::mutex hl_leg_match_mu_;
    /** Throttle for `logs/hl_allmids.json` dumps (steady ms; ~1 dump / 30s). */
    std::atomic<std::int64_t> hl_allmids_dump_last_ms_{0};
    mutable std::mutex feed_err_mu_;
    std::string mettraders_last_err_;
    std::string hl_last_err_;
    std::string neon_last_err_;
    std::thread poll_thread_;
    /** When isMultiTheoFeeds: combined Hyperliquid + CME REST polling. */
    std::thread aux_poll_thread_;
    std::thread mettraders_thread_;
    std::atomic<int> mettraders_raw_msgs_logged_{0};

    mutable std::mutex quote_mutex_;
    ExternalFeedQuote last_quote_;
    /** Canonical FIX symbol -> latest valid quote (neon multi-symbol). */
    std::unordered_map<std::string, ExternalFeedQuote> quotes_by_canonical_;
    std::string display_symbol_;

    mutable std::mutex stats_mutex_;
    Stats stats_;
    std::string last_fetch_error_;
    std::atomic<int> consecutive_quote_errors_{0};
    
    // Callback system for immediate quote update notifications.
    //
    // UAF guard (2026-06-12): each callback is held via a shared_ptr<CallbackEntry> that also
    // tracks an in-flight dispatch count. notifyCallbacks pins the entries and bumps `in_flight`
    // UNDER `callbacks_mutex_`, then runs the callback OUTSIDE the lock. unregisterCallback removes
    // the entry under the lock and then BLOCKS on `callbacks_drain_cv_` until that entry's
    // `in_flight` reaches 0. Because ~MakeMarketStrategy calls unregisterCallback, the strategy
    // object can never be freed while a feed thread is still inside its callback — closing the
    // copy-then-free race in notifyCallbacks.
    struct CallbackEntry {
        FeedUpdateCallback cb;
        std::atomic<int> in_flight{0};
    };
    mutable std::mutex callbacks_mutex_;
    std::condition_variable callbacks_drain_cv_;
    std::vector<std::pair<int, std::shared_ptr<CallbackEntry>>> callbacks_;
    int next_callback_id_{1};

    /** POSIX: child stunnel pid when we spawned it; -1 otherwise. */
    int stunnel_pid_{-1};
    bool stunnel_we_started_{false};

    mutable std::mutex desk_depth_mu_;
    mutable std::chrono::steady_clock::time_point last_desk_depth_write_{};
    mutable bool desk_depth_has_written_{false};
};

} // namespace marketdata
} // namespace architect
