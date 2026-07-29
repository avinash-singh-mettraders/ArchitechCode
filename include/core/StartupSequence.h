#pragma once

/**
 * @file StartupSequence.h
 * @brief 25-Step Platform Startup Framework
 * 
 * Orchestrates the complete startup sequence:
 * - Steps 1-6:   Core System Initialization
 * - Steps 7-12:  API & Connectivity
 * - Steps 13-17: Market Data Infrastructure
 * - Steps 18-21: Trading Infrastructure
 * - Steps 22-23: Pre-Trade Verification
 * - Step 24:     Trading Loop
 * - Step 25:     Graceful Shutdown
 * 
 * Each step can CRASH (throw exception) if a critical check fails.
 */

#include "core/Types.h"
#include "core/Platform.h"  // For SimulationParams
#include <string>
#include <functional>
#include <chrono>
#include <vector>
#include <atomic>
#include <mutex>
#include <condition_variable>
#include <deque>
#include <memory>
#include <cstdint>

namespace architect {
namespace core {

// Pillar C: dedicated continuous fills+positions poller (fetch/parse off the main loop).
class FastPoller;

/**
 * @brief Result of a single startup step
 */
struct StepResult {
    int step_number;
    std::string step_name;
    bool success;
    std::string message;
    std::chrono::milliseconds duration{0};
    
    StepResult(int num, const std::string& name)
        : step_number(num), step_name(name), success(false) {}
};

/**
 * @brief Startup step status for logging
 */
enum class StepStatus {
    PENDING,
    RUNNING,
    SUCCESS,
    FAILED,
    SKIPPED
};

inline const char* stepStatusToString(StepStatus status) {
    switch (status) {
        case StepStatus::PENDING: return "PENDING";
        case StepStatus::RUNNING: return "RUNNING";
        case StepStatus::SUCCESS: return "SUCCESS";
        case StepStatus::FAILED:  return "FAILED";
        case StepStatus::SKIPPED: return "SKIPPED";
        default: return "UNKNOWN";
    }
}

/**
 * @brief Exception thrown when startup sequence fails
 */
class StartupException : public std::runtime_error {
public:
    StartupException(int step, const std::string& step_name, const std::string& msg)
        : std::runtime_error("STEP " + std::to_string(step) + " [" + step_name + "] FAILED: " + msg)
        , step_number_(step)
        , step_name_(step_name) {}
    
    int getStepNumber() const { return step_number_; }
    const std::string& getStepName() const { return step_name_; }
    
private:
    int step_number_;
    std::string step_name_;
};

/**
 * @brief Exit status after graceful shutdown
 */
struct ShutdownStatus {
    bool clean_exit = false;
    bool orders_cancelled = false;
    bool events_drained = false;
    bool logs_flushed = false;
    bool websocket_disconnected = false;
    std::string exit_reason;
    std::chrono::milliseconds shutdown_duration{0};
    
    bool isGraceful() const {
        return clean_exit && orders_cancelled && events_drained && 
               logs_flushed && websocket_disconnected;
    }
};

/**
 * @brief 25-Step Startup Sequence Orchestrator
 */
class StartupSequence {
public:
    /**
     * @brief Get singleton instance
     */
    static StartupSequence& getInstance();
    
    StartupSequence(const StartupSequence&) = delete;
    StartupSequence& operator=(const StartupSequence&) = delete;
    
    // =========================================================================
    // Main Entry Points
    // =========================================================================
    
    /**
     * @brief Execute steps 1-23 (initialization and pre-trade verification)
     * @param params Simulation parameters
     * @throws StartupException if any step fails
     */
    void executeStartup(const SimulationParams& params);
    
    /**
     * @brief Execute step 24 (trading loop) - blocks until exit signal
     * @param exit_signal Atomic flag to signal exit (set by signal handler)
     */
    void executeTradingLoop(std::atomic<bool>& exit_signal);
    
    /**
     * @brief Execute step 25 (graceful shutdown)
     * @param reason Reason for shutdown
     * @return Shutdown status
     */
    ShutdownStatus executeShutdown(const std::string& reason);
    
    /**
     * @brief Get the cached PnL from exchange (updated by pollExchangePositions)
     */
    double getExchangePnL() const { return cached_exchange_pnl_.load(); }

    /**
     * @brief Steady-clock millis when the most recent /fills REST poll
     *        returned (success OR failure — what matters is "we tried and
     *        processed any new fills we got"). Zero means no poll has
     *        completed yet since process start.
     *
     * Consumed by `MakeMarketStrategy`'s reconciliation gate (Tim's spec,
     * 2026-05-21): after this stack cancels an order we cannot place a
     * new order until at least one full fill poll has completed after the
     * cancel response. This guarantees that if the cancelled order actually
     * filled (404 race), the resulting fill event is delivered to
     * `OrderManager::onOrderFilled` and `position_state_.net_position_qty`
     * is incremented BEFORE the cap-gate evaluates the next submit.
     */
    static std::int64_t lastFillPollCompletedMs() {
        return last_fill_poll_completed_ms_.load(std::memory_order_acquire);
    }
    
    // =========================================================================
    // Individual Step Execution (for custom orchestration)
    // =========================================================================
    
    StepResult step01_ValidateParameters(const SimulationParams& params);
    StepResult step02_LoadConfiguration(const std::string& config_path);
    StepResult step03_InitializeLogger(const std::string& simulation_date);
    StepResult step04_InitializeEventManager();
    StepResult step05_InitializeOrderManager();
    StepResult step06_InitializeUserManager();
    
    StepResult step07_InitializeRestClient();
    StepResult step08_InitializeWebSocketClient();
    StepResult step09_Authenticate();
    StepResult step10_VerifyApiConnectivity();
    StepResult step11_DetectFeedMode();
    StepResult step12_CheckExchangeStatus();
    
    StepResult step13_InitializeMarketDataManager();
    StepResult step14_InitializeExternalFeedManager();
    StepResult step15_ConnectWebSocket();
    StepResult step16_AuthenticateWebSocket();
    StepResult step17_SubscribeMarketDataChannels();
    
    StepResult step18_InitializePortfolioManager();
    StepResult step19_VerifyAccountPermissions();
    StepResult step20_SubscribePrivateChannels();
    StepResult step21_InitializeStrategyManager();
    
    StepResult step22_VerifyExternalFeed();
    StepResult step23_VerifyMarketData();
    
    // Step 24: Trading loop (continuous)
    // Step 25: Graceful shutdown
    
    // =========================================================================
    // Status & Progress
    // =========================================================================
    
    /**
     * @brief Get current step number (1-25)
     */
    int getCurrentStep() const { return current_step_.load(); }
    
    /**
     * @brief Get step status
     */
    StepStatus getStepStatus(int step) const;
    
    /**
     * @brief Get all step results
     */
    std::vector<StepResult> getStepResults() const;
    
    /**
     * @brief Check if startup completed successfully
     */
    bool isStartupComplete() const { return startup_complete_.load(); }
    
    /**
     * @brief Check if currently in trading loop (step 24)
     */
    bool isTrading() const { return trading_.load(); }
    
    /**
     * @brief Get last shutdown status
     */
    const ShutdownStatus& getLastShutdownStatus() const { return last_shutdown_; }
    
private:
    StartupSequence();
    ~StartupSequence() = default;
    
    // Helpers
    void logStepStart(int step, const std::string& name);
    void logStepEnd(int step, const StepResult& result);
    void crashOnFailure(const StepResult& result);
    /** S1–S3: cancel OIDs from orders.json + venue open orders, then truncate to {"stacks":[]}. Best-effort. */
    void runOrdersJsonCleanSlateAfterRestVerified();
    void pollExchangeFills(std::string& last_fill_id);
    void pollExchangePositions();

    // --- Pillar C: fetch/apply split + FastPoller handoff --------------------
    // The parse+apply halves ALWAYS run on the main-loop thread (PositionBook
    // single-writer). The FastPoller performs the HTTP GET (the ~RTT) on its own
    // thread and hands the raw body back via the queue below.
    void applyExchangeFillsBody(const std::string& body, std::int64_t start_ts_ns,
                                std::string& last_fill_id);
    void applyExchangePositionsBody(const std::string& body, bool apply_snapshot);
    /** Drain the FastPoller handoff queue and apply on the main loop. Returns count applied. */
    int drainFastPollerHandoff(std::string& last_fill_id);

    struct PollHandoffItem {
        bool is_fills{false};
        std::string body;
        std::int64_t start_ts_ns{0};      // fills only: cursor used for the fetch
        std::int64_t fetch_steady_ms{0};  // steady-clock ms when the fetch completed (for age_ms)
    };
    std::mutex poll_handoff_mutex_;
    std::condition_variable poll_handoff_cv_;
    std::deque<PollHandoffItem> poll_handoff_queue_;
    // Shared fills cursor: FastPoller reads to build its next fetch; the main loop
    // writes it after each apply. Stale reads are harmless (trade_id dedupe covers overlap).
    std::atomic<std::int64_t> fast_poller_fills_cursor_ns_{0};
    std::unique_ptr<FastPoller> fast_poller_;

    std::atomic<int> current_step_{0};
    std::atomic<bool> startup_complete_{false};
    std::atomic<bool> trading_{false};
    
    // Cached exchange PnL for heartbeat display (updated by pollExchangePositions)
    std::atomic<double> cached_exchange_pnl_{0.0};

    // Steady-clock millis of most recent pollExchangeFills completion.
    // See public accessor `lastFillPollCompletedMs()` for usage rationale.
    static std::atomic<std::int64_t> last_fill_poll_completed_ms_;
    
    mutable std::mutex results_mutex_;
    std::vector<StepResult> step_results_;
    std::vector<StepStatus> step_status_;
    
    ShutdownStatus last_shutdown_;
    
    // Cached params
    SimulationParams cached_params_;
};

// Convenience function
inline StartupSequence& Startup() {
    return StartupSequence::getInstance();
}

} // namespace core
} // namespace architect
