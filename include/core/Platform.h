#pragma once

/**
 * @file Platform.h
 * @brief Platform "God Object" - Central access point for all platform components
 * 
 * Initialization sequence:
 * 1. Validate config path
 * 2. Load configuration
 * 3. Initialize Logger (check/create logs directory)
 * 4. Initialize modules one by one (crash on failure)
 * 5. Save config to log directory
 */

#include "core/Types.h"
#include "core/Constants.h"
#include "config/Config.h"
#include "utils/Logger.h"
#include "events/EventManager.h"
#include "orders/OrderManager.h"
#include "orders/OrderBook.h"
#include "user/UserManager.h"
#include "api/RestClient.h"
#include "api/WebSocketClient.h"
#include "marketdata/MarketDataManager.h"
#include "marketdata/ExternalFeedManager.h"
#include "portfolio/PortfolioManager.h"
#include "strategy/Strategy.h"
#include "hedging/HedgeProvider.h"

#include <string>
#include <memory>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <stdexcept>

namespace architect {
namespace core {

namespace fs = std::filesystem;

/**
 * @brief Exception for platform initialization failures
 */
class PlatformInitException : public std::runtime_error {
public:
    explicit PlatformInitException(const std::string& msg) 
        : std::runtime_error("Platform initialization failed: " + msg) {}
};

/**
 * @brief Simulation parameters (3 required inputs)
 */
struct SimulationParams {
    std::string binary_path;        // Path to the binary (argv[0])
    std::string config_path;        // Path to configuration file
    std::string simulation_date;    // Date in YYYYMMDD format (e.g., "20260130")
    
    [[nodiscard]] bool isValid() const {
        return !config_path.empty() && 
               simulation_date.length() == 8;  // YYYYMMDD
    }
    
    [[nodiscard]] std::string validate() const {
        if (config_path.empty()) {
            return "Config path is required";
        }
        if (!fs::exists(config_path)) {
            return "Config file does not exist: " + config_path;
        }
        if (simulation_date.length() != 8) {
            return "Invalid date format. Expected YYYYMMDD, got: " + simulation_date;
        }
        // Basic date validation
        try {
            int year = std::stoi(simulation_date.substr(0, 4));
            int month = std::stoi(simulation_date.substr(4, 2));
            int day = std::stoi(simulation_date.substr(6, 2));
            if (year < 2000 || year > 2100 || month < 1 || month > 12 || day < 1 || day > 31) {
                return "Invalid date: " + simulation_date;
            }
        } catch (...) {
            return "Invalid date format: " + simulation_date;
        }
        return "";  // Empty = valid
    }
};

/**
 * @brief Module initialization status
 */
struct ModuleStatus {
    bool config_loaded      = false;
    bool logger_initialized = false;
    bool events_initialized = false;
    bool orders_initialized = false;
    bool user_initialized   = false;
    bool api_initialized    = false;
    bool marketdata_initialized = false;
    bool portfolio_initialized  = false;
    bool feed_verified      = false;
    
    [[nodiscard]] bool allInitialized() const {
        return config_loaded && logger_initialized && events_initialized &&
               orders_initialized && user_initialized && api_initialized &&
               marketdata_initialized && portfolio_initialized;
    }
};

/**
 * @brief Feed status - determined after initialization
 */
struct FeedStatus {
    FeedMode                mode                = FeedMode::UNKNOWN;
    FeedSource              source              = FeedSource::WEBSOCKET_LIVE;
    bool                    is_live             = false;
    bool                    is_paper            = false;
    bool                    is_simulation       = false;
    bool                    verified            = false;
    std::string             environment;        // "production", "sandbox", "paper"
    std::string             server_time;
    std::uint64_t           latency_ms          = 0;
    MarketStatus            market_status       = MarketStatus::CLOSED;
    std::string             error_message;
};

/**
 * @brief Platform "God Object" - Central coordinator for all components
 * 
 * Usage:
 *   auto& main = core::Main();
 *   main.initialize(params);  // Crashes on failure
 *   main.start();
 *   main.logger()->info("message");
 *   main.logger()->log_order(...);
 */
class Platform {
public:
    /**
     * @brief Get the singleton instance
     */
    static Platform& getInstance();
    
    // Prevent copying
    Platform(const Platform&) = delete;
    Platform& operator=(const Platform&) = delete;
    
    // ==========================================================================
    // Lifecycle
    // ==========================================================================
    
    /**
     * @brief Initialize the platform with simulation parameters
     * 
     * This function will CRASH (throw exception) if any initialization fails.
     * 
     * Initialization order:
     * 1. Validate parameters
     * 2. Load configuration
     * 3. Initialize logger (validates/creates logs directory)
     * 4. Initialize EventManager
     * 5. Initialize OrderManager
     * 6. Initialize UserManager
     * 7. Initialize API clients
     * 8. Initialize MarketDataManager
     * 9. Initialize PortfolioManager
     * 10. Save config to log directory
     * 
     * @param params Simulation parameters (binary_path, config_path, date)
     * @throws PlatformInitException on any failure
     */
    void initialize(const SimulationParams& params);
    
    /**
     * @brief Start the platform (after initialization)
     * @throws PlatformInitException if not initialized
     */
    void start();
    
    /**
     * @brief Stop the platform gracefully
     */
    void stop();
    
    /**
     * @brief Check if platform is running
     */
    [[nodiscard]] bool isRunning() const { return running_.load(); }
    
    /**
     * @brief Check if platform is initialized
     */
    [[nodiscard]] bool isInitialized() const { return initialized_.load(); }
    
    /**
     * @brief Get module status
     */
    [[nodiscard]] const ModuleStatus& getModuleStatus() const { return module_status_; }
    
    /**
     * @brief Get mutable module status (for StartupSequence to set flags)
     */
    ModuleStatus& moduleStatus() { return module_status_; }
    
    /**
     * @brief Initialize event handlers for order gateway and logging
     * Called by StartupSequence after event/order managers are ready
     */
    void initializeEventHandlers();
    
    // ==========================================================================
    // Component Access (God Object pattern)
    // ==========================================================================
    
    /**
     * @brief Get the logger instance
     * Usage: main->logger()->info("message")
     *        main->logger()->log_order(...)
     *        main->logger()->log_to_csv(...)
     */
    utils::Logger* logger() { return &utils::Logger::getInstance(); }
    const utils::Logger* logger() const { return &utils::Logger::getInstance(); }
    
    /**
     * @brief Get the configuration
     */
    config::Config* config() { return &config::Config::getInstance(); }
    const config::Config* config() const { return &config::Config::getInstance(); }
    
    /**
     * @brief Get the event manager
     */
    events::EventManager* events() { return &events::EventManager::getInstance(); }
    const events::EventManager* events() const { return &events::EventManager::getInstance(); }
    
    /**
     * @brief Get the order manager
     */
    orders::OrderManager* orders() { return &orders::OrderManager::getInstance(); }
    const orders::OrderManager* orders() const { return &orders::OrderManager::getInstance(); }
    
    /**
     * @brief Get the order book manager
     */
    orders::OrderBookManager* orderbooks() { return &orders::OrderBookManager::getInstance(); }
    const orders::OrderBookManager* orderbooks() const { return &orders::OrderBookManager::getInstance(); }
    
    /**
     * @brief Get the user manager
     */
    user::UserManager* user() { return &user::UserManager::getInstance(); }
    const user::UserManager* user() const { return &user::UserManager::getInstance(); }
    
    /**
     * @brief Get the REST API client
     */
    api::RestClient* rest() { return &api::RestClient::getInstance(); }
    const api::RestClient* rest() const { return &api::RestClient::getInstance(); }
    
    /**
     * @brief Get the WebSocket client
     */
    api::WebSocketClient* websocket() { return &api::WebSocketClient::getInstance(); }
    const api::WebSocketClient* websocket() const { return &api::WebSocketClient::getInstance(); }
    
    /**
     * @brief Get the market data manager
     */
    marketdata::MarketDataManager* marketdata() { return &marketdata::MarketDataManager::getInstance(); }
    const marketdata::MarketDataManager* marketdata() const { return &marketdata::MarketDataManager::getInstance(); }
    
    /**
     * @brief Get the external feed manager (theo pricing from external_feed config)
     */
    marketdata::ExternalFeedManager* externalFeed() { return &marketdata::ExternalFeedManager::getInstance(); }
    const marketdata::ExternalFeedManager* externalFeed() const { return &marketdata::ExternalFeedManager::getInstance(); }
    
    /**
     * @brief Get the portfolio manager
     */
    portfolio::PortfolioManager* portfolio() { return &portfolio::PortfolioManager::getInstance(); }
    const portfolio::PortfolioManager* portfolio() const { return &portfolio::PortfolioManager::getInstance(); }
    
    /**
     * @brief Get the strategy manager (register strategies, fire timers)
     */
    strategy::StrategyManager* strategyManager() { return &strategy::StrategyManager::getInstance(); }
    const strategy::StrategyManager* strategyManager() const { return &strategy::StrategyManager::getInstance(); }

    /**
     * @brief Set the hedge provider (called on every fill when hedge.enabled).
     * Default is NoOpHedgeProvider. Replace with a per-client implementation that sends
     * offsetting orders to the client's hedging exchange.
     */
    void setHedgeProvider(std::unique_ptr<hedging::IHedgeProvider> provider);
    
    // ==========================================================================
    // Simulation Info
    // ==========================================================================
    
    /**
     * @brief Get the simulation date (YYYYMMDD)
     */
    std::string getSimulationDate() const { return params_.simulation_date; }
    
    /**
     * @brief Get the log directory path
     */
    std::string getLogDirectory() const;
    
    /**
     * @brief Get simulation parameters
     */
    const SimulationParams& getParams() const { return params_; }
    
    // ==========================================================================
    // Feed Status (detected after initialization)
    // ==========================================================================
    
    /**
     * @brief Get the current feed status
     */
    [[nodiscard]] const FeedStatus& getFeedStatus() const { return feed_status_; }
    
    /**
     * @brief Check if feed is real-time (live or paper with live prices)
     */
    [[nodiscard]] bool isRealTimeFeed() const { 
        return feed_status_.is_live || feed_status_.is_paper; 
    }
    
    /**
     * @brief Check if feed is simulation (historical replay)
     */
    [[nodiscard]] bool isSimulationFeed() const { 
        return feed_status_.is_simulation; 
    }
    
    /**
     * @brief Get feed mode as string
     */
    [[nodiscard]] std::string getFeedModeString() const { 
        return feedModeToString(feed_status_.mode); 
    }
    
    /**
     * @brief Verify feed connection and determine mode
     * Called automatically during start() if verify_on_startup is true
     * @return true if verification successful
     */
    bool verifyFeed();
    
    // ==========================================================================
    // Utilities
    // ==========================================================================
    
    /**
     * @brief Dump current state for debugging
     */
    void dumpState();

    /**
     * Live/paper (non-simulation): if a required *connection* is down (Architect order WS, or external
     * pricing transport: FIX after Logon, or REST last HTTP poll), cancel open orders per
     * getMarketDataSubscriptionSymbols() (watchlist + MM legs); if that list is empty, cancel-all with no symbol.
     * Clears OrderManager and strategy order tracking. See trading.feed_guardian_* config.
     */
    void runFeedGuardianHeartbeat();

    /**
     * Place MANY already-created OrderManager orders CONCURRENTLY (one wire round-trip
     * for the whole batch instead of one per order). Each id must refer to an order
     * created via OrderManager::submitOrderNoDispatch (i.e. NOT yet published to the
     * synchronous ORDER_SUBMITTED handler). For each id this builds the same wire body
     * as the single-order path, fires them all in parallel via
     * RestClient::placeOrdersRawConcurrent, then dispatches onOrderAccepted /
     * onOrderRejected exactly like the single-order handler. Runs on the caller's
     * thread (the per-AX mover), preserving reserve-before-accept ordering.
     */
    void placeOrdersConcurrent(const std::vector<orders::OrderId>& order_ids);

    /**
     * @brief Cancel a batch of orders CONCURRENTLY (one curl-multi batch, ~1 RTT).
     *
     * Resolves each local order's exchange_order_id from the OrderManager, fires all
     * cancels together via RestClient::cancelOrdersGatewayConcurrent, then marks each
     * order cancelled locally on success/benign-404. Hard failures are left open so the
     * strategy's async cancel-timeout reconcile re-sends. Runs on the caller's thread.
     */
    void cancelOrdersConcurrent(const std::vector<orders::OrderId>& cancel_ids);

    /**
     * @brief Fire cancels AND places in ONE curl-multi batch (true c,c,p,p, ~1 RTT).
     *
     * `place_ids` were created via submit_order_no_dispatch (reserved, not yet wired);
     * `cancel_ids` are the resting legs to pull. Cancels and places are driven together
     * (RestClient::cancelThenPlaceOrdersConcurrent) so the whole cancel-replace cycle costs
     * ~1 round-trip. Place responses dispatch onOrderAccepted/onOrderRejected exactly like
     * placeOrdersConcurrent; cancel responses do the same local-cancel bookkeeping as
     * cancelOrdersConcurrent.
     *
     * SELF-TRADE HAZARD: combined batching loses cancel-before-place ordering at the venue —
     * the CALLER must gate this behind the self-trade guard (new bid >= old ask, or new ask
     * <= old bid) and use cancelOrdersConcurrent(await)+placeOrdersConcurrent instead when a
     * fresh leg could cross a still-live old leg.
     */
    void placeAndCancelConcurrent(const std::vector<orders::OrderId>& place_ids,
                                  const std::vector<orders::OrderId>& cancel_ids);

    /**
     * @brief Get platform uptime
     */
    std::chrono::seconds getUptime() const;
    
    /**
     * @brief Platform statistics
     */
    struct Stats {
        std::uint64_t events_processed      = 0;
        std::uint64_t orders_submitted      = 0;
        std::uint64_t orders_filled         = 0;
        std::uint64_t ticks_received        = 0;
        std::chrono::seconds uptime{0};
    };
    
    Stats getStats() const;
    
private:
    Platform() = default;
    ~Platform();
    
    // Initialization steps (each throws on failure)
    void initLogger();
    void initConfig();
    void initEvents();
    void initOrders();
    void initUser();
    void initApi();
    void initMarketData();
    void initPortfolio();
    void saveConfigToLogDir();
    void setupEventHandlers();
    
    // Order-gateway wire helpers for the concurrent batch path (placeOrdersConcurrent).
    // These MIRROR the single-order ORDER_SUBMITTED handler's inline body build + exchange-id
    // parse (src/core/Platform.cpp). KEEP THE TWO IN SYNC: if the venue place_order body
    // format or the exchange-id field names change, update both the lambda and these helpers.
    /**
     * Build the place_order wire JSON from an order's fields. Returns true on success
     * (out_json filled); false if the order must be rejected (out_reject_msg filled) —
     * e.g. non-LIMIT, non-positive price, or quantity below the venue minimum_order_size.
     */
    bool buildPlaceOrderJson(const std::string& symbol,
                             core::Side side,
                             core::Price price,
                             core::Quantity quantity,
                             std::string& out_json,
                             std::string& out_reject_msg) const;
    /** Parse the venue's assigned order id (oid/order_id/id) from a place response body. */
    static std::string parsePlaceOrderExchangeId(const std::string& body,
                                                 orders::OrderId fallback_order_id);

    /**
     * Shared batch tail helpers (used by placeOrdersConcurrent / cancelOrdersConcurrent /
     * placeAndCancelConcurrent). buildPlaceBodies filters `order_ids` to those that still
     * need a wire place (rejecting invalid ones inline) and returns the surviving ids +
     * bodies index-aligned. buildCancelBodies resolves exchange oids into {"oid":...} bodies.
     * dispatchPlaceResponses / dispatchCancelResponses apply the per-leg accept/cancel
     * bookkeeping given the index-aligned HttpResponses.
     */
    void buildPlaceBodies(const std::vector<orders::OrderId>& order_ids,
                          std::vector<orders::OrderId>& out_wire_ids,
                          std::vector<std::string>& out_bodies);
    void buildCancelBodies(const std::vector<orders::OrderId>& cancel_ids,
                           std::vector<orders::OrderId>& out_wire_ids,
                           std::vector<std::string>& out_bodies);
    void dispatchPlaceResponses(const std::vector<orders::OrderId>& wire_ids,
                                const std::vector<api::HttpResponse>& responses,
                                long long batch_rtt_us);
    void dispatchCancelResponses(const std::vector<orders::OrderId>& wire_ids,
                                 const std::vector<api::HttpResponse>& responses);

    // Feed verification
    void detectFeedMode();
    FeedMode parseFeedModeString(const std::string& mode_str) const;
    FeedSource parseFeedSourceString(const std::string& source_str) const;
    
    SimulationParams params_;
    ModuleStatus module_status_;
    FeedStatus feed_status_;
    
    std::atomic<bool> running_{false};
    std::atomic<bool> initialized_{false};
    bool session_clock_started_{false};
    std::chrono::steady_clock::time_point start_time_;
    
    // Event subscription handles
    events::SubscriptionHandle order_sub_handle_;
    events::SubscriptionHandle cancel_sub_handle_;  // Handle order cancellations
    events::SubscriptionHandle modify_sub_handle_;  // Handle order modifications
    events::SubscriptionHandle fill_sub_handle_;
    events::SubscriptionHandle partial_fill_sub_handle_;  // Handle partial fills separately
    events::SubscriptionHandle tick_sub_handle_;
    events::SubscriptionHandle order_accepted_handle_;

    std::unique_ptr<hedging::IHedgeProvider> hedge_provider_;
};

// =============================================================================
// Global Access Helper
// =============================================================================

/**
 * @brief Get the platform instance (shorthand)
 * Usage: auto& main = Main(); main.logger()->info("...");
 */
inline Platform& Main() {
    return Platform::getInstance();
}

} // namespace core
} // namespace architect
