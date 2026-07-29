#pragma once

/**
 * @file Strategy.h
 * @brief Base class for all trading strategies in the platform
 * 
 * Strategies are action-taking components that:
 * - Subscribe to timer events (time_rack based)
 * - Subscribe to order events (fills, rejects, etc.)
 * - Subscribe to predictor signals
 * - Take actions: submit orders, cancel orders, modify positions
 * 
 * To create a custom strategy:
 * 1. Inherit from BaseStrategy
 * 2. Override on_timer(), on_fill(), on_signal(), etc.
 * 3. Use submit_order(), cancel_order() to take actions
 * 
 * Example:
 *   class MyStrategy : public BaseStrategy {
 *   public:
 *       MyStrategy() : BaseStrategy("my_strategy") {
 *           add_timer(93000000);  // 9:30 AM
 *           subscribe_predictor("momentum_predictor");
 *       }
 *       
 *       void on_signal(const PredictorSignalData& signal) override {
 *           if (signal.signal_value > 0.5) {
 *               submit_order(OrderRequest::market_buy("BTC-USD", 1.0));
 *           }
 *       }
 *   };
 */

#include "core/Types.h"
#include "events/Event.h"
#include "events/EventManager.h"
#include "strategy/Predictor.h"

#include <string>
#include <vector>
#include <set>
#include <map>
#include <queue>
#include <functional>
#include <memory>
#include <atomic>
#include <mutex>

namespace architect {
namespace strategy {

using namespace core;
using namespace events;

// Forward declarations
class StrategyManager;

// =============================================================================
// Order Request Types
// =============================================================================

/**
 * @brief Order request to be submitted by a strategy
 */
struct OrderRequest {
    std::string     symbol;
    Side            side            = Side::BUY;
    OrderType       type            = OrderType::MARKET;
    TimeInForce     time_in_force   = TimeInForce::DAY;
    Price           price           = 0.0;          // For limit orders
    Price           stop_price      = 0.0;          // For stop orders
    Quantity        quantity        = 0.0;
    std::string     client_order_id;                // Optional client-assigned ID
    std::string     strategy_name;                  // Strategy that created this
    std::string     reason;                         // Why this order was submitted
    DateInt         date            = 0;
    TimeRack        time_rack       = 0;
    
    // Factory methods for common order types
    static OrderRequest market_buy(const std::string& symbol, Quantity qty, 
                                   const std::string& reason = "") {
        OrderRequest req;
        req.symbol = symbol;
        req.side = Side::BUY;
        req.type = OrderType::MARKET;
        req.quantity = qty;
        req.reason = reason;
        return req;
    }
    
    static OrderRequest market_sell(const std::string& symbol, Quantity qty,
                                    const std::string& reason = "") {
        OrderRequest req;
        req.symbol = symbol;
        req.side = Side::SELL;
        req.type = OrderType::MARKET;
        req.quantity = qty;
        req.reason = reason;
        return req;
    }
    
    static OrderRequest limit_buy(const std::string& symbol, Quantity qty, Price price,
                                  const std::string& reason = "") {
        OrderRequest req;
        req.symbol = symbol;
        req.side = Side::BUY;
        req.type = OrderType::LIMIT;
        req.price = price;
        req.quantity = qty;
        req.reason = reason;
        return req;
    }
    
    static OrderRequest limit_sell(const std::string& symbol, Quantity qty, Price price,
                                   const std::string& reason = "") {
        OrderRequest req;
        req.symbol = symbol;
        req.side = Side::SELL;
        req.type = OrderType::LIMIT;
        req.price = price;
        req.quantity = qty;
        req.reason = reason;
        return req;
    }
    
    static OrderRequest stop_buy(const std::string& symbol, Quantity qty, Price stop_price,
                                 const std::string& reason = "") {
        OrderRequest req;
        req.symbol = symbol;
        req.side = Side::BUY;
        req.type = OrderType::STOP;
        req.stop_price = stop_price;
        req.quantity = qty;
        req.reason = reason;
        return req;
    }
    
    static OrderRequest stop_sell(const std::string& symbol, Quantity qty, Price stop_price,
                                  const std::string& reason = "") {
        OrderRequest req;
        req.symbol = symbol;
        req.side = Side::SELL;
        req.type = OrderType::STOP;
        req.stop_price = stop_price;
        req.quantity = qty;
        req.reason = reason;
        return req;
    }
    
    static OrderRequest stop_limit_buy(const std::string& symbol, Quantity qty, 
                                       Price stop_price, Price limit_price,
                                       const std::string& reason = "") {
        OrderRequest req;
        req.symbol = symbol;
        req.side = Side::BUY;
        req.type = OrderType::STOP_LIMIT;
        req.stop_price = stop_price;
        req.price = limit_price;
        req.quantity = qty;
        req.reason = reason;
        return req;
    }
    
    static OrderRequest stop_limit_sell(const std::string& symbol, Quantity qty,
                                        Price stop_price, Price limit_price,
                                        const std::string& reason = "") {
        OrderRequest req;
        req.symbol = symbol;
        req.side = Side::SELL;
        req.type = OrderType::STOP_LIMIT;
        req.stop_price = stop_price;
        req.price = limit_price;
        req.quantity = qty;
        req.reason = reason;
        return req;
    }
};

/**
 * @brief Cancel request for an existing order
 */
struct CancelRequest {
    OrderId         order_id        = 0;
    std::string     client_order_id;
    std::string     symbol;
    std::string     strategy_name;
    std::string     reason;
    DateInt         date            = 0;
    TimeRack        time_rack       = 0;
};

/**
 * @brief Modify request for an existing order
 */
struct ModifyRequest {
    OrderId         order_id        = 0;
    std::string     client_order_id;
    std::string     symbol;
    Price           new_price       = 0.0;      // 0 = no change
    Quantity        new_quantity    = 0.0;      // 0 = no change
    std::string     strategy_name;
    std::string     reason;
    DateInt         date            = 0;
    TimeRack        time_rack       = 0;
};

// =============================================================================
// Strategy Configuration & Stats
// =============================================================================

/**
 * @brief Configuration for a strategy
 */
struct StrategyConfig {
    std::string                 name;
    bool                        enabled             = true;
    std::vector<TimeRack>       timer_racks;
    std::vector<std::string>    predictor_subscriptions;
    std::vector<std::string>    symbols;
    double                      max_position_size   = 0.0;  // 0 = unlimited
    double                      max_order_value     = 0.0;  // 0 = unlimited
    int                         max_orders_per_day  = 0;    // 0 = unlimited
    std::map<std::string, double> parameters;
};

/**
 * @brief Statistics for a strategy
 */
struct StrategyStats {
    std::uint64_t   timer_callbacks         = 0;
    std::uint64_t   signal_callbacks        = 0;
    std::uint64_t   fill_callbacks          = 0;
    std::uint64_t   orders_submitted        = 0;
    std::uint64_t   orders_filled           = 0;
    std::uint64_t   orders_cancelled        = 0;
    std::uint64_t   orders_rejected         = 0;
    std::uint64_t   errors                  = 0;
    double          total_pnl               = 0.0;
    double          realized_pnl            = 0.0;
    double          unrealized_pnl          = 0.0;
    Timestamp       last_action_time;
};

// =============================================================================
// Base Strategy Class
// =============================================================================

/**
 * @brief Base class for all trading strategies
 */
class BaseStrategy {
public:
    explicit BaseStrategy(const std::string& name);
    virtual ~BaseStrategy();
    
    // Prevent copying
    BaseStrategy(const BaseStrategy&) = delete;
    BaseStrategy& operator=(const BaseStrategy&) = delete;
    
    // ==========================================================================
    // Lifecycle
    // ==========================================================================
    
    virtual void initialize(DateInt date);
    virtual void start();
    virtual void stop();
    virtual void shutdown();
    virtual void on_eod(DateInt date);
    
    // ==========================================================================
    // Event Callbacks (Override in derived classes)
    // ==========================================================================
    
    /**
     * @brief Called when a subscribed timer fires
     */
    virtual void on_timer(DateInt /*date*/, TimeRack /*time_rack*/) {}
    
    /**
     * @brief Called when a predictor emits a signal
     */
    virtual void on_signal(const PredictorSignalData& /*signal*/) {}
    
    /**
     * @brief Called when an order is filled
     */
    virtual void on_fill(const OrderEventData& /*fill*/) {}
    
    /**
     * @brief Called when an order is partially filled
     */
    virtual void on_partial_fill(const OrderEventData& /*fill*/) {}
    
    /**
     * @brief Called when an order is rejected
     */
    virtual void on_reject(const OrderEventData& /*reject*/) {}
    
    /**
     * @brief Called when an order is cancelled
     */
    virtual void on_cancel(const OrderEventData& /*cancel*/) {}
    
    /**
     * @brief Called when an order is accepted
     */
    virtual void on_accept(const OrderEventData& /*accept*/) {}
    
    /**
     * @brief Called on L1/tick update
     */
    virtual void on_tick(const TickEventData& /*tick*/, DateInt /*date*/, TimeRack /*time_rack*/) {}
    
    /**
     * @brief Called on position update
     */
    virtual void on_position_update(const PositionEventData& /*position*/) {}
    
    // ==========================================================================
    // Configuration
    // ==========================================================================
    
    void add_timer(TimeRack time_rack);
    void add_timers(const std::vector<TimeRack>& time_racks);
    void remove_timer(TimeRack time_rack);
    void clear_timers();
    
    void subscribe_predictor(const std::string& predictor_name);
    void unsubscribe_predictor(const std::string& predictor_name);
    
    void subscribe_symbol(const std::string& symbol);
    void unsubscribe_symbol(const std::string& symbol);
    
    void set_parameter(const std::string& key, double value);
    double get_parameter(const std::string& key, double default_value = 0.0) const;
    
    // ==========================================================================
    // Order Submission
    // ==========================================================================
    
    /**
     * @brief Submit an order request
     * @return Client order ID (can be used for tracking)
     */
    std::string submit_order(const OrderRequest& request);

    /**
     * @brief Create + register an order like submit_order but DEFER the wire (no HTTP).
     *
     * Uses OrderManager::submitOrderNoDispatch so the synchronous ORDER_SUBMITTED handler
     * does NOT fire. Performs the same local strategy bookkeeping (open_orders_, counters)
     * and returns the client order id; the numeric OrderManager id is returned via
     * out_order_id so the caller can fire the wire in a batch (Platform::placeOrdersConcurrent)
     * and let the resulting ORDER_ACCEPTED balance the reservation. Returns "" on failure.
     */
    std::string submit_order_no_dispatch(const OrderRequest& request, OrderId& out_order_id);
    
    /**
     * @brief Submit a cancel request
     */
    bool cancel_order(const CancelRequest& request);
    
    /**
     * @brief Cancel an order by ID
     */
    bool cancel_order(OrderId order_id, const std::string& reason = "");
    
    /**
     * @brief Cancel all orders for a symbol
     */
    void cancel_all_orders(const std::string& symbol, const std::string& reason = "");
    
    /**
     * @brief Modify an existing order
     */
    bool modify_order(const ModifyRequest& request);
    
    /**
     * @brief Close position for a symbol
     */
    void close_position(const std::string& symbol, const std::string& reason = "");
    
    /**
     * @brief Flatten all positions
     */
    void flatten_all(const std::string& reason = "EOD flatten");
    
    // ==========================================================================
    // Position & Order Queries
    // ==========================================================================
    
    /**
     * @brief Get current position for a symbol
     */
    Quantity get_position(const std::string& symbol) const;
    
    /**
     * @brief Get all positions
     */
    std::map<std::string, Quantity> get_all_positions() const;
    
    /**
     * @brief Get open orders for a symbol
     */
    std::vector<OrderId> get_open_orders(const std::string& symbol) const;
    
    /**
     * @brief Get all open orders
     */
    std::vector<OrderId> get_all_open_orders() const;
    
    /**
     * @brief Check if we have an open position
     */
    bool has_position(const std::string& symbol) const;
    
    /**
     * @brief Get number of orders submitted today
     */
    int get_orders_today() const;
    
    // ==========================================================================
    // Accessors
    // ==========================================================================
    
    [[nodiscard]] const std::string& getName() const { return name_; }
    [[nodiscard]] bool isEnabled() const { return enabled_.load(); }
    [[nodiscard]] bool isRunning() const { return running_.load(); }
    [[nodiscard]] const std::set<TimeRack>& getTimers() const { return timer_racks_; }
    [[nodiscard]] const std::set<std::string>& getSubscribedPredictors() const { return predictor_subscriptions_; }
    [[nodiscard]] StrategyStats getStats() const {
        StrategyStats s;
        s.orders_submitted = orders_submitted_.load(std::memory_order_relaxed);
        s.orders_filled = orders_filled_.load(std::memory_order_relaxed);
        s.orders_rejected = orders_rejected_.load(std::memory_order_relaxed);
        s.orders_cancelled = orders_cancelled_.load(std::memory_order_relaxed);
        s.errors = errors_count_.load(std::memory_order_relaxed);
        // P&L stats from cold path (needs lock)
        {
            std::lock_guard<std::mutex> lock(stats_mutex_);
            s.realized_pnl = stats_.realized_pnl;
            s.unrealized_pnl = stats_.unrealized_pnl;
            s.total_pnl = stats_.total_pnl;
        }
        return s;
    }
    [[nodiscard]] DateInt getCurrentDate() const { return current_date_; }
    [[nodiscard]] TimeRack getCurrentTimeRack() const { return current_time_rack_; }
    
    void setEnabled(bool enabled) { enabled_ = enabled; }
    
protected:
    // ==========================================================================
    // Helper Methods for Derived Classes
    // ==========================================================================
    
    DateInt getDate() const { return current_date_; }
    TimeRack getTimeRack() const { return current_time_rack_; }
    
    void log_info(const std::string& msg);
    void log_warn(const std::string& msg);
    void log_error(const std::string& msg);
    void log_debug(const std::string& msg);
    
    /**
     * @brief Log a strategy action for audit trail
     */
    void log_action(StrategyActionType action, const std::string& symbol,
                    Side side, Price price, Quantity qty, const std::string& reason);
    
    /**
     * @brief Generate a unique client order ID
     */
    std::string generate_client_order_id();

    /** Clear strategy-local open-order id sets (after exchange cancel-all + OM reset). */
    void clearOpenOrderTracking();

    /** Register a working order id adopted from the exchange (e.g. MM desk) without ORDER_SUBMITTED. */
    void noteAdoptedWorkingOrder(OrderId order_id, const std::string& symbol);

    /**
     * True when `client_order_id` belongs to this strategy (delimiter-bounded name prefix).
     * Prevents sibling MM stacks whose names share a prefix (e.g. `eca8` vs `eca823da`) from
     * stealing each other's ORDER_ACCEPTED events.
     */
    [[nodiscard]] bool clientOrderIdBelongsToThisStrategy(const std::string& client_order_id) const;

    /**
     * Route ORDER_* events to this strategy. Default: order_id is in open_orders_.
     * MakeMarketStrategy extends this so late fills still match bid_order_id_/ask_order_id_ after cancel races.
     */
    virtual bool orderEventMatchesStrategy(const events::OrderEventData& data, events::EventType type) const;

private:
    friend class StrategyManager;
    
    // Internal event handlers
    void handleTimerEvent(const EventPtr& event);
    void handleSignalEvent(const EventPtr& event);
    void handleOrderEvent(const EventPtr& event);
    void handleTickEvent(const EventPtr& event);
    void handlePositionEvent(const EventPtr& event);
    
    void setupSubscriptions();
    void teardownSubscriptions();
    void updatePosition(const std::string& symbol, Quantity delta);
    
    std::string             name_;
    std::atomic<bool>       enabled_{true};
    std::atomic<bool>       running_{false};
    std::atomic<bool>       initialized_{false};
    
    DateInt                 current_date_{0};
    TimeRack                current_time_rack_{0};
    
    std::set<TimeRack>      timer_racks_;
    std::set<std::string>   predictor_subscriptions_;
    std::set<std::string>   symbol_subscriptions_;
    std::map<std::string, double> parameters_;
    
    // Position tracking
    mutable std::mutex      position_mutex_;
    std::map<std::string, Quantity> positions_;
    
    // Open orders tracking
    mutable std::mutex      orders_mutex_;
    std::set<OrderId>       open_orders_;
    std::map<std::string, std::set<OrderId>> orders_by_symbol_;
    
    // Event subscriptions
    std::vector<SubscriptionHandle> subscription_handles_;
    
    // Order ID generation
    std::atomic<std::uint64_t> next_order_id_{1};
    int orders_today_{0};
    
    // Hot path statistics (lock-free atomics)
    std::atomic<std::uint64_t> orders_submitted_{0};
    std::atomic<std::uint64_t> orders_filled_{0};
    std::atomic<std::uint64_t> orders_rejected_{0};
    std::atomic<std::uint64_t> orders_cancelled_{0};
    std::atomic<std::uint64_t> errors_count_{0};
    
    // Cold stats (for P&L, rarely accessed)
    mutable std::mutex      stats_mutex_;
    StrategyStats           stats_;
};

/**
 * @brief Smart pointer for strategies
 */
using StrategyPtr = std::shared_ptr<BaseStrategy>;

// =============================================================================
// Strategy Manager
// =============================================================================

/**
 * @brief Manager for all strategies in the system
 */
class StrategyManager {
public:
    static StrategyManager& getInstance();
    
    StrategyManager(const StrategyManager&) = delete;
    StrategyManager& operator=(const StrategyManager&) = delete;
    
    // ==========================================================================
    // Strategy Management
    // ==========================================================================
    
    void registerStrategy(StrategyPtr strategy);
    void unregisterStrategy(const std::string& name);
    StrategyPtr getStrategy(const std::string& name);
    std::vector<StrategyPtr> getAllStrategies();
    
    // ==========================================================================
    // Lifecycle
    // ==========================================================================
    
    void initializeAll(DateInt date);
    void startAll();
    void stopAll();
    void shutdownAll();
    void eodAll(DateInt date);
    
    // ==========================================================================
    // Timer Management
    // ==========================================================================
    
    void fireTimer(DateInt date, TimeRack time_rack);
    std::set<TimeRack> getAllTimerRacks() const;
    
    // ==========================================================================
    // Statistics
    // ==========================================================================
    
    struct AggregateStats {
        std::uint64_t total_orders_submitted    = 0;
        std::uint64_t total_orders_filled       = 0;
        std::uint64_t total_orders_rejected     = 0;
        double total_pnl                        = 0.0;
        std::uint64_t active_strategies         = 0;
    };
    
    AggregateStats getAggregateStats() const;

    /** Clear open-order id tracking on every registered strategy (after account-wide cancel-all). */
    void clearOpenOrderTrackingAll();

private:
    StrategyManager() = default;
    ~StrategyManager() = default;
    
    mutable std::shared_mutex strategies_mutex_;
    std::map<std::string, StrategyPtr> strategies_;
};

} // namespace strategy
} // namespace architect
