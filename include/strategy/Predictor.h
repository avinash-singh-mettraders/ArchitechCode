#pragma once

/**
 * @file Predictor.h
 * @brief Base class for all predictors in the trading platform
 * 
 * Predictors are computational components that:
 * - Subscribe to timer events (time_rack based)
 * - Subscribe to market data events
 * - Compute signals/features at specified times
 * - Emit predictor signals for strategies to consume
 * 
 * To create a custom predictor:
 * 1. Inherit from BasePredictor
 * 2. Override on_timer() and/or on_tick()
 * 3. Call emit_signal() to publish computed values
 * 
 * Example:
 *   class MyPredictor : public BasePredictor {
 *   public:
 *       MyPredictor() : BasePredictor("my_predictor") {
 *           add_timer(93000000);  // 9:30 AM
 *           subscribe_symbol("BTC-USD");
 *       }
 *       
 *       void on_timer(DateInt date, TimeRack time_rack) override {
 *           double signal = compute_signal();
 *           emit_signal("momentum", signal, 0.85);
 *       }
 *   };
 */

#include "core/Types.h"
#include "events/Event.h"
#include "events/EventManager.h"

#include <string>
#include <vector>
#include <set>
#include <map>
#include <functional>
#include <memory>
#include <atomic>
#include <mutex>

namespace architect {
namespace strategy {

using namespace core;
using namespace events;

// Forward declaration
class PredictorManager;

/**
 * @brief Configuration for a predictor
 */
struct PredictorConfig {
    std::string             name;
    bool                    enabled             = true;
    std::vector<TimeRack>   timer_racks;        // Times to trigger on_timer
    std::vector<std::string> symbols;           // Symbols to subscribe
    bool                    subscribe_l1        = true;
    bool                    subscribe_trades    = false;
    int                     warmup_bars         = 0;    // Bars needed before signals
    std::map<std::string, double> parameters;           // Custom parameters
};

/**
 * @brief Statistics for a predictor
 */
struct PredictorStats {
    std::uint64_t   timer_callbacks         = 0;
    std::uint64_t   tick_callbacks          = 0;
    std::uint64_t   signals_emitted         = 0;
    std::uint64_t   errors                  = 0;
    double          avg_compute_time_ns     = 0.0;
    double          max_compute_time_ns     = 0.0;
    Timestamp       last_compute_time;
};

/**
 * @brief Base class for all predictors
 */
class BasePredictor {
public:
    explicit BasePredictor(const std::string& name);
    virtual ~BasePredictor();
    
    // Prevent copying
    BasePredictor(const BasePredictor&) = delete;
    BasePredictor& operator=(const BasePredictor&) = delete;
    
    // ==========================================================================
    // Lifecycle
    // ==========================================================================
    
    /**
     * @brief Initialize the predictor
     * Called once during setup, before any events are dispatched
     */
    virtual void initialize(DateInt date);
    
    /**
     * @brief Start the predictor
     */
    virtual void start();
    
    /**
     * @brief Stop the predictor
     */
    virtual void stop();
    
    /**
     * @brief Shutdown and cleanup
     */
    virtual void shutdown();
    
    /**
     * @brief Called at end of day
     */
    virtual void on_eod(DateInt date);
    
    // ==========================================================================
    // Event Callbacks (Override in derived classes)
    // ==========================================================================
    
    /**
     * @brief Called when a subscribed timer fires
     * @param date Current date (YYYYMMDD)
     * @param time_rack Time the timer fired (HHMMSSMMM)
     */
    virtual void on_timer(DateInt /*date*/, TimeRack /*time_rack*/) {}
    
    /**
     * @brief Called on L1/tick update for subscribed symbols
     * @param tick The tick data
     * @param date Current date
     * @param time_rack Current time
     */
    virtual void on_tick(const TickEventData& /*tick*/, DateInt /*date*/, TimeRack /*time_rack*/) {}
    
    /**
     * @brief Called on trade update for subscribed symbols
     */
    virtual void on_trade(const TradeEventData& /*trade*/, DateInt /*date*/, TimeRack /*time_rack*/) {}
    
    /**
     * @brief Called on L2 book update for subscribed symbols
     */
    virtual void on_book_update(const Symbol& /*symbol*/, DateInt /*date*/, TimeRack /*time_rack*/) {}
    
    // ==========================================================================
    // Configuration
    // ==========================================================================
    
    /**
     * @brief Add a time_rack to trigger on_timer
     * @param time_rack Time in HHMMSSMMM format (e.g., 93000000 for 9:30 AM)
     */
    void add_timer(TimeRack time_rack);
    
    /**
     * @brief Add multiple timers
     */
    void add_timers(const std::vector<TimeRack>& time_racks);
    
    /**
     * @brief Remove a timer
     */
    void remove_timer(TimeRack time_rack);
    
    /**
     * @brief Clear all timers
     */
    void clear_timers();
    
    /**
     * @brief Subscribe to a symbol's market data
     */
    void subscribe_symbol(const std::string& symbol);
    
    /**
     * @brief Unsubscribe from a symbol
     */
    void unsubscribe_symbol(const std::string& symbol);
    
    /**
     * @brief Set a parameter
     */
    void set_parameter(const std::string& key, double value);
    
    /**
     * @brief Get a parameter
     */
    double get_parameter(const std::string& key, double default_value = 0.0) const;
    
    // ==========================================================================
    // Signal Emission
    // ==========================================================================
    
    /**
     * @brief Emit a signal for strategies to consume
     * @param signal_name Name/identifier of the signal
     * @param value Signal value
     * @param confidence Confidence level [0.0, 1.0]
     * @param symbol Related symbol (optional)
     */
    void emit_signal(const std::string& signal_name, double value, 
                     double confidence = 1.0, const std::string& symbol = "");
    
    /**
     * @brief Emit a signal with metadata
     */
    void emit_signal_with_metadata(const std::string& signal_name, double value,
                                   double confidence, const std::string& symbol,
                                   const std::string& metadata_json);
    
    // ==========================================================================
    // Accessors
    // ==========================================================================
    
    [[nodiscard]] const std::string& getName() const { return name_; }
    [[nodiscard]] bool isEnabled() const { return enabled_.load(); }
    [[nodiscard]] bool isRunning() const { return running_.load(); }
    [[nodiscard]] const std::set<TimeRack>& getTimers() const { return timer_racks_; }
    [[nodiscard]] const std::set<std::string>& getSubscribedSymbols() const { return subscribed_symbols_; }
    [[nodiscard]] PredictorStats getStats() const {
        PredictorStats s;
        s.timer_callbacks = timer_callbacks_.load(std::memory_order_relaxed);
        s.tick_callbacks = tick_callbacks_.load(std::memory_order_relaxed);
        s.signals_emitted = signals_emitted_.load(std::memory_order_relaxed);
        s.errors = errors_.load(std::memory_order_relaxed);
        return s;
    }
    [[nodiscard]] DateInt getCurrentDate() const { return current_date_; }
    [[nodiscard]] TimeRack getCurrentTimeRack() const { return current_time_rack_; }
    
    void setEnabled(bool enabled) { enabled_ = enabled; }
    
protected:
    // ==========================================================================
    // Helper Methods for Derived Classes
    // ==========================================================================
    
    /**
     * @brief Get current simulation/live date
     */
    DateInt getDate() const { return current_date_; }
    
    /**
     * @brief Get current time_rack
     */
    TimeRack getTimeRack() const { return current_time_rack_; }
    
    /**
     * @brief Log a message (via platform logger)
     */
    void log_info(const std::string& msg);
    void log_warn(const std::string& msg);
    void log_error(const std::string& msg);
    void log_debug(const std::string& msg);
    
    /**
     * @brief Record compute time for statistics
     */
    void record_compute_time(std::uint64_t nanos);
    
private:
    friend class PredictorManager;
    
    // Internal event handlers
    void handleTimerEvent(const EventPtr& event);
    void handleTickEvent(const EventPtr& event);
    void handleTradeEvent(const EventPtr& event);
    void handleL2Event(const EventPtr& event);
    
    void setupSubscriptions();
    void teardownSubscriptions();
    
    std::string             name_;
    std::atomic<bool>       enabled_{true};
    std::atomic<bool>       running_{false};
    std::atomic<bool>       initialized_{false};
    
    DateInt                 current_date_{0};
    TimeRack                current_time_rack_{0};
    
    std::set<TimeRack>      timer_racks_;
    std::set<std::string>   subscribed_symbols_;
    std::map<std::string, double> parameters_;
    
    // Event subscriptions
    std::vector<SubscriptionHandle> subscription_handles_;
    
    // Hot path statistics (lock-free atomics)
    std::atomic<std::uint64_t> timer_callbacks_{0};
    std::atomic<std::uint64_t> tick_callbacks_{0};
    std::atomic<std::uint64_t> signals_emitted_{0};
    std::atomic<std::uint64_t> errors_{0};
    
    // Cold stats (rarely accessed)
    mutable std::mutex      stats_mutex_;
    PredictorStats          stats_;
};

/**
 * @brief Smart pointer for predictors
 */
using PredictorPtr = std::shared_ptr<BasePredictor>;

/**
 * @brief Manager for all predictors in the system
 */
class PredictorManager {
public:
    static PredictorManager& getInstance();
    
    PredictorManager(const PredictorManager&) = delete;
    PredictorManager& operator=(const PredictorManager&) = delete;
    
    // ==========================================================================
    // Predictor Management
    // ==========================================================================
    
    /**
     * @brief Register a predictor
     */
    void registerPredictor(PredictorPtr predictor);
    
    /**
     * @brief Unregister a predictor
     */
    void unregisterPredictor(const std::string& name);
    
    /**
     * @brief Get a predictor by name
     */
    PredictorPtr getPredictor(const std::string& name);
    
    /**
     * @brief Get all registered predictors
     */
    std::vector<PredictorPtr> getAllPredictors();
    
    // ==========================================================================
    // Lifecycle
    // ==========================================================================
    
    /**
     * @brief Initialize all predictors for a given date
     */
    void initializeAll(DateInt date);
    
    /**
     * @brief Start all predictors
     */
    void startAll();
    
    /**
     * @brief Stop all predictors
     */
    void stopAll();
    
    /**
     * @brief Shutdown all predictors
     */
    void shutdownAll();
    
    /**
     * @brief Call end-of-day on all predictors
     */
    void eodAll(DateInt date);
    
    // ==========================================================================
    // Timer Management
    // ==========================================================================
    
    /**
     * @brief Fire timer for a specific time_rack
     * Called by the timer system when a time_rack is reached
     */
    void fireTimer(DateInt date, TimeRack time_rack);
    
    /**
     * @brief Get all registered timer racks
     */
    std::set<TimeRack> getAllTimerRacks() const;
    
private:
    PredictorManager() = default;
    ~PredictorManager() = default;
    
    mutable std::shared_mutex predictors_mutex_;
    std::map<std::string, PredictorPtr> predictors_;
};

} // namespace strategy
} // namespace architect
