#pragma once

/**
 * @file TimerSystem.h
 * @brief Timer system for triggering events at specific time_racks
 * 
 * The TimerSystem is responsible for:
 * - Managing registered time_racks from predictors and strategies
 * - Firing timer events at the precise nanosecond level
 * - Supporting both real-time and simulation modes
 */

#include "core/Types.h"
#include "events/Event.h"
#include "events/EventManager.h"

#include <set>
#include <map>
#include <mutex>
#include <shared_mutex>
#include <thread>
#include <atomic>
#include <chrono>
#include <functional>
#include <condition_variable>

namespace architect {
namespace strategy {

using namespace core;
using namespace events;

/**
 * @brief Timer system configuration
 */
struct TimerConfig {
    bool            enabled             = true;
    bool            realtime_mode       = true;     // false = simulation
    std::uint32_t   tick_interval_us    = 1000;     // Microsecond resolution
    bool            emit_second_events  = true;
    bool            emit_minute_events  = true;
    DateInt         simulation_date     = 0;
    TimeRack        simulation_start    = 0;        // Start time for simulation
    TimeRack        simulation_end      = 0;        // End time for simulation
    double          simulation_speed    = 1.0;      // 1.0 = realtime, 2.0 = 2x speed
};

/**
 * @brief Timer statistics
 */
struct TimerStats {
    std::uint64_t   timers_fired            = 0;
    std::uint64_t   tick_events             = 0;
    std::uint64_t   second_events           = 0;
    std::uint64_t   minute_events           = 0;
    std::uint64_t   late_fires              = 0;    // Fired late
    double          avg_latency_us          = 0.0;
    double          max_latency_us          = 0.0;
    TimeRack        last_fired_time_rack    = 0;
};

/**
 * @brief Callback type for timer triggers
 */
using TimerCallback = std::function<void(DateInt date, TimeRack time_rack)>;

/**
 * @brief Timer system for nanosecond-level event triggering
 */
class TimerSystem {
public:
    /**
     * @brief Get the singleton instance
     */
    static TimerSystem& getInstance();
    
    TimerSystem(const TimerSystem&) = delete;
    TimerSystem& operator=(const TimerSystem&) = delete;
    
    // ==========================================================================
    // Configuration
    // ==========================================================================
    
    /**
     * @brief Initialize with configuration
     */
    void initialize(const TimerConfig& config);
    
    /**
     * @brief Set the simulation date
     */
    void setDate(DateInt date);
    
    /**
     * @brief Set realtime vs simulation mode
     */
    void setRealtimeMode(bool realtime);
    
    /**
     * @brief Set simulation speed (only for simulation mode)
     */
    void setSimulationSpeed(double speed);
    
    // ==========================================================================
    // Timer Registration
    // ==========================================================================
    
    /**
     * @brief Register a time_rack to fire on
     * @param time_rack Time in HHMMSSMMM format
     * @param name Optional timer name for logging
     */
    void registerTimer(TimeRack time_rack, const std::string& name = "");
    
    /**
     * @brief Register multiple time_racks
     */
    void registerTimers(const std::set<TimeRack>& time_racks);
    
    /**
     * @brief Unregister a time_rack
     */
    void unregisterTimer(TimeRack time_rack);
    
    /**
     * @brief Clear all registered timers
     */
    void clearTimers();
    
    /**
     * @brief Get all registered time_racks
     */
    std::set<TimeRack> getRegisteredTimers() const;
    
    /**
     * @brief Register a callback for timer events
     */
    void setCallback(TimerCallback callback);
    
    // ==========================================================================
    // Lifecycle
    // ==========================================================================
    
    /**
     * @brief Start the timer system
     */
    void start();
    
    /**
     * @brief Stop the timer system
     */
    void stop();
    
    /**
     * @brief Check if running
     */
    [[nodiscard]] bool isRunning() const { return running_.load(); }
    
    // ==========================================================================
    // Manual Control (for simulation)
    // ==========================================================================
    
    /**
     * @brief Advance time to a specific time_rack (simulation mode)
     * Fires all timers between current time and target time
     */
    void advanceTo(TimeRack time_rack);
    
    /**
     * @brief Fire a specific timer manually
     */
    void fireTimer(TimeRack time_rack);
    
    /**
     * @brief Fire timer with custom date
     */
    void fireTimer(DateInt date, TimeRack time_rack);
    
    /**
     * @brief Get current time_rack
     */
    [[nodiscard]] TimeRack getCurrentTimeRack() const { return current_time_rack_.load(); }
    
    /**
     * @brief Get current date
     */
    [[nodiscard]] DateInt getCurrentDate() const { return current_date_; }
    
    // ==========================================================================
    // Statistics
    // ==========================================================================
    
    [[nodiscard]] TimerStats getStats() const;
    void resetStats();
    
private:
    TimerSystem() = default;
    ~TimerSystem();
    
    void timerLoop();
    TimeRack getCurrentSystemTimeRack() const;
    void emitTimerEvent(DateInt date, TimeRack time_rack, const std::string& name = "");
    void emitSecondEvent(DateInt date, TimeRack time_rack);
    void emitMinuteEvent(DateInt date, TimeRack time_rack);
    
    TimerConfig             config_;
    
    std::set<TimeRack>      registered_timers_;
    std::map<TimeRack, std::string> timer_names_;
    mutable std::shared_mutex timers_mutex_;
    
    DateInt                 current_date_{0};
    std::atomic<TimeRack>   current_time_rack_{0};
    
    std::atomic<bool>       running_{false};
    std::thread             timer_thread_;
    std::condition_variable_any cv_;
    
    TimerCallback           callback_;
    
    // Statistics
    mutable std::mutex      stats_mutex_;
    TimerStats              stats_;
    
    // For second/minute tracking
    TimeRack                last_second_{0};
    TimeRack                last_minute_{0};
};

// =============================================================================
// Inline Helper: Convert system time to TimeRack
// =============================================================================

inline TimeRack systemTimeToTimeRack() {
    auto now = std::chrono::system_clock::now();
    auto time_t_now = std::chrono::system_clock::to_time_t(now);
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        now.time_since_epoch()) % 1000;
    
    std::tm* local = std::localtime(&time_t_now);
    
    return makeTimeRack(local->tm_hour, local->tm_min, local->tm_sec, 
                        static_cast<int>(ms.count()));
}

inline DateInt systemDateToDateInt() {
    auto now = std::chrono::system_clock::now();
    auto time_t_now = std::chrono::system_clock::to_time_t(now);
    std::tm* local = std::localtime(&time_t_now);
    
    return makeDateInt(local->tm_year + 1900, local->tm_mon + 1, local->tm_mday);
}

} // namespace strategy
} // namespace architect
