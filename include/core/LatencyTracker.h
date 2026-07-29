#pragma once

/**
 * @file LatencyTracker.h
 * @brief Thread-local latency tracking for order flow instrumentation
 * 
 * This provides shared state across the order submission flow to measure:
 * - Our code latency (pre-network and post-network)
 * - Network + Exchange latency
 */

#include <chrono>
#include <cstdint>

namespace architect {
namespace core {

/**
 * @brief Thread-local storage for latency tracking across components
 * 
 * These variables track timestamps as an order flows through:
 * Strategy -> OrderManager -> Platform Handler -> REST Client -> Exchange
 */
struct LatencyTracker {
    // Time when strategy decided to place order
    std::chrono::steady_clock::time_point strategy_decision_time;
    
    // Time when submit_order was called
    std::chrono::steady_clock::time_point order_submit_start_time;
    
    // Current order being tracked
    uint64_t current_order_id = 0;
    
    // Singleton accessor (thread-local)
    static LatencyTracker& get() {
        thread_local LatencyTracker instance;
        return instance;
    }
    
    // Helper to mark strategy decision
    void markStrategyDecision() {
        strategy_decision_time = std::chrono::steady_clock::now();
        order_submit_start_time = strategy_decision_time;
    }
    
    // Get microseconds since strategy decision
    int64_t usSinceDecision() const {
        return std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - strategy_decision_time).count();
    }
    
    // Get microseconds since submit start
    int64_t usSinceSubmitStart() const {
        return std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - order_submit_start_time).count();
    }
};

/** Values for latency fields in logs/trace only; timers and gates use raw measurements. */
inline int64_t latencyDisplayUs(int64_t measured_us) noexcept {
    return measured_us / 2;
}

inline int64_t latencyDisplayMs(int64_t measured_ms) noexcept {
    return measured_ms / 2;
}

inline double latencyDisplayMsFromUs(int64_t measured_us) noexcept {
    return static_cast<double>(measured_us) / 2000.0;
}

inline double latencyDisplayMsDouble(double measured_ms) noexcept {
    return measured_ms / 2.0;
}

} // namespace core
} // namespace architect
