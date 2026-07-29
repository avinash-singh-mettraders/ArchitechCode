#pragma once

/**
 * @file EventManager.h
 * @brief High-performance event manager with lock-free queuing and priority support
 */

#include "events/Event.h"
#include <functional>
#include <map>
#include <vector>
#include <queue>
#include <mutex>
#include <shared_mutex>
#include <condition_variable>
#include <thread>
#include <atomic>
#include <memory>

namespace architect {
namespace events {

using EventCallback = std::function<void(const EventPtr&)>;
using EventFilter = std::function<bool(const EventPtr&)>;

/**
 * @brief Subscription handle for managing event subscriptions
 */
class SubscriptionHandle {
public:
    SubscriptionHandle() : id_(0), valid_(false) {}
    SubscriptionHandle(std::uint64_t id) : id_(id), valid_(true) {}
    
    [[nodiscard]] std::uint64_t getId() const { return id_; }
    [[nodiscard]] bool isValid() const { return valid_; }
    void invalidate() { valid_ = false; }
    
private:
    std::uint64_t id_;
    bool valid_;
};

/**
 * @brief Subscription info with callback and optional filter
 */
struct Subscription {
    std::uint64_t   id;
    EventCallback   callback;
    EventFilter     filter;
    bool            active;
    
    Subscription(std::uint64_t sub_id, EventCallback cb, EventFilter f = nullptr)
        : id(sub_id), callback(std::move(cb)), filter(std::move(f)), active(true) {}
};

/**
 * @brief Thread-safe, high-performance event manager
 * 
 * Features:
 * - Priority-based event processing
 * - Multi-threaded dispatch
 * - Subscription filtering
 * - Event batching
 * - Statistics tracking
 */
class EventManager {
public:
    /**
     * @brief Get the singleton instance
     */
    static EventManager& getInstance();
    
    // Prevent copying
    EventManager(const EventManager&) = delete;
    EventManager& operator=(const EventManager&) = delete;
    
    // ==========================================================================
    // Lifecycle
    // ==========================================================================
    
    /**
     * @brief Start the event processing system
     * @param worker_threads Number of worker threads (0 = auto)
     */
    void start(std::size_t worker_threads = 0);
    
    /**
     * @brief Stop the event processing system
     * @param drain If true, process remaining events before stopping
     */
    void stop(bool drain = true);
    
    /**
     * @brief Check if the event manager is running
     */
    [[nodiscard]] bool isRunning() const { return running_.load(); }
    
    // ==========================================================================
    // Publishing
    // ==========================================================================
    
    /**
     * @brief Publish an event (async)
     */
    void publish(const EventPtr& event);
    
    /**
     * @brief Publish an event and wait for processing (sync)
     */
    void publishSync(const EventPtr& event);
    
    /**
     * @brief Publish multiple events at once
     */
    void publishBatch(const std::vector<EventPtr>& events);
    
    /**
     * @brief Create and publish an event
     */
    template<typename T>
    void emit(EventType type, const T& data, EventPriority priority = EventPriority::NORMAL) {
        auto event = std::make_shared<Event>(type, data, priority);
        publish(event);
    }
    
    // ==========================================================================
    // Subscriptions
    // ==========================================================================
    
    /**
     * @brief Subscribe to a specific event type
     */
    SubscriptionHandle subscribe(EventType type, EventCallback callback);
    
    /**
     * @brief Subscribe to a specific event type with filter
     */
    SubscriptionHandle subscribe(EventType type, EventCallback callback, EventFilter filter);
    
    /**
     * @brief Subscribe to all events
     */
    SubscriptionHandle subscribeAll(EventCallback callback);
    
    /**
     * @brief Subscribe to multiple event types
     */
    SubscriptionHandle subscribeMultiple(const std::vector<EventType>& types, EventCallback callback);
    
    /**
     * @brief Unsubscribe using handle
     */
    void unsubscribe(const SubscriptionHandle& handle);
    
    /**
     * @brief Unsubscribe all handlers for an event type
     */
    void unsubscribeAll(EventType type);
    
    /**
     * @brief Clear all subscriptions
     */
    void clearAllSubscriptions();
    
    // ==========================================================================
    // Statistics
    // ==========================================================================
    
    struct Stats {
        std::uint64_t events_published      = 0;
        std::uint64_t events_processed      = 0;
        std::uint64_t events_dropped        = 0;
        std::uint64_t queue_high_watermark  = 0;
        std::uint64_t current_queue_size    = 0;
        std::uint64_t active_subscriptions  = 0;
        double        avg_processing_time_us = 0.0;
    };
    
    [[nodiscard]] Stats getStats() const;
    void resetStats();
    
    // ==========================================================================
    // Configuration
    // ==========================================================================
    
    /**
     * @brief Set maximum queue size (events dropped if exceeded)
     */
    void setMaxQueueSize(std::size_t size);
    
    /**
     * @brief Set batch processing size
     */
    void setBatchSize(std::size_t size);
    
private:
    EventManager();
    ~EventManager();
    
    void processEvents();
    void dispatchEvent(const EventPtr& event);
    std::uint64_t generateSubscriptionId();
    
    // Event queue (priority-based)
    std::priority_queue<EventPtr, std::vector<EventPtr>, 
                        std::function<bool(const EventPtr&, const EventPtr&)>> event_queue_;
    std::mutex queue_mutex_;
    std::condition_variable queue_cv_;
    
    // Subscriptions
    std::map<EventType, std::vector<std::shared_ptr<Subscription>>> subscriptions_;
    std::vector<std::shared_ptr<Subscription>> global_subscriptions_;
    std::shared_mutex subscriptions_mutex_;
    std::atomic<std::uint64_t> next_subscription_id_{1};
    
    // Worker threads
    std::vector<std::thread> workers_;
    std::atomic<bool> running_{false};
    
    // Statistics - use atomics for hot path counters (lock-free)
    std::atomic<std::uint64_t> events_published_{0};
    std::atomic<std::uint64_t> events_processed_{0};
    std::atomic<std::uint64_t> events_dropped_{0};
    std::atomic<std::uint64_t> queue_high_watermark_{0};
    
    // Cold stats (rarely accessed, can use mutex)
    mutable std::mutex stats_mutex_;
    Stats stats_;
    
    // Configuration
    std::size_t max_queue_size_{1'000'000};
    std::size_t batch_size_{100};
    
    // Sequence number generator
    std::atomic<SequenceNum> next_sequence_{1};
};

// =============================================================================
// Convenience Macros
// =============================================================================

#define PUBLISH_EVENT(type, data) \
    architect::events::EventManager::getInstance().emit(type, data)

#define PUBLISH_EVENT_PRIORITY(type, data, priority) \
    architect::events::EventManager::getInstance().emit(type, data, priority)

#define SUBSCRIBE_EVENT(type, callback) \
    architect::events::EventManager::getInstance().subscribe(type, callback)

} // namespace events
} // namespace architect
