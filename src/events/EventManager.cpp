#include "events/EventManager.h"
#include "config/Config.h"
#include <algorithm>
#include <chrono>

namespace architect {
namespace events {

EventManager& EventManager::getInstance() {
    static EventManager instance;
    return instance;
}

EventManager::EventManager()
    : event_queue_([](const EventPtr& a, const EventPtr& b) {
        // Higher priority first, then earlier timestamp
        if (a->priority != b->priority) {
            return static_cast<int>(a->priority) < static_cast<int>(b->priority);
        }
        return a->timestamp > b->timestamp;
    })
{
    max_queue_size_ = config::Config::getInstance().getEventQueueSize();
}

EventManager::~EventManager() {
    stop(false);
}

void EventManager::start(std::size_t worker_threads) {
    if (running_.exchange(true)) {
        return; // Already running
    }
    
    if (worker_threads == 0) {
        worker_threads = config::Config::getInstance().getWorkerThreads();
    }
    
    workers_.reserve(worker_threads);
    for (std::size_t i = 0; i < worker_threads; ++i) {
        workers_.emplace_back(&EventManager::processEvents, this);
    }
}

void EventManager::stop(bool drain) {
    if (!running_.exchange(false)) {
        return; // Already stopped
    }
    
    if (drain) {
        // Process remaining events
        std::unique_lock<std::mutex> lock(queue_mutex_);
        while (!event_queue_.empty()) {
            auto event = event_queue_.top();
            event_queue_.pop();
            lock.unlock();
            dispatchEvent(event);
            lock.lock();
        }
    }
    
    queue_cv_.notify_all();
    
    for (auto& worker : workers_) {
        if (worker.joinable()) {
            worker.join();
        }
    }
    workers_.clear();
}

void EventManager::publish(const EventPtr& event) {
    event->sequence_num = next_sequence_.fetch_add(1, std::memory_order_relaxed);
    
    std::size_t queue_size;
    {
        std::unique_lock<std::mutex> lock(queue_mutex_);
        
        if (event_queue_.size() >= max_queue_size_) {
            // Use atomic increment instead of lock for hot path
            events_dropped_.fetch_add(1, std::memory_order_relaxed);
            return;
        }
        
        event_queue_.push(event);
        queue_size = event_queue_.size();
    }
    
    // Update stats atomically - no lock needed in hot path
    events_published_.fetch_add(1, std::memory_order_relaxed);
    
    // Update high watermark using CAS (lock-free)
    std::uint64_t current_hwm = queue_high_watermark_.load(std::memory_order_relaxed);
    while (queue_size > current_hwm && 
           !queue_high_watermark_.compare_exchange_weak(current_hwm, queue_size,
               std::memory_order_relaxed, std::memory_order_relaxed)) {
        // CAS failed, current_hwm updated, retry
    }
    
    queue_cv_.notify_one();
}

void EventManager::publishSync(const EventPtr& event) {
    event->sequence_num = next_sequence_.fetch_add(1);
    dispatchEvent(event);
}

void EventManager::publishBatch(const std::vector<EventPtr>& events) {
    std::size_t published = 0;
    std::size_t queue_size = 0;
    
    {
        std::unique_lock<std::mutex> lock(queue_mutex_);
        
        for (const auto& event : events) {
            if (event_queue_.size() >= max_queue_size_) {
                events_dropped_.fetch_add(events.size() - published, std::memory_order_relaxed);
                break;
            }
            
            const_cast<EventPtr&>(event)->sequence_num = 
                next_sequence_.fetch_add(1, std::memory_order_relaxed);
            event_queue_.push(event);
            ++published;
        }
        queue_size = event_queue_.size();
    }
    
    // Update stats atomically (no lock needed)
    events_published_.fetch_add(published, std::memory_order_relaxed);
    
    // Update high watermark using CAS
    std::uint64_t current_hwm = queue_high_watermark_.load(std::memory_order_relaxed);
    while (queue_size > current_hwm && 
           !queue_high_watermark_.compare_exchange_weak(current_hwm, queue_size,
               std::memory_order_relaxed, std::memory_order_relaxed)) {
    }
    
    queue_cv_.notify_all();
}

void EventManager::processEvents() {
    while (running_.load(std::memory_order_relaxed)) {
        EventPtr event;
        
        {
            std::unique_lock<std::mutex> lock(queue_mutex_);
            queue_cv_.wait(lock, [this] {
                return !event_queue_.empty() || !running_.load(std::memory_order_relaxed);
            });
            
            if (!running_.load(std::memory_order_relaxed) && event_queue_.empty()) {
                return;
            }
            
            if (!event_queue_.empty()) {
                event = event_queue_.top();
                event_queue_.pop();
            }
        }
        
        if (event) {
            // Dispatch without holding any locks
            dispatchEvent(event);
            
            // Atomic increment - no lock needed
            events_processed_.fetch_add(1, std::memory_order_relaxed);
        }
    }
}

void EventManager::dispatchEvent(const EventPtr& event) {
    // Copy subscribers under lock, then invoke without holding subscriptions_mutex_.
    // Handlers use publishSync for order flow (e.g. ORDER_SUBMITTED -> HTTP -> ORDER_ACCEPTED);
    // re-entering dispatchEvent while the outer call still holds a shared_lock would deadlock
    // on std::shared_mutex (same thread cannot take a second shared lock).
    std::vector<std::shared_ptr<Subscription>> to_run;
    to_run.reserve(8);
    {
        std::shared_lock<std::shared_mutex> lock(subscriptions_mutex_);
        auto it = subscriptions_.find(event->type);
        if (it != subscriptions_.end()) {
            for (const auto& sub : it->second) {
                if (sub->active && (!sub->filter || sub->filter(event))) {
                    to_run.push_back(sub);
                }
            }
        }
        for (const auto& sub : global_subscriptions_) {
            if (sub->active && (!sub->filter || sub->filter(event))) {
                to_run.push_back(sub);
            }
        }
    }
    for (const auto& sub : to_run) {
        try {
            sub->callback(event);
        } catch (...) {
            // Log but don't propagate exceptions
        }
    }
}

SubscriptionHandle EventManager::subscribe(EventType type, EventCallback callback) {
    return subscribe(type, std::move(callback), nullptr);
}

SubscriptionHandle EventManager::subscribe(EventType type, EventCallback callback, EventFilter filter) {
    auto id = generateSubscriptionId();
    auto sub = std::make_shared<Subscription>(id, std::move(callback), std::move(filter));
    
    // Subscription is cold path - lock is acceptable
    std::unique_lock<std::shared_mutex> lock(subscriptions_mutex_);
    subscriptions_[type].push_back(sub);
    
    return SubscriptionHandle(id);
}

SubscriptionHandle EventManager::subscribeAll(EventCallback callback) {
    auto id = generateSubscriptionId();
    auto sub = std::make_shared<Subscription>(id, std::move(callback), nullptr);
    
    std::unique_lock<std::shared_mutex> lock(subscriptions_mutex_);
    global_subscriptions_.push_back(sub);
    
    return SubscriptionHandle(id);
}

SubscriptionHandle EventManager::subscribeMultiple(const std::vector<EventType>& types, EventCallback callback) {
    auto id = generateSubscriptionId();
    
    std::unique_lock<std::shared_mutex> lock(subscriptions_mutex_);
    
    for (const auto& type : types) {
        auto sub = std::make_shared<Subscription>(id, callback, nullptr);
        subscriptions_[type].push_back(sub);
    }
    
    return SubscriptionHandle(id);
}

void EventManager::unsubscribe(const SubscriptionHandle& handle) {
    if (!handle.isValid()) return;
    
    // Unsubscribe is cold path - lock is acceptable
    std::unique_lock<std::shared_mutex> lock(subscriptions_mutex_);
    
    // Search in type-specific subscriptions
    for (auto& [type, subs] : subscriptions_) {
        auto it = std::remove_if(subs.begin(), subs.end(),
            [&handle](const std::shared_ptr<Subscription>& sub) {
                return sub->id == handle.getId();
            });
        
        if (it != subs.end()) {
            subs.erase(it, subs.end());
        }
    }
    
    // Search in global subscriptions
    auto it = std::remove_if(global_subscriptions_.begin(), global_subscriptions_.end(),
        [&handle](const std::shared_ptr<Subscription>& sub) {
            return sub->id == handle.getId();
        });
    
    if (it != global_subscriptions_.end()) {
        global_subscriptions_.erase(it, global_subscriptions_.end());
    }
}

void EventManager::unsubscribeAll(EventType type) {
    std::unique_lock<std::shared_mutex> lock(subscriptions_mutex_);
    subscriptions_.erase(type);
}

void EventManager::clearAllSubscriptions() {
    std::unique_lock<std::shared_mutex> lock(subscriptions_mutex_);
    subscriptions_.clear();
    global_subscriptions_.clear();
}

EventManager::Stats EventManager::getStats() const {
    Stats stats;
    // Read atomic counters (no lock needed)
    stats.events_published = events_published_.load(std::memory_order_relaxed);
    stats.events_processed = events_processed_.load(std::memory_order_relaxed);
    stats.events_dropped = events_dropped_.load(std::memory_order_relaxed);
    stats.queue_high_watermark = queue_high_watermark_.load(std::memory_order_relaxed);
    
    // Note: We can't easily get queue size in a const method without mutable mutex
    // Return 0 for now - this is cold path stats anyway
    stats.current_queue_size = 0;
    
    // Subscription count also requires lock - return cached value or 0
    stats.active_subscriptions = 0;
    
    return stats;
}

void EventManager::resetStats() {
    // Reset atomic counters
    events_published_.store(0, std::memory_order_relaxed);
    events_processed_.store(0, std::memory_order_relaxed);
    events_dropped_.store(0, std::memory_order_relaxed);
    queue_high_watermark_.store(0, std::memory_order_relaxed);
}

void EventManager::setMaxQueueSize(std::size_t size) {
    max_queue_size_ = size;
}

void EventManager::setBatchSize(std::size_t size) {
    batch_size_ = size;
}

std::uint64_t EventManager::generateSubscriptionId() {
    return next_subscription_id_.fetch_add(1);
}

} // namespace events
} // namespace architect
