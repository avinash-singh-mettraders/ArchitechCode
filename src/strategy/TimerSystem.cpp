#include "strategy/TimerSystem.h"
#include "core/Platform.h"
#include <algorithm>

namespace architect {
namespace strategy {

TimerSystem& TimerSystem::getInstance() {
    static TimerSystem instance;
    return instance;
}

TimerSystem::~TimerSystem() {
    if (running_) {
        stop();
    }
}

void TimerSystem::initialize(const TimerConfig& config) {
    config_ = config;
    current_date_ = config.simulation_date;
    
    if (!config_.realtime_mode && config_.simulation_start > 0) {
        current_time_rack_ = config_.simulation_start;
    }
}

void TimerSystem::setDate(DateInt date) {
    current_date_ = date;
}

void TimerSystem::setRealtimeMode(bool realtime) {
    config_.realtime_mode = realtime;
}

void TimerSystem::setSimulationSpeed(double speed) {
    config_.simulation_speed = speed;
}

void TimerSystem::registerTimer(TimeRack time_rack, const std::string& name) {
    std::unique_lock<std::shared_mutex> lock(timers_mutex_);
    registered_timers_.insert(time_rack);
    if (!name.empty()) {
        timer_names_[time_rack] = name;
    }
}

void TimerSystem::registerTimers(const std::set<TimeRack>& time_racks) {
    std::unique_lock<std::shared_mutex> lock(timers_mutex_);
    registered_timers_.insert(time_racks.begin(), time_racks.end());
}

void TimerSystem::unregisterTimer(TimeRack time_rack) {
    std::unique_lock<std::shared_mutex> lock(timers_mutex_);
    registered_timers_.erase(time_rack);
    timer_names_.erase(time_rack);
}

void TimerSystem::clearTimers() {
    std::unique_lock<std::shared_mutex> lock(timers_mutex_);
    registered_timers_.clear();
    timer_names_.clear();
}

std::set<TimeRack> TimerSystem::getRegisteredTimers() const {
    std::shared_lock<std::shared_mutex> lock(timers_mutex_);
    return registered_timers_;
}

void TimerSystem::setCallback(TimerCallback callback) {
    callback_ = std::move(callback);
}

void TimerSystem::start() {
    if (running_) {
        return;
    }
    
    running_ = true;
    
    if (config_.realtime_mode) {
        timer_thread_ = std::thread(&TimerSystem::timerLoop, this);
    }
    
    core::Main().logger()->info("[TimerSystem] Started in {} mode",
        config_.realtime_mode ? "realtime" : "simulation");
}

void TimerSystem::stop() {
    if (!running_) {
        return;
    }
    
    running_ = false;
    cv_.notify_all();
    
    if (timer_thread_.joinable()) {
        timer_thread_.join();
    }
    
    core::Main().logger()->info("[TimerSystem] Stopped. Fired {} timers", stats_.timers_fired);
}

void TimerSystem::advanceTo(TimeRack target_time_rack) {
    if (config_.realtime_mode) {
        return;  // Only for simulation mode
    }
    
    std::shared_lock<std::shared_mutex> lock(timers_mutex_);
    
    TimeRack current = current_time_rack_.load();
    
    // Find all timers between current and target
    auto it_start = registered_timers_.upper_bound(current);
    auto it_end = registered_timers_.upper_bound(target_time_rack);
    
    for (auto it = it_start; it != it_end; ++it) {
        TimeRack tr = *it;
        
        // Get timer name if available
        std::string name;
        auto name_it = timer_names_.find(tr);
        if (name_it != timer_names_.end()) {
            name = name_it->second;
        }
        
        // Check for second/minute boundaries
        if (config_.emit_second_events) {
            TimeRack second = (tr / 1000) * 1000;  // Round to second
            if (second != last_second_) {
                emitSecondEvent(current_date_, second);
                last_second_ = second;
            }
        }
        
        if (config_.emit_minute_events) {
            TimeRack minute = (tr / 100000) * 100000;  // Round to minute
            if (minute != last_minute_) {
                emitMinuteEvent(current_date_, minute);
                last_minute_ = minute;
            }
        }
        
        emitTimerEvent(current_date_, tr, name);
    }
    
    current_time_rack_ = target_time_rack;
}

void TimerSystem::fireTimer(TimeRack time_rack) {
    fireTimer(current_date_, time_rack);
}

void TimerSystem::fireTimer(DateInt date, TimeRack time_rack) {
    std::string name;
    {
        std::shared_lock<std::shared_mutex> lock(timers_mutex_);
        auto it = timer_names_.find(time_rack);
        if (it != timer_names_.end()) {
            name = it->second;
        }
    }
    
    emitTimerEvent(date, time_rack, name);
    current_time_rack_ = time_rack;
}

TimerStats TimerSystem::getStats() const {
    std::lock_guard<std::mutex> lock(stats_mutex_);
    return stats_;
}

void TimerSystem::resetStats() {
    std::lock_guard<std::mutex> lock(stats_mutex_);
    stats_ = TimerStats{};
}

void TimerSystem::timerLoop() {
    while (running_) {
        auto loop_start = std::chrono::high_resolution_clock::now();
        
        TimeRack current_tr = getCurrentSystemTimeRack();
        current_time_rack_ = current_tr;
        
        // Get timers to fire
        std::vector<std::pair<TimeRack, std::string>> to_fire;
        
        {
            std::shared_lock<std::shared_mutex> lock(timers_mutex_);
            
            for (TimeRack tr : registered_timers_) {
                // Check if this timer should fire
                // Timer fires if: timer_time <= current_time AND timer_time > last_fired_time
                if (tr <= current_tr && tr > stats_.last_fired_time_rack) {
                    std::string name;
                    auto name_it = timer_names_.find(tr);
                    if (name_it != timer_names_.end()) {
                        name = name_it->second;
                    }
                    to_fire.emplace_back(tr, name);
                }
            }
        }
        
        // Fire timers in order
        std::sort(to_fire.begin(), to_fire.end());
        
        for (const auto& [tr, name] : to_fire) {
            auto fire_start = std::chrono::high_resolution_clock::now();
            
            // Check for second/minute boundaries
            if (config_.emit_second_events) {
                TimeRack second = (tr / 1000) * 1000;
                if (second != last_second_) {
                    emitSecondEvent(current_date_, second);
                    last_second_ = second;
                }
            }
            
            if (config_.emit_minute_events) {
                TimeRack minute = (tr / 100000) * 100000;
                if (minute != last_minute_) {
                    emitMinuteEvent(current_date_, minute);
                    last_minute_ = minute;
                }
            }
            
            emitTimerEvent(current_date_, tr, name);
            
            auto fire_end = std::chrono::high_resolution_clock::now();
            auto latency_us = std::chrono::duration_cast<std::chrono::microseconds>(
                fire_end - fire_start).count();
            
            // Update stats
            {
                std::lock_guard<std::mutex> lock(stats_mutex_);
                double total = stats_.avg_latency_us * stats_.timers_fired;
                total += static_cast<double>(latency_us);
                stats_.avg_latency_us = total / (stats_.timers_fired + 1);
                if (latency_us > stats_.max_latency_us) {
                    stats_.max_latency_us = static_cast<double>(latency_us);
                }
                stats_.last_fired_time_rack = tr;
            }
        }
        
        // Sleep until next tick
        auto loop_end = std::chrono::high_resolution_clock::now();
        auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
            loop_end - loop_start);
        
        auto sleep_time = std::chrono::microseconds(config_.tick_interval_us) - elapsed;
        if (sleep_time.count() > 0) {
            std::this_thread::sleep_for(sleep_time);
        }
    }
}

TimeRack TimerSystem::getCurrentSystemTimeRack() const {
    return systemTimeToTimeRack();
}

void TimerSystem::emitTimerEvent(DateInt date, TimeRack time_rack, const std::string& name) {
    TimerEventData data(date, time_rack, name);
    data.sequence = stats_.timers_fired + 1;
    
    auto event = std::make_shared<Event>(EventType::TIMER_EVENT, data, EventPriority::HIGH);
    EventManager::getInstance().publish(event);
    
    // Call callback if set
    if (callback_) {
        callback_(date, time_rack);
    }
    
    {
        std::lock_guard<std::mutex> lock(stats_mutex_);
        ++stats_.timers_fired;
    }
    
    core::Main().logger()->debug("[TimerSystem] Fired timer {} at {}", 
        name.empty() ? "unnamed" : name, timeRackToString(time_rack));
}

void TimerSystem::emitSecondEvent(DateInt date, TimeRack time_rack) {
    TimerEventData data(date, time_rack, "SECOND");
    auto event = std::make_shared<Event>(EventType::TIMER_SECOND, data, EventPriority::NORMAL);
    EventManager::getInstance().publish(event);
    
    {
        std::lock_guard<std::mutex> lock(stats_mutex_);
        ++stats_.second_events;
    }
}

void TimerSystem::emitMinuteEvent(DateInt date, TimeRack time_rack) {
    TimerEventData data(date, time_rack, "MINUTE");
    auto event = std::make_shared<Event>(EventType::TIMER_MINUTE, data, EventPriority::NORMAL);
    EventManager::getInstance().publish(event);
    
    {
        std::lock_guard<std::mutex> lock(stats_mutex_);
        ++stats_.minute_events;
    }
}

} // namespace strategy
} // namespace architect
