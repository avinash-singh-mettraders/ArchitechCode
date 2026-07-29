#include "strategy/Predictor.h"
#include "core/Platform.h"
#include <chrono>

namespace architect {
namespace strategy {

// =============================================================================
// BasePredictor Implementation
// =============================================================================

BasePredictor::BasePredictor(const std::string& name)
    : name_(name)
{
}

BasePredictor::~BasePredictor() {
    if (running_) {
        stop();
    }
    teardownSubscriptions();
}

void BasePredictor::initialize(DateInt date) {
    if (initialized_) {
        return;
    }
    
    current_date_ = date;
    log_info("Initializing predictor: " + name_ + " for date " + dateIntToString(date));
    
    setupSubscriptions();
    initialized_ = true;
    
    log_info("Predictor " + name_ + " initialized with " + 
             std::to_string(timer_racks_.size()) + " timers and " +
             std::to_string(subscribed_symbols_.size()) + " symbols");
}

void BasePredictor::start() {
    if (!initialized_) {
        log_error("Cannot start predictor " + name_ + " - not initialized");
        return;
    }
    
    if (running_) {
        return;
    }
    
    running_ = true;
    log_info("Predictor " + name_ + " started");
}

void BasePredictor::stop() {
    if (!running_) {
        return;
    }
    
    running_ = false;
    log_info("Predictor " + name_ + " stopped");
}

void BasePredictor::shutdown() {
    stop();
    teardownSubscriptions();
    initialized_ = false;
    log_info("Predictor " + name_ + " shutdown complete");
}

void BasePredictor::on_eod(DateInt date) {
    log_info("Predictor " + name_ + " EOD for " + dateIntToString(date));
    
    // Log statistics
    log_info("  Timer callbacks: " + std::to_string(stats_.timer_callbacks));
    log_info("  Tick callbacks: " + std::to_string(stats_.tick_callbacks));
    log_info("  Signals emitted: " + std::to_string(stats_.signals_emitted));
    log_info("  Avg compute time: " + std::to_string(stats_.avg_compute_time_ns) + " ns");
}

// ==========================================================================
// Configuration Methods
// ==========================================================================

void BasePredictor::add_timer(TimeRack time_rack) {
    timer_racks_.insert(time_rack);
    log_debug("Predictor " + name_ + " added timer at " + timeRackToString(time_rack));
}

void BasePredictor::add_timers(const std::vector<TimeRack>& time_racks) {
    for (auto tr : time_racks) {
        timer_racks_.insert(tr);
    }
}

void BasePredictor::remove_timer(TimeRack time_rack) {
    timer_racks_.erase(time_rack);
}

void BasePredictor::clear_timers() {
    timer_racks_.clear();
}

void BasePredictor::subscribe_symbol(const std::string& symbol) {
    subscribed_symbols_.insert(symbol);
    log_debug("Predictor " + name_ + " subscribed to " + symbol);
}

void BasePredictor::unsubscribe_symbol(const std::string& symbol) {
    subscribed_symbols_.erase(symbol);
}

void BasePredictor::set_parameter(const std::string& key, double value) {
    parameters_[key] = value;
}

double BasePredictor::get_parameter(const std::string& key, double default_value) const {
    auto it = parameters_.find(key);
    if (it != parameters_.end()) {
        return it->second;
    }
    return default_value;
}

// ==========================================================================
// Signal Emission
// ==========================================================================

void BasePredictor::emit_signal(const std::string& signal_name, double value,
                                double confidence, const std::string& symbol) {
    emit_signal_with_metadata(signal_name, value, confidence, symbol, "");
}

void BasePredictor::emit_signal_with_metadata(const std::string& signal_name, double value,
                                              double confidence, const std::string& symbol,
                                              const std::string& metadata_json) {
    if (!running_.load(std::memory_order_relaxed) || !enabled_.load(std::memory_order_relaxed)) {
        return;
    }
    
    PredictorSignalData signal_data;
    signal_data.predictor_name = name_;
    signal_data.signal_name = signal_name;
    signal_data.symbol = makeSymbol(symbol);
    signal_data.signal_value = value;
    signal_data.confidence = confidence;
    signal_data.date = current_date_;
    signal_data.time_rack = current_time_rack_;
    signal_data.compute_time = std::chrono::high_resolution_clock::now().time_since_epoch();
    signal_data.metadata = metadata_json;
    
    auto event = std::make_shared<Event>(EventType::PREDICTOR_SIGNAL, signal_data, EventPriority::HIGH);
    EventManager::getInstance().publish(event);
    
    // Atomic increment - no lock needed in hot path
    signals_emitted_.fetch_add(1, std::memory_order_relaxed);
    
    log_debug("Predictor " + name_ + " emitted signal: " + signal_name + 
              " = " + std::to_string(value) + " (conf: " + std::to_string(confidence) + ")");
}

// ==========================================================================
// Logging Helpers
// ==========================================================================

void BasePredictor::log_info(const std::string& msg) {
    core::Main().logger()->info("[{}] {}", name_, msg);
}

void BasePredictor::log_warn(const std::string& msg) {
    core::Main().logger()->warn("[{}] {}", name_, msg);
}

void BasePredictor::log_error(const std::string& msg) {
    core::Main().logger()->error("[{}] {}", name_, msg);
    errors_.fetch_add(1, std::memory_order_relaxed);
}

void BasePredictor::log_debug(const std::string& msg) {
    core::Main().logger()->debug("[{}] {}", name_, msg);
}

void BasePredictor::record_compute_time(std::uint64_t /* nanos */) {
    // Compute time tracking removed for latency optimization
    // If needed, implement using lock-free ring buffer for sampling
}

// ==========================================================================
// Internal Event Handlers
// ==========================================================================

void BasePredictor::handleTimerEvent(const EventPtr& event) {
    // Fast path checks using relaxed memory order
    if (!running_.load(std::memory_order_relaxed) || 
        !enabled_.load(std::memory_order_relaxed)) {
        return;
    }
    
    if (!event->hasData<TimerEventData>()) {
        return;
    }
    
    auto data = event->getData<TimerEventData>();
    
    // Check if this timer is one we subscribed to (read-only, no lock needed after init)
    if (timer_racks_.find(data.time_rack) == timer_racks_.end()) {
        return;
    }
    
    current_time_rack_ = data.time_rack;
    current_date_ = data.date;
    
    try {
        on_timer(data.date, data.time_rack);
    } catch (const std::exception& e) {
        log_error("Exception in on_timer: " + std::string(e.what()));
    }
    
    // Atomic increment - no lock needed
    timer_callbacks_.fetch_add(1, std::memory_order_relaxed);
}

void BasePredictor::handleTickEvent(const EventPtr& event) {
    // Fast path checks
    if (!running_.load(std::memory_order_relaxed) || 
        !enabled_.load(std::memory_order_relaxed)) {
        return;
    }
    
    if (!event->hasData<TickEventData>()) {
        return;
    }
    
    auto tick = event->getData<TickEventData>();
    std::string symbol_str(tick.symbol.data());
    
    // Check if we're subscribed to this symbol (read-only after init)
    if (subscribed_symbols_.find(symbol_str) == subscribed_symbols_.end()) {
        return;
    }
    
    TimeRack tick_time_rack = current_time_rack_;
    
    try {
        on_tick(tick, current_date_, tick_time_rack);
    } catch (const std::exception& e) {
        log_error("Exception in on_tick: " + std::string(e.what()));
    }
    
    // Atomic increment - no lock needed
    tick_callbacks_.fetch_add(1, std::memory_order_relaxed);
}

void BasePredictor::handleTradeEvent(const EventPtr& event) {
    // Fast path checks
    if (!running_.load(std::memory_order_relaxed) || 
        !enabled_.load(std::memory_order_relaxed)) {
        return;
    }
    
    if (!event->hasData<TradeEventData>()) {
        return;
    }
    
    auto trade = event->getData<TradeEventData>();
    std::string symbol_str(trade.symbol.data());
    
    // Check subscription (read-only after init)
    if (subscribed_symbols_.find(symbol_str) == subscribed_symbols_.end()) {
        return;
    }
    
    try {
        on_trade(trade, current_date_, current_time_rack_);
    } catch (const std::exception& e) {
        log_error("Exception in on_trade: " + std::string(e.what()));
    }
}

void BasePredictor::handleL2Event(const EventPtr& /*event*/) {
    // Fast path checks
    if (!running_.load(std::memory_order_relaxed) || 
        !enabled_.load(std::memory_order_relaxed)) {
        return;
    }
    
    // L2 events would contain symbol info
    // For now, just call on_book_update for all subscribed symbols
}

void BasePredictor::setupSubscriptions() {
    auto& em = EventManager::getInstance();
    
    // Subscribe to timer events
    auto timer_handle = em.subscribe(EventType::TIMER_EVENT,
        [this](const EventPtr& event) { handleTimerEvent(event); });
    subscription_handles_.push_back(timer_handle);
    
    // Subscribe to tick events
    auto tick_handle = em.subscribe(EventType::TICK_UPDATE,
        [this](const EventPtr& event) { handleTickEvent(event); });
    subscription_handles_.push_back(tick_handle);
    
    // Subscribe to L1 events
    auto l1_handle = em.subscribe(EventType::L1_UPDATE,
        [this](const EventPtr& event) { handleTickEvent(event); });
    subscription_handles_.push_back(l1_handle);
    
    // Subscribe to trade events
    auto trade_handle = em.subscribe(EventType::TRADE_UPDATE,
        [this](const EventPtr& event) { handleTradeEvent(event); });
    subscription_handles_.push_back(trade_handle);
    
    // Subscribe to L2 events
    auto l2_handle = em.subscribe(EventType::L2_UPDATE,
        [this](const EventPtr& event) { handleL2Event(event); });
    subscription_handles_.push_back(l2_handle);
}

void BasePredictor::teardownSubscriptions() {
    auto& em = EventManager::getInstance();
    
    for (auto& handle : subscription_handles_) {
        em.unsubscribe(handle);
    }
    subscription_handles_.clear();
}

// =============================================================================
// PredictorManager Implementation
// =============================================================================

PredictorManager& PredictorManager::getInstance() {
    static PredictorManager instance;
    return instance;
}

void PredictorManager::registerPredictor(PredictorPtr predictor) {
    if (!predictor) return;
    
    std::unique_lock<std::shared_mutex> lock(predictors_mutex_);
    predictors_[predictor->getName()] = predictor;
}

void PredictorManager::unregisterPredictor(const std::string& name) {
    std::unique_lock<std::shared_mutex> lock(predictors_mutex_);
    predictors_.erase(name);
}

PredictorPtr PredictorManager::getPredictor(const std::string& name) {
    std::shared_lock<std::shared_mutex> lock(predictors_mutex_);
    auto it = predictors_.find(name);
    if (it != predictors_.end()) {
        return it->second;
    }
    return nullptr;
}

std::vector<PredictorPtr> PredictorManager::getAllPredictors() {
    std::shared_lock<std::shared_mutex> lock(predictors_mutex_);
    std::vector<PredictorPtr> result;
    result.reserve(predictors_.size());
    for (const auto& [name, predictor] : predictors_) {
        result.push_back(predictor);
    }
    return result;
}

void PredictorManager::initializeAll(DateInt date) {
    std::shared_lock<std::shared_mutex> lock(predictors_mutex_);
    for (auto& [name, predictor] : predictors_) {
        predictor->initialize(date);
    }
}

void PredictorManager::startAll() {
    std::shared_lock<std::shared_mutex> lock(predictors_mutex_);
    for (auto& [name, predictor] : predictors_) {
        predictor->start();
    }
}

void PredictorManager::stopAll() {
    std::shared_lock<std::shared_mutex> lock(predictors_mutex_);
    for (auto& [name, predictor] : predictors_) {
        predictor->stop();
    }
}

void PredictorManager::shutdownAll() {
    std::shared_lock<std::shared_mutex> lock(predictors_mutex_);
    for (auto& [name, predictor] : predictors_) {
        predictor->shutdown();
    }
}

void PredictorManager::eodAll(DateInt date) {
    std::shared_lock<std::shared_mutex> lock(predictors_mutex_);
    for (auto& [name, predictor] : predictors_) {
        predictor->on_eod(date);
    }
}

void PredictorManager::fireTimer(DateInt date, TimeRack time_rack) {
    // Create timer event
    TimerEventData timer_data(date, time_rack);
    auto event = std::make_shared<Event>(EventType::TIMER_EVENT, timer_data, EventPriority::HIGH);
    
    // Publish event - predictors will receive it through their subscriptions
    EventManager::getInstance().publish(event);
}

std::set<TimeRack> PredictorManager::getAllTimerRacks() const {
    std::set<TimeRack> all_racks;
    
    std::shared_lock<std::shared_mutex> lock(predictors_mutex_);
    for (const auto& [name, predictor] : predictors_) {
        const auto& racks = predictor->getTimers();
        all_racks.insert(racks.begin(), racks.end());
    }
    
    return all_racks;
}

} // namespace strategy
} // namespace architect
