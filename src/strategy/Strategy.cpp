#include "strategy/Strategy.h"
#include "core/Platform.h"
#include "core/LatencyTracker.h"
#include "orders/OrderManager.h"
#include <chrono>
#include <sstream>
#include <iomanip>
#include <iostream>

namespace architect {
namespace strategy {

// =============================================================================
// BaseStrategy Implementation
// =============================================================================

BaseStrategy::BaseStrategy(const std::string& name)
    : name_(name)
{
}

BaseStrategy::~BaseStrategy() {
    if (running_) {
        stop();
    }
    teardownSubscriptions();
}

void BaseStrategy::initialize(DateInt date) {
    if (initialized_) {
        return;
    }
    
    current_date_ = date;
    orders_today_ = 0;
    positions_.clear();
    open_orders_.clear();
    orders_by_symbol_.clear();
    
    log_info("Initializing strategy: " + name_ + " for date " + dateIntToString(date));
    
    setupSubscriptions();
    initialized_ = true;
    
    log_info("Strategy " + name_ + " initialized with " + 
             std::to_string(timer_racks_.size()) + " timers and " +
             std::to_string(predictor_subscriptions_.size()) + " predictor subscriptions");
}

void BaseStrategy::start() {
    if (!initialized_) {
        log_error("Cannot start strategy " + name_ + " - not initialized");
        return;
    }
    
    if (running_) {
        return;
    }
    
    running_ = true;
    log_info("Strategy " + name_ + " started");
}

void BaseStrategy::stop() {
    if (!running_) {
        return;
    }
    
    running_ = false;
    log_info("Strategy " + name_ + " stopped");
}

void BaseStrategy::shutdown() {
    stop();
    teardownSubscriptions();
    initialized_ = false;
    log_info("Strategy " + name_ + " shutdown complete");
}

void BaseStrategy::on_eod(DateInt date) {
    log_info("Strategy " + name_ + " EOD for " + dateIntToString(date));
    
    // Log statistics
    log_info("  Orders submitted: " + std::to_string(stats_.orders_submitted));
    log_info("  Orders filled: " + std::to_string(stats_.orders_filled));
    log_info("  Orders rejected: " + std::to_string(stats_.orders_rejected));
    log_info("  Realized P&L: " + std::to_string(stats_.realized_pnl));
    
    // Log to CSV
    core::Main().logger()->log_strategy(
        name_,
        "",         // symbol
        "EOD",      // action
        "eod_summary",  // reason
        stats_.realized_pnl  // signal_value (using for pnl)
    );
}

// ==========================================================================
// Configuration Methods
// ==========================================================================

void BaseStrategy::add_timer(TimeRack time_rack) {
    timer_racks_.insert(time_rack);
    log_debug("Strategy " + name_ + " added timer at " + timeRackToString(time_rack));
}

void BaseStrategy::add_timers(const std::vector<TimeRack>& time_racks) {
    for (auto tr : time_racks) {
        timer_racks_.insert(tr);
    }
}

void BaseStrategy::remove_timer(TimeRack time_rack) {
    timer_racks_.erase(time_rack);
}

void BaseStrategy::clear_timers() {
    timer_racks_.clear();
}

void BaseStrategy::subscribe_predictor(const std::string& predictor_name) {
    predictor_subscriptions_.insert(predictor_name);
    log_debug("Strategy " + name_ + " subscribed to predictor: " + predictor_name);
}

void BaseStrategy::unsubscribe_predictor(const std::string& predictor_name) {
    predictor_subscriptions_.erase(predictor_name);
}

void BaseStrategy::subscribe_symbol(const std::string& symbol) {
    symbol_subscriptions_.insert(symbol);
}

void BaseStrategy::unsubscribe_symbol(const std::string& symbol) {
    symbol_subscriptions_.erase(symbol);
}

void BaseStrategy::set_parameter(const std::string& key, double value) {
    parameters_[key] = value;
}

double BaseStrategy::get_parameter(const std::string& key, double default_value) const {
    auto it = parameters_.find(key);
    if (it != parameters_.end()) {
        return it->second;
    }
    return default_value;
}

// ==========================================================================
// Order Submission
// ==========================================================================

std::string BaseStrategy::submit_order(const OrderRequest& request) {
    // === LATENCY INSTRUMENTATION: Enter submit_order ===
    auto& lat = core::LatencyTracker::get();
    auto t_enter_submit = std::chrono::steady_clock::now();
    // Anchor each submit to this call — order_submit_start_time defaulted to epoch and
    // markStrategyDecision() was never wired, so deltas were process-uptime scale (~1e10 µs).
    lat.strategy_decision_time = t_enter_submit;
    lat.order_submit_start_time = t_enter_submit;
    auto strategy_prep_us = std::chrono::duration_cast<std::chrono::microseconds>(
        t_enter_submit - lat.order_submit_start_time).count();
    
    std::cout << "[LATENCY_TRACE] T0.1_ENTER_SUBMIT_ORDER: strategy_prep=" << core::latencyDisplayUs(strategy_prep_us) << "us" << std::endl;
    
    if (!running_ || !enabled_) {
        log_warn("Cannot submit order - strategy not running or disabled");
        return "";
    }
    
    // Generate client order ID
    std::string client_order_id = request.client_order_id;
    if (client_order_id.empty()) {
        client_order_id = generate_client_order_id();
    }
    
    // Create modified request with strategy info
    OrderRequest req = request;
    req.client_order_id = client_order_id;
    req.strategy_name = name_;
    req.date = current_date_;
    req.time_rack = current_time_rack_;
    
    // Log the action
    log_action(StrategyActionType::SUBMIT_ORDER, req.symbol, req.side, 
               req.price, req.quantity, req.reason);
    
    // === LATENCY INSTRUMENTATION: Before OrderManager ===
    auto t_before_om = std::chrono::steady_clock::now();
    auto request_build_us = std::chrono::duration_cast<std::chrono::microseconds>(
        t_before_om - t_enter_submit).count();
    std::cout << "[LATENCY_TRACE] T0.2_BEFORE_ORDER_MANAGER: request_build=" << core::latencyDisplayUs(request_build_us) << "us" << std::endl;
    
    // Submit to order manager
    try {
        auto& om = orders::OrderManager::getInstance();
        
        // Convert to internal order format
        orders::OrderRequest internal_req;
        internal_req.symbol = makeSymbol(req.symbol);
        internal_req.side = req.side;
        internal_req.type = req.type;
        internal_req.time_in_force = req.time_in_force;
        internal_req.price = req.price;
        internal_req.stop_price = req.stop_price;
        internal_req.quantity = req.quantity;
        internal_req.client_order_id = req.client_order_id;
        
        // Submit order (returns order_id or 0 on failure)
        // NOTE: This is SYNCHRONOUS - it calls Platform's ORDER_SUBMITTED handler
        // which makes the HTTP call and waits for response
        auto t_before_submit = std::chrono::steady_clock::now();
        
        OrderId order_id = om.submitOrder(internal_req, 0);  // user_id = 0 for now
        
        auto t_after_submit = std::chrono::steady_clock::now();
        auto om_submit_us = std::chrono::duration_cast<std::chrono::microseconds>(
            t_after_submit - t_before_submit).count();
        
        // This includes: OrderManager work + Event publish + Platform handler (HTTP call)
        std::cout << "[LATENCY_TRACE] T0.3_AFTER_ORDER_MANAGER: om_submit_total=" << core::latencyDisplayUs(om_submit_us) 
                  << "us (includes HTTP RTT)" << std::endl;
        
        if (order_id != 0) {
            {
                std::lock_guard<std::mutex> lock(orders_mutex_);
                open_orders_.insert(order_id);
                orders_by_symbol_[req.symbol].insert(order_id);
            }
            
            // Atomic increments - no lock needed
            orders_submitted_.fetch_add(1, std::memory_order_relaxed);
            ++orders_today_;
            
            // === LATENCY INSTRUMENTATION: Full submit_order latency ===
            auto t_exit_submit = std::chrono::steady_clock::now();
            auto total_submit_us = std::chrono::duration_cast<std::chrono::microseconds>(
                t_exit_submit - lat.order_submit_start_time).count();
            std::cout << "[LATENCY_TRACE] T0.4_EXIT_SUBMIT_ORDER: order_id=" << order_id 
                      << " total_submit_order=" << core::latencyDisplayUs(total_submit_us) << "us" << std::endl;
            
            log_info("Order submitted: " + client_order_id + " " + 
                     std::string(sideToString(req.side)) + " " + 
                     std::to_string(req.quantity) + " " + req.symbol);
            
            return client_order_id;
        } else {
            log_error("Failed to submit order");
            return "";
        }
        
    } catch (const std::exception& e) {
        log_error("Exception submitting order: " + std::string(e.what()));
        return "";
    }
}

std::string BaseStrategy::submit_order_no_dispatch(const OrderRequest& request,
                                                   OrderId& out_order_id) {
    out_order_id = 0;
    if (!running_ || !enabled_) {
        log_warn("Cannot submit order (no-dispatch) - strategy not running or disabled");
        return "";
    }

    std::string client_order_id = request.client_order_id;
    if (client_order_id.empty()) {
        client_order_id = generate_client_order_id();
    }

    OrderRequest req = request;
    req.client_order_id = client_order_id;
    req.strategy_name = name_;
    req.date = current_date_;
    req.time_rack = current_time_rack_;

    log_action(StrategyActionType::SUBMIT_ORDER, req.symbol, req.side, req.price, req.quantity,
               req.reason);

    try {
        auto& om = orders::OrderManager::getInstance();
        orders::OrderRequest internal_req;
        internal_req.symbol = makeSymbol(req.symbol);
        internal_req.side = req.side;
        internal_req.type = req.type;
        internal_req.time_in_force = req.time_in_force;
        internal_req.price = req.price;
        internal_req.stop_price = req.stop_price;
        internal_req.quantity = req.quantity;
        internal_req.client_order_id = req.client_order_id;

        // Create + register WITHOUT publishing ORDER_SUBMITTED (no inline HTTP). The caller
        // batches the wire fire and the ORDER_ACCEPTED dispatch afterwards.
        OrderId order_id = om.submitOrderNoDispatch(internal_req, 0);
        if (order_id != 0) {
            {
                std::lock_guard<std::mutex> lock(orders_mutex_);
                open_orders_.insert(order_id);
                orders_by_symbol_[req.symbol].insert(order_id);
            }
            orders_submitted_.fetch_add(1, std::memory_order_relaxed);
            ++orders_today_;
            out_order_id = order_id;
            return client_order_id;
        }
        log_error("Failed to submit order (no-dispatch)");
        return "";
    } catch (const std::exception& e) {
        log_error("Exception submitting order (no-dispatch): " + std::string(e.what()));
        return "";
    }
}

bool BaseStrategy::cancel_order(const CancelRequest& request) {
    if (!running_.load(std::memory_order_relaxed) || 
        !enabled_.load(std::memory_order_relaxed)) {
        return false;
    }
    
    log_action(StrategyActionType::CANCEL_ORDER, request.symbol, Side::BUY, 
               0.0, 0.0, request.reason);
    
    try {
        auto& om = orders::OrderManager::getInstance();
        orders::OrderCancelRequest cancel_req;
        cancel_req.order_id = request.order_id;
        cancel_req.client_order_id = request.client_order_id;
        cancel_req.symbol = makeSymbol(request.symbol);
        return om.cancelOrder(cancel_req);
    } catch (const std::exception& e) {
        log_error("Exception cancelling order: " + std::string(e.what()));
        return false;
    }
}

bool BaseStrategy::cancel_order(OrderId order_id, const std::string& reason) {
    CancelRequest req;
    req.order_id = order_id;
    req.strategy_name = name_;
    req.reason = reason;
    req.date = current_date_;
    req.time_rack = current_time_rack_;
    return cancel_order(req);
}

void BaseStrategy::cancel_all_orders(const std::string& symbol, const std::string& reason) {
    std::vector<OrderId> to_cancel;
    
    {
        std::lock_guard<std::mutex> lock(orders_mutex_);
        auto it = orders_by_symbol_.find(symbol);
        if (it != orders_by_symbol_.end()) {
            to_cancel.assign(it->second.begin(), it->second.end());
        }
    }
    
    for (auto order_id : to_cancel) {
        cancel_order(order_id, reason);
    }
    
    log_info("Cancelled " + std::to_string(to_cancel.size()) + " orders for " + symbol);
}

bool BaseStrategy::modify_order(const ModifyRequest& request) {
    if (!running_.load(std::memory_order_relaxed) || 
        !enabled_.load(std::memory_order_relaxed)) {
        return false;
    }
    
    log_action(StrategyActionType::MODIFY_ORDER, request.symbol, Side::BUY,
               request.new_price, request.new_quantity, request.reason);
    
    try {
        auto& om = orders::OrderManager::getInstance();
        
        orders::OrderModifyRequest internal_req;
        internal_req.order_id = request.order_id;
        internal_req.client_order_id = request.client_order_id;
        if (request.new_price > 0.0) {
            internal_req.new_price = request.new_price;
        }
        if (request.new_quantity > 0.0) {
            internal_req.new_quantity = request.new_quantity;
        }
        
        return om.modifyOrder(internal_req);
    } catch (const std::exception& e) {
        log_error("Exception modifying order: " + std::string(e.what()));
        return false;
    }
}

void BaseStrategy::close_position(const std::string& symbol, const std::string& reason) {
    Quantity pos = get_position(symbol);
    
    if (std::abs(pos) < 1e-8) {
        return;  // No position to close
    }
    
    // Cancel any open orders first
    cancel_all_orders(symbol, "closing position");
    
    // Submit closing order
    OrderRequest req;
    req.symbol = symbol;
    req.type = OrderType::MARKET;
    req.side = (pos > 0) ? Side::SELL : Side::BUY;
    req.quantity = std::abs(pos);
    req.reason = reason;
    
    submit_order(req);
    
    log_info("Closing position: " + symbol + " qty=" + std::to_string(pos));
}

void BaseStrategy::flatten_all(const std::string& reason) {
    auto all_positions = get_all_positions();
    
    for (const auto& [symbol, qty] : all_positions) {
        if (std::abs(qty) > 1e-8) {
            close_position(symbol, reason);
        }
    }
    
    log_info("Flattened all positions: " + reason);
}

// ==========================================================================
// Position & Order Queries
// ==========================================================================

Quantity BaseStrategy::get_position(const std::string& symbol) const {
    std::lock_guard<std::mutex> lock(position_mutex_);
    auto it = positions_.find(symbol);
    if (it != positions_.end()) {
        return it->second;
    }
    return 0.0;
}

std::map<std::string, Quantity> BaseStrategy::get_all_positions() const {
    std::lock_guard<std::mutex> lock(position_mutex_);
    return positions_;
}

std::vector<OrderId> BaseStrategy::get_open_orders(const std::string& symbol) const {
    std::lock_guard<std::mutex> lock(orders_mutex_);
    
    auto it = orders_by_symbol_.find(symbol);
    if (it != orders_by_symbol_.end()) {
        return std::vector<OrderId>(it->second.begin(), it->second.end());
    }
    return {};
}

std::vector<OrderId> BaseStrategy::get_all_open_orders() const {
    std::lock_guard<std::mutex> lock(orders_mutex_);
    return std::vector<OrderId>(open_orders_.begin(), open_orders_.end());
}

bool BaseStrategy::has_position(const std::string& symbol) const {
    return std::abs(get_position(symbol)) > 1e-8;
}

int BaseStrategy::get_orders_today() const {
    return orders_today_;
}

// ==========================================================================
// Logging Helpers
// ==========================================================================

void BaseStrategy::log_info(const std::string& msg) {
    core::Main().logger()->info("[STRATEGY:{}] {}", name_, msg);
}

void BaseStrategy::log_warn(const std::string& msg) {
    core::Main().logger()->warn("[STRATEGY:{}] {}", name_, msg);
}

void BaseStrategy::log_error(const std::string& msg) {
    core::Main().logger()->error("[STRATEGY:{}] {}", name_, msg);
    errors_count_.fetch_add(1, std::memory_order_relaxed);
}

void BaseStrategy::log_debug(const std::string& msg) {
    core::Main().logger()->debug("[STRATEGY:{}] {}", name_, msg);
}

void BaseStrategy::log_action(StrategyActionType action, const std::string& symbol,
                              Side side, Price price, Quantity qty, const std::string& reason) {
    // Create strategy action event
    StrategyActionData action_data;
    action_data.strategy_name = name_;
    action_data.action_type = action;
    action_data.symbol = makeSymbol(symbol);
    action_data.side = side;
    action_data.price = price;
    action_data.quantity = qty;
    action_data.reason = reason;
    action_data.date = current_date_;
    action_data.time_rack = current_time_rack_;
    
    auto event = std::make_shared<Event>(EventType::STRATEGY_ACTION, action_data, EventPriority::NORMAL);
    EventManager::getInstance().publish(event);
    
    // Log to CSV
    core::Main().logger()->log_strategy(
        name_,
        symbol,
        "ACTION",
        reason,
        0.0  // signal_value
    );
}

void BaseStrategy::clearOpenOrderTracking() {
    std::lock_guard<std::mutex> lock(orders_mutex_);
    open_orders_.clear();
    orders_by_symbol_.clear();
}

void BaseStrategy::noteAdoptedWorkingOrder(OrderId order_id, const std::string& symbol) {
    if (order_id == 0) {
        return;
    }
    std::lock_guard<std::mutex> lock(orders_mutex_);
    open_orders_.insert(order_id);
    orders_by_symbol_[symbol].insert(order_id);
}

bool BaseStrategy::clientOrderIdBelongsToThisStrategy(const std::string& client_order_id) const {
    if (client_order_id.empty() || name_.empty()) {
        return false;
    }
    if (client_order_id.size() < name_.size()) {
        return false;
    }
    if (client_order_id.compare(0, name_.size(), name_) != 0) {
        return false;
    }
    if (client_order_id.size() == name_.size()) {
        return true;
    }
    return client_order_id[name_.size()] == '_';
}

bool BaseStrategy::orderEventMatchesStrategy(const OrderEventData& data, EventType type) const {
    (void)type;
    std::lock_guard<std::mutex> lock(orders_mutex_);
    return open_orders_.find(data.order_id) != open_orders_.end();
}

std::string BaseStrategy::generate_client_order_id() {
    std::uint64_t id = next_order_id_++;
    
    std::ostringstream oss;
    oss << name_ << "_" 
        << current_date_ << "_"
        << std::setfill('0') << std::setw(6) << id;
    
    return oss.str();
}

// ==========================================================================
// Internal Event Handlers
// ==========================================================================

void BaseStrategy::handleTimerEvent(const EventPtr& event) {
    // Fast path checks
    if (!running_.load(std::memory_order_relaxed) || 
        !enabled_.load(std::memory_order_relaxed)) {
        return;
    }
    
    if (!event->hasData<TimerEventData>()) {
        return;
    }
    
    auto data = event->getData<TimerEventData>();
    
    // Check if this timer is one we subscribed to (read-only after init)
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
}

void BaseStrategy::handleSignalEvent(const EventPtr& event) {
    // Fast path checks
    if (!running_.load(std::memory_order_relaxed) || 
        !enabled_.load(std::memory_order_relaxed)) {
        return;
    }
    
    if (!event->hasData<PredictorSignalData>()) {
        return;
    }
    
    auto signal = event->getData<PredictorSignalData>();
    
    // Check if we're subscribed to this predictor (read-only after init)
    if (predictor_subscriptions_.find(signal.predictor_name) == predictor_subscriptions_.end()) {
        return;
    }
    
    current_time_rack_ = signal.time_rack;
    current_date_ = signal.date;
    
    try {
        on_signal(signal);
    } catch (const std::exception& e) {
        log_error("Exception in on_signal: " + std::string(e.what()));
    }
}

void BaseStrategy::handleOrderEvent(const EventPtr& event) {
    // Fast path checks
    if (!running_.load(std::memory_order_relaxed) || 
        !enabled_.load(std::memory_order_relaxed)) {
        log_debug("handleOrderEvent: Strategy not running/enabled, skipping event type " + 
            std::to_string(static_cast<int>(event->type)));
        return;
    }
    
    if (!event->hasData<OrderEventData>()) {
        log_debug("handleOrderEvent: Event has no OrderEventData");
        return;
    }
    
    auto data = event->getData<OrderEventData>();
    
    log_debug("handleOrderEvent: Received event type=" + std::to_string(static_cast<int>(event->type)) + 
        " order_id=" + std::to_string(data.order_id) + 
        " client_order_id=" + data.client_order_id);
    
    // Special handling for ORDER_ACCEPTED: it arrives before open_orders_ is updated
    // (synchronous event from inside submitOrder). Check client_order_id prefix instead.
    if (event->type == EventType::ORDER_ACCEPTED) {
        std::string cid(data.client_order_id);
        if (!clientOrderIdBelongsToThisStrategy(cid)) {
            log_debug("handleOrderEvent: ORDER_ACCEPTED client_order_id=" + cid +
                " does NOT belong to strategy '" + name_ + "', skipping");
            return;
        }
        log_debug("handleOrderEvent: ORDER_ACCEPTED for our order client_order_id=" + cid);
        
        // Add to open_orders_ now (since we receive this before submit_order returns)
        {
            std::lock_guard<std::mutex> lock(orders_mutex_);
            open_orders_.insert(data.order_id);
            std::string symbol(data.symbol.data());
            orders_by_symbol_[symbol].insert(data.order_id);
        }
    } else {
        if (!orderEventMatchesStrategy(data, event->type)) {
            log_debug("handleOrderEvent: order_id " + std::to_string(data.order_id) +
                      " does not match this strategy (open_orders_ / derived MM legs)");
            return;
        }
        log_debug("handleOrderEvent: order_id " + std::to_string(data.order_id) + " accepted for strategy");
    }
    
    std::string symbol(data.symbol.data());
    
    switch (event->type) {
        case EventType::ORDER_FILLED:
            {
                // Update position (needs lock)
                Quantity delta = (data.side == Side::BUY) ? data.filled_quantity : -data.filled_quantity;
                updatePosition(symbol, delta);
                
                // Remove from open orders
                {
                    std::lock_guard<std::mutex> lock(orders_mutex_);
                    open_orders_.erase(data.order_id);
                    orders_by_symbol_[symbol].erase(data.order_id);
                }
                
                // Atomic increment - no lock needed
                orders_filled_.fetch_add(1, std::memory_order_relaxed);
                
                try {
                    on_fill(data);
                } catch (const std::exception& e) {
                    log_error("Exception in on_fill: " + std::string(e.what()));
                }
            }
            break;
            
        case EventType::ORDER_PARTIALLY_FILLED:
            {
                // Update position for partial fill
                Quantity delta = (data.side == Side::BUY) ? data.filled_quantity : -data.filled_quantity;
                updatePosition(symbol, delta);
                
                try {
                    on_partial_fill(data);
                } catch (const std::exception& e) {
                    log_error("Exception in on_partial_fill: " + std::string(e.what()));
                }
            }
            break;
            
        case EventType::ORDER_REJECTED:
            {
                // Remove from open orders
                {
                    std::lock_guard<std::mutex> lock(orders_mutex_);
                    open_orders_.erase(data.order_id);
                    orders_by_symbol_[symbol].erase(data.order_id);
                }
                
                // Atomic increment
                orders_rejected_.fetch_add(1, std::memory_order_relaxed);
                
                try {
                    on_reject(data);
                } catch (const std::exception& e) {
                    log_error("Exception in on_reject: " + std::string(e.what()));
                }
            }
            break;
            
        case EventType::ORDER_CANCELLED:
            {
                // Remove from open orders
                {
                    std::lock_guard<std::mutex> lock(orders_mutex_);
                    open_orders_.erase(data.order_id);
                    orders_by_symbol_[symbol].erase(data.order_id);
                }
                
                // Atomic increment
                orders_cancelled_.fetch_add(1, std::memory_order_relaxed);
                
                try {
                    on_cancel(data);
                } catch (const std::exception& e) {
                    log_error("Exception in on_cancel: " + std::string(e.what()));
                }
            }
            break;
            
        case EventType::ORDER_ACCEPTED:
            try {
                on_accept(data);
            } catch (const std::exception& e) {
                log_error("Exception in on_accept: " + std::string(e.what()));
            }
            break;
            
        default:
            break;
    }
}

void BaseStrategy::handleTickEvent(const EventPtr& event) {
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
    if (symbol_subscriptions_.find(symbol_str) == symbol_subscriptions_.end()) {
        return;
    }
    
    try {
        on_tick(tick, current_date_, current_time_rack_);
    } catch (const std::exception& e) {
        log_error("Exception in on_tick: " + std::string(e.what()));
    }
}

void BaseStrategy::handlePositionEvent(const EventPtr& event) {
    // Fast path checks
    if (!running_.load(std::memory_order_relaxed) || 
        !enabled_.load(std::memory_order_relaxed)) {
        return;
    }
    
    if (!event->hasData<PositionEventData>()) {
        return;
    }
    
    auto position = event->getData<PositionEventData>();
    
    try {
        on_position_update(position);
    } catch (const std::exception& e) {
        log_error("Exception in on_position_update: " + std::string(e.what()));
    }
}

void BaseStrategy::setupSubscriptions() {
    auto& em = EventManager::getInstance();
    
    // Subscribe to timer events
    auto timer_handle = em.subscribe(EventType::TIMER_EVENT,
        [this](const EventPtr& event) { handleTimerEvent(event); });
    subscription_handles_.push_back(timer_handle);
    
    // Subscribe to predictor signals
    auto signal_handle = em.subscribe(EventType::PREDICTOR_SIGNAL,
        [this](const EventPtr& event) { handleSignalEvent(event); });
    subscription_handles_.push_back(signal_handle);
    
    // Subscribe to order events
    std::vector<EventType> order_events = {
        EventType::ORDER_FILLED,
        EventType::ORDER_PARTIALLY_FILLED,
        EventType::ORDER_REJECTED,
        EventType::ORDER_CANCELLED,
        EventType::ORDER_ACCEPTED
    };
    
    for (auto event_type : order_events) {
        auto handle = em.subscribe(event_type,
            [this](const EventPtr& event) { handleOrderEvent(event); });
        subscription_handles_.push_back(handle);
    }
    
    // Subscribe to tick events
    auto tick_handle = em.subscribe(EventType::TICK_UPDATE,
        [this](const EventPtr& event) { handleTickEvent(event); });
    subscription_handles_.push_back(tick_handle);
    
    auto l1_handle = em.subscribe(EventType::L1_UPDATE,
        [this](const EventPtr& event) { handleTickEvent(event); });
    subscription_handles_.push_back(l1_handle);
    
    // Subscribe to position events
    auto pos_handle = em.subscribe(EventType::POSITION_UPDATED,
        [this](const EventPtr& event) { handlePositionEvent(event); });
    subscription_handles_.push_back(pos_handle);
}

void BaseStrategy::teardownSubscriptions() {
    auto& em = EventManager::getInstance();
    
    for (auto& handle : subscription_handles_) {
        em.unsubscribe(handle);
    }
    subscription_handles_.clear();
}

void BaseStrategy::updatePosition(const std::string& symbol, Quantity delta) {
    std::lock_guard<std::mutex> lock(position_mutex_);
    positions_[symbol] += delta;
    
    // Remove if zero
    if (std::abs(positions_[symbol]) < 1e-8) {
        positions_.erase(symbol);
    }
}

// =============================================================================
// StrategyManager Implementation
// =============================================================================

StrategyManager& StrategyManager::getInstance() {
    static StrategyManager instance;
    return instance;
}

void StrategyManager::registerStrategy(StrategyPtr strategy) {
    if (!strategy) return;
    
    std::unique_lock<std::shared_mutex> lock(strategies_mutex_);
    strategies_[strategy->getName()] = strategy;
}

void StrategyManager::unregisterStrategy(const std::string& name) {
    std::unique_lock<std::shared_mutex> lock(strategies_mutex_);
    strategies_.erase(name);
}

StrategyPtr StrategyManager::getStrategy(const std::string& name) {
    std::shared_lock<std::shared_mutex> lock(strategies_mutex_);
    auto it = strategies_.find(name);
    if (it != strategies_.end()) {
        return it->second;
    }
    return nullptr;
}

std::vector<StrategyPtr> StrategyManager::getAllStrategies() {
    std::shared_lock<std::shared_mutex> lock(strategies_mutex_);
    std::vector<StrategyPtr> result;
    result.reserve(strategies_.size());
    for (const auto& [name, strategy] : strategies_) {
        result.push_back(strategy);
    }
    return result;
}

void StrategyManager::clearOpenOrderTrackingAll() {
    std::shared_lock<std::shared_mutex> lock(strategies_mutex_);
    for (auto& [name, strategy] : strategies_) {
        if (strategy) {
            strategy->clearOpenOrderTracking();
        }
    }
}

void StrategyManager::initializeAll(DateInt date) {
    std::shared_lock<std::shared_mutex> lock(strategies_mutex_);
    for (auto& [name, strategy] : strategies_) {
        strategy->initialize(date);
    }
}

void StrategyManager::startAll() {
    std::shared_lock<std::shared_mutex> lock(strategies_mutex_);
    for (auto& [name, strategy] : strategies_) {
        strategy->start();
    }
}

void StrategyManager::stopAll() {
    std::shared_lock<std::shared_mutex> lock(strategies_mutex_);
    for (auto& [name, strategy] : strategies_) {
        strategy->stop();
    }
}

void StrategyManager::shutdownAll() {
    std::shared_lock<std::shared_mutex> lock(strategies_mutex_);
    for (auto& [name, strategy] : strategies_) {
        strategy->shutdown();
    }
}

void StrategyManager::eodAll(DateInt date) {
    std::shared_lock<std::shared_mutex> lock(strategies_mutex_);
    for (auto& [name, strategy] : strategies_) {
        strategy->on_eod(date);
    }
}

void StrategyManager::fireTimer(DateInt date, TimeRack time_rack) {
    // Create timer event - strategies will receive via their subscriptions
    TimerEventData timer_data(date, time_rack);
    auto event = std::make_shared<Event>(EventType::TIMER_EVENT, timer_data, EventPriority::HIGH);
    EventManager::getInstance().publish(event);
}

std::set<TimeRack> StrategyManager::getAllTimerRacks() const {
    std::set<TimeRack> all_racks;
    
    std::shared_lock<std::shared_mutex> lock(strategies_mutex_);
    for (const auto& [name, strategy] : strategies_) {
        const auto& racks = strategy->getTimers();
        all_racks.insert(racks.begin(), racks.end());
    }
    
    return all_racks;
}

StrategyManager::AggregateStats StrategyManager::getAggregateStats() const {
    AggregateStats agg;
    
    std::shared_lock<std::shared_mutex> lock(strategies_mutex_);
    for (const auto& [name, strategy] : strategies_) {
        const auto& stats = strategy->getStats();
        agg.total_orders_submitted += stats.orders_submitted;
        agg.total_orders_filled += stats.orders_filled;
        agg.total_orders_rejected += stats.orders_rejected;
        agg.total_pnl += stats.realized_pnl;
        if (strategy->isRunning()) {
            ++agg.active_strategies;
        }
    }
    
    return agg;
}

} // namespace strategy
} // namespace architect
