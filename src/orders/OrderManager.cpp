#include "orders/OrderManager.h"
#include "events/EventManager.h"
#include "api/RestClient.h"
#include "config/Config.h"
#include "core/LatencyTracker.h"
#include "core/Types.h"
#include <algorithm>
#include <chrono>
#include <cstdio>  // snprintf
#include <iostream>

namespace architect {
namespace orders {

OrderManager& OrderManager::getInstance() {
    static OrderManager instance;
    return instance;
}

OrderManager::OrderManager() = default;

OrderId OrderManager::submitOrder(const OrderRequest& request, UserId user_id) {
    // === LATENCY INSTRUMENTATION: Enter OrderManager ===
    auto& lat = core::LatencyTracker::get();
    auto t_enter_om = std::chrono::steady_clock::now();
    auto time_to_om_us = std::chrono::duration_cast<std::chrono::microseconds>(
        t_enter_om - lat.strategy_decision_time).count();
    
    std::cout << "[LATENCY_TRACE] OM_ENTER: time_from_strategy=" << core::latencyDisplayUs(time_to_om_us) << "us" << std::endl;
    
    // Validate request
    auto t_validate_start = std::chrono::steady_clock::now();
    auto validation = OrderValidator::validate(request);
    auto t_validate_end = std::chrono::steady_clock::now();
    auto validate_us = std::chrono::duration_cast<std::chrono::microseconds>(
        t_validate_end - t_validate_start).count();
    
    if (!validation.is_valid) {
        return 0;
    }
    
    // Create order
    auto t_create_start = std::chrono::steady_clock::now();
    auto order = std::make_shared<Order>();
    order->id = generateOrderId();
    order->client_order_id = request.client_order_id.empty() 
        ? generateClientOrderId() 
        : request.client_order_id;
    order->user_id = user_id;
    order->symbol = request.symbol;
    order->side = request.side;
    order->type = request.type;
    order->time_in_force = request.time_in_force;
    order->status = OrderStatus::PENDING;
    order->price = request.price;
    order->stop_price = request.stop_price;
    order->quantity = request.quantity;
    order->remaining_quantity = request.quantity;
    order->post_only = request.post_only;
    order->reduce_only = request.reduce_only;
    order->expire_at = request.expire_at;
    order->created_at = std::chrono::high_resolution_clock::now().time_since_epoch();
    order->updated_at = order->created_at;
    auto t_create_end = std::chrono::steady_clock::now();
    auto create_us = std::chrono::duration_cast<std::chrono::microseconds>(
        t_create_end - t_create_start).count();
    
    // Track this order for latency measurement
    lat.current_order_id = order->id;
    
    // Note: Actual API submission is handled by Platform's event handler
    // Platform subscribes to ORDER_SUBMITTED events and sends to exchange
    // using the correct /orders/place_order endpoint
    
    // Store order
    auto t_store_start = std::chrono::steady_clock::now();
    {
        std::unique_lock<std::shared_mutex> lock(orders_mutex_);
        orders_[order->id] = order;
        client_order_map_[order->client_order_id] = order->id;
        
        std::string symbol_key(order->symbol.data());
        symbol_orders_[order->symbol].push_back(order->id);
        user_orders_[user_id].push_back(order->id);
    }
    auto t_store_end = std::chrono::steady_clock::now();
    auto store_us = std::chrono::duration_cast<std::chrono::microseconds>(
        t_store_end - t_store_start).count();
    
    // Update stats
    auto t_stats_start = std::chrono::steady_clock::now();
    {
        std::lock_guard<std::mutex> lock(stats_mutex_);
        ++stats_.total_orders;
        ++stats_.active_orders;
    }
    auto t_stats_end = std::chrono::steady_clock::now();
    auto stats_us = std::chrono::duration_cast<std::chrono::microseconds>(
        t_stats_end - t_stats_start).count();
    
    std::cout << "[LATENCY_TRACE] OM_PREP: order_id=" << order->id 
              << " validate=" << core::latencyDisplayUs(validate_us) << "us create=" << core::latencyDisplayUs(create_us) 
              << "us store=" << core::latencyDisplayUs(store_us) << "us stats=" << core::latencyDisplayUs(stats_us) << "us" << std::endl;
    
    // Publish event (this triggers the Platform handler which makes HTTP call)
    auto t_publish_start = std::chrono::steady_clock::now();
    publishOrderEvent(events::EventType::ORDER_SUBMITTED, *order);
    auto t_publish_end = std::chrono::steady_clock::now();
    auto publish_us = std::chrono::duration_cast<std::chrono::microseconds>(
        t_publish_end - t_publish_start).count();
    
    std::cout << "[LATENCY_TRACE] OM_PUBLISH: order_id=" << order->id 
              << " event_publish=" << core::latencyDisplayUs(publish_us) << "us (includes HTTP call if sync)" << std::endl;
    
    // Call callback
    if (order_callback_) {
        order_callback_(*order);
    }
    
    auto t_exit_om = std::chrono::steady_clock::now();
    auto total_om_us = std::chrono::duration_cast<std::chrono::microseconds>(
        t_exit_om - t_enter_om).count();
    
    std::cout << "[LATENCY_TRACE] OM_EXIT: order_id=" << order->id 
              << " total_om=" << core::latencyDisplayUs(total_om_us) << "us" << std::endl;
    
    return order->id;
}

OrderId OrderManager::submitOrderNoDispatch(const OrderRequest& request, UserId user_id) {
    // Identical to submitOrder EXCEPT it does not publish ORDER_SUBMITTED (so no inline
    // HTTP place fires). Keep the local bookkeeping byte-for-byte so onOrderAccepted /
    // onOrderRejected behave the same as the single-order path afterwards.
    auto validation = OrderValidator::validate(request);
    if (!validation.is_valid) {
        return 0;
    }

    auto order = std::make_shared<Order>();
    order->id = generateOrderId();
    order->client_order_id = request.client_order_id.empty()
        ? generateClientOrderId()
        : request.client_order_id;
    order->user_id = user_id;
    order->symbol = request.symbol;
    order->side = request.side;
    order->type = request.type;
    order->time_in_force = request.time_in_force;
    order->status = OrderStatus::PENDING;
    order->price = request.price;
    order->stop_price = request.stop_price;
    order->quantity = request.quantity;
    order->remaining_quantity = request.quantity;
    order->post_only = request.post_only;
    order->reduce_only = request.reduce_only;
    order->expire_at = request.expire_at;
    order->created_at = std::chrono::high_resolution_clock::now().time_since_epoch();
    order->updated_at = order->created_at;

    {
        std::unique_lock<std::shared_mutex> lock(orders_mutex_);
        orders_[order->id] = order;
        client_order_map_[order->client_order_id] = order->id;
        symbol_orders_[order->symbol].push_back(order->id);
        user_orders_[user_id].push_back(order->id);
    }

    {
        std::lock_guard<std::mutex> lock(stats_mutex_);
        ++stats_.total_orders;
        ++stats_.active_orders;
    }

    // NOTE: intentionally NO publishOrderEvent(ORDER_SUBMITTED) — the caller drives the
    // wire (batch place) and dispatches the accept. Keep the order callback for parity
    // with submitOrder so any monitoring subscriber sees the new order.
    if (order_callback_) {
        order_callback_(*order);
    }

    return order->id;
}

bool OrderManager::modifyOrder(const OrderModifyRequest& request) {
    auto validation = OrderValidator::validateModify(request);
    if (!validation.is_valid) {
        return false;
    }
    
    std::unique_lock<std::shared_mutex> lock(orders_mutex_);
    
    OrderId order_id = request.order_id;
    if (order_id == 0) {
        auto it = client_order_map_.find(request.client_order_id);
        if (it == client_order_map_.end()) {
            return false;
        }
        order_id = it->second;
    }
    
    auto order_it = orders_.find(order_id);
    if (order_it == orders_.end()) {
        return false;
    }
    
    std::shared_ptr<Order> order = order_it->second;
    if (!order->isActive()) {
        return false;
    }
    
    if (request.new_price.has_value()) {
        order->price = request.new_price.value();
    }
    if (request.new_quantity.has_value()) {
        order->quantity = request.new_quantity.value();
        order->remaining_quantity = order->quantity - order->filled_quantity;
    }
    order->updated_at = std::chrono::high_resolution_clock::now().time_since_epoch();
    
    lock.unlock();
    
    publishOrderEvent(events::EventType::ORDER_MODIFIED, *order);
    
    if (order_callback_) {
        order_callback_(*order);
    }
    
    return true;
}

bool OrderManager::cancelOrder(const OrderCancelRequest& request) {
    auto validation = OrderValidator::validateCancel(request);
    if (!validation.is_valid) {
        return false;
    }
    
    std::unique_lock<std::shared_mutex> lock(orders_mutex_);
    
    OrderId order_id = request.order_id;
    if (order_id == 0) {
        auto it = client_order_map_.find(request.client_order_id);
        if (it == client_order_map_.end()) {
            return false;
        }
        order_id = it->second;
    }
    
    auto order_it = orders_.find(order_id);
    if (order_it == orders_.end()) {
        return false;
    }
    
    std::shared_ptr<Order> order = order_it->second;
    if (!order->isActive()) {
        return false;
    }
    
    order->status = OrderStatus::CANCELLED;
    order->updated_at = std::chrono::high_resolution_clock::now().time_since_epoch();
    
    lock.unlock();
    
    {
        std::lock_guard<std::mutex> lock2(stats_mutex_);
        if (stats_.active_orders > 0) {
            --stats_.active_orders;
        }
        ++stats_.cancelled_orders;
    }
    
    publishOrderEvent(events::EventType::ORDER_CANCELLED, *order);
    
    if (order_callback_) {
        order_callback_(*order);
    }
    
    return true;
}

std::size_t OrderManager::cancelAllOrders(const OrderCancelAllRequest& request, UserId user_id) {
    std::vector<OrderId> to_cancel;
    
    {
        std::shared_lock<std::shared_mutex> lock(orders_mutex_);
        
        auto user_it = user_orders_.find(user_id);
        if (user_it == user_orders_.end()) {
            return 0;
        }
        
        for (OrderId id : user_it->second) {
            auto order_it = orders_.find(id);
            if (order_it != orders_.end() && order_it->second->isActive()) {
                // Apply filters
                if (request.symbol.has_value()) {
                    if (order_it->second->symbol != request.symbol.value()) {
                        continue;
                    }
                }
                if (request.side.has_value()) {
                    if (order_it->second->side != request.side.value()) {
                        continue;
                    }
                }
                to_cancel.push_back(id);
            }
        }
    }
    
    std::size_t cancelled = 0;
    for (OrderId id : to_cancel) {
        OrderCancelRequest cancel_req;
        cancel_req.order_id = id;
        if (cancelOrder(cancel_req)) {
            ++cancelled;
        }
    }
    
    return cancelled;
}

void OrderManager::onOrderAccepted(OrderId order_id, const std::string& exchange_order_id) {
    std::unique_lock<std::shared_mutex> lock(orders_mutex_);
    
    auto it = orders_.find(order_id);
    if (it == orders_.end()) return;
    
    std::shared_ptr<Order> order = it->second;
    order->status = OrderStatus::ACCEPTED;
    order->exchange_order_id = exchange_order_id;
    order->updated_at = std::chrono::high_resolution_clock::now().time_since_epoch();
    
    // Add to exchange order ID index for O(1) lookup
    if (!exchange_order_id.empty()) {
        exchange_order_map_[exchange_order_id] = order_id;
    }
    
    lock.unlock();
    
    publishOrderEvent(events::EventType::ORDER_ACCEPTED, *order);
    
    if (order_callback_) {
        order_callback_(*order);
    }
}

void OrderManager::onOrderRejected(OrderId order_id, ErrorCode code, const std::string& message) {
    std::unique_lock<std::shared_mutex> lock(orders_mutex_);
    
    auto it = orders_.find(order_id);
    if (it == orders_.end()) return;
    
    std::shared_ptr<Order> order = it->second;
    order->status = OrderStatus::REJECTED;
    order->error_code = code;
    order->error_message = message;
    order->updated_at = std::chrono::high_resolution_clock::now().time_since_epoch();
    
    lock.unlock();
    
    {
        std::lock_guard<std::mutex> lock2(stats_mutex_);
        if (stats_.active_orders > 0) {
            --stats_.active_orders;
        }
        ++stats_.rejected_orders;
    }
    
    publishOrderEvent(events::EventType::ORDER_REJECTED, *order);
    
    if (order_callback_) {
        order_callback_(*order);
    }
}

void OrderManager::onOrderFilled(OrderId order_id, const Fill& fill) {
    std::unique_lock<std::shared_mutex> lock(orders_mutex_);
    
    auto it = orders_.find(order_id);
    if (it == orders_.end()) return;
    
    std::shared_ptr<Order> order = it->second;
    
    // Track if this is the first fill that transitions from active
    bool was_active = order->isActive();
    
    // Update average fill price
    Decimal old_value = order->avg_fill_price * order->filled_quantity;
    Decimal new_value = fill.price * fill.quantity;
    order->filled_quantity += fill.quantity;
    order->avg_fill_price = (old_value + new_value) / order->filled_quantity;
    order->remaining_quantity = order->quantity - order->filled_quantity;
    order->fee += fill.fee;
    
    // Determine if fully or partially filled
    bool fully_filled = (order->remaining_quantity <= 0);
    order->status = fully_filled ? OrderStatus::FILLED : OrderStatus::PARTIALLY_FILLED;
    order->filled_at = fill.executed_at;
    order->updated_at = std::chrono::high_resolution_clock::now().time_since_epoch();
    
    lock.unlock();
    
    // Store fill
    {
        std::unique_lock<std::shared_mutex> fill_lock(fills_mutex_);
        order_fills_[order_id].push_back(fill);
    }
    
    {
        std::lock_guard<std::mutex> lock2(stats_mutex_);
        // Only decrement active_orders when transitioning from active to filled
        if (was_active && fully_filled && stats_.active_orders > 0) {
            --stats_.active_orders;
            ++stats_.filled_orders;
        }
        ++stats_.total_fills;
        stats_.total_volume += fill.quantity;
        stats_.total_fees += fill.fee;
    }
    
    auto event_type = fully_filled ? events::EventType::ORDER_FILLED 
                                   : events::EventType::ORDER_PARTIALLY_FILLED;
    publishOrderEvent(event_type, *order);
    
    if (order_callback_) {
        order_callback_(*order);
    }
    if (fill_callback_) {
        fill_callback_(fill);
    }
}

void OrderManager::onOrderPartiallyFilled(OrderId order_id, const Fill& fill) {
    std::unique_lock<std::shared_mutex> lock(orders_mutex_);
    
    auto it = orders_.find(order_id);
    if (it == orders_.end()) return;
    
    std::shared_ptr<Order> order = it->second;
    
    // Update average fill price
    Decimal old_value = order->avg_fill_price * order->filled_quantity;
    Decimal new_value = fill.price * fill.quantity;
    order->filled_quantity += fill.quantity;
    order->avg_fill_price = (old_value + new_value) / order->filled_quantity;
    order->remaining_quantity = order->quantity - order->filled_quantity;
    order->fee += fill.fee;
    order->status = OrderStatus::PARTIALLY_FILLED;
    order->updated_at = std::chrono::high_resolution_clock::now().time_since_epoch();
    
    lock.unlock();
    
    // Store fill
    {
        std::unique_lock<std::shared_mutex> fill_lock(fills_mutex_);
        order_fills_[order_id].push_back(fill);
    }
    
    {
        std::lock_guard<std::mutex> lock2(stats_mutex_);
        ++stats_.total_fills;
        stats_.total_volume += fill.quantity;
        stats_.total_fees += fill.fee;
    }
    
    publishOrderEvent(events::EventType::ORDER_PARTIALLY_FILLED, *order);
    
    if (order_callback_) {
        order_callback_(*order);
    }
    if (fill_callback_) {
        fill_callback_(fill);
    }
}

void OrderManager::onOrderCancelled(OrderId order_id) {
    std::unique_lock<std::shared_mutex> lock(orders_mutex_);
    
    auto it = orders_.find(order_id);
    if (it == orders_.end()) return;
    
    std::shared_ptr<Order> order = it->second;
    // Idempotent: reconcile may race with a normal cancel path or duplicate detection.
    if (!order->isActive()) {
        return;
    }
    order->status = OrderStatus::CANCELLED;
    order->updated_at = std::chrono::high_resolution_clock::now().time_since_epoch();
    
    lock.unlock();
    
    {
        std::lock_guard<std::mutex> lock2(stats_mutex_);
        if (stats_.active_orders > 0) {
            --stats_.active_orders;
        }
        ++stats_.cancelled_orders;
    }
    
    // venue_confirmed=true: this is a NOTIFICATION that the venue already cancelled
    // the order (reconcile sync / batch cancel-replace whose HTTP cancel already
    // fired). The Platform ORDER_CANCELLED handler must NOT re-send the wire cancel.
    publishOrderEvent(events::EventType::ORDER_CANCELLED, *order, /*venue_confirmed=*/true);
    
    if (order_callback_) {
        order_callback_(*order);
    }
}

void OrderManager::onOrderExpired(OrderId order_id) {
    std::unique_lock<std::shared_mutex> lock(orders_mutex_);
    
    auto it = orders_.find(order_id);
    if (it == orders_.end()) return;
    
    std::shared_ptr<Order> order = it->second;
    order->status = OrderStatus::EXPIRED;
    order->updated_at = std::chrono::high_resolution_clock::now().time_since_epoch();
    
    lock.unlock();
    
    {
        std::lock_guard<std::mutex> lock2(stats_mutex_);
        if (stats_.active_orders > 0) {
            --stats_.active_orders;
        }
    }
    
    publishOrderEvent(events::EventType::ORDER_EXPIRED, *order);
    
    if (order_callback_) {
        order_callback_(*order);
    }
}

void OrderManager::onOrderModified(OrderId order_id, Price new_price, Quantity new_quantity) {
    std::unique_lock<std::shared_mutex> lock(orders_mutex_);
    
    auto it = orders_.find(order_id);
    if (it == orders_.end()) return;
    
    std::shared_ptr<Order> order = it->second;
    order->price = new_price;
    order->quantity = new_quantity;
    order->remaining_quantity = new_quantity - order->filled_quantity;
    order->updated_at = std::chrono::high_resolution_clock::now().time_since_epoch();
    
    lock.unlock();
    
    publishOrderEvent(events::EventType::ORDER_MODIFIED, *order);
    
    if (order_callback_) {
        order_callback_(*order);
    }
}

std::shared_ptr<Order> OrderManager::getOrder(OrderId order_id) const {
    std::shared_lock<std::shared_mutex> lock(orders_mutex_);
    auto it = orders_.find(order_id);
    if (it == orders_.end()) return nullptr;
    return it->second;
}

std::shared_ptr<Order> OrderManager::getOrderByClientId(const std::string& client_order_id) const {
    std::shared_lock<std::shared_mutex> lock(orders_mutex_);
    auto it = client_order_map_.find(client_order_id);
    if (it == client_order_map_.end()) return nullptr;
    auto order_it = orders_.find(it->second);
    if (order_it == orders_.end()) return nullptr;
    return order_it->second;
}

std::shared_ptr<Order> OrderManager::getOrderByExchangeId(const std::string& exchange_order_id) const {
    std::shared_lock<std::shared_mutex> lock(orders_mutex_);
    // O(1) lookup using exchange order ID index
    auto it = exchange_order_map_.find(exchange_order_id);
    if (it == exchange_order_map_.end()) return nullptr;
    auto order_it = orders_.find(it->second);
    if (order_it == orders_.end()) return nullptr;
    return order_it->second;
}

std::vector<std::shared_ptr<Order>> OrderManager::getOrdersByUser(UserId user_id) const {
    std::shared_lock<std::shared_mutex> lock(orders_mutex_);
    std::vector<std::shared_ptr<Order>> result;
    
    auto it = user_orders_.find(user_id);
    if (it != user_orders_.end()) {
        for (OrderId id : it->second) {
            auto order_it = orders_.find(id);
            if (order_it != orders_.end()) {
                result.push_back(order_it->second);
            }
        }
    }
    
    return result;
}

std::vector<std::shared_ptr<Order>> OrderManager::getOrdersBySymbol(const Symbol& symbol) const {
    std::shared_lock<std::shared_mutex> lock(orders_mutex_);
    std::vector<std::shared_ptr<Order>> result;
    
    auto it = symbol_orders_.find(symbol);
    if (it != symbol_orders_.end()) {
        for (OrderId id : it->second) {
            auto order_it = orders_.find(id);
            if (order_it != orders_.end()) {
                result.push_back(order_it->second);
            }
        }
    }
    
    return result;
}

std::vector<std::shared_ptr<Order>> OrderManager::getActiveOrders() const {
    std::shared_lock<std::shared_mutex> lock(orders_mutex_);
    std::vector<std::shared_ptr<Order>> result;
    
    for (const auto& [id, order] : orders_) {
        if (order->isActive()) {
            result.push_back(order);
        }
    }
    
    return result;
}

std::vector<std::shared_ptr<Order>> OrderManager::getActiveOrdersByUser(UserId user_id) const {
    std::shared_lock<std::shared_mutex> lock(orders_mutex_);
    std::vector<std::shared_ptr<Order>> result;
    
    auto it = user_orders_.find(user_id);
    if (it != user_orders_.end()) {
        for (OrderId id : it->second) {
            auto order_it = orders_.find(id);
            if (order_it != orders_.end() && order_it->second->isActive()) {
                result.push_back(order_it->second);
            }
        }
    }
    
    return result;
}

std::vector<Fill> OrderManager::getFills(OrderId order_id) const {
    std::shared_lock<std::shared_mutex> lock(fills_mutex_);
    auto it = order_fills_.find(order_id);
    if (it == order_fills_.end()) return {};
    return it->second;
}

std::vector<Fill> OrderManager::getFillsByUser(UserId user_id) const {
    std::vector<Fill> result;
    
    std::vector<OrderId> order_ids;
    {
        std::shared_lock<std::shared_mutex> lock(orders_mutex_);
        auto it = user_orders_.find(user_id);
        if (it != user_orders_.end()) {
            order_ids = it->second;
        }
    }
    
    {
        std::shared_lock<std::shared_mutex> lock(fills_mutex_);
        for (OrderId id : order_ids) {
            auto it = order_fills_.find(id);
            if (it != order_fills_.end()) {
                result.insert(result.end(), it->second.begin(), it->second.end());
            }
        }
    }
    
    return result;
}

void OrderManager::setOrderCallback(OrderCallback callback) {
    order_callback_ = std::move(callback);
}

void OrderManager::setFillCallback(FillCallback callback) {
    fill_callback_ = std::move(callback);
}

OrderManager::Stats OrderManager::getStats() const {
    std::lock_guard<std::mutex> lock(stats_mutex_);
    return stats_;
}

void OrderManager::resetStats() {
    std::lock_guard<std::mutex> lock(stats_mutex_);
    stats_ = Stats{};
}

std::size_t OrderManager::purgeOldOrders(std::chrono::seconds max_age) {
    auto cutoff = std::chrono::high_resolution_clock::now().time_since_epoch() 
                  - std::chrono::duration_cast<Timestamp>(max_age);
    
    std::vector<OrderId> to_remove;
    
    {
        std::shared_lock<std::shared_mutex> lock(orders_mutex_);
        for (const auto& [id, order] : orders_) {
            if (order->isTerminal() && order->updated_at < cutoff) {
                to_remove.push_back(id);
            }
        }
    }
    
    {
        std::unique_lock<std::shared_mutex> lock(orders_mutex_);
        for (OrderId id : to_remove) {
            auto it = orders_.find(id);
            if (it != orders_.end()) {
                client_order_map_.erase(it->second->client_order_id);
                if (!it->second->exchange_order_id.empty()) {
                    exchange_order_map_.erase(it->second->exchange_order_id);
                }
                orders_.erase(it);
            }
        }
    }
    
    {
        std::unique_lock<std::shared_mutex> lock(fills_mutex_);
        for (OrderId id : to_remove) {
            order_fills_.erase(id);
        }
    }
    
    return to_remove.size();
}

void OrderManager::clearAll() {
    {
        std::unique_lock<std::shared_mutex> lock(orders_mutex_);
        orders_.clear();
        client_order_map_.clear();
        exchange_order_map_.clear();  // Clear exchange ID index
        symbol_orders_.clear();
        user_orders_.clear();
    }
    
    {
        std::unique_lock<std::shared_mutex> lock(fills_mutex_);
        order_fills_.clear();
    }
    
    resetStats();
}

void OrderManager::detachLocalOrderById(OrderId id) {
    if (id == 0) {
        return;
    }
    std::size_t active_removed = 0;
    {
        std::unique_lock<std::shared_mutex> lock(orders_mutex_);
        auto it = orders_.find(id);
        if (it == orders_.end() || !it->second) {
            return;
        }
        auto& o = it->second;
        if (o->isActive()) {
            ++active_removed;
        }
        client_order_map_.erase(o->client_order_id);
        if (!o->exchange_order_id.empty()) {
            exchange_order_map_.erase(o->exchange_order_id);
        }
        const UserId uid = o->user_id;
        auto ui = user_orders_.find(uid);
        if (ui != user_orders_.end()) {
            auto& vec = ui->second;
            vec.erase(std::remove(vec.begin(), vec.end(), id), vec.end());
        }
        const Symbol sym = o->symbol;
        auto si = symbol_orders_.find(sym);
        if (si != symbol_orders_.end()) {
            auto& vec = si->second;
            vec.erase(std::remove(vec.begin(), vec.end(), id), vec.end());
        }
        orders_.erase(it);
    }
    if (active_removed > 0) {
        std::lock_guard<std::mutex> s(stats_mutex_);
        if (stats_.active_orders >= active_removed) {
            stats_.active_orders -= active_removed;
        } else {
            stats_.active_orders = 0;
        }
    }
    {
        std::unique_lock<std::shared_mutex> lock(fills_mutex_);
        order_fills_.erase(id);
    }
}

void OrderManager::purgeNonTerminalOrdersForSymbol(const std::string& symbol) {
    std::vector<OrderId> to_remove;
    {
        std::shared_lock<std::shared_mutex> lock(orders_mutex_);
        for (const auto& [id, order] : orders_) {
            if (!order) {
                continue;
            }
            if (std::string(order->symbol.data()) != symbol) {
                continue;
            }
            if (!order->isTerminal()) {
                to_remove.push_back(id);
            }
        }
    }

    std::size_t active_removed = 0;
    {
        std::unique_lock<std::shared_mutex> lock(orders_mutex_);
        for (OrderId id : to_remove) {
            auto it = orders_.find(id);
            if (it == orders_.end() || !it->second) {
                continue;
            }
            auto& o = it->second;
            if (std::string(o->symbol.data()) != symbol || o->isTerminal()) {
                continue;
            }
            if (o->isActive()) {
                ++active_removed;
            }
            client_order_map_.erase(o->client_order_id);
            if (!o->exchange_order_id.empty()) {
                exchange_order_map_.erase(o->exchange_order_id);
            }
            const UserId uid = o->user_id;
            auto ui = user_orders_.find(uid);
            if (ui != user_orders_.end()) {
                auto& vec = ui->second;
                vec.erase(std::remove(vec.begin(), vec.end(), id), vec.end());
            }
            const Symbol sym = makeSymbol(symbol);
            auto si = symbol_orders_.find(sym);
            if (si != symbol_orders_.end()) {
                auto& vec = si->second;
                vec.erase(std::remove(vec.begin(), vec.end(), id), vec.end());
            }
            orders_.erase(it);
        }
    }

    if (active_removed > 0) {
        std::lock_guard<std::mutex> s(stats_mutex_);
        if (stats_.active_orders >= active_removed) {
            stats_.active_orders -= active_removed;
        } else {
            stats_.active_orders = 0;
        }
    }

    {
        std::unique_lock<std::shared_mutex> lock(fills_mutex_);
        for (OrderId id : to_remove) {
            order_fills_.erase(id);
        }
    }
}

OrderId OrderManager::adoptExternalLimitOrder(UserId user_id,
                                                const std::string& symbol,
                                                Side side,
                                                Price price,
                                                Quantity order_qty,
                                                Quantity remaining_qty,
                                                const std::string& exchange_order_id,
                                                const std::string& client_order_id) {
    if (exchange_order_id.empty() || order_qty <= 0 || remaining_qty < 0) {
        return 0;
    }
    {
        std::shared_lock<std::shared_mutex> lock(orders_mutex_);
        auto ex = exchange_order_map_.find(exchange_order_id);
        if (ex != exchange_order_map_.end()) {
            return ex->second;
        }
    }

    auto order = std::make_shared<Order>();
    order->id = generateOrderId();
    order->client_order_id =
        client_order_id.empty() ? (std::string("desk-adopt-") + exchange_order_id) : client_order_id;
    order->user_id = user_id;
    order->symbol = makeSymbol(symbol);
    order->side = side;
    order->type = OrderType::LIMIT;
    order->time_in_force = TimeInForce::GTC;
    order->status = OrderStatus::ACCEPTED;
    order->price = price;
    order->quantity = order_qty;
    const Quantity filled = std::max(0.0, order_qty - remaining_qty);
    order->filled_quantity = filled;
    order->remaining_quantity = remaining_qty;
    order->exchange_order_id = exchange_order_id;
    const auto now = std::chrono::high_resolution_clock::now().time_since_epoch();
    order->created_at = now;
    order->updated_at = now;

    {
        std::unique_lock<std::shared_mutex> lock(orders_mutex_);
        if (exchange_order_map_.count(exchange_order_id)) {
            return exchange_order_map_.at(exchange_order_id);
        }
        if (client_order_map_.count(order->client_order_id)) {
            order->client_order_id += "-" + std::to_string(order->id);
        }
        orders_[order->id] = order;
        client_order_map_[order->client_order_id] = order->id;
        exchange_order_map_[exchange_order_id] = order->id;
        symbol_orders_[order->symbol].push_back(order->id);
        user_orders_[user_id].push_back(order->id);
    }

    {
        std::lock_guard<std::mutex> s(stats_mutex_);
        ++stats_.total_orders;
        ++stats_.active_orders;
    }

    return order->id;
}

OrderId OrderManager::generateOrderId() {
    return next_order_id_.fetch_add(1);
}

std::string OrderManager::generateClientOrderId() {
    auto now = std::chrono::system_clock::now();
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();
    auto counter = client_order_counter_.fetch_add(1, std::memory_order_relaxed);
    
    // Fast path: use snprintf instead of ostringstream (avoids heap allocation)
    char buffer[64];
    int len = snprintf(buffer, sizeof(buffer), "ORD-%llx-%llu", 
                       static_cast<unsigned long long>(ms), 
                       static_cast<unsigned long long>(counter));
    return std::string(buffer, len > 0 ? len : 0);
}

void OrderManager::publishOrderEvent(events::EventType type, const Order& order,
                                     bool venue_confirmed) {
    events::OrderEventData data;
    data.venue_confirmed = venue_confirmed;
    data.order_id = order.id;
    data.user_id = order.user_id;
    data.symbol = order.symbol;
    data.side = order.side;
    data.order_type = order.type;
    data.status = order.status;
    data.price = order.price;
    data.quantity = order.quantity;
    data.filled_quantity = order.filled_quantity;
    data.avg_fill_price = order.avg_fill_price;
    data.error_code = order.error_code;
    data.error_message = order.error_message;
    data.client_order_id = order.client_order_id;
    data.order_timestamp = order.created_at;
    
    auto event = events::EventFactory::createOrderEvent(type, data);
    
    // Use synchronous dispatch for critical order events to minimize latency
    // These events trigger immediate actions (API calls, strategy updates).
    // ORDER_CANCELLED / ORDER_MODIFIED must be sync too: async queue reorders them behind
    // other work so REST cancel runs after replacements — stale oids → "not found" / "cannot cancel".
    if (type == events::EventType::ORDER_SUBMITTED ||
        type == events::EventType::ORDER_ACCEPTED ||
        type == events::EventType::ORDER_FILLED ||
        type == events::EventType::ORDER_REJECTED ||
        type == events::EventType::ORDER_CANCELLED ||
        type == events::EventType::ORDER_MODIFIED) {
        events::EventManager::getInstance().publishSync(event);
    } else {
        events::EventManager::getInstance().publish(event);
    }
}

} // namespace orders
} // namespace architect
