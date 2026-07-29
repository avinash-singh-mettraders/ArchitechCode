#pragma once

/**
 * @file OrderManager.h
 * @brief High-performance order management system
 * 
 * Optimizations:
 * - Object pool for Order allocation (zero heap allocation per order)
 * - Hash indices for O(1) lookup by exchange_order_id
 * - Cache-line aligned statistics
 * - Lock-free counters where possible
 */

#include "orders/Order.h"
#include "events/Event.h"
#include "utils/Hash.h"
#include <unordered_map>
#include <vector>
#include <mutex>
#include <shared_mutex>
#include <memory>
#include <functional>
#include <atomic>
#include <array>

namespace architect {
namespace orders {

using OrderCallback = std::function<void(const Order&)>;
using FillCallback = std::function<void(const Fill&)>;

// Forward declare for friend
class OrderPool;

/**
 * @brief Thread-safe order management system
 * 
 * Features:
 * - Fast order lookup by ID, client ID, exchange ID, and symbol
 * - Order lifecycle management with object pooling
 * - Fill tracking with pre-allocated buffers
 * - Lock-free statistics and monitoring
 */
class OrderManager {
public:
    /**
     * @brief Get the singleton instance
     */
    static OrderManager& getInstance();
    
    // Prevent copying
    OrderManager(const OrderManager&) = delete;
    OrderManager& operator=(const OrderManager&) = delete;
    
    // ==========================================================================
    // Order Operations
    // ==========================================================================
    
    /**
     * @brief Submit a new order
     * @return Order ID (0 if failed)
     */
    OrderId submitOrder(const OrderRequest& request, UserId user_id);

    /**
     * @brief Create + register a new order EXACTLY like submitOrder, but do NOT publish
     * the ORDER_SUBMITTED event — so the synchronous Platform place handler (which does
     * the blocking HTTP place + inline ORDER_ACCEPTED) does NOT fire.
     *
     * This is the async/batch place seam: the caller creates N orders with this, then
     * drives the wire itself (e.g. Platform::placeOrdersConcurrent fires them in parallel)
     * and calls onOrderAccepted / onOrderRejected per order. Local bookkeeping (indices,
     * stats, order callback) is identical to submitOrder so the two paths are
     * interchangeable up to the wire dispatch. Returns Order ID (0 if failed).
     */
    OrderId submitOrderNoDispatch(const OrderRequest& request, UserId user_id);
    
    /**
     * @brief Modify an existing order
     */
    bool modifyOrder(const OrderModifyRequest& request);
    
    /**
     * @brief Cancel an order
     */
    bool cancelOrder(const OrderCancelRequest& request);
    
    /**
     * @brief Cancel all orders
     */
    std::size_t cancelAllOrders(const OrderCancelAllRequest& request, UserId user_id);

    /** Remove non-terminal orders for symbol from local maps only (no HTTP). For MM desk / C++ resync. */
    void purgeNonTerminalOrdersForSymbol(const std::string& symbol);

    /**
     * Remove one order from local indices only (no venue HTTP). Used when rebinding a single desk stack
     * so sibling mm_req_* stacks on the same AX symbol keep their adopted rows in OrderManager.
     */
    void detachLocalOrderById(OrderId id);

    /**
     * Register an existing exchange limit order in OrderManager so fill polling matches exchange_order_id.
     * Does not publish events or call the venue.
     */
    OrderId adoptExternalLimitOrder(UserId user_id,
                                    const std::string& symbol,
                                    Side side,
                                    Price price,
                                    Quantity order_qty,
                                    Quantity remaining_qty,
                                    const std::string& exchange_order_id,
                                    const std::string& client_order_id);
    
    // ==========================================================================
    // Order Updates (from exchange)
    // ==========================================================================
    
    /**
     * @brief Update order status (called when exchange confirms)
     */
    void onOrderAccepted(OrderId order_id, const std::string& exchange_order_id);
    void onOrderRejected(OrderId order_id, ErrorCode code, const std::string& message);
    void onOrderFilled(OrderId order_id, const Fill& fill);
    void onOrderPartiallyFilled(OrderId order_id, const Fill& fill);
    void onOrderCancelled(OrderId order_id);
    void onOrderExpired(OrderId order_id);
    void onOrderModified(OrderId order_id, Price new_price, Quantity new_quantity);
    
    // ==========================================================================
    // Queries
    // ==========================================================================
    
    /**
     * @brief Get order by ID
     */
    std::shared_ptr<Order> getOrder(OrderId order_id) const;
    
    /**
     * @brief Get order by client order ID
     */
    std::shared_ptr<Order> getOrderByClientId(const std::string& client_order_id) const;
    
    /**
     * @brief Get order by exchange order ID
     */
    std::shared_ptr<Order> getOrderByExchangeId(const std::string& exchange_order_id) const;
    
    /**
     * @brief Get all orders for a user
     */
    std::vector<std::shared_ptr<Order>> getOrdersByUser(UserId user_id) const;
    
    /**
     * @brief Get all orders for a symbol
     */
    std::vector<std::shared_ptr<Order>> getOrdersBySymbol(const Symbol& symbol) const;
    
    /**
     * @brief Get all active orders
     */
    std::vector<std::shared_ptr<Order>> getActiveOrders() const;
    
    /**
     * @brief Get all active orders for a user
     */
    std::vector<std::shared_ptr<Order>> getActiveOrdersByUser(UserId user_id) const;
    
    /**
     * @brief Get fills for an order
     */
    std::vector<Fill> getFills(OrderId order_id) const;
    
    /**
     * @brief Get all fills for a user
     */
    std::vector<Fill> getFillsByUser(UserId user_id) const;
    
    // ==========================================================================
    // Callbacks
    // ==========================================================================
    
    void setOrderCallback(OrderCallback callback);
    void setFillCallback(FillCallback callback);
    
    // ==========================================================================
    // Statistics (cache-line aligned for performance)
    // ==========================================================================
    
    struct alignas(64) Stats {
        std::uint64_t total_orders          = 0;
        std::uint64_t active_orders         = 0;
        std::uint64_t filled_orders         = 0;
        std::uint64_t cancelled_orders      = 0;
        std::uint64_t rejected_orders       = 0;
        std::uint64_t total_fills           = 0;
        Decimal       total_volume          = 0.0;
        Decimal       total_fees            = 0.0;
        char          _padding[8];          // Ensure 64-byte alignment
    };
    
    [[nodiscard]] Stats getStats() const;
    void resetStats();
    
    // ==========================================================================
    // Maintenance
    // ==========================================================================
    
    /**
     * @brief Remove completed orders older than specified age
     */
    std::size_t purgeOldOrders(std::chrono::seconds max_age);
    
    /**
     * @brief Clear all orders (use with caution)
     */
    void clearAll();
    
private:
    OrderManager();
    ~OrderManager() = default;
    
    OrderId generateOrderId();
    std::string generateClientOrderId();
    // venue_confirmed=true marks an ORDER_CANCELLED as a "venue already did it"
    // notification so the Platform wire-cancel handler skips the redundant HTTP
    // cancel (prevents the cancels=4 double-fire). Ignored for non-cancel events.
    void publishOrderEvent(events::EventType type, const Order& order,
                           bool venue_confirmed = false);
    void updateOrderStatus(OrderId order_id, OrderStatus new_status);
    
    // === Order storage (with multiple indices for O(1) lookup) ===
    std::unordered_map<OrderId, std::shared_ptr<Order>> orders_;
    std::unordered_map<std::string, OrderId> client_order_map_;
    std::unordered_map<std::string, OrderId> exchange_order_map_;  // O(1) exchange ID lookup
    std::unordered_map<Symbol, std::vector<OrderId>, utils::SymbolHash, utils::SymbolEqual> symbol_orders_;
    std::unordered_map<UserId, std::vector<OrderId>> user_orders_;
    
    // Fill storage with pre-reserved capacity
    std::unordered_map<OrderId, std::vector<Fill>> order_fills_;
    
    mutable std::shared_mutex orders_mutex_;
    mutable std::shared_mutex fills_mutex_;
    
    // ID generation (atomic for lock-free increment)
    alignas(64) std::atomic<OrderId> next_order_id_{1};
    alignas(64) std::atomic<std::uint64_t> client_order_counter_{0};
    
    // Callbacks
    OrderCallback order_callback_;
    FillCallback fill_callback_;
    
    // Statistics (separate cache line to avoid false sharing)
    alignas(64) mutable std::mutex stats_mutex_;
    Stats stats_;
};

} // namespace orders
} // namespace architect
