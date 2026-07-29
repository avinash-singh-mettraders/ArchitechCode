#pragma once

/**
 * @file Order.h
 * @brief Order data structures and utilities
 */

#include "core/Types.h"
#include <string>
#include <chrono>
#include <optional>

namespace architect {
namespace orders {

using namespace core;

/**
 * @brief Complete order representation
 */
struct Order {
    // Identifiers
    OrderId             id              = 0;
    std::string         client_order_id;
    std::string         exchange_order_id;
    UserId              user_id         = 0;
    
    // Instrument
    Symbol              symbol;
    
    // Order details
    Side                side            = Side::BUY;
    OrderType           type            = OrderType::LIMIT;
    TimeInForce         time_in_force   = TimeInForce::GTC;
    OrderStatus         status          = OrderStatus::PENDING;
    
    // Pricing
    Price               price           = 0.0;
    Price               stop_price      = 0.0;
    Price               avg_fill_price  = 0.0;
    
    // Quantity
    Quantity            quantity        = 0.0;
    Quantity            filled_quantity = 0.0;
    Quantity            remaining_quantity = 0.0;
    
    // Fees
    Decimal             fee             = 0.0;
    std::string         fee_currency;
    
    // Timestamps
    Timestamp           created_at;
    Timestamp           updated_at;
    Timestamp           filled_at;
    std::optional<Timestamp> expire_at;
    
    // Post-only / reduce-only flags
    bool                post_only       = false;
    bool                reduce_only     = false;
    bool                hidden          = false;
    
    // Error info
    ErrorCode           error_code      = ErrorCode::SUCCESS;
    std::string         error_message;
    
    // ==========================================================================
    // Computed Properties
    // ==========================================================================
    
    [[nodiscard]] bool isFilled() const noexcept {
        return status == OrderStatus::FILLED;
    }
    
    [[nodiscard]] bool isPartiallyFilled() const noexcept {
        return status == OrderStatus::PARTIALLY_FILLED;
    }
    
    [[nodiscard]] bool isCancelled() const noexcept {
        return status == OrderStatus::CANCELLED;
    }
    
    [[nodiscard]] bool isActive() const noexcept {
        return status == OrderStatus::NEW || 
               status == OrderStatus::ACCEPTED ||
               status == OrderStatus::PARTIALLY_FILLED;
    }
    
    [[nodiscard]] bool isTerminal() const noexcept {
        return status == OrderStatus::FILLED ||
               status == OrderStatus::CANCELLED ||
               status == OrderStatus::REJECTED ||
               status == OrderStatus::EXPIRED;
    }
    
    [[nodiscard]] Quantity getRemainingQuantity() const noexcept {
        return quantity - filled_quantity;
    }
    
    [[nodiscard]] Decimal getNotionalValue() const noexcept {
        return price * quantity;
    }
    
    [[nodiscard]] Decimal getFilledValue() const noexcept {
        return avg_fill_price * filled_quantity;
    }
    
    [[nodiscard]] double getFillPercentage() const noexcept {
        if (quantity <= 0) return 0.0;
        return (filled_quantity / quantity) * 100.0;
    }
};

/**
 * @brief Order request for creating new orders
 */
struct OrderRequest {
    std::string         client_order_id;
    Symbol              symbol;
    Side                side            = Side::BUY;
    OrderType           type            = OrderType::LIMIT;
    TimeInForce         time_in_force   = TimeInForce::GTC;
    Price               price           = 0.0;
    Price               stop_price      = 0.0;
    Quantity            quantity        = 0.0;
    bool                post_only       = false;
    bool                reduce_only     = false;
    std::optional<Timestamp> expire_at;
    
    // Validation
    [[nodiscard]] bool isValid() const noexcept {
        if (quantity <= 0) return false;
        if (type == OrderType::LIMIT && price <= 0) return false;
        if ((type == OrderType::STOP || type == OrderType::STOP_LIMIT) && stop_price <= 0) return false;
        return true;
    }
};

/**
 * @brief Order modification request
 */
struct OrderModifyRequest {
    OrderId             order_id        = 0;
    std::string         client_order_id;
    std::optional<Price> new_price;
    std::optional<Quantity> new_quantity;
    
    [[nodiscard]] bool hasChanges() const noexcept {
        return new_price.has_value() || new_quantity.has_value();
    }
};

/**
 * @brief Order cancel request
 */
struct OrderCancelRequest {
    OrderId             order_id        = 0;
    std::string         client_order_id;
    Symbol              symbol;
};

/**
 * @brief Order cancel all request
 */
struct OrderCancelAllRequest {
    std::optional<Symbol> symbol;       // If empty, cancel all symbols
    std::optional<Side> side;           // If empty, cancel both sides
};

/**
 * @brief Order fill/execution report
 */
struct Fill {
    TradeId             trade_id        = 0;
    OrderId             order_id        = 0;
    Symbol              symbol;
    Side                side            = Side::BUY;
    Price               price           = 0.0;
    Quantity            quantity        = 0.0;
    Decimal             fee             = 0.0;
    std::string         fee_currency;
    bool                is_maker        = false;
    Timestamp           executed_at;
    
    [[nodiscard]] Decimal getNotionalValue() const noexcept {
        return price * quantity;
    }
};

/**
 * @brief Order validator utility
 */
class OrderValidator {
public:
    struct ValidationResult {
        bool is_valid = true;
        ErrorCode error_code = ErrorCode::SUCCESS;
        std::string error_message;
    };
    
    static ValidationResult validate(const OrderRequest& request);
    static ValidationResult validateModify(const OrderModifyRequest& request);
    static ValidationResult validateCancel(const OrderCancelRequest& request);
    
private:
    static constexpr Quantity MIN_QUANTITY = 0.00000001;
    static constexpr Price MIN_PRICE = 0.00000001;
};

} // namespace orders
} // namespace architect
