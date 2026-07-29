#include "orders/Order.h"

namespace architect {
namespace orders {

OrderValidator::ValidationResult OrderValidator::validate(const OrderRequest& request) {
    ValidationResult result;
    
    // Check quantity
    if (request.quantity <= MIN_QUANTITY) {
        result.is_valid = false;
        result.error_code = ErrorCode::INVALID_QUANTITY;
        result.error_message = "Quantity must be greater than minimum";
        return result;
    }
    
    // Check price for limit orders
    if (request.type == OrderType::LIMIT || request.type == OrderType::STOP_LIMIT) {
        if (request.price <= MIN_PRICE) {
            result.is_valid = false;
            result.error_code = ErrorCode::INVALID_PRICE;
            result.error_message = "Price must be greater than minimum for limit orders";
            return result;
        }
    }
    
    // Check stop price for stop orders
    if (request.type == OrderType::STOP || request.type == OrderType::STOP_LIMIT) {
        if (request.stop_price <= MIN_PRICE) {
            result.is_valid = false;
            result.error_code = ErrorCode::INVALID_PRICE;
            result.error_message = "Stop price must be greater than minimum for stop orders";
            return result;
        }
    }
    
    return result;
}

OrderValidator::ValidationResult OrderValidator::validateModify(const OrderModifyRequest& request) {
    ValidationResult result;
    
    if (request.order_id == 0 && request.client_order_id.empty()) {
        result.is_valid = false;
        result.error_code = ErrorCode::INVALID_REQUEST;
        result.error_message = "Order ID or client order ID required";
        return result;
    }
    
    if (!request.hasChanges()) {
        result.is_valid = false;
        result.error_code = ErrorCode::INVALID_REQUEST;
        result.error_message = "No changes specified";
        return result;
    }
    
    if (request.new_price.has_value() && request.new_price.value() <= MIN_PRICE) {
        result.is_valid = false;
        result.error_code = ErrorCode::INVALID_PRICE;
        result.error_message = "New price must be greater than minimum";
        return result;
    }
    
    if (request.new_quantity.has_value() && request.new_quantity.value() <= MIN_QUANTITY) {
        result.is_valid = false;
        result.error_code = ErrorCode::INVALID_QUANTITY;
        result.error_message = "New quantity must be greater than minimum";
        return result;
    }
    
    return result;
}

OrderValidator::ValidationResult OrderValidator::validateCancel(const OrderCancelRequest& request) {
    ValidationResult result;
    
    if (request.order_id == 0 && request.client_order_id.empty()) {
        result.is_valid = false;
        result.error_code = ErrorCode::INVALID_REQUEST;
        result.error_message = "Order ID or client order ID required";
        return result;
    }
    
    return result;
}

} // namespace orders
} // namespace architect
