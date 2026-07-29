#pragma once

/**
 * @file User.h
 * @brief User data structures and authentication types
 */

#include "core/Types.h"
#include <string>
#include <chrono>
#include <optional>

namespace architect {
namespace user {

using namespace core;

/**
 * @brief API credentials for authentication
 */
struct ApiCredentials {
    std::string api_key;
    std::string api_secret;
    std::string session_token;
    
    [[nodiscard]] bool isValid() const {
        return !api_key.empty();
    }
    
    [[nodiscard]] bool hasSessionToken() const {
        return !session_token.empty();
    }
};

/**
 * @brief User account information (maps to Architect WhoAmI response)
 */
struct User {
    UserId              id              = 0;
    std::string         username;
    std::string         created_at;
    bool                enabled_2fa     = false;
    bool                is_onboarded    = false;
    bool                is_close_only   = false;
    bool                is_frozen       = false;
    bool                is_admin        = false;
    Decimal             maker_fee       = 0.0;
    Decimal             taker_fee       = 0.0;
    
    // Local tracking
    ApiCredentials      credentials;
    Timestamp           last_login;
    Timestamp           last_activity;
    bool                is_authenticated = false;
    
    [[nodiscard]] bool canTrade() const {
        return is_authenticated && !is_frozen && !is_close_only;
    }
    
    [[nodiscard]] bool canOpenPositions() const {
        return canTrade() && is_onboarded;
    }
};

/**
 * @brief Authentication request
 */
struct AuthRequest {
    std::string api_key;
    std::string api_secret;
    std::optional<std::string> totp_code;  // For 2FA
};

/**
 * @brief Authentication response
 */
struct AuthResponse {
    bool            success         = false;
    std::string     session_token;
    std::string     error_message;
    ErrorCode       error_code      = ErrorCode::SUCCESS;
    Timestamp       expires_at;
    User            user;
};

/**
 * @brief Session information
 */
struct Session {
    std::string     token;
    UserId          user_id         = 0;
    Timestamp       created_at;
    Timestamp       expires_at;
    Timestamp       last_activity;
    bool            is_active       = false;
    
    [[nodiscard]] bool isExpired() const {
        auto now = std::chrono::high_resolution_clock::now().time_since_epoch();
        return now > expires_at;
    }
    
    [[nodiscard]] std::chrono::seconds timeToExpiry() const {
        auto now = std::chrono::high_resolution_clock::now().time_since_epoch();
        if (now >= expires_at) return std::chrono::seconds(0);
        return std::chrono::duration_cast<std::chrono::seconds>(expires_at - now);
    }
};

/**
 * @brief Rate limit status
 */
struct RateLimitStatus {
    int             requests_remaining  = 0;
    int             requests_limit      = 0;
    Timestamp       reset_time;
    bool            is_limited          = false;
};

/**
 * @brief User permissions
 */
struct Permissions {
    bool can_trade          = false;
    bool can_withdraw       = false;
    bool can_deposit        = false;
    bool can_cancel_orders  = true;
    bool can_view_orders    = true;
    bool can_view_positions = true;
    bool can_view_balances  = true;
    
    // Trading limits
    std::optional<Decimal> max_order_size;
    std::optional<Decimal> max_position_size;
    std::optional<int>     max_open_orders;
};

} // namespace user
} // namespace architect
