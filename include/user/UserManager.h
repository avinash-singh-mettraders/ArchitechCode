#pragma once

/**
 * @file UserManager.h
 * @brief User and session management
 */

#include "user/User.h"
#include <map>
#include <mutex>
#include <shared_mutex>
#include <memory>
#include <functional>

namespace architect {
namespace user {

using AuthCallback = std::function<void(const AuthResponse&)>;
using SessionCallback = std::function<void(const Session&)>;

/**
 * @brief User and session manager
 */
class UserManager {
public:
    /**
     * @brief Get the singleton instance
     */
    static UserManager& getInstance();
    
    // Prevent copying
    UserManager(const UserManager&) = delete;
    UserManager& operator=(const UserManager&) = delete;
    
    // ==========================================================================
    // Authentication
    // ==========================================================================
    
    /**
     * @brief Authenticate with API credentials
     */
    AuthResponse authenticate(const AuthRequest& request);
    
    /**
     * @brief Authenticate using stored credentials from config
     */
    AuthResponse authenticateFromConfig();
    
    /**
     * @brief Refresh the current session token
     */
    bool refreshSession();
    
    /**
     * @brief Logout and invalidate session
     */
    void logout();
    
    /**
     * @brief Check if currently authenticated
     */
    [[nodiscard]] bool isAuthenticated() const;
    
    // ==========================================================================
    // User Information
    // ==========================================================================
    
    /**
     * @brief Get current user
     */
    std::shared_ptr<User> getCurrentUser() const;
    
    /**
     * @brief Fetch current user info from API (whoami)
     */
    bool fetchUserInfo();
    
    /**
     * @brief Get user by ID
     */
    std::shared_ptr<User> getUser(UserId user_id) const;
    
    // ==========================================================================
    // Session Management
    // ==========================================================================
    
    /**
     * @brief Get current session
     */
    std::shared_ptr<Session> getCurrentSession() const;
    
    /**
     * @brief Check if session is valid
     */
    [[nodiscard]] bool hasValidSession() const;
    
    /**
     * @brief Get session token for API requests
     */
    std::string getSessionToken() const;
    
    /**
     * @brief Set session token (e.g. from config or after login) and establish session
     */
    void setSessionToken(const std::string& token);
    
    /**
     * @brief Update last activity timestamp
     */
    void updateActivity();
    
    // ==========================================================================
    // Rate Limiting
    // ==========================================================================
    
    /**
     * @brief Check rate limit status
     */
    [[nodiscard]] RateLimitStatus getRateLimitStatus() const;
    
    /**
     * @brief Update rate limit from API response headers
     */
    void updateRateLimit(int remaining, int limit, Timestamp reset_time);
    
    /**
     * @brief Check if can make request (rate limit check)
     */
    [[nodiscard]] bool canMakeRequest() const;
    
    // ==========================================================================
    // Permissions
    // ==========================================================================
    
    /**
     * @brief Get current user permissions
     */
    Permissions getPermissions() const;
    
    /**
     * @brief Check if user can perform action
     */
    [[nodiscard]] bool canTrade() const;
    [[nodiscard]] bool canWithdraw() const;
    [[nodiscard]] bool canCancelOrders() const;
    
    // ==========================================================================
    // Callbacks
    // ==========================================================================
    
    void setAuthCallback(AuthCallback callback);
    void setSessionCallback(SessionCallback callback);
    
    // ==========================================================================
    // Credentials Management
    // ==========================================================================
    
    /**
     * @brief Set API credentials
     */
    void setCredentials(const ApiCredentials& credentials);
    
    /**
     * @brief Get API credentials
     */
    ApiCredentials getCredentials() const;
    
    /**
     * @brief Clear stored credentials
     */
    void clearCredentials();
    
private:
    UserManager();
    ~UserManager() = default;
    
    void createSession(const std::string& token, UserId user_id, Timestamp expires_at);
    void invalidateSession();
    
    // Current user and session
    std::shared_ptr<User> current_user_;
    std::shared_ptr<Session> current_session_;
    ApiCredentials credentials_;
    
    // Rate limiting
    RateLimitStatus rate_limit_;
    
    // User cache
    std::map<UserId, std::shared_ptr<User>> user_cache_;
    
    mutable std::shared_mutex mutex_;
    mutable std::mutex rate_limit_mutex_;
    
    // Callbacks
    AuthCallback auth_callback_;
    SessionCallback session_callback_;
};

} // namespace user
} // namespace architect
