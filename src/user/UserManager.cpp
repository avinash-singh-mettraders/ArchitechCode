#include "user/UserManager.h"
#include "config/Config.h"
#include "events/EventManager.h"

namespace architect {
namespace user {

UserManager& UserManager::getInstance() {
    static UserManager instance;
    return instance;
}

UserManager::UserManager() {
    // Initialize from config
    auto& config = config::Config::getInstance();
    credentials_.api_key = config.getApiKey();
    credentials_.api_secret = config.getApiSecret();
    credentials_.session_token = config.getSessionToken();
}

AuthResponse UserManager::authenticate(const AuthRequest& request) {
    AuthResponse response;
    
    // Store credentials
    {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        credentials_.api_key = request.api_key;
        credentials_.api_secret = request.api_secret;
    }
    
    // TODO: Make actual API call to authenticate
    // For now, create a placeholder response
    // In production, this would call the REST API
    
    response.success = !request.api_key.empty() && !request.api_secret.empty();
    
    if (response.success) {
        // Create session
        response.session_token = "session_" + request.api_key.substr(0, 8);
        response.expires_at = std::chrono::high_resolution_clock::now().time_since_epoch() 
                              + std::chrono::hours(24);
        
        // Create user
        response.user.id = 1; // Would come from API
        response.user.username = "user"; // Would come from API
        response.user.is_authenticated = true;
        response.user.credentials = credentials_;
        response.user.credentials.session_token = response.session_token;
        
        {
            std::unique_lock<std::shared_mutex> lock(mutex_);
            current_user_ = std::make_shared<User>(response.user);
            credentials_.session_token = response.session_token;
            createSession(response.session_token, response.user.id, response.expires_at);
        }
        
        // Publish auth success event
        events::ConnectionEventData event_data;
        event_data.status = ConnectionStatus::CONNECTED;
        event_data.endpoint = "auth";
        auto event = std::make_shared<events::Event>(
            events::EventType::AUTH_SUCCESS, 
            event_data, 
            events::EventPriority::HIGH
        );
        events::EventManager::getInstance().publish(event);
    } else {
        response.error_code = ErrorCode::AUTHENTICATION_FAILED;
        response.error_message = "Invalid credentials";
        
        // Publish auth failure event
        events::ConnectionEventData event_data;
        event_data.status = ConnectionStatus::ERROR;
        event_data.endpoint = "auth";
        event_data.error_message = response.error_message;
        auto event = std::make_shared<events::Event>(
            events::EventType::AUTH_FAILURE, 
            event_data, 
            events::EventPriority::HIGH
        );
        events::EventManager::getInstance().publish(event);
    }
    
    if (auth_callback_) {
        auth_callback_(response);
    }
    
    return response;
}

AuthResponse UserManager::authenticateFromConfig() {
    auto& config = config::Config::getInstance();
    
    AuthRequest request;
    request.api_key = config.getApiKey();
    request.api_secret = config.getApiSecret();
    
    return authenticate(request);
}

bool UserManager::refreshSession() {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    
    if (!current_session_ || current_session_->isExpired()) {
        return false;
    }
    
    // TODO: Call API to refresh token
    // For now, just extend the session
    
    lock.unlock();
    
    {
        std::unique_lock<std::shared_mutex> write_lock(mutex_);
        if (current_session_) {
            current_session_->expires_at = std::chrono::high_resolution_clock::now().time_since_epoch() 
                                           + std::chrono::hours(24);
            current_session_->last_activity = std::chrono::high_resolution_clock::now().time_since_epoch();
        }
    }
    
    // Publish token refresh event
    auto event = std::make_shared<events::Event>(
        events::EventType::AUTH_TOKEN_REFRESH, 
        std::string("Token refreshed"), 
        events::EventPriority::NORMAL
    );
    events::EventManager::getInstance().publish(event);
    
    return true;
}

void UserManager::logout() {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    
    invalidateSession();
    
    if (current_user_) {
        current_user_->is_authenticated = false;
    }
    current_user_.reset();
    credentials_.session_token.clear();
    
    lock.unlock();
    
    // Publish session expired event
    auto event = std::make_shared<events::Event>(
        events::EventType::AUTH_SESSION_EXPIRED, 
        std::string("User logged out"), 
        events::EventPriority::HIGH
    );
    events::EventManager::getInstance().publish(event);
}

bool UserManager::isAuthenticated() const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return current_user_ && current_user_->is_authenticated && hasValidSession();
}

std::shared_ptr<User> UserManager::getCurrentUser() const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return current_user_;
}

bool UserManager::fetchUserInfo() {
    // TODO: Call /whoami API endpoint
    // For now, return true if we have a session
    return hasValidSession();
}

std::shared_ptr<User> UserManager::getUser(UserId user_id) const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    
    auto it = user_cache_.find(user_id);
    if (it != user_cache_.end()) {
        return it->second;
    }
    
    if (current_user_ && current_user_->id == user_id) {
        return current_user_;
    }
    
    return nullptr;
}

std::shared_ptr<Session> UserManager::getCurrentSession() const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return current_session_;
}

bool UserManager::hasValidSession() const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return current_session_ && current_session_->is_active && !current_session_->isExpired();
}

std::string UserManager::getSessionToken() const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return current_session_ ? current_session_->token : "";
}

void UserManager::setSessionToken(const std::string& token) {
    if (token.empty()) return;
    std::unique_lock<std::shared_mutex> lock(mutex_);
    credentials_.session_token = token;
    auto expires_at = std::chrono::high_resolution_clock::now().time_since_epoch()
                      + std::chrono::hours(24);
    createSession(token, 0, std::chrono::duration_cast<Timestamp>(expires_at));
    current_user_ = std::make_shared<User>();
    current_user_->id = 0;
    current_user_->username = "user";
    current_user_->is_authenticated = true;
    current_user_->credentials = credentials_;
}

void UserManager::updateActivity() {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    
    if (current_session_) {
        current_session_->last_activity = std::chrono::high_resolution_clock::now().time_since_epoch();
    }
    if (current_user_) {
        current_user_->last_activity = std::chrono::high_resolution_clock::now().time_since_epoch();
    }
}

RateLimitStatus UserManager::getRateLimitStatus() const {
    std::lock_guard<std::mutex> lock(rate_limit_mutex_);
    return rate_limit_;
}

void UserManager::updateRateLimit(int remaining, int limit, Timestamp reset_time) {
    std::lock_guard<std::mutex> lock(rate_limit_mutex_);
    rate_limit_.requests_remaining = remaining;
    rate_limit_.requests_limit = limit;
    rate_limit_.reset_time = reset_time;
    rate_limit_.is_limited = (remaining <= 0);
}

bool UserManager::canMakeRequest() const {
    std::lock_guard<std::mutex> lock(rate_limit_mutex_);
    
    if (!rate_limit_.is_limited) {
        return true;
    }
    
    auto now = std::chrono::high_resolution_clock::now().time_since_epoch();
    return now > rate_limit_.reset_time;
}

Permissions UserManager::getPermissions() const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    
    Permissions perms;
    
    if (current_user_) {
        perms.can_trade = current_user_->canTrade();
        perms.can_withdraw = !current_user_->is_frozen;
        perms.can_deposit = true;
        perms.can_cancel_orders = true;
        perms.can_view_orders = true;
        perms.can_view_positions = true;
        perms.can_view_balances = true;
    }
    
    return perms;
}

bool UserManager::canTrade() const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return current_user_ && current_user_->canTrade();
}

bool UserManager::canWithdraw() const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return current_user_ && !current_user_->is_frozen;
}

bool UserManager::canCancelOrders() const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return current_user_ && current_user_->is_authenticated;
}

void UserManager::setAuthCallback(AuthCallback callback) {
    auth_callback_ = std::move(callback);
}

void UserManager::setSessionCallback(SessionCallback callback) {
    session_callback_ = std::move(callback);
}

void UserManager::setCredentials(const ApiCredentials& credentials) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    credentials_ = credentials;
}

ApiCredentials UserManager::getCredentials() const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return credentials_;
}

void UserManager::clearCredentials() {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    credentials_ = ApiCredentials{};
}

void UserManager::createSession(const std::string& token, UserId user_id, Timestamp expires_at) {
    current_session_ = std::make_shared<Session>();
    current_session_->token = token;
    current_session_->user_id = user_id;
    current_session_->created_at = std::chrono::high_resolution_clock::now().time_since_epoch();
    current_session_->expires_at = expires_at;
    current_session_->last_activity = current_session_->created_at;
    current_session_->is_active = true;
    
    if (session_callback_) {
        session_callback_(*current_session_);
    }
}

void UserManager::invalidateSession() {
    if (current_session_) {
        current_session_->is_active = false;
        
        if (session_callback_) {
            session_callback_(*current_session_);
        }
        
        current_session_.reset();
    }
}

} // namespace user
} // namespace architect
