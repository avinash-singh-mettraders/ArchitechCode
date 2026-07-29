#include "api/WebSocketClient.h"
#include "config/Config.h"
#include "events/EventManager.h"
#include "core/Constants.h"
#include <chrono>

namespace architect {
namespace api {

// WSMessage implementation
std::string WSMessage::serialize() const {
    json msg;
    
    switch (type) {
        case WSMessageType::SUBSCRIBE:
            msg["type"] = "subscribe";
            break;
        case WSMessageType::UNSUBSCRIBE:
            msg["type"] = "unsubscribe";
            break;
        case WSMessageType::PING:
            msg["type"] = "ping";
            break;
        case WSMessageType::PONG:
            msg["type"] = "pong";
            break;
        case WSMessageType::AUTH:
            msg["type"] = "auth";
            break;
        default:
            msg["type"] = "data";
    }
    
    if (!channel.empty()) {
        msg["channel"] = channel;
    }
    if (!symbol.empty()) {
        msg["symbol"] = symbol;
    }
    if (!data.empty()) {
        msg["data"] = data;
    }
    
    return msg.dump();
}

WSMessage WSMessage::deserialize(const std::string& str) {
    WSMessage msg;
    
    try {
        json j = json::parse(str);
        
        std::string type_str = j.value("type", "data");
        if (type_str == "subscribe") msg.type = WSMessageType::SUBSCRIBE;
        else if (type_str == "unsubscribe") msg.type = WSMessageType::UNSUBSCRIBE;
        else if (type_str == "ping") msg.type = WSMessageType::PING;
        else if (type_str == "pong") msg.type = WSMessageType::PONG;
        else if (type_str == "auth") msg.type = WSMessageType::AUTH;
        else if (type_str == "error") msg.type = WSMessageType::ERROR;
        else if (type_str == "heartbeat") msg.type = WSMessageType::HEARTBEAT;
        else msg.type = WSMessageType::DATA;
        
        msg.channel = j.value("channel", "");
        msg.symbol = j.value("symbol", "");
        
        if (j.contains("data")) {
            msg.data = j["data"];
        }
        
        msg.timestamp = std::chrono::high_resolution_clock::now().time_since_epoch();
        
    } catch (...) {
        msg.type = WSMessageType::ERROR;
        msg.data = {{"error", "Failed to parse message"}};
    }
    
    return msg;
}

// WebSocketClient implementation
WebSocketClient& WebSocketClient::getInstance() {
    static WebSocketClient instance;
    return instance;
}

WebSocketClient::WebSocketClient() {
    auto& config = config::Config::getInstance();
    url_ = config.getWebSocketEndpoint();
    reconnect_delay_ = std::chrono::milliseconds(
        config.getInt("websocket.reconnect_delay_ms", 1000)
    );
    heartbeat_interval_ = std::chrono::milliseconds(
        config.getInt("websocket.heartbeat_interval_ms", 15000)
    );
    auto_reconnect_ = config.getBool("websocket.auto_reconnect", true);
}

WebSocketClient::~WebSocketClient() {
    disconnect();
}

bool WebSocketClient::connect() {
    if (state_ == WSState::CONNECTED || state_ == WSState::CONNECTING) {
        return true;
    }
    
    setState(WSState::CONNECTING);
    
    // TODO: Implement actual WebSocket connection using a library like
    // libwebsockets, Beast, or websocketpp
    // For now, this is a placeholder implementation
    
    // Simulated successful connection
    setState(WSState::CONNECTED);
    
    running_ = true;
    
    // Start heartbeat thread
    heartbeat_thread_ = std::thread(&WebSocketClient::heartbeatLoop, this);
    
    // Start receive thread
    receive_thread_ = std::thread(&WebSocketClient::receiveLoop, this);
    
    // Publish connection event
    events::ConnectionEventData event_data;
    event_data.status = core::ConnectionStatus::CONNECTED;
    event_data.endpoint = url_;
    auto event = std::make_shared<events::Event>(
        events::EventType::CONNECTION_ESTABLISHED,
        event_data,
        events::EventPriority::HIGH
    );
    events::EventManager::getInstance().publish(event);
    
    // Resubscribe to any existing subscriptions
    {
        std::shared_lock<std::shared_mutex> lock(subscriptions_mutex_);
        for (auto& [key, sub] : subscriptions_) {
            if (sub.active) {
                // Re-send subscription message
                WSMessage msg;
                msg.type = WSMessageType::SUBSCRIBE;
                msg.channel = sub.channel;
                msg.symbol = sub.symbol;
                send(msg.serialize());
            }
        }
    }
    
    return true;
}

void WebSocketClient::disconnect() {
    if (state_ == WSState::DISCONNECTED || state_ == WSState::CLOSED) {
        return;
    }
    
    setState(WSState::CLOSING);
    running_ = false;
    
    // Wait for threads to finish
    if (heartbeat_thread_.joinable()) {
        heartbeat_thread_.join();
    }
    if (receive_thread_.joinable()) {
        receive_thread_.join();
    }
    
    // TODO: Close actual WebSocket connection
    
    setState(WSState::CLOSED);
    
    // Publish disconnect event
    events::ConnectionEventData event_data;
    event_data.status = core::ConnectionStatus::DISCONNECTED;
    event_data.endpoint = url_;
    auto event = std::make_shared<events::Event>(
        events::EventType::CONNECTION_LOST,
        event_data,
        events::EventPriority::HIGH
    );
    events::EventManager::getInstance().publish(event);
}

bool WebSocketClient::isConnected() const {
    return state_ == WSState::CONNECTED || state_ == WSState::AUTHENTICATED;
}

WSState WebSocketClient::getState() const {
    return state_.load();
}

bool WebSocketClient::authenticate(const std::string& session_token) {
    if (!isConnected()) {
        return false;
    }
    
    setState(WSState::AUTHENTICATING);
    
    WSMessage msg;
    msg.type = WSMessageType::AUTH;
    msg.data = {{"token", session_token}};
    
    if (send(msg.serialize())) {
        // In a real implementation, we would wait for auth response
        setState(WSState::AUTHENTICATED);
        return true;
    }
    
    setState(WSState::CONNECTED);
    return false;
}

void WebSocketClient::setUrl(const std::string& url) {
    url_ = url;
}

void WebSocketClient::setAutoReconnect(bool enabled) {
    auto_reconnect_ = enabled;
}

void WebSocketClient::setReconnectDelay(std::chrono::milliseconds delay) {
    reconnect_delay_ = delay;
}

void WebSocketClient::setHeartbeatInterval(std::chrono::milliseconds interval) {
    heartbeat_interval_ = interval;
}

std::string WebSocketClient::makeSubscriptionKey(const std::string& channel, const std::string& symbol) const {
    return symbol.empty() ? channel : channel + ":" + symbol;
}

bool WebSocketClient::subscribe(const std::string& channel, const std::string& symbol, 
                                const SubscribeOptions& options) {
    std::string key = makeSubscriptionKey(channel, symbol);
    
    {
        std::unique_lock<std::shared_mutex> lock(subscriptions_mutex_);
        
        auto it = subscriptions_.find(key);
        if (it != subscriptions_.end() && it->second.active) {
            return true; // Already subscribed
        }
        
        Subscription sub;
        sub.channel = channel;
        sub.symbol = symbol;
        sub.active = true;
        sub.subscribed_at = std::chrono::high_resolution_clock::now().time_since_epoch();
        sub.level = options.level;
        sub.depth = options.depth;
        sub.throttle_ms = options.throttle_ms;
        subscriptions_[key] = sub;
    }
    
    if (isConnected()) {
        WSMessage msg;
        msg.type = WSMessageType::SUBSCRIBE;
        msg.channel = channel;
        msg.symbol = symbol;
        
        // Add subscription parameters to data
        msg.data = {
            {"level", core::subscriptionLevelToString(options.level)},
            {"depth", options.depth},
            {"throttle_ms", options.throttle_ms},
            {"snapshot_only", options.snapshot_only}
        };
        
        if (!options.start_time.empty()) {
            msg.data["start_time"] = options.start_time;
        }
        if (!options.end_time.empty()) {
            msg.data["end_time"] = options.end_time;
        }
        
        return send(msg.serialize());
    }
    
    return true; // Will subscribe on connect
}

bool WebSocketClient::subscribeWithParams(const core::SubscriptionParams& params) {
    SubscribeOptions options;
    options.level = params.level;
    options.depth = params.depth;
    options.snapshot_only = params.snapshot_only;
    options.throttle_ms = params.throttle_ms;
    options.start_time = params.start_time;
    options.end_time = params.end_time;
    
    // Determine channel based on level
    std::string channel;
    switch (params.level) {
        case core::SubscriptionLevel::L1:
        case core::SubscriptionLevel::L2:
        case core::SubscriptionLevel::L3:
            channel = core::constants::api::WS_ORDERBOOK;
            break;
        case core::SubscriptionLevel::TRADES:
            channel = core::constants::api::WS_TRADES;
            break;
        case core::SubscriptionLevel::TICKER:
            channel = core::constants::api::WS_TICKER;
            break;
    }
    
    return subscribe(channel, params.symbol, options);
}

bool WebSocketClient::subscribeMultiple(const std::vector<std::pair<std::string, std::string>>& channels) {
    bool all_success = true;
    for (const auto& [channel, symbol] : channels) {
        if (!subscribe(channel, symbol)) {
            all_success = false;
        }
    }
    return all_success;
}

bool WebSocketClient::unsubscribe(const std::string& channel, const std::string& symbol) {
    std::string key = makeSubscriptionKey(channel, symbol);
    
    {
        std::unique_lock<std::shared_mutex> lock(subscriptions_mutex_);
        
        auto it = subscriptions_.find(key);
        if (it == subscriptions_.end() || !it->second.active) {
            return true; // Not subscribed
        }
        
        it->second.active = false;
    }
    
    if (isConnected()) {
        WSMessage msg;
        msg.type = WSMessageType::UNSUBSCRIBE;
        msg.channel = channel;
        msg.symbol = symbol;
        return send(msg.serialize());
    }
    
    return true;
}

void WebSocketClient::unsubscribeAll() {
    std::vector<std::pair<std::string, std::string>> to_unsubscribe;
    
    {
        std::unique_lock<std::shared_mutex> lock(subscriptions_mutex_);
        
        for (auto& [key, sub] : subscriptions_) {
            if (sub.active) {
                to_unsubscribe.emplace_back(sub.channel, sub.symbol);
                sub.active = false;
            }
        }
    }
    
    for (const auto& [channel, symbol] : to_unsubscribe) {
        WSMessage msg;
        msg.type = WSMessageType::UNSUBSCRIBE;
        msg.channel = channel;
        msg.symbol = symbol;
        send(msg.serialize());
    }
}

std::vector<Subscription> WebSocketClient::getSubscriptions() const {
    std::shared_lock<std::shared_mutex> lock(subscriptions_mutex_);
    
    std::vector<Subscription> result;
    result.reserve(subscriptions_.size());
    
    for (const auto& [key, sub] : subscriptions_) {
        if (sub.active) {
            result.push_back(sub);
        }
    }
    
    return result;
}

bool WebSocketClient::subscribeL1(const std::string& symbol, std::uint32_t throttle_ms) {
    return subscribe(core::constants::api::WS_ORDERBOOK, symbol,
                     SubscribeOptions::L1(throttle_ms));
}

bool WebSocketClient::subscribeL2(const std::string& symbol, std::uint32_t depth, std::uint32_t throttle_ms) {
    return subscribe(core::constants::api::WS_ORDERBOOK, symbol, 
                     SubscribeOptions::L2(depth, throttle_ms));
}

bool WebSocketClient::subscribeOrderbook(const std::string& symbol) {
    // Default to L2 with configured depth
    auto& config = config::Config::getInstance();
    int depth = config.getL2Depth();
    return subscribeL2(symbol, static_cast<std::uint32_t>(depth));
}

bool WebSocketClient::subscribeTicker(const std::string& symbol, std::uint32_t throttle_ms) {
    return subscribe(core::constants::api::WS_TICKER, symbol, 
                     SubscribeOptions::Ticker(throttle_ms));
}

bool WebSocketClient::subscribeTrades(const std::string& symbol, std::uint32_t throttle_ms) {
    return subscribe(core::constants::api::WS_TRADES, symbol, 
                     SubscribeOptions::Trades(throttle_ms));
}

bool WebSocketClient::subscribeOrders() {
    return subscribe(core::constants::api::WS_ORDERS);
}

bool WebSocketClient::subscribeFills() {
    return subscribe(core::constants::api::WS_FILLS);
}

bool WebSocketClient::subscribePositions() {
    return subscribe(core::constants::api::WS_POSITIONS);
}

bool WebSocketClient::send(const std::string& message) {
    std::lock_guard<std::mutex> lock(ws_mutex_);
    
    if (!isConnected()) {
        return false;
    }
    
    // TODO: Implement actual WebSocket send
    // For now, just track stats
    
    {
        std::lock_guard<std::mutex> stats_lock(stats_mutex_);
        ++stats_.messages_sent;
        stats_.bytes_sent += message.size();
    }
    
    return true;
}

bool WebSocketClient::send(const json& message) {
    return send(message.dump());
}

void WebSocketClient::ping() {
    WSMessage msg;
    msg.type = WSMessageType::PING;
    send(msg.serialize());
}

void WebSocketClient::setMessageCallback(WSMessageCallback callback) {
    std::unique_lock<std::shared_mutex> lock(callbacks_mutex_);
    message_callback_ = std::move(callback);
}

void WebSocketClient::setChannelCallback(const std::string& channel, WSMessageCallback callback) {
    std::unique_lock<std::shared_mutex> lock(callbacks_mutex_);
    channel_callbacks_[channel] = std::move(callback);
}

void WebSocketClient::setStateCallback(WSStateCallback callback) {
    std::unique_lock<std::shared_mutex> lock(callbacks_mutex_);
    state_callback_ = std::move(callback);
}

void WebSocketClient::setErrorCallback(WSErrorCallback callback) {
    std::unique_lock<std::shared_mutex> lock(callbacks_mutex_);
    error_callback_ = std::move(callback);
}

WebSocketClient::Stats WebSocketClient::getStats() const {
    std::lock_guard<std::mutex> lock(stats_mutex_);
    return stats_;
}

void WebSocketClient::resetStats() {
    std::lock_guard<std::mutex> lock(stats_mutex_);
    stats_ = Stats{};
}

void WebSocketClient::setState(WSState new_state) {
    WSState old_state = state_.exchange(new_state);
    
    if (old_state != new_state) {
        std::shared_lock<std::shared_mutex> lock(callbacks_mutex_);
        if (state_callback_) {
            state_callback_(old_state, new_state);
        }
    }
}

void WebSocketClient::reconnect() {
    if (!auto_reconnect_) {
        return;
    }
    
    setState(WSState::RECONNECTING);
    
    // Publish reconnecting event
    events::ConnectionEventData event_data;
    event_data.status = core::ConnectionStatus::RECONNECTING;
    event_data.endpoint = url_;
    auto event = std::make_shared<events::Event>(
        events::EventType::CONNECTION_RECONNECTING,
        event_data,
        events::EventPriority::HIGH
    );
    events::EventManager::getInstance().publish(event);
    
    std::this_thread::sleep_for(reconnect_delay_);
    
    {
        std::lock_guard<std::mutex> lock(stats_mutex_);
        ++stats_.reconnect_count;
    }
    
    connect();
}

void WebSocketClient::heartbeatLoop() {
    while (running_) {
        std::this_thread::sleep_for(heartbeat_interval_);
        
        if (running_ && isConnected()) {
            ping();
        }
    }
}

void WebSocketClient::receiveLoop() {
    while (running_) {
        // TODO: Implement actual WebSocket receive
        // This would block waiting for messages from the WebSocket
        
        // For now, just sleep to avoid busy-waiting
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        
        // In a real implementation:
        // 1. Receive message from WebSocket
        // 2. Call processMessage(received_data)
    }
}

void WebSocketClient::processMessage(const std::string& message) {
    WSMessage msg = WSMessage::deserialize(message);
    
    {
        std::lock_guard<std::mutex> lock(stats_mutex_);
        ++stats_.messages_received;
        stats_.bytes_received += message.size();
        stats_.last_message_at = std::chrono::high_resolution_clock::now().time_since_epoch();
    }
    
    // Update subscription message count
    if (!msg.channel.empty()) {
        std::string key = makeSubscriptionKey(msg.channel, msg.symbol);
        
        std::unique_lock<std::shared_mutex> lock(subscriptions_mutex_);
        auto it = subscriptions_.find(key);
        if (it != subscriptions_.end()) {
            ++it->second.message_count;
        }
    }
    
    // Handle special message types
    switch (msg.type) {
        case WSMessageType::PONG:
            // Heartbeat response, nothing to do
            return;
            
        case WSMessageType::ERROR:
            {
                std::shared_lock<std::shared_mutex> lock(callbacks_mutex_);
                if (error_callback_) {
                    error_callback_(msg.data.value("error", "Unknown error"));
                }
            }
            return;
            
        case WSMessageType::HEARTBEAT:
            // Server heartbeat
            return;
            
        default:
            break;
    }
    
    dispatchMessage(msg);
}

void WebSocketClient::dispatchMessage(const WSMessage& msg) {
    std::shared_lock<std::shared_mutex> lock(callbacks_mutex_);
    
    // Channel-specific callback
    if (!msg.channel.empty()) {
        auto it = channel_callbacks_.find(msg.channel);
        if (it != channel_callbacks_.end() && it->second) {
            it->second(msg);
        }
    }
    
    // Global message callback
    if (message_callback_) {
        message_callback_(msg);
    }
}

} // namespace api
} // namespace architect
