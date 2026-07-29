#pragma once

/**
 * @file WebSocketClient.h
 * @brief WebSocket client for real-time market data and order updates
 */

#include "core/Types.h"
#include "events/Event.h"
#include <string>
#include <vector>
#include <map>
#include <set>
#include <functional>
#include <mutex>
#include <shared_mutex>
#include <thread>
#include <atomic>
#include <chrono>
#include <nlohmann/json.hpp>

namespace architect {
namespace api {

using json = nlohmann::json;

/**
 * @brief WebSocket message types
 */
enum class WSMessageType {
    SUBSCRIBE,
    UNSUBSCRIBE,
    PING,
    PONG,
    AUTH,
    DATA,
    ERROR,
    HEARTBEAT
};

/**
 * @brief WebSocket message
 */
struct WSMessage {
    WSMessageType   type;
    std::string     channel;
    std::string     symbol;
    json            data;
    core::Timestamp timestamp;
    
    [[nodiscard]] std::string serialize() const;
    static WSMessage deserialize(const std::string& str);
};

/**
 * @brief Channel subscription info
 */
struct Subscription {
    std::string             channel;
    std::string             symbol;
    bool                    active          = false;
    core::Timestamp         subscribed_at;
    std::uint64_t           message_count   = 0;
    core::SubscriptionLevel level           = core::SubscriptionLevel::L1;
    std::uint32_t           depth           = 25;
    std::uint32_t           throttle_ms     = 0;
};

/**
 * @brief Extended subscription parameters for L1/L2/Trades
 */
struct SubscribeOptions {
    core::SubscriptionLevel level           = core::SubscriptionLevel::L1;
    std::uint32_t           depth           = 25;       // For L2/L3 order book depth
    std::uint32_t           throttle_ms     = 0;        // Rate limit updates
    bool                    snapshot_only   = false;    // Get snapshot, no streaming
    std::string             start_time;                 // For historical (ISO8601)
    std::string             end_time;                   // For historical (ISO8601)
    
    static SubscribeOptions L1(std::uint32_t throttle_ms = 0) {
        return SubscribeOptions{core::SubscriptionLevel::L1, 1, throttle_ms, false, "", ""};
    }
    
    static SubscribeOptions L2(std::uint32_t depth = 25, std::uint32_t throttle = 0) {
        return SubscribeOptions{core::SubscriptionLevel::L2, depth, throttle, false, "", ""};
    }
    
    static SubscribeOptions Trades(std::uint32_t throttle = 0) {
        return SubscribeOptions{core::SubscriptionLevel::TRADES, 0, throttle, false, "", ""};
    }
    
    static SubscribeOptions Ticker(std::uint32_t throttle = 100) {
        return SubscribeOptions{core::SubscriptionLevel::TICKER, 0, throttle, false, "", ""};
    }
};

/**
 * @brief WebSocket connection state
 */
enum class WSState {
    DISCONNECTED,
    CONNECTING,
    CONNECTED,
    AUTHENTICATING,
    AUTHENTICATED,
    RECONNECTING,
    CLOSING,
    CLOSED,
    ERROR
};

/**
 * @brief Callback types
 */
using WSMessageCallback = std::function<void(const WSMessage&)>;
using WSStateCallback = std::function<void(WSState old_state, WSState new_state)>;
using WSErrorCallback = std::function<void(const std::string& error)>;

/**
 * @brief WebSocket client for Architect Exchange
 * 
 * Features:
 * - Automatic reconnection
 * - Heartbeat/ping-pong
 * - Channel subscription management
 * - Message parsing and dispatching
 * - Thread-safe operation
 */
class WebSocketClient {
public:
    /**
     * @brief Get the singleton instance
     */
    static WebSocketClient& getInstance();
    
    // Prevent copying
    WebSocketClient(const WebSocketClient&) = delete;
    WebSocketClient& operator=(const WebSocketClient&) = delete;
    
    // ==========================================================================
    // Connection Management
    // ==========================================================================
    
    /**
     * @brief Connect to WebSocket server
     */
    bool connect();
    
    /**
     * @brief Disconnect from WebSocket server
     */
    void disconnect();
    
    /**
     * @brief Check if connected
     */
    [[nodiscard]] bool isConnected() const;
    
    /**
     * @brief Get current connection state
     */
    [[nodiscard]] WSState getState() const;
    
    /**
     * @brief Authenticate the WebSocket connection
     */
    bool authenticate(const std::string& session_token);
    
    // ==========================================================================
    // Configuration
    // ==========================================================================
    
    /**
     * @brief Set WebSocket URL
     */
    void setUrl(const std::string& url);
    
    /**
     * @brief Enable/disable auto-reconnect
     */
    void setAutoReconnect(bool enabled);
    
    /**
     * @brief Set reconnect delay
     */
    void setReconnectDelay(std::chrono::milliseconds delay);
    
    /**
     * @brief Set heartbeat interval
     */
    void setHeartbeatInterval(std::chrono::milliseconds interval);
    
    // ==========================================================================
    // Subscriptions
    // ==========================================================================
    
    /**
     * @brief Subscribe to a channel with options
     */
    bool subscribe(const std::string& channel, const std::string& symbol = "", 
                   const SubscribeOptions& options = SubscribeOptions());
    
    /**
     * @brief Subscribe to multiple channels
     */
    bool subscribeMultiple(const std::vector<std::pair<std::string, std::string>>& channels);
    
    /**
     * @brief Subscribe with full parameters struct
     */
    bool subscribeWithParams(const core::SubscriptionParams& params);
    
    /**
     * @brief Unsubscribe from a channel
     */
    bool unsubscribe(const std::string& channel, const std::string& symbol = "");
    
    /**
     * @brief Unsubscribe from all channels
     */
    void unsubscribeAll();
    
    /**
     * @brief Get active subscriptions
     */
    std::vector<Subscription> getSubscriptions() const;
    
    // ==========================================================================
    // Convenience Subscription Methods (with configurable options)
    // ==========================================================================
    
    /**
     * @brief Subscribe to L1 order book (best bid/ask)
     * @param symbol Trading symbol
     * @param throttle_ms Update rate limit in milliseconds (0 = no limit)
     */
    bool subscribeL1(const std::string& symbol, std::uint32_t throttle_ms = 0);
    
    /**
     * @brief Subscribe to L2 order book (market depth)
     * @param symbol Trading symbol
     * @param depth Order book depth (default 25)
     * @param throttle_ms Update rate limit in milliseconds
     */
    bool subscribeL2(const std::string& symbol, std::uint32_t depth = 25, std::uint32_t throttle_ms = 0);
    
    /**
     * @brief Subscribe to orderbook (legacy, defaults to L2)
     */
    bool subscribeOrderbook(const std::string& symbol);
    
    /**
     * @brief Subscribe to ticker updates
     * @param throttle_ms Update rate limit in milliseconds (default 100ms)
     */
    bool subscribeTicker(const std::string& symbol, std::uint32_t throttle_ms = 100);
    
    /**
     * @brief Subscribe to trade executions
     */
    bool subscribeTrades(const std::string& symbol, std::uint32_t throttle_ms = 0);
    
    /**
     * @brief Subscribe to private order updates (requires auth)
     */
    bool subscribeOrders();
    
    /**
     * @brief Subscribe to private fill updates (requires auth)
     */
    bool subscribeFills();
    
    /**
     * @brief Subscribe to private position updates (requires auth)
     */
    bool subscribePositions();
    
    // ==========================================================================
    // Sending Messages
    // ==========================================================================
    
    /**
     * @brief Send a raw message
     */
    bool send(const std::string& message);
    
    /**
     * @brief Send a JSON message
     */
    bool send(const json& message);
    
    /**
     * @brief Send a ping
     */
    void ping();
    
    // ==========================================================================
    // Callbacks
    // ==========================================================================
    
    /**
     * @brief Set callback for all messages
     */
    void setMessageCallback(WSMessageCallback callback);
    
    /**
     * @brief Set callback for specific channel
     */
    void setChannelCallback(const std::string& channel, WSMessageCallback callback);
    
    /**
     * @brief Set state change callback
     */
    void setStateCallback(WSStateCallback callback);
    
    /**
     * @brief Set error callback
     */
    void setErrorCallback(WSErrorCallback callback);
    
    // ==========================================================================
    // Statistics
    // ==========================================================================
    
    struct Stats {
        std::uint64_t messages_received     = 0;
        std::uint64_t messages_sent         = 0;
        std::uint64_t reconnect_count       = 0;
        std::uint64_t bytes_received        = 0;
        std::uint64_t bytes_sent            = 0;
        core::Timestamp connected_at;
        core::Timestamp last_message_at;
    };
    
    [[nodiscard]] Stats getStats() const;
    void resetStats();
    
private:
    WebSocketClient();
    ~WebSocketClient();
    
    void setState(WSState new_state);
    void reconnect();
    void heartbeatLoop();
    void receiveLoop();
    void processMessage(const std::string& message);
    void dispatchMessage(const WSMessage& msg);
    std::string makeSubscriptionKey(const std::string& channel, const std::string& symbol) const;
    
    std::string url_;
    std::atomic<WSState> state_{WSState::DISCONNECTED};
    bool auto_reconnect_ = true;
    std::chrono::milliseconds reconnect_delay_{1000};
    std::chrono::milliseconds heartbeat_interval_{15000};
    
    // Subscriptions
    std::map<std::string, Subscription> subscriptions_;
    mutable std::shared_mutex subscriptions_mutex_;
    
    // Callbacks
    WSMessageCallback message_callback_;
    std::map<std::string, WSMessageCallback> channel_callbacks_;
    WSStateCallback state_callback_;
    WSErrorCallback error_callback_;
    mutable std::shared_mutex callbacks_mutex_;
    
    // Worker threads
    std::thread receive_thread_;
    std::thread heartbeat_thread_;
    std::atomic<bool> running_{false};
    
    // Statistics
    mutable std::mutex stats_mutex_;
    Stats stats_;
    
    std::mutex ws_mutex_;
};

} // namespace api
} // namespace architect
