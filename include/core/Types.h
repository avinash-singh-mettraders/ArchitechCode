#pragma once

/**
 * @file Types.h
 * @brief Core type definitions for the Architect Platform Core
 * 
 * This file contains all fundamental type aliases, enums, and POD structures
 * used throughout the trading platform. Designed for ultra-low latency with
 * cache-friendly layouts and minimal indirection.
 */

#include <cstdint>
#include <chrono>
#include <string>
#include <string_view>
#include <array>
#include <optional>
#include <variant>

namespace architect {
namespace core {

// =============================================================================
// Type Aliases
// =============================================================================

using OrderId       = std::uint64_t;
using UserId        = std::uint64_t;
using TradeId       = std::uint64_t;
using InstrumentId  = std::uint64_t;
using SequenceNum   = std::uint64_t;
using RequestId     = std::uint64_t;

using Price         = double;
using Quantity      = double;
using Volume        = double;
using Decimal       = double;

using Timestamp     = std::chrono::nanoseconds;
using TimePoint     = std::chrono::time_point<std::chrono::high_resolution_clock>;

// Fixed-size string for symbol names (cache-friendly)
constexpr std::size_t MAX_SYMBOL_LENGTH = 32;
using Symbol = std::array<char, MAX_SYMBOL_LENGTH>;

// =============================================================================
// Enumerations
// =============================================================================

enum class Side : std::uint8_t {
    BUY  = 0,
    SELL = 1
};

enum class OrderType : std::uint8_t {
    MARKET          = 0,
    LIMIT           = 1,
    STOP            = 2,
    STOP_LIMIT      = 3,
    TRAILING_STOP   = 4,
    FILL_OR_KILL    = 5,
    IMMEDIATE_OR_CANCEL = 6,
    GOOD_TIL_CANCELLED  = 7,
    GOOD_TIL_DATE       = 8
};

enum class OrderStatus : std::uint8_t {
    PENDING         = 0,
    NEW             = 1,
    ACCEPTED        = 2,
    REJECTED        = 3,
    PARTIALLY_FILLED = 4,
    FILLED          = 5,
    CANCELLED       = 6,
    EXPIRED         = 7,
    REPLACED        = 8
};

enum class TimeInForce : std::uint8_t {
    DAY             = 0,
    GTC             = 1,  // Good til cancelled
    IOC             = 2,  // Immediate or cancel
    FOK             = 3,  // Fill or kill
    GTD             = 4,  // Good til date
    AT_OPEN         = 5,
    AT_CLOSE        = 6
};

enum class OrderAction : std::uint8_t {
    NEW             = 0,
    MODIFY          = 1,
    CANCEL          = 2,
    REPLACE         = 3
};

enum class ExecutionType : std::uint8_t {
    NEW             = 0,
    TRADE           = 1,
    CANCELLED       = 2,
    REPLACED        = 3,
    REJECTED        = 4,
    EXPIRED         = 5
};

enum class MarketStatus : std::uint8_t {
    CLOSED          = 0,
    PRE_OPEN        = 1,
    OPEN            = 2,
    HALTED          = 3,
    AUCTION         = 4,
    POST_CLOSE      = 5
};

enum class ConnectionStatus : std::uint8_t {
    DISCONNECTED    = 0,
    CONNECTING      = 1,
    CONNECTED       = 2,
    RECONNECTING    = 3,
    ERROR           = 4
};

/**
 * @brief Feed mode - determines whether data is live or simulated
 */
enum class FeedMode : std::uint8_t {
    UNKNOWN         = 0,    // Not yet determined
    LIVE            = 1,    // Real-time production feed
    PAPER           = 2,    // Paper trading (real prices, simulated orders)
    SIMULATION      = 3,    // Historical replay simulation
    BACKTEST        = 4     // Offline backtesting mode
};

/**
 * @brief Feed source type
 */
enum class FeedSource : std::uint8_t {
    WEBSOCKET_LIVE      = 0,    // Live WebSocket stream
    WEBSOCKET_DELAYED   = 1,    // Delayed WebSocket stream
    REST_POLLING        = 2,    // REST API polling
    HISTORICAL_FILE     = 3,    // Historical data file replay
    HISTORICAL_DB       = 4,    // Historical data database replay
    MOCK                = 5     // Mock data for testing
};

/**
 * @brief Market data subscription level
 */
enum class SubscriptionLevel : std::uint8_t {
    L1              = 0,    // Top of book (BBO)
    L2              = 1,    // Market depth (order book)
    L3              = 2,    // Full order book with order IDs
    TRADES          = 3,    // Trade executions
    TICKER          = 4     // Ticker/summary data
};

enum class ErrorCode : std::uint16_t {
    SUCCESS                 = 0,
    UNKNOWN_ERROR           = 1,
    INVALID_REQUEST         = 100,
    INVALID_SYMBOL          = 101,
    INVALID_QUANTITY        = 102,
    INVALID_PRICE           = 103,
    INVALID_ORDER_TYPE      = 104,
    INVALID_SIDE            = 105,
    INVALID_TIME_IN_FORCE   = 106,
    ORDER_NOT_FOUND         = 200,
    ORDER_ALREADY_CANCELLED = 201,
    ORDER_ALREADY_FILLED    = 202,
    INSUFFICIENT_BALANCE    = 300,
    INSUFFICIENT_MARGIN     = 301,
    POSITION_NOT_FOUND      = 302,
    AUTHENTICATION_FAILED   = 400,
    UNAUTHORIZED            = 401,
    SESSION_EXPIRED         = 402,
    RATE_LIMIT_EXCEEDED     = 429,
    MARKET_CLOSED           = 500,
    TRADING_HALTED          = 501,
    CONNECTION_ERROR        = 600,
    TIMEOUT                 = 601,
    NETWORK_ERROR           = 602
};

// =============================================================================
// Utility Functions
// =============================================================================

inline constexpr const char* sideToString(Side side) noexcept {
    switch (side) {
        case Side::BUY:  return "BUY";
        case Side::SELL: return "SELL";
        default:         return "UNKNOWN";
    }
}

inline constexpr const char* orderTypeToString(OrderType type) noexcept {
    switch (type) {
        case OrderType::MARKET:             return "MARKET";
        case OrderType::LIMIT:              return "LIMIT";
        case OrderType::STOP:               return "STOP";
        case OrderType::STOP_LIMIT:         return "STOP_LIMIT";
        case OrderType::TRAILING_STOP:      return "TRAILING_STOP";
        case OrderType::FILL_OR_KILL:       return "FOK";
        case OrderType::IMMEDIATE_OR_CANCEL: return "IOC";
        case OrderType::GOOD_TIL_CANCELLED: return "GTC";
        case OrderType::GOOD_TIL_DATE:      return "GTD";
        default:                            return "UNKNOWN";
    }
}

inline constexpr const char* orderStatusToString(OrderStatus status) noexcept {
    switch (status) {
        case OrderStatus::PENDING:          return "PENDING";
        case OrderStatus::NEW:              return "NEW";
        case OrderStatus::ACCEPTED:         return "ACCEPTED";
        case OrderStatus::REJECTED:         return "REJECTED";
        case OrderStatus::PARTIALLY_FILLED: return "PARTIALLY_FILLED";
        case OrderStatus::FILLED:           return "FILLED";
        case OrderStatus::CANCELLED:        return "CANCELLED";
        case OrderStatus::EXPIRED:          return "EXPIRED";
        case OrderStatus::REPLACED:         return "REPLACED";
        default:                            return "UNKNOWN";
    }
}

inline constexpr Side oppositeSide(Side side) noexcept {
    return side == Side::BUY ? Side::SELL : Side::BUY;
}

inline constexpr const char* feedModeToString(FeedMode mode) noexcept {
    switch (mode) {
        case FeedMode::UNKNOWN:     return "UNKNOWN";
        case FeedMode::LIVE:        return "LIVE";
        case FeedMode::PAPER:       return "PAPER";
        case FeedMode::SIMULATION:  return "SIMULATION";
        case FeedMode::BACKTEST:    return "BACKTEST";
        default:                    return "UNKNOWN";
    }
}

inline constexpr const char* feedSourceToString(FeedSource source) noexcept {
    switch (source) {
        case FeedSource::WEBSOCKET_LIVE:    return "WEBSOCKET_LIVE";
        case FeedSource::WEBSOCKET_DELAYED: return "WEBSOCKET_DELAYED";
        case FeedSource::REST_POLLING:      return "REST_POLLING";
        case FeedSource::HISTORICAL_FILE:   return "HISTORICAL_FILE";
        case FeedSource::HISTORICAL_DB:     return "HISTORICAL_DB";
        case FeedSource::MOCK:              return "MOCK";
        default:                            return "UNKNOWN";
    }
}

inline constexpr const char* subscriptionLevelToString(SubscriptionLevel level) noexcept {
    switch (level) {
        case SubscriptionLevel::L1:     return "L1";
        case SubscriptionLevel::L2:     return "L2";
        case SubscriptionLevel::L3:     return "L3";
        case SubscriptionLevel::TRADES: return "TRADES";
        case SubscriptionLevel::TICKER: return "TICKER";
        default:                        return "UNKNOWN";
    }
}

// Helper to create Symbol from string
inline Symbol makeSymbol(std::string_view str) noexcept {
    Symbol sym{};
    const auto len = std::min(str.size(), MAX_SYMBOL_LENGTH - 1);
    std::copy_n(str.begin(), len, sym.begin());
    return sym;
}

inline std::string_view symbolToString(const Symbol& sym) noexcept {
    return std::string_view(sym.data());
}

// =============================================================================
// Core Structures (Cache-line optimized)
// =============================================================================

/**
 * @brief Price level in the order book
 */
struct alignas(32) PriceLevel {
    Price       price       = 0.0;
    Quantity    quantity    = 0.0;
    std::uint32_t order_count = 0;
    std::uint32_t padding     = 0;
};

/**
 * @brief Market tick data (best bid/ask)
 */
struct alignas(64) Tick {
    Symbol      symbol;
    Price       bid_price   = 0.0;
    Price       ask_price   = 0.0;
    Quantity    bid_size    = 0.0;
    Quantity    ask_size    = 0.0;
    Price       last_price  = 0.0;
    Quantity    last_size   = 0.0;
    Volume      volume_24h  = 0.0;
    Timestamp   timestamp;
};

/**
 * @brief Trade execution
 */
struct alignas(64) Trade {
    TradeId     trade_id    = 0;
    OrderId     order_id    = 0;
    OrderId     counter_order_id = 0;
    Symbol      symbol;
    Side        side        = Side::BUY;
    Price       price       = 0.0;
    Quantity    quantity    = 0.0;
    Timestamp   executed_at;
    Decimal     fee         = 0.0;
    std::array<char, 8> fee_currency;
};

/**
 * @brief User account information (matches Architect /whoami response)
 */
struct WhoAmIResponse {
    UserId      id;
    std::string username;
    std::string created_at;
    bool        enabled_2fa     = false;
    bool        is_onboarded    = false;
    bool        is_close_only   = false;
    bool        is_frozen       = false;
    bool        is_admin        = false;
    Decimal     maker_fee       = 0.0;
    Decimal     taker_fee       = 0.0;
};

/**
 * @brief Account balance
 */
struct Balance {
    std::string currency;
    Decimal     available   = 0.0;
    Decimal     locked      = 0.0;
    Decimal     total       = 0.0;
};

/**
 * @brief Trading position
 */
struct Position {
    Symbol      symbol;
    Side        side        = Side::BUY;
    Quantity    quantity    = 0.0;
    Price       entry_price = 0.0;
    Price       mark_price  = 0.0;
    Decimal     unrealized_pnl = 0.0;
    Decimal     realized_pnl   = 0.0;
    Decimal     margin      = 0.0;
    Timestamp   opened_at;
    Timestamp   updated_at;
};

/**
 * @brief Feed configuration - how to fetch market data
 */
struct FeedConfig {
    FeedMode            mode                = FeedMode::UNKNOWN;
    FeedSource          source              = FeedSource::WEBSOCKET_LIVE;
    SubscriptionLevel   default_level       = SubscriptionLevel::L1;
    bool                subscribe_trades    = true;
    bool                subscribe_ticker    = true;
    std::uint32_t       l2_depth            = 25;       // Order book depth for L2
    std::uint32_t       snapshot_interval_ms = 0;       // 0 = use streaming
    std::uint32_t       throttle_ms         = 0;        // Rate limit (0 = no limit)
    
    // Simulation-specific settings
    std::string         historical_start_date;          // YYYYMMDD
    std::string         historical_end_date;            // YYYYMMDD
    std::string         historical_data_path;           // Path to historical data files
    double              replay_speed        = 1.0;      // 1.0 = realtime, 2.0 = 2x speed
    bool                loop_replay         = false;    // Loop when data ends
};

/**
 * @brief Market status response from exchange
 */
struct MarketStatusResponse {
    bool                is_live             = false;    // True if live trading
    bool                is_paper            = false;    // True if paper trading
    bool                is_maintenance      = false;    // True if under maintenance
    MarketStatus        status              = MarketStatus::CLOSED;
    std::string         environment;                    // "production", "sandbox", "paper"
    std::string         server_time;                    // Server timestamp
    std::uint64_t       latency_ms          = 0;        // Round-trip latency
};

/**
 * @brief WebSocket subscription parameters
 */
struct SubscriptionParams {
    std::string         symbol;                         // Symbol to subscribe
    SubscriptionLevel   level               = SubscriptionLevel::L1;
    std::uint32_t       depth               = 25;       // For L2/L3
    bool                snapshot_only       = false;    // Get snapshot, no updates
    std::uint32_t       throttle_ms         = 0;        // Rate limit updates
    
    // For historical replay
    std::string         start_time;                     // ISO8601 or YYYYMMDD
    std::string         end_time;                       // ISO8601 or YYYYMMDD
};

// =============================================================================
// Result Types
// =============================================================================

template<typename T>
struct Result {
    std::optional<T> value;
    ErrorCode error_code = ErrorCode::SUCCESS;
    std::string error_message;
    
    [[nodiscard]] bool isSuccess() const noexcept { 
        return error_code == ErrorCode::SUCCESS && value.has_value(); 
    }
    [[nodiscard]] bool isError() const noexcept { 
        return error_code != ErrorCode::SUCCESS; 
    }
    
    static Result success(T val) {
        return Result{std::move(val), ErrorCode::SUCCESS, ""};
    }
    
    static Result error(ErrorCode code, std::string msg = "") {
        return Result{std::nullopt, code, std::move(msg)};
    }
};

} // namespace core
} // namespace architect
