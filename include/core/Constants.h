#pragma once

/**
 * @file Constants.h
 * @brief Global constants and compile-time configuration
 */

#include <cstddef>
#include <cstdint>
#include <chrono>

namespace architect {
namespace core {
namespace constants {

// =============================================================================
// Network Configuration
// =============================================================================
// Defaults point at AX PRODUCTION (real funds). Override via config (api.rest_endpoint /
// api.ws_endpoint) or credentials.local.json to use the sandbox
// (https://gateway.sandbox.architect.exchange/api / wss://gateway.sandbox.architect.exchange/orders/ws).
constexpr const char* DEFAULT_REST_ENDPOINT     = "https://gateway.architect.exchange/api";
constexpr const char* DEFAULT_WS_ENDPOINT       = "wss://gateway.architect.exchange/orders/ws";
constexpr std::uint16_t DEFAULT_REST_PORT       = 443;
constexpr std::uint16_t DEFAULT_WS_PORT         = 443;

// =============================================================================
// Timeouts (milliseconds)
// =============================================================================
constexpr std::chrono::milliseconds DEFAULT_CONNECT_TIMEOUT{5000};
constexpr std::chrono::milliseconds DEFAULT_READ_TIMEOUT{30000};
constexpr std::chrono::milliseconds DEFAULT_WRITE_TIMEOUT{5000};
constexpr std::chrono::milliseconds DEFAULT_HEARTBEAT_INTERVAL{15000};
constexpr std::chrono::milliseconds DEFAULT_RECONNECT_DELAY{1000};
constexpr std::chrono::milliseconds MAX_RECONNECT_DELAY{60000};

// =============================================================================
// Buffer Sizes
// =============================================================================
constexpr std::size_t DEFAULT_EVENT_QUEUE_SIZE      = 1'000'000;
constexpr std::size_t DEFAULT_ORDER_QUEUE_SIZE      = 100'000;
constexpr std::size_t DEFAULT_MARKET_DATA_QUEUE_SIZE = 500'000;
constexpr std::size_t DEFAULT_WS_BUFFER_SIZE        = 65536;
constexpr std::size_t DEFAULT_HTTP_BUFFER_SIZE      = 16384;

// =============================================================================
// Order Book Configuration
// =============================================================================
constexpr std::size_t DEFAULT_ORDER_BOOK_DEPTH      = 100;
constexpr std::size_t MAX_ORDER_BOOK_DEPTH          = 1000;
constexpr std::size_t DEFAULT_L1_DEPTH              = 1;
constexpr std::size_t DEFAULT_L2_DEPTH              = 25;

// =============================================================================
// Rate Limiting
// =============================================================================
constexpr std::uint32_t DEFAULT_MAX_REQUESTS_PER_SECOND = 100;
constexpr std::uint32_t DEFAULT_MAX_ORDERS_PER_SECOND   = 50;
constexpr std::uint32_t DEFAULT_MAX_CONNECTIONS         = 10;

// =============================================================================
// Precision
// =============================================================================
constexpr double PRICE_EPSILON      = 1e-8;
constexpr double QUANTITY_EPSILON   = 1e-8;
constexpr int DEFAULT_PRICE_PRECISION    = 8;
constexpr int DEFAULT_QUANTITY_PRECISION = 8;

// =============================================================================
// Thread Pool
// =============================================================================
constexpr std::size_t DEFAULT_WORKER_THREADS = 4;
constexpr std::size_t MIN_WORKER_THREADS     = 1;
constexpr std::size_t MAX_WORKER_THREADS     = 64;

// =============================================================================
// API Paths (Architect Exchange)
// =============================================================================
namespace api {
    // Authentication (Architect Exchange: exchange api_key+api_secret for bearer token)
    // https://docs.architect.exchange/api-reference/user-management/authenticate.md
    constexpr const char* AUTHENTICATE   = "/authenticate";
    // User Management
    constexpr const char* WHOAMI            = "/whoami";
    
    // Orders (REST /api/orders is legacy; order placement uses orders gateway)
    constexpr const char* ORDERS            = "/orders";
    constexpr const char* ORDERS_CANCEL     = "/orders/cancel";
    constexpr const char* ORDERS_CANCEL_ALL = "/orders/cancel-all";
    /** Orders gateway: POST cancel all (Architect); optional JSON {"symbol":"..."} or {}. */
    constexpr const char* CANCEL_ALL_ORDERS     = "/cancel-all-orders";
    constexpr const char* CANCEL_ALL_ORDERS_ALT = "/cancel_all_orders";
    /** Orders gateway: POST to base orders URL + PLACE_ORDER. Body: s, d, q, p, tif, po. */
    constexpr const char* PLACE_ORDER       = "/place_order";
    /** Orders gateway: POST to base orders URL + CANCEL_ORDER. Body: {"oid": "..."}. */
    constexpr const char* CANCEL_ORDER      = "/cancel_order";
    /** Orders gateway: POST to base orders URL + MODIFY_ORDER. Body: s, oid, p, q (see Platform). */
    constexpr const char* MODIFY_ORDER      = "/modify_order";
    
    // Market Data
    /** Exchange instrument catalog (tick_size, minimum_order_size, …). GET {api.rest_endpoint}/instruments */
    constexpr const char* INSTRUMENTS       = "/instruments";
    constexpr const char* MARKETS           = "/markets";
    constexpr const char* TICKER            = "/ticker";
    constexpr const char* ORDERBOOK         = "/orderbook";
    constexpr const char* TRADES            = "/trades";
    constexpr const char* CANDLES           = "/candles";
    
    // Account
    constexpr const char* ACCOUNTS          = "/accounts";
    constexpr const char* BALANCES          = "/balances";
    constexpr const char* POSITIONS         = "/positions";
    constexpr const char* FILLS             = "/fills";
    
    // WebSocket Channels
    constexpr const char* WS_TICKER         = "ticker";
    constexpr const char* WS_ORDERBOOK      = "orderbook";
    constexpr const char* WS_TRADES         = "trades";
    constexpr const char* WS_ORDERS         = "orders";
    constexpr const char* WS_FILLS          = "fills";
    constexpr const char* WS_POSITIONS      = "positions";
}

// =============================================================================
// Version Information
// =============================================================================
constexpr const char* PLATFORM_VERSION  = "1.0.0";
constexpr const char* API_VERSION       = "v1";

} // namespace constants
} // namespace core
} // namespace architect
