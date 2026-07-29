#pragma once

/**
 * @file Event.h
 * @brief Event types and base event structure for the event-driven architecture
 */

#include "core/Types.h"
#include <memory>
#include <variant>
#include <any>
#include <string>
#include <chrono>

namespace architect {
namespace events {

using namespace core;

// =============================================================================
// Event Types
// =============================================================================

enum class EventType : std::uint16_t {
    // System Events
    SYSTEM_STARTUP          = 0,
    SYSTEM_SHUTDOWN         = 1,
    SYSTEM_ERROR            = 2,
    HEARTBEAT               = 3,
    
    // Timer Events (for Predictors/Strategies)
    TIMER_EVENT             = 10,       // Time-rack triggered timer
    TIMER_TICK              = 11,       // High-frequency timer tick
    TIMER_SECOND            = 12,       // Second boundary
    TIMER_MINUTE            = 13,       // Minute boundary
    
    // Connection Events
    CONNECTION_ESTABLISHED  = 100,
    CONNECTION_LOST         = 101,
    CONNECTION_RECONNECTING = 102,
    CONNECTION_ERROR        = 103,
    
    // Authentication Events
    AUTH_SUCCESS            = 200,
    AUTH_FAILURE            = 201,
    AUTH_TOKEN_REFRESH      = 202,
    AUTH_SESSION_EXPIRED    = 203,
    
    // Order Events
    ORDER_SUBMITTED         = 300,
    ORDER_ACCEPTED          = 301,
    ORDER_REJECTED          = 302,
    ORDER_FILLED            = 303,
    ORDER_PARTIALLY_FILLED  = 304,
    ORDER_CANCELLED         = 305,
    ORDER_CANCEL_REJECTED   = 306,
    ORDER_MODIFIED          = 307,
    ORDER_MODIFY_REJECTED   = 308,
    ORDER_EXPIRED           = 309,
    
    // Trade Events
    TRADE_EXECUTED          = 400,
    TRADE_BUSTED            = 401,
    
    // Market Data Events
    TICK_UPDATE             = 500,
    L1_UPDATE               = 501,
    L2_UPDATE               = 502,
    TRADE_UPDATE            = 503,
    ORDERBOOK_SNAPSHOT      = 504,
    ORDERBOOK_DELTA         = 505,
    MARKET_STATUS_CHANGE    = 506,
    
    // Position Events
    POSITION_OPENED         = 600,
    POSITION_CLOSED         = 601,
    POSITION_UPDATED        = 602,
    
    // Account Events
    BALANCE_UPDATE          = 700,
    MARGIN_UPDATE           = 701,
    
    // Predictor/Strategy Events
    PREDICTOR_SIGNAL        = 800,      // Predictor generated a signal
    STRATEGY_ACTION         = 801,      // Strategy took an action
    
    // Custom/User Events
    CUSTOM_EVENT            = 1000
};

inline constexpr const char* eventTypeToString(EventType type) noexcept {
    switch (type) {
        case EventType::SYSTEM_STARTUP:          return "SYSTEM_STARTUP";
        case EventType::SYSTEM_SHUTDOWN:         return "SYSTEM_SHUTDOWN";
        case EventType::SYSTEM_ERROR:            return "SYSTEM_ERROR";
        case EventType::HEARTBEAT:               return "HEARTBEAT";
        case EventType::TIMER_EVENT:             return "TIMER_EVENT";
        case EventType::TIMER_TICK:              return "TIMER_TICK";
        case EventType::TIMER_SECOND:            return "TIMER_SECOND";
        case EventType::TIMER_MINUTE:            return "TIMER_MINUTE";
        case EventType::CONNECTION_ESTABLISHED:  return "CONNECTION_ESTABLISHED";
        case EventType::CONNECTION_LOST:         return "CONNECTION_LOST";
        case EventType::CONNECTION_RECONNECTING: return "CONNECTION_RECONNECTING";
        case EventType::CONNECTION_ERROR:        return "CONNECTION_ERROR";
        case EventType::AUTH_SUCCESS:            return "AUTH_SUCCESS";
        case EventType::AUTH_FAILURE:            return "AUTH_FAILURE";
        case EventType::AUTH_TOKEN_REFRESH:      return "AUTH_TOKEN_REFRESH";
        case EventType::AUTH_SESSION_EXPIRED:    return "AUTH_SESSION_EXPIRED";
        case EventType::ORDER_SUBMITTED:         return "ORDER_SUBMITTED";
        case EventType::ORDER_ACCEPTED:          return "ORDER_ACCEPTED";
        case EventType::ORDER_REJECTED:          return "ORDER_REJECTED";
        case EventType::ORDER_FILLED:            return "ORDER_FILLED";
        case EventType::ORDER_PARTIALLY_FILLED:  return "ORDER_PARTIALLY_FILLED";
        case EventType::ORDER_CANCELLED:         return "ORDER_CANCELLED";
        case EventType::ORDER_CANCEL_REJECTED:   return "ORDER_CANCEL_REJECTED";
        case EventType::ORDER_MODIFIED:          return "ORDER_MODIFIED";
        case EventType::ORDER_MODIFY_REJECTED:   return "ORDER_MODIFY_REJECTED";
        case EventType::ORDER_EXPIRED:           return "ORDER_EXPIRED";
        case EventType::TRADE_EXECUTED:          return "TRADE_EXECUTED";
        case EventType::TRADE_BUSTED:            return "TRADE_BUSTED";
        case EventType::TICK_UPDATE:             return "TICK_UPDATE";
        case EventType::L1_UPDATE:               return "L1_UPDATE";
        case EventType::L2_UPDATE:               return "L2_UPDATE";
        case EventType::TRADE_UPDATE:            return "TRADE_UPDATE";
        case EventType::ORDERBOOK_SNAPSHOT:      return "ORDERBOOK_SNAPSHOT";
        case EventType::ORDERBOOK_DELTA:         return "ORDERBOOK_DELTA";
        case EventType::MARKET_STATUS_CHANGE:    return "MARKET_STATUS_CHANGE";
        case EventType::POSITION_OPENED:         return "POSITION_OPENED";
        case EventType::POSITION_CLOSED:         return "POSITION_CLOSED";
        case EventType::POSITION_UPDATED:        return "POSITION_UPDATED";
        case EventType::BALANCE_UPDATE:          return "BALANCE_UPDATE";
        case EventType::MARGIN_UPDATE:           return "MARGIN_UPDATE";
        case EventType::PREDICTOR_SIGNAL:        return "PREDICTOR_SIGNAL";
        case EventType::STRATEGY_ACTION:         return "STRATEGY_ACTION";
        case EventType::CUSTOM_EVENT:            return "CUSTOM_EVENT";
        default:                                 return "UNKNOWN";
    }
}

// =============================================================================
// Event Priority
// =============================================================================

enum class EventPriority : std::uint8_t {
    LOW         = 0,
    NORMAL      = 1,
    HIGH        = 2,
    CRITICAL    = 3
};

// =============================================================================
// Base Event Structure
// =============================================================================

struct Event {
    EventType       type;
    EventPriority   priority;
    SequenceNum     sequence_num;
    Timestamp       timestamp;
    std::string     source;
    std::any        data;
    
    Event() = default;
    
    Event(EventType t, EventPriority p = EventPriority::NORMAL)
        : type(t)
        , priority(p)
        , sequence_num(0)
        , timestamp(std::chrono::high_resolution_clock::now().time_since_epoch())
        , source("")
        , data()
    {}
    
    template<typename T>
    Event(EventType t, const T& payload, EventPriority p = EventPriority::NORMAL)
        : type(t)
        , priority(p)
        , sequence_num(0)
        , timestamp(std::chrono::high_resolution_clock::now().time_since_epoch())
        , source("")
        , data(payload)
    {}
    
    template<typename T>
    T getData() const {
        return std::any_cast<T>(data);
    }
    
    template<typename T>
    bool hasData() const {
        return data.has_value() && data.type() == typeid(T);
    }
    
    bool operator<(const Event& other) const {
        // Higher priority events should come first (for priority queue)
        if (priority != other.priority) {
            return static_cast<int>(priority) < static_cast<int>(other.priority);
        }
        // Earlier events should come first (for same priority)
        return timestamp > other.timestamp;
    }
};

using EventPtr = std::shared_ptr<Event>;

// =============================================================================
// Typed Event Data Structures
// =============================================================================

struct OrderEventData {
    OrderId     order_id;
    UserId      user_id;
    Symbol      symbol;
    Side        side;
    OrderType   order_type;
    OrderStatus status;
    Price       price;
    Quantity    quantity;
    Quantity    filled_quantity;
    Price       avg_fill_price;
    ErrorCode   error_code;
    std::string error_message;
    std::string client_order_id;
    Timestamp   order_timestamp;
    // True when this ORDER_CANCELLED is a NOTIFICATION that the venue already
    // cancelled the order (reconcile sync, or a batch cancel-replace whose HTTP
    // cancel already went out). The Platform wire-cancel handler MUST NOT re-send
    // a cancel for these — doing so double-counts MM_JOB_VENUE_CALLS cancels and
    // double-hits the wire. Left false for the legacy "please cancel" path
    // (OrderManager::cancelOrder), which still needs the handler to send.
    bool        venue_confirmed{false};
};

struct TradeEventData {
    TradeId     trade_id;
    OrderId     order_id;
    Symbol      symbol;
    Side        side;
    Price       price;
    Quantity    quantity;
    Decimal     fee;
    std::string fee_currency;
    Timestamp   execution_time;
};

struct TickEventData {
    Symbol      symbol;
    Price       bid;
    Price       ask;
    Price       last;
    Quantity    bid_size;
    Quantity    ask_size;
    Quantity    last_size;
    Volume      volume;
    Timestamp   exchange_timestamp;
};

struct ConnectionEventData {
    ConnectionStatus    status;
    std::string         endpoint;
    std::string         error_message;
    int                 reconnect_count;
};

struct BalanceEventData {
    std::string currency;
    Decimal     available;
    Decimal     locked;
    Decimal     total;
};

struct PositionEventData {
    Symbol      symbol;
    Side        side;
    Quantity    quantity;
    Price       entry_price;
    Price       current_price;
    Decimal     unrealized_pnl;
    Decimal     realized_pnl;
};

// =============================================================================
// Time Rack Type (HHMMSSMMM format - 8 or 9 digits)
// =============================================================================

/**
 * @brief TimeRack represents time as integer in format HHMMSSMMM
 * Examples:
 *   92900000 = 09:29:00.000 (9:29 AM)
 *   93000000 = 09:30:00.000 (9:30 AM, market open)
 *   160000000 = 16:00:00.000 (4:00 PM, market close)
 *   93015500 = 09:30:15.500 (9:30:15.500 AM)
 */
using TimeRack = std::uint32_t;

/**
 * @brief Date in YYYYMMDD format
 */
using DateInt = std::uint32_t;

/**
 * @brief Combined date-time representation
 */
struct DateTime {
    DateInt     date;           // YYYYMMDD
    TimeRack    time_rack;      // HHMMSSMMM
    
    [[nodiscard]] std::uint64_t toNanos() const;
    [[nodiscard]] std::string toString() const;
    
    static DateTime fromTimestamp(Timestamp ts, DateInt date);
    static DateTime now(DateInt date);
    
    bool operator<(const DateTime& other) const {
        if (date != other.date) return date < other.date;
        return time_rack < other.time_rack;
    }
    
    bool operator==(const DateTime& other) const {
        return date == other.date && time_rack == other.time_rack;
    }
};

/**
 * @brief Helper functions for TimeRack
 */
inline TimeRack makeTimeRack(int hours, int minutes, int seconds, int millis = 0) {
    return static_cast<TimeRack>(hours * 10000000 + minutes * 100000 + seconds * 1000 + millis);
}

inline void parseTimeRack(TimeRack tr, int& hours, int& minutes, int& seconds, int& millis) {
    millis = tr % 1000;
    tr /= 1000;
    seconds = tr % 100;
    tr /= 100;
    minutes = tr % 100;
    hours = tr / 100;
}

inline std::string timeRackToString(TimeRack tr) {
    int h, m, s, ms;
    parseTimeRack(tr, h, m, s, ms);
    char buf[16];
    snprintf(buf, sizeof(buf), "%02d:%02d:%02d.%03d", h, m, s, ms);
    return buf;
}

inline DateInt makeDateInt(int year, int month, int day) {
    return static_cast<DateInt>(year * 10000 + month * 100 + day);
}

inline void parseDateInt(DateInt d, int& year, int& month, int& day) {
    day = d % 100;
    d /= 100;
    month = d % 100;
    year = d / 100;
}

inline std::string dateIntToString(DateInt d) {
    int y, m, day;
    parseDateInt(d, y, m, day);
    char buf[16];
    snprintf(buf, sizeof(buf), "%04d-%02d-%02d", y, m, day);
    return buf;
}

// =============================================================================
// Timer Event Data
// =============================================================================

struct TimerEventData {
    DateInt         date;               // YYYYMMDD
    TimeRack        time_rack;          // HHMMSSMMM
    Timestamp       precise_time;       // Nanosecond timestamp
    std::string     timer_name;         // Optional name for this timer
    std::uint64_t   sequence;           // Timer sequence number
    
    TimerEventData() = default;
    TimerEventData(DateInt d, TimeRack tr, const std::string& name = "")
        : date(d), time_rack(tr)
        , precise_time(std::chrono::high_resolution_clock::now().time_since_epoch())
        , timer_name(name), sequence(0) {}
};

// =============================================================================
// Predictor Signal Event Data
// =============================================================================

struct PredictorSignalData {
    std::string     predictor_name;     // Name of the predictor
    std::string     signal_name;        // Signal identifier
    Symbol          symbol;             // Related symbol (if any)
    double          signal_value;       // Numeric signal value
    double          confidence;         // Confidence level [0.0, 1.0]
    DateInt         date;
    TimeRack        time_rack;
    Timestamp       compute_time;       // When computation finished
    std::string     metadata;           // Additional JSON metadata
};

// =============================================================================
// Strategy Action Event Data  
// =============================================================================

enum class StrategyActionType : std::uint8_t {
    NONE            = 0,
    SUBMIT_ORDER    = 1,
    CANCEL_ORDER    = 2,
    MODIFY_ORDER    = 3,
    CLOSE_POSITION  = 4,
    HEDGE           = 5,
    SIGNAL_ONLY     = 6
};

struct StrategyActionData {
    std::string         strategy_name;
    StrategyActionType  action_type;
    Symbol              symbol;
    Side                side;
    OrderType           order_type;
    Price               price;
    Quantity            quantity;
    std::string         reason;         // Why this action was taken
    DateInt             date;
    TimeRack            time_rack;
    std::string         metadata;       // Additional JSON metadata
};

// =============================================================================
// Event Factory
// =============================================================================

class EventFactory {
public:
    static EventPtr createOrderEvent(EventType type, const OrderEventData& data, 
                                     EventPriority priority = EventPriority::HIGH) {
        auto event = std::make_shared<Event>(type, data, priority);
        return event;
    }
    
    static EventPtr createTradeEvent(const TradeEventData& data,
                                     EventPriority priority = EventPriority::HIGH) {
        auto event = std::make_shared<Event>(EventType::TRADE_EXECUTED, data, priority);
        return event;
    }
    
    static EventPtr createTickEvent(const TickEventData& data,
                                    EventPriority priority = EventPriority::NORMAL) {
        auto event = std::make_shared<Event>(EventType::TICK_UPDATE, data, priority);
        return event;
    }
    
    static EventPtr createConnectionEvent(EventType type, const ConnectionEventData& data,
                                          EventPriority priority = EventPriority::CRITICAL) {
        auto event = std::make_shared<Event>(type, data, priority);
        return event;
    }
    
    static EventPtr createSystemEvent(EventType type, const std::string& message = "",
                                      EventPriority priority = EventPriority::CRITICAL) {
        auto event = std::make_shared<Event>(type, message, priority);
        return event;
    }
};

} // namespace events
} // namespace architect
