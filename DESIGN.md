# Architect Platform Core - System Design Document

## Table of Contents
1. [Overview](#overview)
2. [Architecture Diagram](#architecture-diagram)
3. [Module Design](#module-design)
4. [API Integration](#api-integration)
5. [Event System](#event-system)
6. [Data Flow](#data-flow)
7. [Latency Optimizations](#latency-optimizations)

---

## Overview

The Architect Platform Core is an ultra-low-latency C++ trading platform designed to interface with the [Architect Exchange API](https://docs.architect.exchange/api-reference/user-management/get-whoami). It provides:

- **Event-driven architecture** for nanosecond-level responsiveness
- **Modular design** with clear separation of concerns
- **Configurable components** via JSON configuration
- **Strategy framework** for algorithmic trading
- **Comprehensive logging** with CSV output for analysis

### Key Design Principles
- Minimize lock contention in hot paths
- Use lock-free data structures where possible
- Event-driven rather than polling-based
- Cache-friendly memory layouts (aligned structures)
- Zero-copy message passing where feasible

---

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              ARCHITECT PLATFORM CORE                             │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │                         CONFIGURATION LAYER                              │   │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐    │   │
│  │  │   Config    │  │  Logger     │  │ SimParams   │  │  Constants  │    │   │
│  │  │   (JSON)    │  │  (CSV/Log)  │  │ (CLI Args)  │  │  (Compile)  │    │   │
│  │  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘    │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                      │                                          │
│                                      ▼                                          │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │                           PLATFORM (God Object)                          │   │
│  │                                                                          │   │
│  │   initialize() ──► Validates config, creates log dirs, init modules     │   │
│  │   start()      ──► Verifies feed, starts event loop                     │   │
│  │   stop()       ──► Graceful shutdown, final metrics                     │   │
│  │                                                                          │   │
│  │   Accessors: config(), logger(), events(), orders(), marketdata()...    │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                      │                                          │
│         ┌────────────────────────────┼────────────────────────────┐            │
│         ▼                            ▼                            ▼            │
│  ┌─────────────┐           ┌─────────────────┐           ┌─────────────┐      │
│  │    API      │           │  EVENT MANAGER  │           │   MARKET    │      │
│  │   LAYER     │           │                 │           │    DATA     │      │
│  │             │           │  ┌───────────┐  │           │             │      │
│  │ RestClient  │◄─────────►│  │  Queue    │  │◄─────────►│ MarketData  │      │
│  │ WebSocket   │           │  │ (Priority)│  │           │  Manager    │      │
│  │   Client    │           │  └───────────┘  │           │             │      │
│  └─────────────┘           │        │        │           │ OrderBook   │      │
│         │                  │        ▼        │           │  Manager    │      │
│         │                  │  ┌───────────┐  │           └─────────────┘      │
│         │                  │  │ Dispatch  │  │                  │             │
│         │                  │  │ (Workers) │  │                  │             │
│         │                  │  └───────────┘  │                  │             │
│         │                  └────────┬────────┘                  │             │
│         │                           │                           │             │
│         │         ┌─────────────────┼─────────────────┐        │             │
│         │         ▼                 ▼                 ▼        │             │
│         │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  │             │
│         │  │   TIMER     │  │ PREDICTOR   │  │  STRATEGY   │  │             │
│         │  │   SYSTEM    │  │  MANAGER    │  │  MANAGER    │  │             │
│         │  │             │  │             │  │             │  │             │
│         │  │ TimeRack    │  │ Predictors[]│  │ Strategies[]│  │             │
│         │  │ Scheduler   │  │ on_timer()  │  │ on_signal() │  │             │
│         │  │             │  │ on_tick()   │  │ on_fill()   │  │             │
│         │  └─────────────┘  └─────────────┘  └─────────────┘  │             │
│         │         │                 │                 │        │             │
│         │         └─────────────────┴─────────────────┘        │             │
│         │                           │                          │             │
│         │                           ▼                          │             │
│         │                  ┌─────────────────┐                 │             │
│         │                  │ ORDER MANAGER   │◄────────────────┘             │
│         │                  │                 │                               │
│         └─────────────────►│ createOrder()   │                               │
│                            │ cancelOrder()   │                               │
│                            │ modifyOrder()   │                               │
│                            └─────────────────┘                               │
│                                     │                                         │
│                                     ▼                                         │
│                            ┌─────────────────┐                               │
│                            │    PORTFOLIO    │                               │
│                            │    MANAGER      │                               │
│                            │                 │                               │
│                            │ Positions       │                               │
│                            │ Balances        │                               │
│                            │ P&L Tracking    │                               │
│                            └─────────────────┘                               │
│                                                                              │
└──────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                        ARCHITECT EXCHANGE (External)                          │
│                                                                               │
│  REST API: https://gateway.architect.exchange/api  (sandbox: gateway.sandbox.architect.exchange)  │
│  WebSocket: wss://gateway.architect.exchange/orders/ws                        │
│                                                                               │
└───────────────────────────────────────────────────────────────────────────────┘
```

---

## Module Design

### 1. Core Module (`include/core/`)

| File | Purpose |
|------|---------|
| `Platform.h` | God object - central access to all components |
| `Types.h` | Type aliases, enums, POD structures (cache-aligned) |
| `Constants.h` | Compile-time constants, API paths |

### 2. Configuration Module (`include/config/`)

| File | Purpose |
|------|---------|
| `Config.h` | JSON config loading, type-safe accessors |

### 3. Events Module (`include/events/`)

| File | Purpose |
|------|---------|
| `Event.h` | Event types, TimeRack, event data structures |
| `EventManager.h` | Priority queue, worker threads, pub/sub |

### 4. Strategy Module (`include/strategy/`)

| File | Purpose |
|------|---------|
| `Predictor.h` | Base class for signal generators |
| `Strategy.h` | Base class for trading strategies |
| `TimerSystem.h` | Nanosecond-precision timer scheduling |

### 5. API Module (`include/api/`)

| File | Purpose |
|------|---------|
| `RestClient.h` | HTTP REST client with CURL |
| `WebSocketClient.h` | WebSocket streaming client |

### 6. Market Data Module (`include/marketdata/`)

| File | Purpose |
|------|---------|
| `MarketDataManager.h` | Tick aggregation, book management |

### 7. Orders Module (`include/orders/`)

| File | Purpose |
|------|---------|
| `Order.h` | Order validation |
| `OrderManager.h` | Order lifecycle management |
| `OrderBook.h` | L1/L2/L3 book representation |

### 8. Portfolio Module (`include/portfolio/`)

| File | Purpose |
|------|---------|
| `PortfolioManager.h` | Position tracking, P&L calculation |

### 9. User Module (`include/user/`)

| File | Purpose |
|------|---------|
| `User.h` | User validation |
| `UserManager.h` | Authentication, session management |

### 10. Utils Module (`include/utils/`)

| File | Purpose |
|------|---------|
| `Logger.h` | Structured logging, CSV output |

---

## API Integration

### REST API Endpoints (Architect Exchange)

Based on [Architect Exchange API Documentation](https://docs.architect.exchange/api-reference/user-management/get-whoami):

| Endpoint | Method | Description | Implemented |
|----------|--------|-------------|-------------|
| `/whoami` | GET | Get current user details | ✅ |
| `/orders` | GET | List orders | ✅ |
| `/orders` | POST | Create order | ✅ |
| `/orders/{id}` | GET | Get order by ID | ✅ |
| `/orders/{id}` | DELETE | Cancel order | ✅ |
| `/orders/cancel-all` | DELETE | Cancel all orders | ✅ |
| `/markets` | GET | List available markets | ✅ |
| `/ticker` | GET | Get ticker for symbol | ✅ |
| `/orderbook` | GET | Get order book snapshot | ✅ |
| `/trades` | GET | Get recent trades | ✅ |
| `/candles` | GET | Get OHLCV candles | ✅ |
| `/accounts` | GET | Get accounts | ✅ |
| `/balances` | GET | Get balances | ✅ |
| `/positions` | GET | Get positions | ✅ |
| `/fills` | GET | Get fills/executions | ✅ |

### WebSocket Channels

| Channel | Description | Implemented |
|---------|-------------|-------------|
| `ticker` | Real-time ticker updates | ✅ |
| `orderbook` | L1/L2 book updates | ✅ |
| `trades` | Trade executions | ✅ |
| `orders` | Private order updates | ✅ |
| `fills` | Private fill updates | ✅ |
| `positions` | Private position updates | ✅ |

### WhoAmI Response Schema

From the API documentation:

```cpp
struct WhoAmIResponse {
    std::string id;           // User ID
    std::string username;     // Username
    std::string created_at;   // ISO8601 timestamp
    bool enabled_2fa;         // 2FA enabled
    bool is_onboarded;        // Onboarding complete
    bool is_close_only;       // Close-only mode (paper trading indicator)
    bool is_frozen;           // Account frozen
    bool is_admin;            // Admin privileges
    std::string maker_fee;    // Maker fee rate
    std::string taker_fee;    // Taker fee rate
};
```

---

## Event System

### Event Types

```
┌─────────────────────────────────────────────────────────────────┐
│                        EVENT TYPES                              │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  SYSTEM EVENTS (0-99)         TIMER EVENTS (10-19)             │
│  ├── SYSTEM_STARTUP (0)       ├── TIMER_EVENT (10)             │
│  ├── SYSTEM_SHUTDOWN (1)      ├── TIMER_TICK (11)              │
│  ├── SYSTEM_ERROR (2)         ├── TIMER_SECOND (12)            │
│  └── HEARTBEAT (3)            └── TIMER_MINUTE (13)            │
│                                                                 │
│  CONNECTION EVENTS (100-199)  AUTH EVENTS (200-299)            │
│  ├── CONNECTION_ESTABLISHED   ├── AUTH_SUCCESS                 │
│  ├── CONNECTION_LOST          ├── AUTH_FAILURE                 │
│  ├── CONNECTION_RECONNECTING  ├── AUTH_TOKEN_REFRESH           │
│  └── CONNECTION_ERROR         └── AUTH_SESSION_EXPIRED         │
│                                                                 │
│  ORDER EVENTS (300-399)       TRADE EVENTS (400-499)           │
│  ├── ORDER_SUBMITTED          ├── TRADE_EXECUTED               │
│  ├── ORDER_ACCEPTED           └── TRADE_BUSTED                 │
│  ├── ORDER_REJECTED                                            │
│  ├── ORDER_FILLED             MARKET DATA EVENTS (500-599)     │
│  ├── ORDER_PARTIALLY_FILLED   ├── TICK_UPDATE                  │
│  ├── ORDER_CANCELLED          ├── L1_UPDATE                    │
│  ├── ORDER_CANCEL_REJECTED    ├── L2_UPDATE                    │
│  ├── ORDER_MODIFIED           ├── TRADE_UPDATE                 │
│  ├── ORDER_MODIFY_REJECTED    ├── ORDERBOOK_SNAPSHOT           │
│  └── ORDER_EXPIRED            ├── ORDERBOOK_DELTA              │
│                               └── MARKET_STATUS_CHANGE         │
│  POSITION EVENTS (600-699)                                      │
│  ├── POSITION_OPENED          ACCOUNT EVENTS (700-799)         │
│  ├── POSITION_CLOSED          ├── BALANCE_UPDATE               │
│  └── POSITION_UPDATED         └── MARGIN_UPDATE                │
│                                                                 │
│  STRATEGY EVENTS (800-899)                                      │
│  ├── PREDICTOR_SIGNAL (800)                                    │
│  └── STRATEGY_ACTION (801)                                     │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### TimeRack Format

```
TimeRack: HHMMSSMMM (8-9 digits)
─────────────────────────────────
  92900000 = 09:29:00.000 (Pre-market)
  93000000 = 09:30:00.000 (Market Open)
  93015500 = 09:30:15.500
 160000000 = 16:00:00.000 (Market Close)

DateInt: YYYYMMDD (8 digits)
─────────────────────────────────
  20260130 = January 30, 2026
```

---

## Data Flow

### Market Data Flow

```
┌──────────────────────────────────────────────────────────────────────────┐
│                         MARKET DATA FLOW                                  │
└──────────────────────────────────────────────────────────────────────────┘

    Architect Exchange                    Platform Core
    ────────────────                     ──────────────

    WebSocket Server                     WebSocketClient
         │                                     │
         │ {"channel":"ticker",...}           │
         ├────────────────────────────────────►│
         │                                     │
         │                              ┌──────▼──────┐
         │                              │  Message    │
         │                              │  Parser     │
         │                              └──────┬──────┘
         │                                     │
         │                              ┌──────▼──────┐
         │                              │ MarketData  │
         │                              │  Manager    │
         │                              │             │
         │                              │ updateTick()│
         │                              │ updateBook()│
         │                              └──────┬──────┘
         │                                     │
         │                              ┌──────▼──────┐
         │                              │   Event     │
         │                              │  Factory    │
         │                              │             │
         │                              │TICK_UPDATE  │
         │                              │L1_UPDATE    │
         │                              │L2_UPDATE    │
         │                              └──────┬──────┘
         │                                     │
         │                              ┌──────▼──────┐
         │                              │   Event     │
         │                              │  Manager    │
         │                              │             │
         │                              │ Priority    │
         │                              │ Queue       │
         │                              └──────┬──────┘
         │                                     │
         │                    ┌────────────────┼────────────────┐
         │                    ▼                ▼                ▼
         │             ┌──────────┐     ┌──────────┐     ┌──────────┐
         │             │Predictor │     │Predictor │     │ Strategy │
         │             │    A     │     │    B     │     │    X     │
         │             │          │     │          │     │          │
         │             │on_tick() │     │on_tick() │     │on_tick() │
         │             └──────────┘     └──────────┘     └──────────┘
```

### Order Flow

```
┌──────────────────────────────────────────────────────────────────────────┐
│                            ORDER FLOW                                     │
└──────────────────────────────────────────────────────────────────────────┘

    Strategy                   Platform                    Exchange
    ────────                   ────────                    ────────

    on_signal()
        │
        ▼
    submit_order()
        │
        │ OrderRequest
        ▼
    ┌──────────────┐
    │ OrderManager │
    │              │
    │ Validate     │
    │ Track        │
    └──────┬───────┘
           │
           ▼
    ┌──────────────┐                              ┌─────────────┐
    │  RestClient  │ ──── POST /orders ──────────►│   Architect │
    │              │                              │   Exchange  │
    │              │◄───── 200 OK ────────────────│             │
    └──────┬───────┘                              └─────────────┘
           │
           ▼
    ┌──────────────┐
    │ EventManager │
    │              │
    │ORDER_SUBMITTED│
    └──────┬───────┘
           │
           ▼
    ┌──────────────┐
    │  Strategy    │
    │              │
    │ on_accept()  │
    │ on_fill()    │
    │ on_reject()  │
    └──────────────┘
```

### Timer Event Flow

```
┌──────────────────────────────────────────────────────────────────────────┐
│                         TIMER EVENT FLOW                                  │
└──────────────────────────────────────────────────────────────────────────┘

    TimerSystem              EventManager           Predictors/Strategies
    ───────────              ────────────           ─────────────────────

    ┌───────────┐
    │ Registered│
    │ TimeRacks │
    │           │
    │ 93000000  │
    │ 93015000  │
    │ 160000000 │
    └─────┬─────┘
          │
          │ (Time reaches 93000000)
          │
          ▼
    ┌───────────┐
    │ fireTimer │
    │ (93000000)│
    └─────┬─────┘
          │
          │ TimerEventData
          │ {date: 20260130, time_rack: 93000000}
          │
          ▼
    ┌───────────────┐
    │ TIMER_EVENT   │──────────────────────────────►┌───────────────┐
    │               │                               │  Predictor A  │
    │               │                               │  on_timer()   │
    │               │                               └───────────────┘
    │               │──────────────────────────────►┌───────────────┐
    │               │                               │  Strategy X   │
    │               │                               │  on_timer()   │
    └───────────────┘                               └───────────────┘
```

---

## Latency Optimizations

### Lock-Free Patterns Used

1. **Atomic Operations**
   - `std::atomic<bool>` for running/enabled flags
   - `std::atomic<TimeRack>` for current time tracking
   - Lock-free statistics counters

2. **Read-Write Locks**
   - `std::shared_mutex` for read-heavy data (subscriptions, callbacks)
   - Readers don't block each other

3. **Lock Avoidance**
   - Event publishing is lock-free (lock-free queue)
   - Tick updates minimize lock scope
   - Callbacks stored by value to avoid pointer indirection

### Memory Layout Optimizations

```cpp
// Cache-line aligned structures (64 bytes)
struct alignas(64) Tick {
    Symbol      symbol;        // 32 bytes
    Price       bid_price;     // 8 bytes
    Price       ask_price;     // 8 bytes
    // ... (fits in 64-byte cache line)
};

// Contiguous storage for hot data
std::vector<Tick> tick_buffer_;  // Better cache locality than map
```

### Critical Path Optimizations

| Path | Optimization |
|------|--------------|
| Tick → Event | No heap allocation, stack-based event |
| Event → Predictor | Direct callback, no virtual dispatch |
| Signal → Strategy | Pre-allocated signal pool |
| Order Submit | Batched REST requests when possible |

---

## Directory Structure

```
platform_core/
├── CMakeLists.txt              # Root build config
├── README.md                   # Usage documentation
├── DESIGN.md                   # This document
├── build.sh                    # Build script
├── bin/                        # Build output (gitignored)
│   ├── make/                   # CMake build files
│   └── ax_bin/                 # Final binaries
│       ├── trading_client      # Main executable
│       └── lib/                # Libraries
├── config/
│   └── default_config.json     # Default configuration
├── include/
│   ├── api/
│   │   ├── RestClient.h
│   │   └── WebSocketClient.h
│   ├── config/
│   │   └── Config.h
│   ├── core/
│   │   ├── Constants.h
│   │   ├── Platform.h
│   │   └── Types.h
│   ├── events/
│   │   ├── Event.h
│   │   └── EventManager.h
│   ├── marketdata/
│   │   └── MarketDataManager.h
│   ├── orders/
│   │   ├── Order.h
│   │   ├── OrderBook.h
│   │   └── OrderManager.h
│   ├── portfolio/
│   │   └── PortfolioManager.h
│   ├── strategy/
│   │   ├── Predictor.h
│   │   ├── Strategy.h
│   │   └── TimerSystem.h
│   ├── user/
│   │   ├── User.h
│   │   └── UserManager.h
│   └── utils/
│       └── Logger.h
├── src/
│   ├── api/
│   ├── config/
│   ├── core/
│   ├── events/
│   ├── marketdata/
│   ├── orders/
│   ├── portfolio/
│   ├── strategy/
│   ├── user/
│   └── utils/
├── examples/
│   └── main.cpp
└── logs/                       # Runtime logs (gitignored)
    └── {YYYYMMDD}/
        ├── {date}.platform.log
        ├── {date}.orders.csv
        ├── {date}.fills.csv
        └── ...
```

---

## Usage

### Build

**Quick Build (recommended):**
```bash
./build.sh release    # Release build with optimizations
./build.sh debug      # Debug build with symbols
./build.sh clean      # Clean all build artifacts
./build.sh rebuild    # Clean + release build
```

**Manual Build:**
```bash
# From project root:
cd bin/make
cmake ../.. -DCMAKE_BUILD_TYPE=Release
make -j16
```

**Output Structure:**
```
bin/
├── make/           # CMake build files (intermediate)
│   ├── CMakeCache.txt
│   ├── Makefile
│   └── ...
└── ax_bin/         # Final binaries
    ├── trading_client    # Main executable
    └── lib/              # Static/shared libraries
        ├── libcore_lib.a
        ├── libevents_lib.a
        └── ...
```

### Run

```bash
./trading_client <config_path> <simulation_date>

# Example:
./trading_client config/default_config.json 20260130
```

### Creating a Custom Predictor

```cpp
class MomentumPredictor : public BasePredictor {
public:
    MomentumPredictor() : BasePredictor("momentum") {
        add_timer(93000000);  // 9:30 AM
        subscribe_symbol("BTC-USD");
    }
    
    void on_tick(const TickEventData& tick, DateInt date, TimeRack tr) override {
        update_momentum(tick.last);
    }
    
    void on_timer(DateInt date, TimeRack time_rack) override {
        double signal = calculate_momentum();
        emit_signal("momentum", signal, 0.9);
    }
};
```

### Creating a Custom Strategy

```cpp
class TrendFollower : public BaseStrategy {
public:
    TrendFollower() : BaseStrategy("trend_follower") {
        subscribe_predictor("momentum");
        add_timer(155500000);  // 3:55 PM (flatten)
    }
    
    void on_signal(const PredictorSignalData& signal) override {
        if (signal.signal_value > 0.5) {
            submit_order(OrderRequest::market_buy("BTC-USD", 1.0, "trend_up"));
        }
    }
    
    void on_timer(DateInt date, TimeRack tr) override {
        if (tr >= 155500000) flatten_all("EOD");
    }
};
```

---

## Version History

| Version | Date | Changes |
|---------|------|---------|
| 1.0.0 | 2026-02-02 | Initial release |

---

*Document generated for Architect Platform Core v1.0.0*
