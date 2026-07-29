# Architect Platform Core

A high-performance, ultra-configurable C++20 trading platform core for the [Architect Exchange API](https://docs.architect.exchange/api-reference/user-management/get-whoami).

## Features

- **Ultra-Fast Performance**: Optimized for low-latency trading with lock-free data structures, cache-aligned memory layouts, and compile-time optimizations
- **God Object Pattern**: Single entry point (`Platform`) for accessing all components
- **Sophisticated Logging**: CSV logging with dated directories, structured data output
- **Modular Architecture**: Clean separation of concerns with independent modules
- **Highly Configurable**: JSON-based configuration with runtime updates
- **Complete API Coverage**: Full support for REST and WebSocket APIs

## Quick Start

### Running a Simulation

```bash
./trading_client <config_path> <simulation_date>
```

**Arguments:**
| Argument | Description | Example |
|----------|-------------|---------|
| `config_path` | Path to JSON config file | `config/default_config.json` |
| `simulation_date` | Date in YYYY-MM-DD format | `2024-01-15` |

**Example:**
```bash
./trading_client config/default_config.json 20260204
```
Use date as **YYYYMMDD** (8 digits, no dashes). If you see `zsh: number expected`, pass the date explicitly (e.g. `20260204`) instead of `$(date +%Y%m%d)`.

### API keys and real API usage

All orders and feed verification use the **real Architect Exchange API** (REST and WebSocket). There is no local-only or simulated order path; every order is sent to the exchange via REST.

`config/default_config.json` does not contain API keys (do not commit real keys). Either:

- **Environment (recommended):** set `ARCHITECT_API_KEY`, `ARCHITECT_API_SECRET`, and optionally `ARCHITECT_SESSION_TOKEN`, then run `./run_simulation.sh [config] [date]` so keys are injected at runtime.
- **Local config:** copy to e.g. `config/my_config.json`, set `api.api_key`, `api.api_secret`, `api.session_token`, and use that file when running.

**Authentication:** The client exchanges your `api_key` and `api_secret` for a bearer token automatically via **POST /authenticate** ([docs](https://docs.architect.exchange/api-reference/user-management/authenticate.md)). No need to set `session_token` manually unless you already have a token. Default REST base ships pointing at **AX PRODUCTION** (`https://gateway.architect.exchange/api`, real funds), with WS at `wss://gateway.architect.exchange/orders/ws`. To trade paper money instead, point `api.rest_endpoint` at `https://gateway.sandbox.architect.exchange/api` and `api.ws_endpoint` at `wss://gateway.sandbox.architect.exchange/orders/ws` (and use sandbox-issued keys).

**Order placement** follows the [place_order API](https://docs.architect.exchange/api-reference/order-management/place-order): symbol as `PAIR-PERP` (e.g. `GBPUSD-PERP`), side `B`/`S`, quantity in contracts (use `market_maker.order_size_step` when the exchange requires a multiple, e.g. 100 for GBPUSD-PERP).

**Market maker: why only two orders at the open?** The strategy keeps exactly **one bid** and **one ask** live. At the first timer it sends those two; on later timers it does **not** send more unless (1) one side **filled** (then it sends a new order for that side) or (2) **theo moved** by more than one tick (then it cancels both and resubmits on the next tick). So “next” orders are **replacements** after a fill or a theo move, not extra orders every interval.

**Where to see if orders got filled**
- **Architect UI (production):** Use the live exchange web app → Orders / Trades / Fills to see accepted orders and fills.
- **Platform logs:** In the run directory under `logs/<date>/`: `*.execution.csv` and `*.fills.csv`; the main log prints `ORDER FILLED: ...` and `log_execution` for each fill. Portfolio (realized P&L) also updates on fill.

### Output Structure

The platform creates a dated directory structure for each run:

```
logs/
└── 2024-01-15/                    # Simulation date
    └── 143052_123/                # Run ID (HHMMSS_ms)
        ├── platform.log           # Main log file
        ├── config/
        │   ├── config_used.json   # Configuration snapshot
        │   ├── config_original.json
        │   └── simulation_params.txt
        └── csv/
            ├── orders.csv         # All order events
            ├── fills.csv          # Trade executions
            ├── ticks.csv          # Market ticks
            ├── positions.csv      # Position updates
            ├── balances.csv       # Balance changes
            ├── events.csv         # System events
            └── metrics.csv        # Performance metrics
```

## Architecture

```
platform_core/
├── include/
│   ├── core/
│   │   ├── Types.h              # Core types (OrderId, Price, etc.)
│   │   ├── Constants.h          # API endpoints, defaults
│   │   └── Platform.h           # God Object - main entry point
│   ├── config/Config.h          # Configuration management
│   ├── utils/Logger.h           # Sophisticated logging + CSV
│   ├── events/                   # Event system
│   ├── orders/                   # Order management
│   ├── user/                     # Authentication
│   ├── api/                      # REST + WebSocket clients
│   ├── marketdata/              # Market data handling
│   └── portfolio/               # Position/P&L tracking
├── src/                          # Implementations
├── config/default_config.json    # Default configuration
└── examples/main.cpp            # Simulation runner
```

## Building

### Prerequisites

- CMake 3.20+
- C++20 compiler (GCC 10+, Clang 12+)
- libcurl, OpenSSL

### Build Commands

```bash
mkdir build && cd build
cmake -DCMAKE_BUILD_TYPE=Release -DBUILD_EXAMPLES=ON ..
cmake --build . -j$(nproc)
```

## God Object Pattern

The `Platform` class serves as a single entry point to all components:

```cpp
#include "core/Platform.h"

using namespace architect;

int main(int argc, char* argv[]) {
    // Initialize with simulation parameters
    core::SimulationParams params;
    params.config_path = argv[1];
    params.simulation_date = argv[2];
    
    // Get the god object
    auto& main = core::Main();
    
    // Initialize and start
    main.initialize(params);
    main.start();
    
    // Access any component through main->
    main.logger()->info("Starting simulation");
    main.logger()->log_to_csv(utils::CSVLogType::EVENTS, row);
    
    main.config()->getString("api.endpoint");
    
    main.events()->subscribe(events::EventType::ORDER_FILLED, callback);
    
    main.orders()->submitOrder(request, user_id);
    
    main.marketdata()->subscribe("BTC-USD");
    
    main.portfolio()->getPosition("BTC-USD");
    
    // Graceful shutdown
    main.stop();
    
    return 0;
}
```

## Logger Usage

### Console/File Logging

```cpp
auto& main = core::Main();

main.logger()->trace("Trace message");
main.logger()->debug("Debug: value={}", 42);
main.logger()->info("Info message");
main.logger()->warn("Warning!");
main.logger()->error("Error: {}", error_msg);
main.logger()->critical("Critical failure");
```

### CSV Logging

```cpp
// Log order
main.logger()->log_order(order_id, client_id, symbol, side, type, 
                         status, price, quantity, filled_qty, message);

// Log fill
main.logger()->log_fill(trade_id, order_id, symbol, side, 
                        price, quantity, fee, fee_currency);

// Log tick
main.logger()->log_tick(symbol, bid, ask, last, bid_size, ask_size);

// Log position
main.logger()->log_position(symbol, side, qty, entry, mark, 
                            unrealized_pnl, realized_pnl);

// Log metrics
main.logger()->log_metrics("latency_ms", 0.5, "ms");

// Custom CSV
utils::CSVRow row;
row.add("field1").add(123).add(45.67);
main.logger()->log_to_csv("custom_log", row);
```

### Direct File Logging

```cpp
main.logger()->log_to_file("debug.txt", "Custom content\n", true);  // append
```

## Configuration

### Full Configuration Example

```json
{
    "api": {
        "rest_endpoint": "https://gateway.architect.exchange/api",
        "ws_endpoint": "wss://gateway.architect.exchange/orders/ws",
        "api_key": "your_key",
        "api_secret": "your_secret",
        "timeout_read_ms": 30000
    },
    "trading": {
        "watchlist": ["BTC-USD", "ETH-USD"],
        "max_orders_per_second": 50
    },
    "logging": {
        "directory": "logs",
        "level": "INFO",
        "console_enabled": true,
        "file_enabled": true,
        "csv_enabled": true
    },
    "performance": {
        "event_queue_size": 1000000,
        "worker_threads": 4
    }
}
```

### External feed (theo pricing)

An optional second feed is used for pricing orders (theo). Default: **Neon FIX** (`external_feed.provider`: `neon_fix`). For REST-only theo, set `external_feed.rest_url` and `rest_uses_spot_ticker_path` (spot vs futures bookTicker path). Replace per client (e.g. CME FX/metals, FX ECN, LSEG).

Config (`external_feed`):

| Key | Description | Default |
|-----|-------------|---------|
| `enabled` | Use external feed | `true` |
| `rest_url` | HTTPS base for REST bookTicker (when not using `neon_fix`) | `""` (FIX default) |
| `symbol` | Exchange symbol (FIX 55 or venue id) | `""` until set in config |
| `display_symbol` | Symbol key for theo cache / desk | `""` until set in config |
| `poll_interval_ms` | REST poll interval | `2000` |

Usage:

```cpp
auto& main = core::Main();
main.externalFeed()->start();  // started automatically with platform
auto theo = main.externalFeed()->getTheoPrice(main.config()->getMarketMakerTheoSymbol());
if (theo) { /* use *theo for pricing */ }
auto quote = main.externalFeed()->getLastQuote();  // bid, ask, mid
```

### Schedule and instrument status

Trading schedule is **22/7** to start. Per-symbol open/close is available from the **instruments** query and from the **status** field on stats objects; use these as the source of truth for schedule rather than a config-file schedule.

### Accessing Configuration

```cpp
auto& main = core::Main();

std::string endpoint = main.config()->getString("api.rest_endpoint");
int timeout = main.config()->getInt("api.timeout_read_ms", 30000);
auto watchlist = main.config()->getWatchlist();
```

## Event System

```cpp
auto& main = core::Main();

// Subscribe to events
main.events()->subscribe(events::EventType::ORDER_FILLED,
    [&main](const events::EventPtr& event) {
        auto data = event->getData<events::OrderEventData>();
        main.logger()->info("Filled: {} @ {}", data.quantity, data.avg_fill_price);
    });

// Multiple event types
main.events()->subscribeMultiple(
    {events::EventType::CONNECTION_ESTABLISHED, events::EventType::CONNECTION_LOST},
    [](const events::EventPtr& event) {
        // Handle connection events
    });

// Publish events
main.events()->emit(events::EventType::CUSTOM_EVENT, custom_data);
```

## Order Management

```cpp
auto& main = core::Main();

// Create order request
orders::OrderRequest request;
request.symbol = core::makeSymbol("BTC-USD");
request.side = core::Side::BUY;
request.type = core::OrderType::LIMIT;
request.price = 50000.0;
request.quantity = 0.001;

// Submit order
auto user = main.user()->getCurrentUser();
auto order_id = main.orders()->submitOrder(request, user->id);

// Query orders
auto order = main.orders()->getOrder(order_id);
auto active_orders = main.orders()->getActiveOrders();

// Cancel order
orders::OrderCancelRequest cancel;
cancel.order_id = order_id;
main.orders()->cancelOrder(cancel);
```

## Market Data

```cpp
auto& main = core::Main();

// Subscribe
main.marketdata()->subscribe("BTC-USD");

// Get data
auto tick = main.marketdata()->getTick("BTC-USD");
auto book = main.marketdata()->getOrderBook("BTC-USD");
auto bbo = main.marketdata()->getBBO("BTC-USD");
auto mid = main.marketdata()->getMidPrice("BTC-USD");

// Order book analysis
if (book) {
    auto vwap = book->calculateVWAP(core::Side::BUY, 1.0);
    auto slippage = book->calculateSlippage(core::Side::BUY, 1.0);
}
```

## Portfolio

```cpp
auto& main = core::Main();

// Get summary
auto summary = main.portfolio()->getSummary();
std::cout << "Equity: " << summary.total_equity << std::endl;
std::cout << "P&L: " << summary.unrealized_pnl << std::endl;

// Get position
auto position = main.portfolio()->getPosition("BTC-USD");
if (position) {
    std::cout << "Position: " << position->quantity << std::endl;
}

// Risk management
auto size = main.portfolio()->calculatePositionSize(1.0, 50000, 49000);
bool ok = main.portfolio()->checkRiskLimits(order_request);
```

## CSV Output Format

### orders.csv
```csv
timestamp,order_id,client_order_id,symbol,side,type,status,price,quantity,filled_qty,message
2024-01-15 14:30:52.123,1,ORD-123,BTC-USD,BUY,LIMIT,ACCEPTED,50000.0,0.001,0.0,
```

### fills.csv
```csv
timestamp,trade_id,order_id,symbol,side,price,quantity,fee,fee_currency
2024-01-15 14:30:53.456,100,1,BTC-USD,BUY,50000.0,0.001,0.05,USD
```

### ticks.csv
```csv
timestamp,symbol,bid,ask,last,bid_size,ask_size
2024-01-15 14:30:52.100,BTC-USD,49999.5,50000.5,50000.0,1.5,2.0
```

### metrics.csv
```csv
timestamp,metric_name,value,unit
2024-01-15 14:30:52.000,latency_ms,0.5,ms
2024-01-15 14:31:52.000,equity,100000.0,USD
```

## Performance Optimization

1. **Compile Flags**: `-O3 -march=native -flto`
2. **Cache Alignment**: Core structures are 64-byte aligned
3. **Buffer Sizing**: Configure `event_queue_size` based on throughput
4. **Thread Count**: Set `worker_threads` based on CPU cores

## License

MIT License - See LICENSE file for details.

## References

- [Architect Exchange API](https://docs.architect.exchange/api-reference/user-management/get-whoami)
- [Architect SDK Guide](https://docs.architect.co/architect-api-and-sdk-guide)
# architech
