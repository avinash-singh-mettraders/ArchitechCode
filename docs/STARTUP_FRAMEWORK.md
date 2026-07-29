# 25-Step Platform Startup & Trading Framework

## Overview

This document describes the **25-step framework** for the Architect Platform Core trading system. The framework ensures:

- **Steps 1-23**: Systematic health checks, initialization, and API verification. **Crash immediately if any step fails.**
- **Step 24**: Market data polling begins, strategy starts trading
- **Step 25**: Graceful shutdown verification when feed stops or signal received

---

## Step-by-Step Framework

### Phase 1: Core System Initialization (Steps 1-6)

| Step | Name | Description | Crash Condition |
|------|------|-------------|-----------------|
| 1 | **Validate Parameters** | Verify config path exists, date format is YYYYMMDD | Invalid params |
| 2 | **Load Configuration** | Parse JSON config file into memory | Parse error, missing required fields |
| 3 | **Initialize Logger** | Create log directory, open log files, CSV writers | Directory creation fails, file write fails |
| 4 | **Initialize Event Manager** | Create event queue, spawn worker threads | Thread spawn fails, queue allocation fails |
| 5 | **Initialize Order Manager** | Setup order tracking structures | Allocation failure |
| 6 | **Initialize User Manager** | Setup session management | Allocation failure |

### Phase 2: API & Connectivity (Steps 7-12)

| Step | Name | Description | Crash Condition |
|------|------|-------------|-----------------|
| 7 | **Initialize REST Client** | Configure base URL, timeouts | Invalid URL format |
| 8 | **Initialize WebSocket Client** | Configure WS endpoint, heartbeat params | Invalid URL format |
| 9 | **Authenticate with Exchange** | Exchange API key for JWT or validate session token | Auth fails (401/403) |
| 10 | **Verify API Connectivity (whoami)** | Call `/whoami` endpoint, verify response | HTTP error, timeout, invalid response |
| 11 | **Detect Feed Mode** | Determine LIVE/PAPER/SIMULATION from API response | Unable to determine mode |
| 12 | **Check Exchange Status** | Verify exchange is operational (not in maintenance) | Exchange in maintenance |

### Phase 3: Market Data Infrastructure (Steps 13-17)

| Step | Name | Description | Crash Condition |
|------|------|-------------|-----------------|
| 13 | **Initialize Market Data Manager** | Setup tick storage, orderbook managers | Allocation failure |
| 14 | **Initialize External Feed Manager** | Setup external theo feed (Neon FIX or REST) | Config error (missing symbol, invalid URL) |
| 15 | **Connect WebSocket** | Establish WS connection to exchange | Connection refused, timeout |
| 16 | **Authenticate WebSocket** | Send auth message on WS channel | Auth failure on WS |
| 17 | **Subscribe to Market Data Channels** | Subscribe to orderbook, trades, ticker for watchlist | Subscription rejected |

### Phase 4: Trading Infrastructure (Steps 18-21)

| Step | Name | Description | Crash Condition |
|------|------|-------------|-----------------|
| 18 | **Initialize Portfolio Manager** | Load positions, balances from API | API failure, parse error |
| 19 | **Verify Account Permissions** | Check trading enabled, not close-only (if live) | Account frozen, close-only |
| 20 | **Subscribe to Private Channels** | Subscribe to orders, fills, positions WS channels | Subscription failure |
| 21 | **Initialize Strategy Manager** | Register strategies, setup timers | Strategy registration fails |

### Phase 5: Pre-Trade Verification (Steps 22-23)

| Step | Name | Description | Crash Condition |
|------|------|-------------|-----------------|
| 22 | **Verify External Feed** | Confirm external feed returning valid quotes | No data after timeout |
| 23 | **Verify Market Data** | Confirm receiving ticks/orderbook updates | No market data after timeout |

### Phase 6: Trading (Step 24)

| Step | Name | Description | Exit Condition |
|------|------|-------------|----------------|
| 24 | **Start Trading Loop** | Begin market data polling, fire strategy timers, process fills | Signal (SIGINT/SIGTERM) or feed disconnect |

**During Step 24:**
- External feed polls at configured interval (default 2s)
- Strategy timers fire, calculate theo midpoint
- Quote Orders (QO) calculated and validated against market
- Orders submitted to exchange
- Fills processed, hedge logic triggered (placeholder)
- Positions updated

### Phase 7: Shutdown (Step 25)

| Step | Name | Description | Status |
|------|------|-------------|--------|
| 25 | **Graceful Shutdown** | Stop strategies, cancel open orders, disconnect, flush logs | SUCCESS / FAILURE |

**Shutdown Checks:**
- All open orders cancelled or confirmed filled
- All event queues drained
- All log files flushed and closed
- WebSocket disconnected cleanly
- Final statistics logged

---

## Strategy Logic (Base Case)

### Quote Order (QO) Calculation

```
┌────────────────────────────────────────────────────────────────────┐
│                     THEO PRICING FLOW                               │
├────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  External Feed (theo)            Strategy Logic                   │
│  ─────────────────────            ──────────────                   │
│                                                                     │
│  ┌─────────────────┐                                               │
│  │ Reference TOB   │                                               │
│  │ (FIX or REST)   │                                               │
│  │                 │                                               │
│  │ Bid: 1.0850     │                                               │
│  │ Ask: 1.0852     │                                               │
│  └────────┬────────┘                                               │
│           │                                                         │
│           │ Poll (2s)                                               │
│           ▼                                                         │
│  ┌─────────────────┐                                               │
│  │ Theo Midpoint   │ = (Bid + Ask) / 2 = 1.08510                   │
│  └────────┬────────┘                                               │
│           │                                                         │
│           │ + Basis (config)                                        │
│           ▼                                                         │
│  ┌─────────────────┐                                               │
│  │ Futures Mid     │ = Theo + Basis = 1.08510 + 0.0000             │
│  └────────┬────────┘                                               │
│           │                                                         │
│           │ Apply Spread (config: bid_w_bps, ask_w_bps; per side)   │
│           ▼                                                         │
│  ┌─────────────────┐                                               │
│  │ QO Bid Price    │ = Mid - (Mid × bid_w_bps / 10000)             │
│  │ QO Ask Price    │ = Mid + (Mid × ask_w_bps / 10000)             │
│  └────────┬────────┘                                               │
│           │                                                         │
│           │ Round to Tick (config: price_tick)                      │
│           ▼                                                         │
│  ┌─────────────────┐                                               │
│  │ Final QO Bid    │ = round(QO_Bid / tick) × tick                 │
│  │ Final QO Ask    │ = round(QO_Ask / tick) × tick                 │
│  └─────────────────┘                                               │
│                                                                     │
└────────────────────────────────────────────────────────────────────┘
```

### QO Validation Rules

Before submitting Quote Orders, validate:

1. **QO Bid ≤ Market Front Offer**: Our bid price must not cross the market's best ask
2. **QO Ask ≥ Market Front Bid**: Our ask price must not cross the market's best bid

```cpp
bool validateQuoteOrders(Price qo_bid, Price qo_ask, Price market_bid, Price market_ask) {
    // Rule 1: QO Bid must not cross market offer (avoid immediate buy)
    if (qo_bid > market_ask) {
        log_warn("QO Bid {} > Market Ask {} - would cross spread!", qo_bid, market_ask);
        return false;
    }
    
    // Rule 2: QO Ask must not cross market bid (avoid immediate sell)
    if (qo_ask < market_bid) {
        log_warn("QO Ask {} < Market Bid {} - would cross spread!", qo_ask, market_bid);
        return false;
    }
    
    // Rule 3: Our spread must be valid (bid < ask)
    if (qo_bid >= qo_ask) {
        log_warn("Invalid QO spread: Bid {} >= Ask {}", qo_bid, qo_ask);
        return false;
    }
    
    return true;
}
```

### Order Lifecycle

```
┌──────────────────────────────────────────────────────────────────────┐
│                       ORDER LIFECYCLE                                 │
├──────────────────────────────────────────────────────────────────────┤
│                                                                       │
│  Timer Fires (every N seconds)                                       │
│       │                                                               │
│       ▼                                                               │
│  ┌─────────────────────┐                                             │
│  │ Get Theo from       │                                             │
│  │ External Feed       │                                             │
│  └──────────┬──────────┘                                             │
│             │                                                         │
│             ▼                                                         │
│  ┌─────────────────────┐        ┌─────────────────────┐             │
│  │ Calculate QO Prices │───────►│ Validate vs Market  │             │
│  └─────────────────────┘        └──────────┬──────────┘             │
│                                             │                         │
│                      ┌──────────────────────┴───────────────────┐    │
│                      │                                          │    │
│                      ▼                                          ▼    │
│         ┌─────────────────────┐                    ┌────────────────┐│
│         │ Valid: Submit Orders │                    │ Invalid: Skip  ││
│         └──────────┬──────────┘                    └────────────────┘│
│                    │                                                  │
│         ┌──────────┴──────────┐                                      │
│         ▼                     ▼                                      │
│  ┌─────────────┐      ┌─────────────┐                               │
│  │ BID ORDER   │      │ ASK ORDER   │                               │
│  │ (SUBMITTED) │      │ (SUBMITTED) │                               │
│  └──────┬──────┘      └──────┬──────┘                               │
│         │                    │                                        │
│         ▼                    ▼                                        │
│  ┌─────────────┐      ┌─────────────┐                               │
│  │ ACCEPTED    │      │ ACCEPTED    │  ◄── Track order IDs          │
│  └──────┬──────┘      └──────┬──────┘                               │
│         │                    │                                        │
│         │   ┌────────────────┘                                       │
│         │   │                                                         │
│         ▼   ▼                                                         │
│  ┌───────────────────────────────────────┐                          │
│  │         MONITORING STATE              │                          │
│  │  - Watch for fills (partial/full)     │                          │
│  │  - Watch for theo price moves         │                          │
│  │  - Watch for market crosses           │                          │
│  └──────────────┬────────────────────────┘                          │
│                 │                                                     │
│      ┌──────────┴──────────┬────────────────────┐                   │
│      ▼                     ▼                    ▼                    │
│ ┌──────────┐        ┌────────────┐      ┌─────────────┐            │
│ │  FILL    │        │ THEO MOVED │      │ PRICE CROSS │            │
│ └────┬─────┘        └─────┬──────┘      └──────┬──────┘            │
│      │                    │                    │                     │
│      ▼                    ▼                    ▼                     │
│ ┌──────────┐        ┌────────────┐      ┌─────────────┐            │
│ │ Hedge    │        │ Cancel &   │      │ Cancel &    │            │
│ │ Logic    │        │ Replace    │      │ Requote     │            │
│ │ (Empty)  │        │ Orders     │      │             │            │
│ └────┬─────┘        └────────────┘      └─────────────┘            │
│      │                                                               │
│      ▼                                                               │
│ ┌──────────┐                                                        │
│ │ Reload   │  ◄── Replenish the filled order to full size          │
│ │ QO Order │                                                        │
│ └──────────┘                                                        │
│                                                                       │
└──────────────────────────────────────────────────────────────────────┘
```

---

## Fill & Hedge Logic (Placeholder)

When a Quote Order is filled:

```
┌──────────────────────────────────────────────────────────────────────┐
│                     FILL PROCESSING FLOW                              │
├──────────────────────────────────────────────────────────────────────┤
│                                                                       │
│  ORDER_FILLED Event                                                  │
│       │                                                               │
│       ▼                                                               │
│  ┌─────────────────────────────────────────────┐                    │
│  │ 1. Calculate Notional Value of Fill         │                    │
│  │    notional = fill_price × fill_quantity    │                    │
│  └──────────────────────┬──────────────────────┘                    │
│                         │                                             │
│                         ▼                                             │
│  ┌─────────────────────────────────────────────┐                    │
│  │ 2. Update Net Position Tally               │                    │
│  │    If BUY:  net_position += fill_quantity  │                    │
│  │    If SELL: net_position -= fill_quantity  │                    │
│  └──────────────────────┬──────────────────────┘                    │
│                         │                                             │
│                         ▼                                             │
│  ┌─────────────────────────────────────────────┐                    │
│  │ 3. Add to Directional Exposure             │                    │
│  │    exposure += signed_notional             │                    │
│  │    (positive for long, negative for short) │                    │
│  └──────────────────────┬──────────────────────┘                    │
│                         │                                             │
│                         ▼                                             │
│  ┌─────────────────────────────────────────────┐                    │
│  │ 4. Hedge Decision (PLACEHOLDER)            │                    │
│  │    - Check if abs(exposure) > threshold    │                    │
│  │    - If yes, send hedge order (future)     │                    │
│  │    - For now: log hedge intent only        │                    │
│  └──────────────────────┬──────────────────────┘                    │
│                         │                                             │
│                         ▼                                             │
│  ┌─────────────────────────────────────────────┐                    │
│  │ 5. Reload Quote Order                       │                    │
│  │    - Submit new order at same level         │                    │
│  │    - Or recalculate if theo moved          │                    │
│  └─────────────────────────────────────────────┘                    │
│                                                                       │
└──────────────────────────────────────────────────────────────────────┘
```

### Example: QO Fill & Hedge Trade Decision

```
Initial State:
  net_position = 0
  exposure = 0

Event: BID ORDER FILLED (BUY 100 @ 1.0851)
  notional = 100 × 1.0851 = 108.51
  net_position = 0 + 100 = +100 (long)
  exposure = 0 + 108.51 = +108.51

  Hedge Decision:
    - We are long 100 units
    - Hedge would be: SELL on hedge exchange
    - (For this version: log only, no actual hedge order)
  
  Reload:
    - Submit new BID order for 100 @ current theo level

Event: ASK ORDER FILLED (SELL 50 @ 1.0855)
  notional = 50 × 1.0855 = 54.275
  net_position = +100 - 50 = +50 (still long)
  exposure = +108.51 - 54.275 = +54.235

  Hedge Decision:
    - We are long 50 units
    - Hedge would be: SELL 50 on hedge exchange
    - (For this version: log only)
  
  Reload:
    - Submit new ASK order for 100 @ current theo level
```

---

## Logging Configuration

### Configurable Logging Options

```json
{
    "logging": {
        "directory": "logs",
        "level": "INFO",
        "console_enabled": true,
        "file_enabled": true,
        "csv_enabled": true,
        
        "log_order_lifecycle": true,
        "log_feed_updates": true,
        "log_theo_updates": true,
        "log_connectivity": true,
        "log_strategy_decisions": true,
        "log_fills": true,
        "log_hedge_signals": true
    }
}
```

### What Gets Logged

| Log Type | Config Key | CSV File | Description |
|----------|------------|----------|-------------|
| Order Lifecycle | `log_order_lifecycle` | `orders.csv` | SUBMITTED, ACCEPTED, REJECTED, FILLED, CANCELLED |
| Live Feed Updates | `log_feed_updates` | `ticks.csv` | Every tick from exchange WS |
| Theo Updates | `log_theo_updates` | `theo.csv` | External feed theo midpoint changes |
| Connectivity | `log_connectivity` | `events.csv` | WS connect/disconnect, API errors |
| Strategy Decisions | `log_strategy_decisions` | `strategy.csv` | Quote calculations, validation results |
| Fills | `log_fills` | `execution.csv` | Fill details, notional, position update |
| Hedge Signals | `log_hedge_signals` | `hedge.csv` | Hedge intent (even when empty logic) |

---

## External Feed Configuration

### Neon FIX (typical default)

```json
{
    "external_feed": {
        "enabled": true,
        "name": "neon_fx_quote",
        "provider": "neon_fix",
        "rest_url": "",
        "rest_uses_spot_ticker_path": true,
        "symbol": "GBP/USD",
        "display_symbol": "GBPUSD",
        "poll_interval_ms": 2000,
        "fix": { "host": "127.0.0.1", "port": 14507 }
    }
}
```

### REST bookTicker (optional)

When `provider` is not a FIX variant, set `rest_url` to an HTTPS API base and `symbol` to the venue’s instrument code; use `rest_uses_spot_ticker_path` to select `/api/v3/...` vs `/fapi/v1/...` bookTicker paths (same shape as `ExternalFeedManager` in C++).

### Future Replacement Structure

The external feed is designed for easy replacement:

```cpp
// Interface for external feed providers
class IExternalFeedProvider {
public:
    virtual ~IExternalFeedProvider() = default;
    virtual void start() = 0;
    virtual void stop() = 0;
    virtual std::optional<ExternalFeedQuote> getQuote(const std::string& symbol) = 0;
    virtual std::string getProviderName() const = 0;
};

// Example implementations
class RestBookTickerFeedProvider : public IExternalFeedProvider { ... };

// Future: CME, LSEG, FX ECN
class CMEFeedProvider : public IExternalFeedProvider { ... };
class LSEGFeedProvider : public IExternalFeedProvider { ... };
```

---

## Configuration Reference

### Complete Market Maker Config

```json
{
    "market_maker": {
        "enabled": true,
        "symbol": "BTC-USD",
        "order_symbol": "",
        "order_size_step": 1,
        "quantity": 0.001,
        "spread_bps": 10,
        "basis": 0.0,
        "theo_symbol": "GBPUSD",
        "update_interval_sec": 2,
        "price_tick": 0.01,
        "paper_simulate_fills": true,
        
        "validate_vs_market": true,
        "max_theo_age_ms": 5000,
        "cancel_on_disconnect": true
    }
}
```

### Startup Sequence Config

```json
{
    "startup": {
        "crash_on_auth_failure": true,
        "crash_on_feed_failure": true,
        "feed_verify_timeout_ms": 10000,
        "market_data_verify_timeout_ms": 10000,
        "require_external_feed": true
    }
}
```

---

## Error Codes

| Code | Name | Description | Step |
|------|------|-------------|------|
| E001 | INVALID_CONFIG | Config file missing or invalid JSON | 2 |
| E002 | LOG_DIR_FAIL | Cannot create log directory | 3 |
| E003 | AUTH_FAIL | Authentication failed (401/403) | 9 |
| E004 | API_UNREACHABLE | Cannot reach exchange API | 10 |
| E005 | WS_CONNECT_FAIL | WebSocket connection failed | 15 |
| E006 | FEED_TIMEOUT | No external feed data received | 22 |
| E007 | MARKET_DATA_TIMEOUT | No market data received | 23 |
| E008 | ACCOUNT_FROZEN | Trading account is frozen | 19 |
| E009 | CLOSE_ONLY | Account in close-only mode (live) | 19 |

---

## Implementation Status

| Component | Status | Notes |
|-----------|--------|-------|
| 25-Step Orchestration | 🔄 In Progress | StartupSequence class |
| QO Validation | 🔄 In Progress | Market cross checks |
| Fill Handling | ✅ Exists | Basic fill processing |
| Hedge Logic (Placeholder) | 🔄 In Progress | Structure only |
| Net Position Tracking | 🔄 In Progress | PortfolioManager |
| Logging Toggles | 🔄 In Progress | Config expansion |
| External Feed | ✅ Exists | Neon FIX + REST bookTicker |
| Graceful Shutdown | ✅ Exists | Signal handling |

---

*Document Version: 1.0 | Last Updated: 2026-02-06*
