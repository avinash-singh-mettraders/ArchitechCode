#pragma once

/**
 * @file MarketDataManager.h
 * @brief Market data management and distribution
 */

#include "core/Types.h"
#include "orders/OrderBook.h"
#include "api/WebSocketClient.h"
#include <string>
#include <vector>
#include <map>
#include <set>
#include <mutex>
#include <shared_mutex>
#include <functional>
#include <memory>

namespace architect {
namespace marketdata {

using namespace core;
using namespace orders;

/**
 * @brief Market information
 */
struct Market {
    Symbol          symbol;
    std::string     base_currency;
    std::string     quote_currency;
    MarketStatus    status          = MarketStatus::CLOSED;
    Price           min_price       = 0.0;
    Price           max_price       = 0.0;
    Quantity        min_quantity    = 0.0;
    Quantity        max_quantity    = 0.0;
    Quantity        min_notional    = 0.0;
    int             price_precision = 8;
    int             quantity_precision = 8;
    Decimal         maker_fee       = 0.0;
    Decimal         taker_fee       = 0.0;
    bool            is_tradeable    = true;
};

/**
 * @brief OHLCV candle data
 */
struct Candle {
    Timestamp       timestamp;
    Price           open;
    Price           high;
    Price           low;
    Price           close;
    Volume          volume;
    Volume          quote_volume;
    std::uint64_t   trade_count = 0;
};

/**
 * @brief Callback types
 */
using TickCallback = std::function<void(const Symbol&, const Tick&)>;
using BookCallback = std::function<void(const Symbol&, const BookSnapshot&)>;
using TradeCallback = std::function<void(const Symbol&, const Trade&)>;
using MarketCallback = std::function<void(const Symbol&, MarketStatus)>;

/**
 * @brief Market data manager
 * 
 * Aggregates and distributes market data from various sources:
 * - Real-time ticks
 * - Order book updates
 * - Trade executions
 * - Market status changes
 */
class MarketDataManager {
public:
    /**
     * @brief Get the singleton instance
     */
    static MarketDataManager& getInstance();
    
    // Prevent copying
    MarketDataManager(const MarketDataManager&) = delete;
    MarketDataManager& operator=(const MarketDataManager&) = delete;
    
    // ==========================================================================
    // Lifecycle
    // ==========================================================================
    
    /**
     * @brief Start market data manager
     */
    void start();
    
    /**
     * @brief Stop market data manager
     */
    void stop();
    
    /**
     * @brief Check if running
     */
    [[nodiscard]] bool isRunning() const { return running_; }
    
    // ==========================================================================
    // Subscriptions
    // ==========================================================================
    
    /**
     * @brief Subscribe to market data for a symbol
     */
    bool subscribe(const std::string& symbol);
    
    /**
     * @brief Subscribe to multiple symbols
     */
    bool subscribeMultiple(const std::vector<std::string>& symbols);
    
    /**
     * @brief Unsubscribe from a symbol
     */
    bool unsubscribe(const std::string& symbol);
    
    /**
     * @brief Unsubscribe from all symbols
     */
    void unsubscribeAll();
    
    /**
     * @brief Get subscribed symbols
     */
    std::vector<std::string> getSubscribedSymbols() const;
    
    // ==========================================================================
    // Market Data Access
    // ==========================================================================
    
    /**
     * @brief Get latest tick for a symbol
     */
    std::optional<Tick> getTick(const std::string& symbol) const;
    
    /**
     * @brief Get order book for a symbol
     */
    std::shared_ptr<OrderBook> getOrderBook(const std::string& symbol);
    
    /**
     * @brief Get L1 (BBO) for a symbol
     */
    std::optional<std::pair<BookLevel, BookLevel>> getBBO(const std::string& symbol) const;
    
    /**
     * @brief Get L2 book snapshot
     */
    std::optional<BookSnapshot> getL2Snapshot(const std::string& symbol, std::size_t depth = 25) const;
    
    /**
     * @brief Get mid price
     */
    std::optional<Price> getMidPrice(const std::string& symbol) const;
    
    /**
     * @brief Get spread
     */
    std::optional<Price> getSpread(const std::string& symbol) const;
    
    /**
     * @brief Get recent trades
     */
    std::vector<Trade> getRecentTrades(const std::string& symbol, std::size_t count = 100) const;
    
    // ==========================================================================
    // Market Information
    // ==========================================================================
    
    /**
     * @brief Get market info for a symbol
     */
    std::optional<Market> getMarket(const std::string& symbol) const;
    
    /**
     * @brief Get all available markets
     */
    std::vector<Market> getAllMarkets() const;
    
    /**
     * @brief Check if symbol is valid
     */
    bool isValidSymbol(const std::string& symbol) const;
    
    /**
     * @brief Check if market is open
     */
    bool isMarketOpen(const std::string& symbol) const;
    
    /**
     * @brief Refresh market list from API
     */
    void refreshMarkets();
    
    // ==========================================================================
    // Data Updates (internal use)
    // ==========================================================================
    
    /**
     * @brief Update tick data
     */
    void updateTick(const Symbol& symbol, const Tick& tick);
    
    /**
     * @brief Update order book
     */
    void updateOrderBook(const Symbol& symbol, const BookUpdate& update);
    
    /**
     * @brief Apply order book snapshot
     */
    void applyBookSnapshot(const BookSnapshot& snapshot);
    
    /**
     * @brief Add trade
     */
    void addTrade(const Symbol& symbol, const Trade& trade);
    
    /**
     * @brief Update market status
     */
    void updateMarketStatus(const Symbol& symbol, MarketStatus status);
    
    // ==========================================================================
    // Callbacks
    // ==========================================================================
    
    void setTickCallback(TickCallback callback);
    void setBookCallback(BookCallback callback);
    void setTradeCallback(TradeCallback callback);
    void setMarketCallback(MarketCallback callback);
    
    // ==========================================================================
    // Statistics
    // ==========================================================================
    
    struct Stats {
        std::uint64_t ticks_received        = 0;
        std::uint64_t book_updates_received = 0;
        std::uint64_t trades_received       = 0;
        std::uint64_t symbols_subscribed    = 0;
    };
    
    [[nodiscard]] Stats getStats() const;
    void resetStats();
    
private:
    MarketDataManager();
    ~MarketDataManager();
    
    void setupWebSocketCallbacks();
    void handleTickerMessage(const api::WSMessage& msg);
    void handleOrderbookMessage(const api::WSMessage& msg);
    void handleTradesMessage(const api::WSMessage& msg);
    
    std::atomic<bool> running_{false};
    
    // Subscribed symbols
    std::set<std::string> subscribed_symbols_;
    mutable std::shared_mutex subscriptions_mutex_;
    
    // Market data storage
    std::map<std::string, Tick> ticks_;
    std::map<std::string, std::shared_ptr<OrderBook>> order_books_;
    std::map<std::string, std::vector<Trade>> recent_trades_;
    std::map<std::string, Market> markets_;
    mutable std::shared_mutex data_mutex_;
    
    // Trade history size limit
    static constexpr std::size_t MAX_TRADE_HISTORY = 1000;
    
    // Callbacks
    TickCallback tick_callback_;
    BookCallback book_callback_;
    TradeCallback trade_callback_;
    MarketCallback market_callback_;
    mutable std::shared_mutex callbacks_mutex_;
    
    // Statistics
    mutable std::mutex stats_mutex_;
    Stats stats_;
};

} // namespace marketdata
} // namespace architect
