#pragma once

/**
 * @file OrderBook.h
 * @brief High-performance order book implementation
 */

#include "core/Types.h"
#include <map>
#include <vector>
#include <mutex>
#include <shared_mutex>
#include <functional>
#include <memory>

namespace architect {
namespace orders {

using namespace core;

/**
 * @brief Order book level with price, quantity, and order count
 */
struct BookLevel {
    Price       price       = 0.0;
    Quantity    quantity    = 0.0;
    std::uint32_t order_count = 0;
    
    BookLevel() = default;
    BookLevel(Price p, Quantity q, std::uint32_t count = 1)
        : price(p), quantity(q), order_count(count) {}
};

/**
 * @brief Order book update (delta)
 */
struct BookUpdate {
    enum class Action : std::uint8_t {
        ADD     = 0,
        UPDATE  = 1,
        DELETE  = 2
    };
    
    Side        side;
    Action      action;
    Price       price;
    Quantity    quantity;
    Timestamp   timestamp;
};

/**
 * @brief Order book snapshot
 */
struct BookSnapshot {
    Symbol                  symbol;
    std::vector<BookLevel>  bids;
    std::vector<BookLevel>  asks;
    SequenceNum             sequence;
    Timestamp               timestamp;
    
    [[nodiscard]] Price getBestBid() const {
        return bids.empty() ? 0.0 : bids.front().price;
    }
    
    [[nodiscard]] Price getBestAsk() const {
        return asks.empty() ? 0.0 : asks.front().price;
    }
    
    [[nodiscard]] Price getMidPrice() const {
        Price bid = getBestBid();
        Price ask = getBestAsk();
        if (bid <= 0 || ask <= 0) return 0.0;
        return (bid + ask) / 2.0;
    }
    
    [[nodiscard]] Price getSpread() const {
        Price bid = getBestBid();
        Price ask = getBestAsk();
        if (bid <= 0 || ask <= 0) return 0.0;
        return ask - bid;
    }
    
    [[nodiscard]] double getSpreadBps() const {
        Price mid = getMidPrice();
        if (mid <= 0) return 0.0;
        return (getSpread() / mid) * 10000.0;
    }
};

using BookUpdateCallback = std::function<void(const Symbol&, const BookUpdate&)>;
using BookSnapshotCallback = std::function<void(const BookSnapshot&)>;

/**
 * @brief High-performance order book for a single symbol
 * 
 * Features:
 * - O(log n) insert/update/delete
 * - Efficient snapshot generation
 * - Delta tracking
 * - Thread-safe operations
 */
class OrderBook {
public:
    explicit OrderBook(const Symbol& symbol, std::size_t max_depth = 100);
    
    // ==========================================================================
    // Updates
    // ==========================================================================
    
    /**
     * @brief Apply a book update
     */
    void applyUpdate(const BookUpdate& update);
    
    /**
     * @brief Apply a full snapshot (replaces current book)
     */
    void applySnapshot(const BookSnapshot& snapshot);
    
    /**
     * @brief Clear the order book
     */
    void clear();
    
    // ==========================================================================
    // Queries
    // ==========================================================================
    
    /**
     * @brief Get the current snapshot
     */
    [[nodiscard]] BookSnapshot getSnapshot(std::size_t depth = 0) const;
    
    /**
     * @brief Get best bid/ask
     */
    [[nodiscard]] std::pair<BookLevel, BookLevel> getBBO() const;
    
    /**
     * @brief Get bid levels
     */
    [[nodiscard]] std::vector<BookLevel> getBids(std::size_t depth = 0) const;
    
    /**
     * @brief Get ask levels
     */
    [[nodiscard]] std::vector<BookLevel> getAsks(std::size_t depth = 0) const;
    
    /**
     * @brief Get quantity at a specific price
     */
    [[nodiscard]] Quantity getQuantityAtPrice(Side side, Price price) const;
    
    /**
     * @brief Calculate VWAP for a given quantity
     */
    [[nodiscard]] Price calculateVWAP(Side side, Quantity quantity) const;
    
    /**
     * @brief Calculate slippage for a given quantity
     */
    [[nodiscard]] double calculateSlippage(Side side, Quantity quantity) const;
    
    // ==========================================================================
    // Properties
    // ==========================================================================
    
    [[nodiscard]] const Symbol& getSymbol() const { return symbol_; }
    [[nodiscard]] SequenceNum getSequence() const { return sequence_; }
    [[nodiscard]] Timestamp getLastUpdateTime() const { return last_update_time_; }
    [[nodiscard]] bool isEmpty() const;
    [[nodiscard]] std::size_t getBidLevels() const;
    [[nodiscard]] std::size_t getAskLevels() const;
    
    // ==========================================================================
    // Callbacks
    // ==========================================================================
    
    void setUpdateCallback(BookUpdateCallback callback);
    void setSnapshotCallback(BookSnapshotCallback callback);
    
private:
    void addLevel(Side side, Price price, Quantity quantity);
    void updateLevel(Side side, Price price, Quantity quantity);
    void deleteLevel(Side side, Price price);
    void truncateToDepth();
    void notifyUpdate(const BookUpdate& update);
    
    Symbol symbol_;
    std::size_t max_depth_;
    
    // Bids: descending order (highest first)
    std::map<Price, BookLevel, std::greater<Price>> bids_;
    // Asks: ascending order (lowest first)
    std::map<Price, BookLevel, std::less<Price>> asks_;
    
    SequenceNum sequence_ = 0;
    Timestamp last_update_time_;
    
    mutable std::shared_mutex mutex_;
    
    BookUpdateCallback update_callback_;
    BookSnapshotCallback snapshot_callback_;
};

/**
 * @brief Manager for multiple order books
 */
class OrderBookManager {
public:
    static OrderBookManager& getInstance();
    
    OrderBookManager(const OrderBookManager&) = delete;
    OrderBookManager& operator=(const OrderBookManager&) = delete;
    
    // ==========================================================================
    // Order Book Management
    // ==========================================================================
    
    /**
     * @brief Get or create an order book for a symbol
     */
    std::shared_ptr<OrderBook> getOrderBook(const Symbol& symbol);
    
    /**
     * @brief Check if order book exists
     */
    bool hasOrderBook(const Symbol& symbol) const;
    
    /**
     * @brief Remove an order book
     */
    void removeOrderBook(const Symbol& symbol);
    
    /**
     * @brief Get all symbols with active order books
     */
    std::vector<Symbol> getActiveSymbols() const;
    
    /**
     * @brief Clear all order books
     */
    void clearAll();
    
    // ==========================================================================
    // Bulk Operations
    // ==========================================================================
    
    /**
     * @brief Apply update to appropriate order book
     */
    void applyUpdate(const Symbol& symbol, const BookUpdate& update);
    
    /**
     * @brief Apply snapshot to appropriate order book
     */
    void applySnapshot(const BookSnapshot& snapshot);
    
private:
    OrderBookManager() = default;
    
    std::unordered_map<std::string, std::shared_ptr<OrderBook>> books_;
    mutable std::shared_mutex mutex_;
    std::size_t default_depth_ = 100;
};

} // namespace orders
} // namespace architect
