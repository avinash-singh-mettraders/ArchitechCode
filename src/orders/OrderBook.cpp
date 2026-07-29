#include "orders/OrderBook.h"
#include "core/Constants.h"
#include <algorithm>

namespace architect {
namespace orders {

OrderBook::OrderBook(const Symbol& symbol, std::size_t max_depth)
    : symbol_(symbol)
    , max_depth_(max_depth)
    , last_update_time_(std::chrono::high_resolution_clock::now().time_since_epoch())
{}

void OrderBook::applyUpdate(const BookUpdate& update) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    
    switch (update.action) {
        case BookUpdate::Action::ADD:
            addLevel(update.side, update.price, update.quantity);
            break;
        case BookUpdate::Action::UPDATE:
            updateLevel(update.side, update.price, update.quantity);
            break;
        case BookUpdate::Action::DELETE:
            deleteLevel(update.side, update.price);
            break;
    }
    
    last_update_time_ = update.timestamp;
    ++sequence_;
    
    truncateToDepth();
    
    lock.unlock();
    notifyUpdate(update);
}

void OrderBook::applySnapshot(const BookSnapshot& snapshot) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    
    bids_.clear();
    asks_.clear();
    
    for (const auto& level : snapshot.bids) {
        bids_[level.price] = level;
    }
    
    for (const auto& level : snapshot.asks) {
        asks_[level.price] = level;
    }
    
    sequence_ = snapshot.sequence;
    last_update_time_ = snapshot.timestamp;
    
    truncateToDepth();
    
    lock.unlock();
    
    if (snapshot_callback_) {
        snapshot_callback_(snapshot);
    }
}

void OrderBook::clear() {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    bids_.clear();
    asks_.clear();
    sequence_ = 0;
}

BookSnapshot OrderBook::getSnapshot(std::size_t depth) const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    
    BookSnapshot snapshot;
    snapshot.symbol = symbol_;
    snapshot.sequence = sequence_;
    snapshot.timestamp = last_update_time_;
    
    std::size_t bid_depth = depth > 0 ? std::min(depth, bids_.size()) : bids_.size();
    std::size_t ask_depth = depth > 0 ? std::min(depth, asks_.size()) : asks_.size();
    
    snapshot.bids.reserve(bid_depth);
    snapshot.asks.reserve(ask_depth);
    
    std::size_t count = 0;
    for (const auto& [price, level] : bids_) {
        if (count++ >= bid_depth) break;
        snapshot.bids.push_back(level);
    }
    
    count = 0;
    for (const auto& [price, level] : asks_) {
        if (count++ >= ask_depth) break;
        snapshot.asks.push_back(level);
    }
    
    return snapshot;
}

std::pair<BookLevel, BookLevel> OrderBook::getBBO() const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    
    BookLevel best_bid, best_ask;
    
    if (!bids_.empty()) {
        best_bid = bids_.begin()->second;
    }
    if (!asks_.empty()) {
        best_ask = asks_.begin()->second;
    }
    
    return {best_bid, best_ask};
}

std::vector<BookLevel> OrderBook::getBids(std::size_t depth) const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    
    std::vector<BookLevel> result;
    std::size_t count = depth > 0 ? std::min(depth, bids_.size()) : bids_.size();
    result.reserve(count);
    
    std::size_t i = 0;
    for (const auto& [price, level] : bids_) {
        if (i++ >= count) break;
        result.push_back(level);
    }
    
    return result;
}

std::vector<BookLevel> OrderBook::getAsks(std::size_t depth) const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    
    std::vector<BookLevel> result;
    std::size_t count = depth > 0 ? std::min(depth, asks_.size()) : asks_.size();
    result.reserve(count);
    
    std::size_t i = 0;
    for (const auto& [price, level] : asks_) {
        if (i++ >= count) break;
        result.push_back(level);
    }
    
    return result;
}

Quantity OrderBook::getQuantityAtPrice(Side side, Price price) const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    
    if (side == Side::BUY) {
        auto it = bids_.find(price);
        return it != bids_.end() ? it->second.quantity : 0.0;
    } else {
        auto it = asks_.find(price);
        return it != asks_.end() ? it->second.quantity : 0.0;
    }
}

Price OrderBook::calculateVWAP(Side side, Quantity quantity) const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    
    const auto& book = (side == Side::BUY) 
        ? reinterpret_cast<const std::map<Price, BookLevel>&>(asks_)  // Buy from asks
        : reinterpret_cast<const std::map<Price, BookLevel>&>(bids_); // Sell to bids
    
    Decimal total_value = 0.0;
    Quantity remaining = quantity;
    
    for (const auto& [price, level] : book) {
        Quantity fill = std::min(remaining, level.quantity);
        total_value += price * fill;
        remaining -= fill;
        
        if (remaining <= core::constants::QUANTITY_EPSILON) {
            break;
        }
    }
    
    Quantity filled = quantity - remaining;
    return filled > 0 ? total_value / filled : 0.0;
}

double OrderBook::calculateSlippage(Side side, Quantity quantity) const {
    auto [best_bid, best_ask] = getBBO();
    
    Price reference_price = (side == Side::BUY) ? best_ask.price : best_bid.price;
    if (reference_price <= 0) return 0.0;
    
    Price vwap = calculateVWAP(side, quantity);
    if (vwap <= 0) return 0.0;
    
    double slippage = (side == Side::BUY) 
        ? (vwap - reference_price) / reference_price
        : (reference_price - vwap) / reference_price;
    
    return slippage * 100.0; // Return as percentage
}

bool OrderBook::isEmpty() const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return bids_.empty() && asks_.empty();
}

std::size_t OrderBook::getBidLevels() const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return bids_.size();
}

std::size_t OrderBook::getAskLevels() const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return asks_.size();
}

void OrderBook::setUpdateCallback(BookUpdateCallback callback) {
    update_callback_ = std::move(callback);
}

void OrderBook::setSnapshotCallback(BookSnapshotCallback callback) {
    snapshot_callback_ = std::move(callback);
}

void OrderBook::addLevel(Side side, Price price, Quantity quantity) {
    BookLevel level(price, quantity, 1);
    
    if (side == Side::BUY) {
        bids_[price] = level;
    } else {
        asks_[price] = level;
    }
}

void OrderBook::updateLevel(Side side, Price price, Quantity quantity) {
    if (quantity <= core::constants::QUANTITY_EPSILON) {
        deleteLevel(side, price);
        return;
    }
    
    if (side == Side::BUY) {
        auto it = bids_.find(price);
        if (it != bids_.end()) {
            it->second.quantity = quantity;
        } else {
            addLevel(side, price, quantity);
        }
    } else {
        auto it = asks_.find(price);
        if (it != asks_.end()) {
            it->second.quantity = quantity;
        } else {
            addLevel(side, price, quantity);
        }
    }
}

void OrderBook::deleteLevel(Side side, Price price) {
    if (side == Side::BUY) {
        bids_.erase(price);
    } else {
        asks_.erase(price);
    }
}

void OrderBook::truncateToDepth() {
    while (bids_.size() > max_depth_) {
        bids_.erase(std::prev(bids_.end()));
    }
    while (asks_.size() > max_depth_) {
        asks_.erase(std::prev(asks_.end()));
    }
}

void OrderBook::notifyUpdate(const BookUpdate& update) {
    if (update_callback_) {
        update_callback_(symbol_, update);
    }
}

// =============================================================================
// OrderBookManager
// =============================================================================

OrderBookManager& OrderBookManager::getInstance() {
    static OrderBookManager instance;
    return instance;
}

std::shared_ptr<OrderBook> OrderBookManager::getOrderBook(const Symbol& symbol) {
    std::string key(symbol.data());
    
    {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        auto it = books_.find(key);
        if (it != books_.end()) {
            return it->second;
        }
    }
    
    {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        // Double-check
        auto it = books_.find(key);
        if (it != books_.end()) {
            return it->second;
        }
        
        auto book = std::make_shared<OrderBook>(symbol, default_depth_);
        books_[key] = book;
        return book;
    }
}

bool OrderBookManager::hasOrderBook(const Symbol& symbol) const {
    std::string key(symbol.data());
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return books_.find(key) != books_.end();
}

void OrderBookManager::removeOrderBook(const Symbol& symbol) {
    std::string key(symbol.data());
    std::unique_lock<std::shared_mutex> lock(mutex_);
    books_.erase(key);
}

std::vector<Symbol> OrderBookManager::getActiveSymbols() const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    std::vector<Symbol> result;
    result.reserve(books_.size());
    
    for (const auto& [key, book] : books_) {
        result.push_back(book->getSymbol());
    }
    
    return result;
}

void OrderBookManager::clearAll() {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    books_.clear();
}

void OrderBookManager::applyUpdate(const Symbol& symbol, const BookUpdate& update) {
    auto book = getOrderBook(symbol);
    book->applyUpdate(update);
}

void OrderBookManager::applySnapshot(const BookSnapshot& snapshot) {
    auto book = getOrderBook(snapshot.symbol);
    book->applySnapshot(snapshot);
}

} // namespace orders
} // namespace architect
