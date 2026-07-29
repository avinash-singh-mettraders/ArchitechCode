#include "marketdata/MarketDataManager.h"
#include "api/RestClient.h"
#include "api/WebSocketClient.h"
#include "events/EventManager.h"
#include "config/Config.h"

namespace architect {
namespace marketdata {

MarketDataManager& MarketDataManager::getInstance() {
    static MarketDataManager instance;
    return instance;
}

MarketDataManager::MarketDataManager() {
    setupWebSocketCallbacks();
}

MarketDataManager::~MarketDataManager() {
    stop();
}

void MarketDataManager::start() {
    if (running_.exchange(true)) {
        return; // Already running
    }
    
    // Load markets from API
    refreshMarkets();
    
    // Subscribe to watchlist symbols from config
    auto& config = config::Config::getInstance();
    const auto watchlist = config.getMarketDataSubscriptionSymbols();
    if (!watchlist.empty()) {
        subscribeMultiple(watchlist);
    }
}

void MarketDataManager::stop() {
    if (!running_.exchange(false)) {
        return; // Already stopped
    }
    
    unsubscribeAll();
}

void MarketDataManager::setupWebSocketCallbacks() {
    auto& ws = api::WebSocketClient::getInstance();
    
    // Set channel callbacks
    ws.setChannelCallback(core::constants::api::WS_TICKER, 
        [this](const api::WSMessage& msg) { handleTickerMessage(msg); });
    
    ws.setChannelCallback(core::constants::api::WS_ORDERBOOK,
        [this](const api::WSMessage& msg) { handleOrderbookMessage(msg); });
    
    ws.setChannelCallback(core::constants::api::WS_TRADES,
        [this](const api::WSMessage& msg) { handleTradesMessage(msg); });
}

bool MarketDataManager::subscribe(const std::string& symbol) {
    {
        std::unique_lock<std::shared_mutex> lock(subscriptions_mutex_);
        if (subscribed_symbols_.count(symbol)) {
            return true; // Already subscribed
        }
        subscribed_symbols_.insert(symbol);
    }
    
    auto& ws = api::WebSocketClient::getInstance();
    
    // Subscribe to all relevant channels for this symbol
    bool success = true;
    success &= ws.subscribeTicker(symbol);
    success &= ws.subscribeOrderbook(symbol);
    success &= ws.subscribeTrades(symbol);
    
    if (success) {
        // Create order book for this symbol
        std::unique_lock<std::shared_mutex> lock(data_mutex_);
        Symbol sym = makeSymbol(symbol);
        auto book = std::make_shared<OrderBook>(sym, 
            config::Config::getInstance().getInt("orderbook.default_depth", 100));
        order_books_[symbol] = book;
        
        std::lock_guard<std::mutex> stats_lock(stats_mutex_);
        ++stats_.symbols_subscribed;
    }
    
    return success;
}

bool MarketDataManager::subscribeMultiple(const std::vector<std::string>& symbols) {
    bool all_success = true;
    for (const auto& symbol : symbols) {
        if (!subscribe(symbol)) {
            all_success = false;
        }
    }
    return all_success;
}

bool MarketDataManager::unsubscribe(const std::string& symbol) {
    {
        std::unique_lock<std::shared_mutex> lock(subscriptions_mutex_);
        if (!subscribed_symbols_.erase(symbol)) {
            return true; // Was not subscribed
        }
    }
    
    auto& ws = api::WebSocketClient::getInstance();
    ws.unsubscribe(core::constants::api::WS_TICKER, symbol);
    ws.unsubscribe(core::constants::api::WS_ORDERBOOK, symbol);
    ws.unsubscribe(core::constants::api::WS_TRADES, symbol);
    
    {
        std::lock_guard<std::mutex> stats_lock(stats_mutex_);
        if (stats_.symbols_subscribed > 0) {
            --stats_.symbols_subscribed;
        }
    }
    
    return true;
}

void MarketDataManager::unsubscribeAll() {
    std::vector<std::string> symbols;
    {
        std::shared_lock<std::shared_mutex> lock(subscriptions_mutex_);
        symbols.assign(subscribed_symbols_.begin(), subscribed_symbols_.end());
    }
    
    for (const auto& symbol : symbols) {
        unsubscribe(symbol);
    }
}

std::vector<std::string> MarketDataManager::getSubscribedSymbols() const {
    std::shared_lock<std::shared_mutex> lock(subscriptions_mutex_);
    return std::vector<std::string>(subscribed_symbols_.begin(), subscribed_symbols_.end());
}

std::optional<Tick> MarketDataManager::getTick(const std::string& symbol) const {
    std::shared_lock<std::shared_mutex> lock(data_mutex_);
    auto it = ticks_.find(symbol);
    if (it == ticks_.end()) {
        return std::nullopt;
    }
    return it->second;
}

std::shared_ptr<OrderBook> MarketDataManager::getOrderBook(const std::string& symbol) {
    std::shared_lock<std::shared_mutex> lock(data_mutex_);
    auto it = order_books_.find(symbol);
    if (it == order_books_.end()) {
        return nullptr;
    }
    return it->second;
}

std::optional<std::pair<BookLevel, BookLevel>> MarketDataManager::getBBO(const std::string& symbol) const {
    std::shared_lock<std::shared_mutex> lock(data_mutex_);
    auto it = order_books_.find(symbol);
    if (it == order_books_.end() || it->second->isEmpty()) {
        return std::nullopt;
    }
    return it->second->getBBO();
}

std::optional<BookSnapshot> MarketDataManager::getL2Snapshot(const std::string& symbol, std::size_t depth) const {
    std::shared_lock<std::shared_mutex> lock(data_mutex_);
    auto it = order_books_.find(symbol);
    if (it == order_books_.end()) {
        return std::nullopt;
    }
    return it->second->getSnapshot(depth);
}

std::optional<Price> MarketDataManager::getMidPrice(const std::string& symbol) const {
    auto bbo = getBBO(symbol);
    if (!bbo.has_value()) {
        return std::nullopt;
    }
    
    auto [bid, ask] = bbo.value();
    if (bid.price <= 0 || ask.price <= 0) {
        return std::nullopt;
    }
    
    return (bid.price + ask.price) / 2.0;
}

std::optional<Price> MarketDataManager::getSpread(const std::string& symbol) const {
    auto bbo = getBBO(symbol);
    if (!bbo.has_value()) {
        return std::nullopt;
    }
    
    auto [bid, ask] = bbo.value();
    if (bid.price <= 0 || ask.price <= 0) {
        return std::nullopt;
    }
    
    return ask.price - bid.price;
}

std::vector<Trade> MarketDataManager::getRecentTrades(const std::string& symbol, std::size_t count) const {
    std::shared_lock<std::shared_mutex> lock(data_mutex_);
    auto it = recent_trades_.find(symbol);
    if (it == recent_trades_.end()) {
        return {};
    }
    
    const auto& trades = it->second;
    std::size_t actual_count = std::min(count, trades.size());
    
    // Return most recent trades
    return std::vector<Trade>(trades.end() - actual_count, trades.end());
}

std::optional<Market> MarketDataManager::getMarket(const std::string& symbol) const {
    std::shared_lock<std::shared_mutex> lock(data_mutex_);
    auto it = markets_.find(symbol);
    if (it == markets_.end()) {
        return std::nullopt;
    }
    return it->second;
}

std::vector<Market> MarketDataManager::getAllMarkets() const {
    std::shared_lock<std::shared_mutex> lock(data_mutex_);
    std::vector<Market> result;
    result.reserve(markets_.size());
    for (const auto& [symbol, market] : markets_) {
        result.push_back(market);
    }
    return result;
}

bool MarketDataManager::isValidSymbol(const std::string& symbol) const {
    std::shared_lock<std::shared_mutex> lock(data_mutex_);
    return markets_.find(symbol) != markets_.end();
}

bool MarketDataManager::isMarketOpen(const std::string& symbol) const {
    auto market = getMarket(symbol);
    if (!market.has_value()) {
        return false;
    }
    return market->status == MarketStatus::OPEN;
}

void MarketDataManager::refreshMarkets() {
    auto& rest = api::RestClient::getInstance();
    auto response = rest.getMarkets();
    
    if (!response.isOk()) {
        return;
    }
    
    try {
        auto json_data = response.parseJson();
        
        std::unique_lock<std::shared_mutex> lock(data_mutex_);
        markets_.clear();
        
        if (json_data.is_array()) {
            for (const auto& item : json_data) {
                Market market;
                
                std::string symbol_str = item.value("symbol", "");
                market.symbol = makeSymbol(symbol_str);
                market.base_currency = item.value("base_currency", "");
                market.quote_currency = item.value("quote_currency", "");
                market.min_price = item.value("min_price", 0.0);
                market.max_price = item.value("max_price", 0.0);
                market.min_quantity = item.value("min_quantity", 0.0);
                market.max_quantity = item.value("max_quantity", 0.0);
                market.min_notional = item.value("min_notional", 0.0);
                market.price_precision = item.value("price_precision", 8);
                market.quantity_precision = item.value("quantity_precision", 8);
                market.maker_fee = item.value("maker_fee", 0.0);
                market.taker_fee = item.value("taker_fee", 0.0);
                market.is_tradeable = item.value("is_tradeable", true);
                
                std::string status_str = item.value("status", "open");
                if (status_str == "open") market.status = MarketStatus::OPEN;
                else if (status_str == "closed") market.status = MarketStatus::CLOSED;
                else if (status_str == "halted") market.status = MarketStatus::HALTED;
                
                markets_[symbol_str] = market;
            }
        }
    } catch (...) {
        // Failed to parse markets
    }
}

void MarketDataManager::updateTick(const Symbol& symbol, const Tick& tick) {
    std::string symbol_str(symbol.data());
    
    {
        std::unique_lock<std::shared_mutex> lock(data_mutex_);
        ticks_[symbol_str] = tick;
    }
    
    {
        std::lock_guard<std::mutex> lock(stats_mutex_);
        ++stats_.ticks_received;
    }
    
    // Notify subscribers
    {
        std::shared_lock<std::shared_mutex> lock(callbacks_mutex_);
        if (tick_callback_) {
            tick_callback_(symbol, tick);
        }
    }
    
    // Publish event
    events::TickEventData event_data;
    event_data.symbol = symbol;
    event_data.bid = tick.bid_price;
    event_data.ask = tick.ask_price;
    event_data.last = tick.last_price;
    event_data.bid_size = tick.bid_size;
    event_data.ask_size = tick.ask_size;
    event_data.last_size = tick.last_size;
    event_data.volume = tick.volume_24h;
    event_data.exchange_timestamp = tick.timestamp;
    
    auto event = events::EventFactory::createTickEvent(event_data);
    events::EventManager::getInstance().publish(event);
}

void MarketDataManager::updateOrderBook(const Symbol& symbol, const BookUpdate& update) {
    std::string symbol_str(symbol.data());
    
    std::shared_ptr<OrderBook> book;
    {
        std::shared_lock<std::shared_mutex> lock(data_mutex_);
        auto it = order_books_.find(symbol_str);
        if (it != order_books_.end()) {
            book = it->second;
        }
    }
    
    if (book) {
        book->applyUpdate(update);
        
        {
            std::lock_guard<std::mutex> lock(stats_mutex_);
            ++stats_.book_updates_received;
        }
        
        // Notify subscribers
        {
            std::shared_lock<std::shared_mutex> lock(callbacks_mutex_);
            if (book_callback_) {
                book_callback_(symbol, book->getSnapshot());
            }
        }
        
        // Emit L2_UPDATE event for predictors/strategies
        auto snapshot = book->getSnapshot();
        if (!snapshot.bids.empty() && !snapshot.asks.empty()) {
            // Create tick event data from L2 update
            events::TickEventData tick_data;
            tick_data.symbol = symbol;
            tick_data.bid = snapshot.bids[0].price;
            tick_data.ask = snapshot.asks[0].price;
            tick_data.bid_size = snapshot.bids[0].quantity;
            tick_data.ask_size = snapshot.asks[0].quantity;
            tick_data.exchange_timestamp = std::chrono::high_resolution_clock::now().time_since_epoch();
            
            auto event = std::make_shared<events::Event>(
                events::EventType::L2_UPDATE, tick_data, events::EventPriority::NORMAL);
            events::EventManager::getInstance().publish(event);
        }
    }
}

void MarketDataManager::applyBookSnapshot(const BookSnapshot& snapshot) {
    std::string symbol_str(snapshot.symbol.data());
    
    std::shared_ptr<OrderBook> book;
    {
        std::unique_lock<std::shared_mutex> lock(data_mutex_);
        auto it = order_books_.find(symbol_str);
        if (it == order_books_.end()) {
            book = std::make_shared<OrderBook>(snapshot.symbol);
            order_books_[symbol_str] = book;
        } else {
            book = it->second;
        }
    }
    
    if (book) {
        book->applySnapshot(snapshot);
        
        // Notify subscribers
        {
            std::shared_lock<std::shared_mutex> lock(callbacks_mutex_);
            if (book_callback_) {
                book_callback_(snapshot.symbol, snapshot);
            }
        }
    }
}

void MarketDataManager::addTrade(const Symbol& symbol, const Trade& trade) {
    std::string symbol_str(symbol.data());
    
    {
        std::unique_lock<std::shared_mutex> lock(data_mutex_);
        auto& trades = recent_trades_[symbol_str];
        trades.push_back(trade);
        
        // Limit trade history size
        if (trades.size() > MAX_TRADE_HISTORY) {
            trades.erase(trades.begin(), trades.begin() + (trades.size() - MAX_TRADE_HISTORY));
        }
    }
    
    {
        std::lock_guard<std::mutex> lock(stats_mutex_);
        ++stats_.trades_received;
    }
    
    // Notify subscribers
    {
        std::shared_lock<std::shared_mutex> lock(callbacks_mutex_);
        if (trade_callback_) {
            trade_callback_(symbol, trade);
        }
    }
    
    // Publish trade event
    auto event = std::make_shared<events::Event>(
        events::EventType::TRADE_UPDATE,
        trade,
        events::EventPriority::NORMAL
    );
    events::EventManager::getInstance().publish(event);
}

void MarketDataManager::updateMarketStatus(const Symbol& symbol, MarketStatus status) {
    std::string symbol_str(symbol.data());
    
    {
        std::unique_lock<std::shared_mutex> lock(data_mutex_);
        auto it = markets_.find(symbol_str);
        if (it != markets_.end()) {
            it->second.status = status;
        }
    }
    
    // Notify subscribers
    {
        std::shared_lock<std::shared_mutex> lock(callbacks_mutex_);
        if (market_callback_) {
            market_callback_(symbol, status);
        }
    }
    
    // Publish market status event
    auto event = std::make_shared<events::Event>(
        events::EventType::MARKET_STATUS_CHANGE,
        static_cast<int>(status),
        events::EventPriority::HIGH
    );
    events::EventManager::getInstance().publish(event);
}

void MarketDataManager::handleTickerMessage(const api::WSMessage& msg) {
    if (msg.data.empty()) return;
    
    try {
        Symbol symbol = makeSymbol(msg.symbol);
        
        Tick tick;
        tick.symbol = symbol;
        tick.bid_price = msg.data.value("bid", 0.0);
        tick.ask_price = msg.data.value("ask", 0.0);
        tick.bid_size = msg.data.value("bid_size", 0.0);
        tick.ask_size = msg.data.value("ask_size", 0.0);
        tick.last_price = msg.data.value("last", 0.0);
        tick.last_size = msg.data.value("last_size", 0.0);
        tick.volume_24h = msg.data.value("volume", 0.0);
        tick.timestamp = msg.timestamp;
        
        updateTick(symbol, tick);
    } catch (...) {
        // Failed to parse ticker message
    }
}

void MarketDataManager::handleOrderbookMessage(const api::WSMessage& msg) {
    if (msg.data.empty()) return;
    
    try {
        Symbol symbol = makeSymbol(msg.symbol);
        
        // Check if this is a snapshot or delta
        bool is_snapshot = msg.data.value("type", "") == "snapshot";
        
        if (is_snapshot) {
            BookSnapshot snapshot;
            snapshot.symbol = symbol;
            snapshot.timestamp = msg.timestamp;
            snapshot.sequence = msg.data.value("sequence", 0ULL);
            
            if (msg.data.contains("bids")) {
                for (const auto& bid : msg.data["bids"]) {
                    BookLevel level;
                    level.price = bid[0].get<Price>();
                    level.quantity = bid[1].get<Quantity>();
                    snapshot.bids.push_back(level);
                }
            }
            
            if (msg.data.contains("asks")) {
                for (const auto& ask : msg.data["asks"]) {
                    BookLevel level;
                    level.price = ask[0].get<Price>();
                    level.quantity = ask[1].get<Quantity>();
                    snapshot.asks.push_back(level);
                }
            }
            
            applyBookSnapshot(snapshot);
        } else {
            // Delta update
            if (msg.data.contains("changes")) {
                for (const auto& change : msg.data["changes"]) {
                    BookUpdate update;
                    
                    std::string side = change[0].get<std::string>();
                    update.side = (side == "buy") ? Side::BUY : Side::SELL;
                    update.price = change[1].get<Price>();
                    update.quantity = change[2].get<Quantity>();
                    update.timestamp = msg.timestamp;
                    
                    if (update.quantity <= 0) {
                        update.action = BookUpdate::Action::DELETE;
                    } else {
                        update.action = BookUpdate::Action::UPDATE;
                    }
                    
                    updateOrderBook(symbol, update);
                }
            }
        }
    } catch (...) {
        // Failed to parse orderbook message
    }
}

void MarketDataManager::handleTradesMessage(const api::WSMessage& msg) {
    if (msg.data.empty()) return;
    
    try {
        Symbol symbol = makeSymbol(msg.symbol);
        
        auto trades_array = msg.data.is_array() ? msg.data : api::json::array({msg.data});
        
        for (const auto& trade_data : trades_array) {
            Trade trade;
            trade.trade_id = trade_data.value("trade_id", 0ULL);
            trade.symbol = symbol;
            
            std::string side_str = trade_data.value("side", "buy");
            trade.side = (side_str == "buy") ? Side::BUY : Side::SELL;
            
            trade.price = trade_data.value("price", 0.0);
            trade.quantity = trade_data.value("quantity", 0.0);
            trade.executed_at = msg.timestamp;
            
            addTrade(symbol, trade);
        }
    } catch (...) {
        // Failed to parse trades message
    }
}

void MarketDataManager::setTickCallback(TickCallback callback) {
    std::unique_lock<std::shared_mutex> lock(callbacks_mutex_);
    tick_callback_ = std::move(callback);
}

void MarketDataManager::setBookCallback(BookCallback callback) {
    std::unique_lock<std::shared_mutex> lock(callbacks_mutex_);
    book_callback_ = std::move(callback);
}

void MarketDataManager::setTradeCallback(TradeCallback callback) {
    std::unique_lock<std::shared_mutex> lock(callbacks_mutex_);
    trade_callback_ = std::move(callback);
}

void MarketDataManager::setMarketCallback(MarketCallback callback) {
    std::unique_lock<std::shared_mutex> lock(callbacks_mutex_);
    market_callback_ = std::move(callback);
}

MarketDataManager::Stats MarketDataManager::getStats() const {
    std::lock_guard<std::mutex> lock(stats_mutex_);
    return stats_;
}

void MarketDataManager::resetStats() {
    std::lock_guard<std::mutex> lock(stats_mutex_);
    
    auto subscribed = stats_.symbols_subscribed;
    stats_ = Stats{};
    stats_.symbols_subscribed = subscribed;
}

} // namespace marketdata
} // namespace architect
