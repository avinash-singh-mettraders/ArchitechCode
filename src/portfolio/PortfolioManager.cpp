#include "portfolio/PortfolioManager.h"
#include "api/RestClient.h"
#include "events/EventManager.h"
#include "marketdata/MarketDataManager.h"
#include "utils/PositionRestQty.h"
#include <nlohmann/json.hpp>
#include <cctype>
#include <cmath>
#include <string_view>

namespace architect {
namespace portfolio {

namespace {

std::string normalizePositionMapKey(std::string_view sv) {
    std::string out;
    out.reserve(sv.size());
    for (unsigned char uc : sv) {
        const char c = static_cast<char>(uc);
        if (c == ' ' || c == '-' || c == '_') {
            continue;
        }
        out.push_back(static_cast<char>(std::toupper(static_cast<unsigned char>(c))));
    }
    return out;
}

double parseJsonDoubleMember(const nlohmann::json& j, std::initializer_list<const char*> keys) {
    if (!j.is_object()) {
        return 0.0;
    }
    for (const char* key : keys) {
        if (!j.contains(key)) {
            continue;
        }
        const auto& v = j.at(key);
        if (v.is_string()) {
            try {
                return std::stod(v.get<std::string>());
            } catch (...) {
                continue;
            }
        }
        if (v.is_number()) {
            return v.get<double>();
        }
    }
    return 0.0;
}

} // namespace

PortfolioManager& PortfolioManager::getInstance() {
    static PortfolioManager instance;
    return instance;
}

PortfolioManager::PortfolioManager() = default;

void PortfolioManager::initialize() {
    refresh();
}

void PortfolioManager::refresh() {
    auto& rest = api::RestClient::getInstance();
    
    // Fetch balances — OpenAPI GetBalancesResponse: { "balances": [ { "symbol", "amount" } ], "usd_borrow"? }
    // https://docs.architect.exchange/api-reference/portfolio-management/get-balances
    auto balances_response = rest.getBalances();
    if (balances_response.isOk()) {
        try {
            auto json_data = balances_response.parseJson();
            nlohmann::json balances_array;
            bool have_snapshot = false;
            if (json_data.is_array()) {
                balances_array = std::move(json_data);
                have_snapshot = true;
            } else if (json_data.is_object() && json_data.contains("balances")
                       && json_data["balances"].is_array()) {
                balances_array = json_data["balances"];
                have_snapshot = true;
            }
            if (have_snapshot && balances_array.is_array()) {
                {
                    std::unique_lock<std::shared_mutex> lock(balances_mutex_);
                    balances_.clear();
                }
                for (const auto& item : balances_array) {
                    if (!item.is_object()) {
                        continue;
                    }
                    AccountBalance balance;
                    balance.currency = item.value("symbol", item.value("currency", ""));
                    balance.updated_at = std::chrono::high_resolution_clock::now().time_since_epoch();
                    if (item.contains("amount")) {
                        const auto& av = item["amount"];
                        double amt = 0.0;
                        if (av.is_string()) {
                            try {
                                amt = std::stod(av.get<std::string>());
                            } catch (...) {
                                amt = 0.0;
                            }
                        } else if (av.is_number()) {
                            amt = av.get<double>();
                        }
                        balance.available = amt;
                        balance.locked = 0.0;
                        balance.total = amt;
                    } else {
                        balance.available = item.value("available", 0.0);
                        balance.locked = item.value("locked", 0.0);
                        balance.total = item.value("total", 0.0);
                        if (balance.total <= 0.0 && (balance.available > 0.0 || balance.locked > 0.0)) {
                            balance.total = balance.available + balance.locked;
                        }
                    }
                    if (!balance.currency.empty()) {
                        updateBalance(balance);
                    }
                }
            }
        } catch (...) {}
    }
    
    // Fetch positions (unwrap { "positions": [...] }, alternate field names, normalized symbol keys)
    auto positions_response = rest.getPositions();
    if (positions_response.isOk()) {
        try {
            auto json_data = positions_response.parseJson();
            nlohmann::json positions_array;
            bool have_snapshot = false;
            if (json_data.is_array()) {
                positions_array = std::move(json_data);
                have_snapshot = true;
            } else if (json_data.is_object()) {
                if (json_data.contains("positions") && json_data["positions"].is_array()) {
                    positions_array = json_data["positions"];
                    have_snapshot = true;
                } else {
                    for (const char* k : {"data", "items", "results"}) {
                        if (json_data.contains(k) && json_data[k].is_array()) {
                            positions_array = json_data[k];
                            have_snapshot = true;
                            break;
                        }
                    }
                }
            }
            if (have_snapshot && positions_array.is_array()) {
                {
                    std::unique_lock<std::shared_mutex> lock(positions_mutex_);
                    positions_.clear();
                }
                for (const auto& item : positions_array) {
                    if (!item.is_object()) {
                        continue;
                    }
                    std::string raw_symbol = item.value("symbol", item.value("s", ""));
                    if (raw_symbol.empty()) {
                        continue;
                    }
                    const double signed_qty = utils::signedPositionQtyFromRestRow(item);
                    const double abs_qty = std::abs(signed_qty);
                    if (abs_qty < 1e-12) {
                        continue;
                    }
                    Position position;
                    position.symbol = makeSymbol(raw_symbol);
                    position.side = (signed_qty >= 0.0) ? Side::BUY : Side::SELL;
                    position.quantity = abs_qty;
                    position.entry_price = parseJsonDoubleMember(item, {
                        "average_price", "avg_price", "avgPrice", "entry_price",
                        "entryPrice", "cost_basis", "avgEntryPrice"});
                    position.mark_price = parseJsonDoubleMember(item, {
                        "mark_price", "markPrice", "mp", "last_price", "lastPrice"});
                    if (position.mark_price <= 0.0 && position.entry_price > 0.0) {
                        position.mark_price = position.entry_price;
                    }
                    // OpenAPI: signed_notional (string); derive ~mark from |notional|/|qty| when price fields absent
                    const double signed_notional = utils::signedNotionalFromRestRow(item);
                    if (position.mark_price <= 0.0 && position.entry_price <= 0.0 && abs_qty > 1e-12
                        && std::abs(signed_notional) > 1e-12) {
                        const double px = std::abs(signed_notional) / abs_qty;
                        position.entry_price = px;
                        position.mark_price = px;
                    }
                    position.liquidation_price = parseJsonDoubleMember(item, {
                        "liquidation_price", "liquidationPrice", "liq_price"});
                    position.unrealized_pnl = parseJsonDoubleMember(item, {
                        "unrealized_pnl", "unrealizedPnl", "upnl", "unrealizedProfit",
                        "uPnl", "open_pnl", "floating_pnl"});
                    position.realized_pnl = parseJsonDoubleMember(item, {
                        "realized_pnl", "realizedPnl", "rpnl", "realizedProfit",
                        "rPnl", "closed_pnl", "session_pnl"});
                    position.margin = parseJsonDoubleMember(item, {"margin", "initial_margin", "position_margin"});
                    position.leverage = parseJsonDoubleMember(item, {"leverage", "lev"});
                    if (position.leverage <= 0.0) {
                        position.leverage = 1.0;
                    }
                    position.updated_at = std::chrono::high_resolution_clock::now().time_since_epoch();
                    updatePosition(position);
                }
            }
        } catch (...) {}
    }
    
    notifyPortfolioChange();
}

std::optional<AccountBalance> PortfolioManager::getBalance(const std::string& currency) const {
    std::shared_lock<std::shared_mutex> lock(balances_mutex_);
    auto it = balances_.find(currency);
    if (it == balances_.end()) {
        return std::nullopt;
    }
    return it->second;
}

std::vector<AccountBalance> PortfolioManager::getAllBalances() const {
    std::shared_lock<std::shared_mutex> lock(balances_mutex_);
    std::vector<AccountBalance> result;
    result.reserve(balances_.size());
    for (const auto& [currency, balance] : balances_) {
        result.push_back(balance);
    }
    return result;
}

Decimal PortfolioManager::getTotalEquity(const std::string& quote_currency) const {
    std::shared_lock<std::shared_mutex> lock(balances_mutex_);
    
    // For now, just sum all balances
    // In production, would need to convert to quote currency
    Decimal total = 0.0;
    for (const auto& [currency, balance] : balances_) {
        if (currency == quote_currency) {
            total += balance.total;
        }
        // TODO: Add currency conversion for non-quote currencies
    }
    
    return total;
}

Decimal PortfolioManager::getAvailableBalance(const std::string& currency) const {
    auto balance = getBalance(currency);
    return balance.has_value() ? balance->available : 0.0;
}

void PortfolioManager::updateBalance(const AccountBalance& balance) {
    {
        std::unique_lock<std::shared_mutex> lock(balances_mutex_);
        balances_[balance.currency] = balance;
    }
    
    notifyBalanceChange(balance);
    
    // Publish event
    events::BalanceEventData event_data;
    event_data.currency = balance.currency;
    event_data.available = balance.available;
    event_data.locked = balance.locked;
    event_data.total = balance.total;
    
    auto event = std::make_shared<events::Event>(
        events::EventType::BALANCE_UPDATE,
        event_data,
        events::EventPriority::NORMAL
    );
    events::EventManager::getInstance().publish(event);
}

std::optional<Position> PortfolioManager::getPosition(const std::string& symbol) const {
    const std::string key = normalizePositionMapKey(symbol);
    std::shared_lock<std::shared_mutex> lock(positions_mutex_);
    auto it = positions_.find(key);
    if (it == positions_.end()) {
        return std::nullopt;
    }
    return it->second;
}

std::vector<Position> PortfolioManager::getAllPositions() const {
    std::shared_lock<std::shared_mutex> lock(positions_mutex_);
    std::vector<Position> result;
    result.reserve(positions_.size());
    for (const auto& [symbol, position] : positions_) {
        if (position.isOpen()) {
            result.push_back(position);
        }
    }
    return result;
}

std::vector<Position> PortfolioManager::getPositionsBySide(Side side) const {
    std::shared_lock<std::shared_mutex> lock(positions_mutex_);
    std::vector<Position> result;
    for (const auto& [symbol, position] : positions_) {
        if (position.isOpen() && position.side == side) {
            result.push_back(position);
        }
    }
    return result;
}

bool PortfolioManager::hasPosition(const std::string& symbol) const {
    const std::string key = normalizePositionMapKey(symbol);
    std::shared_lock<std::shared_mutex> lock(positions_mutex_);
    auto it = positions_.find(key);
    return it != positions_.end() && it->second.isOpen();
}

std::size_t PortfolioManager::getPositionCount() const {
    std::shared_lock<std::shared_mutex> lock(positions_mutex_);
    std::size_t count = 0;
    for (const auto& [symbol, position] : positions_) {
        if (position.isOpen()) {
            ++count;
        }
    }
    return count;
}

void PortfolioManager::updatePosition(const Position& position) {
    const std::string map_key = normalizePositionMapKey(
        std::string_view(position.symbol.data()));
    
    {
        std::unique_lock<std::shared_mutex> lock(positions_mutex_);
        positions_[map_key] = position;
    }
    
    notifyPositionChange(position);
    
    // Publish event
    events::PositionEventData event_data;
    event_data.symbol = position.symbol;
    event_data.side = position.side;
    event_data.quantity = position.quantity;
    event_data.entry_price = position.entry_price;
    event_data.current_price = position.mark_price;
    event_data.unrealized_pnl = position.unrealized_pnl;
    event_data.realized_pnl = position.realized_pnl;
    
    auto event = std::make_shared<events::Event>(
        events::EventType::POSITION_UPDATED,
        event_data,
        events::EventPriority::NORMAL
    );
    events::EventManager::getInstance().publish(event);
}

void PortfolioManager::closePosition(const std::string& symbol) {
    const std::string key = normalizePositionMapKey(symbol);
    std::unique_lock<std::shared_mutex> lock(positions_mutex_);
    auto it = positions_.find(key);
    if (it != positions_.end()) {
        it->second.quantity = 0.0;
        it->second.updated_at = std::chrono::high_resolution_clock::now().time_since_epoch();
        
        // Move unrealized to realized
        {
            std::lock_guard<std::mutex> pnl_lock(pnl_mutex_);
            realized_pnl_today_ += it->second.unrealized_pnl;
        }
        it->second.unrealized_pnl = 0.0;
    }
}

PnLResult PortfolioManager::calculatePnL(const std::string& symbol) const {
    PnLResult result;
    result.calculated_at = std::chrono::high_resolution_clock::now().time_since_epoch();
    
    auto position = getPosition(symbol);
    if (!position.has_value() || !position->isOpen()) {
        return result;
    }
    
    // Get current price
    auto& md = marketdata::MarketDataManager::getInstance();
    auto mid_price = md.getMidPrice(symbol);
    
    Price current_price = mid_price.has_value() ? mid_price.value() : position->mark_price;
    
    // Calculate P&L
    Decimal price_diff = current_price - position->entry_price;
    if (position->isShort()) {
        price_diff = -price_diff;
    }
    
    result.unrealized_pnl = price_diff * position->quantity;
    result.realized_pnl = position->realized_pnl;
    result.total_pnl = result.unrealized_pnl + result.realized_pnl;
    
    // ROI
    Decimal cost = position->entry_price * position->quantity;
    if (cost > 0) {
        result.roi_percentage = (result.total_pnl / cost) * 100.0;
    }
    
    return result;
}

PnLResult PortfolioManager::calculateTotalPnL() const {
    PnLResult result;
    result.calculated_at = std::chrono::high_resolution_clock::now().time_since_epoch();
    
    auto positions = getAllPositions();
    
    Decimal total_cost = 0.0;
    for (const auto& position : positions) {
        std::string symbol_str(position.symbol.data());
        auto pnl = calculatePnL(symbol_str);
        result.unrealized_pnl += pnl.unrealized_pnl;
        result.realized_pnl += pnl.realized_pnl;
        total_cost += position.entry_price * position.quantity;
    }
    
    result.total_pnl = result.unrealized_pnl + result.realized_pnl;
    
    if (total_cost > 0) {
        result.roi_percentage = (result.total_pnl / total_cost) * 100.0;
    }
    
    return result;
}

Decimal PortfolioManager::getUnrealizedPnL() const {
    return calculateTotalPnL().unrealized_pnl;
}

Decimal PortfolioManager::getRealizedPnL() const {
    std::lock_guard<std::mutex> lock(pnl_mutex_);
    return realized_pnl_today_;
}

void PortfolioManager::updatePnLFromFill(const orders::Fill& fill) {
    const std::string map_key = normalizePositionMapKey(
        std::string_view(fill.symbol.data()));
    
    std::unique_lock<std::shared_mutex> lock(positions_mutex_);
    auto it = positions_.find(map_key);
    
    if (it == positions_.end()) {
        // New position
        Position position;
        position.symbol = fill.symbol;
        position.side = fill.side;
        position.quantity = fill.quantity;
        position.entry_price = fill.price;
        position.mark_price = fill.price;
        position.realized_pnl = 0;
        position.opened_at = position.updated_at = fill.executed_at;
        positions_[map_key] = position;
        lock.unlock();
        notifyPositionChange(position);
        return;
    }
    // Update existing position based on fill
    {
        auto& position = it->second;
        
        if (fill.side == position.side) {
            // Adding to position
            Decimal old_value = position.entry_price * position.quantity;
            Decimal new_value = fill.price * fill.quantity;
            position.quantity += fill.quantity;
            position.entry_price = (old_value + new_value) / position.quantity;
        } else {
            // Reducing position
            if (fill.quantity >= position.quantity) {
                // Position closed
                Decimal pnl = (fill.price - position.entry_price) * position.quantity;
                if (position.isShort()) pnl = -pnl;
                
                {
                    std::lock_guard<std::mutex> pnl_lock(pnl_mutex_);
                    realized_pnl_today_ += pnl;
                }
                
                position.realized_pnl += pnl;
                position.quantity = 0;
            } else {
                // Partial close
                Decimal pnl = (fill.price - position.entry_price) * fill.quantity;
                if (position.isShort()) pnl = -pnl;
                
                {
                    std::lock_guard<std::mutex> pnl_lock(pnl_mutex_);
                    realized_pnl_today_ += pnl;
                }
                
                position.realized_pnl += pnl;
                position.quantity -= fill.quantity;
            }
        }
        
        position.updated_at = std::chrono::high_resolution_clock::now().time_since_epoch();
        Position p = position;
        lock.unlock();
        notifyPositionChange(p);
    }
}

PortfolioSummary PortfolioManager::getSummary() const {
    PortfolioSummary summary;
    
    // Calculate equity from API balances; when empty (e.g. paper), use PnL so UI shows non-zero
    summary.total_equity = getTotalEquity();
    summary.realized_pnl_today = getRealizedPnL();
    Decimal unrealized = getUnrealizedPnL();
    if (summary.total_equity == 0 && (summary.realized_pnl_today != 0 || unrealized != 0)) {
        summary.total_equity = summary.realized_pnl_today + unrealized;
    }
    
    // Sum available balance
    auto balances = getAllBalances();
    for (const auto& balance : balances) {
        summary.available_balance += balance.available;
    }
    
    // Calculate position values
    auto positions = getAllPositions();
    summary.open_positions = positions.size();
    
    for (const auto& position : positions) {
        summary.total_position_value += position.getNotionalValue();
        summary.margin_used += position.margin;
        summary.unrealized_pnl += position.unrealized_pnl;
    }
    if (summary.unrealized_pnl == 0 && !positions.empty()) {
        summary.unrealized_pnl = unrealized;
    }
    
    // Margin ratio
    if (summary.total_equity > 0) {
        summary.margin_ratio = summary.margin_used / summary.total_equity;
    }
    
    // TODO: Get active order count from OrderManager
    summary.active_orders = 0;
    
    return summary;
}

double PortfolioManager::getMarginRatio() const {
    return getSummary().margin_ratio;
}

bool PortfolioManager::isMarginCallRisk() const {
    return getMarginRatio() >= margin_call_threshold_;
}

Quantity PortfolioManager::calculatePositionSize(double risk_percentage,
                                                  Price entry_price,
                                                  Price stop_loss) const {
    Decimal equity = getTotalEquity();
    Decimal risk_amount = equity * (risk_percentage / 100.0);
    
    Price price_diff = std::abs(entry_price - stop_loss);
    if (price_diff <= 0) {
        return 0.0;
    }
    
    return risk_amount / price_diff;
}

bool PortfolioManager::checkRiskLimits(const orders::OrderRequest& request) const {
    std::string symbol_str(request.symbol.data());
    
    // Check position size limit
    auto it = max_position_sizes_.find(symbol_str);
    if (it != max_position_sizes_.end()) {
        auto current_position = getPosition(symbol_str);
        Quantity current_qty = current_position.has_value() ? current_position->quantity : 0.0;
        
        if (current_qty + request.quantity > it->second) {
            return false;
        }
    }
    
    // Check total exposure limit
    if (max_total_exposure_ > 0) {
        Decimal order_value = request.price * request.quantity;
        auto summary = getSummary();
        
        if (summary.total_position_value + order_value > max_total_exposure_) {
            return false;
        }
    }
    
    // Check available margin
    auto summary = getSummary();
    Decimal order_margin = (request.price * request.quantity) / 10.0; // Assuming 10x leverage
    
    if (order_margin > summary.available_balance) {
        return false;
    }
    
    return true;
}

void PortfolioManager::setBalanceCallback(BalanceCallback callback) {
    std::unique_lock<std::shared_mutex> lock(callbacks_mutex_);
    balance_callback_ = std::move(callback);
}

void PortfolioManager::setPositionCallback(PositionCallback callback) {
    std::unique_lock<std::shared_mutex> lock(callbacks_mutex_);
    position_callback_ = std::move(callback);
}

void PortfolioManager::setPortfolioCallback(PortfolioCallback callback) {
    std::unique_lock<std::shared_mutex> lock(callbacks_mutex_);
    portfolio_callback_ = std::move(callback);
}

void PortfolioManager::setMaxPositionSize(const std::string& symbol, Quantity max_size) {
    max_position_sizes_[symbol] = max_size;
}

void PortfolioManager::setMaxTotalExposure(Decimal max_exposure) {
    max_total_exposure_ = max_exposure;
}

void PortfolioManager::setMarginCallThreshold(double threshold) {
    margin_call_threshold_ = threshold;
}

void PortfolioManager::notifyBalanceChange(const AccountBalance& balance) {
    std::shared_lock<std::shared_mutex> lock(callbacks_mutex_);
    if (balance_callback_) {
        balance_callback_(balance);
    }
}

void PortfolioManager::notifyPositionChange(const Position& position) {
    std::shared_lock<std::shared_mutex> lock(callbacks_mutex_);
    if (position_callback_) {
        position_callback_(position);
    }
}

void PortfolioManager::notifyPortfolioChange() {
    std::shared_lock<std::shared_mutex> lock(callbacks_mutex_);
    if (portfolio_callback_) {
        portfolio_callback_(getSummary());
    }
}

} // namespace portfolio
} // namespace architect
