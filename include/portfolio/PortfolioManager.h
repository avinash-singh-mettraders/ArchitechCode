#pragma once

/**
 * @file PortfolioManager.h
 * @brief Portfolio, position, and balance management
 */

#include "core/Types.h"
#include "orders/Order.h"
#include <string>
#include <vector>
#include <map>
#include <mutex>
#include <shared_mutex>
#include <functional>
#include <memory>

namespace architect {
namespace portfolio {

using namespace core;

/**
 * @brief Account balance for a currency
 */
struct AccountBalance {
    std::string     currency;
    Decimal         available       = 0.0;
    Decimal         locked          = 0.0;
    Decimal         total           = 0.0;
    Decimal         unrealized_pnl  = 0.0;
    Timestamp       updated_at;
    
    [[nodiscard]] Decimal getTotal() const {
        return available + locked;
    }
};

/**
 * @brief Trading position for a symbol
 */
struct Position {
    Symbol          symbol;
    Side            side            = Side::BUY;  // LONG or SHORT
    Quantity        quantity        = 0.0;
    Price           entry_price     = 0.0;
    Price           mark_price      = 0.0;
    Price           liquidation_price = 0.0;
    Decimal         unrealized_pnl  = 0.0;
    Decimal         realized_pnl    = 0.0;
    Decimal         margin          = 0.0;
    Decimal         leverage        = 1.0;
    Timestamp       opened_at;
    Timestamp       updated_at;
    
    [[nodiscard]] bool isLong() const { return side == Side::BUY; }
    [[nodiscard]] bool isShort() const { return side == Side::SELL; }
    [[nodiscard]] bool isOpen() const { return quantity > 0; }
    
    [[nodiscard]] Decimal getNotionalValue() const {
        return mark_price * quantity;
    }
    
    [[nodiscard]] double getROE() const {
        if (margin <= 0) return 0.0;
        return (unrealized_pnl / margin) * 100.0;
    }
};

/**
 * @brief Portfolio summary
 */
struct PortfolioSummary {
    Decimal         total_equity        = 0.0;
    Decimal         available_balance   = 0.0;
    Decimal         margin_used         = 0.0;
    Decimal         unrealized_pnl      = 0.0;
    Decimal         realized_pnl_today  = 0.0;
    Decimal         total_position_value = 0.0;
    std::size_t     open_positions      = 0;
    std::size_t     active_orders       = 0;
    double          margin_ratio        = 0.0;  // margin_used / total_equity
};

/**
 * @brief P&L calculation result
 */
struct PnLResult {
    Decimal         unrealized_pnl  = 0.0;
    Decimal         realized_pnl    = 0.0;
    Decimal         total_pnl       = 0.0;
    double          roi_percentage  = 0.0;
    Timestamp       calculated_at;
};

using BalanceCallback = std::function<void(const AccountBalance&)>;
using PositionCallback = std::function<void(const Position&)>;
using PortfolioCallback = std::function<void(const PortfolioSummary&)>;

/**
 * @brief Portfolio manager
 * 
 * Manages:
 * - Account balances
 * - Trading positions
 * - P&L calculations
 * - Risk metrics
 */
class PortfolioManager {
public:
    /**
     * @brief Get the singleton instance
     */
    static PortfolioManager& getInstance();
    
    // Prevent copying
    PortfolioManager(const PortfolioManager&) = delete;
    PortfolioManager& operator=(const PortfolioManager&) = delete;
    
    // ==========================================================================
    // Lifecycle
    // ==========================================================================
    
    /**
     * @brief Initialize portfolio from API
     */
    void initialize();
    
    /**
     * @brief Refresh portfolio data from API
     */
    void refresh();
    
    // ==========================================================================
    // Balance Management
    // ==========================================================================
    
    /**
     * @brief Get balance for a currency
     */
    std::optional<AccountBalance> getBalance(const std::string& currency) const;
    
    /**
     * @brief Get all balances
     */
    std::vector<AccountBalance> getAllBalances() const;
    
    /**
     * @brief Get total equity in quote currency
     */
    Decimal getTotalEquity(const std::string& quote_currency = "USD") const;
    
    /**
     * @brief Get available balance for trading
     */
    Decimal getAvailableBalance(const std::string& currency) const;
    
    /**
     * @brief Update balance (internal use)
     */
    void updateBalance(const AccountBalance& balance);
    
    // ==========================================================================
    // Position Management
    // ==========================================================================
    
    /**
     * @brief Get position for a symbol
     */
    std::optional<Position> getPosition(const std::string& symbol) const;
    
    /**
     * @brief Get all open positions
     */
    std::vector<Position> getAllPositions() const;
    
    /**
     * @brief Get positions for a specific side
     */
    std::vector<Position> getPositionsBySide(Side side) const;
    
    /**
     * @brief Check if has position in symbol
     */
    bool hasPosition(const std::string& symbol) const;
    
    /**
     * @brief Get total position count
     */
    std::size_t getPositionCount() const;
    
    /**
     * @brief Update position (internal use)
     */
    void updatePosition(const Position& position);
    
    /**
     * @brief Close position tracking (internal use)
     */
    void closePosition(const std::string& symbol);
    
    // ==========================================================================
    // P&L Calculations
    // ==========================================================================
    
    /**
     * @brief Calculate P&L for a position
     */
    PnLResult calculatePnL(const std::string& symbol) const;
    
    /**
     * @brief Calculate total portfolio P&L
     */
    PnLResult calculateTotalPnL() const;
    
    /**
     * @brief Get unrealized P&L
     */
    Decimal getUnrealizedPnL() const;
    
    /**
     * @brief Get realized P&L (for the day)
     */
    Decimal getRealizedPnL() const;
    
    /**
     * @brief Update P&L from fill
     */
    void updatePnLFromFill(const orders::Fill& fill);
    
    // ==========================================================================
    // Portfolio Summary
    // ==========================================================================
    
    /**
     * @brief Get portfolio summary
     */
    PortfolioSummary getSummary() const;
    
    /**
     * @brief Get margin ratio
     */
    double getMarginRatio() const;
    
    /**
     * @brief Check if margin call imminent
     */
    bool isMarginCallRisk() const;
    
    // ==========================================================================
    // Risk Management
    // ==========================================================================
    
    /**
     * @brief Calculate position size for a trade
     * @param risk_percentage Max risk as percentage of equity
     * @param entry_price Expected entry price
     * @param stop_loss Stop loss price
     */
    Quantity calculatePositionSize(double risk_percentage, 
                                   Price entry_price, 
                                   Price stop_loss) const;
    
    /**
     * @brief Check if order would exceed risk limits
     */
    bool checkRiskLimits(const orders::OrderRequest& request) const;
    
    // ==========================================================================
    // Callbacks
    // ==========================================================================
    
    void setBalanceCallback(BalanceCallback callback);
    void setPositionCallback(PositionCallback callback);
    void setPortfolioCallback(PortfolioCallback callback);
    
    // ==========================================================================
    // Configuration
    // ==========================================================================
    
    /**
     * @brief Set maximum position size per symbol
     */
    void setMaxPositionSize(const std::string& symbol, Quantity max_size);
    
    /**
     * @brief Set maximum total exposure
     */
    void setMaxTotalExposure(Decimal max_exposure);
    
    /**
     * @brief Set margin call threshold
     */
    void setMarginCallThreshold(double threshold);
    
private:
    PortfolioManager();
    ~PortfolioManager() = default;
    
    void recalculateUnrealizedPnL();
    void notifyBalanceChange(const AccountBalance& balance);
    void notifyPositionChange(const Position& position);
    void notifyPortfolioChange();
    
    // Balances
    std::map<std::string, AccountBalance> balances_;
    mutable std::shared_mutex balances_mutex_;
    
    // Positions
    std::map<std::string, Position> positions_;
    mutable std::shared_mutex positions_mutex_;
    
    // P&L tracking
    Decimal realized_pnl_today_ = 0.0;
    mutable std::mutex pnl_mutex_;
    
    // Risk configuration
    std::map<std::string, Quantity> max_position_sizes_;
    Decimal max_total_exposure_ = 0.0;
    double margin_call_threshold_ = 0.8;  // 80%
    
    // Callbacks
    BalanceCallback balance_callback_;
    PositionCallback position_callback_;
    PortfolioCallback portfolio_callback_;
    mutable std::shared_mutex callbacks_mutex_;
};

} // namespace portfolio
} // namespace architect
