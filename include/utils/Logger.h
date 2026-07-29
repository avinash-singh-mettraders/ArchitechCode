#pragma once

/**
 * @file Logger.h
 * @brief Sophisticated logging system with CSV support and dated directories
 * 
 * Log file naming convention: {date}.{filename}.csv
 * Directory structure: logs/{date}/
 * 
 * Example: logs/20260130/20260130.orders.csv
 */

#include "core/Types.h"
#include <string>
#include <memory>
#include <mutex>
#include <fstream>
#include <sstream>
#include <filesystem>
#include <chrono>
#include <map>
#include <vector>
#include <functional>
#include <stdexcept>
#include <spdlog/spdlog.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/sinks/rotating_file_sink.h>
#include <spdlog/fmt/fmt.h>

namespace architect {
namespace utils {

namespace fs = std::filesystem;

/**
 * @brief Exception for logger initialization failures
 */
class LoggerInitException : public std::runtime_error {
public:
    explicit LoggerInitException(const std::string& msg) 
        : std::runtime_error("Logger initialization failed: " + msg) {}
};

/**
 * @brief Log levels
 */
enum class LogLevel {
    TRACE,
    DEBUG,
    INFO,
    WARN,
    ERROR,
    CRITICAL,
    OFF
};

/**
 * @brief CSV log types - comprehensive list based on trading system needs
 */
enum class CSVLogType {
    // Order Management
    ORDERS,
    OPEN_ORDERS,
    CLOSE_ORDERS,
    MASTER_ORDERS,
    
    // Executions & Fills
    FILLS,
    EXECUTION,
    
    // Market Data
    TICKS,
    QUOTES,
    TRADES,
    
    // Positions & Portfolio
    POSITIONS,
    CRASH_POS_STATE,
    OVERNIGHT_POS,
    POS_STATE,
    
    // P&L
    PNL,
    LINDPNL,        // Linear P&L
    REALIZED_PNL,
    UNREALIZED_PNL,
    
    // Balances & Margin
    BALANCES,
    MARGIN,
    
    // Strategy & Signals
    STRATEGY,
    SIGNALS,
    PREDICTORS,
    
    // Risk & Analytics
    GREEKS,
    RISK_METRICS,
    
    // System Events
    EVENTS,
    METRICS,
    
    // Connectivity
    GATEWAY,
    EXCHANGE,
    
    // Misc
    CAUSAL_RELATION,
    UNIVERSE,
    INSTRUMENTS,
    
    // Custom
    CUSTOM
};

/**
 * @brief Get string representation of CSV log type (filename part)
 */
inline const char* csvLogTypeToFilename(CSVLogType type) {
    switch (type) {
        case CSVLogType::ORDERS:            return "orders";
        case CSVLogType::OPEN_ORDERS:       return "open_orders";
        case CSVLogType::CLOSE_ORDERS:      return "close_orders";
        case CSVLogType::MASTER_ORDERS:     return "master_orders";
        case CSVLogType::FILLS:             return "fills";
        case CSVLogType::EXECUTION:         return "execution";
        case CSVLogType::TICKS:             return "ticks";
        case CSVLogType::QUOTES:            return "quotes";
        case CSVLogType::TRADES:            return "trades";
        case CSVLogType::POSITIONS:         return "positions";
        case CSVLogType::CRASH_POS_STATE:   return "crash_pos_state";
        case CSVLogType::OVERNIGHT_POS:     return "overnight_pos";
        case CSVLogType::POS_STATE:         return "pos_state";
        case CSVLogType::PNL:               return "pnl";
        case CSVLogType::LINDPNL:           return "lindpnl";
        case CSVLogType::REALIZED_PNL:      return "realized_pnl";
        case CSVLogType::UNREALIZED_PNL:    return "unrealized_pnl";
        case CSVLogType::BALANCES:          return "balances";
        case CSVLogType::MARGIN:            return "margin";
        case CSVLogType::STRATEGY:          return "strategy";
        case CSVLogType::SIGNALS:           return "signals";
        case CSVLogType::PREDICTORS:        return "predictors";
        case CSVLogType::GREEKS:            return "greeks";
        case CSVLogType::RISK_METRICS:      return "risk_metrics";
        case CSVLogType::EVENTS:            return "events";
        case CSVLogType::METRICS:           return "metrics";
        case CSVLogType::GATEWAY:           return "gateway";
        case CSVLogType::EXCHANGE:          return "exchange";
        case CSVLogType::CAUSAL_RELATION:   return "causal_relation";
        case CSVLogType::UNIVERSE:          return "universe";
        case CSVLogType::INSTRUMENTS:       return "instruments";
        case CSVLogType::CUSTOM:            return "custom";
        default:                            return "unknown";
    }
}

/**
 * @brief CSV row builder for type-safe logging
 */
class CSVRow {
public:
    CSVRow() = default;
    
    template<typename T>
    CSVRow& add(const T& value) {
        if (!first_) ss_ << ",";
        first_ = false;
        ss_ << value;
        return *this;
    }
    
    // Specialization for strings (with escaping)
    CSVRow& add(const std::string& value) {
        if (!first_) ss_ << ",";
        first_ = false;
        
        // Escape if contains comma, quote, or newline
        if (value.find_first_of(",\"\n") != std::string::npos) {
            ss_ << "\"";
            for (char c : value) {
                if (c == '"') ss_ << "\"\"";
                else ss_ << c;
            }
            ss_ << "\"";
        } else {
            ss_ << value;
        }
        return *this;
    }
    
    CSVRow& add(const char* value) {
        return add(std::string(value));
    }
    
    std::string str() const { return ss_.str(); }
    void clear() { ss_.str(""); ss_.clear(); first_ = true; }
    
private:
    std::stringstream ss_;
    bool first_ = true;
};

/**
 * @brief Sophisticated logger with CSV support
 * 
 * Creates dated directories with files named: {date}.{filename}.csv
 */
class Logger {
public:
    /**
     * @brief Logger configuration
     */
    struct Config {
        std::string base_log_dir        = "logs";
        std::string simulation_date;                     // YYYYMMDD format (e.g., "20260130")
        std::string run_id;                              // Optional run identifier
        LogLevel console_level          = LogLevel::INFO;
        LogLevel file_level             = LogLevel::DEBUG;
        bool console_enabled            = true;
        bool file_enabled               = true;
        bool csv_enabled                = true;
        std::size_t max_file_size       = 10 * 1024 * 1024;  // 10MB
        std::size_t max_files           = 5;
        std::string log_pattern         = "[%Y-%m-%d %H:%M:%S.%e] [%^%l%$] [%t] %v";
        bool crash_on_init_failure      = true;
    };
    
    /**
     * @brief Initialize the logger - throws LoggerInitException on failure
     */
    static void initialize(const Config& config);
    
    /**
     * @brief Get the singleton instance
     */
    static Logger& getInstance();
    
    /**
     * @brief Check if logger is initialized
     */
    static bool isInitialized() { return instance_ != nullptr && instance_->initialized_; }
    
    // Prevent copying
    Logger(const Logger&) = delete;
    Logger& operator=(const Logger&) = delete;
    
    // ==========================================================================
    // Console/File Logging
    // ==========================================================================
    
    void trace(const std::string& message);
    void debug(const std::string& message);
    void info(const std::string& message);
    void warn(const std::string& message);
    void error(const std::string& message);
    void critical(const std::string& message);
    
    template<typename... Args>
    void trace(fmt::format_string<Args...> fmt, Args&&... args) {
        if (spdlog_logger_) spdlog_logger_->trace(fmt, std::forward<Args>(args)...);
    }
    
    template<typename... Args>
    void debug(fmt::format_string<Args...> fmt, Args&&... args) {
        if (spdlog_logger_) spdlog_logger_->debug(fmt, std::forward<Args>(args)...);
    }
    
    template<typename... Args>
    void info(fmt::format_string<Args...> fmt, Args&&... args) {
        if (spdlog_logger_) spdlog_logger_->info(fmt, std::forward<Args>(args)...);
    }
    
    template<typename... Args>
    void warn(fmt::format_string<Args...> fmt, Args&&... args) {
        if (spdlog_logger_) spdlog_logger_->warn(fmt, std::forward<Args>(args)...);
    }
    
    template<typename... Args>
    void error(fmt::format_string<Args...> fmt, Args&&... args) {
        if (spdlog_logger_) spdlog_logger_->error(fmt, std::forward<Args>(args)...);
    }
    
    template<typename... Args>
    void critical(fmt::format_string<Args...> fmt, Args&&... args) {
        if (spdlog_logger_) spdlog_logger_->critical(fmt, std::forward<Args>(args)...);
    }
    
    // ==========================================================================
    // CSV Logging - Files named: {date}.{filename}.csv
    // ==========================================================================
    
    /**
     * @brief Log to a specific CSV file type
     * Creates file: {date}.{type}.csv
     */
    void log_to_csv(CSVLogType type, const CSVRow& row);
    
    /**
     * @brief Log to a custom CSV file
     * Creates file: {date}.{filename}.csv
     */
    void log_to_csv(const std::string& filename, const CSVRow& row);
    
    /**
     * @brief Set CSV header for a log type (call once before logging)
     */
    void set_csv_header(CSVLogType type, const std::vector<std::string>& headers);
    void set_csv_header(const std::string& filename, const std::vector<std::string>& headers);
    
    // ==========================================================================
    // Convenience CSV Logging Methods
    // ==========================================================================
    
    // Order Management
    void log_order(core::OrderId order_id, const std::string& client_order_id,
                   const std::string& symbol, const std::string& side,
                   const std::string& type, const std::string& status,
                   core::Price price, core::Quantity quantity,
                   core::Quantity filled_qty, const std::string& message = "");
    
    void log_open_order(core::OrderId order_id, const std::string& symbol,
                        const std::string& side, core::Price price,
                        core::Quantity quantity, core::Quantity remaining);
    
    void log_execution(core::TradeId trade_id, core::OrderId order_id,
                       const std::string& symbol, const std::string& side,
                       core::Price price, core::Quantity quantity,
                       core::Decimal fee, const std::string& exec_type);
    
    void log_fill(core::TradeId trade_id, core::OrderId order_id,
                  const std::string& symbol, const std::string& side,
                  core::Price price, core::Quantity quantity,
                  core::Decimal fee, const std::string& fee_currency);
    
    // Market Data
    void log_tick(const std::string& symbol, core::Price bid, core::Price ask,
                  core::Price last, core::Quantity bid_size, core::Quantity ask_size);
    
    void log_quote(const std::string& symbol, core::Price bid, core::Price ask,
                   core::Quantity bid_size, core::Quantity ask_size,
                   const std::string& exchange);
    
    void log_trade(const std::string& symbol, core::Price price,
                   core::Quantity quantity, const std::string& side,
                   const std::string& exchange);
    
    // Positions
    void log_position(const std::string& symbol, const std::string& side,
                      core::Quantity quantity, core::Price entry_price,
                      core::Price mark_price, core::Decimal unrealized_pnl,
                      core::Decimal realized_pnl);
    
    void log_crash_pos_state(const std::string& symbol, core::Quantity quantity,
                             core::Price avg_price, core::Decimal pnl,
                             const std::string& state);
    
    void log_overnight_pos(const std::string& symbol, core::Quantity quantity,
                           core::Price price, const std::string& account);
    
    // P&L
    void log_pnl(const std::string& symbol, core::Decimal realized,
                 core::Decimal unrealized, core::Decimal total,
                 core::Decimal fees);
    
    void log_lindpnl(const std::string& symbol, core::Decimal lindpnl,
                     core::Decimal delta_pnl, core::Price ref_price);
    
    // Balances
    void log_balance(const std::string& currency, core::Decimal available,
                     core::Decimal locked, core::Decimal total);
    
    void log_margin(const std::string& account, core::Decimal margin_used,
                    core::Decimal margin_available, double margin_ratio);
    
    // Strategy & Signals
    void log_strategy(const std::string& strategy_name, const std::string& symbol,
                      const std::string& action, const std::string& reason,
                      double signal_value);
    
    void log_signal(const std::string& signal_name, const std::string& symbol,
                    double value, double threshold, const std::string& action);
    
    void log_predictor(const std::string& predictor_name, const std::string& symbol,
                       double prediction, double confidence, double actual = 0.0);
    
    // Risk
    void log_greeks(const std::string& symbol, double delta, double gamma,
                    double theta, double vega, double rho);
    
    void log_risk_metric(const std::string& metric_name, double value,
                         double limit, const std::string& status);
    
    // System
    void log_event(const std::string& event_type, const std::string& source,
                   const std::string& data);
    
    void log_metrics(const std::string& metric_name, double value,
                     const std::string& unit = "");
    
    void log_gateway(const std::string& gateway_name, const std::string& status,
                     const std::string& message, int latency_us = 0);
    
    void log_exchange(const std::string& exchange_name, const std::string& status,
                      const std::string& message);
    
    // Instruments
    void log_instrument(const std::string& symbol, const std::string& type,
                        const std::string& exchange, double tick_size,
                        double lot_size, const std::string& status);
    
    // ==========================================================================
    // File Operations
    // ==========================================================================
    
    /**
     * @brief Log arbitrary content to a file (named: {date}.{filename})
     */
    void log_to_file(const std::string& filename, const std::string& content,
                     bool append = true);
    
    /**
     * @brief Get the current log directory
     */
    std::string getLogDirectory() const { return log_directory_; }
    
    /**
     * @brief Get the simulation date (YYYYMMDD)
     */
    std::string getSimulationDate() const { return date_str_; }
    
    /**
     * @brief Get full path for a log file
     */
    std::string getLogFilePath(const std::string& filename) const;
    
    /**
     * @brief Flush all logs
     */
    void flush();
    
    /**
     * @brief Shutdown the logger
     */
    void shutdown();
    
    // ==========================================================================
    // Configuration
    // ==========================================================================
    
    void setLevel(LogLevel level);
    LogLevel getLevel() const;
    
public:
    // Destructor must be public for unique_ptr
    ~Logger();

private:
    Logger() = default;
    
    void validateAndCreateDirectories(const Config& config);
    void setupSpdlog(const Config& config);
    void setupCSVHeaders();
    std::string getCurrentTimestamp() const;
    std::string buildCSVFilename(const std::string& base_name) const;
    std::ofstream& getCSVStream(const std::string& base_name);
    
    static std::unique_ptr<Logger> instance_;
    static std::once_flag init_flag_;
    
    Config config_;
    std::string log_directory_;
    std::string date_str_;          // YYYYMMDD format
    
    std::shared_ptr<spdlog::logger> spdlog_logger_;
    
    // CSV file streams - keyed by base filename
    std::map<std::string, std::ofstream> csv_streams_;
    std::map<std::string, bool> csv_headers_written_;
    std::mutex csv_mutex_;
    
    bool initialized_ = false;
};

// =============================================================================
// Convenience Macros
// =============================================================================

#define LOG_TRACE(...)    architect::utils::Logger::getInstance().trace(__VA_ARGS__)
#define LOG_DEBUG(...)    architect::utils::Logger::getInstance().debug(__VA_ARGS__)
#define LOG_INFO(...)     architect::utils::Logger::getInstance().info(__VA_ARGS__)
#define LOG_WARN(...)     architect::utils::Logger::getInstance().warn(__VA_ARGS__)
#define LOG_ERROR(...)    architect::utils::Logger::getInstance().error(__VA_ARGS__)
#define LOG_CRITICAL(...) architect::utils::Logger::getInstance().critical(__VA_ARGS__)

#define LOG_CSV(type, row) architect::utils::Logger::getInstance().log_to_csv(type, row)

} // namespace utils
} // namespace architect
