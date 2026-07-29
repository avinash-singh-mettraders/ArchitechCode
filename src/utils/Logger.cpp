#include "utils/Logger.h"
#include "core/LatencyTracker.h"
#include <iomanip>
#include <ctime>
#include <iostream>

namespace architect {
namespace utils {

std::unique_ptr<Logger> Logger::instance_ = nullptr;
std::once_flag Logger::init_flag_;

void Logger::initialize(const Config& config) {
    std::call_once(init_flag_, [&config]() {
        instance_ = std::unique_ptr<Logger>(new Logger());
        instance_->config_ = config;
        
        try {
            instance_->validateAndCreateDirectories(config);
            instance_->setupSpdlog(config);
            instance_->setupCSVHeaders();
            instance_->initialized_ = true;
        } catch (const std::exception& e) {
            if (config.crash_on_init_failure) {
                throw LoggerInitException(e.what());
            }
            std::cerr << "Logger initialization failed: " << e.what() << std::endl;
        }
    });
}

Logger& Logger::getInstance() {
    if (!instance_) {
        throw LoggerInitException("Logger not initialized. Call Logger::initialize() first.");
    }
    return *instance_;
}

Logger::~Logger() {
    shutdown();
}

void Logger::validateAndCreateDirectories(const Config& config) {
    // Parse and validate simulation date (YYYYMMDD format)
    date_str_ = config.simulation_date;
    
    // If date not provided, use current date
    if (date_str_.empty()) {
        auto now = std::chrono::system_clock::now();
        auto time = std::chrono::system_clock::to_time_t(now);
        std::stringstream ss;
        ss << std::put_time(std::localtime(&time), "%Y%m%d");
        date_str_ = ss.str();
    }
    
    // Validate date format (basic check for 8 digits)
    if (date_str_.length() != 8) {
        throw std::runtime_error("Invalid date format. Expected YYYYMMDD, got: " + date_str_);
    }
    
    // Check if base logs directory exists
    fs::path base_path(config.base_log_dir);
    
    if (!fs::exists(base_path)) {
        // Try to create it
        std::error_code ec;
        if (!fs::create_directories(base_path, ec)) {
            throw std::runtime_error("Cannot create base logs directory: " + 
                                     config.base_log_dir + " - " + ec.message());
        }
    }
    
    // Check if base path is a directory and writable
    if (!fs::is_directory(base_path)) {
        throw std::runtime_error("Logs path is not a directory: " + config.base_log_dir);
    }
    
    // Create dated subdirectory: logs/YYYYMMDD/
    log_directory_ = config.base_log_dir + "/" + date_str_;
    fs::path dated_path(log_directory_);
    
    if (!fs::exists(dated_path)) {
        std::error_code ec;
        if (!fs::create_directories(dated_path, ec)) {
            throw std::runtime_error("Cannot create dated log directory: " + 
                                     log_directory_ + " - " + ec.message());
        }
    }
    
    // Verify we can write to the directory by creating a test file
    std::string test_file = log_directory_ + "/.write_test";
    std::ofstream test_stream(test_file);
    if (!test_stream.is_open()) {
        throw std::runtime_error("Cannot write to log directory: " + log_directory_);
    }
    test_stream.close();
    fs::remove(test_file);
}

void Logger::setupSpdlog(const Config& config) {
    std::vector<spdlog::sink_ptr> sinks;
    
    // Console sink
    if (config.console_enabled) {
        auto console_sink = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
        console_sink->set_pattern(config.log_pattern);
        
        switch (config.console_level) {
            case LogLevel::TRACE:    console_sink->set_level(spdlog::level::trace); break;
            case LogLevel::DEBUG:    console_sink->set_level(spdlog::level::debug); break;
            case LogLevel::INFO:     console_sink->set_level(spdlog::level::info); break;
            case LogLevel::WARN:     console_sink->set_level(spdlog::level::warn); break;
            case LogLevel::ERROR:    console_sink->set_level(spdlog::level::err); break;
            case LogLevel::CRITICAL: console_sink->set_level(spdlog::level::critical); break;
            case LogLevel::OFF:      console_sink->set_level(spdlog::level::off); break;
        }
        
        sinks.push_back(console_sink);
    }
    
    // File sink - named: {date}.platform.log
    if (config.file_enabled) {
        std::string log_file = log_directory_ + "/" + date_str_ + ".platform.log";
        auto file_sink = std::make_shared<spdlog::sinks::rotating_file_sink_mt>(
            log_file, config.max_file_size, config.max_files);
        file_sink->set_pattern(config.log_pattern);
        
        switch (config.file_level) {
            case LogLevel::TRACE:    file_sink->set_level(spdlog::level::trace); break;
            case LogLevel::DEBUG:    file_sink->set_level(spdlog::level::debug); break;
            case LogLevel::INFO:     file_sink->set_level(spdlog::level::info); break;
            case LogLevel::WARN:     file_sink->set_level(spdlog::level::warn); break;
            case LogLevel::ERROR:    file_sink->set_level(spdlog::level::err); break;
            case LogLevel::CRITICAL: file_sink->set_level(spdlog::level::critical); break;
            case LogLevel::OFF:      file_sink->set_level(spdlog::level::off); break;
        }
        
        sinks.push_back(file_sink);
    }
    
    spdlog_logger_ = std::make_shared<spdlog::logger>("platform", sinks.begin(), sinks.end());
    spdlog_logger_->set_level(spdlog::level::trace);
    spdlog::register_logger(spdlog_logger_);
    spdlog::set_default_logger(spdlog_logger_);
}

void Logger::setupCSVHeaders() {
    if (!config_.csv_enabled) return;
    
    // Orders
    set_csv_header(CSVLogType::ORDERS, {
        "timestamp", "order_id", "client_order_id", "symbol", "side", "type",
        "status", "price", "quantity", "filled_qty", "message"
    });
    
    set_csv_header(CSVLogType::OPEN_ORDERS, {
        "timestamp", "order_id", "symbol", "side", "price", "quantity", "remaining"
    });
    
    // Executions
    set_csv_header(CSVLogType::EXECUTION, {
        "timestamp", "trade_id", "order_id", "symbol", "side",
        "price", "quantity", "fee", "exec_type"
    });
    
    set_csv_header(CSVLogType::FILLS, {
        "timestamp", "trade_id", "order_id", "symbol", "side",
        "price", "quantity", "fee", "fee_currency"
    });
    
    // Market Data
    set_csv_header(CSVLogType::TICKS, {
        "timestamp", "symbol", "bid", "ask", "last", "bid_size", "ask_size"
    });
    
    set_csv_header(CSVLogType::QUOTES, {
        "timestamp", "symbol", "bid", "ask", "bid_size", "ask_size", "exchange"
    });
    
    set_csv_header(CSVLogType::TRADES, {
        "timestamp", "symbol", "price", "quantity", "side", "exchange"
    });
    
    // Positions
    set_csv_header(CSVLogType::POSITIONS, {
        "timestamp", "symbol", "side", "quantity", "entry_price",
        "mark_price", "unrealized_pnl", "realized_pnl"
    });
    
    set_csv_header(CSVLogType::CRASH_POS_STATE, {
        "timestamp", "symbol", "quantity", "avg_price", "pnl", "state"
    });
    
    set_csv_header(CSVLogType::OVERNIGHT_POS, {
        "timestamp", "symbol", "quantity", "price", "account"
    });
    
    set_csv_header(CSVLogType::POS_STATE, {
        "timestamp", "symbol", "quantity", "avg_price", "mark_price", "pnl"
    });
    
    // P&L
    set_csv_header(CSVLogType::PNL, {
        "timestamp", "symbol", "realized", "unrealized", "total", "fees"
    });
    
    set_csv_header(CSVLogType::LINDPNL, {
        "timestamp", "symbol", "lindpnl", "delta_pnl", "ref_price"
    });
    
    // Balances
    set_csv_header(CSVLogType::BALANCES, {
        "timestamp", "currency", "available", "locked", "total"
    });
    
    set_csv_header(CSVLogType::MARGIN, {
        "timestamp", "account", "margin_used", "margin_available", "margin_ratio"
    });
    
    // Strategy
    set_csv_header(CSVLogType::STRATEGY, {
        "timestamp", "strategy_name", "symbol", "action", "reason", "signal_value"
    });
    
    set_csv_header(CSVLogType::SIGNALS, {
        "timestamp", "signal_name", "symbol", "value", "threshold", "action"
    });
    
    set_csv_header(CSVLogType::PREDICTORS, {
        "timestamp", "predictor_name", "symbol", "prediction", "confidence", "actual"
    });
    
    // Risk
    set_csv_header(CSVLogType::GREEKS, {
        "timestamp", "symbol", "delta", "gamma", "theta", "vega", "rho"
    });
    
    set_csv_header(CSVLogType::RISK_METRICS, {
        "timestamp", "metric_name", "value", "limit", "status"
    });
    
    // System
    set_csv_header(CSVLogType::EVENTS, {
        "timestamp", "event_type", "source", "data"
    });
    
    set_csv_header(CSVLogType::METRICS, {
        "timestamp", "metric_name", "value", "unit"
    });
    
    set_csv_header(CSVLogType::GATEWAY, {
        "timestamp", "gateway_name", "status", "message", "latency_us"
    });
    
    set_csv_header(CSVLogType::EXCHANGE, {
        "timestamp", "exchange_name", "status", "message"
    });
    
    // Instruments
    set_csv_header(CSVLogType::INSTRUMENTS, {
        "timestamp", "symbol", "type", "exchange", "tick_size", "lot_size", "status"
    });
}

std::string Logger::getCurrentTimestamp() const {
    auto now = std::chrono::system_clock::now();
    auto time = std::chrono::system_clock::to_time_t(now);
    auto us = std::chrono::duration_cast<std::chrono::microseconds>(
        now.time_since_epoch()).count() % 1000000;
    
    std::stringstream ss;
    ss << std::put_time(std::localtime(&time), "%Y-%m-%d %H:%M:%S")
       << "." << std::setfill('0') << std::setw(6) << us;
    return ss.str();
}

std::string Logger::buildCSVFilename(const std::string& base_name) const {
    // Pattern: {date}.{name}.csv
    return date_str_ + "." + base_name + ".csv";
}

std::string Logger::getLogFilePath(const std::string& filename) const {
    return log_directory_ + "/" + buildCSVFilename(filename);
}

std::ofstream& Logger::getCSVStream(const std::string& base_name) {
    auto it = csv_streams_.find(base_name);
    if (it == csv_streams_.end()) {
        std::string filepath = log_directory_ + "/" + buildCSVFilename(base_name);
        // TRUNCATE mode: overwrite file on each new simulation run
        // This ensures clean CSV files with single header row
        csv_streams_[base_name].open(filepath, std::ios::out | std::ios::trunc);
        csv_headers_written_[base_name] = false;
        
        if (!csv_streams_[base_name].is_open()) {
            throw std::runtime_error("Failed to open CSV file: " + filepath);
        }
    }
    
    return csv_streams_[base_name];
}

// =============================================================================
// Console/File Logging
// =============================================================================

void Logger::trace(const std::string& message) {
    if (spdlog_logger_) spdlog_logger_->trace(message);
}

void Logger::debug(const std::string& message) {
    if (spdlog_logger_) spdlog_logger_->debug(message);
}

void Logger::info(const std::string& message) {
    if (spdlog_logger_) spdlog_logger_->info(message);
}

void Logger::warn(const std::string& message) {
    if (spdlog_logger_) spdlog_logger_->warn(message);
}

void Logger::error(const std::string& message) {
    if (spdlog_logger_) spdlog_logger_->error(message);
}

void Logger::critical(const std::string& message) {
    if (spdlog_logger_) spdlog_logger_->critical(message);
}

// =============================================================================
// CSV Logging
// =============================================================================

void Logger::log_to_csv(CSVLogType type, const CSVRow& row) {
    log_to_csv(csvLogTypeToFilename(type), row);
}

void Logger::log_to_csv(const std::string& filename, const CSVRow& row) {
    if (!config_.csv_enabled) return;
    
    std::lock_guard<std::mutex> lock(csv_mutex_);
    
    try {
        auto& stream = getCSVStream(filename);
        if (stream.is_open()) {
            stream << row.str() << "\n";
            stream.flush();
        }
    } catch (const std::exception& e) {
        if (spdlog_logger_) {
            spdlog_logger_->error("Failed to write to CSV {}: {}", filename, e.what());
        }
    }
}

void Logger::set_csv_header(CSVLogType type, const std::vector<std::string>& headers) {
    set_csv_header(csvLogTypeToFilename(type), headers);
}

void Logger::set_csv_header(const std::string& filename, const std::vector<std::string>& headers) {
    if (!config_.csv_enabled) return;
    
    std::lock_guard<std::mutex> lock(csv_mutex_);
    
    try {
        auto& stream = getCSVStream(filename);
        
        if (!csv_headers_written_[filename] && stream.is_open()) {
            CSVRow header_row;
            for (const auto& h : headers) {
                header_row.add(h);
            }
            stream << header_row.str() << "\n";
            stream.flush();
            csv_headers_written_[filename] = true;
        }
    } catch (const std::exception& e) {
        if (spdlog_logger_) {
            spdlog_logger_->error("Failed to write CSV header for {}: {}", filename, e.what());
        }
    }
}

// =============================================================================
// Convenience Logging Methods
// =============================================================================

void Logger::log_order(core::OrderId order_id, const std::string& client_order_id,
                       const std::string& symbol, const std::string& side,
                       const std::string& type, const std::string& status,
                       core::Price price, core::Quantity quantity,
                       core::Quantity filled_qty, const std::string& message) {
    CSVRow row;
    row.add(getCurrentTimestamp()).add(order_id).add(client_order_id)
       .add(symbol).add(side).add(type).add(status)
       .add(price).add(quantity).add(filled_qty).add(message);
    log_to_csv(CSVLogType::ORDERS, row);
}

void Logger::log_open_order(core::OrderId order_id, const std::string& symbol,
                            const std::string& side, core::Price price,
                            core::Quantity quantity, core::Quantity remaining) {
    CSVRow row;
    row.add(getCurrentTimestamp()).add(order_id).add(symbol)
       .add(side).add(price).add(quantity).add(remaining);
    log_to_csv(CSVLogType::OPEN_ORDERS, row);
}

void Logger::log_execution(core::TradeId trade_id, core::OrderId order_id,
                           const std::string& symbol, const std::string& side,
                           core::Price price, core::Quantity quantity,
                           core::Decimal fee, const std::string& exec_type) {
    CSVRow row;
    row.add(getCurrentTimestamp()).add(trade_id).add(order_id)
       .add(symbol).add(side).add(price).add(quantity).add(fee).add(exec_type);
    log_to_csv(CSVLogType::EXECUTION, row);
}

void Logger::log_fill(core::TradeId trade_id, core::OrderId order_id,
                      const std::string& symbol, const std::string& side,
                      core::Price price, core::Quantity quantity,
                      core::Decimal fee, const std::string& fee_currency) {
    CSVRow row;
    row.add(getCurrentTimestamp()).add(trade_id).add(order_id)
       .add(symbol).add(side).add(price).add(quantity).add(fee).add(fee_currency);
    log_to_csv(CSVLogType::FILLS, row);
}

void Logger::log_tick(const std::string& symbol, core::Price bid, core::Price ask,
                      core::Price last, core::Quantity bid_size, core::Quantity ask_size) {
    CSVRow row;
    row.add(getCurrentTimestamp()).add(symbol).add(bid).add(ask)
       .add(last).add(bid_size).add(ask_size);
    log_to_csv(CSVLogType::TICKS, row);
}

void Logger::log_quote(const std::string& symbol, core::Price bid, core::Price ask,
                       core::Quantity bid_size, core::Quantity ask_size,
                       const std::string& exchange) {
    CSVRow row;
    row.add(getCurrentTimestamp()).add(symbol).add(bid).add(ask)
       .add(bid_size).add(ask_size).add(exchange);
    log_to_csv(CSVLogType::QUOTES, row);
}

void Logger::log_trade(const std::string& symbol, core::Price price,
                       core::Quantity quantity, const std::string& side,
                       const std::string& exchange) {
    CSVRow row;
    row.add(getCurrentTimestamp()).add(symbol).add(price)
       .add(quantity).add(side).add(exchange);
    log_to_csv(CSVLogType::TRADES, row);
}

void Logger::log_position(const std::string& symbol, const std::string& side,
                          core::Quantity quantity, core::Price entry_price,
                          core::Price mark_price, core::Decimal unrealized_pnl,
                          core::Decimal realized_pnl) {
    CSVRow row;
    row.add(getCurrentTimestamp()).add(symbol).add(side).add(quantity)
       .add(entry_price).add(mark_price).add(unrealized_pnl).add(realized_pnl);
    log_to_csv(CSVLogType::POSITIONS, row);
}

void Logger::log_crash_pos_state(const std::string& symbol, core::Quantity quantity,
                                 core::Price avg_price, core::Decimal pnl,
                                 const std::string& state) {
    CSVRow row;
    row.add(getCurrentTimestamp()).add(symbol).add(quantity)
       .add(avg_price).add(pnl).add(state);
    log_to_csv(CSVLogType::CRASH_POS_STATE, row);
}

void Logger::log_overnight_pos(const std::string& symbol, core::Quantity quantity,
                               core::Price price, const std::string& account) {
    CSVRow row;
    row.add(getCurrentTimestamp()).add(symbol).add(quantity).add(price).add(account);
    log_to_csv(CSVLogType::OVERNIGHT_POS, row);
}

void Logger::log_pnl(const std::string& symbol, core::Decimal realized,
                     core::Decimal unrealized, core::Decimal total,
                     core::Decimal fees) {
    CSVRow row;
    row.add(getCurrentTimestamp()).add(symbol).add(realized)
       .add(unrealized).add(total).add(fees);
    log_to_csv(CSVLogType::PNL, row);
}

void Logger::log_lindpnl(const std::string& symbol, core::Decimal lindpnl,
                         core::Decimal delta_pnl, core::Price ref_price) {
    CSVRow row;
    row.add(getCurrentTimestamp()).add(symbol).add(lindpnl)
       .add(delta_pnl).add(ref_price);
    log_to_csv(CSVLogType::LINDPNL, row);
}

void Logger::log_balance(const std::string& currency, core::Decimal available,
                         core::Decimal locked, core::Decimal total) {
    CSVRow row;
    row.add(getCurrentTimestamp()).add(currency).add(available).add(locked).add(total);
    log_to_csv(CSVLogType::BALANCES, row);
}

void Logger::log_margin(const std::string& account, core::Decimal margin_used,
                        core::Decimal margin_available, double margin_ratio) {
    CSVRow row;
    row.add(getCurrentTimestamp()).add(account).add(margin_used)
       .add(margin_available).add(margin_ratio);
    log_to_csv(CSVLogType::MARGIN, row);
}

void Logger::log_strategy(const std::string& strategy_name, const std::string& symbol,
                          const std::string& action, const std::string& reason,
                          double signal_value) {
    CSVRow row;
    row.add(getCurrentTimestamp()).add(strategy_name).add(symbol)
       .add(action).add(reason).add(signal_value);
    log_to_csv(CSVLogType::STRATEGY, row);
}

void Logger::log_signal(const std::string& signal_name, const std::string& symbol,
                        double value, double threshold, const std::string& action) {
    CSVRow row;
    row.add(getCurrentTimestamp()).add(signal_name).add(symbol)
       .add(value).add(threshold).add(action);
    log_to_csv(CSVLogType::SIGNALS, row);
}

void Logger::log_predictor(const std::string& predictor_name, const std::string& symbol,
                           double prediction, double confidence, double actual) {
    CSVRow row;
    row.add(getCurrentTimestamp()).add(predictor_name).add(symbol)
       .add(prediction).add(confidence).add(actual);
    log_to_csv(CSVLogType::PREDICTORS, row);
}

void Logger::log_greeks(const std::string& symbol, double delta, double gamma,
                        double theta, double vega, double rho) {
    CSVRow row;
    row.add(getCurrentTimestamp()).add(symbol).add(delta).add(gamma)
       .add(theta).add(vega).add(rho);
    log_to_csv(CSVLogType::GREEKS, row);
}

void Logger::log_risk_metric(const std::string& metric_name, double value,
                             double limit, const std::string& status) {
    CSVRow row;
    row.add(getCurrentTimestamp()).add(metric_name).add(value).add(limit).add(status);
    log_to_csv(CSVLogType::RISK_METRICS, row);
}

void Logger::log_event(const std::string& event_type, const std::string& source,
                       const std::string& data) {
    CSVRow row;
    row.add(getCurrentTimestamp()).add(event_type).add(source).add(data);
    log_to_csv(CSVLogType::EVENTS, row);
}

void Logger::log_metrics(const std::string& metric_name, double value,
                         const std::string& unit) {
    CSVRow row;
    row.add(getCurrentTimestamp()).add(metric_name).add(value).add(unit);
    log_to_csv(CSVLogType::METRICS, row);
}

void Logger::log_gateway(const std::string& gateway_name, const std::string& status,
                         const std::string& message, int latency_us) {
    CSVRow row;
    row.add(getCurrentTimestamp()).add(gateway_name).add(status)
       .add(message).add(core::latencyDisplayUs(static_cast<int64_t>(latency_us)));
    log_to_csv(CSVLogType::GATEWAY, row);
}

void Logger::log_exchange(const std::string& exchange_name, const std::string& status,
                          const std::string& message) {
    CSVRow row;
    row.add(getCurrentTimestamp()).add(exchange_name).add(status).add(message);
    log_to_csv(CSVLogType::EXCHANGE, row);
}

void Logger::log_instrument(const std::string& symbol, const std::string& type,
                            const std::string& exchange, double tick_size,
                            double lot_size, const std::string& status) {
    CSVRow row;
    row.add(getCurrentTimestamp()).add(symbol).add(type).add(exchange)
       .add(tick_size).add(lot_size).add(status);
    log_to_csv(CSVLogType::INSTRUMENTS, row);
}

// =============================================================================
// File Operations
// =============================================================================

void Logger::log_to_file(const std::string& filename, const std::string& content,
                         bool append) {
    std::string filepath = log_directory_ + "/" + date_str_ + "." + filename;
    
    std::ofstream file;
    if (append) {
        file.open(filepath, std::ios::app);
    } else {
        file.open(filepath, std::ios::trunc);
    }
    
    if (file.is_open()) {
        file << content;
        file.close();
    } else if (spdlog_logger_) {
        spdlog_logger_->error("Failed to write to file: {}", filepath);
    }
}

void Logger::flush() {
    if (spdlog_logger_) {
        spdlog_logger_->flush();
    }
    
    std::lock_guard<std::mutex> lock(csv_mutex_);
    for (auto& [name, stream] : csv_streams_) {
        if (stream.is_open()) {
            stream.flush();
        }
    }
}

void Logger::shutdown() {
    flush();
    
    if (spdlog_logger_) {
        spdlog::shutdown();
        spdlog_logger_.reset();
    }
    
    std::lock_guard<std::mutex> lock(csv_mutex_);
    for (auto& [name, stream] : csv_streams_) {
        if (stream.is_open()) {
            stream.close();
        }
    }
    csv_streams_.clear();
    
    initialized_ = false;
}

void Logger::setLevel(LogLevel level) {
    if (!spdlog_logger_) return;
    
    spdlog::level::level_enum spdlog_level;
    switch (level) {
        case LogLevel::TRACE:    spdlog_level = spdlog::level::trace; break;
        case LogLevel::DEBUG:    spdlog_level = spdlog::level::debug; break;
        case LogLevel::INFO:     spdlog_level = spdlog::level::info; break;
        case LogLevel::WARN:     spdlog_level = spdlog::level::warn; break;
        case LogLevel::ERROR:    spdlog_level = spdlog::level::err; break;
        case LogLevel::CRITICAL: spdlog_level = spdlog::level::critical; break;
        case LogLevel::OFF:      spdlog_level = spdlog::level::off; break;
    }
    
    spdlog_logger_->set_level(spdlog_level);
}

LogLevel Logger::getLevel() const {
    if (!spdlog_logger_) return LogLevel::INFO;
    
    switch (spdlog_logger_->level()) {
        case spdlog::level::trace:    return LogLevel::TRACE;
        case spdlog::level::debug:    return LogLevel::DEBUG;
        case spdlog::level::info:     return LogLevel::INFO;
        case spdlog::level::warn:     return LogLevel::WARN;
        case spdlog::level::err:      return LogLevel::ERROR;
        case spdlog::level::critical: return LogLevel::CRITICAL;
        case spdlog::level::off:      return LogLevel::OFF;
        default:                      return LogLevel::INFO;
    }
}

} // namespace utils
} // namespace architect
