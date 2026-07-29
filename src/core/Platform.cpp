#include "core/Platform.h"
#include "core/StartupSequence.h"
#include "core/LatencyTracker.h"
#include "orders/Order.h"
#include "strategy/MakeMarketStrategy.h"
#include <fstream>
#include <iomanip>
#include <iostream>
#include <map>
#include <set>
#include <unordered_map>
#include <sstream>
#include <thread>
#include <cmath>
#include <nlohmann/json.hpp>

namespace architect {
namespace core {

Platform& Platform::getInstance() {
    static Platform instance;
    return instance;
}

Platform::~Platform() {
    if (running_) {
        stop();
    }
}

void Platform::initialize(const SimulationParams& params) {
    if (initialized_) {
        throw PlatformInitException("Platform already initialized");
    }
    
    params_ = params;
    
    // Step 1: Validate parameters
    std::string validation_error = params_.validate();
    if (!validation_error.empty()) {
        throw PlatformInitException(validation_error);
    }
    
    std::cout << "========================================" << std::endl;
    std::cout << "  Architect Platform Core" << std::endl;
    std::cout << "  Version: " << constants::PLATFORM_VERSION << std::endl;
    std::cout << "========================================" << std::endl;
    std::cout << "Simulation Date: " << params_.simulation_date << std::endl;
    std::cout << "Config Path: " << params_.config_path << std::endl;
    std::cout << "Binary Path: " << params_.binary_path << std::endl;
    std::cout << "========================================" << std::endl;
    
    // Step 2: Load configuration
    std::cout << "[INIT] Loading configuration..." << std::endl;
    initConfig();
    
    // Step 3: Initialize logger (validates/creates log directory)
    std::cout << "[INIT] Initializing logger..." << std::endl;
    initLogger();
    
    // From here, we can use the logger
    logger()->info("========================================");
    logger()->info("  Architect Platform Core Initializing");
    logger()->info("  Version: {}", constants::PLATFORM_VERSION);
    logger()->info("========================================");
    logger()->info("Simulation Date: {}", params_.simulation_date);
    logger()->info("Config Path: {}", params_.config_path);
    logger()->info("Log Directory: {}", getLogDirectory());
    
    // Step 4: Initialize EventManager
    logger()->info("[INIT] Initializing EventManager...");
    initEvents();
    
    // Step 5: Initialize OrderManager
    logger()->info("[INIT] Initializing OrderManager...");
    initOrders();
    
    // Step 6: Initialize UserManager
    logger()->info("[INIT] Initializing UserManager...");
    initUser();
    
    // Step 7: Initialize API clients
    logger()->info("[INIT] Initializing API clients...");
    initApi();
    
    // Step 8: Initialize MarketDataManager
    logger()->info("[INIT] Initializing MarketDataManager...");
    initMarketData();
    
    // Step 9: Initialize PortfolioManager
    logger()->info("[INIT] Initializing PortfolioManager...");
    initPortfolio();
    
    // Step 10: Save configuration to log directory
    logger()->info("[INIT] Saving configuration to log directory...");
    saveConfigToLogDir();
    
    // Step 11: Setup event handlers for logging
    logger()->info("[INIT] Setting up event handlers...");
    setupEventHandlers();
    
    initialized_ = true;
    
    logger()->info("========================================");
    logger()->info("  Platform Initialization Complete");
    logger()->info("  All modules initialized successfully");
    logger()->info("========================================");
    
    // Log event
    logger()->log_event("INIT", "platform", "initialization_complete");
}

void Platform::initConfig() {
    try {
        if (!config()->loadFromFileWithOptionalOverlays(params_.config_path)) {
            throw PlatformInitException("Failed to load configuration from: " + params_.config_path);
        }
        module_status_.config_loaded = true;
    } catch (const std::exception& e) {
        throw PlatformInitException(std::string("Config initialization failed: ") + e.what());
    }
}

void Platform::initLogger() {
    try {
        utils::Logger::Config log_config;
        log_config.base_log_dir = config()->getString("logging.directory", "logs");
        log_config.simulation_date = params_.simulation_date;  // YYYYMMDD format
        log_config.crash_on_init_failure = true;
        
        // Parse log level
        std::string level_str = config()->getLogLevel();
        if (level_str == "TRACE" || level_str == "trace") {
            log_config.console_level = utils::LogLevel::TRACE;
            log_config.file_level = utils::LogLevel::TRACE;
        } else if (level_str == "DEBUG" || level_str == "debug") {
            log_config.console_level = utils::LogLevel::DEBUG;
            log_config.file_level = utils::LogLevel::DEBUG;
        } else if (level_str == "INFO" || level_str == "info") {
            log_config.console_level = utils::LogLevel::INFO;
            log_config.file_level = utils::LogLevel::DEBUG;
        } else if (level_str == "WARN" || level_str == "warn") {
            log_config.console_level = utils::LogLevel::WARN;
            log_config.file_level = utils::LogLevel::INFO;
        } else if (level_str == "ERROR" || level_str == "error") {
            log_config.console_level = utils::LogLevel::ERROR;
            log_config.file_level = utils::LogLevel::WARN;
        }
        
        log_config.console_enabled = config()->isConsoleLogging();
        log_config.file_enabled = config()->isFileLogging();
        log_config.csv_enabled = config()->getBool("logging.csv_enabled", true);
        
        // This will throw LoggerInitException on failure
        utils::Logger::initialize(log_config);
        
        if (!utils::Logger::isInitialized()) {
            throw PlatformInitException("Logger failed to initialize");
        }
        
        module_status_.logger_initialized = true;
        
    } catch (const utils::LoggerInitException& e) {
        throw PlatformInitException(std::string("Logger initialization failed: ") + e.what());
    } catch (const std::exception& e) {
        throw PlatformInitException(std::string("Logger initialization failed: ") + e.what());
    }
}

void Platform::initEvents() {
    try {
        // EventManager is lazy-initialized, just verify it's accessible
        auto& em = events::EventManager::getInstance();
        (void)em;  // Suppress unused warning
        module_status_.events_initialized = true;
    } catch (const std::exception& e) {
        logger()->critical("EventManager initialization failed: {}", e.what());
        throw PlatformInitException(std::string("EventManager initialization failed: ") + e.what());
    }
}

void Platform::initOrders() {
    try {
        // OrderManager is lazy-initialized, verify accessibility
        auto& om = orders::OrderManager::getInstance();
        (void)om;
        module_status_.orders_initialized = true;
    } catch (const std::exception& e) {
        logger()->critical("OrderManager initialization failed: {}", e.what());
        throw PlatformInitException(std::string("OrderManager initialization failed: ") + e.what());
    }
}

void Platform::initUser() {
    try {
        // UserManager is lazy-initialized
        auto& um = user::UserManager::getInstance();
        (void)um;
        module_status_.user_initialized = true;
    } catch (const std::exception& e) {
        logger()->critical("UserManager initialization failed: {}", e.what());
        throw PlatformInitException(std::string("UserManager initialization failed: ") + e.what());
    }
}

void Platform::initApi() {
    try {
        // Initialize REST client with config (session token or API key for auth)
        auto& rc = api::RestClient::getInstance();
        rc.setBaseUrl(config()->getRestEndpoint());
        std::string auth = config()->getSessionToken();
        if (auth.empty() && !config()->getApiKey().empty()) {
            auth = config()->getApiKey();
        }
        rc.setSessionToken(auth);
        
        // WebSocket client
        auto& ws = api::WebSocketClient::getInstance();
        ws.setUrl(config()->getWebSocketEndpoint());
        
        module_status_.api_initialized = true;
    } catch (const std::exception& e) {
        logger()->critical("API initialization failed: {}", e.what());
        throw PlatformInitException(std::string("API initialization failed: ") + e.what());
    }
}

void Platform::initMarketData() {
    try {
        auto& md = marketdata::MarketDataManager::getInstance();
        (void)md;
        module_status_.marketdata_initialized = true;
    } catch (const std::exception& e) {
        logger()->critical("MarketDataManager initialization failed: {}", e.what());
        throw PlatformInitException(std::string("MarketDataManager initialization failed: ") + e.what());
    }
}

void Platform::initPortfolio() {
    try {
        auto& pm = portfolio::PortfolioManager::getInstance();
        (void)pm;
        module_status_.portfolio_initialized = true;
    } catch (const std::exception& e) {
        logger()->critical("PortfolioManager initialization failed: {}", e.what());
        throw PlatformInitException(std::string("PortfolioManager initialization failed: ") + e.what());
    }
}

void Platform::saveConfigToLogDir() {
    try {
        std::string log_dir = getLogDirectory();
        
        // Save used configuration
        std::string config_file = log_dir + "/" + params_.simulation_date + ".config_used.json";
        config()->saveToFile(config_file);
        
        // Copy original config
        if (fs::exists(params_.config_path)) {
            std::string orig_config = log_dir + "/" + params_.simulation_date + ".config_original.json";
            fs::copy_file(params_.config_path, orig_config, fs::copy_options::overwrite_existing);
        }
        
        // Save simulation parameters
        std::string params_file = log_dir + "/" + params_.simulation_date + ".simulation_params.txt";
        std::ofstream pf(params_file);
        if (pf.is_open()) {
            auto now = std::chrono::system_clock::now();
            auto time = std::chrono::system_clock::to_time_t(now);
            
            pf << "# Simulation Parameters\n";
            pf << "binary_path: " << params_.binary_path << "\n";
            pf << "config_path: " << params_.config_path << "\n";
            pf << "simulation_date: " << params_.simulation_date << "\n";
            pf << "init_time: " << std::put_time(std::localtime(&time), "%Y-%m-%d %H:%M:%S") << "\n";
            pf << "platform_version: " << constants::PLATFORM_VERSION << "\n";
            pf.close();
        }
        
        logger()->info("Configuration saved to: {}", log_dir);
        
    } catch (const std::exception& e) {
        logger()->error("Failed to save configuration: {}", e.what());
        // Don't throw - this is not critical
    }
}

void Platform::initializeEventHandlers() {
    // Public method called by StartupSequence
    setupEventHandlers();
}

void Platform::setupEventHandlers() {
    if (!hedge_provider_)
        hedge_provider_ = std::make_unique<hedging::NoOpHedgeProvider>();

    // Log order events and send to exchange (order gateway)
    order_sub_handle_ = events()->subscribe(events::EventType::ORDER_SUBMITTED,
        [this](const events::EventPtr& event) {
            // === LATENCY INSTRUMENTATION: T1 Enter Platform Handler ===
            auto& lat = LatencyTracker::get();
            auto t1_enter_handler = std::chrono::steady_clock::now();
            auto time_to_handler_us = std::chrono::duration_cast<std::chrono::microseconds>(
                t1_enter_handler - lat.strategy_decision_time).count();
            
            if (!event->hasData<events::OrderEventData>()) return;
            auto data = event->getData<events::OrderEventData>();
            
            std::cout << "[LATENCY_TRACE] T1_PLATFORM_HANDLER_ENTER: order_id=" << data.order_id 
                      << " time_from_strategy_decision=" << latencyDisplayUs(time_to_handler_us) << "us" << std::endl;
            
            logger()->log_order(
                data.order_id,
                data.client_order_id,
                std::string(data.symbol.data()),
                sideToString(data.side),
                orderTypeToString(data.order_type),
                orderStatusToString(data.status),
                data.price,
                data.quantity,
                data.filled_quantity,
                data.error_message
            );
            // Order gateway: Architect place_order API
            // https://docs.architect.exchange/api-reference/order-management/place-order
            // Body: s (symbol), d (B/S), q (contracts, int64), p (string), tif, po. Symbol format: PAIR-PERP (e.g. GBPUSD-PERP).
            if (data.order_type != core::OrderType::LIMIT || data.price <= 0) return;
            std::string symbol_str(data.symbol.data());
            // Per-strategy `data.symbol` is authoritative — it carries the leg's `order_symbol`
            // resolved by MakeMarketStrategy::mmAxSymbol() (e.g. JPYUSD-PERP, XAU-PERP, MXNUSD-PERP).
            // Falling through to `market_maker.order_symbol` here would route every product's
            // quotes onto one global default instrument and the gateway rejects them with
            // "Price Out of Bounds" / "Invalid Price Increment" when price/qty don't fit that
            // product's tick and band. Only synthesize when the strategy didn't supply a symbol.
            std::string ax_symbol = symbol_str;
            if (ax_symbol.empty()) {
                ax_symbol = config()->getMarketMakerOrderSymbol();
            }
            if (ax_symbol.empty()) {
                // API doc format: no hyphen in pair, suffix -PERP (e.g. BTC-USD -> BTCUSD-PERP)
                ax_symbol = symbol_str;
                for (auto it = ax_symbol.begin(); it != ax_symbol.end(); )
                    if (*it == '-') it = ax_symbol.erase(it); else ++it;
                if (ax_symbol.find("PERP") == std::string::npos)
                    ax_symbol += "-PERP";
            }
            // Resolve order-size step from the per-instrument catalog, NOT from the
            // global `market_maker.order_size_step`. The global value is a single
            // scalar (typically shaped for EURUSD = 100) and applying it to every
            // product corrupts smaller-tick instruments: e.g. an MM strategy that
            // submits qty=7 for XAG-PERP would have floor(7/100)*100 = 0, then the
            // legacy fallback re-assigned `contracts = step = 100` and the wire body
            // emitted q=100. The strategy's own `mmEffOrderSizeStepInt` already uses
            // the per-symbol step from `market_maker.instruments[]` and the AX
            // gateway catalog (`/instruments` minimum_order_size); this snap is just
            // a defense-in-depth guard for non-MM orders. It MUST use the same
            // per-symbol source the strategy used, otherwise it overrides the
            // strategy's correct quantity with the wrong one.
            int step = config()->getMarketMakerInstrumentOrderSizeStepForAxSymbol(ax_symbol);
            const char* step_source = "instruments_leg";
            if (step <= 0) {
                step = config()->getAxGatewayInstrumentMinimumOrderSize(ax_symbol);
                step_source = "ax_catalog";
            }
            if (step <= 0) {
                step = 1;
                step_source = "default_1";
            }
            const double q_raw = static_cast<double>(data.quantity);
            const int64_t contracts =
                static_cast<int64_t>(std::floor(q_raw / static_cast<double>(step))) *
                static_cast<int64_t>(step);
            if (contracts <= 0) {
                // The strategy/user submitted a quantity smaller than the venue's
                // minimum_order_size for this instrument. Reject loudly. NEVER
                // silently upscale to `step` (past behavior) — that turned user-typed
                // "7" into wire "100" and caused $560 trades to settle as $8005.
                logger()->warn(
                    "place_order rejected: order_id={} side={} sym={} requested_qty={} step={} "
                    "step_source={} — quantity below venue minimum_order_size (no silent upscale)",
                    data.order_id, sideToString(data.side), ax_symbol,
                    data.quantity, step, step_source);
                orders()->onOrderRejected(
                    data.order_id,
                    core::ErrorCode::INVALID_QUANTITY,
                    "Quantity below venue minimum_order_size; fix orders.json stack order_size");
                return;
            }
            
            // === LATENCY INSTRUMENTATION: T1.1 JSON Build Start ===
            auto t1_1_json_start = std::chrono::steady_clock::now();
            
            // FAST PATH: Build JSON string directly (avoids nlohmann::json overhead)
            char json_buf[256];
            int len = snprintf(json_buf, sizeof(json_buf),
                R"({"s":"%s","d":"%s","q":%lld,"p":"%s","tif":"GTC","po":false})",
                ax_symbol.c_str(),
                data.side == core::Side::BUY ? "B" : "S",
                static_cast<long long>(contracts),
                std::to_string(data.price).c_str()
            );
            
            // === LATENCY INSTRUMENTATION: T1.2 JSON Build Complete ===
            auto t1_2_json_done = std::chrono::steady_clock::now();
            auto json_build_us = std::chrono::duration_cast<std::chrono::microseconds>(
                t1_2_json_done - t1_1_json_start).count();
            
            // Calculate OUR CODE latency up to this point
            auto our_code_pre_http_us = std::chrono::duration_cast<std::chrono::microseconds>(
                t1_2_json_done - lat.strategy_decision_time).count();
            
            std::cout << "[LATENCY_TRACE] T1.2_BEFORE_HTTP: order_id=" << data.order_id 
                      << " json_build=" << latencyDisplayUs(json_build_us) << "us"
                      << " OUR_CODE_PRE_HTTP=" << latencyDisplayUs(our_code_pre_http_us) << "us" << std::endl;
            
            // === LATENCY INSTRUMENTATION: T2 HTTP Call Start ===
            auto t2_http_start = std::chrono::steady_clock::now();
            auto response = rest()->placeOrderRaw(std::string(json_buf, len));
            auto t2_http_end = std::chrono::steady_clock::now();
            
            // === LATENCY INSTRUMENTATION: T2 HTTP Call Complete ===
            auto http_rtt_us = std::chrono::duration_cast<std::chrono::microseconds>(
                t2_http_end - t2_http_start).count();
            
            {
                std::ostringstream ms;
                ms << std::fixed << std::setprecision(2) << latencyDisplayMsFromUs(http_rtt_us);
                std::cout << "[LATENCY_TRACE] T2_HTTP_COMPLETE: order_id=" << data.order_id
                          << " NETWORK+EXCHANGE=" << latencyDisplayUs(http_rtt_us) << "us (" << ms.str() << "ms)" << std::endl;
            }
            
            // === LATENCY INSTRUMENTATION: T2.1 Response Parse Start ===
            auto t2_1_parse_start = std::chrono::steady_clock::now();
            
            if (response.is_success && response.status_code >= 200 && response.status_code < 300) {
                std::string exchange_id;
                std::string exchange_ts;
                try {
                    auto body = nlohmann::json::parse(response.body);
                    if (body.contains("oid")) exchange_id = body["oid"].get<std::string>();
                    else if (body.contains("order_id")) exchange_id = body["order_id"].get<std::string>();
                    else if (body.contains("id")) exchange_id = body["id"].get<std::string>();
                    else exchange_id = std::to_string(data.order_id);
                    // Check for exchange timestamp
                    if (body.contains("recv_time")) exchange_ts = body["recv_time"].get<std::string>();
                    else if (body.contains("timestamp")) exchange_ts = body["timestamp"].get<std::string>();
                    else if (body.contains("created_at")) exchange_ts = body["created_at"].get<std::string>();
                    else if (body.contains("t")) exchange_ts = std::to_string(body["t"].get<int64_t>());
                } catch (...) {
                    exchange_id = std::to_string(data.order_id);
                }
                
                // === LATENCY INSTRUMENTATION: T2.2 Response Parse Complete ===
                auto t2_2_parse_done = std::chrono::steady_clock::now();
                auto parse_us = std::chrono::duration_cast<std::chrono::microseconds>(
                    t2_2_parse_done - t2_1_parse_start).count();
                
                // === LATENCY INSTRUMENTATION: Before onOrderAccepted ===
                auto t2_3_before_accept = std::chrono::steady_clock::now();
                
                // Log latency metrics
                logger()->info("[ORDER_LATENCY] order_id={} exchange_id={} rtt={}us ({:.2f}ms) http_latency={}ms exchange_ts={}",
                    data.order_id, exchange_id, latencyDisplayUs(http_rtt_us), latencyDisplayMsFromUs(http_rtt_us),
                    latencyDisplayMs(response.latency.count()),
                    exchange_ts.empty() ? "N/A" : exchange_ts);
                
                orders()->onOrderAccepted(data.order_id, exchange_id);
                
                // === LATENCY INSTRUMENTATION: After onOrderAccepted ===
                auto t2_4_after_accept = std::chrono::steady_clock::now();
                auto accept_dispatch_us = std::chrono::duration_cast<std::chrono::microseconds>(
                    t2_4_after_accept - t2_3_before_accept).count();
                
                // Calculate OUR CODE latency after HTTP
                auto our_code_post_http_us = std::chrono::duration_cast<std::chrono::microseconds>(
                    t2_4_after_accept - t2_http_end).count();
                
                // Calculate total OUR CODE latency
                auto total_our_code_us = our_code_pre_http_us + our_code_post_http_us;
                
                std::cout << "[LATENCY_TRACE] T2.4_HANDLER_EXIT: order_id=" << data.order_id 
                          << " response_parse=" << latencyDisplayUs(parse_us) << "us"
                          << " accept_dispatch=" << latencyDisplayUs(accept_dispatch_us) << "us"
                          << " OUR_CODE_POST_HTTP=" << latencyDisplayUs(our_code_post_http_us) << "us" << std::endl;
                
                // === FINAL LATENCY SUMMARY ===
                auto total_latency_us = std::chrono::duration_cast<std::chrono::microseconds>(
                    t2_4_after_accept - lat.strategy_decision_time).count();
                
                auto fmt_ms = [](long long us) {
                    std::ostringstream o;
                    o << std::fixed << std::setprecision(2) << latencyDisplayMsFromUs(us);
                    return o.str();
                };
                std::ostringstream pct;
                pct << std::fixed << std::setprecision(1) << (100.0 * total_our_code_us / total_latency_us);

                std::cout << "╔════════════════════ LATENCY BREAKDOWN ════════════════════╗" << std::endl;
                std::cout << "║ Order ID: " << data.order_id << std::endl;
                std::cout << "║ ─────────────────────────────────────────────────────────" << std::endl;
                std::cout << "║ OUR CODE (pre-network):  " << std::setw(8) << latencyDisplayUs(our_code_pre_http_us) << " us ("
                          << fmt_ms(our_code_pre_http_us) << " ms)" << std::endl;
                std::cout << "║ NETWORK + EXCHANGE:      " << std::setw(8) << latencyDisplayUs(http_rtt_us) << " us ("
                          << fmt_ms(http_rtt_us) << " ms)" << std::endl;
                std::cout << "║ OUR CODE (post-network): " << std::setw(8) << latencyDisplayUs(our_code_post_http_us) << " us ("
                          << fmt_ms(our_code_post_http_us) << " ms)" << std::endl;
                std::cout << "║ ─────────────────────────────────────────────────────────" << std::endl;
                std::cout << "║ TOTAL OUR CODE:          " << std::setw(8) << latencyDisplayUs(total_our_code_us) << " us ("
                          << fmt_ms(total_our_code_us) << " ms)" << std::endl;
                std::cout << "║ TOTAL LATENCY:           " << std::setw(8) << latencyDisplayUs(total_latency_us) << " us ("
                          << fmt_ms(total_latency_us) << " ms)" << std::endl;
                std::cout << "║ OUR CODE %:              " << std::setw(8) << pct.str() << " %" << std::endl;
                std::cout << "╚═══════════════════════════════════════════════════════════╝" << std::endl;
                
            } else {
                std::string msg = response.error_message.empty() ? response.body : response.error_message;
                if (msg.empty()) msg = "HTTP " + std::to_string(response.status_code);
                logger()->warn("[ORDER_LATENCY] order_id={} REJECTED rtt={}us ({:.2f}ms) reason={}",
                    data.order_id, latencyDisplayUs(http_rtt_us), latencyDisplayMsFromUs(http_rtt_us), msg);
                orders()->onOrderRejected(data.order_id, core::ErrorCode::UNKNOWN_ERROR, msg);
            }
        });
    
    // Handle order cancellations - send to exchange cancel_order endpoint
    cancel_sub_handle_ = events()->subscribe(events::EventType::ORDER_CANCELLED,
        [this](const events::EventPtr& event) {
            if (!event->hasData<events::OrderEventData>()) return;
            auto data = event->getData<events::OrderEventData>();

            // venue_confirmed cancels are NOTIFICATIONS (reconcile sync, or a batch
            // cancel-replace whose HTTP cancel already went out on the wire). Re-sending
            // here would double-hit the venue AND double-count MM_JOB_VENUE_CALLS cancels
            // (the cancels=4-vs-2 bug). The local state is already CANCELLED by the caller,
            // so there is nothing left to do on the wire.
            if (data.venue_confirmed) {
                return;
            }

            // Get exchange order ID from OrderManager
            auto order = orders()->getOrder(data.order_id);
            if (!order) {
                // Order might already be removed from manager - try to use exchange_order_id from event if available
                logger()->debug("[ORDER_CANCEL] order_id={} not found in manager, skipping", data.order_id);
                return;
            }
            
            std::string exchange_id = order->exchange_order_id;
            if (exchange_id.empty()) {
                logger()->debug("[ORDER_CANCEL] order_id={} has no exchange_id, skipping", data.order_id);
                return;
            }
            
            // ULTRA-FAST: Build JSON directly, fire async (non-blocking)
            char json_buf[128];
            int len = snprintf(json_buf, sizeof(json_buf), R"({"oid":"%s"})", exchange_id.c_str());
            
            // Fire cancel to exchange (non-blocking - we don't wait for response)
            auto submit_time = std::chrono::steady_clock::now();
            auto response = rest()->cancelOrderGateway(std::string(json_buf, len));
            auto ack_time = std::chrono::steady_clock::now();
            auto rtt_us = std::chrono::duration_cast<std::chrono::microseconds>(ack_time - submit_time).count();
            
            if (response.is_success && response.status_code >= 200 && response.status_code < 300) {
                logger()->debug("[ORDER_CANCEL] order_id={} exchange_id={} rtt={}us ({:.2f}ms)",
                    data.order_id, exchange_id, latencyDisplayUs(rtt_us), latencyDisplayMsFromUs(rtt_us));
            } else {
                std::string msg = response.error_message.empty() ? response.body : response.error_message;
                if (msg.empty()) msg = "HTTP " + std::to_string(response.status_code);
                const bool benign = msg.find("not found") != std::string::npos ||
                    msg.find("cannot be canceled") != std::string::npos ||
                    msg.find("cannot be cancelled") != std::string::npos;
                if (benign) {
                    logger()->debug("[ORDER_CANCEL] order_id={} noop rtt={}us reason={} (order already gone or terminal)",
                        data.order_id, latencyDisplayUs(rtt_us), msg);
                } else {
                    logger()->warn("[ORDER_CANCEL] order_id={} FAILED rtt={}us reason={}",
                        data.order_id, latencyDisplayUs(rtt_us), msg);
                }
            }
        });
    
    // Handle order modifications - send to exchange modify_order endpoint
    modify_sub_handle_ = events()->subscribe(events::EventType::ORDER_MODIFIED,
        [this](const events::EventPtr& event) {
            if (!event->hasData<events::OrderEventData>()) return;
            auto data = event->getData<events::OrderEventData>();
            
            std::string symbol_str(data.symbol.data());
            // Per-order `data.symbol` is authoritative — see place-order handler above for full
            // rationale. modify_order on the wrong market 404s the exchange order id.
            std::string ax_symbol = symbol_str;
            if (ax_symbol.empty()) {
                ax_symbol = config()->getMarketMakerOrderSymbol();
            }
            if (ax_symbol.empty()) {
                ax_symbol = symbol_str;
                for (auto it = ax_symbol.begin(); it != ax_symbol.end(); )
                    if (*it == '-') it = ax_symbol.erase(it); else ++it;
                if (ax_symbol.find("PERP") == std::string::npos)
                    ax_symbol += "-PERP";
            }
            
            // Need exchange order ID for modify - get from OrderManager
            auto order = orders()->getOrder(data.order_id);
            if (!order) {
                logger()->warn("[ORDER_MODIFY] order_id={} not found", data.order_id);
                return;
            }
            
            std::string exchange_id = order->exchange_order_id;
            if (exchange_id.empty()) {
                logger()->warn("[ORDER_MODIFY] order_id={} has no exchange_id - order may not be accepted yet", data.order_id);
                return;
            }
            
            // Match place_order: symbol + contract qty on the order gateway. `data.quantity` is the
            // full order size; modify must send remaining contracts (same step snapping as place).
            const double raw_rem = (order->remaining_quantity > 1e-12)
                ? static_cast<double>(order->remaining_quantity)
                : std::max(0.0, static_cast<double>(order->quantity - order->filled_quantity));
            // Per-symbol step (see PLACE handler note above). MUST NOT fall through
            // to the global `market_maker.order_size_step`, which on this runtime
            // config is 100 and would corrupt XAG/XAU/SPY modifies the same way it
            // corrupted PLACE.
            int step = config()->getMarketMakerInstrumentOrderSizeStepForAxSymbol(ax_symbol);
            const char* step_source = "instruments_leg";
            if (step <= 0) {
                step = config()->getAxGatewayInstrumentMinimumOrderSize(ax_symbol);
                step_source = "ax_catalog";
            }
            if (step <= 0) {
                step = 1;
                step_source = "default_1";
            }
            const int64_t contracts =
                static_cast<int64_t>(std::floor(raw_rem / static_cast<double>(step))) *
                static_cast<int64_t>(step);
            if (contracts <= 0) {
                logger()->warn(
                    "[ORDER_MODIFY] order_id={} skipped: remaining qty below venue minimum "
                    "(sym={} raw_rem={} step={} step_source={}) — no silent upscale to step",
                    data.order_id, ax_symbol, raw_rem, step, step_source);
                return;
            }
            
            char json_buf[384];
            int len = snprintf(json_buf, sizeof(json_buf),
                R"({"s":"%s","oid":"%s","p":"%s","q":%lld})",
                ax_symbol.c_str(),
                exchange_id.c_str(),
                std::to_string(data.price).c_str(),
                contracts);
            
            auto submit_time = std::chrono::steady_clock::now();
            auto response = rest()->modifyOrderRaw(std::string(json_buf, len));
            auto ack_time = std::chrono::steady_clock::now();
            auto rtt_us = std::chrono::duration_cast<std::chrono::microseconds>(ack_time - submit_time).count();
            
            if (response.is_success && response.status_code >= 200 && response.status_code < 300) {
                logger()->info("[ORDER_MODIFY] order_id={} exchange_id={} new_price={} rtt={}us ({:.2f}ms)",
                    data.order_id, exchange_id, data.price, latencyDisplayUs(rtt_us), latencyDisplayMsFromUs(rtt_us));
            } else {
                std::string msg = response.error_message.empty() ? response.body : response.error_message;
                if (msg.empty()) msg = "HTTP " + std::to_string(response.status_code);
                logger()->warn(
                    "[ORDER_MODIFY] order_id={} FAILED rtt={}us reason={} (s={} oid={} q={})",
                    data.order_id,
                    latencyDisplayUs(rtt_us),
                    msg,
                    ax_symbol,
                    exchange_id,
                    contracts);
            }
        });
    
    // Common fill handler - processes both full and partial fills
    auto processFillEvent = [this](const events::EventPtr& event, const char* fill_type) {
        if (!event->hasData<events::OrderEventData>()) return;
        auto data = event->getData<events::OrderEventData>();
        std::string symbol_str(data.symbol.data());
        std::string side_str = sideToString(data.side);
        
        logger()->info("[FILL_EVENT] {} symbol={} side={} qty={} price={:.6f} order_id={}",
            fill_type, symbol_str, side_str, data.filled_quantity, data.avg_fill_price, data.order_id);
        
        // Log to execution.csv
        logger()->log_execution(
            0,  // trade_id
            data.order_id,
            symbol_str,
            side_str,
            data.avg_fill_price,
            data.filled_quantity,
            0.0,  // fee
            fill_type
        );
        
        // Log to fills.csv
        logger()->log_fill(
            0,  // trade_id (can be set from exchange response)
            data.order_id,
            symbol_str,
            side_str,
            data.avg_fill_price,
            data.filled_quantity,
            0.0,  // fee
            "USD"  // fee_currency
        );
        
        // Update portfolio with fill
        orders::Fill fill;
        fill.order_id = data.order_id;
        fill.symbol = data.symbol;
        fill.side = data.side;
        fill.price = data.avg_fill_price;
        fill.quantity = data.filled_quantity;
        fill.executed_at = std::chrono::high_resolution_clock::now().time_since_epoch();
        portfolio()->updatePnLFromFill(fill);
        
        // Log position update
        auto positions = portfolio()->getAllPositions();
        if (!positions.empty()) {
            for (const auto& pos : positions) {
                std::string pos_symbol(pos.symbol.data());
                logger()->log_position(
                    pos_symbol,
                    sideToString(pos.side),
                    pos.quantity,
                    pos.entry_price,
                    data.avg_fill_price,
                    pos.unrealized_pnl,
                    pos.realized_pnl
                );
                logger()->log_pnl(
                    pos_symbol,
                    pos.realized_pnl,
                    pos.unrealized_pnl,
                    pos.realized_pnl + pos.unrealized_pnl,
                    0.0  // total fees
                );
            }
        }
        
        // Hedge: generic provider (per-client hedging exchange); default is no-op
        if (config()->isHedgeEnabled() && hedge_provider_) {
            hedging::HedgeFillInfo info;
            info.symbol   = std::string(data.symbol.data());
            info.side    = data.side;
            info.quantity = data.filled_quantity;
            info.price   = data.avg_fill_price;
            info.order_id = data.order_id;
            hedge_provider_->onFill(info);
        }
        // Hedge webhook: optional POST to URL (recipient can place hedge order on another venue)
        if (config()->isHedgeEnabled()) {
            std::string url = config()->getHedgeWebhookUrl();
            if (!url.empty()) {
                std::string sym_str(data.symbol.data());
                std::string sd_str = (data.side == core::Side::BUY ? "buy" : "sell");
                nlohmann::json body;
                body["symbol"] = sym_str;
                body["side"] = sd_str;
                body["quantity"] = data.filled_quantity;
                body["price"] = data.avg_fill_price;
                body["order_id"] = data.order_id;
                std::string body_str = body.dump();
                std::thread([this, url, body_str]() {
                    api::HttpRequest req(api::HttpMethod::POST, "");
                    req.absolute_url = url;
                    req.skip_auth = true;
                    req.body = body_str;
                    req.headers["Content-Type"] = "application/json";
                    auto resp = rest()->request(req);
                    if (!resp.isOk())
                        logger()->warn("Hedge webhook POST failed: HTTP {} {}", resp.status_code, resp.body.substr(0, 80));
                }).detach();
            }
        }
    };
    
    // Log FULL fills and update portfolio PnL
    fill_sub_handle_ = events()->subscribe(events::EventType::ORDER_FILLED,
        [processFillEvent](const events::EventPtr& event) {
            processFillEvent(event, "FULL_FILL");
        });
    
    // Log PARTIAL fills and update portfolio PnL - critical for position tracking!
    partial_fill_sub_handle_ = events()->subscribe(events::EventType::ORDER_PARTIALLY_FILLED,
        [processFillEvent](const events::EventPtr& event) {
            processFillEvent(event, "PARTIAL_FILL");
        });
    
    // Log ticks
    tick_sub_handle_ = events()->subscribe(events::EventType::TICK_UPDATE,
        [this](const events::EventPtr& event) {
            if (event->hasData<events::TickEventData>()) {
                auto data = event->getData<events::TickEventData>();
                logger()->log_tick(
                    std::string(data.symbol.data()),
                    data.bid,
                    data.ask,
                    data.last,
                    data.bid_size,
                    data.ask_size
                );
            }
        });

    // Paper fill simulator: after ORDER_ACCEPTED, optionally simulate a fill locally (exchange order stays OPEN)
    order_accepted_handle_ = events()->subscribe(events::EventType::ORDER_ACCEPTED,
        [this](const events::EventPtr& event) {
            if (!config()->getMarketMakerPaperSimulateFills() || !feed_status_.is_paper) return;
            if (!event->hasData<events::OrderEventData>()) return;
            auto data = event->getData<events::OrderEventData>();
            orders::OrderId oid = data.order_id;
            std::thread([this, oid]() {
                std::this_thread::sleep_for(std::chrono::milliseconds(500));
                auto& om = orders::OrderManager::getInstance();
                auto order = om.getOrder(oid);
                if (!order || !order->isActive()) return;
                logger()->info("[PAPER_FILL] Simulating fill for order {} (price={} qty={})", 
                    oid, order->price, order->quantity);
                orders::Fill f;
                f.order_id = oid;
                f.symbol = order->symbol;
                f.side = order->side;
                f.price = order->price;
                f.quantity = order->quantity;
                f.fee = 0;
                f.executed_at = std::chrono::high_resolution_clock::now().time_since_epoch();
                om.onOrderFilled(oid, f);
            }).detach();
        });
}

void Platform::start() {
    if (!initialized_) {
        throw PlatformInitException("Platform not initialized. Call initialize() first.");
    }
    
    if (running_) {
        return;  // Already running
    }
    
    logger()->info("Starting platform...");
    
    // Start event manager
    events()->start(config()->getWorkerThreads());
    logger()->info("EventManager started with {} worker threads", config()->getWorkerThreads());
    
    // Verify feed if configured
    if (config()->shouldVerifyFeedOnStartup()) {
        logger()->info("[START] Verifying feed connection...");
        if (!verifyFeed()) {
            logger()->warn("Feed verification failed: {}", feed_status_.error_message);
            // Don't crash, just log warning - feed might become available later
        } else {
            logger()->info("[START] Feed verified: mode={}, is_live={}, is_paper={}", 
                          feedModeToString(feed_status_.mode), 
                          feed_status_.is_live, 
                          feed_status_.is_paper);
        }
    } else {
        // Just detect from config without API verification
        detectFeedMode();
        logger()->info("[START] Feed mode from config: {}", feedModeToString(feed_status_.mode));
    }
    
    // Log feed status
    logger()->log_event("FEED", "platform", 
        "mode=" + std::string(feedModeToString(feed_status_.mode)) + 
        " source=" + std::string(feedSourceToString(feed_status_.source)) +
        " is_live=" + std::to_string(feed_status_.is_live) +
        " is_paper=" + std::to_string(feed_status_.is_paper));
    
    // Initialize portfolio from API
    portfolio()->initialize();
    
    // Start market data manager
    marketdata()->start();
    
    // Start external feed (theo pricing) if enabled
    externalFeed()->start();
    if (config()->isExternalFeedEnabled()) {
        auto quote = externalFeed()->getLastQuote();
        if (quote) {
            logger()->info("[ExternalFeed] Ready (theo available for {})", config()->getMarketMakerTheoSymbol());
        } else {
            std::string err = externalFeed()->getLastFetchError();
            logger()->warn("[ExternalFeed] Initial fetch failed - no theo until feed delivers. {}", err.empty() ? "Check external_feed (neon_fix + stunnel, or REST rest_url + symbol)." : err);
        }
    }

    running_ = true;
    session_clock_started_ = true;
    start_time_ = std::chrono::steady_clock::now();
    
    logger()->info("========================================");
    logger()->info("  Platform Started");
    logger()->info("  Feed Mode: {}", feedModeToString(feed_status_.mode));
    logger()->info("  Feed Source: {}", feedSourceToString(feed_status_.source));
    logger()->info("  Is Real-Time: {}", isRealTimeFeed() ? "YES" : "NO");
    logger()->info("  Is Simulation: {}", isSimulationFeed() ? "YES" : "NO");
    logger()->info("========================================");
    
    logger()->log_event("START", "platform", "platform_started");
}

void Platform::stop() {
    if (!running_) {
        return;
    }
    
    logger()->info("Stopping platform...");
    
    running_ = false;
    
    // Stop components in reverse order
    externalFeed()->stop();
    marketdata()->stop();
    websocket()->disconnect();
    events()->stop(true);  // Drain events
    
    // Final statistics
    auto stats = getStats();
    logger()->info("Platform Statistics:");
    logger()->info("  Uptime: {} seconds", stats.uptime.count());
    logger()->info("  Events Processed: {}", stats.events_processed);
    logger()->info("  Orders Submitted: {}", stats.orders_submitted);
    logger()->info("  Orders Filled: {}", stats.orders_filled);
    logger()->info("  Ticks Received: {}", stats.ticks_received);
    
    // Log final metrics to CSV
    logger()->log_metrics("uptime_seconds", static_cast<double>(stats.uptime.count()), "s");
    logger()->log_metrics("events_processed", static_cast<double>(stats.events_processed), "count");
    logger()->log_metrics("orders_submitted", static_cast<double>(stats.orders_submitted), "count");
    logger()->log_metrics("orders_filled", static_cast<double>(stats.orders_filled), "count");
    logger()->log_metrics("ticks_received", static_cast<double>(stats.ticks_received), "count");
    
    logger()->log_event("STOP", "platform", "platform_stopped");
    
    logger()->flush();
    logger()->info("Platform stopped");
    logger()->shutdown();
}

std::string Platform::getLogDirectory() const {
    if (utils::Logger::isInitialized()) {
        return logger()->getLogDirectory();
    }
    // Fallback
    return config()->getString("logging.directory", "logs") + "/" + params_.simulation_date;
}

void Platform::dumpState() {
    // Compact heartbeat: orders, portfolio, events on single line
    auto order_stats = orders()->getStats();
    auto event_stats = events()->getStats();
    
    // Handle underflow: if active_orders is huge (underflowed), show 0
    auto active = order_stats.active_orders;
    if (active > 1000000) active = 0;  // Obviously underflowed
    
    // Use exchange PnL if available (from StartupSequence::pollExchangePositions)
    double exchange_pnl = Startup().getExchangePnL();
    
    // total_orders = lifetime submits in OM stats (resets on clearAll); active = non-terminal working rows.
    //
    // === SILVER max_position breach fix (2026-05-21) ===========================
    // Desk-managed fills routed via `processFillDeskStack` bypass
    // `OrderManager::onOrderFilled` so they are NOT counted in
    // `order_stats.total_fills`. The XAG 2026-05-21 incident logs showed
    // `fills=0` for 7 minutes straight even though 2 venue fills occurred —
    // an incident-triage trap. Sum the per-strategy desk synth counter from
    // every active MakeMarketStrategy and surface it as `desk_synth_fills`
    // so operators can spot venue-truth fills that didn't go through OM.
    std::uint64_t desk_synth_fills = 0;
    for (const auto& sp :
         strategy::StrategyManager::getInstance().getAllStrategies()) {
        auto mm = std::dynamic_pointer_cast<strategy::MakeMarketStrategy>(sp);
        if (!mm) {
            continue;
        }
        desk_synth_fills += mm->mmDeskSynthFillsCount();
    }
    logger()->info(
        "[HEARTBEAT] om_active={} om_total_submits={} fills={} (complete={}) "
        "desk_synth_fills={} | exchange_pnl=${:.2f} | events={}",
        active,
        order_stats.total_orders,
        order_stats.total_fills,
        order_stats.filled_orders,
        desk_synth_fills,
        exchange_pnl,
        event_stats.events_processed);
}

void Platform::runFeedGuardianHeartbeat() {
    if (!config()) {
        return;
    }
    auto& cfg = *config();
    if (!cfg.getBool("trading.feed_guardian_enabled", true)) {
        return;
    }
    if (isSimulationFeed()) {
        return;
    }
    if (!isRealTimeFeed()) {
        return;
    }

    const bool need_ws = cfg.getBool("trading.feed_guardian_require_ws", true);
    const bool need_ext =
        cfg.getBool("trading.feed_guardian_require_external", true) && externalFeed()->isEnabled();

    const bool ws_down = need_ws && !websocket()->isConnected();
    auto* efm = externalFeed();

    // WS-down is global: order routing is shared so cancel everything we can.
    if (ws_down) {
        logger()->error(
            "[FEED_GUARDIAN] Architect WS down — cancel open AX orders (per subscribed/MM symbols). "
            "Cause: architect_ws_not_connected");
        const std::vector<std::string> syms = cfg.getMarketDataSubscriptionSymbols();
        bool any_symbol = false;
        for (const std::string& sym : syms) {
            if (sym.empty()) {
                continue;
            }
            any_symbol = true;
            auto r = rest()->cancelAllOrders(sym);
            if (!r.is_success) {
                logger()->error("[FEED_GUARDIAN] cancel-all symbol={} HTTP {} — {}", sym, r.status_code,
                    r.error_message.empty() ? r.body.substr(0, 200) : r.error_message);
            }
        }
        if (!any_symbol) {
            auto r = rest()->cancelAllOrders(std::nullopt);
            if (!r.is_success) {
                logger()->error("[FEED_GUARDIAN] cancel-all (no symbol list) HTTP {} — {}", r.status_code,
                    r.error_message.empty() ? r.body.substr(0, 200) : r.error_message);
            }
        }
        orders()->clearAll();
        strategyManager()->clearOpenOrderTrackingAll();
        strategy::MakeMarketStrategy::feedGuardianResetAllRegistered("feed_guardian");
        return;
    }

    if (!need_ext) {
        return;
    }

    // Multi-feed: per-feed cancel by feed->instrument mapping. If a source heartbeat is down,
    // cancel ALL instruments mapped to that source (config + live MM instances).
    // Recovery hook: when all previously down feed-mapped symbols are healthy again, trigger
    // one pair-enforce pass so each mapped MM stack restores bid+ask quickly.
    static std::set<std::string> prev_down_feed_symbols;
    static std::unordered_map<std::string, int> down_feed_warn_counts;
    if (cfg.isMultiTheoFeeds() && cfg.hasMarketMakerInstrumentList()) {
        std::map<std::string, std::set<std::string>> down_src_to_symbols;
        std::set<std::string> current_down_feed_symbols;
        auto add_target = [&](const std::string& src_raw, const std::string& sym_raw) {
            const std::string src = src_raw;
            const std::string sym = sym_raw;
            if (src.empty() || sym.empty()) {
                return;
            }
            down_src_to_symbols[src].insert(sym);
            current_down_feed_symbols.insert(sym);
        };

        // Primary mapping from configured instruments.
        for (const auto& leg : cfg.getMarketMakerInstruments()) {
            const std::string src = cfg.getResolvedTheoSourceForLeg(leg);
            if (src.empty()) {
                continue;
            }
            if (efm->isFeedUpForSource(src)) {
                continue;
            }
            std::string ax = leg.order_symbol.empty() ? leg.ax_symbol : leg.order_symbol;
            if (ax.empty()) {
                continue;
            }
            add_target(src, ax);
        }

        // Runtime mapping from active MM strategies (covers desk-spawned stacks / overrides).
        for (const auto& sp : strategyManager()->getAllStrategies()) {
            auto mm = std::dynamic_pointer_cast<strategy::MakeMarketStrategy>(sp);
            if (!mm) {
                continue;
            }
            const std::string src = mm->mmTheoSource();
            if (src.empty() || efm->isFeedUpForSource(src)) {
                continue;
            }
            add_target(src, mm->mmAxSymbol());
        }

        if (!down_src_to_symbols.empty()) {
            for (const auto& [src, symbols] : down_src_to_symbols) {
                std::string src_list;
                for (const auto& ax : symbols) {
                    if (!src_list.empty()) {
                        src_list += ", ";
                    }
                    src_list += ax;

                    auto r = rest()->cancelAllOrders(ax);
                    if (!r.is_success) {
                        logger()->error("[FEED_GUARDIAN] cancel-all symbol={} ({}) HTTP {} — {}", ax, src,
                            r.status_code,
                            r.error_message.empty() ? r.body.substr(0, 200) : r.error_message);
                    } else {
                        // Keep local OM coherent with feed-guardian venue cancel.
                        orders()->purgeNonTerminalOrdersForSymbol(ax);
                    }
                }
                const int emission = ++down_feed_warn_counts[src];
                if (emission <= 3) {
                    logger()->warn(
                        "[FEED_GUARDIAN] Per-feed cancel — source={} down; mapped instruments cancelled: {}",
                        src,
                        src_list);
                } else {
                    logger()->debug(
                        "[FEED_GUARDIAN] Per-feed cancel (suppressed warn after 3 emits) — source={} down; "
                        "mapped instruments cancelled: {}",
                        src,
                        src_list);
                }
            }
            // Local MM reset only for affected feed-mapped instances (no global wipe).
            strategy::MakeMarketStrategy::feedGuardianResetForDownFeeds("feed_guardian");
            prev_down_feed_symbols = std::move(current_down_feed_symbols);
            return;
        }

        if (!prev_down_feed_symbols.empty()) {
            std::string recovered_list;
            for (const auto& sym : prev_down_feed_symbols) {
                if (!recovered_list.empty()) {
                    recovered_list += ", ";
                }
                recovered_list += sym;
            }
            logger()->info(
                "[FEED_GUARDIAN] Feed recovered for mapped symbols: {} — triggering MM pair restore",
                recovered_list);
            try {
                strategy::MakeMarketStrategy::ensureStartupQuotePairAllRegistered("feed_recovered");
            } catch (const std::exception& e) {
                logger()->warn("[FEED_GUARDIAN] feed_recovered pair-enforce error: {}", e.what());
            } catch (...) {
            }
            prev_down_feed_symbols.clear();
        }
        return;
    }

    // Single-feed legacy mode: external transport down means everything is dark.
    if (!efm->isPricingTransportConnected()) {
        logger()->error(
            "[FEED_GUARDIAN] External pricing transport down — cancel open AX orders (per subscribed/MM symbols). "
            "Cause: external_pricing_transport_down");
        const std::vector<std::string> syms = cfg.getMarketDataSubscriptionSymbols();
        bool any_symbol = false;
        for (const std::string& sym : syms) {
            if (sym.empty()) {
                continue;
            }
            any_symbol = true;
            auto r = rest()->cancelAllOrders(sym);
            if (!r.is_success) {
                logger()->error("[FEED_GUARDIAN] cancel-all symbol={} HTTP {} — {}", sym, r.status_code,
                    r.error_message.empty() ? r.body.substr(0, 200) : r.error_message);
            }
        }
        if (!any_symbol) {
            auto r = rest()->cancelAllOrders(std::nullopt);
            if (!r.is_success) {
                logger()->error("[FEED_GUARDIAN] cancel-all (no symbol list) HTTP {} — {}", r.status_code,
                    r.error_message.empty() ? r.body.substr(0, 200) : r.error_message);
            }
        }
        orders()->clearAll();
        strategyManager()->clearOpenOrderTrackingAll();
        strategy::MakeMarketStrategy::feedGuardianResetAllRegistered("feed_guardian");
    }
}

bool Platform::buildPlaceOrderJson(const std::string& symbol,
                                   core::Side side,
                                   core::Price price,
                                   core::Quantity quantity,
                                   std::string& out_json,
                                   std::string& out_reject_msg) const {
    // MIRRORS the single-order ORDER_SUBMITTED handler's inline body build (see the lambda in
    // setupEventHandlers / initializeEventHandlers). Keep in sync with that path.
    out_json.clear();
    out_reject_msg.clear();
    if (price <= 0) {
        out_reject_msg = "non-positive price";
        return false;
    }
    std::string ax_symbol = symbol;
    if (ax_symbol.empty()) {
        ax_symbol = config()->getMarketMakerOrderSymbol();
    }
    if (ax_symbol.empty()) {
        ax_symbol = symbol;
        for (auto it = ax_symbol.begin(); it != ax_symbol.end();) {
            if (*it == '-') {
                it = ax_symbol.erase(it);
            } else {
                ++it;
            }
        }
        if (ax_symbol.find("PERP") == std::string::npos) {
            ax_symbol += "-PERP";
        }
    }
    int step = config()->getMarketMakerInstrumentOrderSizeStepForAxSymbol(ax_symbol);
    const char* step_source = "instruments_leg";
    if (step <= 0) {
        step = config()->getAxGatewayInstrumentMinimumOrderSize(ax_symbol);
        step_source = "ax_catalog";
    }
    if (step <= 0) {
        step = 1;
        step_source = "default_1";
    }
    const double q_raw = static_cast<double>(quantity);
    const int64_t contracts =
        static_cast<int64_t>(std::floor(q_raw / static_cast<double>(step))) *
        static_cast<int64_t>(step);
    if (contracts <= 0) {
        // NEVER silently upscale to `step` — that turned user-typed "7" into wire "100".
        out_reject_msg = "quantity " + std::to_string(quantity) +
                         " below venue minimum_order_size (step=" + std::to_string(step) +
                         " source=" + step_source + ")";
        return false;
    }
    char json_buf[256];
    const int len = snprintf(json_buf, sizeof(json_buf),
        R"({"s":"%s","d":"%s","q":%lld,"p":"%s","tif":"GTC","po":false})",
        ax_symbol.c_str(),
        side == core::Side::BUY ? "B" : "S",
        static_cast<long long>(contracts),
        std::to_string(price).c_str());
    if (len <= 0 || len >= static_cast<int>(sizeof(json_buf))) {
        out_reject_msg = "place_order json build failed";
        return false;
    }
    out_json.assign(json_buf, static_cast<std::size_t>(len));
    return true;
}

std::string Platform::parsePlaceOrderExchangeId(const std::string& body,
                                                orders::OrderId fallback_order_id) {
    // MIRRORS the single-order handler's response parse. Keep in sync.
    std::string exchange_id;
    try {
        auto j = nlohmann::json::parse(body);
        if (j.contains("oid")) {
            exchange_id = j["oid"].get<std::string>();
        } else if (j.contains("order_id")) {
            exchange_id = j["order_id"].get<std::string>();
        } else if (j.contains("id")) {
            exchange_id = j["id"].get<std::string>();
        } else {
            exchange_id = std::to_string(fallback_order_id);
        }
    } catch (...) {
        exchange_id = std::to_string(fallback_order_id);
    }
    return exchange_id;
}

void Platform::buildPlaceBodies(const std::vector<orders::OrderId>& order_ids,
                                std::vector<orders::OrderId>& out_wire_ids,
                                std::vector<std::string>& out_bodies) {
    out_wire_ids.clear();
    out_bodies.clear();
    out_wire_ids.reserve(order_ids.size());
    out_bodies.reserve(order_ids.size());
    for (orders::OrderId id : order_ids) {
        auto order = orders()->getOrder(id);
        if (!order) {
            continue;
        }
        if (order->type != core::OrderType::LIMIT || order->price <= 0) {
            // Match the single-order handler: silently skip non-limit / bad-price.
            continue;
        }
        std::string json_body;
        std::string reject_msg;
        if (!buildPlaceOrderJson(std::string(order->symbol.data()), order->side, order->price,
                                 order->quantity, json_body, reject_msg)) {
            logger()->warn(
                "[BATCH_PLACE] rejected pre-flight: order_id={} side={} sym={} qty={} reason={}",
                id, sideToString(order->side), std::string(order->symbol.data()), order->quantity,
                reject_msg);
            orders()->onOrderRejected(id, core::ErrorCode::INVALID_QUANTITY, reject_msg);
            continue;
        }
        out_wire_ids.push_back(id);
        out_bodies.push_back(std::move(json_body));
    }
}

void Platform::buildCancelBodies(const std::vector<orders::OrderId>& cancel_ids,
                                 std::vector<orders::OrderId>& out_wire_ids,
                                 std::vector<std::string>& out_bodies) {
    out_wire_ids.clear();
    out_bodies.clear();
    out_wire_ids.reserve(cancel_ids.size());
    out_bodies.reserve(cancel_ids.size());
    for (orders::OrderId id : cancel_ids) {
        auto order = orders()->getOrder(id);
        if (!order) {
            continue;
        }
        // Already terminal (filled/cancelled) — nothing to pull. A not-yet-active order has no
        // exchange oid yet; skip (the strategy's reconcile handles those edge cases).
        if (order->exchange_order_id.empty()) {
            continue;
        }
        nlohmann::json body = nlohmann::json::object();
        body["oid"] = order->exchange_order_id;
        out_wire_ids.push_back(id);
        out_bodies.push_back(body.dump());
    }
}

void Platform::dispatchPlaceResponses(const std::vector<orders::OrderId>& wire_ids,
                                      const std::vector<api::HttpResponse>& responses,
                                      long long batch_rtt_us) {
    // Dispatch accept/reject per leg, exactly like the single-order handler's tail. Reserve
    // (in the strategy) happened before any wire fire, so the inline ORDER_ACCEPTED here
    // balances the pending-accept counter regardless of completion order.
    for (std::size_t i = 0; i < wire_ids.size(); ++i) {
        const orders::OrderId id = wire_ids[i];
        const api::HttpResponse& response =
            (i < responses.size()) ? responses[i] : api::HttpResponse{};
        if (response.is_success && response.status_code >= 200 && response.status_code < 300) {
            const std::string exchange_id = parsePlaceOrderExchangeId(response.body, id);
            logger()->info(
                "[BATCH_PLACE] accepted order_id={} exchange_id={} batch_legs={} batch_rtt={}us",
                id, exchange_id, wire_ids.size(), batch_rtt_us);
            orders()->onOrderAccepted(id, exchange_id);
        } else {
            std::string msg = response.error_message.empty() ? response.body : response.error_message;
            if (msg.empty()) {
                msg = "HTTP " + std::to_string(response.status_code);
            }
            logger()->warn("[BATCH_PLACE] REJECTED order_id={} reason={}", id, msg);
            orders()->onOrderRejected(id, core::ErrorCode::UNKNOWN_ERROR, msg);
        }
    }
}

void Platform::dispatchCancelResponses(const std::vector<orders::OrderId>& wire_ids,
                                       const std::vector<api::HttpResponse>& responses) {
    for (std::size_t i = 0; i < wire_ids.size(); ++i) {
        const orders::OrderId id = wire_ids[i];
        const api::HttpResponse& r =
            (i < responses.size()) ? responses[i] : api::HttpResponse{};
        const bool ok = r.is_success && r.status_code >= 200 && r.status_code < 300;
        // A 404 means the venue already dropped it (filled/cancelled/never-propagated) — benign,
        // treat as cancelled locally so we don't leak the local order. Any other hard failure is
        // left OPEN so the strategy's async cancel-timeout reconcile re-sends it.
        const bool benign_gone = (r.status_code == 404);
        if (ok || benign_gone) {
            // The venue already cancelled this leg (our batch sent the HTTP cancel above).
            // Sync local state via onOrderCancelled — it publishes ORDER_CANCELLED with
            // venue_confirmed=true, so the wire-cancel handler does NOT re-send (no double
            // cancel, no double MM_JOB_VENUE_CALLS count). cancelOrder() would have looked
            // like a fresh "please cancel" and re-hit the wire.
            orders()->onOrderCancelled(id);
            logger()->info("[BATCH_CANCEL] order_id={} http={} result={}", id, r.status_code,
                           ok ? "cancelled" : "benign_gone");
        } else {
            std::string msg = r.error_message.empty() ? r.body : r.error_message;
            if (msg.empty()) {
                msg = "HTTP " + std::to_string(r.status_code);
            }
            logger()->warn(
                "[BATCH_CANCEL] FAILED order_id={} http={} reason={} — left open for reconcile",
                id, r.status_code, msg);
        }
    }
}

void Platform::placeOrdersConcurrent(const std::vector<orders::OrderId>& order_ids) {
    if (order_ids.empty()) {
        return;
    }
    std::vector<orders::OrderId> wire_ids;
    std::vector<std::string> bodies;
    buildPlaceBodies(order_ids, wire_ids, bodies);
    if (wire_ids.empty()) {
        return;
    }
    // Fire every leg on the wire CONCURRENTLY — ~1 round-trip for the whole batch.
    const auto t_http_start = std::chrono::steady_clock::now();
    std::vector<api::HttpResponse> responses = rest()->placeOrdersRawConcurrent(bodies);
    const auto t_http_end = std::chrono::steady_clock::now();
    const auto batch_rtt_us =
        std::chrono::duration_cast<std::chrono::microseconds>(t_http_end - t_http_start).count();
    dispatchPlaceResponses(wire_ids, responses, batch_rtt_us);
}

void Platform::cancelOrdersConcurrent(const std::vector<orders::OrderId>& cancel_ids) {
    if (cancel_ids.empty()) {
        return;
    }
    std::vector<orders::OrderId> wire_ids;
    std::vector<std::string> bodies;
    buildCancelBodies(cancel_ids, wire_ids, bodies);
    if (wire_ids.empty()) {
        return;
    }
    std::vector<api::HttpResponse> responses = rest()->cancelOrdersGatewayConcurrent(bodies);
    dispatchCancelResponses(wire_ids, responses);
}

void Platform::placeAndCancelConcurrent(const std::vector<orders::OrderId>& place_ids,
                                        const std::vector<orders::OrderId>& cancel_ids) {
    // Build both body sets first (rejecting invalid places / skipping oid-less cancels inline).
    std::vector<orders::OrderId> place_wire_ids;
    std::vector<std::string> place_bodies;
    buildPlaceBodies(place_ids, place_wire_ids, place_bodies);
    std::vector<orders::OrderId> cancel_wire_ids;
    std::vector<std::string> cancel_bodies;
    buildCancelBodies(cancel_ids, cancel_wire_ids, cancel_bodies);

    if (place_bodies.empty() && cancel_bodies.empty()) {
        return;
    }
    // Nothing to combine → fall back to the single-kind batch (no wasted round-trip).
    if (place_bodies.empty()) {
        std::vector<api::HttpResponse> resp = rest()->cancelOrdersGatewayConcurrent(cancel_bodies);
        dispatchCancelResponses(cancel_wire_ids, resp);
        return;
    }
    if (cancel_bodies.empty()) {
        const auto t0 = std::chrono::steady_clock::now();
        std::vector<api::HttpResponse> resp = rest()->placeOrdersRawConcurrent(place_bodies);
        const auto t1 = std::chrono::steady_clock::now();
        dispatchPlaceResponses(
            place_wire_ids, resp,
            std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count());
        return;
    }

    // Combined c,c,p,p in ONE curl-multi batch (~1 RTT for the whole cancel-replace cycle).
    const auto t_http_start = std::chrono::steady_clock::now();
    std::vector<api::HttpResponse> responses =
        rest()->cancelThenPlaceOrdersConcurrent(cancel_bodies, place_bodies);
    const auto t_http_end = std::chrono::steady_clock::now();
    const auto batch_rtt_us =
        std::chrono::duration_cast<std::chrono::microseconds>(t_http_end - t_http_start).count();

    // Return layout: [0 .. n_cancel) cancels, then [n_cancel .. ) places.
    const std::size_t n_cancel = cancel_bodies.size();
    std::vector<api::HttpResponse> cancel_resp(
        responses.begin(),
        responses.begin() + std::min(n_cancel, responses.size()));
    std::vector<api::HttpResponse> place_resp;
    if (responses.size() > n_cancel) {
        place_resp.assign(responses.begin() + n_cancel, responses.end());
    }
    dispatchCancelResponses(cancel_wire_ids, cancel_resp);
    dispatchPlaceResponses(place_wire_ids, place_resp, batch_rtt_us);
}

std::chrono::seconds Platform::getUptime() const {
    if (!session_clock_started_) {
        return std::chrono::seconds(0);
    }
    return std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::steady_clock::now() - start_time_);
}

Platform::Stats Platform::getStats() const {
    Stats stats;
    stats.uptime = getUptime();
    
    auto event_stats = events()->getStats();
    stats.events_processed = event_stats.events_processed;
    
    auto order_stats = orders()->getStats();
    stats.orders_submitted = order_stats.total_orders;
    stats.orders_filled = order_stats.filled_orders;
    
    auto md_stats = marketdata()->getStats();
    stats.ticks_received = md_stats.ticks_received;

    return stats;
}

void Platform::setHedgeProvider(std::unique_ptr<hedging::IHedgeProvider> provider) {
    hedge_provider_ = std::move(provider);
}

// =============================================================================
// Feed Verification & Detection
// =============================================================================

FeedMode Platform::parseFeedModeString(const std::string& mode_str) const {
    if (mode_str == "live" || mode_str == "LIVE") return FeedMode::LIVE;
    if (mode_str == "paper" || mode_str == "PAPER") return FeedMode::PAPER;
    if (mode_str == "simulation" || mode_str == "SIMULATION") return FeedMode::SIMULATION;
    if (mode_str == "backtest" || mode_str == "BACKTEST") return FeedMode::BACKTEST;
    return FeedMode::UNKNOWN;  // "auto" or unrecognized
}

FeedSource Platform::parseFeedSourceString(const std::string& source_str) const {
    if (source_str == "websocket_live") return FeedSource::WEBSOCKET_LIVE;
    if (source_str == "websocket_delayed") return FeedSource::WEBSOCKET_DELAYED;
    if (source_str == "rest_polling") return FeedSource::REST_POLLING;
    if (source_str == "historical_file") return FeedSource::HISTORICAL_FILE;
    if (source_str == "historical_db") return FeedSource::HISTORICAL_DB;
    if (source_str == "mock") return FeedSource::MOCK;
    return FeedSource::WEBSOCKET_LIVE;
}

void Platform::detectFeedMode() {
    // Get configured mode and source
    std::string mode_str = config()->getFeedMode();
    std::string source_str = config()->getFeedSource();
    
    feed_status_.source = parseFeedSourceString(source_str);
    
    // If historical replay is enabled, force simulation mode
    if (config()->isHistoricalReplayEnabled()) {
        feed_status_.mode = FeedMode::SIMULATION;
        feed_status_.source = FeedSource::HISTORICAL_FILE;
        feed_status_.is_simulation = true;
        feed_status_.is_live = false;
        feed_status_.is_paper = false;
        logger()->info("Feed mode: SIMULATION (historical replay enabled)");
        return;
    }
    
    // Parse mode from config
    FeedMode config_mode = parseFeedModeString(mode_str);
    
    if (config_mode != FeedMode::UNKNOWN) {
        feed_status_.mode = config_mode;
        feed_status_.is_live = (config_mode == FeedMode::LIVE);
        feed_status_.is_paper = (config_mode == FeedMode::PAPER);
        feed_status_.is_simulation = (config_mode == FeedMode::SIMULATION || 
                                       config_mode == FeedMode::BACKTEST);
    } else {
        // Auto-detect based on source
        if (feed_status_.source == FeedSource::WEBSOCKET_LIVE) {
            feed_status_.mode = FeedMode::LIVE;
            feed_status_.is_live = true;
        } else if (feed_status_.source == FeedSource::HISTORICAL_FILE ||
                   feed_status_.source == FeedSource::HISTORICAL_DB) {
            feed_status_.mode = FeedMode::SIMULATION;
            feed_status_.is_simulation = true;
        } else {
            feed_status_.mode = FeedMode::UNKNOWN;
        }
    }
}

bool Platform::verifyFeed() {
    // First detect from config
    detectFeedMode();
    
    // If simulation mode, no need to verify with API
    if (feed_status_.is_simulation) {
        feed_status_.verified = true;
        module_status_.feed_verified = true;
        return true;
    }
    
    // Try to verify with the exchange API
    try {
        auto start_time = std::chrono::steady_clock::now();
        
        // Call whoami to verify connection and check environment
        auto response = rest()->whoami();
        
        auto end_time = std::chrono::steady_clock::now();
        feed_status_.latency_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            end_time - start_time).count();
        
        if (!response.is_success) {
            std::string detail = response.error_message.empty()
                ? ("HTTP " + std::to_string(response.status_code) + (response.body.empty() ? "" : " " + response.body.substr(0, 200)))
                : response.error_message;
            feed_status_.error_message = "API call failed: " + detail;
            feed_status_.verified = false;
            if (response.status_code == 401 || (response.body.find("token not found") != std::string::npos)) {
                logger()->warn("Architect requires a JWT session token, not the raw API key. "
                               "Get a JWT from Architect (dashboard or their auth endpoint) and set it in config: api.session_token");
            }
            return false;
        }
        
        // Parse response to determine environment
        try {
            auto json_response = nlohmann::json::parse(response.body);
            
            // Check for environment indicators in response
            if (json_response.contains("environment")) {
                feed_status_.environment = json_response["environment"].get<std::string>();
                
                if (feed_status_.environment == "production") {
                    feed_status_.mode = FeedMode::LIVE;
                    feed_status_.is_live = true;
                    feed_status_.is_paper = false;
                } else if (feed_status_.environment == "sandbox" || 
                           feed_status_.environment == "paper" ||
                           feed_status_.environment == "testnet") {
                    feed_status_.mode = FeedMode::PAPER;
                    feed_status_.is_live = false;
                    feed_status_.is_paper = true;
                }
            }
            
            // Check for is_close_only flag (indicates paper/test mode)
            if (json_response.contains("is_close_only") && 
                json_response["is_close_only"].get<bool>()) {
                // If close-only, likely paper trading
                feed_status_.is_paper = true;
                if (feed_status_.mode == FeedMode::UNKNOWN || 
                    feed_status_.mode == FeedMode::LIVE) {
                    feed_status_.mode = FeedMode::PAPER;
                }
            }
            
            // If still unknown but we got a valid response, assume live
            if (feed_status_.mode == FeedMode::UNKNOWN) {
                feed_status_.mode = FeedMode::LIVE;
                feed_status_.is_live = true;
            }
            
        } catch (const std::exception& e) {
            logger()->warn("Failed to parse whoami response: {}", e.what());
            // Response was successful but couldn't parse - assume live
            if (feed_status_.mode == FeedMode::UNKNOWN) {
                feed_status_.mode = FeedMode::LIVE;
                feed_status_.is_live = true;
            }
        }
        
        // Try to get market status if available
        try {
            auto market_response = rest()->getMarkets();
            if (market_response.is_success) {
                // Markets API is working, feed is real-time
                logger()->debug("Markets API verified, feed is real-time");
            }
        } catch (...) {
            // Market status not critical
        }
        
        feed_status_.verified = true;
        module_status_.feed_verified = true;
        
        logger()->info("Feed verification successful:");
        logger()->info("  Mode: {}", feedModeToString(feed_status_.mode));
        logger()->info("  Environment: {}", feed_status_.environment.empty() ? "unknown" : feed_status_.environment);
        logger()->info("  Is Live: {}", feed_status_.is_live ? "YES" : "NO");
        logger()->info("  Is Paper: {}", feed_status_.is_paper ? "YES" : "NO");
        logger()->info("  API Latency: {} ms", latencyDisplayMs(feed_status_.latency_ms));
        
        return true;
        
    } catch (const std::exception& e) {
        feed_status_.error_message = std::string("Exception during feed verification: ") + e.what();
        feed_status_.verified = false;
        logger()->error("Feed verification failed: {}", e.what());
        return false;
    }
}

} // namespace core
} // namespace architect
