#include "config/Config.h"
#include "config/AxGatewayInstruments.h"
#include "core/Constants.h"
#include <atomic>
#include <cctype>
#include <chrono>
#include <cstdint>
#include <set>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <mutex>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <system_error>
#include <cmath>
#include <unistd.h>  // getpid (atomic saveToFile unique temp name)

namespace architect {
namespace config {

namespace fs = std::filesystem;

namespace {

std::mutex g_mm_reload_count_persist_mu;

// JSON numbers may parse as float (e.g. 6.0) even when semantically an int; nlohmann's
// is_number_integer() is false for those, which left width_bps at 0 and forced the MM
// engine back to the global spread defaults.
int mmManualStackIntFromJson(const json& stj, const char* key) {
    if (!stj.contains(key)) {
        return 0;
    }
    const auto& v = stj[key];
    if (v.is_number_integer()) {
        return v.get<int>();
    }
    if (v.is_number_float()) {
        return static_cast<int>(std::llround(v.get<double>()));
    }
    return 0;
}

// === reloadPrimaryConfigFromDisk throttling state ===
// Live evidence (logs/20260505 17:37 — multi-MM run): the per-tick disk reload from
// MakeMarketStrategy::onFeedUpdate was serializing 5+ worker threads on the unique_lock
// inside loadFromFileWithOptionalOverlays + merge_patch + 24KB JSON parse, surfacing as
// [FEED_DISPATCH_SLOW] elapsed_ms=250..600 even when no REST was issued. We don't change
// the API surface; we just skip the on-disk re-read when the file mtime is unchanged AND
// the previous reload was within min_interval_ms.
std::mutex g_reload_throttle_mu;
std::chrono::steady_clock::time_point g_last_reload_at = std::chrono::steady_clock::time_point{};
fs::file_time_type g_last_reload_mtime = fs::file_time_type{};
std::string g_last_reload_path;
// Epoch-ms of the primary-config file mtime that the CURRENT in-memory config was last
// loaded from. Stamped into the desk-facing mm_orders.json projection so the desk can prove
// the engine's effective per-instrument gate was derived from a config at least as new as
// its own write (D5 step-2 hold acknowledgement). 0 until the first successful reload.
std::atomic<long long> g_last_reload_file_mtime_ms{0};

// std::filesystem file_time_type -> system_clock epoch ms (C++17 portable-ish conversion).
long long fileTimeToEpochMs(fs::file_time_type ft) {
    const auto sys = std::chrono::time_point_cast<std::chrono::system_clock::duration>(
        ft - fs::file_time_type::clock::now() + std::chrono::system_clock::now());
    return std::chrono::duration_cast<std::chrono::milliseconds>(sys.time_since_epoch()).count();
}

std::vector<std::pair<std::string, std::string>> parse_fix_tag_object(const json& j) {
    std::vector<std::pair<std::string, std::string>> out;
    if (!j.is_object()) {
        return out;
    }
    for (auto it = j.begin(); it != j.end(); ++it) {
        const std::string& key = it.key();
        if (key.empty() || key[0] == '_') {
            continue;
        }
        if (it.value().is_string()) {
            out.emplace_back(key, it.value().get<std::string>());
        } else if (it.value().is_number_integer()) {
            out.emplace_back(key, std::to_string(it.value().get<std::int64_t>()));
        } else if (it.value().is_number_float()) {
            out.emplace_back(key, std::to_string(it.value().get<double>()));
        }
    }
    return out;
}

}  // namespace

Config& Config::getInstance() {
    static Config instance;
    return instance;
}

Config::Config() {
    initDefaults();
}

void Config::initDefaults() {
    using namespace core::constants;
    
    config_ = {
        {"api", {
            {"rest_endpoint", DEFAULT_REST_ENDPOINT},
            {"ws_endpoint", DEFAULT_WS_ENDPOINT},
            {"api_key", ""},
            {"api_secret", ""},
            {"session_token", ""},
            {"timeout_connect_ms", DEFAULT_CONNECT_TIMEOUT.count()},
            {"timeout_read_ms", DEFAULT_READ_TIMEOUT.count()},
            {"timeout_write_ms", DEFAULT_WRITE_TIMEOUT.count()},
            {"fill_poll_cursor_file", "logs/fill_poll_last_trade_id.txt"}
        }},
        {"performance", {
            {"event_queue_size", DEFAULT_EVENT_QUEUE_SIZE},
            {"order_queue_size", DEFAULT_ORDER_QUEUE_SIZE},
            {"market_data_queue_size", DEFAULT_MARKET_DATA_QUEUE_SIZE},
            {"worker_threads", DEFAULT_WORKER_THREADS},
            {"ws_buffer_size", DEFAULT_WS_BUFFER_SIZE},
            {"http_buffer_size", DEFAULT_HTTP_BUFFER_SIZE}
        }},
        {"trading", {
            {"max_orders_per_second", DEFAULT_MAX_ORDERS_PER_SECOND},
            {"max_requests_per_second", DEFAULT_MAX_REQUESTS_PER_SECOND},
            {"default_slippage", 0.001},
            {"price_precision", DEFAULT_PRICE_PRECISION},
            {"quantity_precision", DEFAULT_QUANTITY_PRECISION},
            {"watchlist", json::array()},
            {"feed_guardian_enabled", true},
            {"feed_guardian_require_ws", true},
            {"feed_guardian_require_external", true}
        }},
        {"orderbook", {
            {"default_depth", DEFAULT_ORDER_BOOK_DEPTH},
            {"max_depth", MAX_ORDER_BOOK_DEPTH},
            {"l1_depth", DEFAULT_L1_DEPTH},
            {"l2_depth", DEFAULT_L2_DEPTH}
        }},
        {"external_feed", {
            {"enabled", true},
            {"name", "neon_integral_quote"},
            {"provider", "neon_fix"},
            {"rest_url", ""},
            {"rest_uses_spot_ticker_path", true},
            {"symbol", ""},
            {"display_symbol", ""},
            {"poll_interval_ms", 2000},
            {"consecutive_errors_before_invalidate", 2},
            {"write_desk_theo_cache", true},
            {"desk_theo_cache_path", "logs/mm_external_theo.json"},
            {"write_desk_depth_cache", true},
            {"desk_depth_cache_path", "logs/mm_external_depth.json"},
            {"desk_depth_levels", 20},
            {"desk_depth_write_interval_ms", 1000},
            {"fix", {
                {"host", "127.0.0.1"},
                {"port", 14508},
                {"sender_comp_id", ""},
                {"target_comp_id", ""},
                {"deliver_to_comp_id", ""},
                {"sender_sub_id", ""},
                {"username", ""},
                {"password", ""},
                {"heart_bt_int", 30},
                {"md_update_type", 0},
                {"md_instrument_extra", json{{"460", "4"}, {"167", "FOR"}}},
                {"logon_extra", json::object()},
                {"md_request_root_extra", json::object()},
                {"md_snapshot_only", false},
                {"md_market_depth", 0},
                {"md_symbols", json::array()},
                {"stunnel", {
                    {"autostart", true},
                    {"config", "config/stunnel/integral_quote.example.conf"},
                    {"executable", "stunnel"},
                    {"start_timeout_ms", 15000},
                    {"log_file", "logs/stunnel_autostart.log"}
                }}
            }}
        }},
        {"mettraders_feed", {
            {"enabled", true},
            {"url", "wss://marketdata.mettradersdataservices.com"},
            {"fix_symbol", "GOLD"},
            {"type", "websocket"}
        }},
        {"feed", {
            {"mode", "auto"},
            {"source", "websocket_live"},
            {"default_level", "L1"},
            {"subscribe_trades", true},
            {"subscribe_ticker", true},
            {"l2_depth", DEFAULT_L2_DEPTH},
            {"snapshot_interval_ms", 0},
            {"throttle_ms", 0},
            {"verify_on_startup", true},
            {"historical", {
                {"enabled", false},
                {"data_path", "data/historical"},
                {"start_date", ""},
                {"end_date", ""},
                {"replay_speed", 1.0},
                {"loop_replay", false}
            }}
        }},
        {"websocket", {
            {"heartbeat_interval_ms", DEFAULT_HEARTBEAT_INTERVAL.count()},
            {"reconnect_delay_ms", DEFAULT_RECONNECT_DELAY.count()},
            {"max_reconnect_delay_ms", MAX_RECONNECT_DELAY.count()},
            {"auto_reconnect", true},
            {"ping_pong_enabled", true},
            {"channels", {
                {"orderbook", {{"enabled", true}, {"depth", DEFAULT_L2_DEPTH}, {"throttle_ms", 0}}},
                {"trades", {{"enabled", true}, {"throttle_ms", 0}}},
                {"ticker", {{"enabled", true}, {"throttle_ms", 100}}}
            }}
        }},
        {"logging", {
            {"level", "INFO"},
            {"file", "platform.log"},
            {"console_enabled", true},
            {"file_enabled", false},
            {"format", "[%Y-%m-%d %H:%M:%S.%e] [%l] [%t] %v"}
        }},
        {"features", {
            {"enable_rate_limiting", true},
            {"enable_order_validation", true},
            {"enable_position_tracking", true},
            {"enable_pnl_calculation", true}
        }},
        {"market_maker", {
            {"enabled", true},
            // Default-on so multiple desk stacks on the same AX coexist as independent
            // mm_req_<AX>_<stack_id> strategies (each tracks its own legs and moves per its own
            // width/min_drift). Set to false explicitly to revert to the legacy "single quoter
            // per AX, additional stacks adopt into the base quoter" behavior.
            {"allow_multi_mm_per_ax", true},
            {"multi_theo", false},
            {"symbol", "BTC-USD"},
            {"order_symbol", ""},
            {"order_size_step", 1},
            {"order_size", 1},
            {"quantity", 0.001},
            {"spread_bps", 10},
            {"width_bps", 10},
            {"basis", 0.0},
            {"theo_symbol", ""},
            {"update_interval_sec", 2},
            {"price_tick", 0.0},
            {"spread_ticks", 5},
            {"paper_simulate_fills", false},
            {"max_position", 1000000},
            {"exchange_position_reconcile_sec", 3.5},
            {"adjust_position", 1000},
            {"adjust_ticks", 1},
            {"requote_on_theo_move", true},
            {"mm_orders_enabled", true},
            {"min_theo_move_ticks_to_requote", 2},
            {"requote_on_timer", false},
            {"suppress_periodic_theo_requote_when_at_inventory_cap", false},
            {"feed_log_verbose", false},
            {"feed_log_banner_min_interval_ms", 1500},
            {"post_fill_extra_ticks", 0},
            {"resting_depth_extra_ticks", 0},
            {"validate_vs_market", true},
            {"never_inside_spread", true},
            {"cancel_on_disconnect", true},
            {"cancel_on_external_feed_invalid", true},
            {"max_reload_cycles", 100},
            {"current_reload_count", 0},
            {"reload_cycles_count_fill_only", true},
            {"reload_limit_reset_nonce", ""},
            {"desk_sync_enabled", false},
            {"desk_signal_path", "logs/mm_desk_signal.json"},
            {"orphan_fill_unknown_symbol_reconcile", true}
        }},
        {"hedge", {
            {"enabled", false},
            {"webhook_url", ""}
        }}
    };
}

bool Config::loadFromFile(const std::string& filepath) {
    try {
        std::ifstream file(filepath);
        if (!file.is_open()) {
            return false;
        }
        
        json loaded_config;
        file >> loaded_config;
        
        {
            std::unique_lock<std::shared_mutex> lock(mutex_);
            config_.merge_patch(loaded_config);
        }
        
        return true;
    } catch (const std::exception&) {
        return false;
    }
}

bool Config::loadFromFileWithOptionalOverlays(const std::string& filepath) {
    if (!loadFromFile(filepath)) {
        return false;
    }
    fs::path main_p(filepath);
    fs::path dir = main_p.parent_path();
    if (dir.empty()) {
        dir = fs::current_path();
    }
    static const char* kOverlays[] = {"credentials.local.json", "external_feed.local.json"};
    for (const char* name : kOverlays) {
        const fs::path overlay = dir / name;
        std::error_code ec;
        if (fs::is_regular_file(overlay, ec)) {
            loadFromFile(overlay.string());
        }
    }
    {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        primary_config_path_ = filepath;
    }
    return true;
}

bool Config::reloadPrimaryConfigFromDisk(int min_interval_ms) {
    std::string path_copy;
    {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        path_copy = primary_config_path_;
    }
    if (path_copy.empty()) {
        return false;
    }

    // Throttle: skip the disk read + JSON parse + merge_patch when nothing has actually
    // changed on disk recently. Caller can pass min_interval_ms=0 to force a reload (used
    // by paths that mutate the file and need to re-read their own write).
    if (min_interval_ms > 0) {
        std::error_code ec;
        const auto cur_mtime = fs::last_write_time(path_copy, ec);
        const auto now = std::chrono::steady_clock::now();

        std::lock_guard<std::mutex> lock(g_reload_throttle_mu);
        const bool same_path = (g_last_reload_path == path_copy);
        const bool mtime_known = !ec && (g_last_reload_mtime != fs::file_time_type{});
        const bool mtime_unchanged = mtime_known && (cur_mtime == g_last_reload_mtime);
        const auto since_last = std::chrono::duration_cast<std::chrono::milliseconds>(
                                    now - g_last_reload_at)
                                    .count();
        const bool within_min_interval =
            (g_last_reload_at != std::chrono::steady_clock::time_point{}) &&
            since_last < min_interval_ms;

        // Skip when file is the same, mtime didn't change, AND we reloaded recently.
        // The recency clause means a brand-new process will still do its first reload.
        if (same_path && mtime_unchanged && within_min_interval) {
            return true;  // success — in-memory copy is still authoritative
        }
        // Fall through and reload; record the new mtime + timestamp on success below.
    }

    const bool ok = loadFromFileWithOptionalOverlays(path_copy);
    if (ok) {
        std::error_code ec;
        const auto cur_mtime = fs::last_write_time(path_copy, ec);
        std::lock_guard<std::mutex> lock(g_reload_throttle_mu);
        g_last_reload_at = std::chrono::steady_clock::now();
        g_last_reload_path = path_copy;
        if (!ec) {
            g_last_reload_mtime = cur_mtime;
            g_last_reload_file_mtime_ms.store(fileTimeToEpochMs(cur_mtime),
                                              std::memory_order_relaxed);
        }
    }
    return ok;
}

long long Config::lastPrimaryReloadFileMtimeMs() const {
    return g_last_reload_file_mtime_ms.load(std::memory_order_relaxed);
}

bool Config::loadFromString(const std::string& json_str) {
    try {
        json loaded_config = json::parse(json_str);
        
        {
            std::unique_lock<std::shared_mutex> lock(mutex_);
            config_.merge_patch(loaded_config);
        }
        
        return true;
    } catch (const std::exception&) {
        return false;
    }
}

void Config::merge(const json& config) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    config_.merge_patch(config);
}

bool Config::saveToFile(const std::string& filepath) const {
    // ATOMIC write. This file may be default_config.json — read by the external Python
    // desk (per-instrument mm_orders_enabled gate) AND hand-edited by operators. The old
    // in-place `std::ofstream file(filepath)` truncated the target and streamed the whole
    // ~24KB doc, so any concurrent reader (the desk's read-modify-write of the gate flag)
    // could observe a half-written "valid-json + trailing garbage" file, then atomically
    // republish that truncated version — the exact torn-write class fixed for orders.json.
    // Serialize under the shared lock, write to a per-writer-unique temp in the same
    // directory, then rename() (atomic same-filesystem swap): readers see either the whole
    // old file or the whole new file, never a partial one.
    try {
        std::string payload;
        {
            std::shared_lock<std::shared_mutex> lock(mutex_);
            payload = config_.dump(4);
        }
        fs::path target(filepath);
        std::error_code ec;
        if (target.has_parent_path()) {
            fs::create_directories(target.parent_path(), ec);
        }
        static std::atomic<std::uint64_t> s_ctr{0};
        const std::uint64_t n = s_ctr.fetch_add(1, std::memory_order_relaxed);
        const long long ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                                 std::chrono::steady_clock::now().time_since_epoch())
                                 .count();
        const std::string tmp = filepath + ".tmp." +
                                std::to_string(static_cast<long long>(::getpid())) + "." +
                                std::to_string(ns) + "." + std::to_string(n);
        {
            std::ofstream file(tmp, std::ios::trunc | std::ios::binary);
            if (!file.is_open()) {
                return false;
            }
            file << payload << '\n';
            file.flush();
            if (!file.good()) {
                file.close();
                fs::remove(tmp, ec);
                return false;
            }
        }
        fs::rename(tmp, target, ec);
        if (ec) {
            fs::remove(tmp, ec);
            return false;
        }
        return true;
    } catch (const std::exception&) {
        return false;
    }
}

void Config::reset() {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    initDefaults();
    primary_config_path_.clear();
}

json Config::getValue(const std::string& key) const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    // Support nested keys with dot notation: "api.rest_endpoint"
    std::istringstream iss(key);
    std::string token;
    const json* current = &config_;
    
    while (std::getline(iss, token, '.')) {
        if (current->contains(token)) {
            current = &(*current)[token];
        } else {
            return json(nullptr);
        }
    }
    
    return *current;
}

void Config::setValue(const std::string& key, const json& value) {
    // Support nested keys with dot notation
    std::istringstream iss(key);
    std::string token;
    std::vector<std::string> tokens;
    
    while (std::getline(iss, token, '.')) {
        tokens.push_back(token);
    }
    
    if (tokens.empty()) return;
    
    json* current = &config_;
    for (size_t i = 0; i < tokens.size() - 1; ++i) {
        if (!current->contains(tokens[i])) {
            (*current)[tokens[i]] = json::object();
        }
        current = &(*current)[tokens[i]];
    }
    
    (*current)[tokens.back()] = value;
}

void Config::notifyChange(const std::string& key, const json& old_val, const json& new_val) {
    for (const auto& callback : change_callbacks_) {
        if (callback) {
            callback(key, old_val, new_val);
        }
    }
}

std::string Config::getString(const std::string& key, const std::string& default_value) const {
    return get<std::string>(key, default_value);
}

int Config::getInt(const std::string& key, int default_value) const {
    return get<int>(key, default_value);
}

double Config::getDouble(const std::string& key, double default_value) const {
    return get<double>(key, default_value);
}

bool Config::getBool(const std::string& key, bool default_value) const {
    return get<bool>(key, default_value);
}

std::vector<std::string> Config::getStringArray(const std::string& key) const {
    try {
        json val = getValue(key);
        if (val.is_array()) {
            return val.get<std::vector<std::string>>();
        }
    } catch (...) {}
    return {};
}

json Config::getSection(const std::string& key) const {
    return getValue(key);
}

bool Config::has(const std::string& key) const {
    return !getValue(key).is_null();
}

// API Configuration
std::string Config::getRestEndpoint() const {
    return getString("api.rest_endpoint", core::constants::DEFAULT_REST_ENDPOINT);
}

std::string Config::getWebSocketEndpoint() const {
    return getString("api.ws_endpoint", core::constants::DEFAULT_WS_ENDPOINT);
}

std::string Config::getApiKey() const {
    return getString("api.api_key", "");
}

std::string Config::getApiSecret() const {
    return getString("api.api_secret", "");
}

std::string Config::getSessionToken() const {
    return getString("api.session_token", "");
}

// Performance Configuration
std::size_t Config::getEventQueueSize() const {
    return static_cast<std::size_t>(getInt("performance.event_queue_size", 
        static_cast<int>(core::constants::DEFAULT_EVENT_QUEUE_SIZE)));
}

std::size_t Config::getOrderQueueSize() const {
    return static_cast<std::size_t>(getInt("performance.order_queue_size",
        static_cast<int>(core::constants::DEFAULT_ORDER_QUEUE_SIZE)));
}

std::size_t Config::getMarketDataQueueSize() const {
    return static_cast<std::size_t>(getInt("performance.market_data_queue_size",
        static_cast<int>(core::constants::DEFAULT_MARKET_DATA_QUEUE_SIZE)));
}

std::size_t Config::getWorkerThreads() const {
    return static_cast<std::size_t>(getInt("performance.worker_threads",
        static_cast<int>(core::constants::DEFAULT_WORKER_THREADS)));
}

// Trading Configuration
int Config::getMaxOrdersPerSecond() const {
    return getInt("trading.max_orders_per_second", 
        core::constants::DEFAULT_MAX_ORDERS_PER_SECOND);
}

int Config::getMaxRequestsPerSecond() const {
    return getInt("trading.max_requests_per_second",
        core::constants::DEFAULT_MAX_REQUESTS_PER_SECOND);
}

double Config::getDefaultSlippage() const {
    return getDouble("trading.default_slippage", 0.001);
}

std::vector<std::string> Config::getWatchlist() const {
    return getStringArray("trading.watchlist");
}

std::vector<std::string> Config::getMarketDataSubscriptionSymbols() const {
    std::vector<std::string> out = getStringArray("trading.watchlist");
    auto add = [&out](const std::string& s) {
        if (s.empty()) {
            return;
        }
        for (const auto& x : out) {
            if (x == s) {
                return;
            }
        }
        out.push_back(s);
    };
    if (isMarketMakerEnabled()) {
        if (hasMarketMakerInstrumentList()) {
            for (const auto& leg : getMarketMakerInstruments()) {
                std::string os = leg.order_symbol;
                if (os.empty()) {
                    os = leg.ax_symbol;
                }
                add(os);
            }
        } else {
            std::string os = getMarketMakerOrderSymbol();
            if (os.empty()) {
                os = getMarketMakerSymbol();
            }
            add(os);
        }
    }
    return out;
}

bool Config::hasMarketMakerInstrumentList() const {
    json arr = getValue("market_maker.instruments");
    return arr.is_array() && !arr.empty();
}

std::vector<MarketMakerInstrumentLeg> Config::getMarketMakerInstruments() const {
    std::vector<MarketMakerInstrumentLeg> out;
    json arr = getValue("market_maker.instruments");
    if (!arr.is_array()) {
        return out;
    }
    for (const auto& el : arr) {
        if (!el.is_object()) {
            continue;
        }
        MarketMakerInstrumentLeg L;
        if (el.contains("symbol") && el["symbol"].is_string()) {
            L.ax_symbol = el["symbol"].get<std::string>();
        } else if (el.contains("ax_symbol") && el["ax_symbol"].is_string()) {
            L.ax_symbol = el["ax_symbol"].get<std::string>();
        }
        if (el.contains("order_symbol") && el["order_symbol"].is_string()) {
            L.order_symbol = el["order_symbol"].get<std::string>();
        }
        if (el.contains("reference_fix_symbol") && el["reference_fix_symbol"].is_string()) {
            L.reference_fix_symbol = el["reference_fix_symbol"].get<std::string>();
        } else if (el.contains("theo_symbol") && el["theo_symbol"].is_string()) {
            L.reference_fix_symbol = el["theo_symbol"].get<std::string>();
        }
        if (el.contains("theo_source") && el["theo_source"].is_string()) {
            L.theo_source = el["theo_source"].get<std::string>();
        }
        if (el.contains("theo_venue_symbol") && el["theo_venue_symbol"].is_string()) {
            L.theo_venue_symbol = el["theo_venue_symbol"].get<std::string>();
        }
        // Product-level max_position cap (preferred over per-stack max_position).
        // Accept either "max_position" or shorthand "max_po".
        if (el.contains("max_position") && el["max_position"].is_number_integer()) {
            L.max_position = el["max_position"].get<int>();
        } else if (el.contains("max_po") && el["max_po"].is_number_integer()) {
            L.max_position = el["max_po"].get<int>();
        }
        // Product-level max_reload cap.
        if (el.contains("max_reload_cycles") && el["max_reload_cycles"].is_number_integer()) {
            L.max_reload_cycles = el["max_reload_cycles"].get<int>();
        } else if (el.contains("max_reload") && el["max_reload"].is_number_integer()) {
            L.max_reload_cycles = el["max_reload"].get<int>();
        }
        // Per-leg quote tick — accept either "tick_size" or shorthand "tick" (also legacy "price_tick").
        if (el.contains("tick_size") && el["tick_size"].is_number()) {
            L.tick_size = el["tick_size"].get<double>();
        } else if (el.contains("tick") && el["tick"].is_number()) {
            L.tick_size = el["tick"].get<double>();
        } else if (el.contains("price_tick") && el["price_tick"].is_number()) {
            L.tick_size = el["price_tick"].get<double>();
        }
        if (el.contains("theo_scale") && el["theo_scale"].is_number()) {
            L.theo_scale = el["theo_scale"].get<double>();
        }
        {
            int dw = mmManualStackIntFromJson(el, "default_width_bps");
            if (dw <= 0) {
                dw = mmManualStackIntFromJson(el, "instrument_width_bps");
            }
            if (dw > 0) {
                L.default_width_bps = dw;
            }
        }
        if (el.contains("order_size_step") && el["order_size_step"].is_number_integer()) {
            L.order_size_step = el["order_size_step"].get<int>();
        } else if (el.contains("order_step") && el["order_step"].is_number_integer()) {
            L.order_size_step = el["order_step"].get<int>();
        } else if (el.contains("contract_step") && el["contract_step"].is_number_integer()) {
            L.order_size_step = el["contract_step"].get<int>();
        }
        // MWR (Moving Window Range) volatility-breaker params — accept canonical keys and the
        // operator-facing aliases (SizeOfMove / TimeOfMove / PullDuration).
        if (el.contains("mwr_size_ticks") && el["mwr_size_ticks"].is_number_integer()) {
            L.mwr_size_ticks = el["mwr_size_ticks"].get<int>();
        } else if (el.contains("SizeOfMove") && el["SizeOfMove"].is_number_integer()) {
            L.mwr_size_ticks = el["SizeOfMove"].get<int>();
        }
        if (el.contains("mwr_window_sec") && el["mwr_window_sec"].is_number_integer()) {
            L.mwr_window_sec = el["mwr_window_sec"].get<int>();
        } else if (el.contains("TimeOfMove") && el["TimeOfMove"].is_number_integer()) {
            L.mwr_window_sec = el["TimeOfMove"].get<int>();
        }
        if (el.contains("mwr_pull_sec") && el["mwr_pull_sec"].is_number_integer()) {
            L.mwr_pull_sec = el["mwr_pull_sec"].get<int>();
        } else if (el.contains("PullDuration") && el["PullDuration"].is_number_integer()) {
            L.mwr_pull_sec = el["PullDuration"].get<int>();
        }
        if (el.contains("manual_stacks") && el["manual_stacks"].is_array()) {
            for (const auto& stj : el["manual_stacks"]) {
                if (!stj.is_object()) {
                    continue;
                }
                MarketMakerManualStack S;
                if (stj.contains("id") && stj["id"].is_string()) {
                    S.id = stj["id"].get<std::string>();
                } else {
                    S.id = "";
                }
                if (S.id.empty() && stj.contains("stack_id") && stj["stack_id"].is_string()) {
                    S.id = stj["stack_id"].get<std::string>();
                }
                {
                    const int w0 = mmManualStackIntFromJson(stj, "width_bps");
                    if (w0 > 0) {
                        S.width_bps = w0;
                    }
                }
                if (S.width_bps <= 0) {
                    const int w1 = mmManualStackIntFromJson(stj, "width");
                    if (w1 > 0) {
                        S.width_bps = w1;
                    }
                }
                if (S.width_bps <= 0) {
                    const int w2 = mmManualStackIntFromJson(stj, "width_ticks");
                    if (w2 > 0) {
                        S.width_bps = w2;
                    }
                }
                S.width_ticks = S.width_bps;
                if (stj.contains("order_size") && stj["order_size"].is_number_integer()) {
                    S.order_size = stj["order_size"].get<int>();
                }
                if (stj.contains("order_size_step") && stj["order_size_step"].is_number_integer()) {
                    S.order_size_step = stj["order_size_step"].get<int>();
                } else if (stj.contains("step") && stj["step"].is_number_integer()) {
                    S.order_size_step = stj["step"].get<int>();
                }
                if (stj.contains("max_position") && stj["max_position"].is_number_integer()) {
                    S.max_position = stj["max_position"].get<int>();
                } else if (stj.contains("max_po") && stj["max_po"].is_number_integer()) {
                    S.max_position = stj["max_po"].get<int>();
                }
                if (stj.contains("adjust_position") && stj["adjust_position"].is_number_integer()) {
                    S.adjust_position = stj["adjust_position"].get<int>();
                } else if (stj.contains("adj_position") && stj["adj_position"].is_number_integer()) {
                    S.adjust_position = stj["adj_position"].get<int>();
                } else if (stj.contains("adj_po") && stj["adj_po"].is_number_integer()) {
                    S.adjust_position = stj["adj_po"].get<int>();
                }
                if (stj.contains("adjust_ticks") && stj["adjust_ticks"].is_number_integer()) {
                    S.adjust_ticks = stj["adjust_ticks"].get<int>();
                } else if (stj.contains("adj_ticks") && stj["adj_ticks"].is_number_integer()) {
                    S.adjust_ticks = stj["adj_ticks"].get<int>();
                }
                if (stj.contains("min_theo_move_ticks_to_requote") && stj["min_theo_move_ticks_to_requote"].is_number_integer()) {
                    S.min_theo_move_ticks_to_requote = stj["min_theo_move_ticks_to_requote"].get<int>();
                } else if (stj.contains("min_drift_ticks") && stj["min_drift_ticks"].is_number_integer()) {
                    S.min_theo_move_ticks_to_requote = stj["min_drift_ticks"].get<int>();
                }
                if (stj.contains("max_reload_cycles") && stj["max_reload_cycles"].is_number_integer()) {
                    S.max_reload_cycles = stj["max_reload_cycles"].get<int>();
                }
                // Pricer-snapshot linear transform (per-stack). See MarketMakerManualStack docstring
                // in Config.h for the formula and the disabled-when-zero semantics.
                // Read as `is_number()` (not is_number_integer) because these are floating-point
                // anchors written by the desk as e.g. 7280.7 — JSON parsing keeps them as doubles.
                if (stj.contains("quote_snapshot") && stj["quote_snapshot"].is_number()) {
                    S.quote_snapshot = stj["quote_snapshot"].get<double>();
                }
                if (stj.contains("pricer_snapshot") && stj["pricer_snapshot"].is_number()) {
                    S.pricer_snapshot = stj["pricer_snapshot"].get<double>();
                }
                if (stj.contains("slope") && stj["slope"].is_number()) {
                    S.slope = stj["slope"].get<double>();
                }
                if (stj.contains("desk_seeded") && stj["desk_seeded"].is_boolean()) {
                    S.desk_seeded = stj["desk_seeded"].get<bool>();
                }
                if (stj.contains("bid_exchange_oid") && stj["bid_exchange_oid"].is_string()) {
                    S.bid_exchange_oid = stj["bid_exchange_oid"].get<std::string>();
                }
                if (stj.contains("ask_exchange_oid") && stj["ask_exchange_oid"].is_string()) {
                    S.ask_exchange_oid = stj["ask_exchange_oid"].get<std::string>();
                }
                if (stj.contains("bid_price") && stj["bid_price"].is_number()) {
                    S.placed_bid_price = stj["bid_price"].get<double>();
                }
                if (stj.contains("ask_price") && stj["ask_price"].is_number()) {
                    S.placed_ask_price = stj["ask_price"].get<double>();
                }
                if (stj.contains("bid_qty") && stj["bid_qty"].is_number_integer()) {
                    S.placed_bid_qty = stj["bid_qty"].get<int>();
                } else if (stj.contains("bid_quantity") && stj["bid_quantity"].is_number()) {
                    S.placed_bid_qty = static_cast<int>(stj["bid_quantity"].get<double>());
                }
                if (stj.contains("ask_qty") && stj["ask_qty"].is_number_integer()) {
                    S.placed_ask_qty = stj["ask_qty"].get<int>();
                } else if (stj.contains("ask_quantity") && stj["ask_quantity"].is_number()) {
                    S.placed_ask_qty = static_cast<int>(stj["ask_quantity"].get<double>());
                }
                if (S.order_size_step <= 0 && L.order_size_step > 0) {
                    S.order_size_step = L.order_size_step;
                }
                if (!S.id.empty() || S.width_bps > 0 || S.order_size > 0) {
                    L.manual_stacks.push_back(S);
                }
            }
        }
        if (L.ax_symbol.empty()) {
            continue;
        }
        out.push_back(std::move(L));
    }
    return out;
}

namespace {

std::string normalizeTheoSourceKey(std::string t) {
    for (char& c : t) {
        c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    }
    if (t == "neon" || t == "integral_fix" || t == "fix_integral" || t == "fix_neon") {
        return "neon_fix";
    }
    if (t == "hl" || t == "hyper_liquid") {
        return "hyperliquid";
    }
    if (t == "mettraders" || t == "met_traders" || t == "met-traders" || t == "cme" ||
        t == "cme_gold" || t == "cme_metal") {
        return "mettraders";
    }
    return t;
}

} // namespace

std::string Config::getResolvedTheoSourceForLeg(const MarketMakerInstrumentLeg& leg) const {
    if (!leg.theo_source.empty()) {
        return normalizeTheoSourceKey(leg.theo_source);
    }
    return normalizeTheoSourceKey(getExternalFeedProvider());
}

bool Config::isMultiTheoFeeds() const {
    if (getBool("market_maker.multi_theo", false) && hasMarketMakerInstrumentList()) {
        return true;
    }
    if (!hasMarketMakerInstrumentList()) {
        return false;
    }
    std::set<std::string> kinds;
    for (const auto& leg : getMarketMakerInstruments()) {
        kinds.insert(getResolvedTheoSourceForLeg(leg));
    }
    return kinds.size() > 1U;
}

bool Config::getMettradersFeedEnabled() const {
    return getBool("mettraders_feed.enabled", true);
}

std::string Config::getMettradersFeedUrl() const {
    return getString("mettraders_feed.url", "wss://marketdata.mettradersdataservices.com");
}

std::string Config::getMettradersFeedFixSymbol() const {
    return getString("mettraders_feed.fix_symbol", "GOLD");
}

// Logging Configuration
std::string Config::getLogLevel() const {
    return getString("logging.level", "INFO");
}

std::string Config::getLogFile() const {
    return getString("logging.file", "platform.log");
}

bool Config::isConsoleLogging() const {
    return getBool("logging.console_enabled", true);
}

bool Config::isFileLogging() const {
    return getBool("logging.file_enabled", false);
}

// Feed Configuration
std::string Config::getFeedMode() const {
    return getString("feed.mode", "auto");
}

std::string Config::getFeedSource() const {
    return getString("feed.source", "websocket_live");
}

std::string Config::getDefaultSubscriptionLevel() const {
    return getString("feed.default_level", "L1");
}

int Config::getL2Depth() const {
    return getInt("feed.l2_depth", 25);
}

bool Config::shouldVerifyFeedOnStartup() const {
    return getBool("feed.verify_on_startup", true);
}

bool Config::isHistoricalReplayEnabled() const {
    return getBool("feed.historical.enabled", false);
}

std::string Config::getHistoricalDataPath() const {
    return getString("feed.historical.data_path", "data/historical");
}

double Config::getReplaySpeed() const {
    return getDouble("feed.historical.replay_speed", 1.0);
}

json Config::getWebSocketChannelConfig(const std::string& channel) const {
    return getSection("websocket.channels." + channel);
}

// External feed (theo pricing: neon_fix or REST bookTicker when rest_url is set)
bool Config::isExternalFeedEnabled() const {
    return getBool("external_feed.enabled", false);
}

std::string Config::getExternalFeedProvider() const {
    return getString("external_feed.provider", "neon_fix");
}

std::string Config::getExternalFeedRestUrl() const {
    return getString("external_feed.rest_url", "");
}

bool Config::getExternalFeedRestUsesSpotTickerPath() const {
    return getBool("external_feed.rest_uses_spot_ticker_path", true);
}

std::string Config::getExternalFeedSymbol() const {
    return getString("external_feed.symbol", "");
}

std::string Config::getExternalFeedDisplaySymbol() const {
    return getString("external_feed.display_symbol", "");
}

int Config::getExternalFeedPollIntervalMs() const {
    return getInt("external_feed.poll_interval_ms", 2000);
}

int Config::getExternalFeedConsecutiveErrorsBeforeInvalidate() const {
    int v = getInt("external_feed.consecutive_errors_before_invalidate", 2);
    return v > 0 ? v : 0;
}

bool Config::getExternalFeedWriteDeskDepthCache() const {
    return getBool("external_feed.write_desk_depth_cache", true);
}

std::string Config::getExternalFeedDeskDepthCachePath() const {
    return getString("external_feed.desk_depth_cache_path", "logs/mm_external_depth.json");
}

int Config::getExternalFeedDeskDepthLevels() const {
    return getInt("external_feed.desk_depth_levels", 20);
}

int Config::getExternalFeedDeskDepthWriteIntervalMs() const {
    return getInt("external_feed.desk_depth_write_interval_ms", 1000);
}

std::string Config::getExternalFeedFixHost() const {
    return getString("external_feed.fix.host", "127.0.0.1");
}

int Config::getExternalFeedFixPort() const {
    return getInt("external_feed.fix.port", 14508);
}

std::string Config::getExternalFeedFixSenderCompId() const {
    return getString("external_feed.fix.sender_comp_id", "");
}

std::string Config::getExternalFeedFixTargetCompId() const {
    return getString("external_feed.fix.target_comp_id", "");
}

std::string Config::getExternalFeedFixDeliverToCompId() const {
    return getString("external_feed.fix.deliver_to_comp_id", "");
}

std::string Config::getExternalFeedFixSenderSubId() const {
    return getString("external_feed.fix.sender_sub_id", "");
}

std::string Config::getExternalFeedFixUsername() const {
    return getString("external_feed.fix.username", "");
}

std::string Config::getExternalFeedFixPassword() const {
    return getString("external_feed.fix.password", "");
}

int Config::getExternalFeedFixHeartBtInt() const {
    return getInt("external_feed.fix.heart_bt_int", 30);
}

int Config::getExternalFeedFixMdUpdateType() const {
    return getInt("external_feed.fix.md_update_type", 0);
}

std::vector<std::pair<std::string, std::string>> Config::getExternalFeedFixMdInstrumentExtra() const {
    return parse_fix_tag_object(getValue("external_feed.fix.md_instrument_extra"));
}

std::vector<std::pair<std::string, std::string>> Config::getExternalFeedFixLogonExtra() const {
    return parse_fix_tag_object(getValue("external_feed.fix.logon_extra"));
}

std::vector<std::pair<std::string, std::string>> Config::getExternalFeedFixMdRequestRootExtra() const {
    return parse_fix_tag_object(getValue("external_feed.fix.md_request_root_extra"));
}

bool Config::getExternalFeedFixMdSnapshotOnly() const {
    return getBool("external_feed.fix.md_snapshot_only", false);
}

int Config::getExternalFeedFixMdMarketDepth() const {
    return getInt("external_feed.fix.md_market_depth", 1);
}

std::vector<std::string> Config::getExternalFeedFixMdSymbols() const {
    json v = getValue("external_feed.fix.md_symbols");
    std::vector<std::string> out;
    if (v.is_array()) {
        for (const auto& x : v) {
            if (x.is_string()) {
                out.push_back(x.get<std::string>());
            }
        }
    }
    if (out.empty()) {
        const std::string fb = getExternalFeedSymbol();
        if (!fb.empty()) {
            out.push_back(fb);
        }
    }
    return out;
}

bool Config::getExternalFeedFixStunnelAutostart() const {
    return getBool("external_feed.fix.stunnel.autostart", true);
}

std::string Config::getExternalFeedFixStunnelConfig() const {
    return getString("external_feed.fix.stunnel.config", "");
}

std::string Config::getExternalFeedFixStunnelExecutable() const {
    return getString("external_feed.fix.stunnel.executable", "stunnel");
}

int Config::getExternalFeedFixStunnelStartTimeoutMs() const {
    return getInt("external_feed.fix.stunnel.start_timeout_ms", 15000);
}

std::string Config::getExternalFeedFixStunnelLogFile() const {
    return getString("external_feed.fix.stunnel.log_file", "logs/stunnel_autostart.log");
}

bool Config::isMarketMakerEnabled() const {
    return getBool("market_maker.enabled", false);
}

std::string Config::getMarketMakerSymbol() const {
    return getString("market_maker.symbol", "BTC-USD");
}

std::string Config::getMarketMakerOrderSymbol() const {
    return getString("market_maker.order_symbol", "");
}

int Config::getMarketMakerOrderSizeStep() const {
    int step = getInt("market_maker.order_size_step", 1);
    return step > 0 ? step : 1;
}

double Config::getMarketMakerQuantity() const {
    // Prefer integer order_size if provided, fallback to legacy quantity.
    if (has("market_maker.order_size")) {
        int sz = getInt("market_maker.order_size", 1);
        if (sz > 0) return static_cast<double>(sz);
    }
    return getDouble("market_maker.quantity", 0.001);
}

int Config::getMarketMakerSpreadBps() const {
    return getInt("market_maker.spread_bps", 10);
}

int Config::getMarketMakerSpreadTicks() const {
    // Prefer "width_bps" (user-facing), fallback to legacy width/spread_ticks.
    if (has("market_maker.width_bps")) {
        int v = getInt("market_maker.width_bps", 10);
        return v > 0 ? v : 10;
    }
    if (has("market_maker.width")) {
        int v = getInt("market_maker.width", 10);
        return v > 0 ? v : 10;
    }
    int v = getInt("market_maker.spread_ticks", 10);
    return v > 0 ? v : 10;
}

int Config::getMarketMakerBidSpreadTicks() const {
    const int fallback = getMarketMakerSpreadTicks();
    if (has("market_maker.bid_width_bps")) {
        int v = getInt("market_maker.bid_width_bps", fallback);
        return v > 0 ? v : fallback;
    }
    if (has("market_maker.bid_width")) {
        int v = getInt("market_maker.bid_width", fallback);
        return v > 0 ? v : fallback;
    }
    return fallback;
}

int Config::getMarketMakerAskSpreadTicks() const {
    const int fallback = getMarketMakerSpreadTicks();
    if (has("market_maker.ask_width_bps")) {
        int v = getInt("market_maker.ask_width_bps", fallback);
        return v > 0 ? v : fallback;
    }
    if (has("market_maker.ask_width")) {
        int v = getInt("market_maker.ask_width", fallback);
        return v > 0 ? v : fallback;
    }
    return fallback;
}

int Config::getMarketMakerDefaultWidthBpsForAx(const std::string& ax_symbol) const {
    if (ax_symbol.empty()) {
        return 0;
    }
    for (const auto& leg : getMarketMakerInstruments()) {
        if (leg.ax_symbol != ax_symbol) {
            continue;
        }
        if (leg.default_width_bps > 0) {
            return leg.default_width_bps;
        }
        if (!leg.manual_stacks.empty() && leg.manual_stacks[0].width_bps > 0) {
            return leg.manual_stacks[0].width_bps;
        }
        return 0;
    }
    return 0;
}

int Config::getMarketMakerDeskWidthEmergencyFloorBps() const {
    int v = getInt("market_maker.desk_width_emergency_floor_bps", 6);
    if (v < 1) {
        v = 1;
    }
    return v;
}

double Config::getMarketMakerBasis() const {
    return getDouble("market_maker.basis", 0.0);
}

std::string Config::getMarketMakerTheoSymbol() const {
    return getString("market_maker.theo_symbol", "");
}

int Config::getMarketMakerUpdateIntervalSec() const {
    return getInt("market_maker.update_interval_sec", 2);
}

double Config::getMarketMakerPriceTick() const {
    return getDouble("market_maker.price_tick", 0.0);
}

double Config::getMarketMakerPricingTick() const {
    // Tick size for the pricing instrument (may differ from quote tick)
    // Default to same as price_tick if not specified
    return getDouble("market_maker.pricing_tick", getMarketMakerPriceTick());
}

bool Config::getMarketMakerPaperSimulateFills() const {
    return getBool("market_maker.paper_simulate_fills", false);
}

bool Config::getMarketMakerReloadQty() const {
    return getBool("market_maker.reload_qty", false);
}

bool Config::getMarketMakerRequoteOnTheoMove() const {
    return getBool("market_maker.requote_on_theo_move", true);
}

namespace {

std::string normalizeMmSymbolKey(const std::string& s) {
    std::string o;
    o.reserve(s.size());
    for (unsigned char c : s) {
        if (std::isspace(c)) {
            continue;
        }
        o.push_back(static_cast<char>(std::toupper(c)));
    }
    return o;
}

std::optional<bool> mmInstrumentBoolOverride(const nlohmann::json& arr,
                                             const std::string& ax_symbol,
                                             const char* key) {
    if (!arr.is_array()) {
        return std::nullopt;
    }
    const std::string want = normalizeMmSymbolKey(ax_symbol);
    if (want.empty()) {
        return std::nullopt;
    }
    for (const auto& el : arr) {
        if (!el.is_object()) {
            continue;
        }
        std::string sym;
        if (el.contains("symbol") && el["symbol"].is_string()) {
            sym = el["symbol"].get<std::string>();
        } else if (el.contains("ax_symbol") && el["ax_symbol"].is_string()) {
            sym = el["ax_symbol"].get<std::string>();
        }
        if (normalizeMmSymbolKey(sym) != want) {
            continue;
        }
        if (el.contains(key) && el[key].is_boolean()) {
            return el[key].get<bool>();
        }
        break;
    }
    return std::nullopt;
}

} // namespace

bool Config::getMarketMakerRequoteOnTheoMoveForSymbol(const std::string& ax_symbol) const {
    const auto ov = mmInstrumentBoolOverride(getValue("market_maker.instruments"), ax_symbol,
                                             "requote_on_theo_move");
    return ov.has_value() ? *ov : getMarketMakerRequoteOnTheoMove();
}

bool Config::getMarketMakerMmOrdersEnabled() const {
    return getBool("market_maker.mm_orders_enabled", true);
}

bool Config::getMarketMakerMmOrdersEnabledForSymbol(const std::string& ax_symbol) const {
    const auto ov = mmInstrumentBoolOverride(getValue("market_maker.instruments"), ax_symbol,
                                             "mm_orders_enabled");
    return ov.has_value() ? *ov : getMarketMakerMmOrdersEnabled();
}

int Config::getMarketMakerMinTheoMoveTicksToRequote() const {
    int v = getInt("market_maker.min_theo_move_ticks_to_requote", 1);
    if (v < 1) {
        return 1;
    }
    if (v > 1'000'000) {
        return 1'000'000;
    }
    return v;
}

int Config::getMarketMakerCancelAckTimeoutMs() const {
    int v = getInt("market_maker.cancel_ack_timeout_ms", 3000);
    if (v < 100) {
        return 100;
    }
    if (v > 600'000) {
        return 600'000;
    }
    return v;
}

int Config::getMarketMakerShutdownCancelTimeoutMs() const {
    int v = getInt("market_maker.shutdown_cancel_timeout_ms", 5000);
    if (v < 100) {
        return 100;
    }
    if (v > 600'000) {
        return 600'000;
    }
    return v;
}

bool Config::getMarketMakerRequoteOnTimer() const {
    return getBool("market_maker.requote_on_timer", false);
}

bool Config::getMarketMakerSuppressPeriodicTheoRequoteWhenAtInventoryCap() const {
    // Default false: MakeMarketStrategy no longer consults this — inventory cap uses one-sided reduce + open-aware legs.
    return getBool("market_maker.suppress_periodic_theo_requote_when_at_inventory_cap", false);
}

bool Config::getMarketMakerFeedLogVerbose() const {
    return getBool("market_maker.feed_log_verbose", false);
}

int Config::getMarketMakerFeedLogBannerMinIntervalMs() const {
    const int v = getInt("market_maker.feed_log_banner_min_interval_ms", 1500);
    return v < 0 ? 0 : v;
}

int Config::getMarketMakerPostFillExtraTicks() const {
    int v = getInt("market_maker.post_fill_extra_ticks", 0);
    return v > 0 ? v : 0;
}

int Config::getMarketMakerRestingDepthExtraTicks() const {
    int v = getInt("market_maker.resting_depth_extra_ticks", 0);
    return v >= 0 ? v : 0;
}

bool Config::getMarketMakerValidateVsMarket() const {
    return getBool("market_maker.validate_vs_market", true);
}

bool Config::getMarketMakerNeverInsideSpread() const {
    return getBool("market_maker.never_inside_spread", true);
}

bool Config::getMarketMakerCancelOnDisconnect() const {
    return getBool("market_maker.cancel_on_disconnect", true);
}

bool Config::getMarketMakerCancelOnExternalFeedInvalid() const {
    return getBool("market_maker.cancel_on_external_feed_invalid", true);
}

int Config::getMarketMakerMaxReloadCycles() const {
    int v = getInt("market_maker.max_reload_cycles", 100);
    return v > 0 ? v : 0;
}

int Config::getMarketMakerCurrentReloadCount() const {
    return getInt("market_maker.current_reload_count", 0);
}

bool Config::persistPrimaryConfigMarketMakerCurrentReloadCountLocked(int clamped) {
    std::string path_copy;
    {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        path_copy = primary_config_path_;
    }
    if (path_copy.empty()) {
        set("market_maker.current_reload_count", clamped);
        return true;
    }
    try {
        json j;
        {
            std::ifstream ifs(path_copy);
            if (!ifs) {
                set("market_maker.current_reload_count", clamped);
                return false;
            }
            ifs >> j;
        }
        if (!j.is_object()) {
            set("market_maker.current_reload_count", clamped);
            return false;
        }
        if (!j.contains("market_maker") || !j["market_maker"].is_object()) {
            j["market_maker"] = json::object();
        }
        j["market_maker"]["current_reload_count"] = clamped;
        const std::string tmp = path_copy + ".tmp";
        {
            std::ofstream ofs(tmp, std::ios::binary | std::ios::trunc);
            if (!ofs) {
                set("market_maker.current_reload_count", clamped);
                return false;
            }
            ofs << std::setw(4) << j << "\n";
        }
        std::error_code ec;
        fs::rename(tmp, path_copy, ec);
        if (ec) {
            set("market_maker.current_reload_count", clamped);
            return false;
        }
    } catch (...) {
        set("market_maker.current_reload_count", clamped);
        return false;
    }
    set("market_maker.current_reload_count", clamped);
    return true;
}

bool Config::persistPrimaryConfigMarketMakerCurrentReloadCount(int count) {
    std::lock_guard<std::mutex> lk(g_mm_reload_count_persist_mu);
    return persistPrimaryConfigMarketMakerCurrentReloadCountLocked(std::max(0, count));
}

bool Config::bumpMarketMakerCurrentReloadCountForMmAccept(const std::string& strategy_name,
                                                          const std::string& client_order_id) {
    if (getMarketMakerMaxReloadCycles() <= 0) {
        return true;
    }
    if (strategy_name.empty() || client_order_id.size() < strategy_name.size() ||
        client_order_id.rfind(strategy_name, 0) != 0) {
        return false;
    }
    std::lock_guard<std::mutex> lk(g_mm_reload_count_persist_mu);
    const int cur = getInt("market_maker.current_reload_count", 0);
    return persistPrimaryConfigMarketMakerCurrentReloadCountLocked(cur + 1);
}

bool Config::bumpMarketMakerCurrentReloadCountForInstrumentFill() {
    if (getMarketMakerMaxReloadCycles() <= 0) {
        return true;
    }
    std::lock_guard<std::mutex> lk(g_mm_reload_count_persist_mu);
    const int cur = getInt("market_maker.current_reload_count", 0);
    return persistPrimaryConfigMarketMakerCurrentReloadCountLocked(cur + 1);
}

bool Config::getMarketMakerReloadCyclesCountFillOnly() const {
    return getBool("market_maker.reload_cycles_count_fill_only", true);
}

bool Config::getMarketMakerDeskSyncEnabled() const {
    return getBool("market_maker.desk_sync_enabled", false);
}

std::string Config::getMarketMakerDeskSignalPath() const {
    return getString("market_maker.desk_signal_path", "logs/mm_desk_signal.json");
}

int Config::getMarketMakerMaxPosition() const {
    int v = getInt("market_maker.max_position", 1'000'000);
    return v > 0 ? v : 1'000'000;
}

int Config::getMarketMakerAdjustPosition() const {
    int v = getInt("market_maker.adjust_position", 1);
    return v > 0 ? v : 1;
}

int Config::getMarketMakerAdjustTicks() const {
    // Be permissive here: some save/edit paths may serialize as float or numeric string.
    // If parsing fails, fall back to legacy aliases and finally to 1.
    try {
        json raw = getValue("market_maker.adjust_ticks");
        if (raw.is_number_integer()) {
            const int v = raw.get<int>();
            return v > 0 ? v : 1;
        }
        if (raw.is_number_float()) {
            const int v = static_cast<int>(std::llround(raw.get<double>()));
            return v > 0 ? v : 1;
        }
        if (raw.is_string()) {
            const std::string s = raw.get<std::string>();
            if (!s.empty()) {
                try {
                    const int vi = std::stoi(s);
                    return vi > 0 ? vi : 1;
                } catch (...) {
                    try {
                        const int vf = static_cast<int>(std::llround(std::stod(s)));
                        return vf > 0 ? vf : 1;
                    } catch (...) {}
                }
            }
        }
    } catch (...) {}
    int v = getInt("market_maker.adjust_ticks", getInt("market_maker.mm_adjust_ticks", 1));
    return v > 0 ? v : 1;
}

bool Config::isHedgeEnabled() const {
    return getBool("hedge.enabled", false);
}

std::string Config::getHedgeWebhookUrl() const {
    return getString("hedge.webhook_url", "");
}

void Config::onConfigChange(ConfigChangeCallback callback) {
    change_callbacks_.push_back(std::move(callback));
}

double Config::getAxGatewayInstrumentTickSize(const std::string& ax_symbol) const {
    std::shared_lock<std::shared_mutex> lk(mutex_);
    if (!config_.contains("ax_gateway_instruments_catalog") || !config_["ax_gateway_instruments_catalog"].is_array()) {
        return 0.0;
    }
    const auto q = lookupAxGatewayCatalogQuote(config_["ax_gateway_instruments_catalog"], ax_symbol);
    if (!q || !(q->tick_size > 0.0) || !std::isfinite(q->tick_size)) {
        return 0.0;
    }
    return q->tick_size;
}

int Config::getAxGatewayInstrumentMinimumOrderSize(const std::string& ax_symbol) const {
    std::shared_lock<std::shared_mutex> lk(mutex_);
    if (!config_.contains("ax_gateway_instruments_catalog") || !config_["ax_gateway_instruments_catalog"].is_array()) {
        return 0;
    }
    const auto q = lookupAxGatewayCatalogQuote(config_["ax_gateway_instruments_catalog"], ax_symbol);
    if (!q) {
        return 0;
    }
    return q->minimum_order_size > 0 ? q->minimum_order_size : 1;
}

double Config::getMarketMakerInstrumentTickForAxSymbol(const std::string& ax_symbol) const {
    if (ax_symbol.empty()) {
        return 0.0;
    }
    auto sym_key = [](const std::string& s) {
        std::string o;
        o.reserve(s.size());
        for (unsigned char c : s) {
            if (std::isspace(c)) {
                continue;
            }
            o.push_back(static_cast<char>(std::toupper(c)));
        }
        return o;
    };
    const std::string want = sym_key(ax_symbol);
    if (want.empty()) {
        return 0.0;
    }
    if (hasMarketMakerInstrumentList()) {
        for (const auto& leg : getMarketMakerInstruments()) {
            if (sym_key(leg.ax_symbol) != want && sym_key(leg.order_symbol) != want) {
                continue;
            }
            if (leg.tick_size > 0.0 && std::isfinite(leg.tick_size)) {
                return leg.tick_size;
            }
            return 0.0;
        }
        return 0.0;
    }
    if (sym_key(getMarketMakerSymbol()) == want || sym_key(getMarketMakerOrderSymbol()) == want) {
        const double g = getMarketMakerPriceTick();
        return (g > 0.0 && std::isfinite(g)) ? g : 0.0;
    }
    return 0.0;
}

int Config::getMarketMakerInstrumentOrderSizeStepForAxSymbol(const std::string& ax_symbol) const {
    if (ax_symbol.empty()) {
        return 0;
    }
    auto sym_key = [](const std::string& s) {
        std::string o;
        o.reserve(s.size());
        for (unsigned char c : s) {
            if (std::isspace(c)) {
                continue;
            }
            o.push_back(static_cast<char>(std::toupper(c)));
        }
        return o;
    };
    const std::string want = sym_key(ax_symbol);
    if (want.empty()) {
        return 0;
    }
    if (hasMarketMakerInstrumentList()) {
        for (const auto& leg : getMarketMakerInstruments()) {
            if (sym_key(leg.ax_symbol) != want && sym_key(leg.order_symbol) != want) {
                continue;
            }
            if (leg.order_size_step > 0) {
                return leg.order_size_step;
            }
            return 0;
        }
        return 0;
    }
    if (sym_key(getMarketMakerSymbol()) == want || sym_key(getMarketMakerOrderSymbol()) == want) {
        return getMarketMakerOrderSizeStep();
    }
    return 0;
}

namespace {
// Shared symbol normalizer for the MWR per-leg accessors (matches the local lambdas above).
inline std::string mwrSymKey(const std::string& s) {
    std::string o;
    o.reserve(s.size());
    for (unsigned char c : s) {
        if (std::isspace(c)) {
            continue;
        }
        o.push_back(static_cast<char>(std::toupper(c)));
    }
    return o;
}
}  // namespace

int Config::getMarketMakerInstrumentMwrSizeTicksForAxSymbol(const std::string& ax_symbol) const {
    if (ax_symbol.empty() || !hasMarketMakerInstrumentList()) {
        return 0;
    }
    const std::string want = mwrSymKey(ax_symbol);
    if (want.empty()) {
        return 0;
    }
    for (const auto& leg : getMarketMakerInstruments()) {
        if (mwrSymKey(leg.ax_symbol) != want && mwrSymKey(leg.order_symbol) != want) {
            continue;
        }
        return (leg.mwr_size_ticks > 0) ? leg.mwr_size_ticks : 0;
    }
    return 0;
}

int Config::getMarketMakerInstrumentMwrWindowSecForAxSymbol(const std::string& ax_symbol) const {
    if (ax_symbol.empty() || !hasMarketMakerInstrumentList()) {
        return 0;
    }
    const std::string want = mwrSymKey(ax_symbol);
    if (want.empty()) {
        return 0;
    }
    for (const auto& leg : getMarketMakerInstruments()) {
        if (mwrSymKey(leg.ax_symbol) != want && mwrSymKey(leg.order_symbol) != want) {
            continue;
        }
        return (leg.mwr_window_sec > 0) ? leg.mwr_window_sec : 0;
    }
    return 0;
}

int Config::getMarketMakerInstrumentMwrPullSecForAxSymbol(const std::string& ax_symbol) const {
    if (ax_symbol.empty() || !hasMarketMakerInstrumentList()) {
        return 0;
    }
    const std::string want = mwrSymKey(ax_symbol);
    if (want.empty()) {
        return 0;
    }
    for (const auto& leg : getMarketMakerInstruments()) {
        if (mwrSymKey(leg.ax_symbol) != want && mwrSymKey(leg.order_symbol) != want) {
            continue;
        }
        return (leg.mwr_pull_sec > 0) ? leg.mwr_pull_sec : 0;
    }
    return 0;
}

bool Config::applyAxGatewayInstrumentsHttpBody(const std::string& http_body,
                                               bool strict_match_configured_symbols,
                                               std::string& err) {
    std::string snap_path = "logs/ax_gateway_instruments.json";
    bool persist_primary = true;
    {
        std::shared_lock<std::shared_mutex> lk(mutex_);
        if (config_.contains("startup") && config_["startup"].is_object()) {
            const auto& su = config_["startup"];
            if (su.contains("ax_gateway_instruments_snapshot_path") &&
                su["ax_gateway_instruments_snapshot_path"].is_string()) {
                const std::string p = su["ax_gateway_instruments_snapshot_path"].get<std::string>();
                if (!p.empty()) {
                    snap_path = p;
                }
            }
            if (su.contains("persist_ax_gateway_instruments_to_primary_config") &&
                su["persist_ax_gateway_instruments_to_primary_config"].is_boolean()) {
                persist_primary = su["persist_ax_gateway_instruments_to_primary_config"].get<bool>();
            }
        }
    }
    json catalog_snap = json::array();
    std::string primary_path_copy;
    {
        std::unique_lock<std::shared_mutex> lk(mutex_);
        int n = 0;
        if (!mergeAxGatewayInstrumentsIntoConfig(config_, http_body, strict_match_configured_symbols, err, &n)) {
            return false;
        }
        catalog_snap = config_["ax_gateway_instruments_catalog"];
        (void)n;
        if (persist_primary && !primary_config_path_.empty()) {
            primary_path_copy = primary_config_path_;
        }
    }
    (void)writeAxGatewayInstrumentsSnapshot(snap_path, catalog_snap);
    if (persist_primary && !primary_path_copy.empty()) {
        (void)saveToFile(primary_path_copy);
    }
    return true;
}

} // namespace config
} // namespace architect
