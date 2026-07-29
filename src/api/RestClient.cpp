#include "api/RestClient.h"
#include "api/CurlMultiManager.h"
#include "config/Config.h"
#include "core/LatencyTracker.h"
#include "core/MmJobVenueCalls.h"
#include "user/UserManager.h"
#include <nlohmann/json.hpp>
#include <curl/curl.h>
#include <algorithm>
#include <sstream>
#include <thread>
#include <iostream>
#include <iomanip>

namespace architect {
namespace api {

// CURL write callback
static size_t WriteCallback(void* contents, size_t size, size_t nmemb, std::string* userp) {
    size_t totalSize = size * nmemb;
    userp->append(static_cast<char*>(contents), totalSize);
    return totalSize;
}

// CURL header callback
static size_t HeaderCallback(char* buffer, size_t size, size_t nitems, 
                             std::map<std::string, std::string>* headers) {
    size_t totalSize = size * nitems;
    std::string header(buffer, totalSize);
    
    auto pos = header.find(':');
    if (pos != std::string::npos) {
        std::string key = header.substr(0, pos);
        std::string value = header.substr(pos + 1);
        
        // Trim whitespace
        value.erase(0, value.find_first_not_of(" \t\r\n"));
        value.erase(value.find_last_not_of(" \t\r\n") + 1);
        
        (*headers)[key] = value;
    }
    
    return totalSize;
}

RestClient& RestClient::getInstance() {
    static RestClient instance;
    return instance;
}

// NOTE: per-handle low-latency setopt has moved into
// CurlMultiManager::initEasyHandleForLowLatency. Each handle in the pool is
// pre-configured there once at init time. Per-request code below only sets
// dynamic options (URL, body, headers, method).

RestClient::RestClient() {
    // Touch the manager early so its CURL global init runs before any other
    // libcurl call from this process. The manager balances its global init/
    // cleanup pair against ours via libcurl's reference-counted init.
    curl_global_init(CURL_GLOBAL_ALL);
    (void)CurlMultiManager::getInstance();

    // Initialize from config
    auto& config = config::Config::getInstance();
    base_url_ = config.getRestEndpoint();
    default_timeout_ = std::chrono::milliseconds(
        config.getInt("api.timeout_read_ms", 30000)
    );
    // Tight, bounded caps for the shared order-gateway pool so one stuck call cannot
    // wedge the whole desk (see RestClient.h for the multi_mutex_ rationale).
    order_op_timeout_ = std::chrono::milliseconds(
        std::max(500, config.getInt("api.order_op_timeout_ms", 4000))
    );
    gateway_get_timeout_ = std::chrono::milliseconds(
        std::max(500, config.getInt("api.gateway_get_timeout_ms", 8000))
    );

    // Tell the manager which URL to ping during idle gaps. The orders gateway
    // is what we need to keep warm (placement is the latency-critical path),
    // and `/healthz` (or fallback root) is a cheap GET that does not need auth.
    // We append a known harmless suffix; if the gateway returns 404 for it,
    // the keepalive still happened — we discard the response.
    {
        std::string ping = getOrdersGatewayBase();
        if (!ping.empty()) {
            // Trim trailing slash to avoid `//` in the joined URL.
            while (!ping.empty() && ping.back() == '/') ping.pop_back();
            ping += "/healthz";
            CurlMultiManager::getInstance().setIdlePingUrl(std::move(ping));
        }
    }

    // Warm-connection pool (Fix 1, 2026-07-13). Keep >= curl_warm_connections
    // keep-alive connections hot so an order batch of N legs each reuses one
    // (reused=1 connect=0 tls=0) instead of handshaking, collapsing batch_rtt from
    // ~N RTT back to ~1 RTT. Pre-warm now so the first batch is already hot.
    {
        const int warm = config.getInt("market_maker.curl_warm_connections", 6);
        const int touch = config.getInt("market_maker.curl_keepalive_touch_sec", 20);
        CurlMultiManager::getInstance().configure(warm, touch);
        CurlMultiManager::getInstance().prewarmConnections();
    }
}

RestClient::~RestClient() {
    if (orders_headers_list_) {
        curl_slist_free_all(static_cast<struct curl_slist*>(orders_headers_list_));
        orders_headers_list_ = nullptr;
    }
    curl_global_cleanup();
}

// Rebuild pre-cached header list for orders (called when token changes)
void RestClient::rebuildOrdersHeaderList() {
    // Free old list
    if (orders_headers_list_) {
        curl_slist_free_all(static_cast<struct curl_slist*>(orders_headers_list_));
        orders_headers_list_ = nullptr;
    }
    
    // Get current token
    std::string token = session_token_;
    if (token.empty()) {
        token = user::UserManager::getInstance().getSessionToken();
    }
    
    // Build new list
    struct curl_slist* headers = nullptr;
    headers = curl_slist_append(headers, "Content-Type: application/json");
    headers = curl_slist_append(headers, "Accept: application/json");
    headers = curl_slist_append(headers, "Connection: keep-alive");
    headers = curl_slist_append(headers, "Expect:");  // Disable 100-continue
    
    if (!token.empty()) {
        cached_auth_header_ = "Authorization: Bearer " + token;
        cached_token_for_header_ = token;
        headers = curl_slist_append(headers, cached_auth_header_.c_str());
    }
    
    orders_headers_list_ = headers;
}

void RestClient::setBaseUrl(const std::string& url) {
    base_url_ = url;
}

void RestClient::setSessionToken(const std::string& token) {
    session_token_ = token;
    // Rebuild pre-cached headers for orders
    rebuildOrdersHeaderList();
}

void RestClient::setTimeout(std::chrono::milliseconds timeout) {
    default_timeout_ = timeout;
}

void RestClient::setSslVerify(bool verify) {
    ssl_verify_ = verify;
}

std::string RestClient::buildUrl(const std::string& path, const QueryParams& params) const {
    std::string url = base_url_ + path;
    
    if (!params.empty()) {
        url += "?";
        bool first = true;
        for (const auto& [key, value] : params) {
            if (!first) url += "&";
            url += key + "=" + value;
            first = false;
        }
    }
    
    return url;
}

Headers RestClient::buildHeaders() const {
    Headers headers;
    headers["Content-Type"] = "application/json";
    headers["Accept"] = "application/json";
    headers["User-Agent"] = "ArchitectPlatformCore/1.0";
    
    // Add session token if available
    if (!session_token_.empty()) {
        headers["Authorization"] = "Bearer " + session_token_;
    } else {
        // Try to get from UserManager
        auto token = user::UserManager::getInstance().getSessionToken();
        if (!token.empty()) {
            headers["Authorization"] = "Bearer " + token;
        }
    }
    
    return headers;
}

// Default performRequest uses a pool handle (same code path as before — the
// manager hands out a pre-configured handle so per-request setopt below is
// unchanged).
HttpResponse RestClient::performRequest(const HttpRequest& request) {
    return performRequestOnHandle(request, "general");
}

// Core implementation — OPTIMIZED: only sets dynamic options per-request.
// Static options (TCP_NODELAY, keepalive, SSL session cache, share, etc.) are
// pre-configured in CurlMultiManager when the pool was built.
HttpResponse RestClient::performRequestOnHandle(const HttpRequest& request,
                                                [[maybe_unused]] const char* handle_name) {
    auto pool_handle = CurlMultiManager::getInstance().acquire();

    HttpResponse response;
    auto start_time = std::chrono::steady_clock::now();

    CURL* curl = pool_handle.curl();
    if (!curl) {
        response.error_message = "CURL pool returned null handle";
        return response;
    }
    
    // === DYNAMIC OPTIONS ONLY (everything else pre-configured) ===
    
    // 1. URL
    std::string url = !request.absolute_url.empty()
        ? request.absolute_url
        : buildUrl(request.path, request.params);
    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    
    // 2. Method - MUST use CUSTOMREQUEST to override on reused connections
    curl_easy_setopt(curl, CURLOPT_HTTPGET, 0L);
    curl_easy_setopt(curl, CURLOPT_POST, 0L);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, nullptr);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, 0L);
    
    switch (request.method) {
        case HttpMethod::GET:
            curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "GET");
            curl_easy_setopt(curl, CURLOPT_HTTPGET, 1L);
            break;
        case HttpMethod::POST:
            curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "POST");
            curl_easy_setopt(curl, CURLOPT_POST, 1L);
            break;
        case HttpMethod::PUT:
            curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "PUT");
            break;
        case HttpMethod::DELETE:
            curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "DELETE");
            break;
        case HttpMethod::PATCH:
            curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "PATCH");
            break;
    }
    
    // 3. Headers - build minimal list
    struct curl_slist* headers_list = nullptr;
    headers_list = curl_slist_append(headers_list, "Content-Type: application/json");
    headers_list = curl_slist_append(headers_list, "Accept: application/json");
    headers_list = curl_slist_append(headers_list, "Connection: keep-alive");
    headers_list = curl_slist_append(headers_list, "Expect:");  // Disable 100-continue
    
    // Add auth header
    if (!request.skip_auth) {
        std::string auth_token = session_token_;
        if (auth_token.empty()) {
            auth_token = user::UserManager::getInstance().getSessionToken();
        }
        if (!auth_token.empty()) {
            std::string auth_header = "Authorization: Bearer " + auth_token;
            headers_list = curl_slist_append(headers_list, auth_header.c_str());
        }
    }
    
    // Add custom headers
    for (const auto& [key, value] : request.headers) {
        std::string header = key + ": " + value;
        headers_list = curl_slist_append(headers_list, header.c_str());
    }
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers_list);
    
    // 4. Body
    if (!request.body.empty()) {
        curl_easy_setopt(curl, CURLOPT_POSTFIELDS, request.body.c_str());
        curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, static_cast<long>(request.body.size()));
    } else {
        curl_easy_setopt(curl, CURLOPT_POSTFIELDS, nullptr);
        curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, 0L);
    }
    
    // 5. Response callbacks
    std::string response_body;
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteCallback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response_body);
    curl_easy_setopt(curl, CURLOPT_HEADERFUNCTION, HeaderCallback);
    curl_easy_setopt(curl, CURLOPT_HEADERDATA, &response.headers);
    
    // 6. Custom timeout if specified
    if (request.timeout.count() > 0) {
        curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, request.timeout.count());
    }
    
    // === EXECUTE (curl_multi pool) ===
    CURLcode res = CurlMultiManager::getInstance().performBlocking(curl);

    auto end_time = std::chrono::steady_clock::now();
    response.latency = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);

    // Cleanup headers list
    if (headers_list) {
        curl_slist_free_all(headers_list);
    }

    if (res != CURLE_OK) {
        response.error_message = curl_easy_strerror(res);
        response.is_success = false;

        std::lock_guard<std::mutex> stats_lock(stats_mutex_);
        ++stats_.total_requests;
        ++stats_.failed_requests;

        return response;
    }

    // Get response code
    long http_code = 0;
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);
    response.status_code = static_cast<int>(http_code);
    response.body = response_body;
    response.is_success = response.isOk();
    
    // Update rate limits from headers
    updateRateLimits(response);
    
    // Update stats
    {
        std::lock_guard<std::mutex> stats_lock(stats_mutex_);
        ++stats_.total_requests;
        if (response.is_success) {
            ++stats_.successful_requests;
        } else {
            ++stats_.failed_requests;
        }
        stats_.avg_latency_ms = (stats_.avg_latency_ms * (stats_.total_requests - 1) 
                                 + response.latency.count()) / stats_.total_requests;
    }
    
    return response;
}

void RestClient::updateRateLimits(const HttpResponse& response) {
    // Parse rate limit headers
    auto it = response.headers.find("X-RateLimit-Remaining");
    if (it != response.headers.end()) {
        try {
            rate_limit_remaining_ = std::stoi(it->second);
        } catch (...) {}
    }
    
    it = response.headers.find("X-RateLimit-Limit");
    if (it != response.headers.end()) {
        try {
            rate_limit_total_ = std::stoi(it->second);
        } catch (...) {}
    }
    
    it = response.headers.find("X-RateLimit-Reset");
    if (it != response.headers.end()) {
        try {
            auto reset_sec = std::stoll(it->second);
            rate_limit_reset_ = std::chrono::steady_clock::now() 
                               + std::chrono::seconds(reset_sec);
        } catch (...) {}
    }
    
    // Update UserManager rate limits
    user::UserManager::getInstance().updateRateLimit(
        rate_limit_remaining_,
        rate_limit_total_,
        std::chrono::duration_cast<core::Timestamp>(
            std::chrono::steady_clock::now().time_since_epoch()
        )
    );
}

HttpResponse RestClient::request(const HttpRequest& request) {
    // Check rate limit
    if (isRateLimited()) {
        waitForRateLimit();
    }
    
    return performRequest(request);
}

HttpResponse RestClient::get(const std::string& path, const QueryParams& params) {
    HttpRequest req(HttpMethod::GET, path);
    req.params = params;
    return request(req);
}

HttpResponse RestClient::post(const std::string& path, const json& body) {
    HttpRequest req(HttpMethod::POST, path);
    if (!body.empty()) {
        req.body = body.dump();
    }
    return request(req);
}

HttpResponse RestClient::put(const std::string& path, const json& body) {
    HttpRequest req(HttpMethod::PUT, path);
    if (!body.empty()) {
        req.body = body.dump();
    }
    return request(req);
}

HttpResponse RestClient::del(const std::string& path, const QueryParams& params) {
    HttpRequest req(HttpMethod::DELETE, path);
    req.params = params;
    // del() is the cancel DELETE fallback (cancelOrder -> del). Bind it to the same tight
    // order-op cap the POST cancel path uses (order_op_timeout_), instead of HttpRequest's
    // 30s default: an unbounded fallback cancel can wedge teardown on a black-holed network.
    req.timeout = order_op_timeout_;
    return request(req);
}

void RestClient::requestAsync(const HttpRequest& request, ResponseCallback callback) {
    std::thread([this, request, callback]() {
        auto response = this->request(request);
        if (callback) {
            callback(response);
        }
    }).detach();
}

void RestClient::getAsync(const std::string& path, ResponseCallback callback, const QueryParams& params) {
    HttpRequest req(HttpMethod::GET, path);
    req.params = params;
    requestAsync(req, callback);
}

void RestClient::postAsync(const std::string& path, const json& body, ResponseCallback callback) {
    HttpRequest req(HttpMethod::POST, path);
    if (!body.empty()) {
        req.body = body.dump();
    }
    requestAsync(req, callback);
}

// =============================================================================
// Architect API Endpoints
// =============================================================================

static std::string extractTokenFromJson(const json& j) {
    if (j.contains("token") && j["token"].is_string())
        return j["token"].get<std::string>();
    if (j.contains("access_token") && j["access_token"].is_string())
        return j["access_token"].get<std::string>();
    if (j.contains("jwt") && j["jwt"].is_string())
        return j["jwt"].get<std::string>();
    if (j.contains("data") && j["data"].is_object()) {
        const auto& d = j["data"];
        if (d.contains("access_token") && d["access_token"].is_string())
            return d["access_token"].get<std::string>();
        if (d.contains("token") && d["token"].is_string())
            return d["token"].get<std::string>();
    }
    return {};
}

std::string RestClient::login(const std::string& api_key, const std::string& api_secret) {
    last_login_error_.clear();
    if (api_key.empty() || api_secret.empty()) {
        last_login_error_ = "missing api_key or api_secret";
        return {};
    }
    // Architect Exchange: POST /authenticate (api_key + api_secret -> bearer token)
    // https://docs.architect.exchange/api-reference/user-management/authenticate.md
    json body = {
        {"api_key", api_key},
        {"api_secret", api_secret},
        {"expiration_seconds", 86400}
    };
    HttpRequest req(HttpMethod::POST, core::constants::api::AUTHENTICATE);
    req.body = body.dump();
    req.skip_auth = true;
    HttpResponse resp = performRequest(req);
    if (resp.isOk()) {
        try {
            json j = resp.parseJson();
            std::string token = extractTokenFromJson(j);
            if (!token.empty())
                return token;
        } catch (...) {}
        last_login_error_ = "HTTP 200 but no token in response: " + resp.body.substr(0, 120);
        return {};
    }
    last_login_error_ = "HTTP " + std::to_string(resp.status_code);
    if (!resp.body.empty()) {
        last_login_error_ += " " + resp.body.substr(0, 100);
    }
    return {};
}

std::string RestClient::getLastLoginError() const {
    return last_login_error_;
}

HttpResponse RestClient::whoami() {
    return get(core::constants::api::WHOAMI);
}

HttpResponse RestClient::getInstruments() {
    return get(core::constants::api::INSTRUMENTS);
}

HttpResponse RestClient::getOrders(const QueryParams& filters) {
    core::mmJobVenueNoteGet();  // proof instrumentation: venue GET on the calling (mover?) thread
    return get(core::constants::api::ORDERS, filters);
}

HttpResponse RestClient::getOrder(const std::string& order_id) {
    return get(std::string(core::constants::api::ORDERS) + "/" + order_id);
}

std::string RestClient::getOrdersGatewayBase() const {
    const std::string api_suffix("/api");
    if (base_url_.size() >= api_suffix.size() &&
        base_url_.compare(base_url_.size() - api_suffix.size(), api_suffix.size(), api_suffix) == 0) {
        return base_url_.substr(0, base_url_.size() - api_suffix.size()) + "/orders";
    }
    return base_url_ + (base_url_.empty() || base_url_.back() == '/' ? "" : "/") + "orders";
}

HttpResponse RestClient::createOrder(const json& order) {
    return post(core::constants::api::ORDERS, order);
}

HttpResponse RestClient::placeOrder(const json& place_order_body) {
    // Convert to string and use fast path (placeOrderRaw notes the POST — don't double-count here)
    return placeOrderRaw(place_order_body.dump());
}

// ULTRA-LOW-LATENCY order placement path
// - Pre-cached URL
// - Pre-built headers (no curl_slist_append per request)
// - Minimal dynamic setup
HttpResponse RestClient::performOrderRequest(const std::string& json_body) {
    // === LATENCY INSTRUMENTATION: Enter REST function ===
    auto t_enter = std::chrono::steady_clock::now();

    auto pool_handle = CurlMultiManager::getInstance().acquire();

    auto t_after_lock = std::chrono::steady_clock::now();
    auto lock_wait_us = std::chrono::duration_cast<std::chrono::microseconds>(
        t_after_lock - t_enter).count();

    HttpResponse response;

    CURL* curl = pool_handle.curl();
    if (!curl) {
        response.error_message = "CURL pool returned null handle (orders)";
        return response;
    }
    
    // === STATIC URL (cached) ===
    static std::string cached_url;
    if (cached_url.empty()) {
        cached_url = getOrdersGatewayBase() + core::constants::api::PLACE_ORDER;
    }
    
    // === MINIMAL DYNAMIC SETUP ===
    auto t_setup_start = std::chrono::steady_clock::now();
    
    curl_easy_setopt(curl, CURLOPT_URL, cached_url.c_str());
    curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "POST");
    curl_easy_setopt(curl, CURLOPT_POST, 1L);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, json_body.c_str());
    curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, static_cast<long>(json_body.size()));
    
    // === PRE-BUILT HEADERS ===
    // Check if token changed and rebuild if necessary
    std::string current_token = session_token_;
    if (current_token.empty()) {
        current_token = user::UserManager::getInstance().getSessionToken();
    }
    if (current_token != cached_token_for_header_ || !orders_headers_list_) {
        // Token changed or headers not built yet
        const_cast<RestClient*>(this)->rebuildOrdersHeaderList();
    }
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, static_cast<struct curl_slist*>(orders_headers_list_));
    
    // === RESPONSE HANDLING ===
    std::string response_body;
    response_body.reserve(1024);  // Pre-allocate for typical response
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteCallback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response_body);
    
    // Skip header callback for orders (we don't need response headers)
    curl_easy_setopt(curl, CURLOPT_HEADERFUNCTION, nullptr);
    curl_easy_setopt(curl, CURLOPT_HEADERDATA, nullptr);

    // Hard cap: a stuck place must NOT wedge the shared pool (and thus every other
    // instrument's quoting + the fills poll) for the pool's 30s default. Pool handles
    // are reused, so this is set explicitly on every order-gateway path.
    curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, static_cast<long>(order_op_timeout_.count()));

    auto t_setup_done = std::chrono::steady_clock::now();
    auto curl_setup_us = std::chrono::duration_cast<std::chrono::microseconds>(
        t_setup_done - t_setup_start).count();
    
    // === LATENCY INSTRUMENTATION: Before CURL execute ===
    std::cout << "[LATENCY_TRACE] REST_CURL_SETUP: lock_wait=" << core::latencyDisplayUs(lock_wait_us) 
              << "us curl_setup=" << core::latencyDisplayUs(curl_setup_us) << "us" << std::endl;
    
    // === EXECUTE (curl_multi pool) ===
    auto t_curl_start = std::chrono::steady_clock::now();
    CURLcode res = CurlMultiManager::getInstance().performBlocking(curl);
    auto t_curl_end = std::chrono::steady_clock::now();

    auto curl_perform_us = std::chrono::duration_cast<std::chrono::microseconds>(
        t_curl_end - t_curl_start).count();

    // Get detailed CURL timing info — these still work because the easy
    // handle's internal counters are populated by curl_multi_perform exactly
    // as they were by curl_easy_perform.
    double dns_time = 0, connect_time = 0, tls_time = 0, pretransfer_time = 0, starttransfer_time = 0, total_time = 0;
    curl_easy_getinfo(curl, CURLINFO_NAMELOOKUP_TIME, &dns_time);
    curl_easy_getinfo(curl, CURLINFO_CONNECT_TIME, &connect_time);
    curl_easy_getinfo(curl, CURLINFO_APPCONNECT_TIME, &tls_time);
    curl_easy_getinfo(curl, CURLINFO_PRETRANSFER_TIME, &pretransfer_time);
    curl_easy_getinfo(curl, CURLINFO_STARTTRANSFER_TIME, &starttransfer_time);
    curl_easy_getinfo(curl, CURLINFO_TOTAL_TIME, &total_time);
    
    // === LATENCY INSTRUMENTATION: CURL timing breakdown ===
    std::cout << "[LATENCY_TRACE] REST_CURL_PERFORM: total=" << core::latencyDisplayUs(curl_perform_us) << "us" << std::endl;
    std::cout << "[LATENCY_TRACE] CURL_INTERNALS: dns=" << std::fixed << std::setprecision(3) << core::latencyDisplayMsDouble(dns_time * 1000) 
              << "ms connect=" << core::latencyDisplayMsDouble(connect_time * 1000) << "ms tls=" << core::latencyDisplayMsDouble(tls_time * 1000) 
              << "ms pretransfer=" << core::latencyDisplayMsDouble(pretransfer_time * 1000) << "ms starttransfer=" << core::latencyDisplayMsDouble(starttransfer_time * 1000) 
              << "ms total=" << core::latencyDisplayMsDouble(total_time * 1000) << "ms" << std::endl;
    
    response.latency = std::chrono::duration_cast<std::chrono::milliseconds>(t_curl_end - t_curl_start);
    
    if (res != CURLE_OK) {
        response.error_message = curl_easy_strerror(res);
        response.is_success = false;
        return response;
    }
    
    // Get response code
    long http_code = 0;
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);
    response.status_code = static_cast<int>(http_code);
    response.body = std::move(response_body);
    response.is_success = (http_code >= 200 && http_code < 300);
    
    return response;
}

// Fast path: accepts pre-serialized JSON to avoid serialization overhead
HttpResponse RestClient::placeOrderRaw(const std::string& json_body) {
    core::mmJobVenueNotePost();  // proof instrumentation: venue POST (place) on this thread
    // Use ultra-low-latency path
    return performOrderRequest(json_body);
}

HttpResponse RestClient::modifyOrder(const json& modify_order_body) {
    return modifyOrderRaw(modify_order_body.dump());
}

// ULTRA-LOW-LATENCY order modify path
HttpResponse RestClient::performModifyRequest(const std::string& json_body) {
    auto pool_handle = CurlMultiManager::getInstance().acquire();

    HttpResponse response;
    auto start_time = std::chrono::steady_clock::now();

    CURL* curl = pool_handle.curl();
    if (!curl) {
        response.error_message = "CURL pool returned null handle (modify)";
        return response;
    }
    
    // === STATIC URL (cached) ===
    static std::string cached_modify_url;
    if (cached_modify_url.empty()) {
        cached_modify_url = getOrdersGatewayBase() + core::constants::api::MODIFY_ORDER;
    }
    
    // === MINIMAL DYNAMIC SETUP ===
    curl_easy_setopt(curl, CURLOPT_URL, cached_modify_url.c_str());
    curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "POST");
    curl_easy_setopt(curl, CURLOPT_POST, 1L);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, json_body.c_str());
    curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, static_cast<long>(json_body.size()));
    
    // === PRE-BUILT HEADERS ===
    std::string current_token = session_token_;
    if (current_token.empty()) {
        current_token = user::UserManager::getInstance().getSessionToken();
    }
    if (current_token != cached_token_for_header_ || !orders_headers_list_) {
        const_cast<RestClient*>(this)->rebuildOrdersHeaderList();
    }
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, static_cast<struct curl_slist*>(orders_headers_list_));
    
    // === RESPONSE HANDLING ===
    std::string response_body;
    response_body.reserve(512);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteCallback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response_body);
    curl_easy_setopt(curl, CURLOPT_HEADERFUNCTION, nullptr);
    curl_easy_setopt(curl, CURLOPT_HEADERDATA, nullptr);

    // Hard cap (see performOrderRequest): never let a stuck modify wedge the pool.
    curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, static_cast<long>(order_op_timeout_.count()));

    // === EXECUTE (curl_multi pool) ===
    CURLcode res = CurlMultiManager::getInstance().performBlocking(curl);

    auto end_time = std::chrono::steady_clock::now();
    response.latency = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);

    if (res != CURLE_OK) {
        response.error_message = curl_easy_strerror(res);
        response.is_success = false;
        return response;
    }

    long http_code = 0;
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);
    response.status_code = static_cast<int>(http_code);
    response.body = std::move(response_body);
    response.is_success = (http_code >= 200 && http_code < 300);
    
    return response;
}

HttpResponse RestClient::modifyOrderRaw(const std::string& json_body) {
    return performModifyRequest(json_body);
}

HttpResponse RestClient::cancelOrder(const std::string& order_id) {
    core::mmJobVenueNoteCancel();  // proof instrumentation: venue cancel on this thread
    return del(std::string(core::constants::api::ORDERS) + "/" + order_id);
}

// Cancel via order gateway - uses GENERAL curl handle (separate from order placement)
// This allows cancels to run IN PARALLEL with new order placements
HttpResponse RestClient::postOrdersGatewayRelative(const std::string& path_suffix, const std::string& json_body) {
    auto pool_handle = CurlMultiManager::getInstance().acquire();

    HttpResponse response;
    auto start_time = std::chrono::steady_clock::now();

    CURL* curl = pool_handle.curl();
    if (!curl) {
        response.error_message = "CURL pool returned null handle (post-gateway)";
        return response;
    }
    
    const std::string url = getOrdersGatewayBase() + path_suffix;
    
    // === SETUP FOR POST ===
    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "POST");
    curl_easy_setopt(curl, CURLOPT_POST, 1L);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, json_body.c_str());
    curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, static_cast<long>(json_body.size()));
    
    // === HEADERS ===
    struct curl_slist* headers = nullptr;
    headers = curl_slist_append(headers, "Content-Type: application/json");
    headers = curl_slist_append(headers, "Accept: application/json");
    std::string auth_header = "Authorization: Bearer ";
    std::string token = session_token_;
    if (token.empty()) {
        token = user::UserManager::getInstance().getSessionToken();
    }
    auth_header += token;
    headers = curl_slist_append(headers, auth_header.c_str());
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
    
    // === RESPONSE HANDLING ===
    std::string response_body;
    response_body.reserve(256);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteCallback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response_body);
    curl_easy_setopt(curl, CURLOPT_HEADERFUNCTION, nullptr);
    curl_easy_setopt(curl, CURLOPT_HEADERDATA, nullptr);

    // Hard cap (see performOrderRequest): cancels run through this path; a stuck
    // cancel must not freeze the pool for the whole desk.
    curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, static_cast<long>(order_op_timeout_.count()));

    // === EXECUTE (curl_multi pool) ===
    CURLcode res = CurlMultiManager::getInstance().performBlocking(curl);

    // Cleanup headers
    curl_slist_free_all(headers);

    auto end_time = std::chrono::steady_clock::now();
    response.latency = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
    
    if (res != CURLE_OK) {
        response.error_message = curl_easy_strerror(res);
        response.is_success = false;
        return response;
    }
    
    long http_code = 0;
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);
    response.status_code = static_cast<int>(http_code);
    response.body = std::move(response_body);
    response.is_success = (http_code >= 200 && http_code < 300);
    
    return response;
}

HttpResponse RestClient::getOrdersGatewayRelative(const std::string& path_suffix, const QueryParams& params) {
    core::mmJobVenueNoteGet();  // proof instrumentation: /open-orders (etc.) GET on this thread
    auto pool_handle = CurlMultiManager::getInstance().acquire();

    HttpResponse response;
    auto start_time = std::chrono::steady_clock::now();

    CURL* curl = pool_handle.curl();
    if (!curl) {
        response.error_message = "CURL pool returned null handle (get-gateway)";
        return response;
    }

    std::string url = getOrdersGatewayBase();
    if (!path_suffix.empty()) {
        if (path_suffix.front() != '/') {
            url += '/';
        }
        url += path_suffix;
    }
    if (!params.empty()) {
        url += "?";
        bool first = true;
        for (const auto& [k, v] : params) {
            if (!first) {
                url += "&";
            }
            url += k + "=" + v;
            first = false;
        }
    }

    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_HTTPGET, 1L);
    curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "GET");
    curl_easy_setopt(curl, CURLOPT_POST, 0L);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, nullptr);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, 0L);

    struct curl_slist* headers = nullptr;
    headers = curl_slist_append(headers, "Accept: application/json");
    std::string auth_header = "Authorization: Bearer ";
    std::string token = session_token_;
    if (token.empty()) {
        token = user::UserManager::getInstance().getSessionToken();
    }
    auth_header += token;
    headers = curl_slist_append(headers, auth_header.c_str());
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

    std::string response_body;
    response_body.reserve(1024);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteCallback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response_body);
    curl_easy_setopt(curl, CURLOPT_HEADERFUNCTION, nullptr);
    curl_easy_setopt(curl, CURLOPT_HEADERDATA, nullptr);

    // Hard cap: gateway GETs (fills/position polls) share the same pooled handles and
    // multi_mutex_, so an unbounded GET would freeze quoting just like a stuck order op.
    // Slightly looser than order ops since a poll can legitimately return more data.
    curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, static_cast<long>(gateway_get_timeout_.count()));

    // === EXECUTE (curl_multi pool) ===
    CURLcode res = CurlMultiManager::getInstance().performBlocking(curl);
    curl_slist_free_all(headers);

    auto end_time = std::chrono::steady_clock::now();
    response.latency = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);

    if (res != CURLE_OK) {
        response.error_message = curl_easy_strerror(res);
        response.is_success = false;
        return response;
    }

    long http_code = 0;
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);
    response.status_code = static_cast<int>(http_code);
    response.body = std::move(response_body);
    response.is_success = (http_code >= 200 && http_code < 300);

    return response;
}

HttpResponse RestClient::cancelOrderGateway(const std::string& json_body) {
    core::mmJobVenueNoteCancel();  // proof instrumentation: venue cancel (gateway) on this thread
    return postOrdersGatewayRelative(std::string(core::constants::api::CANCEL_ORDER), json_body);
}

std::vector<HttpResponse> RestClient::performOrderBatchPost(const std::string& url,
                                                           const std::vector<std::string>& bodies) {
    // Single-URL convenience: every body posts to the same endpoint.
    std::vector<std::string> urls(bodies.size(), url);
    return performOrderBatchPost(urls, bodies);
}

std::vector<HttpResponse> RestClient::performOrderBatchPost(const std::vector<std::string>& urls,
                                                           const std::vector<std::string>& bodies) {
    std::vector<HttpResponse> responses(bodies.size());
    if (bodies.empty() || urls.size() != bodies.size()) {
        return responses;
    }

    // Ensure the cached orders header list (Content-Type/Accept/keep-alive/Expect:/Bearer)
    // is current for this token, once for the whole batch. The member slist is read-only
    // during transfer, so sharing it across the concurrent handles on THIS thread is safe.
    {
        std::string current_token = session_token_;
        if (current_token.empty()) {
            current_token = user::UserManager::getInstance().getSessionToken();
        }
        if (current_token != cached_token_for_header_ || !orders_headers_list_) {
            rebuildOrdersHeaderList();
        }
    }
    auto* headers = static_cast<struct curl_slist*>(orders_headers_list_);

    // One pool handle per body, held for the whole batch. Response buffers live in a
    // pre-sized vector so their addresses stay stable for CURLOPT_WRITEDATA (no realloc).
    std::vector<CurlMultiManager::Handle> handles;
    handles.reserve(bodies.size());
    std::vector<CURL*> easys(bodies.size(), nullptr);
    std::vector<std::string> buffers(bodies.size());
    for (auto& b : buffers) {
        b.reserve(1024);
    }

    const auto start_time = std::chrono::steady_clock::now();
    for (std::size_t i = 0; i < bodies.size(); ++i) {
        handles.push_back(CurlMultiManager::getInstance().acquire());
        CURL* curl = handles.back().curl();
        easys[i] = curl;
        if (!curl) {
            responses[i].error_message = "CURL pool returned null handle (batch)";
            responses[i].is_success = false;
            continue;
        }
        curl_easy_setopt(curl, CURLOPT_URL, urls[i].c_str());
        curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "POST");
        curl_easy_setopt(curl, CURLOPT_POST, 1L);
        curl_easy_setopt(curl, CURLOPT_POSTFIELDS, bodies[i].c_str());
        curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, static_cast<long>(bodies[i].size()));
        curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteCallback);
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, &buffers[i]);
        curl_easy_setopt(curl, CURLOPT_HEADERFUNCTION, nullptr);
        curl_easy_setopt(curl, CURLOPT_HEADERDATA, nullptr);
        curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, static_cast<long>(order_op_timeout_.count()));
    }

    const std::vector<CURLcode> codes = CurlMultiManager::getInstance().performBlockingBatch(easys);

    const auto end_time = std::chrono::steady_clock::now();
    const auto latency = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);

    // [BATCH_REQ] per-request curl internals (Fix 1 acceptance instrumentation).
    // After warm-up every leg should show reused=1 connect=0 tls=0, and the batch
    // wall time should collapse to ~max(starttransfer) ≈ one venue RTT. reused is
    // derived from CURLINFO_NUM_CONNECTS (0 new connections == a keep-alive reuse).
    static const bool batch_req_trace =
        config::Config::getInstance().getBool("market_maker.batch_req_trace", true);

    for (std::size_t i = 0; i < bodies.size(); ++i) {
        HttpResponse& r = responses[i];
        r.latency = latency;
        CURL* curl = easys[i];
        if (!curl) {
            r.is_success = false;
            continue;
        }
        if (batch_req_trace) {
            double connect_time = 0, appconnect_time = 0, starttransfer_time = 0;
            long num_connects = 0;
            curl_easy_getinfo(curl, CURLINFO_CONNECT_TIME, &connect_time);
            curl_easy_getinfo(curl, CURLINFO_APPCONNECT_TIME, &appconnect_time);
            curl_easy_getinfo(curl, CURLINFO_STARTTRANSFER_TIME, &starttransfer_time);
            curl_easy_getinfo(curl, CURLINFO_NUM_CONNECTS, &num_connects);
            // tls handshake time ≈ appconnect - connect (0 for a reused connection).
            const double tls_ms = (appconnect_time > connect_time)
                                      ? (appconnect_time - connect_time) * 1000.0 : 0.0;
            const char* kind =
                (urls[i].find("cancel") != std::string::npos) ? "cancel" : "place";
            std::cout << "[BATCH_REQ] i=" << i << " kind=" << kind
                      << " connect_ms=" << std::fixed << std::setprecision(2) << (connect_time * 1000.0)
                      << " tls_ms=" << tls_ms
                      << " starttransfer_ms=" << (starttransfer_time * 1000.0)
                      << " reused=" << (num_connects == 0 ? 1 : 0) << std::endl;
        }
        if (i >= codes.size() || codes[i] != CURLE_OK) {
            r.error_message = (i < codes.size()) ? curl_easy_strerror(codes[i]) : "batch size mismatch";
            r.is_success = false;
            continue;
        }
        long http_code = 0;
        curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);
        r.status_code = static_cast<int>(http_code);
        r.body = std::move(buffers[i]);
        r.is_success = (http_code >= 200 && http_code < 300);
    }
    return responses;
}

std::vector<HttpResponse> RestClient::placeOrdersRawConcurrent(const std::vector<std::string>& bodies) {
    for (std::size_t i = 0; i < bodies.size(); ++i) {
        core::mmJobVenueNotePost();  // proof instrumentation: one venue POST (place) per leg
    }
    static std::string cached_place_url;
    if (cached_place_url.empty()) {
        cached_place_url = getOrdersGatewayBase() + std::string(core::constants::api::PLACE_ORDER);
    }
    return performOrderBatchPost(cached_place_url, bodies);
}

std::vector<HttpResponse> RestClient::cancelOrdersGatewayConcurrent(const std::vector<std::string>& bodies) {
    for (std::size_t i = 0; i < bodies.size(); ++i) {
        core::mmJobVenueNoteCancel();  // proof instrumentation: one venue cancel per leg
    }
    static std::string cached_cancel_url;
    if (cached_cancel_url.empty()) {
        cached_cancel_url = getOrdersGatewayBase() + std::string(core::constants::api::CANCEL_ORDER);
    }
    return performOrderBatchPost(cached_cancel_url, bodies);
}

std::vector<HttpResponse> RestClient::cancelThenPlaceOrdersConcurrent(
    const std::vector<std::string>& cancel_bodies,
    const std::vector<std::string>& place_bodies) {
    // Endpoints (cached once): cancels -> /cancel_order, places -> /place_order.
    static std::string cached_cancel_url;
    static std::string cached_place_url;
    if (cached_cancel_url.empty()) {
        cached_cancel_url = getOrdersGatewayBase() + std::string(core::constants::api::CANCEL_ORDER);
    }
    if (cached_place_url.empty()) {
        cached_place_url = getOrdersGatewayBase() + std::string(core::constants::api::PLACE_ORDER);
    }
    // Index layout: cancels first, then places (matches the documented return contract).
    std::vector<std::string> urls;
    std::vector<std::string> bodies;
    urls.reserve(cancel_bodies.size() + place_bodies.size());
    bodies.reserve(cancel_bodies.size() + place_bodies.size());
    for (const auto& b : cancel_bodies) {
        core::mmJobVenueNoteCancel();  // proof instrumentation: one venue cancel per leg
        urls.push_back(cached_cancel_url);
        bodies.push_back(b);
    }
    for (const auto& b : place_bodies) {
        core::mmJobVenueNotePost();  // proof instrumentation: one venue POST (place) per leg
        urls.push_back(cached_place_url);
        bodies.push_back(b);
    }
    return performOrderBatchPost(urls, bodies);
}

HttpResponse RestClient::cancelAllOrders(const std::optional<std::string>& symbol) {
    nlohmann::json body = nlohmann::json::object();
    if (symbol.has_value()) {
        const std::string& sym = symbol.value();
        if (!sym.empty()) {
            body["symbol"] = sym;
        }
    }
    const std::string payload = body.dump();
    HttpResponse r = postOrdersGatewayRelative(
        std::string(core::constants::api::CANCEL_ALL_ORDERS), payload);
    if (r.status_code == 404) {
        r = postOrdersGatewayRelative(
            std::string(core::constants::api::CANCEL_ALL_ORDERS_ALT), payload);
    }
    return r;
}

HttpResponse RestClient::getMarkets() {
    return get(core::constants::api::MARKETS);
}

HttpResponse RestClient::getTicker(const std::string& symbol) {
    return get(core::constants::api::TICKER, {{"symbol", symbol}});
}

HttpResponse RestClient::getOrderbook(const std::string& symbol, int depth) {
    return get(core::constants::api::ORDERBOOK, {
        {"symbol", symbol},
        {"depth", std::to_string(depth)}
    });
}

HttpResponse RestClient::getTrades(const std::string& symbol, int limit) {
    return get(core::constants::api::TRADES, {
        {"symbol", symbol},
        {"limit", std::to_string(limit)}
    });
}

HttpResponse RestClient::getCandles(const std::string& symbol, const std::string& interval, int limit) {
    return get(core::constants::api::CANDLES, {
        {"symbol", symbol},
        {"interval", interval},
        {"limit", std::to_string(limit)}
    });
}

HttpResponse RestClient::getAccounts() {
    return get(core::constants::api::ACCOUNTS);
}

HttpResponse RestClient::getBalances() {
    return get(core::constants::api::BALANCES);
}

HttpResponse RestClient::getPositions() {
    core::mmJobVenueNoteGet();  // proof instrumentation: positions GET on this thread
    return get(core::constants::api::POSITIONS);
}

HttpResponse RestClient::getFills(const QueryParams& filters) {
    core::mmJobVenueNoteGet();  // proof instrumentation: fills GET on this thread
    return get(core::constants::api::FILLS, filters);
}

// =============================================================================
// Rate Limiting
// =============================================================================

bool RestClient::isRateLimited() const {
    if (rate_limit_remaining_ > 0) return false;
    return std::chrono::steady_clock::now() < rate_limit_reset_;
}

int RestClient::getRemainingRequests() const {
    return rate_limit_remaining_;
}

void RestClient::waitForRateLimit() {
    if (!isRateLimited()) return;
    
    auto now = std::chrono::steady_clock::now();
    if (now < rate_limit_reset_) {
        std::this_thread::sleep_until(rate_limit_reset_);
    }
    
    {
        std::lock_guard<std::mutex> lock(stats_mutex_);
        ++stats_.rate_limited;
    }
}

// =============================================================================
// Statistics
// =============================================================================

RestClient::Stats RestClient::getStats() const {
    std::lock_guard<std::mutex> lock(stats_mutex_);
    return stats_;
}

void RestClient::resetStats() {
    std::lock_guard<std::mutex> lock(stats_mutex_);
    stats_ = Stats{};
}

} // namespace api
} // namespace architect
