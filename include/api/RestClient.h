#pragma once

/**
 * @file RestClient.h
 * @brief HTTP REST client for Architect Exchange API
 */

#include "core/Types.h"
#include "core/Constants.h"
#include <string>
#include <map>
#include <vector>
#include <functional>
#include <optional>
#include <chrono>
#include <mutex>
#include <nlohmann/json.hpp>

namespace architect {
namespace api {

using json = nlohmann::json;
using Headers = std::map<std::string, std::string>;
using QueryParams = std::map<std::string, std::string>;

/**
 * @brief HTTP methods
 */
enum class HttpMethod {
    GET,
    POST,
    PUT,
    DELETE,
    PATCH
};

/**
 * @brief HTTP request configuration
 */
struct HttpRequest {
    HttpMethod      method          = HttpMethod::GET;
    std::string     path;
    Headers         headers;
    QueryParams     params;
    std::string     body;
    std::chrono::milliseconds timeout{30000};
    /** If non-empty, use this URL instead of base_url + path (for external APIs) */
    std::string     absolute_url;
    /** If true, do not add Authorization header (e.g. for login) */
    bool            skip_auth             = false;
    
    HttpRequest() = default;
    HttpRequest(HttpMethod m, const std::string& p) : method(m), path(p) {}
};

/**
 * @brief HTTP response
 */
struct HttpResponse {
    int             status_code     = 0;
    Headers         headers;
    std::string     body;
    std::string     error_message;
    std::chrono::milliseconds latency{0};
    bool            is_success      = false;
    
    [[nodiscard]] bool isOk() const { return status_code >= 200 && status_code < 300; }
    
    [[nodiscard]] json parseJson() const {
        if (body.empty()) return json();
        try {
            return json::parse(body);
        } catch (...) {
            return json();
        }
    }
};

/**
 * @brief Response callback type
 */
using ResponseCallback = std::function<void(const HttpResponse&)>;

/**
 * @brief REST API client for Architect Exchange
 * 
 * Handles all HTTP communication with the exchange API including:
 * - Authentication via session tokens
 * - Rate limiting
 * - Retry logic
 * - Request/response serialization
 */
class RestClient {
public:
    /**
     * @brief Get the singleton instance
     */
    static RestClient& getInstance();
    
    // Prevent copying
    RestClient(const RestClient&) = delete;
    RestClient& operator=(const RestClient&) = delete;
    
    // ==========================================================================
    // Configuration
    // ==========================================================================
    
    /**
     * @brief Set the base URL for API requests
     */
    void setBaseUrl(const std::string& url);
    
    /**
     * @brief Set authentication token
     */
    void setSessionToken(const std::string& token);
    
    /**
     * @brief Set default timeout
     */
    void setTimeout(std::chrono::milliseconds timeout);
    
    /**
     * @brief Enable/disable SSL verification
     */
    void setSslVerify(bool verify);
    
    // ==========================================================================
    // Synchronous Requests
    // ==========================================================================
    
    /**
     * @brief Perform a synchronous HTTP request
     */
    HttpResponse request(const HttpRequest& request);
    
    /**
     * @brief Convenience methods for common operations
     */
    HttpResponse get(const std::string& path, const QueryParams& params = {});
    HttpResponse post(const std::string& path, const json& body = {});
    HttpResponse put(const std::string& path, const json& body = {});
    HttpResponse del(const std::string& path, const QueryParams& params = {});
    
    // ==========================================================================
    // Asynchronous Requests
    // ==========================================================================
    
    /**
     * @brief Perform an asynchronous HTTP request
     */
    void requestAsync(const HttpRequest& request, ResponseCallback callback);
    
    /**
     * @brief Async convenience methods
     */
    void getAsync(const std::string& path, ResponseCallback callback, const QueryParams& params = {});
    void postAsync(const std::string& path, const json& body, ResponseCallback callback);
    
    // ==========================================================================
    // Architect API Endpoints
    // ==========================================================================
    
    // Authentication (exchange API key/secret for JWT)
    /** POST to auth endpoint with api_key/api_secret; returns JWT string or empty on failure */
    std::string login(const std::string& api_key, const std::string& api_secret);
    /** After login() returns empty, returns a short reason (e.g. "HTTP 404" or response snippet) for logging */
    std::string getLastLoginError() const;
    // User Management
    HttpResponse whoami();
    
    // Orders
    HttpResponse getOrders(const QueryParams& filters = {});
    HttpResponse getOrder(const std::string& order_id);
    /** Legacy: POST /api/orders with symbol/side/type/price/quantity (may not be supported). */
    HttpResponse createOrder(const json& order);
    /** Architect order gateway: POST to orders base URL + /place_order. Body: s, d, q, p, tif, po. */
    HttpResponse placeOrder(const json& place_order_body);
    /** Fast path: accepts pre-serialized JSON to avoid serialization overhead */
    HttpResponse placeOrderRaw(const std::string& json_body);
    
    /** Architect order gateway: POST to orders base URL + /modify_order. Body: s, oid, p, q (see Platform). */
    HttpResponse modifyOrder(const json& modify_order_body);
    /** Fast path for order modify: accepts pre-serialized JSON */
    HttpResponse modifyOrderRaw(const std::string& json_body);
    
    HttpResponse cancelOrder(const std::string& order_id);
    /** Architect order gateway: POST to orders base URL + /cancel_order. Body: {"oid": "..."} */
    HttpResponse cancelOrderGateway(const std::string& json_body);

    /**
     * Fire N place_order POSTs CONCURRENTLY on the shared curl pool (one easy handle
     * each, all driven together on this thread's CURLM). Returns one HttpResponse per
     * body, index-aligned to `bodies` — the whole batch completes in ~1 round-trip
     * instead of N sequential ones. Used by the fire-and-track batch place path.
     */
    std::vector<HttpResponse> placeOrdersRawConcurrent(const std::vector<std::string>& bodies);
    /** Same concurrency, for cancel_order gateway POSTs. Body per entry: {"oid": "..."}. */
    std::vector<HttpResponse> cancelOrdersGatewayConcurrent(const std::vector<std::string>& bodies);

    /**
     * Fire cancels AND places in ONE curl-multi batch (true c,c,p,p concurrency; the whole
     * cycle completes in ~1 round-trip). `cancel_bodies` POST to /cancel_order, `place_bodies`
     * POST to /place_order, all handles driven together. Returns responses index-aligned as
     * [0 .. cancel_bodies.size())  = cancel responses, then
     * [cancel_bodies.size() .. )  = place responses.
     * WARNING: combined batching gives up cancel-before-place ordering AT THE VENUE — the
     * caller must apply the self-trade guard (new bid >= old ask, or new ask <= old bid) and
     * fall back to cancels-batch-then-places-batch when the fresh pair could cross a still-live
     * old leg. Used by the fire-and-track cancel-replace path.
     */
    std::vector<HttpResponse> cancelThenPlaceOrdersConcurrent(
        const std::vector<std::string>& cancel_bodies,
        const std::vector<std::string>& place_bodies);
    HttpResponse cancelAllOrders(const std::optional<std::string>& symbol = std::nullopt);

    /** Order gateway GET (Bearer), e.g. path_suffix "/open-orders". */
    HttpResponse getOrdersGatewayRelative(const std::string& path_suffix, const QueryParams& params = {});
    
    // Market Data
    /** GET ``{rest_endpoint}/instruments`` — Architect gateway instrument catalog (auth Bearer). */
    HttpResponse getInstruments();
    HttpResponse getMarkets();
    HttpResponse getTicker(const std::string& symbol);
    HttpResponse getOrderbook(const std::string& symbol, int depth = 25);
    HttpResponse getTrades(const std::string& symbol, int limit = 100);
    HttpResponse getCandles(const std::string& symbol, const std::string& interval, 
                           int limit = 100);
    
    // Account
    HttpResponse getAccounts();
    HttpResponse getBalances();
    HttpResponse getPositions();
    HttpResponse getFills(const QueryParams& filters = {});
    
    // ==========================================================================
    // Rate Limiting
    // ==========================================================================
    
    /**
     * @brief Check if rate limited
     */
    [[nodiscard]] bool isRateLimited() const;
    
    /**
     * @brief Get remaining requests before rate limit
     */
    [[nodiscard]] int getRemainingRequests() const;
    
    /**
     * @brief Wait for rate limit reset (blocking)
     */
    void waitForRateLimit();
    
    // ==========================================================================
    // Statistics
    // ==========================================================================
    
    struct Stats {
        std::uint64_t total_requests    = 0;
        std::uint64_t successful_requests = 0;
        std::uint64_t failed_requests   = 0;
        std::uint64_t rate_limited      = 0;
        double avg_latency_ms           = 0.0;
    };
    
    [[nodiscard]] Stats getStats() const;
    void resetStats();
    
private:
    RestClient();
    ~RestClient();
    
    std::string buildUrl(const std::string& path, const QueryParams& params = {}) const;
    Headers buildHeaders() const;
    /** Base URL for order placement (e.g. https://gateway.architect.exchange/orders for production, https://gateway.sandbox.architect.exchange/orders for sandbox). */
    std::string getOrdersGatewayBase() const;
    void updateRateLimits(const HttpResponse& response);
    HttpResponse performRequest(const HttpRequest& request);
    /**
     * Shared driver for placeOrdersRawConcurrent / cancelOrdersGatewayConcurrent:
     * POST every body to `url` on its own pooled easy handle, all driven together via
     * CurlMultiManager::performBlockingBatch. Reuses the cached orders header list.
     */
    std::vector<HttpResponse> performOrderBatchPost(const std::string& url,
                                                    const std::vector<std::string>& bodies);
    /**
     * Per-request-URL variant of performOrderBatchPost: urls[i] is the endpoint for bodies[i],
     * so cancels (/cancel_order) and places (/place_order) can share ONE performBlockingBatch.
     * `urls` and `bodies` must be the same length. The single-URL overload delegates here.
     */
    std::vector<HttpResponse> performOrderBatchPost(const std::vector<std::string>& urls,
                                                    const std::vector<std::string>& bodies);
    
    std::string base_url_;
    std::string session_token_;
    std::chrono::milliseconds default_timeout_{30000};
    // Hard per-call caps for the shared order-gateway curl pool. Every pooled transfer
    // holds CurlMultiManager::multi_mutex_ for its whole duration, so a single black-holed
    // call freezes EVERY other instrument's quoting AND the fills poll until it returns.
    // These bound that worst case: order ops (place/cancel/modify) must finish fast on a MM
    // desk that re-quotes every ~2s; gateway GETs (fills/position) get a slightly longer cap.
    std::chrono::milliseconds order_op_timeout_{4000};
    std::chrono::milliseconds gateway_get_timeout_{8000};
    bool ssl_verify_ = true;
    
    // Rate limiting
    int rate_limit_remaining_ = 100;
    int rate_limit_total_ = 100;
    std::chrono::steady_clock::time_point rate_limit_reset_;
    
    // Statistics
    mutable std::mutex stats_mutex_;
    Stats stats_;
    
    // Last login attempt (for error reporting when login returns empty)
    mutable std::string last_login_error_;
    
    // === TRANSPORT (curl_multi-backed pool) ===
    // The two persistent CURL* handles + their mutexes have moved into
    // CurlMultiManager (see include/api/CurlMultiManager.h). Each function
    // below acquires an easy handle from the pool, runs its existing setopt
    // logic on that handle, drives it through CurlMultiManager::performBlocking
    // (synchronous from the caller's perspective — same semantics as
    // curl_easy_perform), then releases. Connection / SSL session / DNS state
    // is shared across the pool via CURLSH so warm reuse survives across
    // call sites.
    //
    // Pre-built header list for orders (avoids curl_slist_append per request).
    // Per-token state, not per-handle — stays on RestClient.
    void* orders_headers_list_ = nullptr;  // curl_slist*
    std::mutex orders_headers_mutex_;
    void rebuildOrdersHeaderList();

    // Pre-cached auth header bookkeeping (rebuilt only when token changes).
    std::string cached_auth_header_;
    std::string cached_token_for_header_;

    // Internal: perform request on a freshly-acquired pool handle.
    HttpResponse performRequestOnHandle(const HttpRequest& request, const char* handle_name);
    
    // Fast path for orders with pre-built headers
    HttpResponse performOrderRequest(const std::string& json_body);
    
    // Fast path for order modify with pre-built headers
    HttpResponse performModifyRequest(const std::string& json_body);
    
    /** POST json_body to orders gateway base + path_suffix (e.g. /cancel-all-orders). */
    HttpResponse postOrdersGatewayRelative(const std::string& path_suffix, const std::string& json_body);
};

} // namespace api
} // namespace architect
