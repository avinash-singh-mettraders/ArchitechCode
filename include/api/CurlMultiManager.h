#pragma once

/**
 * @file CurlMultiManager.h
 * @brief Single-process easy-handle pool driven through one shared CURLM/CURLSH.
 *
 * WHY THIS EXISTS
 * ===============
 * `RestClient` previously held two `CURL*` easy handles (general + orders) each
 * behind its own mutex. With a single global `MmOrderMover` worker thread that
 * sequences every cancel-replace cycle, the per-easy-handle setup couldn't share
 * connection / SSL session / DNS state between handles. A connection going stale
 * on one handle forced a fresh TLS handshake on that handle (~600-900ms cold
 * spike) even when other handles were warm.
 *
 * `CurlMultiManager` introduces:
 *   1. A pool of pre-configured easy handles (default 8) — caller acquires one,
 *      uses it for one transfer, releases it back. RAII via `Handle`.
 *   2. A single `CURLM*` (multi handle) that drives every blocking transfer.
 *      Strategy callers still see synchronous "perform → response" semantics —
 *      the polling loop is internal.
 *   3. A single `CURLSH*` (share handle) attached to every easy in the pool.
 *      DNS cache, SSL session ID cache, and connection cache are shared across
 *      handles. When handle A's keepalive connection is still warm, handle B
 *      can borrow it for its next transfer instead of doing a fresh handshake.
 *   4. A small idle-pinger thread that fires a cheap GET against the gateway
 *      every 25s when the pool has been idle, so the server-side keepalive
 *      timeout (typically 60-75s) never fires. This is the actual fix for the
 *      "first call after idle is cold" symptom — `curl_multi` alone doesn't
 *      prevent the server from closing an idle connection.
 *
 * THREAD SAFETY
 * =============
 *   - `acquire()` / `release()` are protected by `pool_mutex_` + `pool_cv_`.
 *     Acquire blocks if the pool is empty (defensive only — pool is sized for
 *     all expected concurrency, so blocking should be rare and short).
 *   - `performBlocking()` holds `multi_mutex_` for its entire run because
 *     `curl_multi_*` is NOT thread-safe with itself. Different easy handles
 *     therefore run serially through the multi loop. With the existing single
 *     `MmOrderMover` worker thread this is a non-issue: the mover is the only
 *     caller for order REST anyway.
 *   - The CURLSH share registers locking callbacks (`shareLock`/`shareUnlock`)
 *     so libcurl can safely access the shared caches from multiple threads.
 *
 * LATENCY-TRACE INTEGRITY
 * =======================
 * `curl_easy_getinfo(easy, CURLINFO_NAMELOOKUP_TIME, ...)`,
 * `CURLINFO_CONNECT_TIME`, `CURLINFO_APPCONNECT_TIME`,
 * `CURLINFO_PRETRANSFER_TIME`, `CURLINFO_STARTTRANSFER_TIME`, and
 * `CURLINFO_TOTAL_TIME` all continue to work — `curl_multi` does not change
 * the easy handle's internal timing accumulators. The `[CURL_INTERNALS]` trace
 * lines in `RestClient` keep their meaning post-refactor.
 */

#include <array>
#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include <curl/curl.h>
// Including curl/curl.h here is cheap and avoids the brittle forward-decl
// dance — libcurl's own typedefs (`CURL`, `CURLM`, `CURLSH`, `CURLcode`) are
// not all opaque structs, so forward declarations clash with the real
// definitions when curl/curl.h is later included from translation units.

namespace architect {
namespace api {

class CurlMultiManager {
public:
    /** Returned by `acquire()`. Releases its `curl` back to the pool on destruction. */
    class Handle {
    public:
        Handle() = default;
        Handle(CURL* curl, CurlMultiManager* mgr) : curl_(curl), mgr_(mgr) {}
        ~Handle();

        Handle(const Handle&) = delete;
        Handle& operator=(const Handle&) = delete;
        Handle(Handle&& other) noexcept : curl_(other.curl_), mgr_(other.mgr_) {
            other.curl_ = nullptr;
            other.mgr_ = nullptr;
        }
        Handle& operator=(Handle&& other) noexcept {
            if (this != &other) {
                release();
                curl_ = other.curl_;
                mgr_ = other.mgr_;
                other.curl_ = nullptr;
                other.mgr_ = nullptr;
            }
            return *this;
        }

        /** Raw libcurl easy handle. Unowned reference; valid until `Handle` destructs. */
        [[nodiscard]] CURL* curl() const noexcept { return curl_; }
        [[nodiscard]] bool valid() const noexcept { return curl_ != nullptr; }

    private:
        void release() noexcept;
        CURL* curl_{nullptr};
        CurlMultiManager* mgr_{nullptr};
    };

    /** Process-wide singleton. First call constructs and starts the idle pinger. */
    static CurlMultiManager& getInstance();

    CurlMultiManager(const CurlMultiManager&) = delete;
    CurlMultiManager& operator=(const CurlMultiManager&) = delete;

    /**
     * Acquire one of the pool's pre-configured easy handles. Blocks if every
     * handle is in use (rare with default pool size of 8 and the mover's
     * single-thread serialization). The returned `Handle` releases on scope exit.
     */
    Handle acquire();

    /**
     * Drive `easy` to transfer completion via `curl_multi_perform` /
     * `curl_multi_wait`. Returns the same `CURLcode` that `curl_easy_perform`
     * would have returned. Synchronous semantics preserved for callers.
     */
    CURLcode performBlocking(CURL* easy);

    /**
     * Drive MANY easy handles to completion CONCURRENTLY on this thread's own
     * CURLM. All handles are added up-front and the loop runs until every one has
     * finished, so N transfers overlap on the wire and complete in ~1 round-trip
     * instead of N sequential round-trips. Returns one CURLcode per input handle,
     * index-aligned to `easys` (CURLE_FAILED_INIT for a null handle).
     *
     * Each easy must be exclusively owned by the caller (acquired from the pool)
     * for the whole call — the same single-multi-ownership invariant performBlocking
     * relies on. Used by the order batch path (fire both legs / both cancels at once).
     */
    std::vector<CURLcode> performBlockingBatch(const std::vector<CURL*>& easys);

    /**
     * Set the gateway base URL used by the idle pinger so it can issue cheap
     * keepalive pings against the right host. Safe to call once at startup
     * after `RestClient` resolves its endpoint.
     */
    void setIdlePingUrl(std::string url);

    /**
     * Warm-connection pool tuning (Fix 1, 2026-07-13). The order batch fires N
     * requests at once; if fewer than N keep-alive connections are warm in the
     * shared connection cache, the extras open fresh TCP+TLS (~3 RTT each) and
     * the batch degrades from ~1 RTT to ~N RTT. `warm_connections` is how many
     * connections the idle keepalive keeps hot; `keepalive_touch_sec` is the
     * idle threshold before a keepalive touch fires. Call once at startup.
     */
    void configure(int warm_connections, int keepalive_touch_sec);

    /**
     * Establish `curl_warm_connections` keep-alive connections to the gateway
     * NOW (fire that many cheap GETs concurrently so the shared connection cache
     * is hot before the first order batch). Best-effort; safe to call after
     * setIdlePingUrl(). Never counts as a mover-job venue call (raw curl, not
     * routed through RestClient's counted methods).
     */
    void prewarmConnections();

private:
    CurlMultiManager();
    ~CurlMultiManager();

    void release(CURL* easy) noexcept;
    /**
     * Return this thread's own CURLM* (lazily created + registered for cleanup).
     *
     * Each calling thread drives its OWN multi handle, so different instruments'
     * order-mover threads perform transfers TRULY in parallel instead of
     * serialising on one global `multi_mutex_` (the v1 per-AX-worker freeze was
     * this contention). libcurl permits many threads to each use a distinct
     * CURLM (and easy handle) concurrently as long as one easy handle is only ever
     * on one multi at a time — which `performBlocking` guarantees (add at entry,
     * remove before return, with the easy handle exclusively owned via the pool).
     * The CURLSH `share_` (with lock callbacks) keeps the shared DNS/SSL/connection
     * caches thread-safe across all of them.
     */
    CURLM* threadLocalMulti();
    /** Background pinger loop — keeps server-side keepalive from expiring. */
    void idlePingerLoop();
    /** Issue a single pinger GET on a borrowed pool handle. Best-effort. */
    void doIdlePing();
    /**
     * Fire up to `n` cheap GETs to the ping URL CONCURRENTLY (one curl-multi
     * batch on the caller's thread), so the shared connection cache ends up with
     * ~n warm keep-alive connections. Used for startup pre-warm and the idle
     * keepalive touch. Best-effort; borrows only currently-free pool handles so
     * it never blocks real order traffic.
     */
    void doWarmBatch(int n);
    /** Apply warm-pool sizing (MAXCONNECTS / MAX_HOST_CONNECTIONS) to a multi. */
    void applyMultiConnLimits(CURLM* multi);
    /** ISO-c style locking callbacks for the CURLSH share. */
    static void shareLock(CURL* h, int data, int access, void* userp);
    static void shareUnlock(CURL* h, int data, void* userp);

    // Curl primitives.
    CURLM* multi_{nullptr};
    CURLSH* share_{nullptr};

    // Per-thread multi handles (created lazily in threadLocalMulti()). Tracked here
    // only so the destructor can reclaim every one that was handed out. Access to the
    // registry is guarded by thread_multis_mutex_; the handles themselves are never
    // touched cross-thread while alive (each is used solely by its owning thread).
    std::mutex thread_multis_mutex_;
    std::vector<CURLM*> thread_multis_;

    // Easy-handle pool. Sized generously so concurrent per-instrument mover threads
    // (plus fills/position polls + the idle pinger) can each hold a handle without
    // blocking in acquire() now that transfers run in parallel rather than serially.
    static constexpr std::size_t kDefaultPoolSize = 32;
    std::mutex pool_mutex_;
    std::condition_variable pool_cv_;
    std::vector<CURL*> free_handles_;
    std::vector<CURL*> all_handles_;          // for cleanup
    std::size_t pool_size_{kDefaultPoolSize};

    // Multi-handle access (curl_multi is not thread-safe with itself).
    std::mutex multi_mutex_;

    // CURLSH locks (one per CURL_LOCK_DATA_LAST = 7 categories — keep generous to be safe).
    std::array<std::mutex, 8> share_locks_{};

    // Idle pinger.
    std::atomic<bool> stop_pinger_{false};
    // Set by prewarmConnections() to ask the pinger thread to warm the pool
    // immediately. Keeps warming OFF the startup / order-critical path so the
    // REST client constructor never blocks on network I/O (Fix 1 hang, 2026-07-13).
    std::atomic<bool> warm_now_{false};
    std::thread pinger_thread_;
    std::atomic<std::int64_t> last_activity_ms_{0};
    std::mutex ping_url_mutex_;
    std::string ping_url_;

    // Warm-connection pool tuning (Fix 1). warm_connections_ keep-alive
    // connections are kept hot in the shared cache; keepalive_touch_sec_ is the
    // idle threshold before the keepalive touch fires a warm batch.
    std::atomic<int> warm_connections_{6};
    std::atomic<int> keepalive_touch_sec_{20};
};

} // namespace api
} // namespace architect
