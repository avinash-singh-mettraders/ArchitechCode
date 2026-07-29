/**
 * @file CurlMultiManager.cpp
 * @brief Implementation notes — see header for the design rationale.
 *
 * Two non-obvious behaviours documented here for future readers:
 *
 * 1. `performBlocking` holds `multi_mutex_` for the entire transfer. Reason:
 *    `curl_multi_add_handle`, `curl_multi_perform`, `curl_multi_info_read`,
 *    and `curl_multi_remove_handle` are NOT thread-safe with each other on the
 *    same `CURLM*`. Holding the mutex throughout is the only correct option
 *    short of a dedicated multi-driver thread (which the system explicitly
 *    forbids). `MmOrderMover` serialises every ORDER REST onto one thread, but
 *    the fills/position polls run on OTHER threads and share this same pool —
 *    so the mutex DOES contend at runtime, and a single unbounded transfer would
 *    block every other instrument's quoting plus the polls for its full duration.
 *    That is why callers cap each transfer with a tight per-call CURLOPT_TIMEOUT_MS
 *    (see RestClient order_op_timeout_ / gateway_get_timeout_) and this handle
 *    default is only a 10s backstop, not the old 30s.
 *
 * 2. The idle pinger fires a `GET` on a low-cost endpoint every ~25s only
 *    when there has been NO `acquire()` call for at least 20s. This stops the
 *    server-side keep-alive timer (typical 60-75s) from closing our pooled
 *    TCP/TLS connection. `curl_multi` + `CURLSH` share the connection cache
 *    across the pool, so a single warm connection benefits every easy handle.
 */

#include "api/CurlMultiManager.h"

#include <algorithm>
#include <chrono>
#include <cstring>
#include <curl/curl.h>
#include <iostream>
#include <vector>

namespace architect {
namespace api {

namespace {

/** Same low-latency option set RestClient used to apply per-handle. */
void initEasyHandleForLowLatency(CURL* curl) {
    if (!curl) return;
    curl_easy_setopt(curl, CURLOPT_FORBID_REUSE, 0L);
    curl_easy_setopt(curl, CURLOPT_FRESH_CONNECT, 0L);
    curl_easy_setopt(curl, CURLOPT_TCP_NODELAY, 1L);
    curl_easy_setopt(curl, CURLOPT_TCP_KEEPALIVE, 1L);
    curl_easy_setopt(curl, CURLOPT_TCP_KEEPIDLE, 30L);
    curl_easy_setopt(curl, CURLOPT_TCP_KEEPINTVL, 15L);
#ifdef CURLOPT_TCP_FASTOPEN
    curl_easy_setopt(curl, CURLOPT_TCP_FASTOPEN, 1L);
#endif
    curl_easy_setopt(curl, CURLOPT_DNS_CACHE_TIMEOUT, 300L);
    curl_easy_setopt(curl, CURLOPT_BUFFERSIZE, 102400L);
#ifdef CURLOPT_UPLOAD_BUFFERSIZE
    curl_easy_setopt(curl, CURLOPT_UPLOAD_BUFFERSIZE, 65536L);
#endif
    curl_easy_setopt(curl, CURLOPT_HTTP_VERSION, CURL_HTTP_VERSION_1_1);
    curl_easy_setopt(curl, CURLOPT_SSL_SESSIONID_CACHE, 1L);
    curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 1L);
    curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 2L);
    curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT_MS, 5000L);
    // Backstop only: every real caller (order place/cancel/modify, gateway GETs) sets a
    // tighter per-call CURLOPT_TIMEOUT_MS. This default just guarantees that any path that
    // forgets cannot hold multi_mutex_ — and thus freeze the whole desk — for the old 30s.
    curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, 10000L);
    curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L);
}

std::int64_t nowMs() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

} // namespace

// ============================================================================
// Handle (RAII wrapper) implementation
// ============================================================================

CurlMultiManager::Handle::~Handle() { release(); }

void CurlMultiManager::Handle::release() noexcept {
    if (curl_ && mgr_) {
        mgr_->release(curl_);
    }
    curl_ = nullptr;
    mgr_ = nullptr;
}

// ============================================================================
// CurlMultiManager
// ============================================================================

CurlMultiManager& CurlMultiManager::getInstance() {
    static CurlMultiManager inst;
    return inst;
}

CurlMultiManager::CurlMultiManager() {
    // libcurl global init must run exactly once per process. RestClient also
    // calls curl_global_init — libcurl handles this idempotently as long as
    // the matching cleanup count balances. Both call sites use the same flags.
    curl_global_init(CURL_GLOBAL_ALL);

    multi_ = curl_multi_init();
    applyMultiConnLimits(multi_);

    share_ = curl_share_init();
    if (share_) {
        curl_share_setopt(share_, CURLSHOPT_LOCKFUNC, &CurlMultiManager::shareLock);
        curl_share_setopt(share_, CURLSHOPT_UNLOCKFUNC, &CurlMultiManager::shareUnlock);
        curl_share_setopt(share_, CURLSHOPT_USERDATA, this);
        // Share DNS and SSL-session caches across every easy handle so even a
        // genuinely new TCP connection skips DNS and does an abbreviated TLS
        // (session resumption) handshake.
        curl_share_setopt(share_, CURLSHOPT_SHARE, CURL_LOCK_DATA_DNS);
        curl_share_setopt(share_, CURLSHOPT_SHARE, CURL_LOCK_DATA_SSL_SESSION);
        // NOTE (batch->1 RTT fix, 2026-07-13): we deliberately do NOT share the
        // CONNECTION cache (CURL_LOCK_DATA_CONNECT). A shared connection cache
        // makes libcurl IGNORE the per-multi CURLMOPT_MAXCONNECTS /
        // MAX_HOST_CONNECTIONS limits and, in practice, retains only ~1 idle
        // keep-alive connection per host across the multis. That capped every
        // order batch at 1 reused leg + N-1 fresh TCP+TLS handshakes
        // (batch_rtt ~= N*RTT). With the connection cache PER multi instead,
        // each mover thread keeps up to `want` (>=8) idle connections warm in
        // its own cache, so a 4-leg cancel+place batch reuses all four
        // (reused=1, connect=0, tls=0) and completes in ~1 venue RTT. All order
        // batches run on a single mover thread's multi anyway, so cross-thread
        // connection borrowing was never needed for the hot path.
    }

    // Pre-fill the pool. Each handle gets the same low-latency option set
    // RestClient used to configure on its two private handles.
    free_handles_.reserve(pool_size_);
    all_handles_.reserve(pool_size_);
    for (std::size_t i = 0; i < pool_size_; ++i) {
        CURL* h = curl_easy_init();
        if (!h) continue;
        initEasyHandleForLowLatency(h);
        if (share_) {
            curl_easy_setopt(h, CURLOPT_SHARE, share_);
        }
        free_handles_.push_back(h);
        all_handles_.push_back(h);
    }
    last_activity_ms_.store(nowMs(), std::memory_order_relaxed);

    // Idle pinger thread: fires a cheap GET when the pool is idle so the
    // server-side keepalive doesn't close our pooled connection.
    pinger_thread_ = std::thread([this]() { idlePingerLoop(); });

    std::cout << "[CURL_MULTI] init: pool=" << all_handles_.size()
              << " share=" << (share_ ? "on" : "off")
              << " multi=" << (multi_ ? "on" : "off") << std::endl;
}

CurlMultiManager::~CurlMultiManager() {
    stop_pinger_.store(true, std::memory_order_release);
    if (pinger_thread_.joinable()) {
        pinger_thread_.join();
    }

    // Reclaim every easy handle (acquired or not). We hold the multi mutex
    // because curl_easy_cleanup on a handle still attached to a multi can
    // race the next perform.
    {
        std::lock_guard<std::mutex> lk(multi_mutex_);
        for (CURL* h : all_handles_) {
            // Defensive: remove from multi if still attached. Idempotent.
            if (multi_ && h) {
                curl_multi_remove_handle(multi_, h);
            }
            if (h) {
                curl_easy_cleanup(h);
            }
        }
        all_handles_.clear();
        free_handles_.clear();

        if (multi_) {
            curl_multi_cleanup(multi_);
            multi_ = nullptr;
        }
    }

    // Reclaim every per-thread multi handed out via threadLocalMulti(). By this point
    // all worker threads have been joined (shutdown), so nothing is mid-transfer on
    // any of them and no easy handle remains attached.
    {
        std::lock_guard<std::mutex> lk(thread_multis_mutex_);
        for (CURLM* m : thread_multis_) {
            if (m) {
                curl_multi_cleanup(m);
            }
        }
        thread_multis_.clear();
    }

    if (share_) {
        // No more easy handles reference it now.
        curl_share_cleanup(share_);
        share_ = nullptr;
    }
    // We do NOT call curl_global_cleanup() here — RestClient's destructor
    // owns that to keep the existing init/cleanup balance intact.
}

CurlMultiManager::Handle CurlMultiManager::acquire() {
    std::unique_lock<std::mutex> lk(pool_mutex_);
    pool_cv_.wait(lk, [this]() { return !free_handles_.empty(); });
    CURL* h = free_handles_.back();
    free_handles_.pop_back();
    last_activity_ms_.store(nowMs(), std::memory_order_relaxed);
    return Handle(h, this);
}

void CurlMultiManager::release(CURL* easy) noexcept {
    if (!easy) return;
    {
        std::lock_guard<std::mutex> lk(pool_mutex_);
        free_handles_.push_back(easy);
        last_activity_ms_.store(nowMs(), std::memory_order_relaxed);
    }
    pool_cv_.notify_one();
}

void CurlMultiManager::applyMultiConnLimits(CURLM* multi) {
    if (!multi) return;
    // Allow the (per-thread) connection cache to hold at least the warm count so a
    // batch of N legs can each reuse a keep-alive connection instead of evicting one
    // another. Ignored when the connection cache is shared via CURLSH, but harmless
    // to set and correct if sharing is ever removed. Keep a floor so tiny warm
    // counts still leave room for concurrent order+cancel legs.
    const long want = static_cast<long>(std::max(8, warm_connections_.load(std::memory_order_relaxed) + 2));
    curl_multi_setopt(multi, CURLMOPT_MAXCONNECTS, want);
#ifdef CURLMOPT_MAX_HOST_CONNECTIONS
    curl_multi_setopt(multi, CURLMOPT_MAX_HOST_CONNECTIONS, want);
#endif
#ifdef CURLMOPT_MAX_TOTAL_CONNECTIONS
    curl_multi_setopt(multi, CURLMOPT_MAX_TOTAL_CONNECTIONS, want);
#endif
}

CURLM* CurlMultiManager::threadLocalMulti() {
    // One CURLM* per calling thread. Shared across the (singleton) manager, so the
    // function-local thread_local is unambiguous. Raw pointer: reclaimed in the
    // destructor via the thread_multis_ registry (mover threads are joined at
    // shutdown before the singleton is torn down at process exit).
    thread_local CURLM* tl_multi = nullptr;
    if (!tl_multi) {
        tl_multi = curl_multi_init();
        if (tl_multi) {
            applyMultiConnLimits(tl_multi);
            std::lock_guard<std::mutex> lk(thread_multis_mutex_);
            thread_multis_.push_back(tl_multi);
        }
    }
    return tl_multi;
}

CURLcode CurlMultiManager::performBlocking(CURL* easy) {
    if (!easy) {
        return CURLE_FAILED_INIT;
    }

    // Per-thread multi handle: NO global multi_mutex_ here. Different threads drive
    // their own multi concurrently, so an order op on one instrument no longer blocks
    // every other instrument's transfer for its full duration. The easy handle is
    // exclusively owned (acquired from the pool) for the whole add→perform→remove
    // window, so it is never on two multis at once.
    CURLM* multi = threadLocalMulti();
    if (!multi) {
        return CURLE_FAILED_INIT;
    }

    CURLMcode mc = curl_multi_add_handle(multi, easy);
    if (mc != CURLM_OK) {
        return CURLE_FAILED_INIT;
    }

    int still_running = 0;
    CURLcode result = CURLE_OK;
    bool finished = false;

    // First perform kicks off the transfer. Subsequent loop alternates
    // wait-for-IO-or-timeout with another perform until our easy completes.
    do {
        mc = curl_multi_perform(multi, &still_running);
        if (mc != CURLM_OK) {
            result = CURLE_RECV_ERROR;
            break;
        }

        // Drain finished messages. This multi is thread-private, so ours is the
        // only easy handle attached — but stay defensive and match on easy_handle.
        int msgs_left = 0;
        CURLMsg* msg = nullptr;
        while ((msg = curl_multi_info_read(multi, &msgs_left)) != nullptr) {
            if (msg->msg == CURLMSG_DONE && msg->easy_handle == easy) {
                result = msg->data.result;
                finished = true;
            }
        }

        if (finished) break;

        if (still_running > 0) {
            // numfds is reserved for future debug; not used.
            int numfds = 0;
            // 100ms slice — short enough that a perform stuck on a dead conn
            // gets noticed quickly, long enough to avoid CPU burn.
            mc = curl_multi_wait(multi, nullptr, 0, 100, &numfds);
            if (mc != CURLM_OK) {
                result = CURLE_RECV_ERROR;
                break;
            }
        }
    } while (still_running > 0);

    curl_multi_remove_handle(multi, easy);
    last_activity_ms_.store(nowMs(), std::memory_order_relaxed);
    return result;
}

std::vector<CURLcode> CurlMultiManager::performBlockingBatch(const std::vector<CURL*>& easys) {
    // Pending sentinel: a successfully-added handle stays CURLE_RECV_ERROR until its
    // CURLMSG_DONE overwrites the slot with the real result. So if the drive loop
    // breaks early (curl error), an unfinished transfer is reported as a failure,
    // never a phantom success. Null / add-failure slots are CURLE_FAILED_INIT.
    std::vector<CURLcode> results(easys.size(), CURLE_FAILED_INIT);

    // Per-thread multi (same rationale as performBlocking): this thread drives its
    // own CURLM, so a batch on one instrument's mover does not serialise against
    // another instrument's transfers or the fills/position polls.
    CURLM* multi = threadLocalMulti();
    if (!multi) {
        return results;  // all CURLE_FAILED_INIT
    }

    // Add every valid handle up-front so all transfers run concurrently.
    std::vector<CURL*> added;
    added.reserve(easys.size());
    for (std::size_t i = 0; i < easys.size(); ++i) {
        CURL* easy = easys[i];
        if (!easy) {
            continue;  // stays CURLE_FAILED_INIT
        }
        if (curl_multi_add_handle(multi, easy) != CURLM_OK) {
            continue;  // stays CURLE_FAILED_INIT
        }
        results[i] = CURLE_RECV_ERROR;  // pending sentinel until DONE
        added.push_back(easy);
    }
    if (added.empty()) {
        return results;
    }

    std::size_t pending = added.size();
    int still_running = 0;
    do {
        if (curl_multi_perform(multi, &still_running) != CURLM_OK) {
            break;  // leave un-DONE handles at the pending sentinel (failure)
        }

        int msgs_left = 0;
        CURLMsg* msg = nullptr;
        while ((msg = curl_multi_info_read(multi, &msgs_left)) != nullptr) {
            if (msg->msg != CURLMSG_DONE) {
                continue;
            }
            for (std::size_t i = 0; i < easys.size(); ++i) {
                if (easys[i] == msg->easy_handle) {
                    results[i] = msg->data.result;  // real result overwrites sentinel
                    if (pending > 0) --pending;
                    break;
                }
            }
        }

        if (pending == 0) {
            break;
        }

        if (still_running > 0) {
            int numfds = 0;
            // 100ms slice — matches performBlocking so a dead connection is noticed
            // quickly without burning CPU.
            if (curl_multi_wait(multi, nullptr, 0, 100, &numfds) != CURLM_OK) {
                break;
            }
        }
    } while (still_running > 0 || pending > 0);

    for (CURL* easy : added) {
        curl_multi_remove_handle(multi, easy);
    }
    last_activity_ms_.store(nowMs(), std::memory_order_relaxed);
    return results;
}

void CurlMultiManager::setIdlePingUrl(std::string url) {
    std::lock_guard<std::mutex> lk(ping_url_mutex_);
    ping_url_ = std::move(url);
}

void CurlMultiManager::configure(int warm_connections, int keepalive_touch_sec) {
    warm_connections_.store(std::max(0, warm_connections), std::memory_order_relaxed);
    keepalive_touch_sec_.store(std::max(1, keepalive_touch_sec), std::memory_order_relaxed);
    // Re-apply limits to the (constructor-created) main multi; per-thread multis pick
    // it up when they are lazily created.
    applyMultiConnLimits(multi_);
    std::cout << "[CURL_MULTI] configure: warm_connections=" << warm_connections_.load()
              << " keepalive_touch_sec=" << keepalive_touch_sec_.load() << std::endl;
}

void CurlMultiManager::prewarmConnections() {
    const int n = warm_connections_.load(std::memory_order_relaxed);
    if (n <= 0) return;
    // NON-BLOCKING: never do network I/O on the caller's thread. The RestClient
    // constructor runs inside the startup sequence (STEP 7) before the session /
    // network is guaranteed ready; a synchronous warm batch here would stall the
    // whole boot (observed hang, 2026-07-13). Hand the warm to the pinger thread,
    // which fires it within ~1s and re-warms on idle thereafter.
    warm_now_.store(true, std::memory_order_release);
    std::cout << "[CURL_MULTI] prewarm: scheduled " << n
              << " keep-alive connection(s) (async, off critical path)" << std::endl;
}

void CurlMultiManager::idlePingerLoop() {
    using namespace std::chrono_literals;
    while (!stop_pinger_.load(std::memory_order_acquire)) {
        // Responsive wait: sleep in short slices so a prewarm request (warm_now_)
        // is serviced within ~250ms and shutdown is prompt, instead of blocking a
        // full 5s. ~5s total between idle-keepalive checks when nothing is pending.
        for (int i = 0; i < 20; ++i) {
            if (stop_pinger_.load(std::memory_order_acquire)) return;
            if (warm_now_.load(std::memory_order_acquire)) break;
            std::this_thread::sleep_for(250ms);
        }
        if (stop_pinger_.load(std::memory_order_acquire)) break;

        const bool immediate = warm_now_.exchange(false, std::memory_order_acq_rel);

        std::string url;
        {
            std::lock_guard<std::mutex> lk(ping_url_mutex_);
            url = ping_url_;
        }
        if (url.empty()) {
            // Ping URL not resolved yet — re-arm so the requested prewarm still
            // happens once RestClient calls setIdlePingUrl(). Sleep first so we do
            // not busy-spin (warm_now_ short-circuits the wait loop above).
            if (immediate) {
                warm_now_.store(true, std::memory_order_release);
                std::this_thread::sleep_for(250ms);
            }
            continue;
        }

        if (immediate) {
            // Startup / explicit prewarm: warm the pool now.
            doWarmBatch(warm_connections_.load(std::memory_order_relaxed));
            continue;
        }

        const std::int64_t touch_ms =
            static_cast<std::int64_t>(keepalive_touch_sec_.load(std::memory_order_relaxed)) * 1000;
        const std::int64_t idle_ms = nowMs() - last_activity_ms_.load(std::memory_order_relaxed);
        // Idle beyond the touch threshold → keep the WARM POOL hot. Server keepalive
        // is typically 60-75s. Firing warm_connections concurrent touches (not one)
        // keeps ~N connections warm in the shared cache so the next order batch of N
        // legs each reuses one instead of handshaking (~3 RTT) — the batch → 1 RTT fix.
        if (idle_ms < touch_ms) continue;

        doWarmBatch(warm_connections_.load(std::memory_order_relaxed));
    }
}

void CurlMultiManager::doWarmBatch(int n) {
    if (n <= 0) return;
    std::string url;
    {
        std::lock_guard<std::mutex> lk(ping_url_mutex_);
        url = ping_url_;
    }
    if (url.empty()) return;

    // Borrow only currently-free handles — never block real order traffic. If the
    // pool is busy, the traffic itself is keeping connections warm, so warming fewer
    // (or none) is fine.
    std::vector<CURL*> borrowed;
    {
        std::unique_lock<std::mutex> lk(pool_mutex_);
        const int take = std::min<int>(n, static_cast<int>(free_handles_.size()));
        for (int i = 0; i < take; ++i) {
            borrowed.push_back(free_handles_.back());
            free_handles_.pop_back();
        }
    }
    if (borrowed.empty()) return;

    static auto sink = +[](char*, size_t size, size_t nmemb, void*) -> size_t {
        return size * nmemb;
    };
    for (CURL* h : borrowed) {
        curl_easy_setopt(h, CURLOPT_URL, url.c_str());
        curl_easy_setopt(h, CURLOPT_HTTPGET, 1L);
        curl_easy_setopt(h, CURLOPT_CUSTOMREQUEST, "GET");
        curl_easy_setopt(h, CURLOPT_POST, 0L);
        curl_easy_setopt(h, CURLOPT_POSTFIELDS, nullptr);
        curl_easy_setopt(h, CURLOPT_WRITEFUNCTION, sink);
        curl_easy_setopt(h, CURLOPT_WRITEDATA, nullptr);
        curl_easy_setopt(h, CURLOPT_HEADERFUNCTION, nullptr);
        curl_easy_setopt(h, CURLOPT_HEADERDATA, nullptr);
        curl_easy_setopt(h, CURLOPT_HTTPHEADER, nullptr);
        curl_easy_setopt(h, CURLOPT_TIMEOUT_MS, 3000L);
    }

    // Fire them all at once so N distinct connections open/refresh in parallel.
    (void)performBlockingBatch(borrowed);

    {
        std::lock_guard<std::mutex> lk(pool_mutex_);
        for (CURL* h : borrowed) {
            free_handles_.push_back(h);
        }
    }
    pool_cv_.notify_all();
}

void CurlMultiManager::doIdlePing() {
    // Steal a handle from the pool (block briefly if all are in use, but skip
    // if pool is contended for too long — pinger must never delay real work).
    CURL* h = nullptr;
    {
        std::unique_lock<std::mutex> lk(pool_mutex_);
        if (free_handles_.empty()) {
            // Pool busy with real traffic; we don't need to ping — that traffic
            // already keeps connections warm.
            return;
        }
        h = free_handles_.back();
        free_handles_.pop_back();
    }

    std::string url;
    {
        std::lock_guard<std::mutex> lk(ping_url_mutex_);
        url = ping_url_;
    }

    // Discard the response body silently.
    static auto sink = +[](char*, size_t size, size_t nmemb, void*) -> size_t {
        return size * nmemb;
    };

    curl_easy_setopt(h, CURLOPT_URL, url.c_str());
    curl_easy_setopt(h, CURLOPT_HTTPGET, 1L);
    curl_easy_setopt(h, CURLOPT_CUSTOMREQUEST, "GET");
    curl_easy_setopt(h, CURLOPT_POST, 0L);
    curl_easy_setopt(h, CURLOPT_POSTFIELDS, nullptr);
    curl_easy_setopt(h, CURLOPT_WRITEFUNCTION, sink);
    curl_easy_setopt(h, CURLOPT_WRITEDATA, nullptr);
    curl_easy_setopt(h, CURLOPT_HEADERFUNCTION, nullptr);
    curl_easy_setopt(h, CURLOPT_HEADERDATA, nullptr);
    curl_easy_setopt(h, CURLOPT_HTTPHEADER, nullptr);
    curl_easy_setopt(h, CURLOPT_TIMEOUT_MS, 3000L);

    (void)performBlocking(h);

    // Return the handle to the pool. We deliberately do NOT log success/fail —
    // the pinger is best-effort and a transient failure is irrelevant.
    {
        std::lock_guard<std::mutex> lk(pool_mutex_);
        free_handles_.push_back(h);
    }
    pool_cv_.notify_one();
}

void CurlMultiManager::shareLock(CURL* /*h*/, int data, int /*access*/, void* userp) {
    auto* self = static_cast<CurlMultiManager*>(userp);
    if (!self) return;
    const std::size_t idx = static_cast<std::size_t>(data) % self->share_locks_.size();
    self->share_locks_[idx].lock();
}

void CurlMultiManager::shareUnlock(CURL* /*h*/, int data, void* userp) {
    auto* self = static_cast<CurlMultiManager*>(userp);
    if (!self) return;
    const std::size_t idx = static_cast<std::size_t>(data) % self->share_locks_.size();
    self->share_locks_[idx].unlock();
}

} // namespace api
} // namespace architect
