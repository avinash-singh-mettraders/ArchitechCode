// =============================================================================
// Pillar C — continuous ms fills+positions poller (FastPoller).
//
// This test links the REAL production component (src/core/FastPoller.cpp) and
// drives it with a mock venue supplied through the fetch callbacks (FastPoller
// is deliberately decoupled from RestClient for exactly this reason). It proves
// the operator's Pillar C spec:
//
//   1. Back-to-back cadence: with polling.*_interval_ms == 0 the poller issues a
//      new request as soon as the previous one is applied, so the natural
//      cadence == mock venue RTT (many polls in a short window), and fills and
//      positions INTERLEAVE (fills -> positions -> fills -> positions).
//   2. Handoff ordering is FIFO: bodies arrive at the push sink in send order.
//   3. Rate governor: a mock 429 (+Retry-After) drives exponential backoff
//      (backoffs++ , throttled=1, POLLER_BACKOFF), and the poller RECOVERS
//      (throttled=0, pushes resume) once the venue stops rate-limiting. The
//      budget is sacrificial — never an error, just backoff.
//   4. Self-healing: a poller thread that keeps throwing is respawned up to
//      max_respawns times (1s spacing in prod; compressed here) and then FALLS
//      BACK (fellBack()==true) instead of silently stopping.
//   5. Saturation isolation (TSAN): the poller runs flat-out (back-to-back)
//      while a concurrent "mover" thread hammers its own state and reads the
//      poller stats — no shared lock is held across an RTT, so there is no
//      contention and TSAN stays clean.
//
// Build under ThreadSanitizer to satisfy Tests §6 (saturation + concurrent
// mover, TSAN clean):
//   cmake -S . -B build -DBUILD_TESTS=ON -DENABLE_TSAN=ON
//   ctest --test-dir build -R concurrency_test_fast_poller --output-on-failure

#include "test_helpers.h"

#include "core/FastPoller.h"

#include <atomic>
#include <chrono>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

using architect::core::FastPoller;

namespace {

// Ordered record of everything pushed to the main-loop sink.
struct PushSink {
    std::mutex mu;
    std::vector<bool> is_fills_seq;       // interleave witness
    std::vector<std::int64_t> seq_tags;   // FIFO witness (monotonic per stream tag)
    std::atomic<int> fills{0};
    std::atomic<int> positions{0};

    void push(bool is_fills, std::string body, std::int64_t /*start_ts*/, std::int64_t /*fetch_ms*/) {
        std::lock_guard<std::mutex> lk(mu);
        is_fills_seq.push_back(is_fills);
        // body is "F:<n>" or "P:<n>" — decode the monotonic tag for FIFO check.
        std::int64_t tag = 0;
        try {
            tag = std::stoll(body.substr(2));
        } catch (...) {
        }
        seq_tags.push_back(tag);
        (is_fills ? fills : positions).fetch_add(1, std::memory_order_relaxed);
    }
};

FastPoller::Config fastCfg() {
    FastPoller::Config c;
    c.enabled = true;
    c.fills_interval_ms = 0;       // back-to-back
    c.positions_interval_ms = 0;   // back-to-back
    c.poll_min_interval_ms = 0;
    c.backoff_base_ms = 20;        // compressed governor for a fast test
    c.backoff_cap_ms = 80;
    c.heartbeat_ms = 100000;       // suppress heartbeat noise in tests
    c.max_respawns = 3;
    c.respawn_spacing_ms = 5;      // compressed respawn spacing
    return c;
}

// --- Test 1+2: back-to-back cadence, interleave, FIFO ordering ---------------
void test_backToBack_interleave_fifo() {
    PushSink sink;
    std::atomic<std::int64_t> fill_tag{0};
    std::atomic<std::int64_t> pos_tag{0};
    constexpr int kRttMs = 5;

    auto fetch_fills = [&](std::int64_t& out_start_ts_ns) {
        out_start_ts_ns = 0;
        std::this_thread::sleep_for(std::chrono::milliseconds(kRttMs));  // model the RTT
        FastPoller::FetchResult r;
        r.ok = true;
        r.status_code = 200;
        r.body = "F:" + std::to_string(fill_tag.fetch_add(1) + 1);
        return r;
    };
    auto fetch_positions = [&]() {
        std::this_thread::sleep_for(std::chrono::milliseconds(kRttMs));
        FastPoller::FetchResult r;
        r.ok = true;
        r.status_code = 200;
        r.body = "P:" + std::to_string(pos_tag.fetch_add(1) + 1);
        return r;
    };
    auto push = [&](bool f, std::string b, std::int64_t s, std::int64_t m) {
        sink.push(f, std::move(b), s, m);
    };
    auto nolog = [](const std::string&) {};

    FastPoller p(fastCfg(), fetch_fills, fetch_positions, push, nolog, nolog);
    p.start();
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    p.stop();

    // Back-to-back: window/RTT_per_cycle cycles. Each cycle ~= 2*kRttMs. 300ms
    // => ~30 cycles. Be generous for CI: require a clearly-continuous cadence
    // (far more than a fixed 300ms-tick poller's ~1 poll).
    TX_REQUIRE(sink.fills.load() >= 8);
    TX_REQUIRE(sink.positions.load() >= 8);

    // Interleave: fills and positions alternate closely — neither stream ever
    // runs away from the other (bounded imbalance across the whole run).
    const int imbalance = std::abs(sink.fills.load() - sink.positions.load());
    TX_REQUIRE(imbalance <= 2);

    // FIFO: within the combined push order, the fills tags are strictly
    // increasing and so are the positions tags (nothing reordered).
    std::lock_guard<std::mutex> lk(sink.mu);
    std::int64_t last_f = 0, last_p = 0;
    for (size_t i = 0; i < sink.is_fills_seq.size(); ++i) {
        if (sink.is_fills_seq[i]) {
            TX_REQUIRE(sink.seq_tags[i] > last_f);
            last_f = sink.seq_tags[i];
        } else {
            TX_REQUIRE(sink.seq_tags[i] > last_p);
            last_p = sink.seq_tags[i];
        }
    }
    // And the very first cycle interleaves fills->positions.
    TX_REQUIRE(sink.is_fills_seq.size() >= 2);
    TX_REQUIRE(sink.is_fills_seq[0] == true);
    TX_REQUIRE(sink.is_fills_seq[1] == false);
}

// --- Test 3: rate governor engages on 429 then recovers ---------------------
void test_governor_backoff_and_recovery() {
    PushSink sink;
    std::atomic<bool> rate_limit_on{true};
    std::atomic<int> backoff_logs{0};

    auto fetch_fills = [&](std::int64_t& out_start_ts_ns) {
        out_start_ts_ns = 0;
        FastPoller::FetchResult r;
        if (rate_limit_on.load()) {
            r.ok = false;
            r.status_code = 429;
            r.rate_limited = true;
            r.retry_after_ms = 10;  // small honored backoff
            return r;
        }
        r.ok = true;
        r.status_code = 200;
        r.body = "F:1";
        return r;
    };
    auto fetch_positions = [&]() {
        FastPoller::FetchResult r;
        if (rate_limit_on.load()) {
            r.ok = false;
            r.status_code = 429;
            r.rate_limited = true;
            r.retry_after_ms = 10;
            return r;
        }
        r.ok = true;
        r.status_code = 200;
        r.body = "P:1";
        return r;
    };
    auto push = [&](bool f, std::string b, std::int64_t s, std::int64_t m) {
        sink.push(f, std::move(b), s, m);
    };
    auto log_warn = [&](const std::string& m) {
        if (m.find("POLLER_BACKOFF") != std::string::npos) {
            backoff_logs.fetch_add(1, std::memory_order_relaxed);
        }
    };
    auto nolog = [](const std::string&) {};

    FastPoller p(fastCfg(), fetch_fills, fetch_positions, push, nolog, log_warn);
    p.start();

    // Phase 1: rate-limited. Expect backoffs to accrue, throttled to latch.
    std::this_thread::sleep_for(std::chrono::milliseconds(150));
    TX_REQUIRE(p.backoffs() > 0);
    TX_REQUIRE(backoff_logs.load() > 0);   // POLLER_BACKOFF logged (calmly)
    TX_REQUIRE(p.throttled() == true);
    TX_REQUIRE(sink.fills.load() == 0);    // nothing applied while rate-limited

    // Phase 2: venue recovers. Poller must resume pushing and clear throttle.
    rate_limit_on.store(false);
    std::this_thread::sleep_for(std::chrono::milliseconds(150));
    p.stop();

    TX_REQUIRE(sink.fills.load() > 0);       // recovered — data flows again
    TX_REQUIRE(p.throttled() == false);      // throttle cleared on success
}

// --- Test 4: respawn-on-death x3 then fallback ------------------------------
void test_respawn_then_fallback() {
    PushSink sink;
    std::atomic<int> throws{0};

    auto fetch_fills = [&](std::int64_t&) -> FastPoller::FetchResult {
        throws.fetch_add(1, std::memory_order_relaxed);
        throw std::runtime_error("mock venue exploded");
    };
    auto fetch_positions = [&]() -> FastPoller::FetchResult { return {}; };
    auto push = [&](bool f, std::string b, std::int64_t s, std::int64_t m) {
        sink.push(f, std::move(b), s, m);
    };
    auto nolog = [](const std::string&) {};

    FastPoller p(fastCfg(), fetch_fills, fetch_positions, push, nolog, nolog);
    p.start();

    // Poll loop throws immediately; supervisor respawns max_respawns (3) times
    // with 5ms spacing, then falls back. Give it comfortably long.
    for (int i = 0; i < 200 && !p.fellBack(); ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    p.stop();

    TX_REQUIRE(p.fellBack() == true);       // never silently stopped
    TX_EQ(static_cast<int>(p.respawns()), 3);
    TX_REQUIRE(throws.load() >= 4);         // initial + 3 respawn attempts
    TX_REQUIRE(p.running() == false);
}

// --- Test 5: saturation + concurrent mover (TSAN) ---------------------------
void test_saturation_isolation() {
    PushSink sink;
    std::atomic<bool> stop{false};
    std::atomic<std::int64_t> mover_work{0};

    auto fetch_fills = [&](std::int64_t& out) {
        out = 0;
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
        FastPoller::FetchResult r;
        r.ok = true;
        r.status_code = 200;
        r.body = "F:1";
        return r;
    };
    auto fetch_positions = [&]() {
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
        FastPoller::FetchResult r;
        r.ok = true;
        r.status_code = 200;
        r.body = "P:1";
        return r;
    };
    auto push = [&](bool f, std::string b, std::int64_t s, std::int64_t m) {
        sink.push(f, std::move(b), s, m);
    };
    auto nolog = [](const std::string&) {};

    FastPoller p(fastCfg(), fetch_fills, fetch_positions, push, nolog, nolog);
    p.start();

    // A concurrent "mover-like" thread that touches its own state AND reads the
    // poller's public stats (the only shared surface). If the poller held a
    // lock across the RTT this would contend; it must not.
    std::thread mover([&] {
        while (!stop.load(std::memory_order_acquire)) {
            mover_work.fetch_add(1, std::memory_order_relaxed);
            (void)p.fillsPolls();
            (void)p.positionsPolls();
            (void)p.medianRttMs();
            (void)p.throttled();
        }
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    stop.store(true, std::memory_order_release);
    mover.join();
    p.stop();

    // Both made progress concurrently; no deadlock, no crash (TSAN proves clean).
    TX_REQUIRE(mover_work.load() > 0);
    TX_REQUIRE(sink.fills.load() > 0);
    TX_REQUIRE(sink.positions.load() > 0);
}

// --- Test 6: self-throttle vs a robust reference (min/expected), recovers -----
// Sustained RTT degradation (median > hi*ref for >= min_samples) must halve the
// poll frequency (POLLER_SELF_THROTTLE) and latch selfThrottled(); once the RTT
// recovers below lo*ref the poller must un-throttle. The reference is anchored to
// min(expected_rtt_ms, session-min) so it is not blinded by a median that
// baselines at the already-throttled value.
void test_self_throttle_engages_and_recovers() {
    PushSink sink;
    std::atomic<int> rtt_ms{8};   // clean baseline
    std::atomic<int> throttle_logs{0};

    auto mk = [&](const char* tag) {
        std::this_thread::sleep_for(std::chrono::milliseconds(rtt_ms.load()));
        FastPoller::FetchResult r;
        r.ok = true;
        r.status_code = 200;
        r.body = tag;
        return r;
    };
    auto fetch_fills = [&](std::int64_t& o) { o = 0; return mk("F:1"); };
    auto fetch_positions = [&]() { return mk("P:1"); };
    auto push = [&](bool f, std::string b, std::int64_t s, std::int64_t m) {
        sink.push(f, std::move(b), s, m);
    };
    auto log_warn = [&](const std::string& m) {
        if (m.find("POLLER_SELF_THROTTLE") != std::string::npos) {
            throttle_logs.fetch_add(1, std::memory_order_relaxed);
        }
    };
    auto nolog = [](const std::string&) {};

    FastPoller::Config c = fastCfg();  // back-to-back sampling
    c.expected_rtt_ms = 100;           // ref anchors to the (much lower) session min
    c.self_throttle_min_samples = 5;   // compress the window for a fast test
    FastPoller p(c, fetch_fills, fetch_positions, push, nolog, log_warn);
    p.start();

    // Phase 0: clean baseline — ref tracks the session min (~8ms), NOT expected(100).
    std::this_thread::sleep_for(std::chrono::milliseconds(250));
    TX_REQUIRE(p.minRttMs() < 100);          // anchored to session min, not expected
    TX_REQUIRE(p.refRttMs() < 100);

    // Phase 1: sustained degradation to ~30ms (> 1.8 * ~8). Give the median window
    // time to cross the threshold, then the min-samples streak triggers a halve.
    rtt_ms.store(30);
    std::this_thread::sleep_for(std::chrono::milliseconds(2500));
    TX_REQUIRE(p.selfThrottled() == true);
    TX_REQUIRE(throttle_logs.load() > 0);    // POLLER_SELF_THROTTLE emitted

    // Phase 2: RTT recovers — median drops below lo*ref and the poller un-throttles.
    rtt_ms.store(8);
    std::this_thread::sleep_for(std::chrono::milliseconds(2500));
    p.stop();

    TX_REQUIRE(p.selfThrottled() == false);  // recovered
    TX_REQUIRE(sink.fills.load() > 0);        // data still flowed throughout
}

}  // namespace

int main() {
    TX_RUN(test_backToBack_interleave_fifo);
    TX_RUN(test_governor_backoff_and_recovery);
    TX_RUN(test_respawn_then_fallback);
    TX_RUN(test_saturation_isolation);
    TX_RUN(test_self_throttle_engages_and_recovers);
    return tx::finish("test_fast_poller");
}
