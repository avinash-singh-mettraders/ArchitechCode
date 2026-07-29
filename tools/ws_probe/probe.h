// =============================================================================
// ws_probe/probe.h — the transport-agnostic probe orchestrator.
// Runs the SAME cancel/place/replace state machine, safety guards, OID
// tracking, CSV/JSONL output and verdict report against either the live venue
// or the in-process mock. Knows nothing about websocketpp/curl directly.
// =============================================================================
#pragma once

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <fstream>
#include <mutex>
#include <optional>
#include <random>
#include <set>
#include <string>
#include <vector>

#include "stats.h"
#include "transport.h"

namespace wsprobe {

// Set by the signal handler; polled between every blocking step so a Ctrl-C is
// observed within one bounded wait and triggers orderly cancel-all cleanup.
extern std::atomic<bool> g_stop;

struct ProbeConfig {
    std::string ws_url;
    std::string rest_url;
    std::string token;
    std::string symbol;
    double far_offset = 0.20;   // fraction away from touch
    double qty = 1.0;           // minimum lot
    int iterations = 50;        // hard-capped to 100
    bool run_a = true;
    bool run_b = true;
    bool run_c = true;
    bool cancel_on_disconnect = true;
    double runtime_cap_sec = 600.0;  // 10 min global cap
    std::string csv_path = "ws_probe_results.csv";
    std::string frames_path = "ws_probe_frames.jsonl";
};

class Probe {
public:
    Probe(ProbeConfig cfg, WsTransport* ws, RestApi* rest);
    ~Probe();

    // Wire on_frame, connect+login, log the snapshot. Returns false on failure.
    bool connect(std::string& err);

    // Fetch top-of-book, compute far prices, enforce the 5%-of-touch refusal.
    // Populates bid_px_/ask_px_/best_*; returns false (with err) if unsafe.
    bool preflightSafety(std::string& err);

    void runModeA();  // WS cancel-ack (C1) + place (C2) + ack->next place
    void runModeB();  // REST baseline
    void runModeC();  // ordering (C3) + atomic replace (C4)

    // Cancel-all this session's orders + verify none remain (idempotent).
    void cleanup();

    void report() const;  // summary table + verdicts + implication paragraph

    double bestBid() const { return best_bid_; }
    double bestAsk() const { return best_ask_; }
    double bidPx() const { return bid_px_; }
    double askPx() const { return ask_px_; }
    const ProbeConfig& cfg() const { return cfg_; }

    // Accessors used by the --dry-run self-tests to assert measurement plumbing.
    std::size_t nPlaceWs() const { return m_place_ws_.n(); }
    std::size_t nCancelWs() const { return m_cancel_ws_.n(); }
    std::size_t nAckNextWs() const { return m_ack_next_ws_.n(); }
    std::size_t nReplaceWs() const { return m_replace_ws_.n(); }
    std::size_t nPlaceRest() const { return m_place_rest_.n(); }
    std::size_t nCancelRest() const { return m_cancel_rest_.n(); }
    int c3Sequences() const { return c3_sequences_; }
    int c3Clean() const { return c3_clean_; }
    int c4Trials() const { return c4_trials_; }
    int c4SingleLive() const { return c4_single_live_; }

private:
    struct Event {
        long long rx_ns = 0;
        EvClass cls = EvClass::Other;
        std::string type_raw;
        std::optional<std::string> oid;
        std::optional<std::string> cid;
        std::optional<long long> seq;
        json j;
    };

    void onFrame(const RxFrame& f);
    void logFrame(const char* dir, long long ts_ns, const std::string& payload);

    long long sendTracked(const json& msg);  // send + log tx frame; returns tx ns

    // Block for the first not-yet-claimed event satisfying pred (timeout ms).
    std::optional<Event> waitFor(const std::function<bool(const Event&)>& pred, int timeout_ms);
    // Collect every matching event that arrives within `window_ms` (claims them).
    std::vector<Event> drainFor(const std::function<bool(const Event&)>& pred, int window_ms);

    std::string nextCid();
    void trackOid(const std::string& oid);
    void csvRow(int iter, const char* mode, const char* metric, const char* transport,
                const std::string& oid, const std::string& cid, double latency_ms);
    bool stopped() const;   // g_stop OR runtime cap exceeded
    void jitterSleep();

    ProbeConfig cfg_;
    WsTransport* ws_;
    RestApi* rest_;

    double best_bid_ = 0.0, best_ask_ = 0.0;
    double bid_px_ = 0.0, ask_px_ = 0.0;

    std::ofstream frames_;
    std::ofstream csv_;

    std::mutex mu_;
    std::condition_variable cv_;
    std::vector<Event> events_;
    std::vector<bool> claimed_;

    std::mutex oid_mu_;
    std::set<std::string> tracked_oids_;

    std::mt19937 rng_;
    std::uint64_t cid_ctr_ = 0;
    std::string run_id_;
    long long start_ns_ = 0;

    // Metrics.
    Metric m_place_ws_{"place ack"};
    Metric m_cancel_ws_{"cancel ack"};
    Metric m_ack_next_ws_{"ack->next place"};
    Metric m_replace_ws_{"atomic replace ack"};
    Metric m_place_rest_{"place ack"};
    Metric m_cancel_rest_{"cancel ack"};

    // Mode C bookkeeping for verdicts.
    int c3_sequences_ = 0;
    int c3_clean_ = 0;         // exactly-one-terminal, no post-terminal, monotonic
    int c4_trials_ = 0;
    int c4_single_live_ = 0;   // exactly one live order at P2
    int c4_cid_inherited_ = 0;
    bool fill_observed_ = false;

    std::atomic<bool> cleaned_{false};
};

}  // namespace wsprobe
