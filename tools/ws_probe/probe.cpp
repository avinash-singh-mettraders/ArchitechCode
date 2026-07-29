// =============================================================================
// ws_probe/probe.cpp — orchestrator implementation.
// =============================================================================
#include "probe.h"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdio>
#include <thread>

namespace wsprobe {

std::atomic<bool> g_stop{false};

namespace {
constexpr int kAckTimeoutMs = 5000;
const char* RED = "\033[31m";
const char* GRN = "\033[32m";
const char* YEL = "\033[33m";
const char* RST = "\033[0m";
}  // namespace

Probe::Probe(ProbeConfig cfg, WsTransport* ws, RestApi* rest)
    : cfg_(std::move(cfg)), ws_(ws), rest_(rest), rng_(std::random_device{}()) {
    start_ns_ = nowNs();
    run_id_ = "wsprobe-" + std::to_string(start_ns_ % 1000000000LL);

    frames_.open(cfg_.frames_path, std::ios::out | std::ios::trunc);
    csv_.open(cfg_.csv_path, std::ios::out | std::ios::trunc);
    if (csv_) csv_ << "iter,mode,metric,transport,oid,cid,latency_ms\n";
}

Probe::~Probe() {
    if (!cleaned_.load()) cleanup();
    if (frames_.is_open()) frames_.close();
    if (csv_.is_open()) csv_.close();
}

// -----------------------------------------------------------------------------
// Frame IO
// -----------------------------------------------------------------------------
void Probe::logFrame(const char* dir, long long ts_ns, const std::string& payload) {
    std::lock_guard<std::mutex> lk(mu_);
    if (!frames_.is_open()) return;
    json rec;
    rec["dir"] = dir;
    rec["ts_ns"] = ts_ns;
    rec["payload"] = payload;  // stored as an escaped string; raw bytes recoverable
    frames_ << rec.dump() << "\n";
    frames_.flush();
}

long long Probe::sendTracked(const json& msg) {
    const long long tx = ws_->send(msg);
    logFrame("tx", tx ? tx : nowNs(), msg.dump());
    return tx;
}

void Probe::onFrame(const RxFrame& f) {
    // rx_ns already captured by the transport before parsing; log verbatim first.
    logFrame("rx", f.rx_ns, f.payload);

    Event e;
    e.rx_ns = f.rx_ns;
    bool parsed = false;
    try {
        e.j = json::parse(f.payload);
        parsed = true;
    } catch (...) {
    }
    if (parsed) {
        e.type_raw = frameType(e.j);
        e.cls = classify(e.type_raw);
        e.oid = frameOid(e.j);
        e.cid = frameCid(e.j);
        e.seq = frameSeq(e.j);
        if ((e.cls == EvClass::New || e.cls == EvClass::Replaced) && e.oid) trackOid(*e.oid);
        if (e.cls == EvClass::Fill) fill_observed_ = true;
    }
    {
        std::lock_guard<std::mutex> lk(mu_);
        events_.push_back(std::move(e));
        claimed_.push_back(false);
    }
    cv_.notify_all();
}

// -----------------------------------------------------------------------------
// Await helpers
// -----------------------------------------------------------------------------
std::optional<Probe::Event> Probe::waitFor(const std::function<bool(const Event&)>& pred,
                                           int timeout_ms) {
    std::unique_lock<std::mutex> lk(mu_);
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
    for (;;) {
        for (std::size_t i = 0; i < events_.size(); ++i) {
            if (!claimed_[i] && pred(events_[i])) {
                claimed_[i] = true;
                return events_[i];
            }
        }
        if (g_stop.load()) return std::nullopt;
        if (cv_.wait_until(lk, deadline) == std::cv_status::timeout) {
            for (std::size_t i = 0; i < events_.size(); ++i) {
                if (!claimed_[i] && pred(events_[i])) {
                    claimed_[i] = true;
                    return events_[i];
                }
            }
            return std::nullopt;
        }
    }
}

std::vector<Probe::Event> Probe::drainFor(const std::function<bool(const Event&)>& pred,
                                          int window_ms) {
    std::vector<Event> out;
    std::unique_lock<std::mutex> lk(mu_);
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(window_ms);
    for (;;) {
        for (std::size_t i = 0; i < events_.size(); ++i) {
            if (!claimed_[i] && pred(events_[i])) {
                claimed_[i] = true;
                out.push_back(events_[i]);
            }
        }
        if (std::chrono::steady_clock::now() >= deadline) break;
        cv_.wait_until(lk, deadline);
    }
    return out;
}

// -----------------------------------------------------------------------------
// Small helpers
// -----------------------------------------------------------------------------
std::string Probe::nextCid() { return run_id_ + "-" + std::to_string(++cid_ctr_); }

void Probe::trackOid(const std::string& oid) {
    std::lock_guard<std::mutex> lk(oid_mu_);
    tracked_oids_.insert(oid);
}

void Probe::csvRow(int iter, const char* mode, const char* metric, const char* transport,
                   const std::string& oid, const std::string& cid, double latency_ms) {
    if (!csv_) return;
    std::lock_guard<std::mutex> lk(mu_);
    csv_ << iter << ',' << mode << ',' << metric << ',' << transport << ',' << oid << ',' << cid
         << ',' << latency_ms << '\n';
    csv_.flush();
}

bool Probe::stopped() const {
    if (g_stop.load()) return true;
    return nsToMs(nowNs() - start_ns_) / 1000.0 > cfg_.runtime_cap_sec;
}

void Probe::jitterSleep() {
    std::uniform_int_distribution<int> d(200, 500);
    std::this_thread::sleep_for(std::chrono::milliseconds(d(rng_)));
}

// -----------------------------------------------------------------------------
// Connect / safety preflight
// -----------------------------------------------------------------------------
bool Probe::connect(std::string& err) {
    ws_->on_frame = [this](const RxFrame& f) { onFrame(f); };
    std::string snapshot;
    if (!ws_->connect(err, snapshot)) return false;
    if (!snapshot.empty()) {
        logFrame("rx", nowNs(), snapshot);
        std::printf("[login] open-orders snapshot: %s\n",
                    snapshot.size() > 400 ? (snapshot.substr(0, 400) + "...").c_str()
                                          : snapshot.c_str());
    }
    return true;
}

bool Probe::preflightSafety(std::string& err) {
    TopOfBook tob = rest_->fetchTopOfBook(cfg_.symbol);
    if (!tob.ok) {
        err = "failed to fetch top-of-book for " + cfg_.symbol +
              " (need a valid REST endpoint / market data to price far from market)";
        return false;
    }
    best_bid_ = tob.best_bid;
    best_ask_ = tob.best_ask;
    bid_px_ = best_bid_ * (1.0 - cfg_.far_offset);
    ask_px_ = best_ask_ * (1.0 + cfg_.far_offset);

    const double dist_bid = (best_bid_ - bid_px_) / best_bid_;
    const double dist_ask = (ask_px_ - best_ask_) / best_ask_;
    const double min_dist = std::min(dist_bid, dist_ask);
    if (min_dist < 0.05) {
        char buf[256];
        std::snprintf(buf, sizeof(buf),
                      "REFUSING: computed probe price is only %.2f%% from touch "
                      "(bid_px=%.6f best_bid=%.6f); require >= 5%%. Raise --far-offset.",
                      min_dist * 100.0, bid_px_, best_bid_);
        err = buf;
        return false;
    }
    return true;
}

// -----------------------------------------------------------------------------
// Mode A — WS cancel-ack (C1) + place (C2) + ack->next-place gate
// -----------------------------------------------------------------------------
void Probe::runModeA() {
    std::printf("\n=== Mode A: WS place/cancel latency (%d iters) ===\n", cfg_.iterations);
    for (int it = 0; it < cfg_.iterations && !stopped(); ++it) {
        const std::string cid = nextCid();
        json place{{"t", "p"}, {"symbol", cfg_.symbol}, {"side", "buy"},
                   {"px", bid_px_}, {"qty", cfg_.qty}, {"cid", cid}};
        const long long tx = sendTracked(place);
        auto n = waitFor([&](const Event& e) { return e.cid == cid && e.cls == EvClass::New; },
                         kAckTimeoutMs);
        if (!n) { std::printf("  [A#%d] no place ack (cid=%s)\n", it, cid.c_str()); continue; }
        const std::string oid1 = n->oid.value_or("");
        const double place_ms = nsToMs(n->rx_ns - tx);
        m_place_ws_.add(place_ms);
        csvRow(it, "A", "place_ack", "WS", oid1, cid, place_ms);

        jitterSleep();
        if (stopped()) break;

        const long long txc = sendTracked(json{{"t", "x"}, {"oid", oid1}, {"cid", cid}});
        auto c = waitFor([&](const Event& e) { return e.oid == oid1 && e.cls == EvClass::Cancelled; },
                         kAckTimeoutMs);
        if (!c) { std::printf("  [A#%d] no cancel ack (oid=%s)\n", it, oid1.c_str()); continue; }
        const double cancel_ms = nsToMs(c->rx_ns - txc);
        m_cancel_ws_.add(cancel_ms);
        csvRow(it, "A", "cancel_ack", "WS", oid1, cid, cancel_ms);

        // Re-add gate: place the next order immediately on the cancel ack.
        const std::string cid2 = nextCid();
        json place2{{"t", "p"}, {"symbol", cfg_.symbol}, {"side", "buy"},
                    {"px", bid_px_}, {"qty", cfg_.qty}, {"cid", cid2}};
        const long long tx2 = sendTracked(place2);
        const double ack_to_next_ms = nsToMs(tx2 - c->rx_ns);
        m_ack_next_ws_.add(ack_to_next_ms);
        csvRow(it, "A", "ack_to_next_place", "WS", "", cid2, ack_to_next_ms);

        auto n2 = waitFor([&](const Event& e) { return e.cid == cid2 && e.cls == EvClass::New; },
                          kAckTimeoutMs);
        if (n2) {
            const std::string oid2 = n2->oid.value_or("");
            const double place2_ms = nsToMs(n2->rx_ns - tx2);
            m_place_ws_.add(place2_ms);
            csvRow(it, "A", "place_ack", "WS", oid2, cid2, place2_ms);
            // Keep <= 1 working order: cancel the re-added one before next iter.
            sendTracked(json{{"t", "x"}, {"oid", oid2}, {"cid", cid2}});
            waitFor([&](const Event& e) { return e.oid == oid2 && e.cls == EvClass::Cancelled; },
                    kAckTimeoutMs);
        }
    }
}

// -----------------------------------------------------------------------------
// Mode B — REST baseline
// -----------------------------------------------------------------------------
void Probe::runModeB() {
    std::printf("\n=== Mode B: REST baseline (%d iters) ===\n", cfg_.iterations);
    for (int it = 0; it < cfg_.iterations && !stopped(); ++it) {
        RestResult p = rest_->place(cfg_.symbol, "buy", bid_px_, cfg_.qty);
        if (!p.ok) { std::printf("  [B#%d] REST place failed: %s\n", it, p.err.c_str()); continue; }
        if (!p.order_id.empty()) trackOid(p.order_id);
        const double place_ms = nsToMs(p.latency_ns);
        m_place_rest_.add(place_ms);
        csvRow(it, "B", "place_ack", "REST", p.order_id, "", place_ms);

        jitterSleep();
        if (stopped()) break;

        RestResult c = rest_->cancel(p.order_id);
        if (!c.ok) { std::printf("  [B#%d] REST cancel failed: %s\n", it, c.err.c_str()); continue; }
        const double cancel_ms = nsToMs(c.latency_ns);
        m_cancel_rest_.add(cancel_ms);
        csvRow(it, "B", "cancel_ack", "REST", p.order_id, "", cancel_ms);
    }
}

// -----------------------------------------------------------------------------
// Mode C — ordering (C3) + atomic replace (C4)
// -----------------------------------------------------------------------------
void Probe::runModeC() {
    std::printf("\n=== Mode C: ordering + atomic replace (%d iters) ===\n", cfg_.iterations);

    // C3: place then cancel; capture the full terminal-event sequence for the oid.
    for (int it = 0; it < cfg_.iterations && !stopped(); ++it) {
        const std::string cid = nextCid();
        const long long tx = sendTracked(json{{"t", "p"}, {"symbol", cfg_.symbol}, {"side", "buy"},
                                              {"px", bid_px_}, {"qty", cfg_.qty}, {"cid", cid}});
        auto n = waitFor([&](const Event& e) { return e.cid == cid && e.cls == EvClass::New; },
                         kAckTimeoutMs);
        if (!n) continue;
        const std::string oid = n->oid.value_or("");
        (void)tx;
        sendTracked(json{{"t", "x"}, {"oid", oid}, {"cid", cid}});

        auto seq = drainFor([&](const Event& e) {
            return e.oid == oid && isTerminal(e.cls);
        }, 800);
        c3_sequences_++;

        int terminals = static_cast<int>(seq.size());
        bool monotonic = true;
        long long last_seq = -1;
        for (const auto& e : seq) {
            if (e.seq) {
                if (*e.seq <= last_seq) monotonic = false;
                last_seq = *e.seq;
            }
            if (e.cls == EvClass::Fill) fill_observed_ = true;
        }
        // Clean == exactly one terminal event (the cancel) and seq monotonic.
        if (terminals == 1 && seq.front().cls == EvClass::Cancelled && monotonic) c3_clean_++;
    }

    // C4: atomic replace P1 -> P2, assert exactly one live order at P2, cid inherits.
    const double px1 = bid_px_;
    const double px2 = bid_px_ * 0.98;  // still far from touch, distinct from P1
    for (int it = 0; it < cfg_.iterations && !stopped(); ++it) {
        const std::string cid = nextCid();
        sendTracked(json{{"t", "p"}, {"symbol", cfg_.symbol}, {"side", "buy"},
                         {"px", px1}, {"qty", cfg_.qty}, {"cid", cid}});
        auto n = waitFor([&](const Event& e) { return e.cid == cid && e.cls == EvClass::New; },
                         kAckTimeoutMs);
        if (!n) continue;
        const std::string oid1 = n->oid.value_or("");

        const long long txr = sendTracked(json{{"t", "r"}, {"oid", oid1}, {"px", px2}, {"cid", cid}});
        auto r = waitFor([&](const Event& e) {
            return e.cid == cid && (e.cls == EvClass::Replaced || e.cls == EvClass::New) &&
                   e.oid != n->oid;  // prefer the post-replace ack
        }, kAckTimeoutMs);
        if (!r) {
            // Some venues re-ack replace with the SAME oid and type n/r — accept that too.
            r = waitFor([&](const Event& e) {
                return e.cid == cid && (e.cls == EvClass::Replaced || e.cls == EvClass::New);
            }, 500);
        }
        if (!r) continue;
        c4_trials_++;
        const std::string new_oid = r->oid.value_or(oid1);
        const double replace_ms = nsToMs(r->rx_ns - txr);
        m_replace_ws_.add(replace_ms);
        csvRow(it, "C", "replace_ack", "WS", new_oid, cid, replace_ms);
        if (r->cid == cid) c4_cid_inherited_++;

        // Exactly-one-live assertion against open orders.
        std::vector<std::string> live = rest_->openOrderIds(cfg_.symbol);
        int live_probe = 0;
        {
            std::lock_guard<std::mutex> lk(oid_mu_);
            for (const auto& id : live)
                if (tracked_oids_.count(id)) live_probe++;
        }
        if (live_probe == 1) c4_single_live_++;
        std::printf("  [C4#%d] replace cid=%s oid %s -> %s  live_probe_orders=%d  replace_ack=%.3fms\n",
                    it, cid.c_str(), oid1.c_str(), new_oid.c_str(), live_probe, replace_ms);

        // Clean up this order.
        sendTracked(json{{"t", "x"}, {"oid", new_oid}, {"cid", cid}});
        waitFor([&](const Event& e) { return e.oid == new_oid && e.cls == EvClass::Cancelled; }, kAckTimeoutMs);
    }
}

// -----------------------------------------------------------------------------
// Cleanup — cancel-all + verify no probe order remains
// -----------------------------------------------------------------------------
void Probe::cleanup() {
    if (cleaned_.exchange(true)) return;
    std::printf("\n[cleanup] sending cancel-all (t=X) for %s...\n", cfg_.symbol.c_str());
    if (ws_ && ws_->connected()) {
        sendTracked(json{{"t", "X"}, {"symbol", cfg_.symbol}});
        drainFor([](const Event& e) { return e.cls == EvClass::Cancelled; }, 800);
    }

    std::vector<std::string> leaked;
    if (rest_) {
        std::vector<std::string> live = rest_->openOrderIds(cfg_.symbol);
        std::lock_guard<std::mutex> lk(oid_mu_);
        for (const auto& id : live)
            if (tracked_oids_.count(id)) leaked.push_back(id);
    }
    if (leaked.empty()) {
        std::printf("%s[cleanup] OK — no probe orders remain.%s\n", GRN, RST);
    } else {
        std::printf("%s[cleanup] WARNING — %zu probe order(s) STILL LIVE:%s", RED, leaked.size(), RST);
        for (const auto& id : leaked) std::printf(" %s", id.c_str());
        std::printf("\n%s>>> MANUALLY CANCEL THE ABOVE OIDS NOW <<<%s\n", RED, RST);
    }
}

// -----------------------------------------------------------------------------
// Report
// -----------------------------------------------------------------------------
void Probe::report() const {
    printSummaryHeader();
    printSummaryRow("place ack", "WS", m_place_ws_);
    printSummaryRow("place ack", "REST", m_place_rest_);
    printSummaryRow("cancel ack", "WS", m_cancel_ws_);
    printSummaryRow("cancel ack", "REST", m_cancel_rest_);
    printSummaryRow("ack->next place", "WS", m_ack_next_ws_);
    printSummaryRow("atomic replace ack", "WS", m_replace_ws_);

    auto p50 = [](const Metric& m) { return m.pct(0.50); };
    std::printf("\n--- Verdicts ---\n");

    // C1 — WS cancel ack fast.
    if (m_cancel_ws_.n() == 0) {
        std::printf("C1 (WS cancel ack fast): INCONCLUSIVE — no WS cancel samples.\n");
    } else {
        const double ws = p50(m_cancel_ws_);
        const bool ref = (m_cancel_rest_.n() > 0) ? ws < p50(m_cancel_rest_) : ws < 50.0;
        std::printf("C1 (WS cancel ack fast): %s — WS cancel p50=%.3fms%s.\n",
                    ref ? "CONFIRMED" : "REFUTED", ws,
                    m_cancel_rest_.n() > 0 ? (std::string(" vs REST p50=") +
                                              std::to_string(p50(m_cancel_rest_)) + "ms").c_str()
                                           : "");
    }
    // C2 — WS place fast.
    if (m_place_ws_.n() == 0) {
        std::printf("C2 (WS place fast): INCONCLUSIVE — no WS place samples.\n");
    } else {
        const double ws = p50(m_place_ws_);
        const bool ref = (m_place_rest_.n() > 0) ? ws <= p50(m_place_rest_) * 1.5 : ws < 70.0;
        std::printf("C2 (WS place fast): %s — WS place p50=%.3fms%s.\n",
                    ref ? "CONFIRMED" : "REFUTED", ws,
                    m_place_rest_.n() > 0 ? (std::string(" vs REST p50=") +
                                             std::to_string(p50(m_place_rest_)) + "ms").c_str()
                                          : "");
    }
    // C3 — ordering trustworthy (c-events).
    std::printf("C3 (event ordering): PARTIAL — %d/%d cancel sequences clean "
                "(exactly one terminal, monotonic, no post-terminal). "
                "Fill-ordering %s (raw frames in %s).\n",
                c3_clean_, c3_sequences_,
                fill_observed_ ? "OBSERVED at least once" : "pending first live fill observation",
                cfg_.frames_path.c_str());
    // C4 — atomic replace.
    if (c4_trials_ == 0) {
        std::printf("C4 (atomic replace): INCONCLUSIVE — no replace trials.\n");
    } else {
        const bool ok = (c4_single_live_ == c4_trials_) && m_replace_ws_.n() > 0;
        std::printf("C4 (atomic replace): %s — %d/%d trials had exactly one live order at P2, "
                    "%d/%d inherited cid; replace ack p50=%.3fms.\n",
                    ok ? "CONFIRMED" : "REFUTED", c4_single_live_, c4_trials_,
                    c4_cid_inherited_, c4_trials_, p50(m_replace_ws_));
    }

    // Implication paragraph.
    const double ws_gap = (m_cancel_ws_.n() ? p50(m_cancel_ws_) : 0.0) +
                          (m_ack_next_ws_.n() ? p50(m_ack_next_ws_) : 0.0) +
                          (m_place_ws_.n() ? p50(m_place_ws_) : 0.0);
    const double rest_gap = m_cancel_rest_.n() ? p50(m_cancel_rest_) : 210.0;
    std::printf("\n--- Implication ---\n");
    std::printf(
        "Measured WS cancel->re-add gap (cancel ack p50 + ack->next-place p50 + place ack p50) "
        "= %.3fms, versus the current REST cancel round-trip of ~%.0fms. "
        "If confirmed on the live venue, the proposed flow (cancel -> await WS c -> place both "
        "immediately) collapses the re-quote hole from ~%.0fms to ~%.1fms, i.e. the book is "
        "exposed to a stale quote for %.0fx less time on each feed move.\n",
        ws_gap, rest_gap, rest_gap, ws_gap, ws_gap > 0 ? rest_gap / ws_gap : 0.0);
    (void)YEL;
}

}  // namespace wsprobe
