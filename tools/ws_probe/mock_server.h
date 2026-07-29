// =============================================================================
// ws_probe/mock_server.h — in-process mock venue for --dry-run / ctest.
//
// Implements the AX order-entry protocol HYPOTHESIS (t=p/x/r/X -> n/c/r) with
// small simulated latencies, on a background delivery thread, so the probe's
// real await/measure/cleanup state machine runs end-to-end with NO network.
// A shared MockBook models account open-orders so the exit-time REST sweep can
// actually observe (and fail on) a leaked order.
// =============================================================================
#pragma once

#include <atomic>
#include <chrono>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "transport.h"

namespace wsprobe {

// Shared open-order book so WS-placed and REST-placed orders land in the same
// account view that openOrderIds() reports at exit.
class MockBook {
public:
    void add(const std::string& oid) {
        std::lock_guard<std::mutex> lk(mu_);
        live_.insert(oid);
    }
    void remove(const std::string& oid) {
        std::lock_guard<std::mutex> lk(mu_);
        live_.erase(oid);
    }
    void replace(const std::string& old_oid, const std::string& new_oid) {
        std::lock_guard<std::mutex> lk(mu_);
        live_.erase(old_oid);
        live_.insert(new_oid);
    }
    std::vector<std::string> ids() const {
        std::lock_guard<std::mutex> lk(mu_);
        return {live_.begin(), live_.end()};
    }
    std::size_t count() const {
        std::lock_guard<std::mutex> lk(mu_);
        return live_.size();
    }

private:
    mutable std::mutex mu_;
    std::set<std::string> live_;
};

// -----------------------------------------------------------------------------
// MockWsTransport
// -----------------------------------------------------------------------------
class MockWsTransport : public WsTransport {
public:
    explicit MockWsTransport(MockBook* book, double latency_ms = 3.0)
        : book_(book), latency_ms_(latency_ms) {}

    ~MockWsTransport() override { close(); }

    bool connect(std::string& /*err*/, std::string& login_snapshot) override {
        connected_.store(true);
        login_snapshot = R"({"t":"snapshot","orders":[]})";
        worker_ = std::thread([this] { deliverLoop(); });
        return true;
    }

    long long send(const json& msg) override {
        const long long tx = nowNs();
        if (!connected_.load()) return 0;
        const std::string t = frameType(msg);
        std::lock_guard<std::mutex> lk(mu_);
        if (t == "p") {
            const std::string oid = "M" + std::to_string(++oid_ctr_);
            std::string cid = frameCid(msg).value_or("");
            std::string side = msg.value("side", "buy");
            double px = msg.value("px", 0.0);
            live_[oid] = {cid, side, px};
            if (book_) book_->add(oid);
            schedule(json{{"t", "n"}, {"oid", oid}, {"cid", cid}, {"px", px}, {"side", side}});
        } else if (t == "x") {
            std::string oid = frameOid(msg).value_or("");
            auto it = live_.find(oid);
            std::string cid = it != live_.end() ? it->second.cid : frameCid(msg).value_or("");
            if (it != live_.end()) live_.erase(it);
            if (book_) book_->remove(oid);
            schedule(json{{"t", "c"}, {"oid", oid}, {"cid", cid}});
        } else if (t == "r") {
            std::string oid = frameOid(msg).value_or("");
            double px2 = msg.value("px", 0.0);
            std::string cid, side;
            auto it = live_.find(oid);
            if (it != live_.end()) { cid = it->second.cid; side = it->second.side; live_.erase(it); }
            const std::string new_oid = "M" + std::to_string(++oid_ctr_);
            live_[new_oid] = {cid, side, px2};   // exactly one live order for cid, at P2
            if (book_) book_->replace(oid, new_oid);
            schedule(json{{"t", "r"}, {"oid", new_oid}, {"prev_oid", oid}, {"cid", cid}, {"px", px2}});
        } else if (t == "X") {
            for (auto& [oid, o] : live_) {
                if (book_) book_->remove(oid);
                schedule(json{{"t", "c"}, {"oid", oid}, {"cid", o.cid}});
            }
            live_.clear();
        }
        return tx;
    }

    void close() override {
        if (!connected_.exchange(false)) return;
        if (worker_.joinable()) worker_.join();
    }
    bool connected() const override { return connected_.load(); }

private:
    struct Order { std::string cid; std::string side; double px; };
    struct Pending { long long due_ns; std::string payload; };

    void schedule(const json& j) {
        pending_.push_back({nowNs() + static_cast<long long>(latency_ms_ * 1.0e6), j.dump()});
    }

    void deliverLoop() {
        while (connected_.load()) {
            std::vector<std::string> ready;
            {
                std::lock_guard<std::mutex> lk(mu_);
                const long long t = nowNs();
                std::vector<Pending> keep;
                for (auto& p : pending_) {
                    if (p.due_ns <= t) ready.push_back(std::move(p.payload));
                    else keep.push_back(std::move(p));
                }
                pending_.swap(keep);
            }
            for (auto& payload : ready) {
                if (on_frame) on_frame(RxFrame{nowNs(), std::move(payload)});
            }
            std::this_thread::sleep_for(std::chrono::microseconds(200));
        }
    }

    MockBook* book_;
    double latency_ms_;
    std::atomic<bool> connected_{false};
    std::thread worker_;
    std::mutex mu_;
    long long oid_ctr_ = 0;
    std::unordered_map<std::string, Order> live_;
    std::vector<Pending> pending_;
};

// -----------------------------------------------------------------------------
// MockRestApi
// -----------------------------------------------------------------------------
class MockRestApi : public RestApi {
public:
    MockRestApi(MockBook* book, double bid, double ask, double place_ms = 60.0,
                double cancel_ms = 200.0)
        : book_(book), bid_(bid), ask_(ask), place_ms_(place_ms), cancel_ms_(cancel_ms) {}

    TopOfBook fetchTopOfBook(const std::string& /*symbol*/) override {
        return TopOfBook{true, bid_, ask_};
    }

    RestResult place(const std::string& /*symbol*/, const std::string& /*side*/,
                     double /*price*/, double /*qty*/) override {
        std::this_thread::sleep_for(std::chrono::microseconds(static_cast<long>(place_ms_ * 1000)));
        RestResult r;
        r.ok = true;
        r.latency_ns = static_cast<long long>(place_ms_ * 1.0e6);
        r.order_id = "R" + std::to_string(++ctr_);
        if (book_) book_->add(r.order_id);
        return r;
    }

    RestResult cancel(const std::string& order_id) override {
        std::this_thread::sleep_for(std::chrono::microseconds(static_cast<long>(cancel_ms_ * 1000)));
        RestResult r;
        r.ok = true;
        r.latency_ns = static_cast<long long>(cancel_ms_ * 1.0e6);
        if (book_) book_->remove(order_id);
        return r;
    }

    std::vector<std::string> openOrderIds(const std::string& /*symbol*/) override {
        return book_ ? book_->ids() : std::vector<std::string>{};
    }

private:
    MockBook* book_;
    double bid_, ask_;
    double place_ms_, cancel_ms_;
    long long ctr_ = 0;
};

}  // namespace wsprobe
