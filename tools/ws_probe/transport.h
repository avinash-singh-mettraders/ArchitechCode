// =============================================================================
// ws_probe/transport.h — transport abstractions + AX order-entry protocol
// helpers. The probe orchestrator (probe.cpp) is written against these
// interfaces so the SAME state machine runs against the live venue and the
// in-process mock (dry-run / ctest). No engine headers are included here.
// =============================================================================
#pragma once

#include <chrono>
#include <functional>
#include <optional>
#include <string>
#include <vector>

#include <nlohmann/json.hpp>

namespace wsprobe {

using json = nlohmann::json;

// -----------------------------------------------------------------------------
// Monotonic clock. Every latency in this probe is measured against this and
// nothing else: recorded at the instant of socket write / frame receive,
// BEFORE any JSON parsing, so parse cost never pollutes a measurement.
// -----------------------------------------------------------------------------
inline long long nowNs() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}
inline double nsToMs(long long ns) { return static_cast<double>(ns) / 1.0e6; }

// A received WS frame with the receive timestamp captured before parsing.
struct RxFrame {
    long long rx_ns = 0;
    std::string payload;
};

// -----------------------------------------------------------------------------
// WsTransport — minimal order-entry WebSocket abstraction.
//   send() MUST capture and return the steady_clock ns AT the socket write.
//   Received frames are delivered via on_frame with rx_ns captured on arrival.
// Both the live (websocketpp+TLS) and mock implementations satisfy this.
// -----------------------------------------------------------------------------
class WsTransport {
public:
    virtual ~WsTransport() = default;

    // Establish + authenticate (login). Returns false and sets `err` on failure.
    // On success, `login_snapshot` is filled with the login response frame
    // (AX login response == open-orders snapshot per the protocol hypothesis).
    virtual bool connect(std::string& err, std::string& login_snapshot) = 0;

    // Serialize `msg`, write it, and return the tx timestamp (ns). Returns 0
    // on write failure. The raw payload is also handed to the frame logger.
    virtual long long send(const json& msg) = 0;

    virtual void close() = 0;
    virtual bool connected() const = 0;

    // Frame arrival callback. Set before connect(). Called from the transport's
    // poll thread; the orchestrator serializes access to its own state.
    std::function<void(const RxFrame&)> on_frame;
};

// -----------------------------------------------------------------------------
// RestApi — the REST surface the probe needs: the one-shot top-of-book fetch
// (for the far-price guard), the REST baseline place/cancel (mode B), and the
// exit-time open-orders sweep + cancel-all cleanup. Live impl uses libcurl;
// mock impl is in-process.
// -----------------------------------------------------------------------------
struct TopOfBook {
    bool ok = false;
    double best_bid = 0.0;
    double best_ask = 0.0;
};

struct RestResult {
    bool ok = false;
    long long latency_ns = 0;   // measured request->response wall time (steady)
    std::string order_id;       // populated by place
    std::string body;           // raw response body (logged)
    std::string err;
};

class RestApi {
public:
    virtual ~RestApi() = default;
    virtual TopOfBook fetchTopOfBook(const std::string& symbol) = 0;
    // Place a limit order; returns latency + assigned order id.
    virtual RestResult place(const std::string& symbol, const std::string& side,
                             double price, double qty) = 0;
    virtual RestResult cancel(const std::string& order_id) = 0;
    // Open-order ids currently live for this account/symbol (exit verification).
    virtual std::vector<std::string> openOrderIds(const std::string& symbol) = 0;
};

// -----------------------------------------------------------------------------
// Protocol-tolerant parsing. The wire spec is a HYPOTHESIS; the venue may name
// fields differently. These helpers look across the common candidates and the
// caller logs the raw frame verbatim regardless, so protocol reality is always
// recoverable from ws_probe_frames.jsonl even when matching fails.
// -----------------------------------------------------------------------------

// Event/message type token. Tries "t" then "type"; lower-cases the first char
// class we care about. Returns "" if none.
inline std::string frameType(const json& j) {
    if (!j.is_object()) return "";
    for (const char* k : {"t", "type", "event", "e"}) {
        if (j.contains(k) && j[k].is_string()) return j[k].get<std::string>();
    }
    return "";
}

// String value of the first present key among candidates (numbers coerced).
inline std::optional<std::string> frameStr(const json& j, std::initializer_list<const char*> keys) {
    if (!j.is_object()) return std::nullopt;
    for (const char* k : keys) {
        if (!j.contains(k)) continue;
        const auto& v = j[k];
        if (v.is_string()) return v.get<std::string>();
        if (v.is_number_integer()) return std::to_string(v.get<long long>());
        if (v.is_number_unsigned()) return std::to_string(v.get<unsigned long long>());
        if (v.is_number()) return std::to_string(v.get<double>());
    }
    return std::nullopt;
}

inline std::optional<std::string> frameOid(const json& j) {
    return frameStr(j, {"oid", "order_id", "orderId", "id", "o"});
}
inline std::optional<std::string> frameCid(const json& j) {
    return frameStr(j, {"cid", "client_id", "clientId", "cl_ord_id", "c"});
}
inline std::optional<long long> frameSeq(const json& j) {
    auto s = frameStr(j, {"seq", "sequence", "sn", "s"});
    if (!s) return std::nullopt;
    try { return std::stoll(*s); } catch (...) { return std::nullopt; }
}

// Classify a frame type into the terminal/ack semantics the probe reasons about.
enum class EvClass { New, Fill, Cancelled, Replaced, Rejected, Error, Other };

inline EvClass classify(const std::string& t) {
    if (t == "n" || t == "new" || t == "ack" || t == "accepted") return EvClass::New;
    if (t == "f" || t == "fill" || t == "filled" || t == "trade") return EvClass::Fill;
    if (t == "c" || t == "cancel" || t == "cancelled" || t == "canceled" || t == "x")
        return EvClass::Cancelled;
    if (t == "r" || t == "replace" || t == "replaced") return EvClass::Replaced;
    if (t == "j" || t == "reject" || t == "rejected") return EvClass::Rejected;
    if (t == "e" || t == "err" || t == "error") return EvClass::Error;
    return EvClass::Other;
}

inline bool isTerminal(EvClass c) {
    return c == EvClass::Cancelled || c == EvClass::Fill || c == EvClass::Rejected;
}

}  // namespace wsprobe
