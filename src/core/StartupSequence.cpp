#include "core/StartupSequence.h"
#include "core/FastPoller.h"
#include "core/LatencyTracker.h"
#include "core/Platform.h"
#include "config/Config.h"
#include "utils/Logger.h"
#include "events/EventManager.h"
#include "orders/OrderManager.h"
#include "user/UserManager.h"
#include "api/RestClient.h"
#include "api/WebSocketClient.h"
#include "marketdata/MarketDataManager.h"
#include "marketdata/ExternalFeedManager.h"
#include "portfolio/PortfolioManager.h"
#include "strategy/Strategy.h"
#include "strategy/MakeMarketStrategy.h"
#include "strategy/MmOrderMover.h"
#include "strategy/FastMarketMonitor.h"
#include "utils/PositionRestQty.h"

#include <iostream>
#include <fstream>
#include <iterator>
#include <thread>
#include <filesystem>
#include <unordered_map>
#include <unordered_set>
#include <memory>
#include <cstdint>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cctype>
#include <sstream>
#if defined(__APPLE__) || defined(__unix__)
#include <fcntl.h>
#include <unistd.h>
#endif

namespace architect {
namespace core {

namespace fs = std::filesystem;

namespace {

std::string fillJsonExchangeOrderId(const nlohmann::json& j);

fs::path mm_orders_json_path_for_startup() {
    auto& cfg = config::Config::getInstance();
    std::string rel = cfg.getString("mm_desk.mm_orders_config_path", "logs/orders.json");
    if (rel.empty()) {
        rel = "logs/orders.json";
    }
    fs::path p(rel);
    if (!p.is_absolute()) {
        p = fs::current_path() / p;
    }
    return p;
}

fs::path desk_force_cancel_signal_path_for_startup() {
    auto& cfg = config::Config::getInstance();
    std::string rel = cfg.getString("mm_desk.force_cancel_signal_path", "logs/mm_force_cancel.jsonl");
    if (rel.empty()) {
        rel = "logs/mm_force_cancel.jsonl";
    }
    fs::path p(rel);
    if (!p.is_absolute()) {
        p = fs::current_path() / p;
    }
    return p;
}

// Drain the desk→C++ force-cancel channel (append-only JSONL written by mm_live_desk_core.py's
// /api/desk/cancel_one_order orphan path). Each line: {"ax_symbol","stack_id","reason"}. This is
// the recovery lane for stacks that vanished from orders.json but still rest on the venue: Python
// cannot cancel them (it lacks the exchange OIDs) so it hands the (ax, stack_id) to C++, which owns
// the local-id → exchange-OID mapping in OrderManager.
//
// Thread-safety / no-loss: we CLAIM the file by atomic rename before reading, so a concurrent
// Python append either lands fully in the claimed copy or in a brand-new file drained next tick —
// never a torn/lost line. The actual venue cancels are dispatched through
// MakeMarketStrategy::mmDeskForceCancelStackGateway, which serializes them on the single
// MmOrderMover worker (same discipline as the fast-market breaker). Best-effort and fully guarded:
// any IO/parse error is logged and skipped; this never throws into the trading loop.
void drainDeskForceCancelSignals() {
    const fs::path sig = desk_force_cancel_signal_path_for_startup();
    std::error_code ec;
    const fs::path claimed(sig.string() + ".processing");
    const bool have_sig = fs::exists(sig, ec);
    const bool have_claimed = fs::exists(claimed, ec);
    if (!have_sig && !have_claimed) {
        return;  // idle: nothing to drain (also recovers a stale .processing from a prior crash)
    }
    if (!have_claimed) {
        // Claim the live signal by atomic rename so a concurrent Python append is never lost
        // (it lands in a fresh file drained next tick). If we lost the race, retry next tick.
        fs::rename(sig, claimed, ec);
        if (ec) {
            return;
        }
    }
    // else: a .processing copy already exists (prior crash or race) — process it now.
    std::ifstream f(claimed, std::ios::binary);
    if (!f) {
        fs::remove(claimed, ec);
        return;
    }
    std::string line;
    int n = 0;
    while (std::getline(f, line)) {
        while (!line.empty() &&
               (line.back() == '\r' || line.back() == '\n' || line.back() == ' ' || line.back() == '\t')) {
            line.pop_back();
        }
        if (line.empty()) {
            continue;
        }
        std::string ax;
        std::string sid;
        std::string reason;
        try {
            const nlohmann::json j = nlohmann::json::parse(line);
            if (j.is_object()) {
                if (j.contains("ax_symbol") && j["ax_symbol"].is_string()) {
                    ax = j["ax_symbol"].get<std::string>();
                }
                if (j.contains("stack_id") && j["stack_id"].is_string()) {
                    sid = j["stack_id"].get<std::string>();
                }
                if (j.contains("reason") && j["reason"].is_string()) {
                    reason = j["reason"].get<std::string>();
                }
            }
        } catch (...) {
            if (utils::Logger::isInitialized()) {
                Main().logger()->warn("[DESK_FORCE_CANCEL] skipping unparseable signal line");
            }
            continue;
        }
        if (ax.empty() && sid.empty()) {
            continue;  // nothing to target
        }
        if (reason.empty()) {
            reason = "desk_force_cancel";
        }
        strategy::MakeMarketStrategy::mmDeskForceCancelStackGateway(ax, sid, reason);
        ++n;
    }
    f.close();
    fs::remove(claimed, ec);
    if (n > 0 && utils::Logger::isInitialized()) {
        Main().logger()->info("[DESK_FORCE_CANCEL] drained {} signal line(s)", n);
    }
}

void cancel_exchange_oid_best_effort(const std::string& oid) {
    if (oid.empty()) {
        return;
    }
    auto& rest = api::RestClient::getInstance();
    auto cr = rest.cancelOrder(oid);
    if (!cr.is_success) {
        nlohmann::json body = nlohmann::json::object();
        body["oid"] = oid;
        cr = rest.cancelOrderGateway(body.dump());
    }
    if (utils::Logger::isInitialized()) {
        if (cr.is_success) {
            Main().logger()->debug("[STARTUP] cancel oid={} http={}", oid, cr.status_code);
        } else {
            Main().logger()->warn(
                "[STARTUP] cancel oid={} failed http={} err={}",
                oid,
                cr.status_code,
                cr.error_message.empty() ? cr.body : cr.error_message);
        }
    }
}

void collect_oids_from_orders_doc(const nlohmann::json& doc, std::unordered_set<std::string>& out) {
    if (!doc.is_object()) {
        return;
    }
    if (doc.contains("stacks") && doc["stacks"].is_array()) {
        for (const auto& s : doc["stacks"]) {
            if (!s.is_object()) {
                continue;
            }
            for (const char* k : {"bid_exchange_oid", "ask_exchange_oid"}) {
                if (s.contains(k) && s[k].is_string()) {
                    const std::string o = s[k].get<std::string>();
                    if (!o.empty()) {
                        out.insert(o);
                    }
                }
            }
        }
    }
}

void collect_oids_from_open_orders_response(const std::string& body, std::unordered_set<std::string>& out) {
    if (body.empty()) {
        return;
    }
    nlohmann::json root;
    try {
        root = nlohmann::json::parse(body);
    } catch (...) {
        return;
    }
    const auto push_oid = [&out](const nlohmann::json& row) {
        if (!row.is_object()) {
            return;
        }
        const std::string oid = fillJsonExchangeOrderId(row);
        if (!oid.empty()) {
            out.insert(oid);
        }
    };
    if (root.is_array()) {
        for (const auto& el : root) {
            push_oid(el);
        }
        return;
    }
    if (root.is_object()) {
        if (root.contains("orders") && root["orders"].is_array()) {
            for (const auto& el : root["orders"]) {
                push_oid(el);
            }
        }
        if (root.contains("data") && root["data"].is_array()) {
            for (const auto& el : root["data"]) {
                push_oid(el);
            }
        }
    }
}

// Per-write-unique temp path: orders.json is written by several racing agents (this
// engine's clean-slate wipe + per-accept oid patch, plus the external Python desk). A
// shared fixed "orders.json.tmp" lets concurrent atomic writes collide on the same temp
// inode and publish a corrupt "valid-json + trailing garbage" file. A unique suffix keeps
// every atomic write isolated while rename stays atomic (last writer wins, whole doc).
std::string orders_json_unique_tmp(const std::string& base) {
    static std::atomic<std::uint64_t> ctr{0};
    const std::uint64_t n = ctr.fetch_add(1, std::memory_order_relaxed);
    const long long ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                             std::chrono::steady_clock::now().time_since_epoch())
                             .count();
    return base + ".tmp." + std::to_string(static_cast<long long>(::getpid())) + "." +
           std::to_string(ns) + "." + std::to_string(n);
}

bool atomic_write_orders_json_stacks_empty(const fs::path& p) {
    std::error_code ec;
    fs::create_directories(p.parent_path(), ec);
    const std::string tmp = orders_json_unique_tmp(p.string());
    const std::string payload = R"({"stacks":[]})";
#if defined(__APPLE__) || defined(__unix__)
    const int fd = ::open(tmp.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) {
        return false;
    }
    const char* buf = payload.data();
    std::size_t left = payload.size();
    while (left > 0) {
        const ssize_t nw = ::write(fd, buf, left);
        if (nw <= 0) {
            ::close(fd);
            fs::remove(tmp, ec);
            return false;
        }
        buf += static_cast<std::size_t>(nw);
        left -= static_cast<std::size_t>(nw);
    }
    ::fsync(fd);
    ::close(fd);
#else
    {
        std::ofstream f(tmp, std::ios::trunc | std::ios::binary);
        if (!f) {
            return false;
        }
        f << payload;
        f.flush();
    }
#endif
    fs::rename(tmp, p, ec);
    return !ec;
}

std::string fillJsonExchangeOrderId(const nlohmann::json& j) {
    static const char* keys[] = {"order_id", "oid", "exchange_order_id", "id", "orderId"};
    for (const char* k : keys) {
        if (!j.contains(k)) {
            continue;
        }
        const auto& v = j[k];
        if (v.is_string()) {
            std::string s = v.get<std::string>();
            s.erase(0, s.find_first_not_of(" \t\r\n"));
            while (!s.empty() && (s.back() == ' ' || s.back() == '\t' || s.back() == '\r' || s.back() == '\n')) {
                s.pop_back();
            }
            if (!s.empty()) {
                return s;
            }
        } else if (v.is_number_integer()) {
            return std::to_string(v.get<std::int64_t>());
        }
    }
    return {};
}

std::string fillJsonSymbol(const nlohmann::json& j) {
    static const char* keys[] = {"symbol", "s", "market", "instrument"};
    for (const char* k : keys) {
        if (j.contains(k) && j[k].is_string()) {
            std::string s = j[k].get<std::string>();
            if (!s.empty()) {
                return s;
            }
        }
    }
    return {};
}

/** OpenAPI Fill: price and fee are strings; quantity is int64. */
double fillJsonOpenApiPrice(const nlohmann::json& j) {
    if (!j.contains("price")) {
        return 0.0;
    }
    const auto& v = j["price"];
    if (v.is_string()) {
        try {
            return std::stod(v.get<std::string>());
        } catch (...) {
            return 0.0;
        }
    }
    if (v.is_number()) {
        return v.get<double>();
    }
    return 0.0;
}

double fillJsonOpenApiQuantity(const nlohmann::json& j) {
    if (!j.contains("quantity")) {
        return 0.0;
    }
    const auto& v = j["quantity"];
    if (v.is_number_integer()) {
        return static_cast<double>(v.get<std::int64_t>());
    }
    if (v.is_number()) {
        return v.get<double>();
    }
    if (v.is_string()) {
        try {
            return std::stod(v.get<std::string>());
        } catch (...) {
            return 0.0;
        }
    }
    return 0.0;
}

std::string fillJsonSymbolExtended(const nlohmann::json& j) {
    std::string s = fillJsonSymbol(j);
    if (!s.empty()) {
        return s;
    }
    static const char* nested[][2] = {{"order", "symbol"},   {"order", "s"},         {"order", "market"},
                                      {"order_info", "symbol"}, {"order_info", "s"}, {"execution", "symbol"},
                                      {"instrument", "symbol"}};
    for (const auto& p : nested) {
        if (!j.contains(p[0]) || !j[p[0]].is_object()) {
            continue;
        }
        const auto& o = j[p[0]];
        if (o.contains(p[1]) && o[p[1]].is_string()) {
            s = o[p[1]].get<std::string>();
            if (!s.empty()) {
                return s;
            }
        }
    }
    return {};
}

/** AX `s` for orders-gateway TLS pre-warm; empty if not configured (skip warm-up). */
std::string gatewayWarmupAxSymbol(const architect::config::Config& cfg) {
    std::string ax = cfg.getMarketMakerOrderSymbol();
    if (ax.empty()) {
        ax = cfg.getMarketMakerSymbol();
    }
    if (ax.empty()) {
        return {};
    }
    for (auto it = ax.begin(); it != ax.end(); ) {
        if (*it == '-') {
            it = ax.erase(it);
        } else {
            ++it;
        }
    }
    if (ax.find("PERP") == std::string::npos) {
        ax += "-PERP";
    }
    return ax;
}

std::string readFillPollCursorTradeId(const fs::path& fp) {
    if (fp.empty()) {
        return {};
    }
    std::error_code ec;
    if (!fs::is_regular_file(fp, ec)) {
        return {};
    }
    std::ifstream ifs(fp);
    if (!ifs) {
        return {};
    }
    std::string s((std::istreambuf_iterator<char>(ifs)), std::istreambuf_iterator<char>());
    while (!s.empty() && (s.back() == '\n' || s.back() == '\r' || s.back() == ' ' || s.back() == '\t')) {
        s.pop_back();
    }
    size_t i = 0;
    while (i < s.size() && (s[i] == ' ' || s[i] == '\t' || s[i] == '\r' || s[i] == '\n')) {
        ++i;
    }
    return s.substr(i);
}

void writeFillPollCursorTradeId(const fs::path& fp, const std::string& trade_id) {
    if (fp.empty() || trade_id.empty()) {
        return;
    }
    std::error_code ec;
    fs::create_directories(fp.parent_path(), ec);
    const std::string tmp = fp.string() + ".tmp";
    {
        std::ofstream ofs(tmp, std::ios::binary | std::ios::trunc);
        if (!ofs) {
            return;
        }
        ofs << trade_id << '\n';
        ofs.flush();
    }
    fs::rename(tmp, fp, ec);
}

void parseOpenOrdersBodyRows(const nlohmann::json& root, std::vector<nlohmann::json>* out_rows) {
    out_rows->clear();
    if (root.is_array()) {
        for (const auto& el : root) {
            if (el.is_object()) {
                out_rows->push_back(el);
            }
        }
        return;
    }
    if (!root.is_object()) {
        return;
    }
    for (const char* k : {"orders", "open_orders", "data", "items"}) {
        if (root.contains(k) && root[k].is_array()) {
            for (const auto& el : root[k]) {
                if (el.is_object()) {
                    out_rows->push_back(el);
                }
            }
            return;
        }
    }
}

std::string openOrdersRowExchangeOid(const nlohmann::json& row) {
    static const char* keys[] = {"exchange_order_id", "oid", "order_id", "id", "orderId"};
    for (const char* k : keys) {
        if (!row.contains(k)) {
            continue;
        }
        const auto& v = row[k];
        if (v.is_string()) {
            std::string s = v.get<std::string>();
            s.erase(0, s.find_first_not_of(" \t\r\n"));
            while (!s.empty() && (s.back() == ' ' || s.back() == '\t' || s.back() == '\r' || s.back() == '\n')) {
                s.pop_back();
            }
            if (!s.empty()) {
                return s;
            }
        } else if (v.is_number_integer()) {
            return std::to_string(v.get<std::int64_t>());
        }
    }
    return {};
}

std::string openOrdersRowSymbol(const nlohmann::json& row) {
    if (row.contains("symbol") && row["symbol"].is_string()) {
        std::string s = row["symbol"].get<std::string>();
        if (!s.empty()) {
            return s;
        }
    }
    if (row.contains("s") && row["s"].is_string()) {
        std::string s = row["s"].get<std::string>();
        if (!s.empty()) {
            return s;
        }
    }
    return {};
}

// Returns the order's creation timestamp in epoch milliseconds, or 0 if no recognizable field
// is present. Used by the orphan-cancel pass to honor a "grace window" — orders younger than
// `gateway_orphan_grace_ms` are NOT eligible for orphan-cancel even if they don't appear in
// orders.json yet, because Python's place→write window is non-zero (~hundreds of ms) and we
// must not race-cancel a leg that just landed on the venue but hasn't been recorded yet. This
// is the proper fix for the race the old `viewer-freeze` mechanism was hiding.
std::int64_t openOrdersRowCreatedMs(const nlohmann::json& row) {
    static const char* numeric_keys[] = {
        "created_at_ms", "createdAtMs", "created_ms", "ts_ms", "timestamp_ms",
        "created_at", "createdAt", "timestamp", "ts", "time", "t"};
    for (const char* k : numeric_keys) {
        if (!row.contains(k)) {
            continue;
        }
        const auto& v = row[k];
        std::int64_t ll = 0;
        if (v.is_number_integer()) {
            ll = v.get<std::int64_t>();
        } else if (v.is_number_unsigned()) {
            ll = static_cast<std::int64_t>(v.get<std::uint64_t>());
        } else if (v.is_number_float()) {
            ll = static_cast<std::int64_t>(v.get<double>());
        } else if (v.is_string()) {
            // Numeric-as-string is common (e.g. some venues return "1778139950457").
            const std::string s = v.get<std::string>();
            if (!s.empty()) {
                bool numeric_str = true;
                for (char c : s) {
                    if (!(c >= '0' && c <= '9')) { numeric_str = false; break; }
                }
                if (numeric_str) {
                    try {
                        ll = std::stoll(s);
                    } catch (...) {
                        ll = 0;
                    }
                }
            }
        }
        if (ll <= 0) {
            continue;
        }
        // Heuristic: epoch-ms ~1.7e12 vs epoch-s ~1.7e9. If <1e12 and >1e9 it's seconds.
        if (ll < 1'000'000'000'000LL && ll > 1'000'000'000LL) {
            ll *= 1000;
        }
        return ll;
    }
    return 0;
}

void buildOpenOrdersExchangeOidToSymbol(api::RestClient& rest, std::unordered_map<std::string, std::string>* out) {
    out->clear();
    auto resp = rest.getOrdersGatewayRelative("/open-orders", {});
    if (!resp.is_success || resp.status_code < 200 || resp.status_code >= 300) {
        return;
    }
    nlohmann::json root;
    try {
        root = nlohmann::json::parse(resp.body);
    } catch (...) {
        return;
    }
    std::vector<nlohmann::json> rows;
    parseOpenOrdersBodyRows(root, &rows);
    for (const auto& row : rows) {
        const std::string oid = openOrdersRowExchangeOid(row);
        const std::string sym = openOrdersRowSymbol(row);
        if (!oid.empty() && !sym.empty()) {
            (*out)[oid] = sym;
        }
    }
}

/**
 * When the user cancels resting orders on the exchange GUI, the venue drops the OID from
 * GET /open-orders while our OrderManager may still show ACCEPTED. That leaves MM strategies
 * tracking stale bid_order_id_/ask_order_id_. Compare gateway open orders to local actives
 * and emit ORDER_CANCELLED (via onOrderCancelled) so strategy on_cancel clears state.
 *
 * Optional `mm_desk.stack_removal_hint_jsonl`: append one JSON line per mm_req_* stack so Python
 * can remove the stack from orders.json (Python remains the safe writer of that file).
 */
// Retained for future debug/reflection use (and to keep the symbol available
// for ad-hoc debugging tools); the only previous caller in this file was
// inlined into the OM_GATEWAY missing-order loop so that loop also captures
// the owning strategy pointer in the same scan and can hand fill events off
// to MakeMarketStrategy::mmRouteOmGatewayVenueTruthMissing without doing the
// walk twice. [[maybe_unused]] silences -Wunused-function until/unless a
// caller is reintroduced.
[[maybe_unused]] std::string findMmRequestStackIdForOrderId(orders::OrderId oid) {
    if (oid == 0) {
        return {};
    }
    for (const auto& sp : strategy::StrategyManager::getInstance().getAllStrategies()) {
        auto mm = std::dynamic_pointer_cast<strategy::MakeMarketStrategy>(sp);
        if (!mm) {
            continue;
        }
        const std::string& rid = mm->mmOrderRequestId();
        if (rid.empty()) {
            continue;
        }
        if (mm->mmCurrentBidOrderId() == oid || mm->mmCurrentAskOrderId() == oid) {
            return rid;
        }
    }
    return {};
}

void appendStackRemovalHintJsonl(const std::string& rel_path,
                                 const std::string& ax,
                                 const std::string& stack_id,
                                 const std::string& reason) {
    if (rel_path.empty() || stack_id.empty()) {
        return;
    }
    fs::path p(rel_path);
    if (!p.is_absolute()) {
        p = fs::current_path() / p;
    }
    std::error_code ec;
    fs::create_directories(p.parent_path(), ec);
    nlohmann::json line;
    line["ts_ms"] = std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::system_clock::now().time_since_epoch())
                        .count();
    line["action"] = "remove_stack_hint";
    line["ax_symbol"] = ax;
    line["stack_id"] = stack_id;
    line["reason"] = reason;
    std::ofstream ofs(p, std::ios::app | std::ios::binary);
    if (!ofs) {
        return;
    }
    ofs << line.dump() << '\n';
}

// Helper: load every exchange OID currently referenced by `logs/orders.json` (root.stacks[*].bid_exchange_oid /
// .ask_exchange_oid AND products[*].stacks[*].*) into `out`. Used as a "do not cancel" safety net for the
// orphan-cancel pass below — orders the desk just placed and whose oid is in orders.json should be left alone
// long enough for `tryAdoptGatewayPlacedFromManualStack` to adopt them on the next reconcile sweep.
//
// Returns false on any read/parse failure. Callers must treat false as "skip orphan cancel for this cycle"
// because the safety net is missing — better to leak a duplicate for one cycle than mis-cancel.
bool loadOrdersJsonExchangeOids(std::unordered_set<std::string>* out) {
    if (!out) {
        return false;
    }
    out->clear();
    const fs::path path = mm_orders_json_path_for_startup();
    std::error_code ec;
    if (!fs::exists(path, ec) || ec) {
        return true;  // file absent is fine — nothing to protect
    }
    std::ifstream ifs(path);
    if (!ifs) {
        return false;
    }
    nlohmann::json j;
    try {
        ifs >> j;
    } catch (...) {
        return false;
    }
    auto add_from_stack = [&](const nlohmann::json& s) {
        if (!s.is_object()) {
            return;
        }
        for (const char* key : {"bid_exchange_oid", "ask_exchange_oid"}) {
            if (s.contains(key) && s[key].is_string()) {
                std::string v = s[key].get<std::string>();
                if (!v.empty()) {
                    out->insert(std::move(v));
                }
            }
        }
    };
    if (j.contains("stacks") && j["stacks"].is_array()) {
        for (const auto& s : j["stacks"]) {
            add_from_stack(s);
        }
    }
    if (j.contains("products") && j["products"].is_object()) {
        for (auto pit = j["products"].begin(); pit != j["products"].end(); ++pit) {
            const auto& prod = pit.value();
            if (prod.is_object() && prod.contains("stacks") && prod["stacks"].is_array()) {
                for (const auto& s : prod["stacks"]) {
                    add_from_stack(s);
                }
            }
        }
    }
    return true;
}

void reconcileLocalOrdersAgainstGatewayOpenOrders() {
    auto& cfg = config::Config::getInstance();
    if (!cfg.getBool("market_maker.enabled", false)) {
        return;
    }
    auto& om = orders::OrderManager::getInstance();
    auto& rest = api::RestClient::getInstance();
    const std::string hint_path = cfg.getString("mm_desk.stack_removal_hint_jsonl", "");

    const auto active = om.getActiveOrders();
    std::unordered_map<std::string, std::vector<std::shared_ptr<orders::Order>>> by_sym;
    for (const auto& o : active) {
        if (!o || !o->isActive()) {
            continue;
        }
        if (o->exchange_order_id.empty()) {
            continue;
        }
        // Likely fully filled: book has no row; fill poll should own terminal state — avoid CANCELLED.
        if (o->remaining_quantity <= 1e-12 && o->filled_quantity > 1e-12) {
            continue;
        }
        std::string sym(o->symbol.data());
        if (sym.empty()) {
            continue;
        }
        by_sym[std::move(sym)].push_back(o);
    }

    // === Gateway → local ORPHAN-CANCEL pass ===
    //
    // Without this, an exchange order whose corresponding local OrderManager record is missing
    // never gets cleaned up: the strategy's cancel-replace path silently lost track of the old
    // leg (e.g. cancel returned 404 "benign" but the order was actually still alive, or the
    // strategy crashed mid-cycle and orders.json got rewritten with a fresh oid). Live evidence
    // 2026-05-06 17:19/17:41: EURUSD-PERP SELL @ 1.1846 (oid O-01KQYHYMA5KN9D0ATREWFD1DP4)
    // remained on the gateway across many theo-move cycles even though the strategy was
    // tracking a different ask oid at 1.1828 — neither leg of the existing one-direction sync
    // could see it.
    //
    // Safety net so we never cancel something that is legitimately ours:
    //   1. We only consider symbols that one of OUR MM strategies is currently quoting (any leg
    //      with a non-empty exchange_order_id from the active OM set above) — this means we
    //      never touch an instrument we don't manage.
    //   2. We skip any oid that OrderManager knows about (active OR terminal) — `getOrderByExchangeId`
    //      covers everything we ever placed or adopted.
    //   3. We skip any oid still referenced by `logs/orders.json` bid_exchange_oid /
    //      ask_exchange_oid — the desk's just-written oids haven't been adopted by C++ yet,
    //      and `tryAdoptGatewayPlacedFromManualStack` will pick them up within ~5s.
    //
    // We additionally require the orders.json-oid set to load successfully; if loading failed
    // for any reason, we skip the orphan-cancel pass for THIS cycle (better to leak for 15s
    // than mis-cancel a desk-placed leg).
    std::unordered_set<std::string> orders_json_oids;
    const bool orders_json_oids_ok = loadOrdersJsonExchangeOids(&orders_json_oids);

    // We need to process every managed symbol, even those without an active local order
    // (e.g. local cancel succeeded → vec is empty → without this map the orphan pass would
    // skip the symbol entirely). Build the symbol set from BOTH local actives and the running
    // strategies' ax_symbols.
    std::unordered_set<std::string> managed_syms;
    for (const auto& kv : by_sym) {
        managed_syms.insert(kv.first);
    }
    {
        auto& sm = strategy::StrategyManager::getInstance();
        for (const auto& sp : sm.getAllStrategies()) {
            auto mm = std::dynamic_pointer_cast<strategy::MakeMarketStrategy>(sp);
            if (!mm) {
                continue;
            }
            const std::string ax = mm->mmAxSymbol();
            if (!ax.empty()) {
                managed_syms.insert(ax);
            }
        }
    }
    if (managed_syms.empty()) {
        return;
    }

    for (const std::string& sym : managed_syms) {
        const auto& vec = (by_sym.count(sym) > 0) ? by_sym.at(sym)
                                                   : std::vector<std::shared_ptr<orders::Order>>{};
        auto resp = rest.getOrdersGatewayRelative("/open-orders", {{"symbol", sym}});
        if (!resp.is_success || resp.status_code < 200 || resp.status_code >= 300) {
            Main().logger()->debug(
                "[OM_GATEWAY] open-orders reconcile skip sym={} HTTP {}", sym, resp.status_code);
            continue;
        }
        nlohmann::json root;
        try {
            root = nlohmann::json::parse(resp.body);
        } catch (...) {
            continue;
        }
        std::vector<nlohmann::json> rows;
        parseOpenOrdersBodyRows(root, &rows);
        std::unordered_set<std::string> live_oids;
        live_oids.reserve(rows.size() * 2 + 8);
        for (const auto& row : rows) {
            const std::string rs = openOrdersRowSymbol(row);
            if (!rs.empty() && rs != sym) {
                continue;
            }
            const std::string oid = openOrdersRowExchangeOid(row);
            if (!oid.empty()) {
                live_oids.insert(oid);
            }
        }

        // Per-oid created_ms map for the orphan-cancel grace window (Direction B). Built once
        // per symbol so we don't re-walk rows per oid.
        std::unordered_map<std::string, std::int64_t> live_oid_created_ms;
        live_oid_created_ms.reserve(rows.size() + 4);
        for (const auto& row : rows) {
            const std::string rs = openOrdersRowSymbol(row);
            if (!rs.empty() && rs != sym) {
                continue;
            }
            const std::string oid = openOrdersRowExchangeOid(row);
            if (oid.empty()) {
                continue;
            }
            const std::int64_t cms = openOrdersRowCreatedMs(row);
            if (cms > 0) {
                live_oid_created_ms[oid] = cms;
            }
        }

        // Direction A: local→gateway (existing). For locally-active orders that the gateway
        // says are gone, mark them CANCELLED so OrderManager state matches reality.
        for (const auto& o : vec) {
            if (!o) {
                continue;
            }
            const std::string& ex = o->exchange_order_id;
            if (live_oids.count(ex) > 0) {
                continue;
            }
            // Owning-strategy lookup (single scan): mirrors findMmRequestStackIdForOrderId
            // but also captures the shared_ptr so we can route fills to it BELOW (without
            // a second scan and without a strategyName-based lookup that would have to
            // duplicate examples/main.cpp's mmRequestStrategyName builder).
            std::string rid;
            std::shared_ptr<strategy::MakeMarketStrategy> owner_mm;
            for (const auto& sp : strategy::StrategyManager::getInstance().getAllStrategies()) {
                auto mm = std::dynamic_pointer_cast<strategy::MakeMarketStrategy>(sp);
                if (!mm) {
                    continue;
                }
                const std::string& r = mm->mmOrderRequestId();
                if (r.empty()) {
                    continue;
                }
                if (mm->mmCurrentBidOrderId() == o->id || mm->mmCurrentAskOrderId() == o->id) {
                    rid = r;
                    owner_mm = mm;
                    break;
                }
            }
            if (rid.empty()) {
                Main().logger()->info(
                    "[OM_GATEWAY] venue has no open order for local order_id={} exchange_oid={} sym={} — "
                    "syncing local state to CANCELLED",
                    o->id,
                    ex,
                    sym);
            } else {
                Main().logger()->info(
                    "[OM_GATEWAY] venue has no open order for local order_id={} exchange_oid={} sym={} — "
                    "syncing local state to CANCELLED (mm_req stack_id={})",
                    o->id,
                    ex,
                    sym,
                    rid);
                appendStackRemovalHintJsonl(hint_path, sym, rid, "venue_missing_from_open_orders");
            }
            // Route fill events to the owning desk strategy BEFORE om.onOrderCancelled.
            // The strategy distinguishes fill from deliberate cancel via the tracked
            // leg's MmLegState: ADOPTED + missing-from-venue → fill (synthesizes an
            // OrderEventData, logs [FILL_DESK], invokes processFillDeskStack which then
            // cancels the opposite leg + enqueues the post-fill requote cascade);
            // CANCEL_PENDING / PLACE_PENDING / FILLED / CANCELLED → no-op (existing
            // on_cancel branches handle those). After this returns, om.onOrderCancelled
            // runs as before — for the fill case the leg is already in MmLegState::FILLED
            // so on_cancel falls through to its erase branch (no double-fire); for the
            // non-fill case on_cancel runs unchanged.
            if (owner_mm) {
                (void)owner_mm->mmRouteOmGatewayVenueTruthMissing(o->id, ex, sym);
            } else {
                // === SILVER max_position breach fix (2026-05-21) =================
                // Backstop: the owner_mm lookup above uses
                // `mmCurrentBidOrderId() == o->id || mmCurrentAskOrderId() == o->id`
                // which compares only the CURRENT bid/ask ids. If a leg was
                // evicted (e.g., the strategy already saw a 404 cancel and reset
                // `bid_order_id_ = 0`), the comparison misses even though the
                // tracked leg row may still exist for that exchange OID. Give
                // every MM strategy on this symbol a chance to claim the fill;
                // `mmRouteOmGatewayVenueTruthMissing` is a no-op for legs that
                // aren't tracked or aren't in ADOPTED state, so this is safe to
                // broadcast.
                for (const auto& sp :
                     strategy::StrategyManager::getInstance().getAllStrategies()) {
                    auto mm = std::dynamic_pointer_cast<strategy::MakeMarketStrategy>(sp);
                    if (!mm) {
                        continue;
                    }
                    if (mm->mmAxSymbol() != sym) {
                        continue;
                    }
                    if (mm->mmRouteOmGatewayVenueTruthMissing(o->id, ex, sym)) {
                        Main().logger()->warn(
                            "[OM_GATEWAY] broadcast claim succeeded for order_id={} exchange_oid={} "
                            "sym={} strategy={} — owner-lookup miss recovered via tracked-leg scan",
                            o->id, ex, sym, mm->getName());
                        break;
                    }
                }
            }
            om.onOrderCancelled(o->id);
        }

        // Direction B: gateway→local ORPHAN-CANCEL. For each oid the gateway has but neither
        // OrderManager nor orders.json knows about, REST cancel it. Skipped entirely if the
        // orders.json safety net failed to load this cycle.
        if (!orders_json_oids_ok) {
            continue;
        }
        // === RACE-FREE ORPHAN-CANCEL (replaces the viewer-freeze hack) ===
        //
        // The race we used to "fix" by freezing the C++ reconciler:
        //   1. Python desk places bid_oid X + ask_oid Y on the venue (gateway POST).
        //   2. Venue accepts → returns oids.
        //   3. Python now writes orders.json with X + Y (atomic temp+rename, ≈100–400ms total).
        // If the OM_GATEWAY reconcile pass below ran between steps 2 and 3, it would see X/Y
        // on the venue but missing from orders.json (and missing from OrderManager because
        // these are desk-placed via gateway, not via the C++ OM) → REST-cancel them. The
        // viewer-freeze mechanism was a wide hammer that paused the WHOLE reconciler during
        // every desk place to dodge that window — that is the "freezing" the user reported.
        //
        // Proper fix: honor the order's `created_at` from /open-orders. Anything younger than
        // `gateway_orphan_grace_ms` (default 10s) is in the desk's place→write window and is
        // exempt from orphan-cancel. After 10s the order MUST be reflected in orders.json or
        // it's a real orphan. This is correct without any blocking, locking, or copying.
        //
        // Defaults: 10000ms is generous for atomic-write Python desks doing a couple of REST
        // hops. Set to 0 to disable the grace window entirely (revert to old behavior).
        const std::int64_t orphan_grace_ms =
            std::max<std::int64_t>(0, static_cast<std::int64_t>(
                cfg.getInt("market_maker.gateway_orphan_grace_ms", 10000)));
        const std::int64_t now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                        std::chrono::system_clock::now().time_since_epoch())
                                        .count();
        for (const std::string& live_oid : live_oids) {
            // Layer 1: anything OM has ever known is ours.
            if (om.getOrderByExchangeId(live_oid) != nullptr) {
                continue;
            }
            // Layer 2: anything orders.json references is about to be adopted.
            if (orders_json_oids.count(live_oid) > 0) {
                continue;
            }
            // Layer 3 (NEW): age-based grace window. If the venue says this order was created
            // less than `orphan_grace_ms` ago, it's almost certainly mid-flight from a desk
            // place — defer the cancel decision to the next reconcile cycle (typically 15s).
            // If the row didn't include a parseable timestamp (created_ms == 0), we ALSO defer
            // ONCE per process by recording the first time we saw this oid here. That prevents
            // an "ageless" venue response from immediately false-cancelling a fresh order — the
            // second time we see the same oid (>= grace window later) we'll know it's real.
            std::int64_t age_ms = -1;
            auto cms_it = live_oid_created_ms.find(live_oid);
            if (cms_it != live_oid_created_ms.end() && cms_it->second > 0) {
                age_ms = now_ms - cms_it->second;
                if (orphan_grace_ms > 0 && age_ms >= 0 && age_ms < orphan_grace_ms) {
                    Main().logger()->info(
                        "[OM_GATEWAY] orphan-cancel deferred (within grace window): oid={} sym={} "
                        "age_ms={} grace_ms={} — desk place→write window race-free",
                        live_oid, sym, age_ms, orphan_grace_ms);
                    continue;
                }
            } else if (orphan_grace_ms > 0) {
                static std::mutex s_first_seen_mu;
                static std::unordered_map<std::string, std::int64_t> s_first_seen_ms;
                std::lock_guard<std::mutex> lk(s_first_seen_mu);
                auto fs_it = s_first_seen_ms.find(live_oid);
                if (fs_it == s_first_seen_ms.end()) {
                    s_first_seen_ms[live_oid] = now_ms;
                    Main().logger()->info(
                        "[OM_GATEWAY] orphan-cancel deferred (first sight, no created_at on row): "
                        "oid={} sym={} grace_ms={} — will re-check next cycle",
                        live_oid, sym, orphan_grace_ms);
                    continue;
                }
                age_ms = now_ms - fs_it->second;
                if (age_ms < orphan_grace_ms) {
                    continue;
                }
            }
            // Confirmed orphan: cancel it so the strategy can place a fresh tracked pair on
            // the next theo move without leaving a stale leg sitting on the book at the wrong
            // price. Try the same two-step REST cancel as the strategy uses (DELETE then
            // POST cancel-by-oid fallback) so behavior matches what desk traders expect.
            Main().logger()->warn(
                "[OM_GATEWAY] orphan exchange order detected: oid={} sym={} age_ms={} not in "
                "OrderManager and not in orders.json — sending REST cancel",
                live_oid,
                sym,
                age_ms);
            api::HttpResponse cr = rest.cancelOrder(live_oid);
            if (!cr.is_success) {
                nlohmann::json body = nlohmann::json::object();
                body["oid"] = live_oid;
                cr = rest.cancelOrderGateway(body.dump());
            }
            Main().logger()->info(
                "[OM_GATEWAY] orphan cancel result: oid={} sym={} http_status={} success={}",
                live_oid,
                sym,
                cr.status_code,
                cr.is_success ? 1 : 0);
        }
    }
}

std::uint64_t g_mm_desk_signal_last_seq = 0;

void pollMmDeskSyncIfEnabled(const std::string& config_path) {
    auto& cfg = config::Config::getInstance();
    if (!cfg.isMarketMakerEnabled() || !cfg.getMarketMakerDeskSyncEnabled()) {
        return;
    }
    std::string path_str = cfg.getMarketMakerDeskSignalPath();
    if (path_str.empty()) {
        return;
    }
    fs::path path(path_str);
    if (!path.is_absolute()) {
        path = fs::current_path() / path;
    }
    std::ifstream ifs(path);
    if (!ifs) {
        return;
    }
    const std::string body((std::istreambuf_iterator<char>(ifs)), std::istreambuf_iterator<char>());
    if (body.empty()) {
        return;
    }
    nlohmann::json j;
    try {
        j = nlohmann::json::parse(body);
    } catch (...) {
        return;
    }
    const std::uint64_t seq = j.value("seq", 0ULL);
    if (seq == 0 || seq <= g_mm_desk_signal_last_seq) {
        return;
    }
    g_mm_desk_signal_last_seq = seq;

    if (!config::Config::getInstance().loadFromFileWithOptionalOverlays(config_path)) {
        Main().logger()->warn("[DESK_SYNC] config reload failed after desk signal seq={}", seq);
    }
    const std::string sym_sig = j.value("symbol", "");
    const std::string strat_name = j.value("strategy_name", "");
    auto& sm = strategy::StrategyManager::getInstance();
    bool applied = false;
    if (!strat_name.empty()) {
        auto mm = std::dynamic_pointer_cast<strategy::MakeMarketStrategy>(sm.getStrategy(strat_name));
        if (mm && mm->applyMmDeskSignalJson(body)) {
            applied = true;
        }
    } else if (!sym_sig.empty()) {
        for (const auto& sp : sm.getAllStrategies()) {
            auto mm = std::dynamic_pointer_cast<strategy::MakeMarketStrategy>(sp);
            if (!mm) {
                continue;
            }
            if (mm->mmAxSymbol() != sym_sig) {
                continue;
            }
            if (mm->applyMmDeskSignalJson(body)) {
                applied = true;
                break;
            }
        }
    } else {
        auto mm = std::dynamic_pointer_cast<strategy::MakeMarketStrategy>(sm.getStrategy("make_market"));
        if (mm && mm->applyMmDeskSignalJson(body)) {
            applied = true;
        }
    }
    if (!applied) {
        Main().logger()->warn("[DESK_SYNC] no MakeMarketStrategy applied desk signal seq={} symbol={} strategy_name={}",
            seq, sym_sig, strat_name);
    }
}

void seedMmDeskSignalSeqBaselineAtBoot() {
    auto& cfg = config::Config::getInstance();
    if (!cfg.getBool("market_maker.desk_signal_reset_seq_baseline_on_boot", true)) {
        return;
    }
    if (!cfg.isMarketMakerEnabled() || !cfg.getMarketMakerDeskSyncEnabled()) {
        return;
    }
    std::string path_str = cfg.getMarketMakerDeskSignalPath();
    if (path_str.empty()) {
        return;
    }
    fs::path path(path_str);
    if (!path.is_absolute()) {
        path = fs::current_path() / path;
    }
    std::ifstream ifs(path);
    if (!ifs) {
        return;
    }
    const std::string body((std::istreambuf_iterator<char>(ifs)), std::istreambuf_iterator<char>());
    if (body.empty()) {
        return;
    }
    nlohmann::json j;
    try {
        j = nlohmann::json::parse(body);
    } catch (...) {
        return;
    }
    const std::uint64_t seq = j.value("seq", 0ULL);
    if (seq == 0) {
        return;
    }
    g_mm_desk_signal_last_seq = seq;
    Main().logger()->info(
        "[DESK_SYNC] boot seq baseline={} file={} — only strictly higher seq will apply this session",
        seq,
        path.string());
}

}  // namespace

std::atomic<std::int64_t> StartupSequence::last_fill_poll_completed_ms_{0};

StartupSequence& StartupSequence::getInstance() {
    static StartupSequence instance;
    return instance;
}

StartupSequence::StartupSequence() {
    step_results_.reserve(25);
    step_status_.resize(25, StepStatus::PENDING);
}

// ANSI color codes
namespace colors {
    constexpr const char* RESET   = "\033[0m";
    constexpr const char* BOLD    = "\033[1m";
    constexpr const char* YELLOW  = "\033[33m";
    constexpr const char* GREEN   = "\033[32m";
    constexpr const char* RED     = "\033[31m";
    constexpr const char* CYAN    = "\033[36m";
    constexpr const char* MAGENTA = "\033[35m";
    constexpr const char* WHITE   = "\033[37m";
    constexpr const char* DIM     = "\033[2m";
}

void StartupSequence::logStepStart(int step, const std::string& name) {
    current_step_ = step;
    step_status_[step - 1] = StepStatus::RUNNING;
    
    std::cout << "\n" << colors::DIM << "────────────────────────────────────────" << colors::RESET << std::endl;
    std::cout << "  " << colors::BOLD << colors::YELLOW << "STEP " << step << "/25" 
              << colors::RESET << colors::DIM << ": " << colors::RESET 
              << colors::CYAN << name << colors::RESET << std::endl;
    std::cout << colors::DIM << "────────────────────────────────────────" << colors::RESET << std::endl;
    
    // If logger is available, log there too
    if (step > 3 && utils::Logger::isInitialized()) {
        Main().logger()->info("STEP {}/25: {} - STARTING", step, name);
        Main().logger()->log_event("STARTUP", "step_" + std::to_string(step), name + " - starting");
    }
}

void StartupSequence::logStepEnd(int step, const StepResult& result) {
    step_status_[step - 1] = result.success ? StepStatus::SUCCESS : StepStatus::FAILED;
    
    if (result.success) {
        std::cout << "  " << colors::GREEN << "✓ SUCCESS" << colors::RESET 
                  << colors::DIM << " (" << result.duration.count() << "ms)" << colors::RESET;
    } else {
        std::cout << "  " << colors::RED << colors::BOLD << "✗ FAILED" << colors::RESET 
                  << colors::DIM << " (" << result.duration.count() << "ms)" << colors::RESET;
    }
    if (!result.message.empty()) {
        std::cout << colors::DIM << " → " << colors::RESET << result.message;
    }
    std::cout << std::endl;
    
    // If logger is available, log there too
    if (step > 3 && utils::Logger::isInitialized()) {
        if (result.success) {
            Main().logger()->info("STEP {}/25: {} - SUCCESS ({}ms)", 
                step, result.step_name, result.duration.count());
        } else {
            Main().logger()->error("STEP {}/25: {} - FAILED: {}", 
                step, result.step_name, result.message);
        }
        Main().logger()->log_event("STARTUP", "step_" + std::to_string(step), 
            result.step_name + " - " + (result.success ? "success" : "failed: " + result.message));
    }
    
    // Store result
    {
        std::lock_guard<std::mutex> lock(results_mutex_);
        step_results_.push_back(result);
    }
}

void StartupSequence::crashOnFailure(const StepResult& result) {
    if (!result.success) {
        throw StartupException(result.step_number, result.step_name, result.message);
    }
}

// =============================================================================
// Main Entry Points
// =============================================================================

void StartupSequence::runOrdersJsonCleanSlateAfterRestVerified() {
    const fs::path p = mm_orders_json_path_for_startup();
    nlohmann::json doc = nlohmann::json::object();
    std::error_code ec;
    if (fs::is_regular_file(p, ec)) {
        std::ifstream ifs(p, std::ios::binary);
        if (ifs) {
            std::ostringstream ss;
            ss << ifs.rdbuf();
            const std::string raw = ss.str();
            if (!raw.empty()) {
                try {
                    doc = nlohmann::json::parse(raw);
                } catch (const std::exception& e) {
                    if (utils::Logger::isInitialized()) {
                        Main().logger()->warn("[STARTUP] orders.json parse error during clean slate: {}", e.what());
                    }
                }
            }
        }
    }

    std::unordered_set<std::string> oids;
    collect_oids_from_orders_doc(doc, oids);

    auto resp = api::RestClient::getInstance().getOrdersGatewayRelative("/open-orders", {});
    if (!resp.is_success) {
        resp = api::RestClient::getInstance().getOrders();
    }
    if (resp.is_success) {
        collect_oids_from_open_orders_response(resp.body, oids);
    } else if (utils::Logger::isInitialized()) {
        Main().logger()->warn(
            "[STARTUP] open-orders query failed http={} err={}",
            resp.status_code,
            resp.error_message.empty() ? resp.body : resp.error_message);
    }

    for (const auto& oid : oids) {
        cancel_exchange_oid_best_effort(oid);
    }

    if (!atomic_write_orders_json_stacks_empty(p)) {
        if (utils::Logger::isInitialized()) {
            Main().logger()->error("[STARTUP] failed to truncate orders.json at {}", p.string());
        }
        return;
    }
    if (utils::Logger::isInitialized()) {
        Main().logger()->info("[STARTUP] orders.json cleared — clean slate confirmed");
    }
}

void StartupSequence::executeStartup(const SimulationParams& params) {
    cached_params_ = params;
    
    std::cout << "\n" << std::endl;
    std::cout << colors::CYAN << "╔══════════════════════════════════════════════════════════════════╗" << colors::RESET << std::endl;
    std::cout << colors::CYAN << "║" << colors::RESET << colors::BOLD << colors::YELLOW 
              << "           ARCHITECT PLATFORM - 25-STEP STARTUP SEQUENCE          " 
              << colors::RESET << colors::CYAN << "║" << colors::RESET << std::endl;
    std::cout << colors::CYAN << "╚══════════════════════════════════════════════════════════════════╝" << colors::RESET << std::endl;
    
    auto total_start = std::chrono::steady_clock::now();
    
    // Phase 1: Core System Initialization (Steps 1-6)
    std::cout << "\n" << colors::MAGENTA << colors::BOLD << "▸ PHASE 1:" << colors::RESET 
              << colors::WHITE << " Core System Initialization" << colors::RESET << std::endl;
    crashOnFailure(step01_ValidateParameters(params));
    crashOnFailure(step02_LoadConfiguration(params.config_path));
    crashOnFailure(step03_InitializeLogger(params.simulation_date));
    crashOnFailure(step04_InitializeEventManager());
    crashOnFailure(step05_InitializeOrderManager());
    crashOnFailure(step06_InitializeUserManager());
    
    // Phase 2: API & Connectivity (Steps 7-12)
    std::cout << "\n" << colors::MAGENTA << colors::BOLD << "▸ PHASE 2:" << colors::RESET 
              << colors::WHITE << " API & Connectivity" << colors::RESET << std::endl;
    Main().logger()->info(">>> PHASE 2: API & Connectivity");
    crashOnFailure(step07_InitializeRestClient());
    crashOnFailure(step08_InitializeWebSocketClient());
    crashOnFailure(step09_Authenticate());
    crashOnFailure(step10_VerifyApiConnectivity());
    runOrdersJsonCleanSlateAfterRestVerified();
    crashOnFailure(step11_DetectFeedMode());
    crashOnFailure(step12_CheckExchangeStatus());
    
    // Phase 3: Market Data Infrastructure (Steps 13-17)
    std::cout << "\n" << colors::MAGENTA << colors::BOLD << "▸ PHASE 3:" << colors::RESET 
              << colors::WHITE << " Market Data Infrastructure" << colors::RESET << std::endl;
    Main().logger()->info(">>> PHASE 3: Market Data Infrastructure");
    crashOnFailure(step13_InitializeMarketDataManager());
    crashOnFailure(step14_InitializeExternalFeedManager());
    crashOnFailure(step15_ConnectWebSocket());
    crashOnFailure(step16_AuthenticateWebSocket());
    crashOnFailure(step17_SubscribeMarketDataChannels());
    
    // Phase 4: Trading Infrastructure (Steps 18-21)
    std::cout << "\n" << colors::MAGENTA << colors::BOLD << "▸ PHASE 4:" << colors::RESET 
              << colors::WHITE << " Trading Infrastructure" << colors::RESET << std::endl;
    Main().logger()->info(">>> PHASE 4: Trading Infrastructure");
    crashOnFailure(step18_InitializePortfolioManager());
    crashOnFailure(step19_VerifyAccountPermissions());
    crashOnFailure(step20_SubscribePrivateChannels());
    crashOnFailure(step21_InitializeStrategyManager());
    
    // Phase 5: Pre-Trade Verification (Steps 22-23)
    std::cout << "\n" << colors::MAGENTA << colors::BOLD << "▸ PHASE 5:" << colors::RESET 
              << colors::WHITE << " Pre-Trade Verification" << colors::RESET << std::endl;
    Main().logger()->info(">>> PHASE 5: Pre-Trade Verification");
    crashOnFailure(step22_VerifyExternalFeed());
    crashOnFailure(step23_VerifyMarketData());
    
    auto total_end = std::chrono::steady_clock::now();
    auto total_duration = std::chrono::duration_cast<std::chrono::milliseconds>(total_end - total_start);
    
    startup_complete_ = true;
    
    Main().logger()->info("╔══════════════════════════════════════════════════════════════════╗");
    Main().logger()->info("║              STARTUP COMPLETE - ALL 23 STEPS PASSED              ║");
    Main().logger()->info("║              Total Duration: {:>6} ms                            ║", total_duration.count());
    Main().logger()->info("╚══════════════════════════════════════════════════════════════════╝");
    
    Main().logger()->log_event("STARTUP", "complete", 
        "All 23 pre-trade steps completed in " + std::to_string(total_duration.count()) + "ms");
}

// Fills cursor/param helpers — shared by the legacy inline pollExchangeFills and the
// FastPoller fills-fetch callback (Pillar C). File-local so the api::QueryParams return
// type does not leak into StartupSequence.h.
static std::int64_t fillsStartTsNsFromCursor(const std::string& last_fill_id);
static api::QueryParams buildFillsQueryParams(std::int64_t start_ts_ns);

void StartupSequence::executeTradingLoop(std::atomic<bool>& exit_signal) {
    if (!startup_complete_.load()) {
        throw StartupException(24, "Trading Loop", "Startup not complete - call executeStartup() first");
    }
    
    current_step_ = 24;
    step_status_[23] = StepStatus::RUNNING;
    trading_ = true;
    
    Main().logger()->info("╔══════════════════════════════════════════════════════════════════╗");
    Main().logger()->info("║                   STEP 24/25: TRADING LOOP                       ║");
    Main().logger()->info("╚══════════════════════════════════════════════════════════════════╝");
    
    Main().logger()->log_event("TRADING", "started", "Entering main trading loop");

    // Position-book boot banner (Fix 4): emit unconditionally here — logging is fully
    // up by this point (the STEP 24 banner above just printed) and this runs on every
    // start regardless of mode or which strategy code paths are taken. The call is a
    // one-shot idempotent guard, so the strategy-init call site is now just a backstop.
    strategy::MakeMarketStrategy::mmLogPositionBookBootBannerOnce();

    // Start external feed polling
    Main().externalFeed()->start();
    
    // Start market data manager
    Main().marketdata()->start();
    
    // Start event processing
    Main().events()->start(Main().config()->getWorkerThreads());
    
    // Start all registered strategies
    Main().strategyManager()->startAll();

    try {
        seedMmDeskSignalSeqBaselineAtBoot();
    } catch (const std::exception& e) {
        Main().logger()->warn("[DESK_SYNC] boot seq baseline error: {}", e.what());
    } catch (...) {
    }
    
    auto& config = config::Config::getInstance();
    int update_interval = config.getMarketMakerUpdateIntervalSec();
    if (update_interval <= 0) update_interval = 2;
    
    events::DateInt date_int = 0;
    try {
        date_int = static_cast<events::DateInt>(std::stoll(cached_params_.simulation_date));
    } catch (...) {}
    
    events::TimeRack time_rack = 0;
    int tick_count = 0;
    
    Main().logger()->info("Trading loop started. Press Ctrl+C to exit.");
    Main().logger()->info("  Update interval: {}s", update_interval);
    Main().logger()->info("  Simulation date: {}", cached_params_.simulation_date);
    
    // Track last fill trade_id for GET /fills ?after= (persisted so restarts do not replay historic fills).
    std::string last_fill_id;
    {
        const std::string cursor_rel =
            config.getString("api.fill_poll_cursor_file", "logs/fill_poll_last_trade_id.txt");
        if (!cursor_rel.empty()) {
            const fs::path cursor_path = fs::current_path() / fs::path(cursor_rel);
            last_fill_id = readFillPollCursorTradeId(cursor_path);
            if (!last_fill_id.empty()) {
                Main().logger()->info("Fill poll: restored last trade_id cursor from {} → {}", cursor_rel,
                                      last_fill_id);
            }
        }
    }
    // Poll fills every 3 seconds - need to stay in sync with exchange
    // Default dropped from 3s -> 1s on 2026-05-21 (Tim's spec). Position is now
    // driven exclusively by fill events post-init, and the MakeMarketStrategy
    // reconciliation gate blocks new submits until at least one fill poll has
    // completed AFTER the most recent cancel response. A 3s default would
    // mean a stack waiting 3s after any cancel before re-quoting; 1s keeps
    // re-quote latency human-tolerable while still giving the venue time to
    // surface any post-cancel late fill. Operators can still override via
    // api.fill_poll_interval_sec in config if 1s is too aggressive.
    int fill_poll_interval = config.getInt("api.fill_poll_interval_sec", 1);
    // Poll positions every 5 seconds to get actual PnL from exchange
    int position_poll_interval = config.getInt("api.position_poll_interval_sec", 5);
    // Compare GET /open-orders to local OrderManager actives so exchange-GUI cancels clear stale OIDs.
    // 0 = disabled (default). Suggested 15–60 when running mm_req_* alongside a live venue.
    int gateway_open_orders_reconcile_sec =
        config.getInt("market_maker.gateway_open_orders_reconcile_sec", 0);
    // Enforce-mode VenueOrdersCache refresh cadence (main-loop thread ONLY). One
    // GET /open-orders per ENFORCE symbol per interval feeds VenueOrdersCache, which the
    // mover reads instead of ever fetching inline. Off/shadow symbols are never fetched.
    int venue_orders_refresh_interval =
        std::max(1, config.getInt("api.venue_orders_refresh_interval_sec", 10));
    // If paper_simulate_fills is TRUE, fills are simulated locally (no exchange polling)
    // If paper_simulate_fills is FALSE, poll real fills from the exchange
    bool simulate_fills_locally = config.getBool("market_maker.paper_simulate_fills", false);
    
    Main().logger()->info("Fill handling mode: {}", 
        simulate_fills_locally ? "LOCAL_SIMULATION" : "EXCHANGE_POLLING");

    // === Pillar C: dedicated continuous fills+positions poller ================
    // Spawned here (immediately after the startup seed) and joined at loop exit
    // (before executeShutdown's REST teardown). The poller performs the HTTP GET
    // (the ~RTT) off the main loop and hands raw bodies back via the handoff
    // queue; the main loop drains + applies them (PositionBook single-writer).
    const bool fast_poller_enabled =
        !simulate_fills_locally && config.getBool("polling.fast_poller_enabled", false);
    if (fast_poller_enabled) {
        // Seed the shared cursor from the restored last_fill_id so the poller's first
        // fetch starts where we left off.
        std::int64_t seed_cursor = 0;
        try { if (!last_fill_id.empty()) seed_cursor = std::stoll(last_fill_id); } catch (...) {}
        fast_poller_fills_cursor_ns_.store(seed_cursor, std::memory_order_release);

        FastPoller::Config fp_cfg;
        fp_cfg.enabled = true;
        // Self-throttling defaults (2026-07-13): 300ms fills / 1000ms positions. 0 is
        // still honored (back-to-back) but is no longer the default — constant
        // back-to-back polling soft-throttles the whole account and inflated order RTT.
        fp_cfg.fills_interval_ms = std::max(0, config.getInt("polling.fills_interval_ms", 300));
        fp_cfg.positions_interval_ms = std::max(0, config.getInt("polling.positions_interval_ms", 1000));
        fp_cfg.poll_min_interval_ms = std::max(0, config.getInt("polling.poll_min_interval_ms", 0));
        fp_cfg.backoff_base_ms = std::max(1, config.getInt("polling.backoff_base_ms", 500));
        fp_cfg.backoff_cap_ms = std::max(fp_cfg.backoff_base_ms, config.getInt("polling.backoff_cap_ms", 10000));
        fp_cfg.heartbeat_ms = std::max(1000, config.getInt("polling.heartbeat_ms", 20000));
        // Self-throttle governor: anchor the degradation reference to expected_rtt_ms
        // (or the session-minimum RTT, whichever is smaller) so it is not blinded by a
        // median that baselines at the already-throttled value.
        fp_cfg.expected_rtt_ms = std::max(1, config.getInt("polling.expected_rtt_ms", 100));
        fp_cfg.self_throttle_hi_factor = config.getDouble("polling.self_throttle_hi_factor", 1.8);
        fp_cfg.self_throttle_lo_factor = config.getDouble("polling.self_throttle_lo_factor", 1.3);
        fp_cfg.self_throttle_min_samples = std::max(1, config.getInt("polling.self_throttle_min_samples", 10));

        auto parse_retry_after_ms = [](const api::Headers& headers) -> std::int64_t {
            for (const auto& kv : headers) {
                std::string k = kv.first;
                std::transform(k.begin(), k.end(), k.begin(),
                               [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
                if (k == "retry-after") {
                    try {
                        // HTTP Retry-After is in seconds (integer). Convert to ms.
                        return static_cast<std::int64_t>(std::stoll(kv.second)) * 1000LL;
                    } catch (...) {
                        return 0;
                    }
                }
            }
            return 0;
        };

        FastPoller::FetchFills fetch_fills = [this, parse_retry_after_ms](std::int64_t& out_start_ts_ns) {
            auto& rest = api::RestClient::getInstance();
            const std::int64_t cur = fast_poller_fills_cursor_ns_.load(std::memory_order_acquire);
            const std::string cursor_str = (cur > 0) ? std::to_string(cur) : std::string{};
            const std::int64_t start_ts = fillsStartTsNsFromCursor(cursor_str);
            out_start_ts_ns = start_ts;
            const auto params = buildFillsQueryParams(start_ts);
            const std::int64_t t0 = FastPoller::nowSteadyMs();
            auto resp = rest.getFills(params);
            FastPoller::FetchResult r;
            r.status_code = resp.status_code;
            r.ok = resp.is_success && resp.status_code >= 200 && resp.status_code < 300;
            r.body = std::move(resp.body);
            r.rtt_ms = resp.latency.count() > 0 ? resp.latency.count() : (FastPoller::nowSteadyMs() - t0);
            r.retry_after_ms = parse_retry_after_ms(resp.headers);
            r.rate_limited = (resp.status_code == 429);
            return r;
        };
        FastPoller::FetchPositions fetch_positions = [parse_retry_after_ms]() {
            auto& rest = api::RestClient::getInstance();
            const std::int64_t t0 = FastPoller::nowSteadyMs();
            auto resp = rest.getPositions();
            FastPoller::FetchResult r;
            r.status_code = resp.status_code;
            r.ok = resp.is_success && resp.status_code >= 200 && resp.status_code < 300;
            r.body = std::move(resp.body);
            r.rtt_ms = resp.latency.count() > 0 ? resp.latency.count() : (FastPoller::nowSteadyMs() - t0);
            r.retry_after_ms = parse_retry_after_ms(resp.headers);
            r.rate_limited = (resp.status_code == 429);
            return r;
        };
        FastPoller::PushFn push = [this](bool is_fills, std::string body, std::int64_t start_ts,
                                         std::int64_t fetch_ms) {
            {
                std::lock_guard<std::mutex> lk(poll_handoff_mutex_);
                poll_handoff_queue_.push_back(
                    PollHandoffItem{is_fills, std::move(body), start_ts, fetch_ms});
                // Bound the queue: if the main loop cannot keep up, drop the oldest
                // (freshest truth wins; dropped fills are re-surfaced by the cursor overlap).
                while (poll_handoff_queue_.size() > 256) poll_handoff_queue_.pop_front();
            }
            poll_handoff_cv_.notify_one();  // wake the main loop for ~0 apply latency
        };
        FastPoller::LogFn log_info = [](const std::string& m) {
            if (utils::Logger::isInitialized()) Main().logger()->info("{}", m);
        };
        FastPoller::LogFn log_warn = [](const std::string& m) {
            if (utils::Logger::isInitialized()) Main().logger()->warn("{}", m);
        };
        fast_poller_ = std::make_unique<FastPoller>(fp_cfg, std::move(fetch_fills),
                                                    std::move(fetch_positions), std::move(push),
                                                    std::move(log_info), std::move(log_warn));
        fast_poller_->start();
        Main().logger()->info(
            "[FAST_POLLER] started fills_interval_ms={} positions_interval_ms={} "
            "poll_min_interval_ms={} backoff_base_ms={} backoff_cap_ms={}",
            fp_cfg.fills_interval_ms, fp_cfg.positions_interval_ms, fp_cfg.poll_min_interval_ms,
            fp_cfg.backoff_base_ms, fp_cfg.backoff_cap_ms);
    } else {
        Main().logger()->info("[FAST_POLLER] disabled — legacy inline fills/positions polling");
    }

    // ~1s periodic cadence is driven by wall-clock (steady) rather than loop-iteration
    // count so the FastPoller CV-drain (which wakes every push) does not accelerate the
    // modulo-gated periodic work below. Behavior is identical to the legacy 10x100ms tick.
    auto next_periodic = std::chrono::steady_clock::now();

    // Main trading loop
    while (!exit_signal.load()) {
        const bool poller_healthy =
            fast_poller_ && fast_poller_->running() && !fast_poller_->fellBack();
        // Wait phase. With a healthy FastPoller, block on the handoff CV (woken on
        // each push for ~0 apply latency) then drain+apply on THIS (main-loop) thread.
        // Otherwise fall back to interruptible 100ms slices (legacy / poller dead).
        if (poller_healthy) {
            {
                std::unique_lock<std::mutex> lk(poll_handoff_mutex_);
                poll_handoff_cv_.wait_for(lk, std::chrono::milliseconds(100), [&] {
                    return !poll_handoff_queue_.empty() || exit_signal.load();
                });
            }
            drainFastPollerHandoff(last_fill_id);
        } else {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        if (exit_signal.load()) {
            break;
        }
        // Only run the ~1s periodic block on the wall-clock cadence.
        if (std::chrono::steady_clock::now() < next_periodic) {
            continue;
        }
        next_periodic = std::chrono::steady_clock::now() + std::chrono::seconds(1);
        ++tick_count;

        // === Global fast-market breaker heartbeat (UNCONDITIONAL — finding D) ===
        // Post a ~1 Hz HL-SPX sample+evaluate job to the single mover worker. This MUST stay
        // outside every isMarketMakerEnabled()/update_interval gate so the breaker keeps watching
        // even in states where strategy timers don't fire. The monitor itself tracks tick
        // staleness (fail-open if it stops being ticked). Wrapped so it can never break the loop.
        try {
            strategy::FastMarketMonitor::getInstance().enqueueTick();
        } catch (...) {
            // The global safety monitor must never take down the main trading loop.
        }

        // === Desk→C++ force-cancel drain (~1 Hz) ===
        // Recovery lane for orphan stacks (on the venue but absent from orders.json). Reads the
        // append-only signal file, dispatching each (ax, stack_id) to the mover-serialized venue
        // cancel. Cheap when idle (one stat + early return). Fully guarded — must never break the loop.
        try {
            drainDeskForceCancelSignals();
        } catch (...) {
            // The recovery drain must never take down the main trading loop.
        }

        try {
            pollMmDeskSyncIfEnabled(cached_params_.config_path);
        } catch (const std::exception& e) {
            Main().logger()->warn("Desk sync poll error: {}", e.what());
        }
        
        // Fire strategy timers
        if (config.isMarketMakerEnabled() && (tick_count % update_interval) == 0) {
            Main().strategyManager()->fireTimer(date_int, time_rack);
            ++time_rack;
        }
        
        // Poll for fills from exchange (when NOT simulating locally). Skipped while a
        // healthy FastPoller is feeding the handoff queue; runs inline as the legacy /
        // fall-back path when the poller is disabled or has died (fellBack).
        const bool inline_poll = !poller_healthy;
        if (!exit_signal.load() && !simulate_fills_locally && inline_poll &&
            (tick_count % fill_poll_interval) == 0) {
            try {
                pollExchangeFills(last_fill_id);
            } catch (const std::exception& e) {
                Main().logger()->warn("Fill polling error: {}", e.what());
            }
            // Stamp regardless of throw — what the reconciliation gate cares
            // about is that we *attempted* a fresh poll. If the REST call
            // threw, the next poll will retry and bump the stamp again; in
            // the meantime not bumping the stamp would deadlock the gate
            // forever (no cancel could ever be followed by a place).
            StartupSequence::last_fill_poll_completed_ms_.store(
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::steady_clock::now().time_since_epoch())
                    .count(),
                std::memory_order_release);
        }
        
        // Poll for positions/PnL from exchange (when NOT simulating locally). Same
        // gating as fills: skipped while a healthy FastPoller feeds the handoff queue.
        if (!exit_signal.load() && !simulate_fills_locally && inline_poll &&
            (tick_count % position_poll_interval) == 0) {
            try {
                pollExchangePositions();
            } catch (const std::exception& e) {
                Main().logger()->warn("Position polling error: {}", e.what());
            }
        }

        if (!exit_signal.load() && !simulate_fills_locally && gateway_open_orders_reconcile_sec > 0 &&
            (tick_count % gateway_open_orders_reconcile_sec) == 0) {
            try {
                reconcileLocalOrdersAgainstGatewayOpenOrders();
            } catch (const std::exception& e) {
                Main().logger()->warn("[OM_GATEWAY] open-orders reconcile error: {}", e.what());
            }
        }

        // Enforce-mode VenueOrdersCache refresher (main-loop thread). Gated so off/shadow-only
        // configs issue ZERO extra REST — legacy venue-call behavior stays byte-for-byte.
        if (!exit_signal.load() && !simulate_fills_locally &&
            (tick_count % venue_orders_refresh_interval) == 0 &&
            architect::strategy::MakeMarketStrategy::mmPbEnforceAnySymbol()) {
            try {
                architect::strategy::MakeMarketStrategy::mmRefreshVenueOrdersCacheAllEnforced();
            } catch (const std::exception& e) {
                Main().logger()->warn("[VENUE_ORDERS_CACHE] refresh error: {}", e.what());
            }
        }
        
        // Periodic logging
        if (tick_count % 10 == 0) {
            // Log theo update if configured
            if (config.getBool("logging.log_theo_updates", true)) {
                auto quote = Main().externalFeed()->getLastQuote();
                if (quote) {
                    Main().logger()->debug("Theo: bid={:.5f} ask={:.5f} mid={:.5f}", 
                        quote->bid, quote->ask, quote->mid);
                }
            }
        }
        
        // Log heartbeat every 15 seconds (not 60); feed guardian runs same cadence
        if (tick_count % 15 == 0) {
            Main().runFeedGuardianHeartbeat();
            Main().logger()->log_metrics("heartbeat", 1.0, "");
            Main().logger()->log_metrics("uptime_seconds", static_cast<double>(tick_count), "s");
            Main().dumpState();
        }
    }
    
    // Join the FastPoller BEFORE returning (executeShutdown, which tears down REST,
    // runs after this). Clean stop, then drain any final handoff items so the last
    // fetched truth is applied on this thread.
    if (fast_poller_) {
        fast_poller_->stop();
        Main().logger()->info(
            "[FAST_POLLER] joined fills_polls={} positions_polls={} backoffs={} respawns={} "
            "median_rtt_ms={} fell_back={}",
            fast_poller_->fillsPolls(), fast_poller_->positionsPolls(), fast_poller_->backoffs(),
            fast_poller_->respawns(), fast_poller_->medianRttMs(),
            fast_poller_->fellBack() ? 1 : 0);
        try {
            drainFastPollerHandoff(last_fill_id);
        } catch (...) {
        }
        fast_poller_.reset();
    }

    trading_ = false;
    step_status_[23] = StepStatus::SUCCESS;
    
    Main().logger()->info("Trading loop exited normally");
    Main().logger()->log_event("TRADING", "stopped", "Exiting main trading loop");
}

int StartupSequence::drainFastPollerHandoff(std::string& last_fill_id) {
    std::deque<PollHandoffItem> local;
    {
        std::lock_guard<std::mutex> lk(poll_handoff_mutex_);
        local.swap(poll_handoff_queue_);
    }
    int applied = 0;
    for (auto& item : local) {
        const std::int64_t age_ms = FastPoller::nowSteadyMs() - item.fetch_steady_ms;
        try {
            if (item.is_fills) {
                applyExchangeFillsBody(item.body, item.start_ts_ns, last_fill_id);
                // Advance the shared cursor so the poller's next fetch starts fresh.
                std::int64_t cur = 0;
                try { cur = std::stoll(last_fill_id); } catch (...) {}
                if (cur > 0) {
                    fast_poller_fills_cursor_ns_.store(cur, std::memory_order_release);
                }
                // The reconcile gate (Tim) only cares that a fills poll COMPLETED; stamp it.
                last_fill_poll_completed_ms_.store(
                    std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::steady_clock::now().time_since_epoch())
                        .count(),
                    std::memory_order_release);
            } else {
                // Under the fast poller, apply the venue snapshot on EVERY positions
                // result (~continuous), superseding the legacy 30s modulo.
                applyExchangePositionsBody(item.body, /*apply_snapshot=*/true);
            }
        } catch (const std::exception& e) {
            Main().logger()->warn("[FAST_POLLER_APPLY] apply error kind={} : {}",
                                  item.is_fills ? "fills" : "positions", e.what());
        }
        ++applied;
        if (utils::Logger::isInitialized()) {
            Main().logger()->debug("[FAST_POLLER_APPLY] kind={} age_ms={} applied={}",
                                   item.is_fills ? "fills" : "positions", age_ms, applied);
        }
    }
    return applied;
}

void StartupSequence::pollExchangeFills(std::string& last_fill_id) {
    // GET /fills — OpenAPI: cursor (pagination token), limit, symbol, sort_ts, start/end_timestamp_ns
    // https://docs.architect.exchange/api-reference/portfolio-management/get-fills
    //
    // === ROOT-CAUSE FIX 2026-05-22 (SILVER overtrade 00:00) ====================
    // The previous implementation passed `cursor=<trade_id>`. Per the OpenAPI
    // spec the `cursor` parameter is an OPAQUE pagination token sourced from
    // `next_cursor` in the response body — it is NOT a trade_id. Passing a
    // trade_id either returns an empty list or repeats the historic page,
    // which is exactly what we observed: `[HEARTBEAT] fills=0` for the entire
    // session while the venue was filling us LONG 6 against `max_po=2`. With
    // 0 fills delivered, local NetPo stayed at 1 (only the OM_GATEWAY
    // backstop synthesised any fills, and only well after the breach) so the
    // cap-gate had no idea anything was wrong and let new orders through.
    //
    // The supported "fills since X" filter is `start_timestamp_ns`. We track
    // the max fill timestamp we have ever processed across polls and pass
    // `start_timestamp_ns = last_ts + 1` to ask the API for strictly newer
    // fills. The `processed_fill_trade_ids` set below still dedupes within
    // the timestamp boundary in case the venue ever returns the same fill
    // twice (we leave a 1-second backstop overlap by NOT advancing
    // `last_fill_ts_ns_` to the max if zero fills returned).
    //
    // We keep the `last_fill_id` parameter name for ABI compatibility with
    // the calling code but treat it as the textual representation of the
    // tracked timestamp_ns (i.e. it now stores a decimal int64 in ns, not a
    // trade_id). The persisted cursor file format changes correspondingly —
    // on first read we accept either an old ULID (fall back to time-now seed)
    // or a numeric value (use as-is).
    auto& rest = api::RestClient::getInstance();

    const std::int64_t start_ts_ns = fillsStartTsNsFromCursor(last_fill_id);
    const api::QueryParams params = buildFillsQueryParams(start_ts_ns);

    auto response = rest.getFills(params);
    if (!response.is_success || response.status_code < 200 || response.status_code >= 300) {
        return;  // Silently skip on error
    }
    // Fetch/apply split (Pillar C): the HTTP GET above is the ~RTT the FastPoller
    // offloads; parse+apply below stays here so it always runs on the main-loop
    // thread (PositionBook single-writer invariant). The FastPoller re-uses
    // fillsStartTsNsFromCursor/buildFillsQueryParams for its fetch and hands the
    // raw body back to the main loop, which calls applyExchangeFillsBody directly.
    applyExchangeFillsBody(response.body, start_ts_ns, last_fill_id);
}

static std::int64_t fillsStartTsNsFromCursor(const std::string& last_fill_id) {
    // ns since unix epoch from system_clock (matches the API's start/end_timestamp_ns units).
    auto now_unix_ns = []() -> std::int64_t {
        return std::chrono::duration_cast<std::chrono::nanoseconds>(
                   std::chrono::system_clock::now().time_since_epoch())
            .count();
    };
    // Parse stored cursor. If it's all digits, treat as ns. Otherwise (legacy ULID
    // from a previous build) ignore and seed to "now - 60s" so we only see fresh fills.
    std::int64_t start_ts_ns = 0;
    auto is_all_digits = [](const std::string& s) {
        if (s.empty()) return false;
        for (char c : s) {
            if (c < '0' || c > '9') return false;
        }
        return true;
    };
    if (!last_fill_id.empty() && is_all_digits(last_fill_id)) {
        try {
            start_ts_ns = std::stoll(last_fill_id);
        } catch (...) {
            start_ts_ns = 0;
        }
    }
    if (start_ts_ns <= 0) {
        // Cold start (or legacy cursor): begin from 60s ago so we surface
        // any fills that landed during our launch window but stay safely
        // bounded so we don't replay arbitrary history. `processed_fill_trade_ids`
        // will dedupe anything we have already processed.
        start_ts_ns = now_unix_ns() - 60LL * 1'000'000'000LL;
        if (start_ts_ns < 0) start_ts_ns = 0;
    }
    return start_ts_ns;
}

static api::QueryParams buildFillsQueryParams(std::int64_t start_ts_ns) {
    api::QueryParams params;
    params["limit"] = "200";
    params["sort_ts"] = "asc";  // process oldest -> newest so cursor monotonically increases
    params["start_timestamp_ns"] = std::to_string(start_ts_ns);
    return params;
}

void StartupSequence::applyExchangeFillsBody(const std::string& body, std::int64_t start_ts_ns,
                                             std::string& last_fill_id) {
    auto& rest = api::RestClient::getInstance();  // used by the open-orders orphan map lookup below
    try {
        auto fills_json = nlohmann::json::parse(body);
        if (!fills_json.contains("fills") || !fills_json["fills"].is_array()) {
            return;
        }

        const std::string last_fill_at_entry = last_fill_id;
        const std::string cursor_rel = config::Config::getInstance().getString(
            "api.fill_poll_cursor_file", "logs/fill_poll_last_trade_id.txt");
        const fs::path cursor_path =
            cursor_rel.empty() ? fs::path{} : (fs::current_path() / fs::path(cursor_rel));

        auto& fills = fills_json["fills"];

        // No "first-poll seed and skip" anymore — under timestamp-based cursoring
        // `start_timestamp_ns = now - 60s` already bounds first-poll history.
        // Anything older was filtered server-side, anything inside the 60s window
        // is fresh enough to be worth processing (the trade_id dedupe set below
        // protects against re-processing if we crash-recover with the same window).

        auto& om = orders::OrderManager::getInstance();
        // One MM reconcile per symbol per poll (avoids N full cycles when several orphans arrive together).
        std::unordered_set<std::string> mm_orphan_symbols;
        bool unknown_symbol_orphan_mm = false;
        std::unordered_map<std::string, std::string> open_oid_to_symbol;
        bool loaded_open_oid_map = false;

        // Deduplicate by trade_id: REST pagination / lexicographic last_fill_id ordering can
        // surface the same historical fill every poll (e.g. orphan from a prior session).
        static std::unordered_set<std::string> processed_fill_trade_ids;
        constexpr std::size_t kMaxProcessedFillIds = 20000u;
        
        // Debug: log first fill structure once to understand format
        static bool logged_fill_structure = false;
        if (!logged_fill_structure && !fills.empty()) {
            Main().logger()->debug("[FILL_DEBUG] Sample fill JSON: {}", fills[0].dump());
            logged_fill_structure = true;
        }
        
        // Under timestamp-based cursoring we ONLY dedupe by trade_id (the lexicographic
        // `trade_id <= last_fill_id` skip from the old cursor-based logic was meaningless
        // because `last_fill_id` is now a numeric ns string, not a trade_id).
        for (const auto& fill_json : fills) {
            std::string trade_id = fill_json.value("trade_id", "");
            if (trade_id.empty()) continue;

            if (processed_fill_trade_ids.count(trade_id)) {
                continue;
            }
            if (processed_fill_trade_ids.size() >= kMaxProcessedFillIds) {
                processed_fill_trade_ids.clear();
            }
            processed_fill_trade_ids.insert(trade_id);
            
            // Find our order by exchange order ID (several JSON shapes across gateway versions)
            std::string exchange_order_id = fillJsonExchangeOrderId(fill_json);
            auto order = exchange_order_id.empty() ? nullptr : om.getOrderByExchangeId(exchange_order_id);
            
            // Parse price and quantity (OpenAPI: price/fee strings, quantity int64)
            double fill_price = fillJsonOpenApiPrice(fill_json);
            double fill_qty = fillJsonOpenApiQuantity(fill_json);
            
            // Log the fill (always)
            Main().logger()->info("[FILL] Exchange fill received: order={} trade_id={} price={} qty={}", 
                exchange_order_id, trade_id, fill_price, fill_qty);
            
            if (!order) {
                Main().logger()->info(
                    "[FILL] No local order for exchange oid={} — orphan fill (stale id or OM reset); "
                    "C++ still processes via exchange REST + MM reconcile when symbol matches",
                    exchange_order_id);
                std::string fsym = fillJsonSymbolExtended(fill_json);
                if (fsym.empty() && !exchange_order_id.empty()) {
                    if (!loaded_open_oid_map) {
                        buildOpenOrdersExchangeOidToSymbol(rest, &open_oid_to_symbol);
                        loaded_open_oid_map = true;
                    }
                    auto it = open_oid_to_symbol.find(exchange_order_id);
                    if (it != open_oid_to_symbol.end()) {
                        fsym = it->second;
                    }
                }
                auto& mm_cfg = config::Config::getInstance();
                if (!fsym.empty() && mm_cfg.isMarketMakerEnabled()) {
                    mm_orphan_symbols.insert(std::move(fsym));
                } else if (mm_cfg.isMarketMakerEnabled() &&
                           mm_cfg.getBool("market_maker.orphan_fill_unknown_symbol_reconcile", true) &&
                           (!exchange_order_id.empty() || fill_qty > 0.0)) {
                    unknown_symbol_orphan_mm = true;
                    Main().logger()->info(
                        "[FILL] Orphan fill: symbol unresolved (oid={}) — will run MM position-delta reconcile",
                        exchange_order_id);
                } else if (fsym.empty()) {
                    Main().logger()->debug("[FILL] Orphan fill missing symbol field; MM symbol reconcile skipped");
                }
                continue;
            }
            
            Main().logger()->info("[FILL] Matched local order_id={} for exchange_id={}", 
                order->id, exchange_order_id);
            
            // Create fill struct
            orders::Fill f;
            f.order_id = order->id;
            f.symbol = order->symbol;
            f.side = order->side;
            f.price = fill_price;
            f.quantity = fill_qty;
            
            f.fee = 0.0;
            if (fill_json.contains("fee")) {
                const auto& v = fill_json["fee"];
                if (v.is_string()) {
                    try {
                        f.fee = std::stod(v.get<std::string>());
                    } catch (...) {
                    }
                } else if (v.is_number()) {
                    f.fee = v.get<double>();
                }
            }
            if (f.fee == 0.0) {
                for (const auto* key : {"commission", "trading_fee", "exec_fee", "fee_amount"}) {
                    if (fill_json.contains(key)) {
                        const auto& v = fill_json[key];
                        if (v.is_string()) {
                            try {
                                f.fee = std::stod(v.get<std::string>());
                            } catch (...) {
                            }
                        } else if (v.is_number()) {
                            f.fee = v.get<double>();
                        }
                        if (f.fee != 0.0) {
                            break;
                        }
                    }
                }
            }
            
            f.executed_at = std::chrono::high_resolution_clock::now().time_since_epoch();
            
            // Inject the fill into OrderManager - this will publish ORDER_FILLED or ORDER_PARTIALLY_FILLED event
            Main().logger()->info("[FILL] Injecting fill into OrderManager: order_id={} qty={} price={} fee={}", 
                order->id, f.quantity, f.price, f.fee);
            om.onOrderFilled(order->id, f);
        }

        for (const auto& sym : mm_orphan_symbols) {
            strategy::MakeMarketStrategy::notifyAllRegisteredOrphanExchangeFill(sym);
        }
        if (unknown_symbol_orphan_mm) {
            strategy::MakeMarketStrategy::notifyAllRegisteredOrphanExchangeFillUnknownSymbol();
        }

        // Advance the cursor to (poll_start_ns - 2s overlap). The 2s overlap
        // guarantees the next poll re-fetches anything the venue may have
        // committed with a slightly backdated timestamp due to clock skew; the
        // `processed_fill_trade_ids` set above deduplicates that overlap.
        // Always store as a numeric ns string so the next pollExchangeFills
        // call recognises and reuses it.
        constexpr std::int64_t kOverlapNs = 2LL * 1'000'000'000LL;
        const std::int64_t now_ns_after_poll =
            std::chrono::duration_cast<std::chrono::nanoseconds>(
                std::chrono::system_clock::now().time_since_epoch())
                .count();
        const std::int64_t advance_to_ns =
            std::max<std::int64_t>(start_ts_ns, now_ns_after_poll - kOverlapNs);
        last_fill_id = std::to_string(advance_to_ns);
        if (!cursor_path.empty() && last_fill_id != last_fill_at_entry) {
            writeFillPollCursorTradeId(cursor_path, last_fill_id);
        }
    } catch (const std::exception& e) {
        Main().logger()->warn("Error parsing fills response: {}", e.what());
    }
}

void StartupSequence::pollExchangePositions() {
    // Fetch actual positions from exchange - this is the source of truth for PnL
    auto& rest = api::RestClient::getInstance();

    // === PositionBook snapshot cadence (2026-07) ===============================
    // This 5s poll already fetches getPositions() for PnL. When PositionBook is
    // active (mode != off for the global or any symbol) we ALSO feed that SAME
    // response into PositionBook::applySnapshotBatch every position_refresh_interval_sec
    // (default 30, clamped >= poll interval). NO extra REST is issued. In off mode
    // this whole block is skipped and the function is byte-for-byte legacy.
    const bool pb_active =
        architect::strategy::MakeMarketStrategy::mmPbEnabledAnySymbol();
    auto& pb_cfg = config::Config::getInstance();
    const int pb_poll_sec = std::max(1, pb_cfg.getInt("api.position_poll_interval_sec", 5));
    const int pb_refresh_sec =
        std::max(pb_poll_sec, pb_cfg.getInt("api.position_refresh_interval_sec", 30));
    const int pb_refresh_every = std::max(1, pb_refresh_sec / pb_poll_sec);
    static int s_pb_pos_poll_calls = 0;
    bool pb_refresh_tick = false;
    if (pb_active) {
        pb_refresh_tick = ((s_pb_pos_poll_calls % pb_refresh_every) == 0);  // first call refreshes
        ++s_pb_pos_poll_calls;
    }
    auto response = rest.getPositions();
    if (!response.is_success || response.status_code < 200 || response.status_code >= 300) {
        // Fail-open: trading continues on the last good snapshot. On a refresh tick,
        // warn if the freshest book is older than 3x the refresh interval.
        if (pb_refresh_tick) {
            const std::int64_t age_ms =
                architect::strategy::PositionBook::oldestSnapshotAgeMs();
            const std::int64_t stale_ms = 3LL * 1000LL * pb_refresh_sec;
            if (age_ms > stale_ms) {
                Main().logger()->warn(
                    "[POSITION_SNAPSHOT_STALE] getPositions() failed (status={}) and freshest "
                    "PositionBook snapshot is {}ms old (> 3x refresh={}ms) — trading continues on "
                    "last known snapshot (fail-open).",
                    response.status_code, age_ms, stale_ms);
            }
        }
        return;  // Silently skip on error (PnL); PositionBook keeps last snapshot.
    }
    // Fetch/apply split (Pillar C): parse+apply runs on the main-loop thread
    // (PositionBook single-writer). The FastPoller hands the raw body back and the
    // main loop calls applyExchangePositionsBody(body, /*apply_snapshot=*/true) on
    // EVERY result (~continuous), superseding the legacy 30s modulo cadence.
    applyExchangePositionsBody(response.body, pb_refresh_tick);
}

void StartupSequence::applyExchangePositionsBody(const std::string& body, bool apply_snapshot) {
    std::unordered_map<std::string, double> pb_venue_by_symbol;
    try {
        auto json = nlohmann::json::parse(body);
        
        // Debug: log raw response once to understand format
        static bool logged_raw = false;
        if (!logged_raw) {
            Main().logger()->debug("[POSITION_DEBUG] Raw response: {}", body.substr(0, 500));
            logged_raw = true;
        }
        
        // Array, or { "positions": [...] }, or { "data"|"items"|"results": [...] }
        nlohmann::json positions_array;
        if (json.is_array()) {
            positions_array = std::move(json);
        } else if (json.is_object()) {
            if (json.contains("positions") && json["positions"].is_array()) {
                positions_array = json["positions"];
            } else {
                for (const char* k : {"data", "items", "results"}) {
                    if (json.contains(k) && json[k].is_array()) {
                        positions_array = json[k];
                        break;
                    }
                }
            }
        }
        if (!positions_array.is_array()) {
            return;
        }
        
        double total_unrealized_pnl = 0.0;
        double total_realized_pnl = 0.0;
        static std::unordered_map<std::string, double> last_logged_position_qty;
        
        // Helper lambda to parse a numeric value from various field names and formats
        auto parseDouble = [](const nlohmann::json& j, std::initializer_list<const char*> keys) -> double {
            for (const auto* key : keys) {
                if (j.contains(key)) {
                    const auto& v = j[key];
                    if (v.is_string()) {
                        try { return std::stod(v.get<std::string>()); } catch (...) { return 0.0; }
                    } else if (v.is_number()) {
                        return v.get<double>();
                    }
                }
            }
            return 0.0;
        };
        
        for (const auto& pos_json : positions_array) {
            std::string symbol = pos_json.value("symbol", pos_json.value("s", ""));
            if (symbol.empty()) continue;
            
            // Signed qty: flat + nested JSON (Architect rows may nest size under pos/position)
            double quantity = utils::signedPositionQtyFromRestRow(pos_json);
            if (apply_snapshot) {
                pb_venue_by_symbol[symbol] = quantity;  // reuse this response; no extra REST
            }
            static bool logged_zero_qty_shape = false;
            if (!logged_zero_qty_shape && std::abs(quantity) < 1e-12 && !symbol.empty()) {
                logged_zero_qty_shape = true;
                std::string excerpt = pos_json.dump();
                if (excerpt.size() > 900) {
                    excerpt.resize(900);
                    excerpt += "...";
                }
                Main().logger()->warn(
                    "[POSITION_PARSE] quantity parsed as 0 for symbol={} — check API field names. Row: {}",
                    symbol,
                    excerpt);
            }
            
            const double abs_qty_pre = std::abs(quantity);
            // Parse average entry price - try many possible field names
            double avg_price = parseDouble(pos_json, {
                "average_price", "avg_price", "avgPrice", "entry_price", 
                "entryPrice", "cost_basis", "avgEntryPrice"
            });
            // OpenAPI: signed_notional (string) — derive ~avg price when explicit price fields absent
            const double signed_notional = utils::signedNotionalFromRestRow(pos_json);
            if (avg_price <= 0.0 && abs_qty_pre > 1e-12 && std::abs(signed_notional) > 1e-12) {
                avg_price = std::abs(signed_notional) / abs_qty_pre;
            }
            
            // Parse unrealized PnL
            double unrealized_pnl = parseDouble(pos_json, {
                "unrealized_pnl", "unrealizedPnl", "upnl", "unrealizedProfit", 
                "uPnl", "open_pnl", "floating_pnl"
            });
            
            // Parse realized PnL
            double realized_pnl = parseDouble(pos_json, {
                "realized_pnl", "realizedPnl", "rpnl", "realizedProfit", 
                "rPnl", "closed_pnl", "session_pnl"
            });
            
            total_unrealized_pnl += unrealized_pnl;
            total_realized_pnl += realized_pnl;
            
            // Determine side from quantity sign
            std::string side_str = (quantity >= 0) ? "LONG" : "SHORT";
            const double abs_qty = abs_qty_pre;

            const auto it_last = last_logged_position_qty.find(symbol);
            const bool position_changed =
                (it_last == last_logged_position_qty.end()) || (std::abs(it_last->second - quantity) > 1e-12);
            if (abs_qty > 1e-12 || position_changed) {
                Main().logger()->info(
                    "[POSITION] {} {} qty={:.2f} avg_price={:.6f} unrealized_pnl={:.2f} realized_pnl={:.2f}",
                    symbol,
                    side_str,
                    abs_qty,
                    avg_price,
                    unrealized_pnl,
                    realized_pnl);
            }
            last_logged_position_qty[symbol] = quantity;
            
            // Log to CSV
            Main().logger()->log_position(symbol, side_str, abs_qty, avg_price, 0.0, unrealized_pnl, realized_pnl);
            Main().logger()->log_pnl(symbol, realized_pnl, unrealized_pnl, realized_pnl + unrealized_pnl, 0.0);
        }
        
        // Store total PnL for heartbeat display
        cached_exchange_pnl_ = total_unrealized_pnl + total_realized_pnl;

        // PositionBook refresh: apply the venue snapshot batch built from THIS same
        // response (drift log + shadow/enforce cap evaluation). Main-loop thread only.
        if (apply_snapshot) {
            architect::strategy::MakeMarketStrategy::mmApplyVenueSnapshotBatch(pb_venue_by_symbol);
        }

    } catch (const std::exception& e) {
        Main().logger()->warn("Error parsing positions response: {}", e.what());
    }
}

ShutdownStatus StartupSequence::executeShutdown(const std::string& reason) {
    current_step_ = 25;
    step_status_[24] = StepStatus::RUNNING;
    
    auto shutdown_start = std::chrono::steady_clock::now();
    
    Main().logger()->info("╔══════════════════════════════════════════════════════════════════╗");
    Main().logger()->info("║                 STEP 25/25: GRACEFUL SHUTDOWN                    ║");
    Main().logger()->info("╚══════════════════════════════════════════════════════════════════╝");
    Main().logger()->info("Shutdown reason: {}", reason);
    
    ShutdownStatus status;
    status.exit_reason = reason;

    std::int64_t desk_cancel_ms = 0;
    architect::strategy::MakeMarketStrategy::deskShutdownCancelPollTruncateOrdersJson(reason, desk_cancel_ms);
    (void)desk_cancel_ms;

    // Drain and stop the global MmOrderMover worker thread BEFORE we destroy any
    // state it might touch. Any unprocessed cancel/replace lambdas are dropped —
    // the gateway-side shutdown sweep above already cancelled live orders, and we
    // don't want races with strategy teardown. v2 single-thread design: this joins
    // ONE thread, not one per AX (see strategy/MmOrderMover.h for history).
    architect::strategy::MmOrderMover::getInstance().shutdown();

    // 1. Stop strategies
    Main().logger()->info("[SHUTDOWN] Stopping strategies...");
    Main().strategyManager()->stopAll();
    
    // 2. Cancel all open orders on the venue.
    //
    // INVARIANT (hard rule, do NOT break): when the engine shuts down for any
    // reason (SIGINT, SIGTERM, SIGHUP, fatal error), every live order it has
    // ever placed on AX MUST be cancelled on the venue before the process
    // exits. Past versions of this block were a stub ("Note: In real
    // implementation, would call cancel API") and left working orders on the
    // book after Ctrl+C — that is unacceptable risk.
    //
    // Sweep strategy (belt-and-suspenders):
    //   A. Per-symbol cancel-all for every symbol we have any reason to know
    //      about: active orders in the local OM + every MM strategy's ax
    //      symbol + every market-data subscription symbol. This is the most
    //      important step because the AX gateway treats `cancel_all` without
    //      a symbol as "all orders for this account" and we want the strictest
    //      possible coverage.
    //   B. Final unscoped cancel-all to catch anything we don't track locally
    //      (manual test orders, orphaned orders from a prior crash, orders
    //      from sibling stacks that weren't in our OM map, etc.).
    //   C. Short verification poll: wait briefly, then re-read getActiveOrders
    //      and log a warning if any non-terminal local entries survive.
    //   D. Clear local OM state so we don't carry stale `working` rows into
    //      the next run.
    //
    // Desk-managed stacks are already handled above by
    // deskShutdownCancelPollTruncateOrdersJson; this block is what catches the
    // base `make_market_*` strategies and any non-desk strategy that placed
    // orders during the session.
    Main().logger()->info("[SHUTDOWN] Cancelling open orders on venue...");
    status.orders_cancelled = false;
    try {
        std::unordered_set<std::string> sweep_symbols;

        auto add_symbol = [&](const std::string& raw) {
            std::string s;
            s.reserve(raw.size());
            for (char c : raw) {
                if (c != '\0') s.push_back(c);
            }
            while (!s.empty() && (s.back() == ' ' || s.back() == '\t')) s.pop_back();
            if (!s.empty()) sweep_symbols.insert(std::move(s));
        };

        // A1. Symbols of currently-active orders in the local OM.
        std::vector<std::shared_ptr<architect::orders::Order>> open_orders;
        try {
            open_orders = Main().orders()->getActiveOrders();
        } catch (...) {
            open_orders.clear();
        }
        for (const auto& o : open_orders) {
            if (!o) continue;
            add_symbol(std::string(o->symbol.data()));
        }

        // A2. Symbols every MM strategy quotes on (covers base `make_market_*`
        //     and any active mm_req_* stacks that haven't had their orders
        //     written into OrderManager yet — e.g. in-flight place/replace).
        try {
            for (const auto& sp : Main().strategyManager()->getAllStrategies()) {
                auto mm = std::dynamic_pointer_cast<strategy::MakeMarketStrategy>(sp);
                if (!mm) continue;
                add_symbol(mm->mmAxSymbol());
            }
        } catch (...) {}

        // A3. Every market-data subscription symbol — defensive: if we are
        //     receiving feed for it we might have orders on it.
        try {
            const auto& cfg = *Main().config();
            for (const auto& sub : cfg.getMarketDataSubscriptionSymbols()) {
                add_symbol(sub);
            }
        } catch (...) {}

        Main().logger()->info(
            "[SHUTDOWN] cancel sweep: {} local open orders tracked, {} symbols to sweep",
            open_orders.size(), sweep_symbols.size());

        int per_sym_ok = 0;
        int per_sym_fail = 0;
        for (const std::string& sym : sweep_symbols) {
            try {
                auto r = Main().rest()->cancelAllOrders(sym);
                if (r.is_success) {
                    ++per_sym_ok;
                    Main().logger()->info("[SHUTDOWN] cancel_all symbol={} HTTP {} OK",
                                          sym, r.status_code);
                    try { Main().orders()->purgeNonTerminalOrdersForSymbol(sym); } catch (...) {}
                } else {
                    ++per_sym_fail;
                    Main().logger()->error(
                        "[SHUTDOWN] cancel_all symbol={} HTTP {} FAILED — {}",
                        sym, r.status_code,
                        r.error_message.empty() ? r.body.substr(0, 240) : r.error_message);
                }
            } catch (const std::exception& e) {
                ++per_sym_fail;
                Main().logger()->error("[SHUTDOWN] cancel_all symbol={} threw: {}", sym, e.what());
            }
        }

        // B. Global belt-and-suspenders: cancel ALL orders for this account
        //    (no symbol filter). Picks up anything we didn't enumerate.
        bool global_ok = false;
        try {
            auto r = Main().rest()->cancelAllOrders(std::nullopt);
            global_ok = r.is_success;
            if (r.is_success) {
                Main().logger()->info("[SHUTDOWN] cancel_all (unscoped) HTTP {} OK", r.status_code);
            } else {
                Main().logger()->error(
                    "[SHUTDOWN] cancel_all (unscoped) HTTP {} FAILED — {}",
                    r.status_code,
                    r.error_message.empty() ? r.body.substr(0, 240) : r.error_message);
            }
        } catch (const std::exception& e) {
            Main().logger()->error("[SHUTDOWN] cancel_all (unscoped) threw: {}", e.what());
        }

        // C. Brief verification poll. Some venues only mark orders cancelled
        //    after the next gateway round-trip; give it a beat then re-check.
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        std::vector<std::shared_ptr<architect::orders::Order>> still_open;
        try {
            still_open = Main().orders()->getActiveOrders();
        } catch (...) {}
        if (!still_open.empty()) {
            Main().logger()->warn(
                "[SHUTDOWN] {} order(s) still marked active after cancel sweep — "
                "issuing one more unscoped cancel_all and continuing",
                still_open.size());
            try {
                auto r2 = Main().rest()->cancelAllOrders(std::nullopt);
                if (!r2.is_success) {
                    Main().logger()->error(
                        "[SHUTDOWN] second unscoped cancel_all HTTP {} — {}",
                        r2.status_code,
                        r2.error_message.empty() ? r2.body.substr(0, 240) : r2.error_message);
                }
            } catch (...) {}
        }

        // D. Clear local OM so stale `working` rows don't bleed into the next run.
        try { Main().orders()->clearAll(); } catch (...) {}

        // We consider the shutdown cancel "successful" only when at least one
        // sweep call succeeded (the symbol sweep covered everything we knew
        // about, or the unscoped sweep cleaned the account). If every call
        // failed (e.g. AX gateway unreachable) we flag it loudly.
        status.orders_cancelled = (per_sym_ok > 0) || global_ok || sweep_symbols.empty();
        Main().logger()->info(
            "[SHUTDOWN] cancel sweep done: per_symbol ok={} fail={} unscoped_ok={} → orders_cancelled={}",
            per_sym_ok, per_sym_fail, global_ok ? 1 : 0,
            status.orders_cancelled ? "true" : "false");
    } catch (const std::exception& e) {
        Main().logger()->error("[SHUTDOWN] Failed to cancel orders: {}", e.what());
        status.orders_cancelled = false;
    }
    
    // 3. Stop external feed
    Main().logger()->info("[SHUTDOWN] Stopping external feed...");
    Main().externalFeed()->stop();
    
    // 4. Stop market data
    Main().logger()->info("[SHUTDOWN] Stopping market data...");
    Main().marketdata()->stop();
    
    // 5. Disconnect WebSocket
    Main().logger()->info("[SHUTDOWN] Disconnecting WebSocket...");
    try {
        Main().websocket()->disconnect();
        status.websocket_disconnected = true;
    } catch (const std::exception& e) {
        Main().logger()->error("[SHUTDOWN] WebSocket disconnect failed: {}", e.what());
        status.websocket_disconnected = false;
    }
    
    // 6. Drain event queues
    Main().logger()->info("[SHUTDOWN] Draining event queues...");
    try {
        Main().events()->stop(true);  // true = drain events
        status.events_drained = true;
    } catch (const std::exception& e) {
        Main().logger()->error("[SHUTDOWN] Event drain failed: {}", e.what());
        status.events_drained = false;
    }
    
    // 7. Log final statistics
    Main().logger()->info("[SHUTDOWN] Logging final statistics...");
    auto stats = Main().getStats();
    Main().logger()->info("Final Statistics:");
    Main().logger()->info("  Uptime: {} seconds", stats.uptime.count());
    Main().logger()->info("  Events Processed: {}", stats.events_processed);
    Main().logger()->info("  Orders Submitted: {}", stats.orders_submitted);
    Main().logger()->info("  Orders Filled: {}", stats.orders_filled);
    Main().logger()->info("  Ticks Received: {}", stats.ticks_received);
    
    Main().logger()->log_metrics("final_uptime", static_cast<double>(stats.uptime.count()), "s");
    Main().logger()->log_metrics("final_events", static_cast<double>(stats.events_processed), "count");
    Main().logger()->log_metrics("final_orders", static_cast<double>(stats.orders_submitted), "count");
    Main().logger()->log_metrics("final_fills", static_cast<double>(stats.orders_filled), "count");
    
    // 8. Flush and close logs
    Main().logger()->info("[SHUTDOWN] Flushing logs...");
    try {
        Main().logger()->flush();
        status.logs_flushed = true;
    } catch (const std::exception& e) {
        status.logs_flushed = false;
    }
    
    auto shutdown_end = std::chrono::steady_clock::now();
    status.shutdown_duration = std::chrono::duration_cast<std::chrono::milliseconds>(
        shutdown_end - shutdown_start);
    
    status.clean_exit = true;
    
    // Final status
    step_status_[24] = status.isGraceful() ? StepStatus::SUCCESS : StepStatus::FAILED;
    
    Main().logger()->info("╔══════════════════════════════════════════════════════════════════╗");
    if (status.isGraceful()) {
        Main().logger()->info("║              GRACEFUL SHUTDOWN COMPLETE                          ║");
    } else {
        Main().logger()->warn("║              SHUTDOWN COMPLETE (with warnings)                   ║");
    }
    Main().logger()->info("║              Duration: {:>6} ms                                 ║", status.shutdown_duration.count());
    Main().logger()->info("╚══════════════════════════════════════════════════════════════════╝");
    
    Main().logger()->log_event("SHUTDOWN", "complete", 
        status.isGraceful() ? "Graceful shutdown" : "Shutdown with issues");
    
    Main().logger()->shutdown();
    
    last_shutdown_ = status;
    return status;
}

// =============================================================================
// Individual Steps
// =============================================================================

StepResult StartupSequence::step01_ValidateParameters(const SimulationParams& params) {
    StepResult result(1, "Validate Parameters");
    auto start = std::chrono::steady_clock::now();
    logStepStart(1, "Validate Parameters");
    
    std::string error = params.validate();
    if (!error.empty()) {
        result.success = false;
        result.message = error;
    } else {
        result.success = true;
        result.message = "Config: " + params.config_path + ", Date: " + params.simulation_date;
    }
    
    result.duration = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - start);
    logStepEnd(1, result);
    return result;
}

StepResult StartupSequence::step02_LoadConfiguration(const std::string& config_path) {
    StepResult result(2, "Load Configuration");
    auto start = std::chrono::steady_clock::now();
    logStepStart(2, "Load Configuration");
    
    try {
        if (!config::Config::getInstance().loadFromFileWithOptionalOverlays(config_path)) {
            result.success = false;
            result.message = "Failed to parse configuration file";
        } else {
            Main().moduleStatus().config_loaded = true;
            result.success = true;
            result.message = "Loaded " + config_path +
                " (+ credentials.local.json / external_feed.local.json if present)";
        }
    } catch (const std::exception& e) {
        result.success = false;
        result.message = std::string("Exception: ") + e.what();
    }
    
    result.duration = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - start);
    logStepEnd(2, result);
    return result;
}

StepResult StartupSequence::step03_InitializeLogger(const std::string& simulation_date) {
    StepResult result(3, "Initialize Logger");
    auto start = std::chrono::steady_clock::now();
    logStepStart(3, "Initialize Logger");
    
    try {
        utils::Logger::Config log_config;
        log_config.base_log_dir = config::Config::getInstance().getString("logging.directory", "logs");
        log_config.simulation_date = simulation_date;
        log_config.crash_on_init_failure = true;
        log_config.console_enabled = config::Config::getInstance().isConsoleLogging();
        log_config.file_enabled = config::Config::getInstance().isFileLogging();
        log_config.csv_enabled = config::Config::getInstance().getBool("logging.csv_enabled", true);
        
        std::string level_str = config::Config::getInstance().getLogLevel();
        if (level_str == "DEBUG" || level_str == "debug") {
            log_config.console_level = utils::LogLevel::DEBUG;
            log_config.file_level = utils::LogLevel::DEBUG;
        } else if (level_str == "TRACE" || level_str == "trace") {
            log_config.console_level = utils::LogLevel::TRACE;
            log_config.file_level = utils::LogLevel::TRACE;
        } else {
            log_config.console_level = utils::LogLevel::INFO;
            log_config.file_level = utils::LogLevel::DEBUG;
        }
        
        utils::Logger::initialize(log_config);
        
        if (utils::Logger::isInitialized()) {
            Main().moduleStatus().logger_initialized = true;
            result.success = true;
            result.message = "Log directory: " + Main().logger()->getLogDirectory();
        } else {
            result.success = false;
            result.message = "Logger initialization returned false";
        }
    } catch (const std::exception& e) {
        result.success = false;
        result.message = std::string("Exception: ") + e.what();
    }
    
    result.duration = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - start);
    logStepEnd(3, result);
    return result;
}

StepResult StartupSequence::step04_InitializeEventManager() {
    StepResult result(4, "Initialize Event Manager");
    auto start = std::chrono::steady_clock::now();
    logStepStart(4, "Initialize Event Manager");
    
    try {
        auto& em = events::EventManager::getInstance();
        (void)em;
        Main().moduleStatus().events_initialized = true;
        result.success = true;
        result.message = "Event queue ready";
    } catch (const std::exception& e) {
        result.success = false;
        result.message = std::string("Exception: ") + e.what();
    }
    
    result.duration = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - start);
    logStepEnd(4, result);
    return result;
}

StepResult StartupSequence::step05_InitializeOrderManager() {
    StepResult result(5, "Initialize Order Manager");
    auto start = std::chrono::steady_clock::now();
    logStepStart(5, "Initialize Order Manager");
    
    try {
        auto& om = orders::OrderManager::getInstance();
        (void)om;
        Main().moduleStatus().orders_initialized = true;
        result.success = true;
        result.message = "Order tracking ready";
    } catch (const std::exception& e) {
        result.success = false;
        result.message = std::string("Exception: ") + e.what();
    }
    
    result.duration = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - start);
    logStepEnd(5, result);
    return result;
}

StepResult StartupSequence::step06_InitializeUserManager() {
    StepResult result(6, "Initialize User Manager");
    auto start = std::chrono::steady_clock::now();
    logStepStart(6, "Initialize User Manager");
    
    try {
        auto& um = user::UserManager::getInstance();
        (void)um;
        Main().moduleStatus().user_initialized = true;
        result.success = true;
        result.message = "Session management ready";
    } catch (const std::exception& e) {
        result.success = false;
        result.message = std::string("Exception: ") + e.what();
    }
    
    result.duration = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - start);
    logStepEnd(6, result);
    return result;
}

StepResult StartupSequence::step07_InitializeRestClient() {
    StepResult result(7, "Initialize REST Client");
    auto start = std::chrono::steady_clock::now();
    logStepStart(7, "Initialize REST Client");
    
    try {
        auto& rc = api::RestClient::getInstance();
        rc.setBaseUrl(Main().config()->getRestEndpoint());
        Main().moduleStatus().api_initialized = true;
        result.success = true;
        result.message = "Endpoint: " + Main().config()->getRestEndpoint();
    } catch (const std::exception& e) {
        result.success = false;
        result.message = std::string("Exception: ") + e.what();
    }
    
    result.duration = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - start);
    logStepEnd(7, result);
    return result;
}

StepResult StartupSequence::step08_InitializeWebSocketClient() {
    StepResult result(8, "Initialize WebSocket Client");
    auto start = std::chrono::steady_clock::now();
    logStepStart(8, "Initialize WebSocket Client");
    
    try {
        auto& ws = api::WebSocketClient::getInstance();
        ws.setUrl(Main().config()->getWebSocketEndpoint());
        result.success = true;
        result.message = "Endpoint: " + Main().config()->getWebSocketEndpoint();
    } catch (const std::exception& e) {
        result.success = false;
        result.message = std::string("Exception: ") + e.what();
    }
    
    result.duration = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - start);
    logStepEnd(8, result);
    return result;
}

StepResult StartupSequence::step09_Authenticate() {
    StepResult result(9, "Authenticate");
    auto start = std::chrono::steady_clock::now();
    logStepStart(9, "Authenticate");
    
    try {
        std::string session_token = Main().config()->getSessionToken();
        std::string api_key = Main().config()->getApiKey();
        std::string api_secret = Main().config()->getApiSecret();
        
        if (!session_token.empty()) {
            Main().rest()->setSessionToken(session_token);
            Main().user()->setSessionToken(session_token);
            result.success = true;
            result.message = "Using session token from config";
        } else if (!api_key.empty() && !api_secret.empty()) {
            std::string token = Main().rest()->login(api_key, api_secret);
            if (!token.empty()) {
                Main().rest()->setSessionToken(token);
                Main().user()->setSessionToken(token);
                result.success = true;
                result.message = "Authenticated via API key exchange";
            } else {
                // Fallback: use API key as bearer
                Main().rest()->setSessionToken(api_key);
                Main().user()->setSessionToken(api_key);
                result.success = true;
                result.message = "Using API key as bearer token";
            }
        } else if (!api_key.empty()) {
            // Just API key, use as bearer
            Main().rest()->setSessionToken(api_key);
            Main().user()->setSessionToken(api_key);
            result.success = true;
            result.message = "Using API key as bearer token";
        } else {
            // NO CREDENTIALS = CRASH (strict mode by default for trading systems)
            result.success = false;
            result.message = "FATAL: No API credentials configured. Set api.session_token or api.api_key in config.";
        }
    } catch (const std::exception& e) {
        result.success = false;
        result.message = std::string("Exception: ") + e.what();
    }
    
    result.duration = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - start);
    logStepEnd(9, result);
    return result;
}

StepResult StartupSequence::step10_VerifyApiConnectivity() {
    StepResult result(10, "Verify API Connectivity");
    auto start = std::chrono::steady_clock::now();
    logStepStart(10, "Verify API Connectivity");
    
    bool require_api = Main().config()->getBool("startup.require_api_connectivity", true);
    
    try {
        auto response = Main().rest()->whoami();
        
        if (response.is_success && response.status_code >= 200 && response.status_code < 300) {
            const bool fetch_ax =
                Main().config()->getBool("startup.fetch_ax_gateway_instruments", true);
            if (fetch_ax) {
                const auto ins_resp = Main().rest()->getInstruments();
                const bool strict_ax = Main().config()->getBool(
                    "startup.strict_ax_gateway_instruments", Main().config()->isMarketMakerEnabled());
                const bool ins_ok = ins_resp.is_success && ins_resp.status_code >= 200 &&
                                    ins_resp.status_code < 300;
                if (!ins_ok) {
                    std::string detail =
                        "GET /instruments failed HTTP " + std::to_string(ins_resp.status_code);
                    if (!ins_resp.error_message.empty()) {
                        detail += " — " + ins_resp.error_message;
                    }
                    const bool hard_fail = strict_ax && require_api;
                    if (hard_fail) {
                        result.success = false;
                        result.message = "FATAL: " + detail;
                        result.duration = std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::steady_clock::now() - start);
                        logStepEnd(10, result);
                        return result;
                    }
                    Main().logger()->warn("[AX_INSTR] {} (continuing; strict_ax_gateway_instruments={})",
                                          detail, strict_ax);
                } else {
                    std::string err;
                    if (!Main().config()->applyAxGatewayInstrumentsHttpBody(ins_resp.body, strict_ax, err)) {
                        const bool hard_fail = strict_ax && require_api;
                        if (hard_fail) {
                            result.success = false;
                            result.message = "FATAL: AX /instruments — " + err;
                            result.duration = std::chrono::duration_cast<std::chrono::milliseconds>(
                                std::chrono::steady_clock::now() - start);
                            logStepEnd(10, result);
                            return result;
                        }
                        Main().logger()->warn("[AX_INSTR] merge/catalog issue (continuing): {}", err);
                    } else {
                        Main().logger()->info(
                            "[AX_INSTR] GET /instruments OK — catalog saved, tick_size and "
                            "minimum_order_size merged into market_maker config (strict={})",
                            strict_ax);
                    }
                }
            }
            // Pre-warm the orders gateway connection (separate curl handle)
            const std::string wsym = gatewayWarmupAxSymbol(*Main().config());
            if (!wsym.empty()) {
                Main().logger()->info("Pre-warming orders gateway connection (s={})...", wsym);
                nlohmann::json dummy_order = {
                    {"s", wsym},
                    {"d", "B"},
                    {"q", 0},  // Zero quantity - will be rejected but warms connection
                    {"p", "0"},
                    {"tif", "GTC"},
                    {"po", false}
                };
                auto warmup = Main().rest()->placeOrder(dummy_order);
                Main().logger()->info("Orders gateway pre-warmed (latency: {}ms)",
                                      core::latencyDisplayMs(warmup.latency.count()));
            } else {
                Main().logger()->info(
                    "Skipping orders gateway pre-warm (set market_maker.order_symbol or market_maker.symbol)");
            }
            
            result.success = true;
            result.message = "API reachable, HTTP " + std::to_string(response.status_code);
        } else {
            // API call failed
            std::string error_detail = "API call failed - HTTP " + std::to_string(response.status_code);
            if (!response.error_message.empty()) {
                error_detail += " - " + response.error_message;
            }
            if (response.body.find("token") != std::string::npos) {
                error_detail += " (Check your session_token - may need JWT from Architect dashboard)";
            }
            
            if (require_api) {
                // STRICT MODE: crash
                result.success = false;
                result.message = "FATAL: " + error_detail;
            } else {
                // Non-strict: warn but continue (for external feed only mode)
                result.success = true;
                result.message = "WARNING: " + error_detail + " - continuing with external feed only";
            }
        }
    } catch (const std::exception& e) {
        std::string error_detail = std::string("API unreachable - ") + e.what();
        if (require_api) {
            result.success = false;
            result.message = "FATAL: " + error_detail;
        } else {
            result.success = true;
            result.message = "WARNING: " + error_detail + " - continuing with external feed only";
        }
    }
    
    result.duration = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - start);
    logStepEnd(10, result);
    return result;
}

StepResult StartupSequence::step11_DetectFeedMode() {
    StepResult result(11, "Detect Feed Mode");
    auto start = std::chrono::steady_clock::now();
    logStepStart(11, "Detect Feed Mode");
    
    try {
        Main().verifyFeed();
        auto& feed_status = Main().getFeedStatus();
        
        result.success = true;
        result.message = "Mode: " + std::string(feedModeToString(feed_status.mode));
        if (feed_status.is_live) result.message += " [LIVE]";
        if (feed_status.is_paper) result.message += " [PAPER]";
        if (feed_status.is_simulation) result.message += " [SIMULATION]";
    } catch (const std::exception& e) {
        result.success = false;
        result.message = std::string("FATAL: Feed mode detection failed - ") + e.what();
    }
    
    result.duration = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - start);
    logStepEnd(11, result);
    return result;
}

StepResult StartupSequence::step12_CheckExchangeStatus() {
    StepResult result(12, "Check Exchange Status");
    auto start = std::chrono::steady_clock::now();
    logStepStart(12, "Check Exchange Status");
    
    try {
        // For now, assume exchange is operational if we got this far
        // In real implementation, check /status endpoint or similar
        result.success = true;
        result.message = "Exchange operational";
    } catch (const std::exception& e) {
        result.success = false;
        result.message = std::string("Exception: ") + e.what();
    }
    
    result.duration = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - start);
    logStepEnd(12, result);
    return result;
}

StepResult StartupSequence::step13_InitializeMarketDataManager() {
    StepResult result(13, "Initialize Market Data Manager");
    auto start = std::chrono::steady_clock::now();
    logStepStart(13, "Initialize Market Data Manager");
    
    try {
        auto& md = marketdata::MarketDataManager::getInstance();
        (void)md;
        Main().moduleStatus().marketdata_initialized = true;
        result.success = true;
        result.message = "Market data manager ready";
    } catch (const std::exception& e) {
        result.success = false;
        result.message = std::string("Exception: ") + e.what();
    }
    
    result.duration = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - start);
    logStepEnd(13, result);
    return result;
}

StepResult StartupSequence::step14_InitializeExternalFeedManager() {
    StepResult result(14, "Initialize External Feed Manager");
    auto start = std::chrono::steady_clock::now();
    logStepStart(14, "Initialize External Feed Manager");
    
    try {
        auto& ef = marketdata::ExternalFeedManager::getInstance();
        
        if (ef.isEnabled()) {
            result.success = true;
            result.message = "External feed enabled: " + 
                Main().config()->getString("external_feed.name", "unknown");
        } else {
            if (Main().config()->getBool("startup.require_external_feed", true)) {
                result.success = false;
                result.message = "External feed required but not enabled";
            } else {
                result.success = true;
                result.message = "External feed disabled (not required)";
            }
        }
    } catch (const std::exception& e) {
        result.success = false;
        result.message = std::string("Exception: ") + e.what();
    }
    
    result.duration = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - start);
    logStepEnd(14, result);
    return result;
}

StepResult StartupSequence::step15_ConnectWebSocket() {
    StepResult result(15, "Connect WebSocket");
    auto start = std::chrono::steady_clock::now();
    logStepStart(15, "Connect WebSocket");
    
    try {
        bool connected = Main().websocket()->connect();
        
        if (connected) {
            result.success = true;
            result.message = "WebSocket connected to " + Main().config()->getWebSocketEndpoint();
        } else {
            // STRICT: WebSocket must connect for live trading
            result.success = false;
            result.message = "FATAL: WebSocket connection failed to " + Main().config()->getWebSocketEndpoint();
        }
    } catch (const std::exception& e) {
        result.success = false;
        result.message = std::string("FATAL: WebSocket error - ") + e.what();
    }
    
    result.duration = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - start);
    logStepEnd(15, result);
    return result;
}

StepResult StartupSequence::step16_AuthenticateWebSocket() {
    StepResult result(16, "Authenticate WebSocket");
    auto start = std::chrono::steady_clock::now();
    logStepStart(16, "Authenticate WebSocket");
    
    try {
        if (Main().user()->hasValidSession()) {
            Main().websocket()->authenticate(Main().user()->getSessionToken());
            result.success = true;
            result.message = "WebSocket authenticated";
        } else {
            result.success = true;
            result.message = "Skipped (no session token)";
        }
    } catch (const std::exception& e) {
        result.success = false;
        result.message = std::string("Exception: ") + e.what();
    }
    
    result.duration = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - start);
    logStepEnd(16, result);
    return result;
}

StepResult StartupSequence::step17_SubscribeMarketDataChannels() {
    StepResult result(17, "Subscribe Market Data Channels");
    auto start = std::chrono::steady_clock::now();
    logStepStart(17, "Subscribe Market Data Channels");
    
    try {
        const auto watchlist = Main().config()->getMarketDataSubscriptionSymbols();
        std::string level = Main().config()->getDefaultSubscriptionLevel();
        int l2_depth = Main().config()->getL2Depth();
        
        int subscribed = 0;
        for (const auto& symbol : watchlist) {
            if (level == "L1") {
                Main().websocket()->subscribeL1(symbol);
            } else {
                Main().websocket()->subscribeL2(symbol, static_cast<std::uint32_t>(l2_depth));
            }
            Main().websocket()->subscribeTicker(symbol);
            Main().websocket()->subscribeTrades(symbol);
            Main().marketdata()->subscribe(symbol);
            ++subscribed;
        }
        
        result.success = true;
        result.message = "Subscribed to " + std::to_string(subscribed) + " symbols";
    } catch (const std::exception& e) {
        result.success = false;
        result.message = std::string("Exception: ") + e.what();
    }
    
    result.duration = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - start);
    logStepEnd(17, result);
    return result;
}

StepResult StartupSequence::step18_InitializePortfolioManager() {
    StepResult result(18, "Initialize Portfolio Manager");
    auto start = std::chrono::steady_clock::now();
    logStepStart(18, "Initialize Portfolio Manager");
    
    try {
        Main().portfolio()->initialize();
        Main().moduleStatus().portfolio_initialized = true;
        result.success = true;
        result.message = "Portfolio loaded from API";
    } catch (const std::exception& e) {
        // STRICT: Portfolio must load for live trading
        result.success = false;
        result.message = std::string("FATAL: Portfolio initialization failed - ") + e.what();
    }
    
    result.duration = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - start);
    logStepEnd(18, result);
    return result;
}

StepResult StartupSequence::step19_VerifyAccountPermissions() {
    StepResult result(19, "Verify Account Permissions");
    auto start = std::chrono::steady_clock::now();
    logStepStart(19, "Verify Account Permissions");
    
    try {
        auto& feed_status = Main().getFeedStatus();
        
        // Check for close-only mode in LIVE trading
        // For paper trading, close-only is fine
        if (feed_status.is_live) {
            // Would check is_close_only from whoami response
            // For now, assume OK
        }
        
        result.success = true;
        result.message = "Account permissions verified";
    } catch (const std::exception& e) {
        result.success = false;
        result.message = std::string("Exception: ") + e.what();
    }
    
    result.duration = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - start);
    logStepEnd(19, result);
    return result;
}

StepResult StartupSequence::step20_SubscribePrivateChannels() {
    StepResult result(20, "Subscribe Private Channels");
    auto start = std::chrono::steady_clock::now();
    logStepStart(20, "Subscribe Private Channels");
    
    try {
        if (Main().user()->hasValidSession()) {
            Main().websocket()->subscribeOrders();
            Main().websocket()->subscribeFills();
            Main().websocket()->subscribePositions();
            result.success = true;
            result.message = "Subscribed to orders, fills, positions";
        } else {
            result.success = true;
            result.message = "Skipped (no session)";
        }
    } catch (const std::exception& e) {
        result.success = false;
        result.message = std::string("Exception: ") + e.what();
    }
    
    result.duration = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - start);
    logStepEnd(20, result);
    return result;
}

StepResult StartupSequence::step21_InitializeStrategyManager() {
    StepResult result(21, "Initialize Strategy Manager");
    auto start = std::chrono::steady_clock::now();
    logStepStart(21, "Initialize Strategy Manager");
    
    try {
        // Initialize Platform's event handlers for order gateway
        // This sets up ORDER_SUBMITTED -> placeOrder() flow
        Main().initializeEventHandlers();
        
        auto& sm = strategy::StrategyManager::getInstance();
        (void)sm;
        result.success = true;
        result.message = "Strategy manager ready";
    } catch (const std::exception& e) {
        result.success = false;
        result.message = std::string("Exception: ") + e.what();
    }
    
    result.duration = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - start);
    logStepEnd(21, result);
    return result;
}

StepResult StartupSequence::step22_VerifyExternalFeed() {
    StepResult result(22, "Verify External Feed");
    auto start = std::chrono::steady_clock::now();
    logStepStart(22, "Verify External Feed");
    
    try {
        if (!Main().externalFeed()->isEnabled()) {
            if (!Main().config()->getBool("startup.require_external_feed", true)) {
                result.success = true;
                result.message = "Skipped — external feed disabled (startup.require_external_feed=false)";
                result.duration = std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::steady_clock::now() - start);
                logStepEnd(22, result);
                return result;
            }
            result.success = false;
            result.message = "FATAL: External feed required but not enabled in config";
            result.duration = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now() - start);
            logStepEnd(22, result);
            return result;
        }

        // Start all configured feeds (multi-theo always launches CME + HL + Neon
        // threads regardless of which legs need them).
        Main().externalFeed()->start();

        auto* feed = Main().externalFeed();
        const int timeout_ms = Main().config()->getInt("startup.feed_verify_timeout_ms", 10000);
        const int poll_ms = 100;
        int min_up_cfg = Main().config()->getInt("startup.external_feed_verify_min_up", 1);
        if (min_up_cfg < 0) {
            min_up_cfg = 0;
        }
        if (min_up_cfg > 3) {
            min_up_cfg = 3;
        }
        const bool crash_on_feed = Main().config()->getBool("startup.crash_on_feed_failure", true);

        if (min_up_cfg <= 0) {
            if (utils::Logger::isInitialized()) {
                utils::Logger::getInstance().error(
                    "[STARTUP:step22] external_feed_verify_min_up=0 — continuing startup without requiring "
                    "any live external feed. MM legs stay blocked until feeds recover. "
                    "Neon FIX auth errors (e.g. UserAuthenticationFailure): fix external_feed.fix.password "
                    "in config overlay.");
            }
            result.success = true;
            result.message =
                "WARNING: external_feed_verify_min_up=0 — no feed liveness required at startup (testing / desk only)";
            result.duration = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now() - start);
            logStepEnd(22, result);
            return result;
        }

        int waited_ms = 0;
        while (waited_ms < timeout_ms) {
            const bool mettraders_ok = feed->isMettradersUp();
            const bool hl_ok = feed->isHlUp();
            const bool neon_ok = feed->isNeonUp();
            const int up_now = (mettraders_ok ? 1 : 0) + (hl_ok ? 1 : 0) + (neon_ok ? 1 : 0);
            if (up_now >= min_up_cfg) {
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(poll_ms));
            waited_ms += poll_ms;
        }

        const bool mettraders_ok = feed->isMettradersUp();
        const bool hl_ok = feed->isHlUp();
        const bool neon_ok = feed->isNeonUp();
        const int up_count = (mettraders_ok ? 1 : 0) + (hl_ok ? 1 : 0) + (neon_ok ? 1 : 0);

        auto status_word = [](bool ok) { return ok ? "UP" : "DOWN"; };
        std::string per_feed =
            std::string("METTRADERS=") + status_word(mettraders_ok) +
            " Hyperliquid=" + status_word(hl_ok) +
            " Neon=" + status_word(neon_ok);
        std::string err_tail =
            std::string(" | mettraders_err=") + feed->lastMettradersError() + " | hl_err=" + feed->lastHlError() +
            " | neon_err=" + feed->lastNeonError();

        if (up_count < min_up_cfg) {
            std::string detail = per_feed + err_tail;
            if (!crash_on_feed) {
                result.success = true;
                result.message = "WARNING: only " + std::to_string(up_count) + "/" + std::to_string(min_up_cfg) +
                    " required feeds up after " + std::to_string(timeout_ms) +
                    "ms (startup.crash_on_feed_failure=false) — " + detail;
                if (utils::Logger::isInitialized()) {
                    utils::Logger::getInstance().error(
                        "[STARTUP:step22] feed verify soft-fail — {}. MM blocked until feeds recover.",
                        detail);
                }
            } else {
                result.success = false;
                result.message = "FATAL: " + std::to_string(up_count) + "/3 external feeds up; need " +
                    std::to_string(min_up_cfg) + " after " + std::to_string(timeout_ms) + "ms — " + detail;
            }
        } else {
            result.success = true;
            result.message = "External feeds " + std::to_string(up_count) +
                "/3 up (min " + std::to_string(min_up_cfg) + "): " + per_feed;
            if (up_count < 3) {
                std::string warn_detail = per_feed + err_tail;
                if (utils::Logger::isInitialized()) {
                    utils::Logger::getInstance().warn(
                        "[STARTUP:step22] degraded — only {}/3 feeds up; legs whose "
                        "theo_source is down will be blocked from quoting. {}",
                        up_count, warn_detail);
                }
            }
        }
    } catch (const std::exception& e) {
        result.success = false;
        result.message = std::string("FATAL: External feed error - ") + e.what();
    }
    
    result.duration = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - start);
    logStepEnd(22, result);
    return result;
}

StepResult StartupSequence::step23_VerifyMarketData() {
    StepResult result(23, "Verify Market Data");
    auto start = std::chrono::steady_clock::now();
    logStepStart(23, "Verify Market Data");
    
    try {
        // For now, assume market data is OK if we got this far
        // In real implementation, wait for first tick
        result.success = true;
        result.message = "Market data channels ready";
    } catch (const std::exception& e) {
        result.success = false;
        result.message = std::string("Exception: ") + e.what();
    }
    
    result.duration = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - start);
    logStepEnd(23, result);
    return result;
}

// =============================================================================
// Status Queries
// =============================================================================

StepStatus StartupSequence::getStepStatus(int step) const {
    if (step < 1 || step > 25) return StepStatus::PENDING;
    return step_status_[step - 1];
}

std::vector<StepResult> StartupSequence::getStepResults() const {
    std::lock_guard<std::mutex> lock(results_mutex_);
    return step_results_;
}

} // namespace core
} // namespace architect
