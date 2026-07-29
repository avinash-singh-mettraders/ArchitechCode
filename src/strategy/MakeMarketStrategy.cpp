#include "strategy/MakeMarketStrategy.h"

#include "strategy/MmProductLeasePolicy.h"
#include "strategy/MmOrderMover.h"
#include "strategy/VenueOrdersCache.h"
#include "core/MmJobVenueCalls.h"
#include "strategy/mm_orders_reconcile_shared.h"
#include "strategy/Strategy.h"
#include "config/Config.h"
#include "core/Platform.h"
#include "core/StartupSequence.h"
#include "marketdata/MarketDataManager.h"
#include "portfolio/PortfolioManager.h"
#include "orders/OrderManager.h"
#include "core/LatencyTracker.h"
#include "utils/Logger.h"
#include <nlohmann/json.hpp>
#include <cctype>
#include <cmath>
#include <optional>
#include <condition_variable>
#include <thread>
#include <future>
#include <iostream>
#include <iomanip>
#include <sstream>
#include <filesystem>
#include <fstream>
#include <mutex>
#include <chrono>
#if defined(__APPLE__) || defined(__unix__)
#include <fcntl.h>
#include <unistd.h>
#endif
#include <algorithm>
#include <vector>
#include <memory>
#include <set>
#include <unordered_map>
#include <unordered_set>
#include <limits>
#include <cassert>
#include <cstdlib>

namespace architect {
namespace strategy {

using namespace core;

namespace {

Quantity mmRoundQtyToStepDown(Quantity q, int step) {
    if (!std::isfinite(q) || q <= 0.0) {
        return 0.0;
    }
    if (step <= 1) {
        return std::floor(q + 1e-12);
    }
    const double s = static_cast<double>(step);
    return std::floor(q / s + 1e-12) * s;
}

std::int64_t mmSteadyMillis() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

/** Stable string for the calling thread id (std::thread::id is not directly fmt-formattable). */
std::string mmThreadIdStr() {
    std::ostringstream oss;
    oss << std::this_thread::get_id();
    return oss.str();
}

// === MAX-POSITION BREACH FIX (2026-05-22 Joe XAG breach) ====================
// Window during which a CANCEL_PENDING leg whose REST cancel returned a benign
// 404 / "unknown order" / etc. is still treated as "potentially fillable" by
// the cap-gate projection (`sumWorkingLegQtyForSymbol`). 404 is ambiguous: the
// order could have just filled with the fill notification still in flight on
// the WS fill stream. Within this window:
//   - cap-gate counts the leg → refuses to admit a cap-breaching replacement
//   - if a real fill arrives, `processFill[DeskStack]` matches by local_oid
//     and updates NetPo correctly
//   - otherwise `mmEvictStaleCancelPendingLegs` evicts the leg at the next
//     `runFullMmQuoteCycle` start, freeing capacity
// 3000 ms is comfortably above the empirical fill-notification arrival window
// observed in `Max Position Breach.txt` (~2 s between 404 cancel response and
// the matching fill event on XAG-PERP) while small enough that legit cancels
// of widely-quoted symbols don't visibly throttle re-quoting.
constexpr std::int64_t kMmCancelUncertainBudgetMs = 3000;

// Serialize desk cancel+replace REST per venue AX so N mm_req_* stacks do not interleave cancels/submits.
std::mutex g_mm_desk_ax_repricer_registry_mu;
std::unordered_map<std::string, std::unique_ptr<std::mutex>> g_mm_desk_ax_repricer_mutexes;

std::mutex& mmDeskAxRepricerMutex(const std::string& ax) {
    std::lock_guard<std::mutex> reg(g_mm_desk_ax_repricer_registry_mu);
    auto& slot = g_mm_desk_ax_repricer_mutexes[ax];
    if (!slot) {
        slot = std::make_unique<std::mutex>();
    }
    return *slot;
}

/**
 * Reload limit (`current_reload_count` vs `max_reload_cycles`): `fill_requote*` increments once per
 * qualifying fill in processFill (instrument counter), not per HTTP accept. Accept bumps cover other
 * cycle reasons when `reload_cycles_count_fill_only` is false; never for theo_move / timer_update.
 */
bool mmCycleReasonCountsTowardReloadLimit(const std::string& cycle_reason, bool fill_only_mode) {
    if (cycle_reason.empty()) {
        return false;
    }
    if (cycle_reason == "theo_move" || cycle_reason == "timer_update" || cycle_reason == "timer_repeg_one_leg" ||
        cycle_reason == "mm_orders_resume") {
        return false;
    }
    if (!fill_only_mode) {
        return true;
    }
    if (cycle_reason.rfind("fill_requote", 0) == 0) {
        return true;
    }
    return false;
}

bool mmReasonAllowsDeskInPlaceModify(const std::string& reason) {
    static constexpr const char* kReasons[] = {
        "mm_startup",
        "theo_move",
        "timer_update",
        "mm_orders_resume",
        "startup_bootstrap_post_start",
        "timer_reconcile",
        "timer_repeg_one_leg",
        "timer_pull_bid",
        "timer_pull_ask",
        "timer_one_leg_bid",
        "timer_one_leg_ask",
    };
    for (const char* r : kReasons) {
        if (reason == r) {
            return true;
        }
    }
    if (reason.rfind("fill_requote", 0) == 0) {
        return true;
    }
    if (reason == "instrument_cap_increased" || reason == "instrument_cap_decreased" ||
        reason == "instrument_limits_update") {
        return true;
    }
    return reason.rfind("mm_pair_enforce", 0) == 0;
}

bool mmIsBenignCancelFailure(const api::HttpResponse& resp) {
    std::string msg = resp.error_message.empty() ? resp.body : resp.error_message;
    for (char& c : msg) {
        c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    }
    return (resp.status_code == 404) || msg.find("not found") != std::string::npos ||
        msg.find("unknown order") != std::string::npos ||
        msg.find("cannot be canceled") != std::string::npos ||
        msg.find("cannot be cancelled") != std::string::npos;
}

bool mmSendRestCancelByOrderId(OrderId tid,
                               Side side,
                               const std::string& strategy_name,
                               const std::string& reason_tag) {
    auto& om = orders::OrderManager::getInstance();
    const auto op = om.getOrder(tid);
    if (!op) {
        return true;
    }
    if (op->type != OrderType::LIMIT || op->side != side) {
        Main().logger()->warn(
            "[STRATEGY:{}] cancel skipped: OrderManager row mismatch order_id={} expected_side={} reason={}",
            strategy_name,
            static_cast<long long>(tid),
            sideToString(side),
            reason_tag);
        return false;
    }
    if (!op->isActive() || op->isTerminal()) {
        return true;
    }
    const std::string oid = op->exchange_order_id;
    if (oid.empty()) {
        Main().logger()->warn(
            "[STRATEGY:{}] cancel skipped: no exchange oid order_id={} side={} reason={}",
            strategy_name,
            static_cast<long long>(tid),
            sideToString(side),
            reason_tag);
        return false;
    }

    // Venue source-of-truth guard (2026-06-11; skip-on-absent REMOVED 2026-06-15): resolve the
    // owning stack only to bail if it is no longer ACTIVE. We NO LONGER skip the cancel when the
    // venue's last-known snapshot lacks this oid: a just-placed order may simply not have
    // propagated into /open-orders yet, and treating "absent from a fresh snapshot" as "already
    // gone" leaked orphans (fast cancel-replace on liquid instruments — verified across 3 prod
    // logs). Refresh venue truth for the NEXT decision, then ALWAYS send the real cancel below;
    // a 404 for a genuinely-gone order is handled as benign success (mmIsBenignCancelFailure).
    if (auto sp = StrategyManager::getInstance().getStrategy(strategy_name)) {
        if (auto mm = std::dynamic_pointer_cast<MakeMarketStrategy>(sp)) {
            if (!mm->mmWorkerMayProceed("rest_cancel_by_oid")) {
                return true;  // state != ACTIVE — bail cleanly, no venue request
            }
            bool cache_fresh = false;
            if (!mm->mmVenueTruthKnowsOid(oid, cache_fresh)) {
                mm->mmTriggerImmediateVenueReconcile();
            }
        }
    }

    Main().logger()->info(
        "[STRATEGY:{}] cancel send: reason={} side={} order_id={} oid={}",
        strategy_name,
        reason_tag,
        sideToString(side),
        static_cast<long long>(tid),
        oid);

    // === CANCEL-SPEED FIX (2026-05-07) =========================================================
    // Empirical evidence from production logs: cancelOrder() (DELETE /orders/{oid}) consistently
    // takes ~900–1100ms RTT, while cancelOrderGateway() (POST /orders-gateway/cancel-orders)
    // takes ~230–250ms RTT — a ~4× speed difference for the SAME logical operation.
    // Each MM theo_move issues TWO cancels (BID + ASK), so this saves ~1.6s per move on a
    // single-thread serial mover. With moves enqueued back-to-back (typical when theo
    // oscillates by 1 tick between feed updates), this is the difference between feeling
    // responsive vs. feeling "frozen" for 3+ seconds.
    //
    // Strategy: POST-first, DELETE-fallback. Behavior preserved end-to-end:
    //  - On POST 2xx: return success (most common path, fastest).
    //  - On POST non-2xx: try DELETE (preserves the original API contract for any edge cases
    //    where the gateway doesn't recognize the oid but the legacy DELETE does).
    //  - On both fail: same benign-vs-warn handling as before.
    api::HttpResponse cr;
    {
        nlohmann::json cancel_body = nlohmann::json::object();
        cancel_body["oid"] = oid;
        cr = Main().rest()->cancelOrderGateway(cancel_body.dump());
    }
    if (!cr.is_success) {
        cr = Main().rest()->cancelOrder(oid);
    }
    if (!cr.is_success) {
        if (!mmIsBenignCancelFailure(cr)) {
            Main().logger()->warn(
                "[STRATEGY:{}] cancel failed: HTTP {} order_id={} oid={} reason={}",
                strategy_name,
                cr.status_code,
                static_cast<long long>(tid),
                oid,
                reason_tag);
            return false;
        }
        Main().logger()->debug(
            "[STRATEGY:{}] cancel benign response: HTTP {} order_id={} oid={} reason={}",
            strategy_name,
            cr.status_code,
            static_cast<long long>(tid),
            oid,
            reason_tag);
    }

    orders::OrderCancelRequest req;
    req.order_id = tid;
    (void)om.cancelOrder(req);
    return true;
}

// ============================================================================
// Concurrent multi-leg cancel (2026-07-14) — parallelise a stack's stale-leg
// cancels so a theo-move pair-cancel costs ~1 venue RTT instead of ~2 serial RTTs.
// ----------------------------------------------------------------------------
// This is the exact concurrent analogue of mmSendRestCancelByOrderId: it repeats
// that function's per-leg pre-flight (OM row validation, non-empty exchange oid,
// venue-truth guard) and its per-leg post-processing (POST-gateway then DELETE
// fallback on hard failure, benign-404 handling, and the SAME om.cancelOrder(req)
// that publishes ORDER_CANCELLED and drives the cancel-ack -> replace path). The
// ONLY behavioural difference is that the gateway cancel POSTs are issued together
// in one curl-multi batch instead of one-at-a-time. DEAD-SAFE: it never places or
// crosses anything, and any leg it cannot confirm is left CANCEL_PENDING for the
// existing async cancel-timeout reconcile (identical to the serial path's failure
// handling — see maybeReconcileDeskCancelAckTimeouts).
void mmSendRestCancelsConcurrentByOrderId(
        const std::vector<std::pair<OrderId, Side>>& legs,
        const std::string& strategy_name,
        const std::string& reason_tag) {
    if (legs.empty()) {
        return;
    }
    auto& om = orders::OrderManager::getInstance();

    // Resolve the owning stack once for the venue-truth guard (mirror of the serial path).
    // If it is no longer ACTIVE, bail cleanly with NO venue I/O — exactly as
    // mmSendRestCancelByOrderId returns early on !mmWorkerMayProceed.
    std::shared_ptr<MakeMarketStrategy> mm;
    if (auto sp = StrategyManager::getInstance().getStrategy(strategy_name)) {
        mm = std::dynamic_pointer_cast<MakeMarketStrategy>(sp);
    }
    if (mm && !mm->mmWorkerMayProceed("rest_cancel_concurrent")) {
        return;
    }

    struct Wire {
        OrderId tid;
        std::string oid;
    };
    std::vector<Wire> wire;
    std::vector<std::string> bodies;
    wire.reserve(legs.size());
    bodies.reserve(legs.size());

    for (const auto& lg : legs) {
        const OrderId tid = lg.first;
        const Side side = lg.second;
        const auto op = om.getOrder(tid);
        if (!op) {
            continue;
        }
        if (op->type != OrderType::LIMIT || op->side != side) {
            Main().logger()->warn(
                "[STRATEGY:{}] cancel skipped (concurrent): OrderManager row mismatch order_id={} "
                "expected_side={} reason={}",
                strategy_name, static_cast<long long>(tid), sideToString(side), reason_tag);
            continue;
        }
        if (!op->isActive() || op->isTerminal()) {
            continue;
        }
        const std::string oid = op->exchange_order_id;
        if (oid.empty()) {
            Main().logger()->warn(
                "[STRATEGY:{}] cancel skipped (concurrent): no exchange oid order_id={} side={} "
                "reason={}",
                strategy_name, static_cast<long long>(tid), sideToString(side), reason_tag);
            continue;
        }
        // Venue source-of-truth guard (skip-on-absent REMOVED 2026-06-15, same as serial):
        // refresh venue truth for the NEXT decision, but ALWAYS send the real cancel below.
        if (mm) {
            bool cache_fresh = false;
            if (!mm->mmVenueTruthKnowsOid(oid, cache_fresh)) {
                mm->mmTriggerImmediateVenueReconcile();
            }
        }
        Main().logger()->info(
            "[STRATEGY:{}] cancel send (concurrent): reason={} side={} order_id={} oid={}",
            strategy_name, reason_tag, sideToString(side), static_cast<long long>(tid), oid);
        nlohmann::json cancel_body = nlohmann::json::object();
        cancel_body["oid"] = oid;
        bodies.push_back(cancel_body.dump());
        wire.push_back(Wire{tid, oid});
    }

    if (wire.empty()) {
        return;
    }

    // ONE curl-multi batch: every gateway cancel leaves together (~1 RTT for the pair).
    const std::vector<api::HttpResponse> responses =
        Main().rest()->cancelOrdersGatewayConcurrent(bodies);

    for (std::size_t i = 0; i < wire.size(); ++i) {
        api::HttpResponse cr = (i < responses.size()) ? responses[i] : api::HttpResponse{};
        // POST-first, DELETE-fallback — byte-identical to mmSendRestCancelByOrderId.
        if (!cr.is_success) {
            cr = Main().rest()->cancelOrder(wire[i].oid);
        }
        if (!cr.is_success && !mmIsBenignCancelFailure(cr)) {
            Main().logger()->warn(
                "[STRATEGY:{}] cancel failed (concurrent): HTTP {} order_id={} oid={} reason={} "
                "— left open for async cancel-timeout reconcile",
                strategy_name, cr.status_code, static_cast<long long>(wire[i].tid), wire[i].oid,
                reason_tag);
            continue;  // leave CANCEL_PENDING; the timeout backstop re-sends. Never re-place here.
        }
        // Success OR benign 'already gone' → identical bookkeeping to the serial path:
        // om.cancelOrder publishes ORDER_CANCELLED (venue already dropped it), which the
        // strategy's on_cancel turns into cancel_confirmed -> (pair settled) replace.
        orders::OrderCancelRequest req;
        req.order_id = wire[i].tid;
        (void)om.cancelOrder(req);
    }
}

/** Desk: reasons that may trigger cancel-replace but must defer during gateway-seed quiet window. */
bool mmDeskReasonDeferredByGatewaySeedQuiet(const std::string& reason) {
    if (reason.rfind("fill_requote", 0) == 0) {
        return false;
    }
    static constexpr const char* kDeferred[] = {
        "mm_startup",
        "theo_move",
        "timer_update",
        "timer_reconcile",
        "mm_orders_resume",
        "startup_bootstrap_post_start",
        "timer_repeg_one_leg",
        "timer_pull_bid",
        "timer_pull_ask",
        "timer_one_leg_bid",
        "timer_one_leg_ask",
    };
    for (const char* r : kDeferred) {
        if (reason == r) {
            return true;
        }
    }
    return reason.rfind("mm_pair_enforce", 0) == 0;
}

void logMarketMakerConfigValidationWarnings() {
    auto& cfg = config::Config::getInstance();
    if (!cfg.isMarketMakerEnabled()) {
        return;
    }
    auto& log = *Main().logger();
    auto warnNonPositiveInt = [&log](const char* key, int v) {
        log.warn("[MM_CONFIG] {}={} is invalid (use integer >= 1)", key, v);
    };
    if (cfg.has("market_maker.width_bps")) {
        int w = cfg.getInt("market_maker.width_bps", 0);
        if (w <= 0) {
            warnNonPositiveInt("market_maker.width_bps", w);
        }
    }
    if (cfg.has("market_maker.spread_ticks")) {
        int w = cfg.getInt("market_maker.spread_ticks", 0);
        if (w <= 0) {
            warnNonPositiveInt("market_maker.spread_ticks", w);
        }
    }
    if (cfg.has("market_maker.bid_width_bps")) {
        int w = cfg.getInt("market_maker.bid_width_bps", 0);
        if (w <= 0) {
            warnNonPositiveInt("market_maker.bid_width_bps", w);
        }
    }
    if (cfg.has("market_maker.ask_width_bps")) {
        int w = cfg.getInt("market_maker.ask_width_bps", 0);
        if (w <= 0) {
            warnNonPositiveInt("market_maker.ask_width_bps", w);
        }
    }
    // One-shot WARN for asymmetric per-side widths. The pipeline supports asymmetric widths,
    // but the desk spec is symmetric (bid_w == ask_w); asymmetric configs break the "2*W apart"
    // invariant operators expect and silently produce skewed placed pairs. Make it loud.
    if (cfg.has("market_maker.bid_width_bps") && cfg.has("market_maker.ask_width_bps")) {
        const int bw = cfg.getInt("market_maker.bid_width_bps", 0);
        const int aw = cfg.getInt("market_maker.ask_width_bps", 0);
        if (bw > 0 && aw > 0 && bw != aw) {
            log.warn("[MM_CONFIG] asymmetric per-side width detected: market_maker.bid_width_bps={} "
                     "market_maker.ask_width_bps={} — placed pair will NOT be 2*W bps apart "
                     "(supported, but desk spec is symmetric)", bw, aw);
        }
    }
    if (cfg.has("market_maker.order_size")) {
        int sz = cfg.getInt("market_maker.order_size", 0);
        if (sz <= 0) {
            warnNonPositiveInt("market_maker.order_size", sz);
        }
    }
    if (!cfg.has("market_maker.order_size") && cfg.has("market_maker.quantity")) {
        double q = cfg.getDouble("market_maker.quantity", 0.0);
        if (q <= 0.0) {
            log.warn("[MM_CONFIG] market_maker.quantity={} is invalid (use value > 0)", q);
        }
    }
    if (cfg.has("market_maker.max_position")) {
        int v = cfg.getInt("market_maker.max_position", 0);
        if (v <= 0) {
            warnNonPositiveInt("market_maker.max_position", v);
        }
    }
    if (cfg.has("market_maker.adjust_position")) {
        int v = cfg.getInt("market_maker.adjust_position", 0);
        if (v <= 0) {
            warnNonPositiveInt("market_maker.adjust_position", v);
        }
    }
    if (cfg.has("market_maker.adjust_ticks")) {
        int v = cfg.getInt("market_maker.adjust_ticks", 0);
        if (v <= 0) {
            warnNonPositiveInt("market_maker.adjust_ticks", v);
        }
    }
    if (cfg.has("market_maker.price_tick")) {
        double t = cfg.getDouble("market_maker.price_tick", 0.0);
        if (t <= 0.0) {
            log.warn("[MM_CONFIG] market_maker.price_tick must be > 0");
        }
    }
}

std::filesystem::path mmDeskResolveLogsRelPathStr(const std::string& rel, const char* fallback) {
    namespace fs = std::filesystem;
    const std::string s = rel.empty() ? std::string(fallback) : rel;
    fs::path p(s);
    if (!p.is_absolute()) {
        p = fs::current_path() / p;
    }
    return p;
}

std::string mmDeskTrimStr(std::string s) {
    const auto not_space = [](unsigned char ch) { return !std::isspace(ch); };
    while (!s.empty() && !not_space(static_cast<unsigned char>(s.front()))) {
        s.erase(s.begin());
    }
    while (!s.empty() && !not_space(static_cast<unsigned char>(s.back()))) {
        s.pop_back();
    }
    return s;
}

std::string mmDeskUpperAscii(std::string s) {
    for (char& c : s) {
        c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
    }
    return s;
}

// Unique per-write temp path for orders.json writers. MULTIPLE processes/threads write
// orders.json (this engine's clean-slate wipe + per-accept exchange-oid patch, AND the
// external Python desk GUI). A FIXED temp name ("orders.json.tmp") shared by all of them
// lets two concurrent atomic writes race on the SAME temp inode: a shorter document written
// over a longer one leaves the longer one's tail behind, so the final rename publishes
// "valid-json + trailing garbage" ("unexpected number literal; expected end of input"). That
// corrupt file makes both the reconcile pass and the per-tick desk-stack gate treat every
// stack as missing, which silently drains all resting orders. A per-writer-unique temp name
// keeps each atomic write isolated (rename stays atomic; last writer wins with a whole doc).
std::string mmDeskUniqueTmpPath(const std::string& base) {
    static std::atomic<std::uint64_t> ctr{0};
    const std::uint64_t n = ctr.fetch_add(1, std::memory_order_relaxed);
    const long long ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                             std::chrono::steady_clock::now().time_since_epoch())
                             .count();
    return base + ".tmp." + std::to_string(static_cast<long long>(::getpid())) + "." +
           std::to_string(ns) + "." + std::to_string(n);
}

bool mmWriteOrdersJsonEmptyAtConfigPath() {
    namespace fs = std::filesystem;
    auto& cfg = config::Config::getInstance();
    const std::string rel = cfg.getString("mm_desk.mm_orders_config_path", "logs/orders.json");
    const fs::path p = mmDeskResolveLogsRelPathStr(rel, "logs/orders.json");
    std::error_code ec;
    fs::create_directories(p.parent_path(), ec);
    const std::string tmp = mmDeskUniqueTmpPath(p.string());
    const std::string payload = R"({"version":1,"stacks":[]})";
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

bool mmDeskJsonRowMatchesStackId(const nlohmann::json& row, const std::string& want_raw) {
    const std::string want = mmDeskTrimStr(want_raw);
    if (want.empty() || !row.is_object()) {
        return false;
    }
    for (const char* key : {"id", "stack_id", "request_id"}) {
        if (!row.contains(key)) {
            continue;
        }
        if (row[key].is_string()) {
            if (mmDeskTrimStr(row[key].get<std::string>()) == want) {
                return true;
            }
        } else if (row[key].is_number_integer()) {
            if (std::to_string(row[key].get<std::int64_t>()) == want) {
                return true;
            }
        }
    }
    return false;
}

/** C++ updates live orders.json stack row after venue ack (T8 / fill-requote). */
bool mmDeskPatchOrdersJsonStackExchange(const std::string& stack_id,
                                        const std::string& bid_oid,
                                        const std::string& ask_oid,
                                        double bid_px,
                                        double ask_px,
                                        int bid_qty,
                                        int ask_qty) {
    if (stack_id.empty()) {
        return false;
    }
    namespace fs = std::filesystem;
    auto& cfg = config::Config::getInstance();
    const std::string rel = cfg.getString("mm_desk.mm_orders_config_path", "logs/orders.json");
    const fs::path p = mmDeskResolveLogsRelPathStr(rel, "logs/orders.json");
    std::error_code ec;
    if (!fs::is_regular_file(p, ec)) {
        return false;
    }
    nlohmann::json doc;
    try {
        std::ifstream ifs(p);
        if (!ifs) {
            return false;
        }
        ifs >> doc;
    } catch (...) {
        return false;
    }
    if (!doc.is_object() || !doc.contains("stacks") || !doc["stacks"].is_array()) {
        return false;
    }
    bool hit = false;
    for (auto& row : doc["stacks"]) {
        if (!mmDeskJsonRowMatchesStackId(row, stack_id)) {
            continue;
        }
        if (!row.is_object()) {
            continue;
        }
        hit = true;
        row["bid_exchange_oid"] = bid_oid;
        row["ask_exchange_oid"] = ask_oid;
        row["bid_price"] = bid_px;
        row["ask_price"] = ask_px;
        row["bid_qty"] = bid_qty;
        row["ask_qty"] = ask_qty;
        break;
    }
    if (!hit) {
        return false;
    }
    doc["updated_ms"] = static_cast<std::int64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count());
    const std::string tmp = mmDeskUniqueTmpPath(p.string());
    try {
        std::ofstream ofs(tmp, std::ios::trunc | std::ios::binary);
        if (!ofs) {
            return false;
        }
        ofs << doc.dump(2) << "\n";
        ofs.flush();
    } catch (...) {
        fs::remove(tmp, ec);
        return false;
    }
#if defined(__APPLE__) || defined(__unix__)
    {
        const int fd = ::open(tmp.c_str(), O_RDWR);
        if (fd >= 0) {
            ::fsync(fd);
            ::close(fd);
        }
    }
#endif
    fs::rename(tmp, p, ec);
    return !ec;
}

bool mmDeskDocContainsStackForAx(const nlohmann::json& doc, const std::string& ax_want,
                                 const std::string& stack_id) {
    if (!doc.is_object()) {
        return false;
    }
    const std::string ax_u = mmDeskUpperAscii(ax_want);
    if (doc.contains("stacks") && doc["stacks"].is_array()) {
        for (const auto& s : doc["stacks"]) {
            if (!s.is_object()) {
                continue;
            }
            const std::string ax = mmDeskUpperAscii(s.value("ax_symbol", std::string{}));
            if (ax != ax_u) {
                continue;
            }
            if (mmDeskJsonRowMatchesStackId(s, stack_id)) {
                return true;
            }
        }
        return false;
    }
    if (!doc.contains("products") || !doc["products"].is_object()) {
        return false;
    }
    const auto& products = doc["products"];
    std::string ax_key;
    if (products.contains(ax_want)) {
        ax_key = ax_want;
    } else {
        for (auto it = products.begin(); it != products.end(); ++it) {
            if (mmDeskUpperAscii(it.key()) == ax_u) {
                ax_key = it.key();
                break;
            }
        }
    }
    if (ax_key.empty()) {
        return false;
    }
    const auto pit = products.find(ax_key);
    if (pit == products.end() || !pit->is_object()) {
        return false;
    }
    const auto& prod = *pit;
    if (!prod.contains("stacks") || !prod["stacks"].is_array()) {
        return false;
    }
    for (const auto& s : prod["stacks"]) {
        if (mmDeskJsonRowMatchesStackId(s, stack_id)) {
            return true;
        }
    }
    return false;
}

}  // namespace

MakeMarketStrategy::MakeMarketStrategy() : BaseStrategy("make_market") {
    auto& config = config::Config::getInstance();
    
    // Hedge configuration (no venue-specific defaults — set hedge.* in config when enabled)
    hedge_symbol_ = config.getString("hedge.symbol", "");
    hedge_multiplier_ = config.getDouble("hedge.multiplier", 1.0);
    hedge_threshold_ratio_ = config.getDouble("hedge.threshold_ratio", 0.5);
    auto_hedge_enabled_ = config.getBool("hedge.enabled", false);
    last_hedge_price_ = config.getDouble("hedge.initial_price", 0.0);
    
    Main().logger()->info("[STRATEGY:make_market] Hedge config loaded:");
    Main().logger()->info("  Symbol: {}", hedge_symbol_);
    Main().logger()->info("  Multiplier: {} USD/point", hedge_multiplier_);
    Main().logger()->info("  Threshold ratio: {}", hedge_threshold_ratio_);
    Main().logger()->info("  Auto-hedge enabled: {}", auto_hedge_enabled_ ? "YES" : "NO");
}

MakeMarketStrategy::MakeMarketStrategy(const std::string& name) : BaseStrategy(name) {
    auto& config = config::Config::getInstance();
    
    // Hedge configuration (no venue-specific defaults — set hedge.* in config when enabled)
    hedge_symbol_ = config.getString("hedge.symbol", "");
    hedge_multiplier_ = config.getDouble("hedge.multiplier", 1.0);
    hedge_threshold_ratio_ = config.getDouble("hedge.threshold_ratio", 0.5);
    auto_hedge_enabled_ = config.getBool("hedge.enabled", false);
    last_hedge_price_ = config.getDouble("hedge.initial_price", 0.0);
}

void MakeMarketStrategy::setMmInstrumentLeg(std::string ax_symbol,
                                            std::string order_symbol,
                                            std::string reference_fix_symbol) {
    mm_symbol_override_ = std::move(ax_symbol);
    mm_order_symbol_override_ = std::move(order_symbol);
    mm_theo_symbol_override_ = std::move(reference_fix_symbol);
}

void MakeMarketStrategy::setOptionalManualStack(const std::optional<architect::config::MarketMakerManualStack>& stack) {
    // CONCURRENCY: exclusive against all cross-thread readers (mmOptionalManualStack()).
    std::unique_lock<std::shared_mutex> lk(mm_desk_cfg_mu_);
    mm_manual_stack_ = stack;
}

bool MakeMarketStrategy::mmIsDeskManaged() const {
    // Lock-free: mm_is_desk_managed_ mirrors "mm_order_request_id_ non-empty"; both it and
    // mm_desk_active_ are atomics, so this hot-path check never touches the string or a lock.
    return mm_is_desk_managed_.load(std::memory_order_acquire) || mm_desk_active_.load(std::memory_order_acquire);
}

std::string MakeMarketStrategy::mmFeedBlockReason() const {
    if (mm_theo_source_.empty()) {
        return std::string();
    }
    auto& efm = marketdata::ExternalFeedManager::getInstance();
    if (efm.isFeedUpForSource(mm_theo_source_)) {
        return std::string();
    }
    std::string err;
    if (mm_theo_source_ == "mettraders") err = efm.lastMettradersError();
    else if (mm_theo_source_ == "hyperliquid") err = efm.lastHlError();
    else if (mm_theo_source_ == "neon_fix" || mm_theo_source_ == "neon") err = efm.lastNeonError();
    if (err.empty()) {
        err = "feed " + mm_theo_source_ + " is down";
    }
    return err;
}

int MakeMarketStrategy::mmEffBidSpreadTicks(const config::Config& cfg) const {
    if (const auto ms = mmOptionalManualStack(); ms.has_value() && ms->width_bps > 0) {
        return ms->width_bps;
    }
    if (mmIsDeskManaged()) {
        const std::string ax_key = !mm_symbol_override_.empty() ? mm_symbol_override_ : mmAxSymbol();
        const int inst_w = cfg.getMarketMakerDefaultWidthBpsForAx(ax_key);
        if (inst_w > 0) {
            return inst_w;
        }
        return cfg.getMarketMakerDeskWidthEmergencyFloorBps();
    }
    return cfg.getMarketMakerBidSpreadTicks();
}

void MakeMarketStrategy::mmCapSpreadBpsToDeskPlacedLadder(
    Price adjusted_theo, int& bid_spread_bps, int& ask_spread_bps) const {
    // === DISABLED 2026-05-19 — ratchet bug (Tim, SPY-from-HL) ===================================
    // Previous implementation lowered `bid_spread_bps` / `ask_spread_bps` to the per-side bps
    // implied by `mm_manual_stack_->placed_bid_price` / `placed_ask_price` vs `adjusted_theo`,
    // via `std::min(..., floor(offset_px * 10000 / adjusted_theo))`. Because the strategy patches
    // every replacement back into `orders.json` (mmDeskPatchOrdersJsonStackExchange on accept),
    // `placed_bid_price` / `placed_ask_price` always track the LAST quoted prices, not the
    // original desk-seeded ladder. The combination
    //     (a) `floor()` of the implied bps          (single-side narrowing per move)
    //     (b) `std::min(width_bps, implied_bps)`    (monotone non-increasing cap)
    //     (c) placed_*_price updated after each move (cap input degrades each cycle)
    // caused the pair to ratchet inward by ~1 bps every theo move. The bug was visible on
    // SPY-from-HL as the bid/ask creeping toward `adjusted_theo` and the spread shrinking from
    // 2 * width_bps to (eventually) zero. Pattern observed (SPY, width_bps=20):
    //   - theo DOWN: bid implied-bps drops first → bid is pulled UP toward theo (wrong direction).
    //   - theo UP:   ask implied-bps drops first → ask is pulled DOWN toward theo (wrong direction).
    // Spec requires the placed pair to stay at exactly `±width_bps` around `adjusted_theo`
    // (plus inventory skew, which is common-mode and cancels in the span). Cap is therefore
    // pure damage with no compensating benefit and is now a no-op. Re-enable only with a fix
    // that (1) anchors against the ORIGINAL desk-seeded prices (snapshotted once at seed and
    // never overwritten by mover-driven replaces) and (2) uses round-to-nearest, not floor.
    (void)adjusted_theo;
    (void)bid_spread_bps;
    (void)ask_spread_bps;
}

int MakeMarketStrategy::mmEffOrderSizeInt(const config::Config& /*cfg*/) const {
    // STRICT SOURCE-OF-TRUTH: order size comes ONLY from the user-entered desk
    // stack (orders.json → `mm_req_<ax>_<stack_id>` strategy). Falling back to
    // `market_maker.order_size` / `market_maker.quantity` from the runtime config
    // is forbidden — those are static defaults that have no connection to what
    // the user typed into the GUI for THIS stack and have, in the past, caused
    // C++ to "move" orders at the wrong size (e.g. quoting 100 when the user
    // submitted 10). When a strategy has no desk-entered size, this returns 0
    // and every caller refuses to place/move (see the `qty_int <= 0` gate in
    // `runFullMmQuoteCycle`).
    if (!mmIsDeskManaged()) {
        return 0;
    }
    if (const auto ms = mmOptionalManualStack(); ms.has_value() && ms->order_size > 0) {
        return ms->order_size;
    }
    return 0;
}

int MakeMarketStrategy::mmEffOrderSizeStepInt(const config::Config& cfg) const {
    // Per-stack step override is required when a stack ships a smaller order_size
    // than the global step (global step rounds qty to 0 otherwise).
    int step = 1;
    const char* source = "default_1";
    if (const auto ms = mmOptionalManualStack(); ms.has_value() && ms->order_size_step > 0) {
        step = std::max(1, ms->order_size_step);
        source = "manual_stack";
    } else if (mm_instrument_order_size_step_ > 0) {
        step = std::max(1, mm_instrument_order_size_step_);
        source = "instrument_leg";
    } else {
        const int leg_step = cfg.getMarketMakerInstrumentOrderSizeStepForAxSymbol(mmAxSymbol());
        if (leg_step > 0) {
            step = std::max(1, leg_step);
            source = "leg_symbol";
        } else {
            const int ax_min = cfg.getAxGatewayInstrumentMinimumOrderSize(mmAxSymbol());
            if (ax_min > 0) {
                step = std::max(1, ax_min);
                source = "ax_catalog_min";
            } else if (mmIsDeskManaged()) {
                // Desk-managed stacks must NOT fall back to the legacy global step:
                // `market_maker.order_size_step` is the EURUSD default (100) and would silently
                // floor any other product's order_size<100 down to 0 (e.g. XAG order_size=5 →
                // qty=0, breaking every reload cycle). Prefer gateway ``minimum_order_size`` above;
                // if still unknown, use 1 until the stack JSON supplies ``order_size_step``.
                step = 1;
                source = "desk_fallback_1";
            } else {
                step = std::max(1, cfg.getMarketMakerOrderSizeStep());
                source = "global";
            }
        }
    }
    const int pre_cap_step = step;
    // Per-stack JSON often copies `order_size_step: 100` from a EUR template while
    // `order_size` is product-specific (e.g. 5). `mmRoundQtyToStepDown` would yield 0
    // legs → MM_PLACE_*_SKIPPED / frozen desk quotes. Cap step by effective size.
    const int sz = mmEffOrderSizeInt(cfg);
    bool capped = false;
    if (sz > 0 && step > sz) {
        step = std::max(1, sz);
        capped = true;
    }
    // Throttled diagnostic — emits at most once every 8s per strategy. The "capped" /
    // "step_exceeds_size" arms are the smoking gun for `qty_bid<=0` symptoms when a
    // shared per-instrument step (e.g. 100) is larger than a small per-stack size
    // (e.g. 10). When the binary in use has the cap fix this line says cap=Y; when
    // the fix is missing this line still prints the raw mismatch and shows the
    // operator that a rebuild is required.
    if (utils::Logger::isInitialized() && Main().logger()) {
        static thread_local std::int64_t last_log_ms = 0;
        static thread_local std::string last_key;
        const auto now_ms = mmSteadyMillis();
        const std::string key = std::string(source) + "/" + std::to_string(pre_cap_step) +
                                "/" + std::to_string(step) + "/" + std::to_string(sz);
        const bool need_log = (now_ms - last_log_ms) >= 8000 || key != last_key;
        if (need_log) {
            last_log_ms = now_ms;
            last_key = key;
            Main().logger()->info(
                "[STRATEGY:{}] MM_STEP_DIAG sym={} order_size={} step_source={} pre_cap_step={} "
                "post_cap_step={} capped={} (step>size triggers cap; without cap qty rounds to 0)",
                getName(),
                mmAxSymbol(),
                sz,
                source,
                pre_cap_step,
                step,
                capped ? "Y" : "N");
        }
    }
    return step;
}

int MakeMarketStrategy::mmEffMaxPositionInt(const config::Config& cfg) const {
    // Per-instrument cap is the ONLY shared rule across L1/L2/L3 (multiple manual_stacks on one product).
    // Do not use per-stack `manual_stacks[].max_position` for gating — stacks would disagree on
    // shouldQuoteSide while sharing one exchange net. Set `market_maker.instruments[].max_position`
    // (leg) for the product cap, else `market_maker.max_position` (global).
    if (mm_instrument_max_position_ > 0) {
        return mm_instrument_max_position_;
    }
    return cfg.getMarketMakerMaxPosition();
}

double MakeMarketStrategy::mmEffPriceTick(const config::Config& cfg) const {
    if (mm_instrument_tick_ > 0.0) {
        return mm_instrument_tick_;
    }
    const double leg_tick = cfg.getMarketMakerInstrumentTickForAxSymbol(mmAxSymbol());
    if (leg_tick > 0.0 && std::isfinite(leg_tick)) {
        return leg_tick;
    }
    const double catalog_tick = cfg.getAxGatewayInstrumentTickSize(mmAxSymbol());
    if (catalog_tick > 0.0 && std::isfinite(catalog_tick)) {
        return catalog_tick;
    }
    const double g = cfg.getMarketMakerPriceTick();
    if (g > 0.0 && std::isfinite(g)) {
        return g;
    }
    return 0.0;
}

core::Price MakeMarketStrategy::mmTransformTheo(core::Price raw_mid) const {
    if (!std::isfinite(raw_mid) || raw_mid <= 0.0) {
        return 0.0;
    }
    // Desk pricer-snapshot transform supersedes the legacy theo_scale multiplier. When the
    // operator has typed (quote_snapshot, pricer_snapshot, slope) into the active manual
    // stack, the snapshot formula already carries the scale relationship between pricer and
    // quote markets, so pass the raw external feed mid straight through.
    //
    // invert_theo was REMOVED (2026-05-12). It silently mapped a 157 USD/JPY mid into a 0.006
    // USD/JPY quote scale and was a recurring source of "orders went out at 1/price" incidents.
    // Use the pricer-snapshot transform for any cross-scale bridging.
    if (const auto ms = mmOptionalManualStack(); ms.has_value() &&
        ms->quote_snapshot > 0.0 &&
        ms->pricer_snapshot > 0.0) {
        if (mm_theo_scale_ > 0.0 && utils::Logger::isInitialized()) {
            static std::mutex s_xform_warn_mu;
            static std::unordered_map<std::string, std::int64_t> s_xform_warn_last_ms;
            const std::int64_t now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                            std::chrono::steady_clock::now().time_since_epoch())
                                            .count();
            bool emit = false;
            {
                std::lock_guard<std::mutex> lk(s_xform_warn_mu);
                auto& slot = s_xform_warn_last_ms[getName()];
                if (slot == 0 || now_ms - slot >= 5000) {
                    slot = now_ms;
                    emit = true;
                }
            }
            if (emit) {
                Main().logger()->warn(
                    "[MM_XFORM] strategy={} sym={} theo_scale={:.6f} qs={:.6f} ps={:.6f} "
                    "— theo_scale IGNORED while pricer-snapshot xform is active",
                    getName(),
                    mmAxSymbol(),
                    mm_theo_scale_,
                    ms->quote_snapshot,
                    ms->pricer_snapshot);
            }
        }
        return raw_mid;
    }
    core::Price out = raw_mid;
    if (mm_theo_scale_ > 0.0) {
        out *= mm_theo_scale_;
    }
    if (!std::isfinite(out) || out <= 0.0) {
        return 0.0;
    }
    return out;
}

std::optional<core::Price> MakeMarketStrategy::mmReadTransformedTheo() const {
    if (!theo_provider_) {
        return std::nullopt;
    }
    auto raw = theo_provider_(mmTheoSymbol());
    if (!raw.has_value()) {
        return std::nullopt;
    }
    const core::Price t = mmTransformTheo(*raw);
    if (!std::isfinite(t) || t <= 0.0) {
        return std::nullopt;
    }
    return t;
}

int MakeMarketStrategy::mmEffAdjustPositionInt(const config::Config& cfg) const {
    if (const auto ms = mmOptionalManualStack(); ms.has_value() && ms->adjust_position > 0) {
        return ms->adjust_position;
    }
    return cfg.getMarketMakerAdjustPosition();
}

void MakeMarketStrategy::noteReduceOnlyState(long long net_po_rounded, long long cap_ll) {
    // Mirror the exact gate in shouldQuoteSide: `|net| < max` is "inside the cap"; `>=` is reduce-only.
    // Side semantics: short ≥ cap → only BUYs (1) flatten us; long ≥ cap → only SELLs (2) flatten us.
    const bool now_active = (std::llabs(net_po_rounded) >= std::max<long long>(1LL, cap_ll));
    int now_side = 0;
    if (now_active) {
        now_side = (net_po_rounded < 0) ? 1 : 2;  // 1=BuyOnly, 2=SellOnly (matches header docstring)
    }

    const bool was_active = mm_reduce_only_active_.exchange(now_active);
    const int  was_side   = mm_reduce_only_side_.exchange(now_side);

    if (now_active && !was_active) {
        Main().logger()->warn(
            "[STRATEGY:{}] MM_REDUCE_ONLY_ENTERED net_po={} cap={} side={} — suppressing grow-side quote, "
            "keeping reduce-side only until |net| < cap",
            getName(), net_po_rounded, cap_ll,
            now_side == 1 ? "BUY_ONLY" : (now_side == 2 ? "SELL_ONLY" : "NONE"));
    } else if (!now_active && was_active) {
        Main().logger()->info(
            "[STRATEGY:{}] MM_REDUCE_ONLY_EXITED net_po={} cap={} — resuming pair quoting on next cycle",
            getName(), net_po_rounded, cap_ll);
    } else if (now_active && was_active && now_side != was_side) {
        // Should be very rare (would require crossing through 0 inside one cycle), but log it
        // so we never silently flip BuyOnly↔SellOnly without a trail.
        Main().logger()->warn(
            "[STRATEGY:{}] MM_REDUCE_ONLY_SIDE_FLIP net_po={} cap={} from={} to={}",
            getName(), net_po_rounded, cap_ll,
            was_side == 1 ? "BUY_ONLY" : (was_side == 2 ? "SELL_ONLY" : "NONE"),
            now_side == 1 ? "BUY_ONLY" : (now_side == 2 ? "SELL_ONLY" : "NONE"));
    }
}

int MakeMarketStrategy::mmEffAdjustTicksInt(const config::Config& cfg) const {
    if (const auto ms = mmOptionalManualStack(); ms.has_value() && ms->adjust_ticks > 0) {
        return ms->adjust_ticks;
    }
    return cfg.getMarketMakerAdjustTicks();
}

int MakeMarketStrategy::mmEffMwrSizeTicks(const config::Config& cfg) const {
    // Per-leg param (no per-stack tier defined) → optional global fallback.
    const int leg = cfg.getMarketMakerInstrumentMwrSizeTicksForAxSymbol(mmAxSymbol());
    if (leg > 0) {
        return leg;
    }
    return cfg.getInt("market_maker.mwr_size_ticks", 0);
}

int MakeMarketStrategy::mmEffMwrWindowSec(const config::Config& cfg) const {
    const int leg = cfg.getMarketMakerInstrumentMwrWindowSecForAxSymbol(mmAxSymbol());
    if (leg > 0) {
        return leg;
    }
    return cfg.getInt("market_maker.mwr_window_sec", 0);
}

int MakeMarketStrategy::mmEffMwrPullSec(const config::Config& cfg) const {
    const int leg = cfg.getMarketMakerInstrumentMwrPullSecForAxSymbol(mmAxSymbol());
    if (leg > 0) {
        return leg;
    }
    return cfg.getInt("market_maker.mwr_pull_sec", 0);
}

double MakeMarketStrategy::mmEffQuoteSnapshot() const {
    if (const auto ms = mmOptionalManualStack(); ms.has_value()) {
        return ms->quote_snapshot;
    }
    return 0.0;
}

double MakeMarketStrategy::mmEffPricerSnapshot() const {
    if (const auto ms = mmOptionalManualStack(); ms.has_value()) {
        return ms->pricer_snapshot;
    }
    return 0.0;
}

double MakeMarketStrategy::mmEffSlope() const {
    if (const auto ms = mmOptionalManualStack(); ms.has_value()) {
        // slope can legitimately be 0 (clamps newMidpoint to quote_snapshot — fixed midpoint),
        // so we MUST NOT treat 0 as "use default 1". Only fall back to 1.0 when no manual stack
        // is configured at all (legacy non-desk path).
        return ms->slope;
    }
    return 1.0;
}

core::Price MakeMarketStrategy::mmApplyPricerSnapshotTransform(core::Price theo) const {
    // === DESK PRICER-SNAPSHOT TRANSFORM (2026-05-08) ============================================
    // Centralizes the desk's spec:
    //   newMidpoint = quote_snapshot + quote_snapshot * slope * ((theo - pricer_snapshot)/pricer_snapshot)
    //
    // Failsafe disable conditions (return raw `theo` unchanged):
    //   1. !mm_manual_stack_  → legacy / non-desk strategy, snapshots not configured.
    //   2. pricer_snapshot <= 0 → divisor would be undefined; operator left field blank.
    //   3. quote_snapshot  <= 0 → no AX-side anchor; transform is meaningless.
    //   4. result is non-finite or <= 0 → snapshots and theo are inconsistent enough that the
    //      transform produced garbage; swallow the bad math rather than push a NaN/negative
    //      price into the gate (which would either crash downstream or place a wild quote).
    //
    // The third arm of the formula collapses to multiplication with theo when pricer_snapshot is
    // close to theo: ((theo - pricer_snapshot)/pricer_snapshot) → 0, so newMidpoint → quote_snapshot.
    // With slope=1 and quote_snapshot==pricer_snapshot, this is the identity transform on theo.
    //
    // Throttled DEBUG line emitted on every cycle so an operator who sees a quote suddenly jump to
    // raw-theo prices can tell at a glance whether the strategy thinks the snapshot anchors are
    // configured. Watch for `xform=disabled` while orders.json shows non-zero anchors → the stack
    // adoption did not propagate the snapshot fields into mm_manual_stack_ for this strategy.
    // One locked snapshot for the whole transform (desk-bg may replace the stack mid-call).
    const auto ms = mmOptionalManualStack();
    const std::string req_id = mmOrderRequestId();
    auto log_xform_state = [&](const char* state, double new_mid_for_log) {
        if (!utils::Logger::isInitialized()) return;
        static std::mutex s_xform_log_mu;
        static std::unordered_map<std::string, std::int64_t> s_xform_log_last_ms;
        const std::int64_t now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                        std::chrono::steady_clock::now().time_since_epoch())
                                        .count();
        bool emit = false;
        {
            std::lock_guard<std::mutex> lk(s_xform_log_mu);
            auto& slot = s_xform_log_last_ms[getName()];
            if (slot == 0 || now_ms - slot >= 5000) {
                slot = now_ms;
                emit = true;
            }
        }
        if (!emit) return;
        const double qs_log = ms.has_value() ? ms->quote_snapshot : 0.0;
        const double ps_log = ms.has_value() ? ms->pricer_snapshot : 0.0;
        const double sl_log = ms.has_value() ? ms->slope : 1.0;
        Main().logger()->debug(
            "[STRATEGY:{}] [PRICER_XFORM] state={} stack_id={} qs={:.6f} ps={:.6f} slope={:.6f} "
            "theo={:.6f} new_mid={:.6f}",
            getName(), state,
            req_id.empty() ? std::string("(legacy)") : req_id,
            qs_log, ps_log, sl_log,
            static_cast<double>(theo), new_mid_for_log);
    };

    if (!ms.has_value()) {
        log_xform_state("disabled_no_manual_stack", static_cast<double>(theo));
        return theo;
    }
    const double qs = ms->quote_snapshot;
    const double ps = ms->pricer_snapshot;
    const double sl = ms->slope;
    if (!(ps > 0.0) || !(qs > 0.0)) {
        log_xform_state("disabled_zero_snapshot", static_cast<double>(theo));
        return theo;
    }
    const double new_mid = qs + qs * sl * ((theo - ps) / ps);
    if (!std::isfinite(new_mid) || new_mid <= 0.0) {
        if (utils::Logger::isInitialized()) {
            // Throttle: only when the snapshot config is bad, not on every quote.
            static std::mutex s_warn_mu;
            static std::unordered_map<std::string, std::int64_t> s_warn_last_ms;
            const std::int64_t now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                            std::chrono::steady_clock::now().time_since_epoch())
                                            .count();
            bool emit = false;
            {
                std::lock_guard<std::mutex> lk(s_warn_mu);
                auto& slot = s_warn_last_ms[getName()];
                if (slot == 0 || now_ms - slot >= 5000) {
                    slot = now_ms;
                    emit = true;
                }
            }
            if (emit) {
                Main().logger()->warn(
                    "[STRATEGY:{}] pricer_snapshot transform produced non-finite/non-positive "
                    "newMidpoint={} (theo={:.6f} qs={:.6f} ps={:.6f} slope={:.6f}) — falling "
                    "back to raw theo this cycle",
                    getName(), new_mid, theo, qs, ps, sl);
            }
        }
        return theo;
    }
    log_xform_state("active", new_mid);
    return new_mid;
}

int MakeMarketStrategy::mmEffMinTheoDriftTicksInt(const config::Config& cfg) const {
    // 1) Per-pair override from logs/orders.json (manual_stacks[].min_theo_move_ticks_to_requote
    //    or alias min_drift_ticks). Highest priority — desk explicitly told us the gate per pair.
    if (const auto ms = mmOptionalManualStack(); ms.has_value() && ms->min_theo_move_ticks_to_requote > 0) {
        return ms->min_theo_move_ticks_to_requote;
    }
    // 2) Desk-managed default. The global market_maker.min_theo_move_ticks_to_requote default
    //    (commonly 2) is too damping for FX-grade tick sizes — pairs can sit stale because the
    //    natural feed jitter is sub-2-ticks. For desk-managed pairs, default to 1 unless the
    //    deployment explicitly overrides it via mm_desk.default_min_drift_ticks.
    if (mmIsDeskManaged()) {
        return std::max(1, cfg.getInt("mm_desk.default_min_drift_ticks", 1));
    }
    // 3) Legacy global default — unchanged for non-desk strategies.
    return cfg.getMarketMakerMinTheoMoveTicksToRequote();
}

int MakeMarketStrategy::mmEffMaxReloadCyclesInt(const config::Config& cfg) const {
    if (mm_instrument_max_reload_cycles_ > 0) {
        return mm_instrument_max_reload_cycles_;
    }
    return cfg.getMarketMakerMaxReloadCycles();
}

std::string MakeMarketStrategy::mmTheoSymbol() const {
    if (!mm_theo_symbol_override_.empty()) {
        return mm_theo_symbol_override_;
    }
    return config::Config::getInstance().getMarketMakerTheoSymbol();
}

MakeMarketStrategy::~MakeMarketStrategy() {
    // Unregister from feed callbacks
    if (feed_callback_id_ >= 0) {
        marketdata::ExternalFeedManager::getInstance().unregisterCallback(feed_callback_id_);
        feed_callback_id_ = -1;
    }
}

void MakeMarketStrategy::registerFeedCallback() {
    if (feed_callback_id_ >= 0) {
        return;  // Already registered
    }
    
    auto& feed = marketdata::ExternalFeedManager::getInstance();
    feed_callback_id_ = feed.registerCallback(
        [this](const marketdata::ExternalFeedQuote& quote) {
            onFeedUpdate(quote);
        }
    );
    
    Main().logger()->info("[STRATEGY:{}] Registered for direct feed updates (callback_id={})", 
        getName(), feed_callback_id_);
}

void MakeMarketStrategy::initialize(DateInt date) {
    BaseStrategy::initialize(date);
    (void)config::Config::getInstance().reloadPrimaryConfigFromDisk();
    first_quote_logged_ = false;
    syncInventoryFromExchangePortfolio();
    auto& cfg = config::Config::getInstance();
    if (!cfg.isMarketMakerEnabled()) {
        return;
    }
    passive_until_first_manual_order_ = true;
    const int eff_max_po = mmEffMaxPositionInt(cfg);
    const std::string max_po_source =
        (mm_instrument_max_position_ > 0)
            ? (mm_max_position_source_.empty() ? std::string("instrument") : mm_max_position_source_)
            : std::string("global");
    const int eff_max_reload = mmEffMaxReloadCyclesInt(cfg);
    const std::string max_reload_source =
        (mm_instrument_max_reload_cycles_ > 0)
            ? (mm_max_reload_source_.empty() ? std::string("instrument") : mm_max_reload_source_)
            : std::string("global");
    Main().logger()->info(
        "[STACK_INIT] stack={} instrument={} effective_max_po={} max_po_source={} effective_max_reload={} max_reload_source={}",
        getName(),
        mmAxSymbol(),
        eff_max_po,
        max_po_source,
        eff_max_reload,
        max_reload_source);
    // === SILVER max_position breach fix (2026-05-21) =========================
    // Promote the per-quote-cycle `WARN_SKEW_ALWAYS_ZERO` log to a one-shot
    // init-time ERROR. With adjust_position >= max_position OR adjust_ticks<=0
    // the inventory skew floor is mathematically always zero — the strategy
    // never widens / shifts the quote based on accumulated inventory, which is
    // a safety brake that nudges fills back toward flat. Joe & Tim were
    // running mm_req stacks with `adjust_position=50000 max_po=2` for XAG/
    // SILVER on 2026-05-21 (see Example.txt @ line 4695). Operators almost
    // certainly want `adjust_position < max_position`; emit a loud one-time
    // warning at stack init so the misconfiguration is caught before the
    // breach window opens, rather than being buried in a cycle-rate WARN.
    {
        const int adjust_po_init =
            cfg.getInt("market_maker.adjust_position", 0);
        const int adjust_ticks_init =
            cfg.getInt("market_maker.adjust_ticks", 0);
        if (eff_max_po > 0 &&
            (adjust_po_init >= eff_max_po || adjust_ticks_init <= 0)) {
            Main().logger()->warn(
                "[MM_CONFIG] STACK_INIT_SKEW_DISABLED stack={} ax={} "
                "max_po={} adjust_position={} adjust_ticks={} — inventory "
                "skew floor will always be 0 (adjust_position>=max_position "
                "or adjust_ticks<=0). Strategy will NOT widen/shift quotes "
                "as inventory accumulates. Operator action: set "
                "adjust_position < max_position and adjust_ticks >= 1 in "
                "orders.json for this stack.",
                getName(), mmAxSymbol(), eff_max_po,
                adjust_po_init, adjust_ticks_init);
        }
    }
    logMarketMakerConfigValidationWarnings();
    syncInventoryFromExchangePortfolio();
    // PositionBook: bind the shared per-symbol book and seed it with the freshly-synced
    // local net (reuses the sync above — NO new REST). Startup only; seed() is exempt from
    // the writer-thread assert. No-op in off mode. Emits the one-shot boot banner.
    mmBindPositionBookIfNeeded();
    mmSeedPositionBookFromLocalNet();
    Main().logger()->info("[STRATEGY:{}] Init complete — PASSIVE, waiting for manual orders", getName());
}

bool MakeMarketStrategy::mmRestOrdersBodyContainsExchangeOid(const std::string& body, const std::string& want_oid) {
    if (want_oid.empty() || body.empty()) {
        return false;
    }
    nlohmann::json root;
    try {
        root = nlohmann::json::parse(body);
    } catch (...) {
        return false;
    }
    const auto row_matches = [&want_oid](const nlohmann::json& row) -> bool {
        if (!row.is_object()) {
            return false;
        }
        static const char* keys[] = {"order_id", "oid", "exchange_order_id", "id", "orderId"};
        for (const char* k : keys) {
            if (!row.contains(k)) {
                continue;
            }
            const auto& v = row[k];
            std::string s;
            if (v.is_string()) {
                s = v.get<std::string>();
            } else if (v.is_number_integer()) {
                s = std::to_string(v.get<std::int64_t>());
            } else {
                continue;
            }
            s.erase(0, s.find_first_not_of(" \t\r\n"));
            while (!s.empty() && (s.back() == ' ' || s.back() == '\t' || s.back() == '\r' || s.back() == '\n')) {
                s.pop_back();
            }
            if (!s.empty() && s == want_oid) {
                return true;
            }
        }
        return false;
    };
    if (root.is_array()) {
        for (const auto& el : root) {
            if (row_matches(el)) {
                return true;
            }
        }
        return false;
    }
    if (root.is_object()) {
        if (root.contains("orders") && root["orders"].is_array()) {
            for (const auto& el : root["orders"]) {
                if (row_matches(el)) {
                    return true;
                }
            }
        }
        if (root.contains("data") && root["data"].is_array()) {
            for (const auto& el : root["data"]) {
                if (row_matches(el)) {
                    return true;
                }
            }
        }
    }
    return false;
}

void MakeMarketStrategy::addOrUpdateTrackedOrder(OrderId order_id,
                                                 const std::string& exchange_oid,
                                                 Side side,
                                                 Price price,
                                                 Quantity qty) {
    if (order_id == 0) {
        return;
    }
    const auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::steady_clock::now().time_since_epoch())
                            .count();
    std::lock_guard<std::mutex> lk(tracked_orders_mutex_);
    auto& vec = (side == Side::BUY) ? tracked_bids_ : tracked_asks_;
    for (auto& t : vec) {
        if (t.local_oid == order_id || (!exchange_oid.empty() && !t.exchange_oid.empty() && t.exchange_oid == exchange_oid)) {
            t.local_oid = order_id;
            if (!exchange_oid.empty()) {
                t.exchange_oid = exchange_oid;
            }
            t.exchange_px = price;
            t.qty = qty > 0.0 ? qty : t.qty;
            if (t.remaining_qty <= 0.0) {
                t.remaining_qty = t.qty;
            }
            t.state = MmLegState::ADOPTED;
            t.state_entered_steady_ms = now_ms;
            return;
        }
    }
    TrackedLeg t;
    t.local_oid = order_id;
    t.exchange_oid = exchange_oid;
    t.side = side;
    t.exchange_px = price;
    t.qty = qty;
    t.remaining_qty = qty;
    t.state = MmLegState::ADOPTED;
    t.state_entered_steady_ms = now_ms;
    vec.clear();
    vec.push_back(t);
}

void MakeMarketStrategy::refreshLegacyTopOfBookTrackingFromVectors() {
    std::lock_guard<std::mutex> lk(tracked_orders_mutex_);
    // CONCURRENCY (2026-05-20 audit, fix C3): bid_order_id_ / ask_order_id_ /
    // last_*_price_ are std::atomic<T> — the helper lambda cannot bind them by
    // non-atomic reference, so it returns the picked pair and we publish with
    // memory_order_release. The release pairs with mmCurrentBidOrderId() /
    // mmCurrentAskOrderId()'s acquire on the desk-snapshot / feed-thread side.
    bid_order_id_.store(0, std::memory_order_release);
    ask_order_id_.store(0, std::memory_order_release);
    last_bid_price_.store(0.0, std::memory_order_release);
    last_ask_price_.store(0.0, std::memory_order_release);
    const auto pick_working = [](const std::vector<TrackedLeg>& vec) -> std::pair<OrderId, Price> {
        for (const auto& t : vec) {
            if (t.local_oid == 0) {
                continue;
            }
            if (t.state == MmLegState::ADOPTED || mmLegStateIsCancelInFlight(t.state) ||
                t.state == MmLegState::PLACE_PENDING) {
                return {t.local_oid, t.exchange_px};
            }
        }
        return {OrderId{0}, Price{0.0}};
    };
    auto [picked_bid_id, picked_bid_px] = pick_working(tracked_bids_);
    auto [picked_ask_id, picked_ask_px] = pick_working(tracked_asks_);
    bid_order_id_.store(picked_bid_id, std::memory_order_release);
    last_bid_price_.store(picked_bid_px, std::memory_order_release);
    ask_order_id_.store(picked_ask_id, std::memory_order_release);
    last_ask_price_.store(picked_ask_px, std::memory_order_release);
}

bool MakeMarketStrategy::hasTrackedOrders() const {
    std::lock_guard<std::mutex> lk(tracked_orders_mutex_);
    return !tracked_bids_.empty() || !tracked_asks_.empty();
}

TrackedLeg* MakeMarketStrategy::findTrackedByOrderId(OrderId order_id, Side* side_out) {
    for (auto& t : tracked_bids_) {
        if (t.local_oid == order_id) {
            if (side_out) *side_out = Side::BUY;
            return &t;
        }
    }
    for (auto& t : tracked_asks_) {
        if (t.local_oid == order_id) {
            if (side_out) *side_out = Side::SELL;
            return &t;
        }
    }
    return nullptr;
}

TrackedLeg* MakeMarketStrategy::findTrackedByPendingClientId(const std::string& client_order_id, Side* side_out) {
    if (client_order_id.empty()) {
        return nullptr;
    }
    for (auto& t : tracked_bids_) {
        if (t.state == MmLegState::PLACE_PENDING && t.pending_place_client_id == client_order_id) {
            if (side_out) *side_out = Side::BUY;
            return &t;
        }
    }
    for (auto& t : tracked_asks_) {
        if (t.state == MmLegState::PLACE_PENDING && t.pending_place_client_id == client_order_id) {
            if (side_out) *side_out = Side::SELL;
            return &t;
        }
    }
    return nullptr;
}

void MakeMarketStrategy::eraseTrackedByOrderId(OrderId order_id) {
    std::lock_guard<std::mutex> lk(tracked_orders_mutex_);
    auto erase_id = [order_id](std::vector<TrackedLeg>& vec) {
        vec.erase(std::remove_if(vec.begin(), vec.end(),
                                 [order_id](const TrackedLeg& t) { return t.local_oid == order_id; }),
                  vec.end());
    };
    erase_id(tracked_bids_);
    erase_id(tracked_asks_);
}

// =============================================================================
// CYCLE-LOCK PREDICATE
// =============================================================================
//
// Returns true if this strategy's pair has any leg mid-cancel-replace. The
// drift-gate paths (onFeedUpdate + the 1Hz timer) check this BEFORE evaluating
// `min_theo_move_ticks_to_requote` so a cycle in flight is never compounded by
// a second cycle started off a feed tick that arrived 50ms after we sent a
// cancel REST. The previous behaviour fired the gate 3-5 times per cycle —
// each evaluation either no-op'd (good but wasted CPU + spammed the log) or
// raced into a redundant cancel-replace (bad — produced phantom OIDs and
// reload-count churn).
//
// Multi-pair independence: each MM stack owns its own MakeMarketStrategy
// instance, so this predicate is naturally per-pair. Pair A in the middle of
// a cycle does NOT cause Pair B's instance to skip — that instance's own
// `tracked_*_` and `mm_pending_accepts_` are independent.
bool MakeMarketStrategy::isPairCycleInFlight() const {
    std::lock_guard<std::mutex> lk(tracked_orders_mutex_);

    // Outstanding place_acks: the place_order REST returned and we are waiting
    // for the venue's accept event before the leg flips to ADOPTED. Decremented
    // exactly once in on_accept per matched pending leg.
    if (mm_pending_accepts_ > 0) {
        return true;
    }

    auto leg_in_flight = [](const TrackedLeg& leg) {
        return mmLegStateIsCancelInFlight(leg.state) ||
               leg.state == MmLegState::PLACE_PENDING;
    };
    for (const auto& leg : tracked_bids_) {
        if (leg_in_flight(leg)) return true;
    }
    for (const auto& leg : tracked_asks_) {
        if (leg_in_flight(leg)) return true;
    }
    return false;
}

// =============================================================================
// CYCLE-COMPLETION RE-CHECK
// =============================================================================
//
// Called from on_accept once a place-ack lands. If this accept brought us back
// to a fully-tracked state (no pending accepts, no in-flight legs), we re-read
// the current theo and check whether it has drifted past min_theo_move_ticks
// against the prices we just placed. If yes → fire a new cycle immediately
// (theo moved during the cycle and our just-placed pair is already stale). If
// no → emit [CYCLE_DONE] and let normal feed-driven eval resume.
//
// We compare against `last_bid_price_` / `last_ask_price_` (the prices we
// placed at), NOT against the venue working price. Reason: the venue could
// fill a leg before this recheck runs; using the venue price would treat
// fill-in-progress as drift. Last-placed price is what the drift gate in
// onFeedUpdate uses too, so the two stay symmetric.
void MakeMarketStrategy::mmMaybePostCycleRecheck() {
    // Defensive: only fire if cycle truly settled. on_accept already decremented
    // mm_pending_accepts_ before calling us, but a sibling leg could still be
    // CANCEL_PENDING or PLACE_PENDING (multi-leg cycle, place_ack races, etc.).
    if (isPairCycleInFlight()) {
        return;
    }

    // Cycle-lock semantics target the desk-managed cancel-replace flow that
    // routes through enqueueQuoteCycleOnMover. The legacy modify-only path
    // does not use this gate.
    if (!mmIsDeskManaged()) {
        return;
    }

    if (!theo_provider_) {
        return;
    }
    Price theo_now = 0.0;
    try {
        auto opt = theo_provider_(mmTheoSymbol());
        if (!opt.has_value()) return;
        theo_now = *opt;
    } catch (...) {
        return;
    }
    if (!std::isfinite(theo_now) || theo_now <= 0.0) {
        return;
    }

    // UNIT-CONSISTENCY (2026-05-20): `last_theo_` is stored as the *pre*-pricer-snapshot value
    // everywhere it is written (see onFeedUpdate line ~4030 — `last_theo_ = feed_mid_transformed`
    // where `feed_mid_transformed = mmTransformTheo(quote.mid)` only; and runFullMmQuoteCycle which
    // stamps `last_theo_ = theo_midpoint` where theo_midpoint also went through `mmTransformTheo`
    // only). For the drift comparison to be unit-consistent we must ONLY apply `mmTransformTheo`
    // here too — not `mmApplyPricerSnapshotTransform`.
    //
    // Live evidence of the bug (SPY logs, 2026-05-20 10:50:36):
    //   [CYCLE_RECHECK] sym=SPY-PERP ... drift_ticks=664433 theo_now=736.570082 last_theo=7380.900000
    // SPY's pricer-snapshot transform divides the raw HL theo by ~10 (qs=736.61 / ps=7381.30),
    // so applying the snapshot here while last_theo_ stores pre-snapshot produced a phantom
    // 664,433-tick "drift" on EVERY cycle, firing a redundant `post_cycle_recheck` cancel-replace
    // immediately after every legitimate move. The extra cycle (a) doubled REST traffic, (b)
    // widened the window where `bid_order_id_=0 && ask_order_id_=0` (race with mm_pair_enforce
    // DESK_RECOVERY → see segfault 37.100), and (c) increased the chance of the cap-breach race.
    const Price feed_mid_transformed = mmTransformTheo(theo_now);

    const auto& cfg = config::Config::getInstance();
    const double quote_tick = mmEffResolvedQuoteTick(cfg);
    const int min_drift_ticks = std::max(1, mmEffMinTheoDriftTicksInt(cfg));
    const double need = static_cast<double>(min_drift_ticks) * quote_tick;
    if (!(quote_tick > 0.0) || !std::isfinite(quote_tick)) {
        return;
    }

    // Drift definition: the magnitude theo moved since the last accepted theo
    // anchor (last_theo_, set at the end of onFeedUpdate when a cycle either
    // fired or the gate passed). This is exactly what the onFeedUpdate
    // evaluation would compute on the next feed tick — keeping the test
    // identical here means [CYCLE_RECHECK] vs [CYCLE_DONE] mirrors what the
    // very next onFeedUpdate call would do.
    const double theo_drift = (last_theo_ > 0.0)
                                  ? std::fabs(static_cast<double>(feed_mid_transformed - last_theo_))
                                  : 0.0;
    const long long drift_ticks = (quote_tick > 0.0)
                                      ? static_cast<long long>(std::llround(theo_drift / quote_tick))
                                      : 0LL;
    const bool drift_tripped = theo_drift + 1e-12 >= need;

    if (drift_tripped) {
        if (utils::Logger::isInitialized()) {
            Main().logger()->info(
                "[CYCLE_RECHECK] sym={} stack={} theo moved during cycle, firing new cycle "
                "immediately drift_ticks={} min_drift={} theo_now={:.6f} last_theo={:.6f}",
                mmAxSymbol(),
                mmOrderRequestIdOrLegacy(),
                drift_ticks, min_drift_ticks,
                static_cast<double>(feed_mid_transformed),
                static_cast<double>(last_theo_));
        }
        enqueueQuoteCycleOnMover("post_cycle_recheck");
        // RECHECK path queues another cycle via runFullMmQuoteCycle which will
        // re-stamp last_theo_ and (indirectly via its own paths) maintain
        // mm_desk_active_. We therefore deliberately fall through to the
        // [CYCLE_STATE_RESTORED] block below so the diagnostic still emits and
        // mm_desk_active_ is recovered immediately rather than waiting on the
        // next cycle to make-good (the recheck cycle could also race with an
        // orders.json reconcile that flips the flag again).
    } else {
        if (utils::Logger::isInitialized()) {
            Main().logger()->info(
                "[CYCLE_DONE] sym={} stack={} cycle complete, resuming theo tracking "
                "drift_ticks={} min_drift={}",
                mmAxSymbol(),
                mmOrderRequestIdOrLegacy(),
                drift_ticks, min_drift_ticks);
        }
        // CYCLE_DONE: refresh the drift baseline to the post-cycle moment so
        // the next feed tick measures "drift since cycle settled" rather than
        // "drift since cycle started." runFullMmQuoteCycle stamps last_theo_
        // at cycle start (the theo it used to compute the just-placed pair),
        // which is up to ~hundreds of ms stale by the time the place-acks land.
        // For the RECHECK branch we skip this — enqueueQuoteCycleOnMover will
        // run runFullMmQuoteCycle in turn and stamp last_theo_ to the freshly
        // read theo on the worker thread.
        last_theo_ = feed_mid_transformed;
    }

    // === Post-cycle state restoration ==========================================
    // Bug observed 2026-05-08 13:55: after a successful cancel-replace cycle the
    // FEED_ROUTE log started reporting `desk_active=false` despite tracked_bids/
    // tracked_asks being populated. Root cause: applyMmDeskSeededJsonLocked
    // (orders.json reconciler) re-evaluates `mm_desk_active_ = (bid_order_id_!=0
    // || ask_order_id_!=0)` whenever it adopts/refreshes; if it runs during the
    // cancel→place_ack window of a cancel-replace cycle, both ids are 0 and the
    // flag flips to false. Nothing else in the cycle path re-arms the flag,
    // because the cancel-replace path itself does not own this flag — it's
    // owned by the desk-adoption path. Result: post-cycle the flag is wrong
    // until the next orders.json reconcile (which only re-arms if it actually
    // re-runs adoption, which is gated by other conditions).
    //
    // Fix: re-evaluate the same predicate at cycle completion. By this point
    // on_accept has called refreshLegacyTopOfBookTrackingFromVectors which set
    // bid_order_id_/ask_order_id_ from the just-adopted tracked legs, so the
    // condition is now accurate.
    //
    // mm_desk_active_ is a plain bool intentionally raced across threads (see
    // existing call sites at 2390 / 2624 / 5195 / 6026 / 7641 / 7906); we follow
    // the same convention here.
    const bool was_desk_active = mm_desk_active_;
    const bool legs_alive_now = (bid_order_id_ != 0 || ask_order_id_ != 0);
    mm_desk_active_ = legs_alive_now;
    int tracked_bids_count = 0;
    int tracked_asks_count = 0;
    {
        std::lock_guard<std::mutex> lk(tracked_orders_mutex_);
        tracked_bids_count = static_cast<int>(tracked_bids_.size());
        tracked_asks_count = static_cast<int>(tracked_asks_.size());
    }
    if (utils::Logger::isInitialized()) {
        Main().logger()->info(
            "[CYCLE_STATE_RESTORED] sym={} stack={} desk_active={} (was={}) "
            "last_theo={:.6f} bid_id={} ask_id={} tracked_bids={} tracked_asks={}",
            mmAxSymbol(),
            mmOrderRequestIdOrLegacy(),
            mm_desk_active_ ? 1 : 0,
            was_desk_active ? 1 : 0,
            static_cast<double>(last_theo_),
            static_cast<long long>(bid_order_id_),
            static_cast<long long>(ask_order_id_),
            tracked_bids_count,
            tracked_asks_count);
    }
}

bool MakeMarketStrategy::mmDeskPairCancelSettledForTheoReplace() const {
    std::lock_guard<std::mutex> lk(tracked_orders_mutex_);
    auto blocks = [](const std::vector<TrackedLeg>& vec) {
        for (const auto& t : vec) {
            if (t.state == MmLegState::ADOPTED || mmLegStateIsCancelInFlight(t.state)) {
                return true;
            }
        }
        return false;
    };
    return !blocks(tracked_bids_) && !blocks(tracked_asks_);
}

bool MakeMarketStrategy::processTrackedOrdersTheoMove(Price theo_midpoint, const std::string& reason) {
    auto leg_state_str = [](MmLegState st) {
        switch (st) {
            case MmLegState::IDLE: return "IDLE";
            case MmLegState::PAUSED: return "PAUSED";
            case MmLegState::ADOPTED: return "ADOPTED";
            case MmLegState::CANCEL_PENDING: return "CANCEL_PENDING";
            case MmLegState::CANCEL_SENT_UNCONFIRMED: return "CANCEL_SENT_UNCONFIRMED";
            case MmLegState::CANCELLED: return "CANCELLED";
            case MmLegState::PLACE_PENDING: return "PLACE_PENDING";
            case MmLegState::FILLED: return "FILLED";
        }
        return "UNKNOWN";
    };
    auto intent_str = [](MmDeskCancelIntent intent) {
        switch (intent) {
            case MmDeskCancelIntent::None: return "None";
            case MmDeskCancelIntent::TheoMove: return "TheoMove";
            case MmDeskCancelIntent::FillOpposite: return "FillOpposite";
            case MmDeskCancelIntent::DeskReseed: return "DeskReseed";
            case MmDeskCancelIntent::FireAndTrack: return "FireAndTrack";
        }
        return "Unknown";
    };
    auto skip_log = [&](const char* side, const std::string& reason_str, const std::string& details) {
        if (!utils::Logger::isInitialized()) {
            return;
        }
        Main().logger()->info("[THEO_MOVE_SKIP] sym={} side={} reason='{}' details='{}'",
                              mmAxSymbol(), side, reason_str, details);
    };

    if (!mmIsDeskManaged()) {
        skip_log("both", "not_desk_managed", "mmIsDeskManaged=false");
        return false;
    }
    if (passive_until_first_manual_order_) {
        skip_log("both", "passive_until_first_manual_order", "passive flag set");
        return false;
    }
    if (mm_desk_stack_paused_) {
        skip_log("both", "desk_stack_paused", "mm_desk_stack_paused=true");
        return false;
    }

    std::lock_guard<std::recursive_mutex> cycle_lock(mm_cycle_mutex_);
    maybeReconcileDeskCancelAckTimeouts();
    auto& cfg = config::Config::getInstance();
    if (!mmMarketMakerOrdersPlacementAllowed(cfg)) {
        skip_log("both", "placement_blocked", "mmMarketMakerOrdersPlacementAllowed=false");
        return false;
    }
    if (!hasTrackedOrders()) {
        skip_log("both", "no_tracked_orders", "tracked vectors empty");
        return false;
    }

    double quote_tick = mmEffResolvedQuoteTick(cfg);
    if (!(quote_tick > 0.0) || !std::isfinite(quote_tick)) {
        skip_log("both", "unresolved_quote_tick",
                 "need ax_gateway_instruments_catalog or merged market_maker.instruments tick_size");
        return false;
    }
    const int eff_spread_pair = mmEffBidSpreadTicks(cfg);
    const int resting_extra_ticks = cfg.getMarketMakerRestingDepthExtraTicks();
    const double basis = cfg.getMarketMakerBasis();
    // Apply desk pricer-snapshot transform BEFORE adding `basis`, so basis stays meaningful in
    // AX-quoted units (i.e. you can still nudge an AX product by an absolute tick offset on top
    // of the slope-mapped midpoint). When the transform is disabled, this is identity → unchanged.
    const Price adjusted_theo = mmApplyPricerSnapshotTransform(theo_midpoint) + basis;
    int bid_spread_bps = eff_spread_pair;
    int ask_spread_bps = eff_spread_pair;
    mmCapSpreadBpsToDeskPlacedLadder(adjusted_theo, bid_spread_bps, ask_spread_bps);
    Price raw_bid = 0.0;
    Price raw_ask = 0.0;
    computeInventorySkewedRawBidAsk(adjusted_theo, bid_spread_bps, ask_spread_bps, quote_tick, raw_bid, raw_ask);
    if (resting_extra_ticks > 0) {
        raw_bid -= static_cast<double>(resting_extra_ticks) * quote_tick;
        raw_ask += static_cast<double>(resting_extra_ticks) * quote_tick;
    }
    Price tgt_bid = 0.0;
    Price tgt_ask = 0.0;
    finalizeMmPairOnTickGrid(raw_bid, raw_ask, bid_spread_bps, ask_spread_bps, quote_tick, tgt_bid, tgt_ask);

    const int min_move_ticks = std::max(1, mmEffMinTheoDriftTicksInt(cfg));

    // Pillar B — fire-and-track (place_before_cancel_ack): ENFORCE mode only. When true,
    // this job sends the pair cancels then IMMEDIATELY places the fresh pair without
    // waiting for the cancel acks. The old legs go to CANCEL_SENT_UNCONFIRMED (intent
    // FireAndTrack) and are resolved asynchronously (ack / fill / venue-absence). Default
    // false = classic ack-wait semantics (legs → CANCEL_PENDING, re-add on ack). Ignored
    // in off/shadow (mmPbEnforce()==false).
    const bool fire_and_track =
        mmPbEnforce() && cfg.getBool("market_maker.place_before_cancel_ack", false);

    struct EvalLeg {
        TrackedLeg* leg{nullptr};
        const char* leg_name{""};
        Side side{Side::BUY};
        Price target_px{0.0};
        double working_px{0.0};
        long long tick_delta{0};
        bool needs_cancel{false};
    };
    struct PendingCancel {
        Side side{Side::BUY};
        OrderId oid{0};
        std::string exch_oid;
        const char* leg{""};
        double working_px{0.0};
    };
    std::vector<PendingCancel> cancels;
    {
        std::lock_guard<std::mutex> lk(tracked_orders_mutex_);
        std::vector<EvalLeg> evals;
        auto collect = [&](std::vector<TrackedLeg>& vec, const char* leg_name, Side side, Price target_px) {
            for (auto& t : vec) {
                const std::string oid_log =
                    !t.exchange_oid.empty() ? t.exchange_oid : std::to_string(static_cast<long long>(t.local_oid));
                if (utils::Logger::isInitialized()) {
                    Main().logger()->info(
                        "[THEO_MOVE_LEG] sym={} side={} leg_state={} exchange_px={:.6f} qty={} oid={} pending_intent={} cancel_after_opposite={}",
                        mmAxSymbol(),
                        sideToString(side),
                        leg_state_str(t.state),
                        t.exchange_px,
                        t.qty,
                        oid_log,
                        intent_str(t.pending_cancel_intent),
                        t.cancel_after_opposite_settled ? 1 : 0);
                }
                if (mmLegStateIsCancelInFlight(t.state) || t.state == MmLegState::PLACE_PENDING) {
                    skip_log(
                        sideToString(side),
                        "pending_transition",
                        "state=" + std::string(leg_state_str(t.state)) + " oid=" + oid_log);
                    continue;
                }
                if (t.state != MmLegState::ADOPTED) {
                    skip_log(
                        sideToString(side),
                        "not_adopted",
                        "state=" + std::string(leg_state_str(t.state)) + " oid=" + oid_log);
                    continue;
                }
                if (t.local_oid == 0) {
                    skip_log(sideToString(side), "missing_local_oid", "state=ADOPTED oid=0");
                    continue;
                }
                const double working_px = t.exchange_px;
                const long long tick_delta =
                    static_cast<long long>(std::llround((target_px - working_px) / quote_tick));
                const bool needs =
                    std::llabs(tick_delta) >= static_cast<long long>(min_move_ticks);
                if (utils::Logger::isInitialized()) {
                    Main().logger()->info(
                        "[THEO_MOVE_TARGET] sym={} side={} current_theo={:.6f} target_px={:.6f} exchange_px={:.6f} tick_delta={} threshold_ticks={}",
                        mmAxSymbol(),
                        sideToString(side),
                        theo_midpoint,
                        target_px,
                        working_px,
                        tick_delta,
                        min_move_ticks);
                }
                if (!needs) {
                    skip_log(
                        sideToString(side),
                        "below_threshold",
                        "tick_delta=" + std::to_string(tick_delta) +
                            " threshold=" + std::to_string(min_move_ticks) +
                            " target_px=" + std::to_string(target_px) +
                            " exchange_px=" + std::to_string(working_px));
                }
                evals.push_back(EvalLeg{&t, leg_name, side, target_px, working_px, tick_delta, needs});
            }
        };
        collect(tracked_bids_, "bid", Side::BUY, tgt_bid);
        collect(tracked_asks_, "ask", Side::SELL, tgt_ask);
        bool any_cancel = false;
        for (const auto& e : evals) {
            if (e.needs_cancel) {
                any_cancel = true;
                break;
            }
        }
        if (!any_cancel) {
            skip_log(
                "both",
                "no_leg_above_threshold",
                "reason=" + reason + " min_move_ticks=" + std::to_string(min_move_ticks) +
                    " eval_count=" + std::to_string(evals.size()));
            return false;
        }
        if (utils::Logger::isInitialized()) {
            for (const auto& e : evals) {
                Main().logger()->info(
                    "[THEO_MOVE] sym={} leg={} working_px={} target_px={} tick_delta={} decision={}",
                    mmAxSymbol(),
                    e.leg_name,
                    e.working_px,
                    e.target_px,
                    e.tick_delta,
                    e.needs_cancel ? "CANCEL_PAIR" : "CANCEL_PAIR_WITH");
            }
        }
        // Pair repricing: if either leg drifted past min_move_ticks, cancel BOTH adopted legs so
        // the replacement pair is laid from one theo snapshot (width + skew preserved).
        for (const auto& e : evals) {
            if (e.leg == nullptr) {
                skip_log(
                    sideToString(e.side),
                    "null_eval_leg",
                    "leg_name=" + std::string(e.leg_name) + " pair_cancel=1");
                continue;
            }
            e.leg->state = fire_and_track ? MmLegState::CANCEL_SENT_UNCONFIRMED
                                          : MmLegState::CANCEL_PENDING;
            e.leg->pending_cancel_intent = fire_and_track ? MmDeskCancelIntent::FireAndTrack
                                                          : MmDeskCancelIntent::TheoMove;
            e.leg->desk_cancel_timeout_retry_sent = false;
            e.leg->state_entered_steady_ms = mmSteadyMillis();
            cancels.push_back(
                PendingCancel{e.side, e.leg->local_oid, e.leg->exchange_oid, e.leg_name,
                              e.working_px});
        }
    }

    if (cancels.empty()) {
        skip_log("both", "cancel_list_empty", "no cancel requests after evaluation");
        return false;
    }

    // Combined c,c,p,p batch (async_batch_submit): when fire-and-track is batching, DON'T
    // enqueue the cancels on the mover ahead of the places — hand the cancel local-ids (plus
    // the old resting prices, for the self-trade guard) to mmFireAndTrackPlacePair, which fires
    // the cancels + fresh places together in ONE curl-multi batch (or the guarded 2-RTT
    // fallback on a big jump). Legacy path (flag off) keeps routing cancels via the mover.
    //
    // DELIBERATE OMISSION (do NOT add back): the mover cancel path (mmSendRestCancelByOrderId)
    // runs a per-cancel venue-truth refresh (mmWorkerMayProceed + mmVenueTruthKnowsOid →
    // mmTriggerImmediateVenueReconcile). The combined-batch path INTENTIONALLY skips it: that
    // refresh is a venue GET on the order thread, which the enforce-mode directive bans and
    // MM_JOB_VENUE_CALLS gets=0 enforces. Its only job was triggering opportunistic reconcile,
    // now covered on its own cadence by the FastPoller + VenueOrdersCache. Re-adding it here
    // would re-infect the sterilized order path.
    const bool combined_batch =
        fire_and_track && cfg.getBool("market_maker.async_batch_submit", false);
    std::vector<OrderId> batch_cancel_ids;
    Price old_bid_px = 0.0;
    Price old_ask_px = 0.0;
    if (combined_batch) {
        batch_cancel_ids.reserve(cancels.size());
        for (const auto& c : cancels) {
            batch_cancel_ids.push_back(c.oid);
            if (c.side == Side::BUY) {
                old_bid_px = c.working_px;
            } else {
                old_ask_px = c.working_px;
            }
            if (utils::Logger::isInitialized()) {
                Main().logger()->info(
                    "[THEO_MOVE] cancel queued (combined-batch): leg={} oid={}",
                    c.leg,
                    c.exch_oid.empty() ? std::to_string(static_cast<long long>(c.oid))
                                       : c.exch_oid);
            }
        }
    } else {
        // Route cancels to the per-AX mover so the timer thread doesn't do REST.
        // The local CANCEL_PENDING state was already set above under tracked_orders_mutex_,
        // so the cancel-ack reconcile (which runs from this same timer thread on a later tick)
        // sees the correct intent regardless of the order in which the mover processes the cancel.
        //
        // CONCURRENT CANCELS (2026-07-14, default ON): fire BOTH stale-leg cancels together in
        // one curl-multi batch (~1 venue RTT) instead of two serial mover jobs (~2 RTT). This is
        // DEAD-SAFE — it changes only the HTTP transport; each leg still gets the identical
        // validation / venue-truth guard / om.cancelOrder(req) bookkeeping, so the event-driven
        // cancel-ack -> replace path is unchanged and the fresh pair is STILL placed only after
        // BOTH cancels are venue-confirmed. Set market_maker.concurrent_cancels=false to restore
        // the strictly-serial path.
        const bool concurrent_cancels = cfg.getBool("market_maker.concurrent_cancels", true);
        if (concurrent_cancels && cancels.size() >= 2) {
            std::vector<std::pair<OrderId, Side>> legs;
            legs.reserve(cancels.size());
            for (const auto& c : cancels) {
                if (utils::Logger::isInitialized()) {
                    Main().logger()->info(
                        "[THEO_MOVE] cancel sent (mover-queued, concurrent): leg={} oid={}",
                        c.leg,
                        c.exch_oid.empty() ? std::to_string(static_cast<long long>(c.oid))
                                           : c.exch_oid);
                }
                legs.emplace_back(c.oid, c.side);
            }
            enqueueRestCancelsConcurrentOnMover(std::move(legs), "theo_move_cancel");
        } else {
            for (const auto& c : cancels) {
                if (utils::Logger::isInitialized()) {
                    Main().logger()->info(
                        "[THEO_MOVE] cancel sent (mover-queued): leg={} oid={}",
                        c.leg,
                        c.exch_oid.empty() ? std::to_string(static_cast<long long>(c.oid))
                                           : c.exch_oid);
                }
                enqueueRestCancelOnMover(c.oid, c.side, "theo_move_cancel");
            }
        }
    }

    // MM_TT: the cancels have left the box (mover-queued) with NO reconcile-gate /
    // venue-reconcile / NetPo read ahead of them. Record and report tick-to-cancel so
    // Tim can compare against the 1.3s baseline / the 20ms cancel-latency complaint.
    const std::int64_t tt_cancel_ms = mmSteadyMillis();
    mm_tt_cancel_sent_ms_.store(tt_cancel_ms, std::memory_order_relaxed);
    const std::int64_t tt_feed_ms = mm_tt_feed_arrival_ms_.load(std::memory_order_relaxed);
    if (utils::Logger::isInitialized()) {
        Main().logger()->info(
            "[MM_TT] sym={} stack={} phase=cancel_sent legs={} feed_arrival_ms={} "
            "cancel_sent_ms={} tick_to_cancel_ms={} ack_mode={}",
            mmAxSymbol(),
            mmOrderRequestIdOrLegacy(),
            static_cast<int>(cancels.size()),
            tt_feed_ms,
            tt_cancel_ms,
            (tt_feed_ms > 0) ? (tt_cancel_ms - tt_feed_ms) : -1,
            fire_and_track ? "pre" : "post");
    }

    last_theo_ = theo_midpoint;
    last_theo_requote_anchor_ = theo_midpoint;
    last_theo_requote_anchor_inited_ = true;
    refreshLegacyTopOfBookTrackingFromVectors();

    // Pillar B — fire-and-track: place the fresh pair NOW, back-to-back after the cancels,
    // WITHOUT waiting for the cancel acks. The old legs remain CANCEL_SENT_UNCONFIRMED and
    // are resolved asynchronously. Wire order is therefore cancel,cancel,place,place.
    //
    // RISK (documented per spec): between place-send and cancel-effect at the venue (~1 RTT)
    // both the old and new leg on a side can be live. Worst transient exposure is
    // max_position + 2*order_size per side, self-correcting within one fills-poll interval
    // (the fills poller books any fill into PositionBook → effective() → grow-side pull).
    // This LOOSENS the 2026-07-03 post-cancel-confirm gating; it ships default-OFF.
    if (fire_and_track) {
        // P6: fire-and-track is a PLACE site and must honor the same single placement
        // predicate as every other. mm_orders_enabled is already gated at the top of this
        // function, so in practice this adds the MWR / fast-market pause gate the
        // fire-and-track path previously bypassed (dormant while place_before_cancel_ack is
        // false — this whole branch is skipped then, so default behaviour is byte-identical).
        // The cancels above already went out (cancels are never gated); during suppression we
        // simply do not re-place — pure suppression, leaving the side pulled until resume.
        auto& cfg_place = config::Config::getInstance();
        if (mmPlacementAllowedForSymbol(cfg_place)) {
            mmFireAndTrackPlacePair(tgt_bid, tgt_ask, theo_midpoint, reason, batch_cancel_ids,
                                    old_bid_px, old_ask_px);
        } else {
            mmLogMmGateThrottled(
                "fire_and_track_placement_suppressed", reason,
                "placement suppressed (per-instrument HOLD or MWR/fast-market pause) "
                "— leaving side pulled after cancels");
        }
    }
    return true;
}

void MakeMarketStrategy::mmFireAndTrackPlacePair(Price tgt_bid, Price tgt_ask,
                                                 Price theo_mid, const std::string& reason,
                                                 const std::vector<OrderId>& batch_cancel_ids,
                                                 Price old_bid_px, Price old_ask_px) {
    if (!isRunning() || !isEnabled()) {
        return;
    }
    auto& cfg = config::Config::getInstance();
    // STRICT: user-entered desk size only (no config fallback) — mirrors the re-peg path.
    const int base_sz = mmEffOrderSizeInt(cfg);
    if (base_sz <= 0) {
        mmLogMmGateThrottled(
            "fire_and_track_no_user_size", "fire_and_track",
            "user-entered order_size missing/<=0 — refusing fire-and-track pair (no config fallback)");
        return;
    }
    const Quantity leg_qty = static_cast<double>(base_sz);
    const long long cap_ll = static_cast<long long>(std::max(1, mmEffMaxPositionInt(cfg)));

    // === SINGLE snapshot of local truth for the WHOLE decision (spec: read effective()
    // ONCE, no re-reads mid-decision). effective() is a local, non-blocking read in ENFORCE.
    const long long eff_net = mmEffectiveNetPoRounded();

    // Optional pessimistic tightener (default OFF — this changes Tim's net-only gate):
    // bias each side's basis by the worst-case STILL-UNCONFIRMED leg qty on that side, so
    // a side that could breach once its unconfirmed cancel loses the race is not re-added.
    const bool pessimistic = cfg.getBool("market_maker.inflight_pessimistic_gate", false);
    long long bid_basis = eff_net;   // placing a BUY grows long
    long long ask_basis = eff_net;   // placing a SELL grows short
    if (pessimistic) {
        const long long buy_worst = static_cast<long long>(std::llround(
            sumWorstCaseInflightLegQtyForSymbol(mmAxSymbol(), Side::BUY, nullptr)));
        const long long sell_worst = static_cast<long long>(std::llround(
            sumWorstCaseInflightLegQtyForSymbol(mmAxSymbol(), Side::SELL, nullptr)));
        bid_basis += buy_worst;
        ask_basis -= sell_worst;
    }
    // Reduce-only, driven purely by effective(): re-add a grow-side leg only while it stays
    // within cap. The reduce side is always allowed. mmPlaceReservedLeg's per-side gate
    // (also effective()-driven) remains the final authority.
    const bool want_bid = (bid_basis < cap_ll);
    const bool want_ask = (-ask_basis < cap_ll);

    const MmProductPlacementLeaseScope product_placement_lease(this);
    if (!product_placement_lease.leaseAcquired()) {
        if (utils::Logger::isInitialized()) {
            Main().logger()->info(
                "[MM_TT] sym={} stack={} phase=place_skipped reason=lease_denied ack_mode=pre",
                mmAxSymbol(), mmOrderRequestIdOrLegacy());
        }
        refreshLegacyTopOfBookTrackingFromVectors();
        return;
    }
    const auto stack_locks = mmAcquireAllStacksCycleLocks(mmAxSymbol(), this);
    mmSeedProductCycleNetSnapshot(this);

    // Concurrent batch (Option B): reserve+create BOTH legs with the wire deferred, then fire
    // them together in ~1 round-trip via Platform::placeOrdersConcurrent. Default OFF —
    // when off, each leg fires synchronously exactly as before.
    const bool batch_submit = cfg.getBool("market_maker.async_batch_submit", false);
    std::vector<OrderId> deferred_ids;

    int placed = 0;
    auto place_side = [&](Side side, Price target_px, bool want) {
        if (!want) {
            return;
        }
        OrderRequest req = (side == Side::BUY)
            ? OrderRequest::limit_buy(mmAxSymbol(), leg_qty, target_px, "mm_bid")
            : OrderRequest::limit_sell(mmAxSymbol(), leg_qty, target_px, "mm_ask");
        req.strategy_name = getName();
        OrderId oid = 0;
        const std::string cid = mmPlaceReservedLeg(side, target_px, leg_qty, req, cap_ll,
                                                   "fire_and_track_replace", batch_submit, &oid);
        if (!cid.empty()) {
            ++placed;
            if (batch_submit && oid != 0) {
                deferred_ids.push_back(oid);
            }
            if (utils::Logger::isInitialized()) {
                Main().logger()->info(
                    "[THEO_MOVE] fire-and-track place {}: leg={} px={} client_id={}",
                    batch_submit ? "queued(batch)" : "sent",
                    (side == Side::BUY) ? "bid" : "ask", target_px, cid);
            }
        }
    };
    // Reserve/create order: bid then ask (defer_wire=batch_submit so no inline HTTP yet).
    place_side(Side::BUY, tgt_bid, want_bid);
    place_side(Side::SELL, tgt_ask, want_ask);

    // MM_TT phase stamps (Fix 5): decompose the tick so we can see where the missing
    // time lives (queue/lease/reserve BEFORE the batch vs the batch RTT itself).
    // decision_done = reservations built, about to dispatch the wire batch.
    const std::int64_t tt_decision_done_ms = mmSteadyMillis();
    std::int64_t tt_batch_started_ms = 0;
    std::int64_t tt_batch_done_ms = 0;

    if (batch_submit && !batch_cancel_ids.empty()) {
        // Combined cancel+place batching. SELF-TRADE GUARD: combined batching loses
        // cancel-before-place ordering at the venue, so on a big theo jump the fresh leg can
        // cross a still-live old leg (new bid at/above old ask, or new ask at/below old bid) →
        // self-trade against our own not-yet-cancelled order. When that's possible, fall back
        // to cancels-batch-first (await) THEN places-batch (~2 RTT). Normal one-tick drifts
        // take the combined ~1 RTT path.
        const bool bid_crosses_old_ask =
            want_bid && old_ask_px > 0.0 && tgt_bid >= old_ask_px;
        const bool ask_crosses_old_bid =
            want_ask && old_bid_px > 0.0 && tgt_ask <= old_bid_px;
        const bool self_trade_risk = bid_crosses_old_ask || ask_crosses_old_bid;
        if (self_trade_risk) {
            if (utils::Logger::isInitialized()) {
                Main().logger()->info(
                    "[THEO_MOVE] combined-batch SELF-TRADE GUARD tripped sym={} "
                    "new_bid={:.6f} old_ask={:.6f} new_ask={:.6f} old_bid={:.6f} "
                    "-> cancels-first(await)+places (~2 RTT)",
                    mmAxSymbol(), tgt_bid, old_ask_px, tgt_ask, old_bid_px);
            }
            tt_batch_started_ms = mmSteadyMillis();
            Main().cancelOrdersConcurrent(batch_cancel_ids);  // awaits cancel completion
            if (!deferred_ids.empty()) {
                Main().placeOrdersConcurrent(deferred_ids);
            }
            tt_batch_done_ms = mmSteadyMillis();
        } else {
            // Preferred path: cancels + places in ONE curl-multi batch (~1 RTT, c,c,p,p).
            tt_batch_started_ms = mmSteadyMillis();
            Main().placeAndCancelConcurrent(deferred_ids, batch_cancel_ids);
            tt_batch_done_ms = mmSteadyMillis();
        }
    } else if (batch_submit && !deferred_ids.empty()) {
        // Places-only batch (legacy cancel routing already enqueued the cancels on the mover).
        tt_batch_started_ms = mmSteadyMillis();
        Main().placeOrdersConcurrent(deferred_ids);
        tt_batch_done_ms = mmSteadyMillis();
    }

    const std::int64_t tt_place_ms = mmSteadyMillis();
    const std::int64_t tt_feed_ms = mm_tt_feed_arrival_ms_.load(std::memory_order_relaxed);
    if (utils::Logger::isInitialized()) {
        // Phase deltas (only meaningful when the feed stamp is set). batch_rtt is
        // batch_done-batch_started; pre_batch is decision_done-feed (queue+lease+reserve).
        const std::int64_t batch_rtt_ms =
            (tt_batch_done_ms > 0 && tt_batch_started_ms > 0) ? (tt_batch_done_ms - tt_batch_started_ms) : -1;
        Main().logger()->info(
            "[MM_TT] sym={} stack={} phase=place_sent reason={} theo={:.6f} placed={} "
            "eff_net={} cap={} want_bid={} want_ask={} pessimistic={} feed_arrival_ms={} "
            "decision_done_ms={} batch_started_ms={} batch_done_ms={} batch_rtt_ms={} "
            "place_sent_ms={} tick_to_place_ms={} ack_mode=pre",
            mmAxSymbol(), mmOrderRequestIdOrLegacy(), reason, theo_mid, placed, eff_net, cap_ll,
            want_bid ? 1 : 0, want_ask ? 1 : 0, pessimistic ? 1 : 0, tt_feed_ms,
            tt_decision_done_ms, tt_batch_started_ms, tt_batch_done_ms, batch_rtt_ms,
            tt_place_ms, (tt_feed_ms > 0) ? (tt_place_ms - tt_feed_ms) : -1);
    }
    // Fire-and-track owns the tick end-to-end here; clear the MM_TT feed stamp so a later
    // async cancel-ack (which logs phase=cancel_confirmed) doesn't recompute a stale delta.
    mm_tt_feed_arrival_ms_.store(0, std::memory_order_relaxed);
    mm_tt_cancel_sent_ms_.store(0, std::memory_order_relaxed);
    refreshLegacyTopOfBookTrackingFromVectors();
}

void MakeMarketStrategy::maybeReconcileDeskCancelAckTimeouts() {
    if (!mmIsDeskManaged()) {
        return;
    }
    auto& cfg = config::Config::getInstance();
    const int timeout_ms = cfg.getMarketMakerCancelAckTimeoutMs();
    // Fire-and-track legs (CANCEL_SENT_UNCONFIRMED) escalate on their own knob so the
    // async-confirmation window is tunable independently of the ack-wait timeout.
    const int confirm_timeout_ms =
        std::max(1, cfg.getInt("market_maker.cancel_confirm_timeout_ms", 3000));
    const std::int64_t now = mmSteadyMillis();
    struct Timed {
        Side side;
        OrderId local_id;
        std::string xoid;
        const char* leg;
    };
    std::vector<Timed> timed_out;
    {
        std::lock_guard<std::mutex> lk(tracked_orders_mutex_);
        auto scan_vec = [&](std::vector<TrackedLeg>& vec, const char* leg) {
            for (auto& t : vec) {
                if (!mmLegStateIsCancelInFlight(t.state)) {
                    continue;
                }
                const int eff_timeout =
                    (t.state == MmLegState::CANCEL_SENT_UNCONFIRMED) ? confirm_timeout_ms : timeout_ms;
                if (now - t.state_entered_steady_ms < eff_timeout) {
                    continue;
                }
                timed_out.push_back(Timed{t.side, t.local_oid, t.exchange_oid, leg});
            }
        };
        scan_vec(tracked_bids_, "bid");
        scan_vec(tracked_asks_, "ask");
    }
    auto& rest = *Main().rest();
    for (const auto& w : timed_out) {
        if (w.local_id == 0) {
            continue;
        }
        if (utils::Logger::isInitialized()) {
            Main().logger()->warn(
                "[THEO_MOVE] cancel ack timeout: leg={} oid={} reconciling",
                w.leg,
                w.xoid.empty() ? std::to_string(static_cast<long long>(w.local_id)) : w.xoid);
        }
        bool still_open = false;
        if (!w.xoid.empty()) {
            if (mmPbEnforce()) {
                // ENFORCE: the mover never GETs. Consult the main-loop VenueOrdersCache; if
                // it is fresh, trust its oid set. If it is stale/missing, TRUST LOCAL cancel
                // intent (treat as no longer open) — consistent with the trust-local directive.
                const VenueOrdersSnapshot snap = VenueOrdersCache::instance().get(mmAxSymbol());
                if (snap.valid && snap.ageMs(VenueOrdersCache::nowSteadyMs()) <=
                                      mmVenueCacheMaxAgeMs()) {
                    still_open = snap.oidSet().count(w.xoid) > 0;
                } else {
                    still_open = false;
                }
            } else {
                const auto r = rest.getOrders();
                if (r.is_success) {
                    still_open = mmRestOrdersBodyContainsExchangeOid(r.body, w.xoid);
                }
            }
        }
        if (!still_open) {
            bool progressed = false;
            Side side_hit = w.side;
            const char* leg_hit = w.leg;
            MmDeskCancelIntent intent = MmDeskCancelIntent::None;
            {
                std::lock_guard<std::mutex> lk(tracked_orders_mutex_);
                TrackedLeg* hit = findTrackedByOrderId(w.local_id, &side_hit);
                if (!hit || !mmLegStateIsCancelInFlight(hit->state)) {
                    progressed = false;
                } else {
                    intent = hit->pending_cancel_intent;
                    leg_hit = (side_hit == Side::BUY) ? "bid" : "ask";
                    if (intent == MmDeskCancelIntent::DeskReseed) {
                        progressed = true;
                    } else {
                        hit->state = MmLegState::CANCELLED;
                        hit->local_oid = 0;
                        hit->exchange_oid.clear();
                        hit->pending_cancel_intent = MmDeskCancelIntent::None;
                        hit->desk_cancel_timeout_retry_sent = false;
                        hit->state_entered_steady_ms = mmSteadyMillis();
                        progressed = true;
                    }
                }
            }
            if (!progressed) {
                continue;
            }
            if (intent == MmDeskCancelIntent::DeskReseed) {
                eraseTrackedByOrderId(w.local_id);
                refreshLegacyTopOfBookTrackingFromVectors();
                continue;
            }
            // FireAndTrack: the replacement pair was already placed at cancel-send time.
            // The cancel confirmation (here, via the timeout backstop) ONLY resolves the
            // old tombstone (done above → CANCELLED); it must NOT enqueue a re-add.
            if (intent == MmDeskCancelIntent::FireAndTrack) {
                if (utils::Logger::isInitialized()) {
                    Main().logger()->info(
                        "[MM_TT] sym={} stack={} phase=cancel_confirmed leg={} src=timeout ack_mode=pre",
                        mmAxSymbol(), mmOrderRequestIdOrLegacy(), leg_hit ? leg_hit : "");
                }
                refreshLegacyTopOfBookTrackingFromVectors();
                continue;
            }
            // Route place-side via the per-AX mover so cancel-ack reconcile (which is itself
            // called from the timer thread inside processTrackedOrdersTheoMove → mm_cycle_mutex_)
            // doesn't fan out fresh REST calls on this thread.
            if (intent == MmDeskCancelIntent::FillOpposite) {
                enqueueFillRequoteOnMover();
            } else if (intent == MmDeskCancelIntent::TheoMove) {
                // Classic cancel->confirm->replace, resolved here via the TIMEOUT backstop
                // (the ack was slow/lost; venue-truth confirmed the order is gone). src=timeout
                // distinguishes this from the fast ack path (src=ack) in the MM_TT stream.
                if (utils::Logger::isInitialized()) {
                    Main().logger()->info(
                        "[MM_TT] sym={} stack={} phase=cancel_confirmed leg={} src=timeout "
                        "ack_mode=post",
                        mmAxSymbol(), mmOrderRequestIdOrLegacy(),
                        leg_hit ? leg_hit : "");
                }
                if (mmDeskPairCancelSettledForTheoReplace()) {
                    if (utils::Logger::isInitialized()) {
                        Main().logger()->info(
                            "[MM_TT] sym={} stack={} phase=replace_enqueued "
                            "reason=theo_move_after_cancel_ack src=timeout ack_mode=post — both "
                            "legs' cancels resolved, placing fresh pair",
                            mmAxSymbol(), mmOrderRequestIdOrLegacy());
                    }
                    enqueueQuoteCycleOnMover("theo_move");
                }
            } else {
                enqueueDeskTheoMoveAfterCancelAckOnMover(side_hit, std::string(leg_hit ? leg_hit : ""));
            }
            refreshLegacyTopOfBookTrackingFromVectors();
            continue;
        }
        bool retry_sent = false;
        bool fire_and_track = false;
        {
            std::lock_guard<std::mutex> lk(tracked_orders_mutex_);
            TrackedLeg* hit = findTrackedByOrderId(w.local_id, nullptr);
            if (!hit || !mmLegStateIsCancelInFlight(hit->state)) {
                continue;
            }
            fire_and_track = (hit->pending_cancel_intent == MmDeskCancelIntent::FireAndTrack);
            if (!hit->desk_cancel_timeout_retry_sent) {
                hit->desk_cancel_timeout_retry_sent = true;
                hit->state_entered_steady_ms = now;
                retry_sent = true;
                // Fire-and-track escalation (spec): a non-terminal cancel result → re-send the
                // cancel ONCE and WARN. The re-add already happened, so this only chases the
                // stale tombstone off the venue; it never gates or re-places.
                if (fire_and_track && utils::Logger::isInitialized()) {
                    Main().logger()->warn(
                        "[MM_ORDERS] CANCEL_UNCONFIRMED_RETRY sym={} leg={} oid={} — cancel not "
                        "confirmed within timeout, re-sending once (fire-and-track)",
                        mmAxSymbol(), w.leg,
                        w.xoid.empty() ? std::to_string(static_cast<long long>(w.local_id)) : w.xoid);
                }
            } else if (utils::Logger::isInitialized()) {
                if (fire_and_track) {
                    Main().logger()->error(
                        "[MM_ORDERS] CANCEL_UNCONFIRMED_UNRESOLVED sym={} leg={} oid={} — still "
                        "open after retry; marking for reconcile path (fire-and-track)",
                        mmAxSymbol(), w.leg,
                        w.xoid.empty() ? std::to_string(static_cast<long long>(w.local_id)) : w.xoid);
                } else {
                    Main().logger()->warn(
                        "[THEO_MOVE] cancel ack timeout: leg={} oid={} exchange still open after retry",
                        w.leg,
                        w.xoid.empty() ? std::to_string(static_cast<long long>(w.local_id)) : w.xoid);
                }
            }
        }
        if (retry_sent) {
            enqueueRestCancelOnMover(w.local_id, w.side,
                                     fire_and_track ? "fire_and_track_cancel_retry"
                                                    : "theo_move_cancel_retry");
        }
    }
}

void MakeMarketStrategy::deskTheoMoveAfterCancelAckPlaceOneLeg(Side side, const char* leg_name) {
    // Placement suppression: this after-cancel-ack survivor-leg re-peg is a PLACE site that
    // bypasses the runFullMmQuoteCycle gates, so it must honor the same single placement
    // predicate — otherwise (a) a volatility spike that is actively filling would keep
    // re-pegging the survivor leg straight back into the move (MWR pause), and (b) — the P1
    // gap this closes — a per-instrument HOLD (mm_orders_enabled=false) issued by
    // "Per-Instrument Cancel All" while a theo-move cancel-ack was in flight would re-place a
    // leg AFTER the hold was observed, leaving an orphan the cancel-all then has to sweep
    // (the §4.5 shape). The cancel that armed this re-peg already completed; returning now
    // simply leaves that leg pulled (cancels/reconcile are never gated — only placement).
    // Mover-thread-only read of cfg/mm_mwr_.
    auto& cfg_gate = config::Config::getInstance();
    if (!mmPlacementAllowedForSymbol(cfg_gate)) {
        const char* why = !mmMarketMakerOrdersPlacementAllowed(cfg_gate)
                              ? "mm_orders_enabled=false / strategy off (per-instrument HOLD) "
                                "— suppressing after-cancel-ack re-peg"
                              : "MWR volatility / fast-market pause active "
                                "— suppressing after-cancel-ack re-peg";
        mmLogMmGateThrottled("placement_suppressed_after_cancel_ack",
                             std::string("theo_move:") + (leg_name ? leg_name : "?"), why);
        return;
    }
    auto place_decide = [&](const std::string& reason, MmLegState st) {
        if (!utils::Logger::isInitialized()) {
            return;
        }
        const char* st_name = "UNKNOWN";
        switch (st) {
            case MmLegState::IDLE: st_name = "IDLE"; break;
            case MmLegState::ADOPTED: st_name = "ADOPTED"; break;
            case MmLegState::CANCEL_PENDING: st_name = "CANCEL_PENDING"; break;
            case MmLegState::CANCEL_SENT_UNCONFIRMED: st_name = "CANCEL_SENT_UNCONFIRMED"; break;
            case MmLegState::CANCELLED: st_name = "CANCELLED"; break;
            case MmLegState::PLACE_PENDING: st_name = "PLACE_PENDING"; break;
            case MmLegState::FILLED: st_name = "FILLED"; break;
            case MmLegState::PAUSED: st_name = "PAUSED"; break;
        }
        Main().logger()->info(
            "[THEO_MOVE_PLACE_DECIDE] sym={} side={} reason='{}' leg_state={} reload_cycle={}",
            mmAxSymbol(),
            sideToString(side),
            reason,
            st_name,
            mm_reload_cycles_used_);
    };

    auto& cfg = config::Config::getInstance();
    auto theo_opt = mmReadTransformedTheo();
    if (!theo_opt) {
        place_decide("gated_by_no_theo", MmLegState::IDLE);
        refreshLegacyTopOfBookTrackingFromVectors();
        return;
    }
    const Price theo_mid = *theo_opt;
    double quote_tick = mmEffResolvedQuoteTick(cfg);
    if (!(quote_tick > 0.0) || !std::isfinite(quote_tick)) {
        place_decide("gated_by_unresolved_quote_tick", MmLegState::IDLE);
        refreshLegacyTopOfBookTrackingFromVectors();
        return;
    }
    const int eff_spread_pair = mmEffBidSpreadTicks(cfg);
    const int resting_extra_ticks = cfg.getMarketMakerRestingDepthExtraTicks();
    const double basis = cfg.getMarketMakerBasis();
    const Price adjusted_theo = mmApplyPricerSnapshotTransform(theo_mid) + basis;
    int bid_spread_bps = eff_spread_pair;
    int ask_spread_bps = eff_spread_pair;
    mmCapSpreadBpsToDeskPlacedLadder(adjusted_theo, bid_spread_bps, ask_spread_bps);
    Price raw_bid = 0.0;
    Price raw_ask = 0.0;
    computeInventorySkewedRawBidAsk(adjusted_theo, bid_spread_bps, ask_spread_bps, quote_tick, raw_bid, raw_ask);
    if (resting_extra_ticks > 0) {
        raw_bid -= static_cast<double>(resting_extra_ticks) * quote_tick;
        raw_ask += static_cast<double>(resting_extra_ticks) * quote_tick;
    }
    Price tgt_bid = 0.0;
    Price tgt_ask = 0.0;
    finalizeMmPairOnTickGrid(raw_bid, raw_ask, bid_spread_bps, ask_spread_bps, quote_tick, tgt_bid, tgt_ask);
    const Price target_px = (side == Side::BUY) ? tgt_bid : tgt_ask;
    // STRICT: order size is the user-entered desk stack size only. No config fallback —
    // see `mmEffOrderSizeInt`. If the user-entered size is missing we MUST NOT
    // substitute a default (that's how C++ ended up "moving" orders at 100 when the
    // user submitted 10). Refuse the leg loudly so the misconfiguration is visible.
    const int base_sz = mmEffOrderSizeInt(cfg);
    if (base_sz <= 0) {
        mmLogMmGateThrottled(
            "theo_move_leg_no_user_size",
            std::string("theo_move:") + leg_name,
            "user-entered order_size missing/<=0 — refusing leg (no config fallback)");
        place_decide("gated_by_no_user_order_size", MmLegState::IDLE);
        refreshLegacyTopOfBookTrackingFromVectors();
        return;
    }
    Quantity leg_qty = static_cast<double>(base_sz);
    OrderRequest req = (side == Side::BUY) ? OrderRequest::limit_buy(mmAxSymbol(), leg_qty, target_px, "mm_bid")
                                           : OrderRequest::limit_sell(mmAxSymbol(), leg_qty, target_px, "mm_ask");
    req.strategy_name = getName();
    MmLegState state_before = MmLegState::IDLE;
    {
        std::lock_guard<std::mutex> lk(tracked_orders_mutex_);
        auto& vec = (side == Side::BUY) ? tracked_bids_ : tracked_asks_;
        if (!vec.empty()) {
            state_before = vec.front().state;
        }
    }
    place_decide("entering_place_after_cancel_ack", state_before);
    const MmProductPlacementLeaseScope product_placement_lease(this);
    if (!product_placement_lease.leaseAcquired()) {
        place_decide("gated_by_product_placement_lease_denied", state_before);
        refreshLegacyTopOfBookTrackingFromVectors();
        return;
    }
    const auto stack_locks = mmAcquireAllStacksCycleLocks(mmAxSymbol(), this);
    mmSeedProductCycleNetSnapshot(this);
    const int max_po_eff = mmEffMaxPositionInt(cfg);
    const long long cap_ll =
        static_cast<long long>(std::max(1, max_po_eff));
    // Reserve the PLACE_PENDING leg + pending-accept BEFORE submit so the inline
    // ORDER_ACCEPTED (dispatched during submit_order on this thread) matches it and
    // flips it to ADOPTED — previously this leg was registered AFTER submit and got
    // pinned in PLACE_PENDING forever (the ce60e217 CYCLE_LOCK).
    const std::string cid = mmPlaceReservedLeg(side, target_px, leg_qty, req, cap_ll, "theo_move_replace");
    if (!cid.empty()) {
        if (utils::Logger::isInitialized()) {
            Main().logger()->info("[THEO_MOVE] place sent: leg={} px={} client_id={}", leg_name, target_px, cid);
        }
    } else {
        place_decide(
            "gated_by_empty_client_id running=" + std::string(isRunning() ? "1" : "0") +
                " enabled=" + std::string(isEnabled() ? "1" : "0") +
                " theo=" + std::to_string(theo_mid) + " target=" + std::to_string(target_px),
            state_before);
    }
}

void MakeMarketStrategy::tryDeskFillRequoteF4PlaceFreshPair() {
    if (!isRunning() || !isEnabled()) {
        return;
    }
    auto fill_decide = [&](const std::string& reason) {
        if (!utils::Logger::isInitialized()) {
            return;
        }
        Main().logger()->info(
            "[FILL_REQUOTE_DECIDE] sym={} reason='{}' new_position={}",
            mmAxSymbol(),
            reason,
            position_state_.net_position_qty);
    };
    const std::string ax = mmAxSymbol();
    struct AxFillRequoteGuard {
        std::string ax;
        bool held{false};
        ~AxFillRequoteGuard() {
            if (held) {
                MakeMarketStrategy::mmReleaseAxFillRequoteInflight(ax);
            }
        }
    } inflight_guard{ax, false};
    if (!mmAcquireAxFillRequoteInflight(ax)) {
        fill_decide("gated_by_ax_fill_requote_inflight");
        return;
    }
    inflight_guard.held = true;

    std::lock_guard<std::recursive_mutex> cycle_lock(mm_cycle_mutex_);
    if (!mmIsDeskManaged()) {
        fill_decide("not_desk_managed");
        return;
    }
    // MWR pause: suppress all placement until resume (confirmed semantics).
    // Fill-requote re-places a fresh skewed pair after one leg fills; it bypasses the
    // runFullMmQuoteCycle MWR gate. During a volatility pause we must NOT re-arm a pair back
    // into the move (precisely the "spike keeps filling" case the breaker exists to stop). The
    // venue reconcile in mmDrainPendingFillRequote already ran before us; only placement is gated.
    if (mmMwrIsPausedNow()) {
        fill_decide("gated_by_mwr_pause");
        return;
    }
    bool layout_ok = false;
    {
        std::lock_guard<std::mutex> lk(tracked_orders_mutex_);
        const auto side_filled = [](const std::vector<TrackedLeg>& vec) {
            return std::any_of(vec.begin(), vec.end(), [](const TrackedLeg& t) {
                return t.state == MmLegState::FILLED;
            });
        };
        const auto opposite_settled_after_fill = [](const std::vector<TrackedLeg>& vec) {
            return std::any_of(vec.begin(), vec.end(), [](const TrackedLeg& t) {
                return t.state == MmLegState::CANCELLED ||
                       mmLegStateIsCancelInFlight(t.state);
            });
        };
        const bool bid_filled = side_filled(tracked_bids_);
        const bool ask_filled = side_filled(tracked_asks_);
        layout_ok = (bid_filled && opposite_settled_after_fill(tracked_asks_)) ||
                    (ask_filled && opposite_settled_after_fill(tracked_bids_));
    }
    long long net_po_layout = 0;
    {
        std::lock_guard<std::mutex> ps_lk(position_state_mutex_);
        net_po_layout = static_cast<long long>(std::llround(position_state_.net_position_qty));
    }
    const long long cap_layout = static_cast<long long>(
        std::max(1, mmEffMaxPositionInt(config::Config::getInstance())));
    // Tim: at |net|>=max only the reduce side may quote — do not block fill_requote on the
    // "filled + opposite cancelled" layout gate (that gate is for two-sided refresh below cap).
    const bool reduce_only_at_cap = std::llabs(net_po_layout) >= cap_layout;
    if (!layout_ok && !reduce_only_at_cap) {
        fill_decide("gated_by_layout_no_filled_cancelled_pair");
        return;
    }

    (void)config::Config::getInstance().reloadPrimaryConfigFromDisk();
    syncReloadLimitStateFromConfig();
    auto& cfg = config::Config::getInstance();
    const bool gate_mm = mmMarketMakerOrdersPlacementAllowed(cfg);
    // === VENUE-vs-LOCAL RECONCILE (2026-05-22, post SILVER overtrade) =========
    // Same defense as runFullMmQuoteCycle: even though processFill just
    // advanced local NetPo for THIS fill, other fills can have landed at the
    // venue without /fills delivering them (proven 2026-05-22 00:00). Force
    // a sync read of /positions and abort if local diverges from venue.
    if (!mmReconcileVenueOrAbort("fill_requote")) {
        return;
    }
    mmWaitReconcileGateBeforePlace("fill_requote");
    const bool can_quote_bid_side = shouldQuoteSide(Side::BUY, true);
    const bool can_quote_ask_side = shouldQuoteSide(Side::SELL, true);
    const int max_reload_cfg = mmEffMaxReloadCyclesInt(cfg);
    const bool gate_reload = !(max_reload_cfg > 0 && mm_reload_cycles_used_ >= max_reload_cfg);
    auto theo_opt = mmReadTransformedTheo();

    // === REDUCE-ONLY-AT-CAP IS NOT A FREEZE ===
    // The previous code bailed entirely when EITHER side failed shouldQuoteSide
    // (`gate_po = bid_ok && ask_ok`). That is exactly the user-reported failure mode:
    // "when one side of an order pair fills, NO NEW ORDER PAIR GOES BASED ON THE SKEWED
    // COMPUTATIONS." If a fill pushed |net| to the cap, the next requote correctly determined
    // we cannot grow the side that is at the cap — but the all-or-nothing bail meant we ALSO
    // refused to place the reduce-only side, leaving the stack stuck with one filled leg and
    // one cancelled leg forever.
    //
    // We now only bail if NEITHER side is quotable (i.e., truly nothing to do). Per-side
    // `shouldQuoteSide` + `mmSubmitOrderWithProductCapGate` (net-only, no open-order term)
    // decide each leg independently.
    if (!can_quote_bid_side && !can_quote_ask_side) {
        mm_desk_stack_paused_ = true;
        fill_decide("gated_by_max_position_both_sides_blocked");
        if (utils::Logger::isInitialized()) {
            Main().logger()->info(
                "[FILL_REQUOTE] both sides blocked by max_position: pausing stack "
                "net_po={} max_po={} (this is rare — typically reduce-only side stays open)",
                static_cast<long long>(std::llround(position_state_.net_position_qty)),
                mmEffMaxPositionInt(cfg));
        }
        return;
    }
    if (!gate_reload) {
        mm_desk_stack_paused_ = true;
        fill_decide("gated_by_max_reload");
        if (utils::Logger::isInitialized()) {
            Main().logger()->info("[FILL_REQUOTE] gated by max_reload_cycles={}", max_reload_cfg);
        }
        return;
    }
    if (!gate_mm || !theo_opt) {
        mm_desk_stack_paused_ = true;
        fill_decide("gated_by_mm_disabled_or_no_theo");
        if (utils::Logger::isInitialized()) {
            Main().logger()->warn("[FILL_REQUOTE] theo unavailable or mm disabled: pausing stack");
        }
        return;
    }

    const Price theo_mid = *theo_opt;
    double quote_tick = mmEffResolvedQuoteTick(cfg);
    if (!(quote_tick > 0.0) || !std::isfinite(quote_tick)) {
        mm_desk_stack_paused_ = true;
        fill_decide("gated_by_unresolved_quote_tick");
        if (utils::Logger::isInitialized()) {
            Main().logger()->warn(
                "[FILL_REQUOTE] unresolved quote_tick for ax={} — pausing stack until catalog or leg tick_size",
                mmAxSymbol());
        }
        return;
    }
    const int eff_spread_pair = mmEffBidSpreadTicks(cfg);
    const int resting_extra_ticks = cfg.getMarketMakerRestingDepthExtraTicks();
    const double basis = cfg.getMarketMakerBasis();
    const Price adjusted_theo = mmApplyPricerSnapshotTransform(theo_mid) + basis;
    int bid_spread_bps = eff_spread_pair;
    int ask_spread_bps = eff_spread_pair;
    mmCapSpreadBpsToDeskPlacedLadder(adjusted_theo, bid_spread_bps, ask_spread_bps);
    Price raw_bid = 0.0;
    Price raw_ask = 0.0;
    computeInventorySkewedRawBidAsk(adjusted_theo, bid_spread_bps, ask_spread_bps, quote_tick, raw_bid, raw_ask);
    if (resting_extra_ticks > 0) {
        raw_bid -= static_cast<double>(resting_extra_ticks) * quote_tick;
        raw_ask += static_cast<double>(resting_extra_ticks) * quote_tick;
    }
    Price tgt_bid = 0.0;
    Price tgt_ask = 0.0;
    finalizeMmPairOnTickGrid(raw_bid, raw_ask, bid_spread_bps, ask_spread_bps, quote_tick, tgt_bid, tgt_ask);

    const long long net_po_pre_place =
        static_cast<long long>(std::llround(position_state_.net_position_qty));
    const long long cap_pre_place =
        static_cast<long long>(std::max(1, mmEffMaxPositionInt(cfg)));
    if (std::llabs(net_po_pre_place) >= cap_pre_place) {
        if (!mmEnsureAtCapReduceSideVenueClean(
                ax, net_po_pre_place, cap_pre_place, "fill_requote_pre_place", this)) {
            fill_decide("gated_by_excess_reduce_side_on_venue_at_cap");
            return;
        }
    }

    if (utils::Logger::isInitialized()) {
        fill_decide("entering_place_pair_after_fill");
        Main().logger()->info(
            "[FILL_REQUOTE] placing fresh pair bid_px={:.6f} ask_px={:.6f} reload_cycle={}",
            tgt_bid,
            tgt_ask,
            mm_reload_cycles_used_);
    }

    (void)cfg.bumpMarketMakerCurrentReloadCountForInstrumentFill();
    syncReloadLimitStateFromConfig();

    // STRICT: only the user-entered desk stack size is permitted. No config fallback.
    // See `mmEffOrderSizeInt`. If the user-entered size is missing the requote MUST
    // refuse, not silently fabricate a quantity from `market_maker.order_size`.
    const int base_sz = mmEffOrderSizeInt(cfg);
    if (base_sz <= 0) {
        mmLogMmGateThrottled(
            "fill_requote_no_user_size",
            "fill_requote",
            "user-entered order_size missing/<=0 — refusing reload (no config fallback)");
        fill_decide("gated_by_no_user_order_size");
        return;
    }
    Quantity leg_qty = static_cast<double>(base_sz);

    // Tim spec (2026-05-26): placement uses net_po only — open/resting orders are excluded
    // from the add-side check. Per-side shouldQuoteSide already encodes reduce-only at cap.
    const long long net_po_ll = static_cast<long long>(std::llround(position_state_.net_position_qty));
    const int max_po_eff_fr = mmEffMaxPositionInt(cfg);
    const long long max_ll_fr = static_cast<long long>(std::max(1, max_po_eff_fr));
    const bool place_bid_fr = can_quote_bid_side;
    const bool place_ask_fr = can_quote_ask_side;
    if (!place_bid_fr || !place_ask_fr) {
        Main().logger()->info(
            "[FILL_REQUOTE] net-only sided-skip strategy={} sym={} net_po={} max_po={} "
            "can_quote_bid={} can_quote_ask={} place_bid={} place_ask={} "
            "(at-cap: grow side off per shouldQuoteSide; reduce-only side still placed)",
            getName(),
            mmAxSymbol(),
            net_po_ll,
            max_po_eff_fr,
            can_quote_bid_side ? 1 : 0,
            can_quote_ask_side ? 1 : 0,
            place_bid_fr ? 1 : 0,
            place_ask_fr ? 1 : 0);
    }

    const MmProductPlacementLeaseScope product_placement_lease(this);
    if (!product_placement_lease.leaseAcquired()) {
        fill_decide("gated_by_product_placement_lease_denied");
        return;
    }
    const auto stack_locks = mmAcquireAllStacksCycleLocks(mmAxSymbol(), this);
    mmSeedProductCycleNetSnapshot(this);
    const long long cap_ll_fr = max_ll_fr;

    std::string cid_b;
    std::string cid_a;

    int pending_reserved = 0;
    if (place_bid_fr) {
        ++pending_reserved;
    }
    if (place_ask_fr) {
        ++pending_reserved;
    }
    if (pending_reserved > 0) {
        mm_pending_accepts_ += pending_reserved;
        mm_pending_accepts_last_change_ms_.store(
            std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now().time_since_epoch())
                .count(),
            std::memory_order_relaxed);
    }

    {
        std::lock_guard<std::mutex> lk(tracked_orders_mutex_);
        tracked_bids_.clear();
        tracked_asks_.clear();
    }

    auto rollback_one_pending_accept = [this]() {
        if (mm_pending_accepts_ > 0) {
            --mm_pending_accepts_;
            mm_pending_accepts_last_change_ms_.store(
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::steady_clock::now().time_since_epoch())
                    .count(),
                std::memory_order_relaxed);
        }
    };

    auto track_place_pending_if_needed = [&](Side side, const std::string& cid, Price px) {
        if (cid.empty()) {
            return;
        }
        std::lock_guard<std::mutex> lk(tracked_orders_mutex_);
        auto& vec = (side == Side::BUY) ? tracked_bids_ : tracked_asks_;
        for (const auto& t : vec) {
            if (t.state == MmLegState::ADOPTED && t.local_oid != 0) {
                return;
            }
        }
        TrackedLeg leg;
        leg.state = MmLegState::PLACE_PENDING;
        leg.pending_place_client_id = cid;
        leg.exchange_px = px;
        leg.qty = leg_qty;
        leg.remaining_qty = leg_qty;
        leg.side = side;
        leg.state_entered_steady_ms = mmSteadyMillis();
        vec.push_back(leg);
    };

    if (place_bid_fr) {
        OrderRequest req_b = OrderRequest::limit_buy(mmAxSymbol(), leg_qty, tgt_bid, "mm_bid");
        req_b.strategy_name = getName();
        cid_b = mmSubmitOrderWithProductCapGate(
            this, req_b, Side::BUY, leg_qty, cap_ll_fr, "fill_requote");
        if (cid_b.empty()) {
            rollback_one_pending_accept();
        } else {
            track_place_pending_if_needed(Side::BUY, cid_b, tgt_bid);
        }
    }
    if (place_ask_fr) {
        OrderRequest req_a = OrderRequest::limit_sell(mmAxSymbol(), leg_qty, tgt_ask, "mm_ask");
        req_a.strategy_name = getName();
        cid_a = mmSubmitOrderWithProductCapGate(
            this, req_a, Side::SELL, leg_qty, cap_ll_fr, "fill_requote");
        if (cid_a.empty()) {
            rollback_one_pending_accept();
        } else {
            track_place_pending_if_needed(Side::SELL, cid_a, tgt_ask);
        }
    }
    // Submit-failure is only an error when we intended to place that side and `submit_order`
    // returned an empty cid. A side intentionally suppressed by the cap-aware gate above is
    // not a failure.
    const bool bid_failed = place_bid_fr && cid_b.empty();
    const bool ask_failed = place_ask_fr && cid_a.empty();
    if (bid_failed || ask_failed) {
        Main().logger()->error(
            "[STRATEGY:{}] fill_requote submit error bid_attempted={} bid_cid_ok={} "
            "ask_attempted={} ask_cid_ok={}",
            getName(),
            place_bid_fr ? 1 : 0,
            cid_b.empty() ? 0 : 1,
            place_ask_fr ? 1 : 0,
            cid_a.empty() ? 0 : 1);
    }
    if (cid_b.empty() && cid_a.empty()) {
        // Nothing got out — bail. (Either both sides were cap-suppressed, or both submits failed.)
        return;
    }
    if (!cid_b.empty()) {
        registerMmOrderForReloadCountBump(cid_b, "fill_requote_pair_bid");
    }
    if (!cid_a.empty()) {
        registerMmOrderForReloadCountBump(cid_a, "fill_requote_pair_ask");
    }

    resetMmSessionFillTracking(leg_qty, leg_qty);
    refreshLegacyTopOfBookTrackingFromVectors();
}

bool MakeMarketStrategy::adoptBootstrappedTrackedOrder(OrderId order_id,
                                                       const std::string& exchange_oid,
                                                       Side side,
                                                       Price price,
                                                       Quantity qty) {
    if (order_id == 0 || qty <= 0.0) {
        return false;
    }
    addOrUpdateTrackedOrder(order_id, exchange_oid, side, price, qty);
    noteAdoptedWorkingOrder(order_id, mmAxSymbol());
    passive_until_first_manual_order_ = false;
    refreshLegacyTopOfBookTrackingFromVectors();
    Main().logger()->info("[STRATEGY:{}] Bootstrapped tracked order: oid={} side={} price={} qty={}",
                          getName(),
                          exchange_oid.empty() ? std::to_string(order_id) : exchange_oid,
                          sideToString(side),
                          price,
                          qty);
    return true;
}

void MakeMarketStrategy::syncInventoryFromExchangePortfolio(bool force) {
    auto& cfg = config::Config::getInstance();
    if (!cfg.isMarketMakerEnabled()) {
        return;
    }

    if (!force) {
        const std::int64_t last_ms = last_fill_time_ms_.load(std::memory_order_acquire);
        if (last_ms > 0) {
            const std::int64_t now_ms =
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::steady_clock::now().time_since_epoch())
                    .count();
            const std::int64_t delta = now_ms - last_ms;
            if (delta >= 0 && delta < 2000) {
                Main().logger()->warn(
                    "[STRATEGY:{}] syncInventoryFromExchangePortfolio: skipping REST overwrite, "
                    "fill was {}ms ago (local net={:.0f})",
                    getName(),
                    delta,
                    position_state_.net_position_qty);
                return;
            }
        }

        // === ELAPSED-TIME THROTTLE (2026-05-07 freeze-fix follow-up) =====================
        // Pre-fix: this function ran a synchronous `Main().portfolio()->refresh()` REST call
        // (~150-250ms) on every drift-met theo_move, even when the previous refresh was
        // moments ago. With min_drift=1 producing back-to-back moves under choppy theo,
        // that's 230ms of dead pre-amble per move on the mover thread (user log timeline:
        // 18:08:21.806 enqueue → 18:08:22.453 first work-log = 647ms gap, of which most was
        // here).
        //
        // Post-fix: cache the last REST timestamp; skip if recent. Ground-truth callers
        // (post-fill, recovery paths) pass `force=true`. 3000ms is tight enough to catch a
        // venue position drift before it invalidates max_position math, loose enough to drop
        // the tax from drift-driven cancel-replace bursts.
        const std::int64_t now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                        std::chrono::steady_clock::now().time_since_epoch())
                                        .count();
        const std::int64_t last_sync = mm_inventory_sync_last_ms_.load(std::memory_order_acquire);
        constexpr std::int64_t kInventorySyncTtlMs = 3000;
        if (last_sync > 0 && now_ms - last_sync < kInventorySyncTtlMs) {
            // Cached — let the strategy use whatever NetPo it already has. The portfolio
            // cache itself is independently refreshed by `mmRefreshNetPositionFromPortfolioCache`'s
            // background thread, so we won't go stale beyond that worker's own throttle.
            return;
        }
    }

    // Source of truth: Architect exchange REST — refresh positions before seeding strategy inventory.
    Main().portfolio()->refresh();
    mm_inventory_sync_last_ms_.store(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now().time_since_epoch())
            .count(),
        std::memory_order_release);

    const std::string lookup = mmAxSymbol();

    auto opt = portfolio::PortfolioManager::getInstance().getPosition(lookup);
    if (!opt.has_value() || !opt->isOpen()) {
        {
            // CONCURRENCY (fix C4): serialize with processFill / cap-gate reads.
            std::lock_guard<std::mutex> ps_lk(position_state_mutex_);
            position_state_.net_position_qty = 0.0;
            position_state_.net_position_usd = 0.0;
        }
        Main().logger()->info(
            "[STRATEGY:{}] Inventory sync (exchange REST): no open position for '{}' — NetPo=0",
            getName(), lookup);
        return;
    }

    const auto& p = *opt;
    const Quantity signed_qty = p.isLong() ? p.quantity : -p.quantity;
    const Price ref_px = (p.mark_price > 0.0) ? p.mark_price : p.entry_price;

    {
        // CONCURRENCY (fix C4): serialize this write against processFill's fill write
        // and the cap-gate's snapshot read (both take position_state_mutex_).
        std::lock_guard<std::mutex> ps_lk(position_state_mutex_);
        position_state_.net_position_qty = signed_qty;
        position_state_.net_position_usd = signed_qty * ref_px;
    }

    const long long net_r = static_cast<long long>(std::llround(signed_qty));
    const bool first_log = std::isnan(inventory_sync_last_logged_net_);
    const bool qty_moved =
        !first_log && std::abs(signed_qty - inventory_sync_last_logged_net_) > 0.5;
    const bool ref_moved =
        !first_log
        && (std::abs(ref_px - inventory_sync_last_logged_ref_) > 1e-8
            || (ref_px > 0.0) != (inventory_sync_last_logged_ref_ > 0.0));
    if (first_log || qty_moved || ref_moved) {
        inventory_sync_last_logged_net_ = signed_qty;
        inventory_sync_last_logged_ref_ = ref_px;
        std::cout << "[MM_SYNC] After exchange REST refresh, position '" << lookup
                  << "': signed_qty=" << signed_qty
                  << " ref_px=" << ref_px << " -> NetPo~=" << net_r
                  << " | skew (when |NetPo|<max_position) = floor(|NetPo|/adjust_position)*sign(NetPo)*adjust_ticks*price_tick "
                     "(same on bid and offer; +NetPo lowers both, -NetPo raises both — symmetric)\n"
                  << std::flush;

        Main().logger()->info(
            "[STRATEGY:{}] Inventory seeded from exchange (REST): symbol={} signed_qty={} ref_px={} net_usd={}",
            getName(), lookup, signed_qty, ref_px, position_state_.net_position_usd);
    }
}

bool MakeMarketStrategy::mmReconcileVenueOrAbort(const char* reason) {
    // ENFORCE: PositionBook is the local source of truth; this synchronous venue
    // cross-check REST is neutralized (the 30s snapshot self-corrects any drift).
    // off/shadow run the full legacy check below, unchanged.
    if (mmPbEnforce()) {
        return true;
    }
    // Synchronously cross-check local NetPo (driven by fill events) against
    // the venue's positions REST truth. This is the post-2026-05-22-overtrade
    // safety net: when the REST /fills poll returns empty while fills are
    // actually occurring at the venue, the local NetPo stays stale and the
    // cap-gate happily lets new orders through. Layering a synchronous
    // /positions read on top closes that hole because /positions and /fills
    // can lag independently — the union of "either source says we're at or
    // past cap" is strictly safer than trusting only one.
    //
    // Policy:
    //   1. Force a synchronous portfolio refresh if the cache is stale
    //      (>1500ms old) OR the symbol is missing entirely from the cache.
    //   2. Read venue NetPo. If refresh failed and we have no cached value,
    //      ABORT the cycle (cannot verify -> safest action is don't place).
    //   3. Compare to local. If they agree, proceed.
    //   4. If they disagree, adopt the larger-magnitude value into local
    //      (so we err on the cap-protective side), log loudly, and ABORT
    //      this cycle. The next cycle will re-check; if /positions was
    //      transiently wrong it self-heals, if it was right we just avoided
    //      a breach.
    const std::string ax_sym = mmAxSymbol();
    if (ax_sym.empty()) {
        return true;  // Nothing to reconcile against.
    }

    constexpr std::int64_t kCacheStaleMs = 1500;

    auto read_venue = [&]() -> std::pair<bool /*have*/, long long /*signed_qty*/> {
        auto opt = portfolio::PortfolioManager::getInstance().getPosition(ax_sym);
        if (!opt.has_value()) {
            return {false, 0};
        }
        if (!opt->isOpen()) {
            return {true, 0};  // explicitly flat
        }
        const double q = opt->isLong() ? opt->quantity : -opt->quantity;
        return {true, static_cast<long long>(std::llround(q))};
    };

    auto cache_age_ms = [&]() -> std::int64_t {
        auto opt = portfolio::PortfolioManager::getInstance().getPosition(ax_sym);
        if (!opt.has_value()) {
            return std::numeric_limits<std::int64_t>::max();
        }
        const auto upd_ns = opt->updated_at.count();
        if (upd_ns <= 0) {
            return std::numeric_limits<std::int64_t>::max();
        }
        const std::int64_t upd_ms = upd_ns / 1'000'000LL;
        const std::int64_t wall_now_ms =
            std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch())
                .count();
        return wall_now_ms - upd_ms;
    };

    // Quick local read so we can decide whether to FORCE a refresh.
    long long local_net_pre = 0;
    {
        std::lock_guard<std::mutex> lk(position_state_mutex_);
        local_net_pre = static_cast<long long>(std::llround(position_state_.net_position_qty));
    }

    // === MAX-POSITION BREACH FIX (2026-05-22 Joe XAG breach) ====================
    // Original behaviour refreshed only when `cache_age_ms() > kCacheStaleMs`. In
    // the Joe XAG-PERP breach (`Max Position Breach.txt` @ 13:25:20.286) the
    // cache was 138 ms old (fresh), so no refresh fired — and yet a stale-cached
    // local NetPo was about to fight a venue position that had already moved.
    // Even when the cache *looks* fresh, the cap-projection is about to send
    // real orders into the venue: it costs ~1 REST hop to confirm. Force a
    // refresh whenever local NetPo is at/over the boundary that matters here,
    // i.e. anywhere near `max_position` on either side. The throttle below
    // keeps the worst case bounded (≤1 forced refresh per 750 ms per stack).
    static std::unordered_map<std::string, std::int64_t> s_force_refresh_last_ms;
    static std::mutex s_force_refresh_mu;
    constexpr std::int64_t kForceRefreshThrottleMs = 750;
    const int cap_for_refresh = mmEffMaxPositionInt(config::Config::getInstance());
    const bool near_cap =
        cap_for_refresh > 0 &&
        std::llabs(local_net_pre) + 1 >= static_cast<long long>(cap_for_refresh);
    const std::int64_t now_ms_rv = std::chrono::duration_cast<std::chrono::milliseconds>(
                                       std::chrono::steady_clock::now().time_since_epoch())
                                       .count();
    bool force_refresh = false;
    if (near_cap) {
        std::lock_guard<std::mutex> lk(s_force_refresh_mu);
        auto& slot = s_force_refresh_last_ms[ax_sym];
        if (slot == 0 || now_ms_rv - slot >= kForceRefreshThrottleMs) {
            slot = now_ms_rv;
            force_refresh = true;
        }
    }

    bool refresh_attempted = false;
    bool refresh_ok = false;
    if (force_refresh || cache_age_ms() > kCacheStaleMs) {
        refresh_attempted = true;
        try {
            if (auto* pf = Main().portfolio()) {
                pf->refresh();
                refresh_ok = true;
            }
        } catch (...) {
            refresh_ok = false;
        }
    }

    auto [have_venue, venue_net] = read_venue();
    long long local_net = 0;
    {
        std::lock_guard<std::mutex> lk(position_state_mutex_);
        local_net = static_cast<long long>(std::llround(position_state_.net_position_qty));
    }

    if (!have_venue) {
        // `read_venue` returned no entry. Two very different causes
        // collapse to the same `{false, 0}` return value and must be
        // disambiguated here:
        //
        //   (a) `pf->refresh()` succeeded (or no refresh was needed
        //       because the cache is still fresh) — the venue WAS asked
        //       and the symbol simply isn't in the response. That is
        //       the venue saying "no open position for this symbol",
        //       i.e. FLAT. Treat it as venue_net=0 and proceed. This is
        //       the same interpretation `syncInventoryFromExchangePortfolio`
        //       uses at init time. Treating it as "unavailable" bricks
        //       every fresh stack: the first theo_move after adoption
        //       calls runFullMmQuoteCycle → here → ABORT, so the orders
        //       are seeded by gateway-adopt but never re-pegged on theo,
        //       which is exactly the 2026-05-21 HL XAU-PERP report
        //       ("MM Live Feed shows HL up, theo moves, working quote
        //       does not").
        //   (b) `pf->refresh()` was attempted and threw (REST/network
        //       error) — we have no trustworthy venue truth. ABORT.
        //
        // The original docstring at the top of this function actually
        // specified (a)+(b) correctly ("ABORT if refresh failed"); the
        // implementation just over-aborted.
        if (!refresh_attempted || refresh_ok) {
            have_venue = true;
            venue_net = 0;
        } else {
            Main().logger()->error(
                "[STRATEGY:{}] MM_VENUE_UNAVAILABLE reason={} ax={} refresh_attempted={} refresh_ok={} "
                "local_net={} — ABORTING this place cycle (cannot verify NetPo)",
                getName(), reason ? reason : "", ax_sym,
                refresh_attempted ? 1 : 0, refresh_ok ? 1 : 0, local_net);
            return false;
        }
    }

    if (venue_net == local_net) {
        Main().logger()->info(
            "[STRATEGY:{}] MM_VENUE_RECONCILE_OK reason={} ax={} net={} refresh={} cache_age_ms={}",
            getName(), reason ? reason : "", ax_sym, local_net,
            refresh_attempted ? (refresh_ok ? "yes" : "FAILED") : "cached",
            cache_age_ms());
        return true;
    }

    const long long resolved_net =
        (std::llabs(venue_net) > std::llabs(local_net)) ? venue_net : local_net;
    Main().logger()->error(
        "[STRATEGY:{}] MM_LOCAL_VENUE_DISAGREE reason={} ax={} local_net={} venue_net={} "
        "resolved_to={} (max-magnitude) — ADOPTING resolved into local AND aborting this place "
        "cycle. refresh_attempted={} refresh_ok={}. Next cycle will re-check; this prevents "
        "the 2026-05-22 SILVER pattern where local stayed at 1 while venue grew to 6.",
        getName(), reason ? reason : "", ax_sym, local_net, venue_net, resolved_net,
        refresh_attempted ? 1 : 0, refresh_ok ? 1 : 0);
    {
        std::lock_guard<std::mutex> lk(position_state_mutex_);
        position_state_.net_position_qty = static_cast<double>(resolved_net);
    }

    // === MAX-POSITION BREACH FIX (2026-05-22 Joe XAG breach) ====================
    // Aborting the cycle prevents NEW grow-side orders, but it does NOT touch
    // any grow-side leg ALREADY resting in the market. In the Joe XAG-PERP
    // breach (`Max Position Breach.txt`):
    //   13:25:17.772  bid 2370 placed (NetPo=1, cap=2)
    //   13:25:19.748  fill on 2368 (the previous bid) → venue NetPo=2
    //   13:25:20.286  MM_LOCAL_VENUE_DISAGREE local=1 venue=2 resolved=2
    //                 → cycle aborts, but bid 2370 STILL RESTING
    //   13:25:21.818  fill on 2370 → venue NetPo=3   ←── BREACH
    // The disagree fired ~1.5 s before the second fill: an explicit cancel
    // at this point would have killed bid 2370 long before its fill.
    //
    // Policy: when we adopted a strictly larger magnitude (we were under-
    // counting), synchronously cancel this stack's resting grow-side leg.
    // Grow side is the side that would push |NetPo| further past the cap:
    //   resolved_net > 0  →  BUY is the grow side
    //   resolved_net < 0  →  SELL is the grow side
    //   resolved_net == 0 →  no grow side, nothing to cancel.
    // We only cancel when `|resolved_net| > |local_net|` so the legacy
    // "REST cache is stale-high" path (Tim's original concern) still keeps
    // its grow-side leg resting — that case correctly relies on the next
    // fill-poll to self-heal local. The cancel call returns 1 even when
    // there is nothing to cancel (`tid == 0`), so this is safe to call
    // unconditionally. `mm_cycle_mutex_` is already held by the caller of
    // `mmReconcileVenueOrAbort` (runFullMmQuoteCycle / fill_requote).
    if (std::llabs(resolved_net) > std::llabs(local_net)) {
        const Side grow_side =
            (resolved_net > 0) ? Side::BUY : (resolved_net < 0 ? Side::SELL : Side::BUY);
        if (resolved_net != 0) {
            const OrderId grow_oid =
                (grow_side == Side::BUY) ? bid_order_id_ : ask_order_id_;
            if (grow_oid != 0) {
                Main().logger()->error(
                    "[STRATEGY:{}] MM_LOCAL_VENUE_DISAGREE_CANCEL_GROW_SIDE ax={} "
                    "resolved_to={} grow_side={} oid={} — cancelling resting grow-side leg "
                    "to prevent post-disagree cap-breach (Joe 2026-05-22 XAG breach pattern)",
                    getName(), ax_sym, resolved_net,
                    grow_side == Side::BUY ? "BUY" : "SELL",
                    static_cast<long long>(grow_oid));
                const int rc = mmCancelTrackedLegThisStackGateway(grow_side);
                if (rc == 1) {
                    if (grow_side == Side::BUY) {
                        bid_order_id_ = 0;
                        last_bid_price_ = 0.0;
                    } else {
                        ask_order_id_ = 0;
                        last_ask_price_ = 0.0;
                    }
                } else {
                    Main().logger()->error(
                        "[STRATEGY:{}] MM_LOCAL_VENUE_DISAGREE_CANCEL_GROW_SIDE_FAILED ax={} "
                        "grow_side={} oid={} — cancel returned rc={}; cycle remains aborted, "
                        "next cycle will retry (no new orders placed in the meantime)",
                        getName(), ax_sym,
                        grow_side == Side::BUY ? "BUY" : "SELL",
                        static_cast<long long>(grow_oid), rc);
                }
            }
        }
    }
    return false;
}

void MakeMarketStrategy::mmResetLocalOrderTrackingOnSide(Side side) {
    if (side == Side::BUY) {
        bid_order_id_ = 0;
        last_bid_price_ = 0.0;
    } else {
        ask_order_id_ = 0;
        last_ask_price_ = 0.0;
    }
}

long long MakeMarketStrategy::mmRefreshNetPositionFromPortfolioCache(const char* call_site) {
    const std::string sym = mmAxSymbol();
    if (sym.empty()) {
        return static_cast<long long>(std::llround(position_state_.net_position_qty));
    }
    // ENFORCE: the book is truth; short-circuit to effective() and NEVER spawn the
    // detached portfolio-refresh worker (no REST). off/shadow keep the legacy path.
    if (mmPbEnforce()) {
        return mmEffectiveNetPoRounded();
    }

    // PortfolioManager::positions_ is only populated by explicit `refresh()` calls — there is NO
    // background poller in this binary keeping it warm (the [POSITION] log line is StartupSequence's
    // own poller, which writes to its own ledger, not into PortfolioManager). So a plain getPosition()
    // returns nullopt → NetPo=0, and the cap gate reads 0 regardless of how big the real position is.
    //
    // === FIX C: BACKGROUND PORTFOLIO REFRESH (race-free, no feed-thread blocking) ===
    // BEFORE: every onFeedUpdate that hit the desk theo-move gate called
    // `Main().portfolio()->refresh()` synchronously here. That is a 200–800ms REST call on the
    // shared `curl_mutex_` (general handle). With ~10 MM strategies per AX × multiple AXes all
    // doing REST cancels, /open-orders probes, and portfolio refreshes through the same mutex,
    // worst-case stalls of multi-second duration would land between a cancel-replace's cancel
    // step and its place step — leaving the strategy orderless. Live incident 2026-05-07
    // 13:36:50: cycle 2 cancelled both EURUSD-PERP legs, then thread 1422098 went silent for
    // 21+ seconds inside this REST chain; the heartbeat thread also stalled.
    //
    // AFTER: dispatch the refresh to a single dedicated worker thread (lazily started). The
    // feed thread signals the worker and returns immediately, reading whatever PortfolioManager
    // currently has cached (≤1.5s stale in the steady state). Net behaviour:
    //   - Feed thread never blocks on portfolio REST (latency drops from 200–800ms to <50us).
    //   - Position freshness is preserved (worker still refreshes every ≥1.5s per symbol).
    //   - Exchange remains the source of truth — we just stop serializing the lookup on the
    //     feed dispatch path.
    static std::mutex s_pf_refresh_mu;
    static std::unordered_map<std::string, std::int64_t> s_pf_refresh_last_ms;
    static std::condition_variable s_pf_refresh_cv;
    static std::atomic<bool> s_pf_refresh_pending{false};
    static std::atomic<bool> s_pf_refresh_thread_started{false};
    constexpr std::int64_t kRefreshThrottleMs = 1500;
    const std::int64_t now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                    std::chrono::steady_clock::now().time_since_epoch())
                                    .count();
    bool do_refresh = false;
    {
        std::lock_guard<std::mutex> lk(s_pf_refresh_mu);
        auto& slot = s_pf_refresh_last_ms[sym];
        if (slot == 0 || now_ms - slot >= kRefreshThrottleMs) {
            slot = now_ms;
            do_refresh = true;
        }
    }
    if (do_refresh) {
        // Lazily start the dedicated portfolio refresh worker thread. Detached so process exit
        // is clean; the loop is guarded by a global flag the worker reads to terminate cleanly
        // if the binary is shutting down (best-effort — not wired to Platform shutdown here).
        if (!s_pf_refresh_thread_started.exchange(true)) {
            std::thread([] {
                for (;;) {
                    std::unique_lock<std::mutex> lk(s_pf_refresh_mu);
                    s_pf_refresh_cv.wait(lk, [] { return s_pf_refresh_pending.load(); });
                    s_pf_refresh_pending.store(false);
                    lk.unlock();
                    try {
                        if (auto* pf = Main().portfolio()) {
                            pf->refresh();
                        }
                    } catch (const std::exception& e) {
                        if (utils::Logger::isInitialized()) {
                            Main().logger()->warn(
                                "[MM_ORDERS] background portfolio refresh exception: {}", e.what());
                        }
                    } catch (...) {
                        if (utils::Logger::isInitialized()) {
                            Main().logger()->warn(
                                "[MM_ORDERS] background portfolio refresh exception: ANY-THROW");
                        }
                    }
                }
            }).detach();
        }
        // Coalesced signal — if multiple strategies request a refresh in the same window the
        // worker only does ONE REST round-trip (PortfolioManager refresh fetches all symbols).
        s_pf_refresh_pending.store(true);
        s_pf_refresh_cv.notify_one();
    }

    auto opt = portfolio::PortfolioManager::getInstance().getPosition(sym);
    long long live_signed = 0;
    double signed_qty_d = 0.0;
    double ref_px = 0.0;
    bool have_position = opt.has_value() && opt->isOpen();
    if (have_position) {
        signed_qty_d = opt->isLong() ? opt->quantity : -opt->quantity;
        ref_px = (opt->mark_price > 0.0) ? opt->mark_price : opt->entry_price;
        live_signed = static_cast<long long>(std::llround(signed_qty_d));
    }
    long long prev_signed = 0;
    {
        // CONCURRENCY (fix C4): stable read against processFill's write.
        std::lock_guard<std::mutex> ps_lk(position_state_mutex_);
        prev_signed = static_cast<long long>(std::llround(position_state_.net_position_qty));
    }
    // === RECENT-FILL OVERRIDE GUARD ===
    // Same logic as syncInventoryFromExchangePortfolio's 2-second skip: if a local fill arrived
    // within the last 2 seconds, `position_state_.onAxFill()` already incremented NetPo with the
    // freshest local truth, but the portfolio cache may not yet reflect that fill (the venue
    // takes ~100–800ms to propagate, and our background refresher may not have re-polled yet).
    // Overriding here would CLOBBER the correct local value with a stale exchange value, then
    // shouldQuoteSide would think we're flat and we'd happily place another grow-side order
    // and breach max_position. That's exactly the user's "MAX_POSITION IS NOT BEING APPLIED"
    // complaint when fills are arriving fast.
    //
    // Take MAX-magnitude of local-pre-call vs cache value. If local already reflects a fill the
    // cache hasn't seen yet, we keep local (more conservative for cap). If cache reflects a
    // taker-on-submit fill that local missed (because OM parsed it as Adopted, not Filled), the
    // cache value is bigger and we adopt it. Either way the cap stays correct.
    const std::int64_t last_fill_ms = last_fill_time_ms_.load(std::memory_order_acquire);
    const std::int64_t now_ms_pos = std::chrono::duration_cast<std::chrono::milliseconds>(
                                        std::chrono::steady_clock::now().time_since_epoch())
                                        .count();
    const bool fresh_local_fill = (last_fill_ms > 0 && now_ms_pos - last_fill_ms < 2000);
    bool kept_local = false;
    if (fresh_local_fill && std::llabs(prev_signed) > std::llabs(live_signed)) {
        // Keep local — already-reflected fill the exchange snapshot hasn't caught up to.
        kept_local = true;
        live_signed = prev_signed;
        // Don't touch position_state_.net_position_qty; it's already prev_signed.
    } else {
        // CONCURRENCY (fix C4): serialize against processFill / cap-gate reads.
        std::lock_guard<std::mutex> ps_lk(position_state_mutex_);
        position_state_.net_position_qty = signed_qty_d;
        if (ref_px > 0.0) {
            position_state_.net_position_usd = signed_qty_d * ref_px;
        } else if (!have_position) {
            position_state_.net_position_usd = 0.0;
        }
    }

    if (utils::Logger::isInitialized()) {
        // Always log this once-per-gate decision. The user requested exchange-as-source-of-truth and
        // wants to see net_po sourced live, so silence is the wrong default — print every refresh.
        Main().logger()->info(
            "[MM_ORDERS] net_position from portfolio cache: strategy={} sym={} live_net={} "
            "local_net_was={} diff={} portfolio_refresh={} have_position={} call_site='{}' "
            "fresh_fill_window={} kept_local_over_cache={}",
            getName(), sym, live_signed, prev_signed, live_signed - prev_signed,
            do_refresh ? 1 : 0,
            have_position ? 1 : 0,
            call_site ? call_site : "",
            fresh_local_fill ? 1 : 0,
            kept_local ? 1 : 0);
    }
    return live_signed;
}

namespace {

/** Per-AX latch: fill-time ACCEPTABLE burst acknowledged; steady-state must not re-flag UNEXPECTED. */
struct MmAxTimCapBurstLatch {
    bool acceptable_burst{false};
};

std::mutex g_mm_tim_cap_burst_mu;
std::unordered_map<std::string, MmAxTimCapBurstLatch> g_mm_tim_cap_burst_by_ax;

MmAxTimCapBurstLatch& mmTimCapBurstLatchFor(const std::string& ax_symbol) {
    return g_mm_tim_cap_burst_by_ax[ax_symbol];
}

void mmClearTimCapBurstLatch(const std::string& ax_symbol) {
    std::lock_guard<std::mutex> lk(g_mm_tim_cap_burst_mu);
    g_mm_tim_cap_burst_by_ax.erase(ax_symbol);
}

// Ownership-retaining collector of all MakeMarketStrategy stacks on an AX symbol (2026-06-11
// venue source-of-truth / UAF guard). Sorted by the stored pointer so every thread acquires the
// per-stack cycle mutexes in the SAME deterministic order (deadlock-free). The returned
// shared_ptrs keep each stack alive for as long as the caller holds the result — so a concurrent
// teardown cannot free a sibling whose raw pointer or `mm_cycle_mutex_` the caller still holds.
// (The former raw `collectMakeMarketStrategiesOnAx` was removed because every caller now retains
// ownership through this variant.)
std::vector<std::shared_ptr<MakeMarketStrategy>> collectMakeMarketStrategySharedPtrsOnAx(
    const std::string& ax_symbol) {
    std::vector<std::shared_ptr<MakeMarketStrategy>> out;
    if (ax_symbol.empty()) {
        return out;
    }
    for (const auto& sp : StrategyManager::getInstance().getAllStrategies()) {
        auto mm = std::dynamic_pointer_cast<MakeMarketStrategy>(sp);
        if (!mm || mm->mmAxSymbol() != ax_symbol) {
            continue;
        }
        out.push_back(std::move(mm));
    }
    std::sort(out.begin(), out.end(),
              [](const std::shared_ptr<MakeMarketStrategy>& a,
                 const std::shared_ptr<MakeMarketStrategy>& b) { return a.get() < b.get(); });
    return out;
}

void mmExtractOpenOrdersRows(const nlohmann::json& root, std::vector<nlohmann::json>& out_rows);
std::string mmExtractRowOid(const nlohmann::json& row);

/** Gateway open-order row symbol (`symbol` or `s`). Empty if absent. */
std::string mmExtractRowSymbol(const nlohmann::json& row) {
    if (row.contains("symbol") && row["symbol"].is_string()) {
        const std::string s = row["symbol"].get<std::string>();
        if (!s.empty()) {
            return s;
        }
    }
    if (row.contains("s") && row["s"].is_string()) {
        const std::string s = row["s"].get<std::string>();
        if (!s.empty()) {
            return s;
        }
    }
    return {};
}

/** Instrument isolation: keep row only if `symbol`/`s` matches `ax_symbol` (case-insensitive).
 * Rows with no symbol are rejected — mixed gateway books must never affect this AX. */
bool mmOpenOrdersRowMatchesAx(const std::string& ax_symbol, const nlohmann::json& row) {
    if (ax_symbol.empty()) {
        return false;
    }
    const std::string row_sym = mmExtractRowSymbol(row);
    if (row_sym.empty()) {
        return false;
    }
    return mmDeskUpperAscii(row_sym) == mmDeskUpperAscii(ax_symbol);
}

constexpr std::int64_t kAtCapConvergeIntervalMs = 15000;
constexpr int kAtCapConvergeEveryNReplaceCycles = 4;
constexpr int kAtCapVenueScrubMaxPasses = 4;

std::mutex g_at_cap_converge_mu;
std::unordered_map<std::string, std::int64_t> g_last_at_cap_converge_ms;
std::unordered_map<std::string, int> g_at_cap_replace_cycle_count;

std::mutex g_ax_fill_requote_inflight_mu;
std::unordered_map<std::string, int> g_ax_fill_requote_inflight;

bool mmAcquireAxFillRequoteInflightImpl(const std::string& ax) {
    if (ax.empty()) {
        return false;
    }
    std::lock_guard<std::mutex> lk(g_ax_fill_requote_inflight_mu);
    int& n = g_ax_fill_requote_inflight[ax];
    if (n > 0) {
        return false;
    }
    n = 1;
    return true;
}

void mmReleaseAxFillRequoteInflightImpl(const std::string& ax) {
    if (ax.empty()) {
        return;
    }
    std::lock_guard<std::mutex> lk(g_ax_fill_requote_inflight_mu);
    auto it = g_ax_fill_requote_inflight.find(ax);
    if (it != g_ax_fill_requote_inflight.end() && it->second > 0) {
        --it->second;
        if (it->second <= 0) {
            g_ax_fill_requote_inflight.erase(it);
        }
    }
}

bool mmIsAxFillRequoteInflightImpl(const std::string& ax) {
    if (ax.empty()) {
        return false;
    }
    std::lock_guard<std::mutex> lk(g_ax_fill_requote_inflight_mu);
    const auto it = g_ax_fill_requote_inflight.find(ax);
    return it != g_ax_fill_requote_inflight.end() && it->second > 0;
}

int mmCountVenueRowsOnSide(const std::vector<MmVenueOpenRow>& rows, Side side) {
    int n = 0;
    for (const auto& r : rows) {
        if ((side == Side::BUY) == r.is_buy) {
            ++n;
        }
    }
    return n;
}

double mmVenueRowPrice(const nlohmann::json& row) {
    static const char* keys[] = {"p", "price", "limit_price", "limitPrice"};
    for (const char* k : keys) {
        if (!row.contains(k)) {
            continue;
        }
        const auto& v = row[k];
        if (v.is_number()) {
            return v.get<double>();
        }
        if (v.is_string()) {
            try {
                return std::stod(v.get<std::string>());
            } catch (...) {
            }
        }
    }
    return 0.0;
}

std::optional<bool> mmVenueRowIsBuy(const nlohmann::json& row) {
    if (row.contains("d") && row["d"].is_string()) {
        const std::string d = row["d"].get<std::string>();
        if (!d.empty()) {
            const char c = static_cast<char>(std::toupper(static_cast<unsigned char>(d[0])));
            if (c == 'B') {
                return true;
            }
            if (c == 'S') {
                return false;
            }
        }
    }
    if (row.contains("side") && row["side"].is_string()) {
        std::string s = row["side"].get<std::string>();
        for (char& ch : s) {
            ch = static_cast<char>(std::toupper(static_cast<unsigned char>(ch)));
        }
        if (s == "BUY" || s == "B") {
            return true;
        }
        if (s == "SELL" || s == "S" || s == "ASK") {
            return false;
        }
    }
    return std::nullopt;
}

bool mmFetchVenueOpenRowsForAx(const std::string& ax_symbol, std::vector<MmVenueOpenRow>& out_rows) {
    out_rows.clear();
    if (ax_symbol.empty()) {
        return false;
    }
    auto resp = Main().rest()->getOrdersGatewayRelative("/open-orders", {{"symbol", ax_symbol}});
    if (!resp.is_success || resp.status_code < 200 || resp.status_code >= 300) {
        if (utils::Logger::isInitialized()) {
            Main().logger()->warn(
                "[MM_PRODUCT_CONVERGE] sym={} GET /open-orders failed http={} — skip venue diff",
                ax_symbol,
                resp.status_code);
        }
        return false;
    }
    nlohmann::json root;
    try {
        root = nlohmann::json::parse(resp.body);
    } catch (const std::exception& e) {
        if (utils::Logger::isInitialized()) {
            Main().logger()->warn(
                "[MM_PRODUCT_CONVERGE] sym={} parse error: {} — skip venue diff",
                ax_symbol,
                e.what());
        }
        return false;
    }
    std::vector<nlohmann::json> rows;
    mmExtractOpenOrdersRows(root, rows);
    const int raw_count = static_cast<int>(rows.size());
    int dropped_foreign_symbol = 0;
    int dropped_no_symbol = 0;
    out_rows.reserve(rows.size());
    for (const auto& row : rows) {
        if (mmExtractRowSymbol(row).empty()) {
            ++dropped_no_symbol;
            continue;
        }
        if (!mmOpenOrdersRowMatchesAx(ax_symbol, row)) {
            ++dropped_foreign_symbol;
            continue;
        }
        std::string oid = mmExtractRowOid(row);
        if (oid.empty()) {
            continue;
        }
        const auto side = mmVenueRowIsBuy(row);
        if (!side.has_value()) {
            continue;
        }
        const double px = mmVenueRowPrice(row);
        out_rows.push_back({std::move(oid), *side, px});
    }
    // Throttle: on a multi-symbol desk every per-AX converge fetches ALL open orders and drops
    // the OTHER symbols' rows, so this benign "dropped foreign rows" line would fire at WARN on
    // every converge pass (~170ms/symbol) and drown the log. It is expected, not actionable
    // ("foreign/ambiguous rows never affect this AX"), so emit at most once per 30s per symbol —
    // enough to still surface a genuine anomaly without the per-pass spam.
    if ((dropped_foreign_symbol > 0 || dropped_no_symbol > 0) && utils::Logger::isInitialized()) {
        static std::mutex s_conv_filter_log_mu;
        static std::unordered_map<std::string, std::int64_t> s_conv_filter_last_ms;
        bool emit = false;
        {
            const std::int64_t now_ms = mmSteadyMillis();
            std::lock_guard<std::mutex> lk(s_conv_filter_log_mu);
            std::int64_t& last = s_conv_filter_last_ms[ax_symbol];
            if (last == 0 || (now_ms - last) >= 30000) {
                last = now_ms;
                emit = true;
            }
        }
        if (emit) {
            Main().logger()->warn(
                "[MM_PRODUCT_CONVERGE] sym={} open-orders instrument filter: raw_rows={} kept={} "
                "dropped_foreign_symbol={} dropped_no_symbol={} — foreign/ambiguous rows never "
                "affect this AX (throttled: >=30s/symbol)",
                ax_symbol,
                raw_count,
                out_rows.size(),
                dropped_foreign_symbol,
                dropped_no_symbol);
        }
    }
    return true;
}

}  // namespace

void MakeMarketStrategy::mmMaybePeriodicConvergeAtCap(MakeMarketStrategy* caller) {
    if (!caller || caller->mmOrderRequestId().empty()) {
        return;
    }
    const std::string ax = caller->mmAxSymbol();
    if (ax.empty()) {
        return;
    }
    auto& cfg = config::Config::getInstance();
    if (!cfg.isMarketMakerEnabled()) {
        return;
    }
    const long long net_po = mmInstrumentNetPoFromPortfolioCache(ax);
    const long long cap_ll = static_cast<long long>(caller->mmEffMaxPositionInt(cfg));
    if (cap_ll <= 0 || std::llabs(net_po) < cap_ll) {
        return;
    }
    const std::int64_t now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                    std::chrono::steady_clock::now().time_since_epoch())
                                    .count();
    bool due = false;
    {
        std::lock_guard<std::mutex> lk(g_at_cap_converge_mu);
        const std::int64_t last = g_last_at_cap_converge_ms[ax];
        if (last <= 0 || (now_ms - last) >= kAtCapConvergeIntervalMs) {
            g_last_at_cap_converge_ms[ax] = now_ms;
            due = true;
        }
    }
    if (due) {
        mmConvergeProductBookOnAx(ax, net_po, cap_ll, "periodic_at_cap", nullptr);
    }
}

void MakeMarketStrategy::mmMaybeConvergeAtCapAfterReplaceCycle(MakeMarketStrategy* caller,
                                                               const char* reason) {
    if (!caller || caller->mmOrderRequestId().empty() || !reason) {
        return;
    }
    mmMaybeConvergeAtCapIfVenueExcess(caller, reason);

    const std::string ax = caller->mmAxSymbol();
    if (ax.empty()) {
        return;
    }
    auto& cfg = config::Config::getInstance();
    const long long net_po = mmInstrumentNetPoFromPortfolioCache(ax);
    const long long cap_ll = static_cast<long long>(caller->mmEffMaxPositionInt(cfg));
    if (cap_ll <= 0 || std::llabs(net_po) < cap_ll) {
        return;
    }
    bool due = false;
    {
        std::lock_guard<std::mutex> lk(g_at_cap_converge_mu);
        const int n = ++g_at_cap_replace_cycle_count[ax];
        if (n % kAtCapConvergeEveryNReplaceCycles == 0) {
            g_last_at_cap_converge_ms[ax] =
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::steady_clock::now().time_since_epoch())
                    .count();
            due = true;
        }
    }
    if (due) {
        mmConvergeProductBookOnAx(ax, net_po, cap_ll, reason, caller);
    }
}

void MakeMarketStrategy::mmWaitReconcileGateBeforePlace(const char* context) const {
    // ENFORCE: no post-cancel fill-poll gate — the book already reflects fills the
    // instant they are polled; placement is not blocked on a REST reconcile. This
    // covers all 3 call sites. off/shadow keep the legacy wait below.
    if (mmPbEnforce()) {
        return;
    }
    const std::int64_t cancel_stamp = last_cancel_response_ms_.load(std::memory_order_acquire);
    if (cancel_stamp <= 0) {
        return;
    }
    const int poll_sec = std::max(
        1, config::Config::getInstance().getInt("api.fill_poll_interval_sec", 1));
    const std::int64_t wait_budget_ms = static_cast<std::int64_t>(poll_sec) * 2 * 1000;
    const std::int64_t deadline = std::chrono::duration_cast<std::chrono::milliseconds>(
                                      std::chrono::steady_clock::now().time_since_epoch())
                                      .count() +
                                  wait_budget_ms;
    bool gate_cleared = false;
    for (;;) {
        const std::int64_t poll_stamp = StartupSequence::lastFillPollCompletedMs();
        if (poll_stamp > cancel_stamp) {
            gate_cleared = true;
            break;
        }
        const std::int64_t now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                        std::chrono::steady_clock::now().time_since_epoch())
                                        .count();
        if (now_ms >= deadline) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    if (utils::Logger::isInitialized()) {
        Main().logger()->info(
            "[STRATEGY:{}] MM_RECONCILE_GATE context={} cancel_stamp_ms={} last_fill_poll_ms={} "
            "cleared={} wait_budget_ms={}",
            getName(),
            context ? context : "?",
            cancel_stamp,
            StartupSequence::lastFillPollCompletedMs(),
            gate_cleared ? 1 : 0,
            wait_budget_ms);
    }
}

bool MakeMarketStrategy::mmAcquireAxFillRequoteInflight(const std::string& ax_symbol) {
    return mmAcquireAxFillRequoteInflightImpl(ax_symbol);
}

void MakeMarketStrategy::mmReleaseAxFillRequoteInflight(const std::string& ax_symbol) {
    mmReleaseAxFillRequoteInflightImpl(ax_symbol);
}

bool MakeMarketStrategy::mmIsAxFillRequoteInflight(const std::string& ax_symbol) {
    return mmIsAxFillRequoteInflightImpl(ax_symbol);
}

void MakeMarketStrategy::mmMaybeConvergeAtCapIfVenueExcess(MakeMarketStrategy* caller,
                                                           const char* reason_tag) {
    if (!caller || caller->mmOrderRequestId().empty() || !reason_tag) {
        return;
    }
    const std::string ax = caller->mmAxSymbol();
    if (ax.empty()) {
        return;
    }
    // ENFORCE: no venue GET on the mover; cap is snapshot-driven. (Belt-and-suspenders:
    // mmConvergeProductBookOnAx is also guarded, but this path GETs directly below.)
    if (mmPbModeForSymbol(ax) == PbMode::Enforce) {
        return;
    }
    auto& cfg = config::Config::getInstance();
    const long long net_po = mmInstrumentNetPoFromPortfolioCache(ax);
    const long long cap_ll = static_cast<long long>(caller->mmEffMaxPositionInt(cfg));
    if (cap_ll <= 0 || std::llabs(net_po) < cap_ll) {
        return;
    }
    std::vector<MmVenueOpenRow> venue_rows;
    if (!mmFetchVenueOpenRowsForAx(ax, venue_rows)) {
        return;
    }
    // Pin shared_ptrs for the whole converge window so a concurrent sibling teardown cannot
    // free a stack whose raw pointer we still hold in desk_mms (UAF guard, 2026-06-11).
    auto mms = collectMakeMarketStrategySharedPtrsOnAx(ax);
    std::vector<MakeMarketStrategy*> desk_mms;
    desk_mms.reserve(mms.size());
    for (auto& mm_sp : mms) {
        auto* mm = mm_sp.get();
        if (mm && !mm->mmOrderRequestId().empty()) {
            desk_mms.push_back(mm);
        }
    }
    const Side reduce_side = (net_po > 0) ? Side::SELL : Side::BUY;
    const std::unordered_set<std::string> protect_oids =
        mmBuildProtectOidsWithTieBreak(desk_mms, venue_rows, true, reduce_side);
    const int reduce_rows = mmCountVenueRowsOnSide(venue_rows, reduce_side);
    const int protect_reduce = static_cast<int>(protect_oids.size());
    if (reduce_rows <= std::max(1, protect_reduce)) {
        return;
    }
    if (utils::Logger::isInitialized()) {
        Main().logger()->warn(
            "[MM_PRODUCT_CONVERGE] sym={} venue_cardinality_at_cap reduce_rows={} protect_oids={} "
            "venue_rows={} reason_tag={} — immediate converge (not waiting for N-cycle throttle)",
            ax,
            reduce_rows,
            protect_reduce,
            static_cast<int>(venue_rows.size()),
            reason_tag);
    }
    mmConvergeProductBookOnAx(ax, net_po, cap_ll, "venue_cardinality_at_cap", caller);
}

bool MakeMarketStrategy::mmEnsureAtCapReduceSideVenueClean(const std::string& ax_symbol,
                                                           long long net_po,
                                                           long long cap_ll,
                                                           const char* reason,
                                                           MakeMarketStrategy* cycle_mutex_held) {
    if (ax_symbol.empty() || cap_ll <= 0 || !reason) {
        return true;
    }
    if (std::llabs(net_po) < cap_ll) {
        return true;
    }
    // ENFORCE: local-truth. The grow-side cap is held by effective() gates + snapshot pull;
    // never scrub via GET /open-orders on the mover. Treat the venue as clean and proceed.
    if (mmPbModeForSymbol(ax_symbol) == PbMode::Enforce) {
        return true;
    }
    const Side reduce_side = (net_po > 0) ? Side::SELL : Side::BUY;
    (void)mmCancelTrackedLegAllStacksOnAx(ax_symbol, reduce_side, cycle_mutex_held);

    if (cycle_mutex_held) {
        cycle_mutex_held->mmWaitReconcileGateBeforePlace(reason);
    } else {
        for (auto& mm_sp : collectMakeMarketStrategySharedPtrsOnAx(ax_symbol)) {
            auto* mm = mm_sp.get();
            if (mm && !mm->mmOrderRequestId().empty()) {
                mm->mmWaitReconcileGateBeforePlace(reason);
                break;
            }
        }
    }

    for (int pass = 0; pass < kAtCapVenueScrubMaxPasses; ++pass) {
        std::vector<MmVenueOpenRow> venue_rows;
        if (!mmFetchVenueOpenRowsForAx(ax_symbol, venue_rows)) {
            return pass > 0;
        }
        const int reduce_rows = mmCountVenueRowsOnSide(venue_rows, reduce_side);
        if (reduce_rows <= 1) {
            return true;
        }
        if (utils::Logger::isInitialized()) {
            Main().logger()->warn(
                "[MM_PRODUCT_CONVERGE] sym={} pre_place_at_cap pass={} reduce_rows={} reason={} — "
                "scrubbing excess reduce-side venue rows before place",
                ax_symbol,
                pass,
                reduce_rows,
                reason);
        }
        mmConvergeProductBookOnAx(ax_symbol, net_po, cap_ll, "pre_place_at_cap", cycle_mutex_held);
        std::this_thread::sleep_for(std::chrono::milliseconds(80));
    }
    std::vector<MmVenueOpenRow> final_rows;
    if (!mmFetchVenueOpenRowsForAx(ax_symbol, final_rows)) {
        return false;
    }
    const int final_reduce = mmCountVenueRowsOnSide(final_rows, reduce_side);
    if (final_reduce > 1 && utils::Logger::isInitialized()) {
        Main().logger()->error(
            "[MM_PRODUCT_CONVERGE] sym={} pre_place_at_cap BLOCKED reason={} reduce_rows={} — "
            "refusing place until venue shows <=1 reduce-side row",
            ax_symbol,
            reason,
            final_reduce);
    }
    return final_reduce <= 1;
}

MmAllStacksCycleLock MakeMarketStrategy::mmAcquireAllStacksCycleLocks(
    const std::string& ax_symbol, MakeMarketStrategy* self_fallback) {
    MmAllStacksCycleLock result;
    result.owners = collectMakeMarketStrategySharedPtrsOnAx(ax_symbol);
    if (result.owners.empty() && self_fallback != nullptr) {
        // No registered stack matched the AX (e.g. self not yet registered, or its AX
        // binding not set). Retain shared ownership of self when it IS registered so the
        // lock can't outlive it; otherwise lock non-owningly — self is alive by
        // construction (the mover lambda that called us holds a shared_ptr to it).
        if (auto self_sp = std::dynamic_pointer_cast<MakeMarketStrategy>(
                StrategyManager::getInstance().getStrategy(self_fallback->getName()))) {
            result.owners.push_back(std::move(self_sp));
        } else {
            result.locks.emplace_back(self_fallback->mm_cycle_mutex_);
            return result;
        }
    }
    result.locks.reserve(result.owners.size());
    for (const auto& mm : result.owners) {
        result.locks.emplace_back(mm->mm_cycle_mutex_);
    }
    return result;
}

namespace {

struct MmAxProductPlacementGate {
    std::mutex mu;
    std::condition_variable cv;
    std::string lease_owner;
    std::int64_t lease_acquired_ms{0};
  /** Set when a fully-degraded waiter (bid/ask id 0) skips product-quiet inside atomic claim. */
    bool lease_degraded_quiet_bypass{false};
    int consecutive_lease_denied{0};
    std::int64_t last_lease_denied_alert_ms{0};
    /** Teardown wait timed out — holder must abort post-HTTP cycle work (alert-only). */
    std::string force_released_owner;
};

thread_local std::string tls_mm_product_net_ax;
thread_local long long tls_mm_product_net_po{0};
thread_local bool tls_mm_product_net_valid{false};

MakeMarketStrategy* mmFindMmOnAxByName(const std::string& ax_symbol, const std::string& name) {
    // Pin during iteration so getName() never derefs a freed sibling; the returned raw pointer's
    // post-return lifetime remains the caller's responsibility (unchanged contract).
    for (auto& mm_sp : collectMakeMarketStrategySharedPtrsOnAx(ax_symbol)) {
        if (mm_sp->getName() == name) {
            return mm_sp.get();
        }
    }
    return nullptr;
}

std::int64_t mmProductPlacementWaitBudgetMs() {
    auto& cfg = config::Config::getInstance();
    const int poll_sec = std::max(1, cfg.getInt("api.fill_poll_interval_sec", 1));
    const std::int64_t rtt_ms = 300;
    const std::int64_t raw =
        static_cast<std::int64_t>(poll_sec) * 2 * 1000 + 2 * rtt_ms + 500;
    return std::clamp(raw, static_cast<std::int64_t>(2000),
                      static_cast<std::int64_t>(10000));
}

// Context-aware wait budget for the quote-cycle placement lease.
//
// The mover is ONE global thread shared by every instrument. If a quote cycle blocks for the
// full multi-second budget spinning on a lease it cannot get, it starves EVERY other AX behind
// it in the FIFO queue (the XAU freeze). When this acquisition runs on the mover thread we use a
// short, non-blocking budget and fail fast: lease acquisition happens BEFORE any cancel/place, so
// bailing leaves no partial one-sided state, and the next feed-driven coalesced quote cycle
// retries within sub-second. Off the mover (timer / desk-bg threads) the full budget is harmless.
std::int64_t mmProductPlacementWaitBudgetMsForQuoteCycle() {
    if (MmOrderMover::isCurrentThreadMover()) {
        return 150;
    }
    return mmProductPlacementWaitBudgetMs();
}

std::int64_t mmProductPlacementLeaseTtlMs() {
    return std::max<std::int64_t>(mmProductPlacementWaitBudgetMs() * 3,
                                  static_cast<std::int64_t>(15000));
}

/** Caller must hold gate.mu. Returns non-empty expire reason if current owner is stale. */
std::string mmProductLeaseExpireReasonLocked(const std::string& ax_symbol,
                                             const MmAxProductPlacementGate& gate,
                                             std::int64_t now_ms) {
    if (gate.lease_owner.empty()) {
        return {};
    }
    const std::int64_t ttl_ms = mmProductPlacementLeaseTtlMs();
    if (gate.lease_acquired_ms > 0 && (now_ms - gate.lease_acquired_ms) >= ttl_ms) {
        return "lease_ttl_ms=" + std::to_string(ttl_ms);
    }
    MakeMarketStrategy* owner = mmFindMmOnAxByName(ax_symbol, gate.lease_owner);
    if (!owner) {
        return "lease_owner_strategy_gone";
    }
    if (!owner->mmIsQuoteCycleRunning()) {
        const std::int64_t last_act = owner->mmLastCycleActivityMs();
        if (last_act <= 0 || (now_ms - last_act) >= ttl_ms) {
            return "lease_owner_inactive_ms=" + std::to_string(now_ms - last_act);
        }
    }
    return {};
}

/**
 * Atomic claim under gate.mu: evaluate expiry, clear stale owner if needed, then assign
 * waiter in one critical section — never a separate check-then-grab across threads.
 */
bool mmTryAtomicClaimProductPlacementLeaseLocked(const std::string& ax_symbol,
                                                 MmAxProductPlacementGate& gate,
                                                 MakeMarketStrategy* waiter_mm,
                                                 const std::string& waiter,
                                                 std::int64_t now_ms,
                                                 bool serialized_single_thread_bypass) {
    if (!gate.lease_owner.empty() && gate.lease_owner != waiter) {
        const std::string expire_reason = mmProductLeaseExpireReasonLocked(ax_symbol, gate, now_ms);
        if (!expire_reason.empty()) {
            if (utils::Logger::isInitialized()) {
                Main().logger()->error(
                    "[MM_ORDERS] product_placement_lease_expired ax={} stale_owner={} reason={} "
                    "— atomic clear+claim path (same gate.mu as acquire)",
                    ax_symbol,
                    gate.lease_owner,
                    expire_reason);
            }
            gate.lease_owner.clear();
            gate.lease_acquired_ms = 0;
            gate.lease_degraded_quiet_bypass = false;
        }
    }
    // Owner is now available (empty or already this waiter) — post-expiry check above.
    // Per-instrument sharded ENFORCE (2026-07): every stack on this AX runs on the SAME mover
    // worker, so at most one stack's placement job is ever executing at a time — cross-stack
    // exclusion is already guaranteed by thread serialization. In this mode the "product quiet"
    // requirement is not just redundant, it is actively wrong for fire-and-track: the mover job
    // deliberately leaves its own just-cancelled legs in CANCEL_SENT_UNCONFIRMED (place before
    // cancel ack), which mmAnyPlacementActivityOnAx() reports as activity forever, so the product
    // is never quiet and the immediate place is denied → falls back to slow DESK_RECOVERY.
    // mmProductLeaseClaimGranted() encodes the full grant decision; off/shadow/global-mover keep
    // the legacy quiet||degraded gate byte-for-byte (serialized_single_thread_bypass == false).
    //
    // The lease granted here is released on EVERY exit path of the mover job via the
    // MmProductPlacementLeaseScope RAII destructor (normal return, early return, and exception
    // unwind), which calls mmReleaseProductPlacementLeaseByName() → clears gate.lease_owner. The
    // per-lease TTL (mmProductLeaseExpireReasonLocked) is a second backstop if a thread is killed
    // before its destructor runs.
    const bool product_quiet =
        !serialized_single_thread_bypass &&
        (MakeMarketStrategy::mmSumProductPendingAcceptsOnAx(ax_symbol) == 0) &&
        !MakeMarketStrategy::mmAnyPlacementActivityOnAx(ax_symbol);
    const bool degraded_bypass =
        waiter_mm != nullptr && waiter_mm->mmIsFullyDegradedForLeaseBypass();
    if (!mmProductLeaseClaimGranted(/*owner_available=*/true, serialized_single_thread_bypass,
                                    product_quiet, degraded_bypass)) {
        return false;
    }
    gate.lease_owner = waiter;
    gate.lease_acquired_ms = now_ms;
    gate.lease_degraded_quiet_bypass =
        (!serialized_single_thread_bypass && !product_quiet && degraded_bypass);
    if (gate.force_released_owner == waiter) {
        gate.force_released_owner.clear();
    }
    if (gate.lease_degraded_quiet_bypass && utils::Logger::isInitialized()) {
        Main().logger()->warn(
            "[MM_ORDERS] product_placement_lease_degraded_quiet_bypass ax={} strategy={} "
            "product_pending={} — fully-degraded 0/0 waiter skips quiet wait; cap gate uses "
            "worst-case inflight (PLACE_PENDING/CANCEL_PENDING/ADOPTED)",
            ax_symbol,
            waiter,
            MakeMarketStrategy::mmSumProductPendingAcceptsOnAx(ax_symbol));
    }
    return true;
}

void mmNoteProductLeaseDenied(const std::string& ax_symbol,
                              MmAxProductPlacementGate& gate,
                              const std::string& waiter) {
    ++gate.consecutive_lease_denied;
    const std::int64_t now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                    std::chrono::steady_clock::now().time_since_epoch())
                                    .count();
    constexpr int kAlertStreak = 5;
    constexpr std::int64_t kAlertThrottleMs = 30000;
    if (gate.consecutive_lease_denied < kAlertStreak) {
        return;
    }
    if (gate.last_lease_denied_alert_ms > 0 &&
        (now_ms - gate.last_lease_denied_alert_ms) < kAlertThrottleMs) {
        return;
    }
    gate.last_lease_denied_alert_ms = now_ms;
    if (utils::Logger::isInitialized()) {
        Main().logger()->error(
            "[MM_ORDERS] ALERT product_placement_stalled ax={} waiter={} "
            "consecutive_lease_denied={} lease_owner={} — MM on this AX may be halted; "
            "check wedged holder or venue ack path",
            ax_symbol,
            waiter,
            gate.consecutive_lease_denied,
            gate.lease_owner.empty() ? std::string("(none)") : gate.lease_owner);
    }
}

MmAxProductPlacementGate& mmProductGateFor(const std::string& ax_symbol) {
    static std::mutex map_mu;
    static std::unordered_map<std::string, std::unique_ptr<MmAxProductPlacementGate>> gates;
    std::lock_guard<std::mutex> lk(map_mu);
    auto& slot = gates[ax_symbol];
    if (!slot) {
        slot = std::make_unique<MmAxProductPlacementGate>();
    }
    return *slot;
}

bool mmProductPlacementForceReleasedFor(const std::string& ax, const std::string& name) {
    if (ax.empty() || name.empty()) {
        return false;
    }
    auto& gate = mmProductGateFor(ax);
    std::lock_guard<std::mutex> lk(gate.mu);
    return gate.force_released_owner == name;
}

bool mmTryAtomicClaimProductPlacementLeaseForConvergeLocked(
    const std::string& ax_symbol,
    MmAxProductPlacementGate& gate,
    const std::string& waiter,
    std::int64_t now_ms) {
    if (!gate.lease_owner.empty() && gate.lease_owner != waiter) {
        const std::string expire_reason = mmProductLeaseExpireReasonLocked(ax_symbol, gate, now_ms);
        if (!expire_reason.empty()) {
            gate.lease_owner.clear();
            gate.lease_acquired_ms = 0;
            gate.lease_degraded_quiet_bypass = false;
        }
    }
    if (!gate.lease_owner.empty() && gate.lease_owner != waiter) {
        return false;
    }
    gate.lease_owner = waiter;
    gate.lease_acquired_ms = now_ms;
    gate.lease_degraded_quiet_bypass = false;
    if (gate.force_released_owner == waiter) {
        gate.force_released_owner.clear();
    }
    return true;
}

bool mmAcquireProductPlacementLeaseForConverge(MakeMarketStrategy* ref_mm) {
    if (!ref_mm) {
        return false;
    }
    const std::string ax = ref_mm->mmAxSymbol();
    const std::string waiter = ref_mm->getName();
    if (ax.empty()) {
        return false;
    }
    auto& gate = mmProductGateFor(ax);
    std::unique_lock<std::mutex> lk(gate.mu);
    const auto deadline = std::chrono::steady_clock::now() +
                          std::chrono::milliseconds(mmProductPlacementWaitBudgetMs());
    while (true) {
        const std::int64_t now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                        std::chrono::steady_clock::now().time_since_epoch())
                                        .count();
        if (mmTryAtomicClaimProductPlacementLeaseForConvergeLocked(ax, gate, waiter, now_ms)) {
            gate.consecutive_lease_denied = 0;
            if (utils::Logger::isInitialized()) {
                Main().logger()->info(
                    "[MM_PRODUCT_CONVERGE] product_placement_lease_acquired ax={} strategy={} "
                    "wait_budget_ms={} — converge holds lease until venue diff completes",
                    ax,
                    waiter,
                    mmProductPlacementWaitBudgetMs());
            }
            return true;
        }
        if (std::chrono::steady_clock::now() >= deadline) {
            if (utils::Logger::isInitialized()) {
                Main().logger()->error(
                    "[MM_PRODUCT_CONVERGE] product_placement_lease_denied ax={} waiter={} "
                    "lease_owner={} — converge aborted (fail-safe)",
                    ax,
                    waiter,
                    gate.lease_owner.empty() ? std::string("(none)") : gate.lease_owner);
            }
            return false;
        }
        gate.cv.wait_for(lk, std::chrono::milliseconds(25));
    }
}

thread_local int tls_mm_product_lease_depth = 0;

}  // namespace

MakeMarketStrategy::MmProductPlacementLeaseScope::MmProductPlacementLeaseScope(MakeMarketStrategy* mm)
    : mm_(mm) {
    if (!mm_ || mm_->mmAxSymbol().empty()) {
        mm_ = nullptr;
        return;
    }
    ax_label_ = mm_->mmAxSymbol();
    strategy_name_ = mm_->getName();
    if (tls_mm_product_lease_depth > 0) {
        ++tls_mm_product_lease_depth;
        lease_acquired_ = true;
        return;
    }
    lease_acquired_ = mmAcquireProductPlacementLease(mm_);
    if (!lease_acquired_) {
        ax_label_.clear();
        strategy_name_.clear();
        return;
    }
    ++tls_mm_product_lease_depth;
    outer_ = true;
}

MakeMarketStrategy::MmProductPlacementLeaseScope::~MmProductPlacementLeaseScope() {
    if (tls_mm_product_lease_depth <= 0) {
        return;
    }
    --tls_mm_product_lease_depth;
    if (tls_mm_product_lease_depth > 0 || !outer_ || !lease_acquired_) {
        return;
    }
    if (!ax_label_.empty() && !strategy_name_.empty()) {
        mmReleaseProductPlacementLeaseByName(ax_label_, strategy_name_);
    } else if (mm_) {
        mmReleaseProductPlacementLease(mm_);
    }
    MakeMarketStrategy::mmClearProductCycleNetSnapshot();
}

int MakeMarketStrategy::mmSumProductPendingAcceptsOnAx(const std::string& ax_symbol) {
    int total = 0;
    for (auto& mm_sp : collectMakeMarketStrategySharedPtrsOnAx(ax_symbol)) {
        total += mm_sp->mm_pending_accepts_;
    }
    return total;
}

bool MakeMarketStrategy::mmAnyPlacementActivityOnAx(const std::string& ax_symbol) {
    if (mmSumProductPendingAcceptsOnAx(ax_symbol) > 0) {
        return true;
    }
    // Self-healing liveness: a leg that has been stuck in PLACE_PENDING / CANCEL_PENDING beyond
    // this TTL no longer counts as "placement activity" that blocks the per-AX lease for OTHER
    // stacks. A genuinely in-flight leg resolves in well under a second (RTT ~300ms) and a normal
    // cancel clears after the 3000ms cancel-vs-fill protection window — so anything older than the
    // TTL is wedged. Without this escape one wedged leg keeps the product permanently non-quiet and
    // starves every sibling stack on the instrument (the XAU lease livelock). The owning stack still
    // reconciles the wedged leg via venue truth; this only stops it from holding the product hostage.
    const std::int64_t now_ms = mmSteadyMillis();
    const std::int64_t stale_ms = std::max<std::int64_t>(
        4000, config::Config::getInstance().getInt("market_maker.leg_pending_stale_ms", 8000));
    for (auto& mm_sp : collectMakeMarketStrategySharedPtrsOnAx(ax_symbol)) {
        auto* mm = mm_sp.get();
        std::lock_guard<std::mutex> lk(mm->tracked_orders_mutex_);
        auto leg_busy = [now_ms, stale_ms](const TrackedLeg& leg) {
            if (!mmLegStateIsCancelInFlight(leg.state) && leg.state != MmLegState::PLACE_PENDING) {
                return false;
            }
            // Wedged beyond the TTL -> ignore for the shared-lease quiet check.
            if (leg.state_entered_steady_ms > 0 &&
                (now_ms - leg.state_entered_steady_ms) >= stale_ms) {
                return false;
            }
            return true;
        };
        for (const auto& t : mm->tracked_bids_) {
            if (leg_busy(t)) {
                return true;
            }
        }
        for (const auto& t : mm->tracked_asks_) {
            if (leg_busy(t)) {
                return true;
            }
        }
    }
    return false;
}

void MakeMarketStrategy::mmNotifyProductPlacementGate(const std::string& ax_symbol) {
    if (ax_symbol.empty()) {
        return;
    }
    auto& gate = mmProductGateFor(ax_symbol);
    gate.cv.notify_all();
}

void MakeMarketStrategy::mmMaybeStaleClearProductPendingAccepts(const std::string& ax_symbol) {
    const std::int64_t now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                    std::chrono::steady_clock::now().time_since_epoch())
                                    .count();
    constexpr std::int64_t kStaleMs = 5000;
    for (auto& mm_sp : collectMakeMarketStrategySharedPtrsOnAx(ax_symbol)) {
        auto* mm = mm_sp.get();
        if (mm->mm_pending_accepts_ <= 0) {
            continue;
        }
        const std::int64_t last_ms =
            mm->mm_pending_accepts_last_change_ms_.load(std::memory_order_relaxed);
        if (last_ms > 0 && (now_ms - last_ms) >= kStaleMs) {
            if (utils::Logger::isInitialized()) {
                Main().logger()->warn(
                    "[MM_ORDERS] product_placement_stale_pending_clear ax={} strategy={} "
                    "pending={} age_ms={}",
                    ax_symbol,
                    mm->getName(),
                    mm->mm_pending_accepts_,
                    now_ms - last_ms);
            }
            mm->mm_pending_accepts_ = 0;
            mm->mm_pending_accepts_last_change_ms_.store(now_ms, std::memory_order_relaxed);
        }
    }
}

bool MakeMarketStrategy::mmAcquireProductPlacementLease(MakeMarketStrategy* mm) {
    if (!mm) {
        return false;
    }
    const std::string ax = mm->mmAxSymbol();
    const std::string name = mm->getName();
    if (ax.empty()) {
        return false;
    }
    auto& gate = mmProductGateFor(ax);
    std::unique_lock<std::mutex> lk(gate.mu);
    // ENFORCE + sharded neutralization (Phase A, 2026-07): under per-instrument sharding every
    // stack on this AX runs on the SAME mover worker, so they are already strictly serialized —
    // the product_placement_lease's cross-stack coordination is redundant and its wait budget is
    // pure dead latency (the 150ms mover budget). Grant on the first (uncontended) claim with a
    // 0ms budget; the owner is still set below so the submit-funnel gate (lease_owner==name)
    // passes. Invariant: REST place blocks until accept ON the worker, so per-AX serialization
    // subsumes the pending_accepts wait too. off/shadow keep the legacy budget byte-for-byte.
    const bool enforce_sharded_neutralize = mmSerializedSingleThreadLeaseBypassEligible(
        mmPbModeForSymbol(ax) == PbMode::Enforce,
        MmOrderMover::getInstance().shardingResolvedOn(),
        MmOrderMover::isCurrentThreadMoverForAx(ax));
    // Fail fast when running on the single global mover thread so a denied stack cannot starve
    // every other instrument behind it in the FIFO queue (see helper for the full rationale).
    const std::int64_t wait_budget_ms =
        enforce_sharded_neutralize ? 0 : mmProductPlacementWaitBudgetMsForQuoteCycle();
    const auto deadline = std::chrono::steady_clock::now() +
                          std::chrono::milliseconds(wait_budget_ms);
    while (true) {
        MakeMarketStrategy::mmMaybeStaleClearProductPendingAccepts(ax);
        const std::int64_t now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                        std::chrono::steady_clock::now().time_since_epoch())
                                        .count();
        if (mmTryAtomicClaimProductPlacementLeaseLocked(ax, gate, mm, name, now_ms,
                                                        enforce_sharded_neutralize)) {
            gate.consecutive_lease_denied = 0;
            if (mm->mmIsFullyDegradedForLeaseBypass()) {
                MakeMarketStrategy::mmClearDeskRecoveryLeaseBackoff(mm);
            }
            if (utils::Logger::isInitialized()) {
                Main().logger()->info(
                    "[MM_ORDERS] product_placement_lease_acquired ax={} strategy={} "
                    "wait_budget_ms={} lease_ttl_ms={} degraded_quiet_bypass={} "
                    "(atomic expire+quiet+claim under gate.mu)",
                    ax,
                    name,
                    wait_budget_ms,
                    mmProductPlacementLeaseTtlMs(),
                    gate.lease_degraded_quiet_bypass ? 1 : 0);
            }
            return true;
        }
        if (std::chrono::steady_clock::now() >= deadline) {
            if (utils::Logger::isInitialized()) {
                Main().logger()->error(
                    "[MM_ORDERS] product_placement_lease_denied ax={} strategy={} "
                    "lease_owner={} product_pending={} wait_budget_ms={} fully_degraded={} "
                    "— NOT placing (fail-safe: never steal lease or pass cap while another "
                    "stack is active)",
                    ax,
                    name,
                    gate.lease_owner.empty() ? std::string("(none)") : gate.lease_owner,
                    mmSumProductPendingAcceptsOnAx(ax),
                    wait_budget_ms,
                    mm->mmIsFullyDegradedForLeaseBypass() ? 1 : 0);
            }
            mmNoteProductLeaseDenied(ax, gate, name);
            if (mm->mmIsFullyDegradedForLeaseBypass()) {
                MakeMarketStrategy::mmScheduleDeskRecoveryLeaseBackoff(mm);
            }
            return false;
        }
        gate.cv.wait_for(lk, std::chrono::milliseconds(25));
    }
}

void MakeMarketStrategy::mmReleaseProductPlacementLease(MakeMarketStrategy* mm) {
    if (!mm) {
        return;
    }
    mmReleaseProductPlacementLeaseByName(mm->mmAxSymbol(), mm->getName());
}

void MakeMarketStrategy::mmReleaseProductPlacementLeaseByName(const std::string& ax,
                                                              const std::string& name) {
    if (ax.empty() || name.empty()) {
        return;
    }
    MakeMarketStrategy* mm = mmFindMmOnAxByName(ax, name);
    auto& gate = mmProductGateFor(ax);
    std::unique_lock<std::mutex> lk(gate.mu);
    const bool force_released = (gate.force_released_owner == name);
    if (mm != nullptr && mm->mm_pending_accepts_ > 0 &&
        (!mm->isRunning() || force_released)) {
        if (utils::Logger::isInitialized()) {
            Main().logger()->warn(
                "[MM_ORDERS] product_placement_release_shutdown_pending_clear ax={} strategy={} "
                "pending={} force_released={}",
                ax,
                name,
                mm->mm_pending_accepts_,
                force_released ? 1 : 0);
        }
        mm->mm_pending_accepts_ = 0;
        mm->mm_pending_accepts_last_change_ms_.store(
            std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now().time_since_epoch())
                .count(),
            std::memory_order_relaxed);
        {
            std::lock_guard<std::mutex> accept_lk(mm->mm_accept_pending_mutex_);
            mm->mm_pending_accept_cycle_reason_.clear();
        }
    }
    const auto deadline = std::chrono::steady_clock::now() +
                          std::chrono::milliseconds(mmProductPlacementWaitBudgetMs());
    while (mm != nullptr && mm->mm_pending_accepts_ > 0) {
        if (std::chrono::steady_clock::now() >= deadline) {
            MakeMarketStrategy::mmMaybeStaleClearProductPendingAccepts(ax);
            if (mm->mm_pending_accepts_ > 0) {
                if (utils::Logger::isInitialized()) {
                    Main().logger()->error(
                        "[MM_ORDERS] product_placement_release_pending_timeout ax={} strategy={} "
                        "pending={} wait_budget_ms={} — stale-clearing local pending; next stack "
                        "still blocked until product_quiet (in-flight legs / other pending)",
                        ax,
                        name,
                        mm->mm_pending_accepts_,
                        mmProductPlacementWaitBudgetMs());
                }
                mm->mm_pending_accepts_ = 0;
                mm->mm_pending_accepts_last_change_ms_.store(
                    std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::steady_clock::now().time_since_epoch())
                        .count(),
                    std::memory_order_relaxed);
            }
            break;
        }
        lk.unlock();
        MakeMarketStrategy::mmMaybeStaleClearProductPendingAccepts(ax);
        mm = mmFindMmOnAxByName(ax, name);
        lk.lock();
        if (mm == nullptr) {
            break;
        }
        gate.cv.wait_for(lk, std::chrono::milliseconds(25));
    }
    if (gate.lease_owner == name) {
        gate.lease_owner.clear();
        gate.lease_acquired_ms = 0;
        gate.lease_degraded_quiet_bypass = false;
    }
    lk.unlock();
    mmNotifyProductPlacementGate(ax);
    if (utils::Logger::isInitialized()) {
        Main().logger()->info(
            "[MM_ORDERS] product_placement_lease_released ax={} strategy={}",
            ax,
            name);
    }
}

void MakeMarketStrategy::mmWaitForStrategyProductPlacementLeaseRelease(const std::string& ax,
                                                                       const std::string& strategy_name,
                                                                       int timeout_ms) {
    if (ax.empty() || strategy_name.empty()) {
        return;
    }
    auto& gate = mmProductGateFor(ax);
    const auto deadline = std::chrono::steady_clock::now() +
                          std::chrono::milliseconds(std::max(0, timeout_ms));
    while (std::chrono::steady_clock::now() < deadline) {
        {
            std::lock_guard<std::mutex> lk(gate.mu);
            if (gate.lease_owner != strategy_name) {
                return;
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(25));
        gate.cv.notify_all();
    }
    if (utils::Logger::isInitialized()) {
        Main().logger()->warn(
            "[MM_ORDERS] product_placement_teardown_wait_timeout ax={} strategy={} timeout_ms={} "
            "— forcing lease release; mover may still be mid-action",
            ax,
            strategy_name,
            timeout_ms);
    }
    {
        std::lock_guard<std::mutex> lk(gate.mu);
        gate.force_released_owner = strategy_name;
    }
    mmReleaseProductPlacementLeaseByName(ax, strategy_name);
}

void MakeMarketStrategy::mmTeardownUnblockProductPlacementLease() {
    if (mm_pending_accepts_ > 0) {
        if (utils::Logger::isInitialized()) {
            Main().logger()->info(
                "[MM_ORDERS] product_placement_teardown_pending_clear ax={} strategy={} pending={}",
                mmAxSymbol(),
                getName(),
                mm_pending_accepts_);
        }
        mm_pending_accepts_ = 0;
        mm_pending_accepts_last_change_ms_.store(
            std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now().time_since_epoch())
                .count(),
            std::memory_order_relaxed);
        {
            std::lock_guard<std::mutex> lk(mm_accept_pending_mutex_);
            mm_pending_accept_cycle_reason_.clear();
        }
    }
    mmNotifyProductPlacementGate(mmAxSymbol());
}

void MakeMarketStrategy::mmScheduleDeskRecoveryLeaseBackoff(MakeMarketStrategy* mm) {
    if (!mm || !mm->mmIsFullyDegradedForLeaseBypass()) {
        return;
    }
    const int streak =
        mm->mm_desk_recovery_lease_denied_streak_.fetch_add(1, std::memory_order_relaxed) + 1;
    const int exp = std::min(streak, 5);
    const std::int64_t delay_ms =
        std::min<std::int64_t>(60000, 3000LL * (1LL << (exp - 1)));
    const std::int64_t now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                    std::chrono::steady_clock::now().time_since_epoch())
                                    .count();
    mm->mm_desk_recovery_next_attempt_ms_.store(now_ms + delay_ms, std::memory_order_relaxed);
    if (utils::Logger::isInitialized()) {
        Main().logger()->warn(
            "[STRATEGY:{}] desk_recovery_lease_backoff streak={} next_attempt_ms={} "
            "delay_ms={} — suppressing DESK_RECOVERY spam until then",
            mm->getName(),
            streak,
            now_ms + delay_ms,
            delay_ms);
    }
}

void MakeMarketStrategy::mmClearDeskRecoveryLeaseBackoff(MakeMarketStrategy* mm) {
    if (!mm) {
        return;
    }
    mm->mm_desk_recovery_lease_denied_streak_.store(0, std::memory_order_relaxed);
    mm->mm_desk_recovery_next_attempt_ms_.store(0, std::memory_order_relaxed);
}

long long MakeMarketStrategy::mmProductNetPoForCapGate(const std::string& ax_symbol,
                                                       MakeMarketStrategy* cycle_mutex_held) {
    if (ax_symbol.empty() || !cycle_mutex_held) {
        return 0;
    }
    syncExchangeNetForAllMakeMarketOnSymbol(ax_symbol, cycle_mutex_held, false);
    std::lock_guard<std::mutex> ps_lk(cycle_mutex_held->position_state_mutex_);
    return static_cast<long long>(
        std::llround(cycle_mutex_held->position_state_.net_position_qty));
}

void MakeMarketStrategy::mmSeedProductCycleNetSnapshot(MakeMarketStrategy* cycle_mutex_held) {
    if (!cycle_mutex_held || cycle_mutex_held->mmAxSymbol().empty()) {
        return;
    }
    const std::string ax = cycle_mutex_held->mmAxSymbol();
    syncExchangeNetForAllMakeMarketOnSymbol(ax, cycle_mutex_held, false);
    std::lock_guard<std::mutex> ps_lk(cycle_mutex_held->position_state_mutex_);
    tls_mm_product_net_po = static_cast<long long>(
        std::llround(cycle_mutex_held->position_state_.net_position_qty));
    tls_mm_product_net_ax = ax;
    tls_mm_product_net_valid = true;
}

void MakeMarketStrategy::mmClearProductCycleNetSnapshot() {
    tls_mm_product_net_valid = false;
    tls_mm_product_net_ax.clear();
    tls_mm_product_net_po = 0;
}

long long MakeMarketStrategy::mmProductNetForCapGateAtSubmit(MakeMarketStrategy* mm) {
    if (!mm) {
        return 0;
    }
    const std::string ax = mm->mmAxSymbol();
    if (tls_mm_product_net_valid && tls_mm_product_net_ax == ax) {
        std::lock_guard<std::mutex> ps_lk(mm->position_state_mutex_);
        return static_cast<long long>(
            std::llround(mm->position_state_.net_position_qty));
    }
    mmClearProductCycleNetSnapshot();
    return mmProductNetPoForCapGate(ax, mm);
}

std::string MakeMarketStrategy::mmReservePendingPlace(Side side, core::Price px, core::Quantity qty) {
    // Pre-generate the client id so we can register the leg BEFORE the synchronous
    // submit. submit_order honours a caller-set req.client_order_id (Strategy.cpp).
    const std::string cid = generate_client_order_id();
    {
        std::lock_guard<std::mutex> lk(tracked_orders_mutex_);
        auto& vec = (side == Side::BUY) ? tracked_bids_ : tracked_asks_;
        // Reuse a dead/empty slot to keep the vector bounded (mirrors the previous
        // post-submit slot-selection logic); otherwise append a fresh leg.
        TrackedLeg* slot = nullptr;
        for (auto& t : vec) {
            if (t.state == MmLegState::CANCELLED) { slot = &t; break; }
        }
        if (!slot) {
            for (auto& t : vec) {
                if (t.local_oid == 0 && t.state != MmLegState::ADOPTED &&
                    t.state != MmLegState::PLACE_PENDING) {
                    slot = &t;
                    break;
                }
            }
        }
        if (!slot) {
            vec.emplace_back();
            slot = &vec.back();
        }
        slot->side = side;
        slot->state = MmLegState::PLACE_PENDING;
        slot->pending_place_client_id = cid;
        slot->local_oid = 0;
        slot->exchange_oid.clear();
        slot->exchange_px = px;
        slot->qty = qty;
        slot->remaining_qty = qty;
        slot->state_entered_steady_ms = mmSteadyMillis();
    }
    ++mm_pending_accepts_;
    mm_pending_accepts_last_change_ms_.store(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now().time_since_epoch())
            .count(),
        std::memory_order_relaxed);
    return cid;
}

void MakeMarketStrategy::mmUnreservePendingPlace(Side side, const std::string& client_order_id) {
    if (client_order_id.empty()) {
        return;
    }
    bool undid = false;
    {
        std::lock_guard<std::mutex> lk(tracked_orders_mutex_);
        auto& vec = (side == Side::BUY) ? tracked_bids_ : tracked_asks_;
        for (auto& t : vec) {
            // Only undo if the reservation is still pending (i.e. no accept landed).
            // If an inline/async accept already matched it, it will be ADOPTED and
            // we must NOT touch it here.
            if (t.state == MmLegState::PLACE_PENDING && t.pending_place_client_id == client_order_id) {
                t.state = MmLegState::CANCELLED;
                t.pending_place_client_id.clear();
                t.local_oid = 0;
                undid = true;
                break;
            }
        }
    }
    if (undid && mm_pending_accepts_ > 0) {
        --mm_pending_accepts_;
        mm_pending_accepts_last_change_ms_.store(
            std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now().time_since_epoch())
                .count(),
            std::memory_order_relaxed);
    }
    // Drop any reason we registered for this cid so it can't leak.
    {
        std::lock_guard<std::mutex> lk(mm_accept_pending_mutex_);
        mm_pending_accept_cycle_reason_.erase(client_order_id);
    }
}

std::string MakeMarketStrategy::mmPlaceReservedLeg(Side side, core::Price px, core::Quantity qty,
                                                   OrderRequest req, long long cap_ll,
                                                   const std::string& reason, bool defer_wire,
                                                   OrderId* out_order_id) {
    if (out_order_id) {
        *out_order_id = 0;
    }
    // Reserve BEFORE submit so the inline ORDER_ACCEPTED (dispatched during
    // submit_order on this same thread) matches the leg and balances the counter.
    // In defer_wire mode the reservation still happens first; the wire (and thus the
    // accept) is fired later by the batch caller — order preserved because ALL legs
    // are reserved before ANY wire fire.
    req.client_order_id = mmReservePendingPlace(side, px, qty);
    req.strategy_name = getName();
    registerMmOrderForReloadCountBump(req.client_order_id, reason);
    {
        std::lock_guard<std::mutex> lk(order_submit_times_mutex_);
        order_submit_times_[req.client_order_id] = std::chrono::steady_clock::now();
    }

    const std::string cid = mmSubmitOrderWithProductCapGate(this, req, side, qty, cap_ll, reason,
                                                            defer_wire, out_order_id);
    if (cid.empty()) {
        // Gate refused or REST failed → no accept will land. Undo everything.
        mmUnreservePendingPlace(side, req.client_order_id);
        std::lock_guard<std::mutex> lk(order_submit_times_mutex_);
        order_submit_times_.erase(req.client_order_id);
        return {};
    }
    return cid;
}

std::string MakeMarketStrategy::mmSubmitOrderWithProductCapGate(MakeMarketStrategy* mm,
                                                                const OrderRequest& req,
                                                                Side side,
                                                                Quantity qty,
                                                                long long cap_ll,
                                                                const std::string& reason,
                                                                bool defer_wire,
                                                                OrderId* out_order_id) {
    if (out_order_id) {
        *out_order_id = 0;
    }
    if (!mm) {
        return {};
    }
    (void)qty;
    const std::string ax = mm->mmAxSymbol();

    // PLACE-FUNNEL THREAD ASSERT (Phase A precondition, 2026-07). Every venue place must run on
    // THIS symbol's per-AX mover worker under sharding. Per-symbol worker serialization is the
    // invariant that subsumes the product_placement_lease's cross-stack coordination — which is
    // why the lease can be neutralized in enforce+sharded (see mmProductPlacementWaitBudget*).
    // Debug builds hard-assert; release logs a throttled WARN (never abort a live engine) so a
    // stray off-mover submit is surfaced without taking the desk down.
    if (MmOrderMover::getInstance().shardingResolvedOn() &&
        MmOrderMover::isCurrentThreadMover() &&
        !MmOrderMover::isCurrentThreadMoverForAx(ax)) {
        // On a mover worker, sharding ON, but a DIFFERENT AX's worker — a cross-AX submit that
        // violates per-AX serialization (the invariant the lease neutralization relies on).
        // (Non-mover callers — tests / synchronous startup — are intentionally not flagged.)
        assert(false && "mm place funnel ran on the wrong AX mover worker");
        if (utils::Logger::isInitialized()) {
            Main().logger()->warn(
                "[MM_PLACE_THREAD_ASSERT] ax={} strategy={} — submit funnel on a DIFFERENT AX's "
                "mover worker (expected this symbol's serial worker); investigate cross-AX routing",
                ax, mm->getName());
        }
    }

    auto& gate = mmProductGateFor(ax);
    {
        std::lock_guard<std::mutex> glk(gate.mu);
        if (gate.lease_owner != mm->getName()) {
            if (utils::Logger::isInitialized()) {
                Main().logger()->warn(
                    "[MM_ORDERS] product_submit_blocked ax={} strategy={} lease_owner={} "
                    "reason={} — no submit without exclusive product lease",
                    ax,
                    mm->getName(),
                    gate.lease_owner.empty() ? std::string("(none)") : gate.lease_owner,
                    reason);
            }
            return {};
        }
    }

    const long long net_po_for_gate = mmProductNetForCapGateAtSubmit(mm);

    if (!mm->shouldQuoteSide(side, true)) {
        if (utils::Logger::isInitialized()) {
            Main().logger()->warn(
                "[STRATEGY:{}] MM_PRODUCT_SUBMIT_SIDE_BLOCKED reason={} sym={} side={} "
                "net_po={} max_po={} — shouldQuoteSide refused (reduce-only / reload / cap)",
                mm->getName(),
                reason,
                ax,
                side == Side::BUY ? "BUY" : "SELL",
                net_po_for_gate,
                cap_ll);
        }
        return {};
    }

    const bool buy_is_grow = (net_po_for_gate >= 0);
    const bool sell_is_grow = (net_po_for_gate <= 0);
    if (side == Side::BUY && buy_is_grow && net_po_for_gate >= cap_ll) {
        if (utils::Logger::isInitialized()) {
            Main().logger()->warn(
                "[STRATEGY:{}] MM_PRODUCT_SUBMIT_BID_HARD_CAP reason={} sym={} net_po={} max_po={}",
                mm->getName(), reason, ax, net_po_for_gate, cap_ll);
        }
        return {};
    }
    if (side == Side::SELL && sell_is_grow && -net_po_for_gate >= cap_ll) {
        if (utils::Logger::isInitialized()) {
            Main().logger()->warn(
                "[STRATEGY:{}] MM_PRODUCT_SUBMIT_ASK_HARD_CAP reason={} sym={} net_po={} max_po={}",
                mm->getName(), reason, ax, net_po_for_gate, cap_ll);
        }
        return {};
    }

    if (defer_wire) {
        OrderId oid = 0;
        std::string cid = mm->submit_order_no_dispatch(req, oid);
        if (out_order_id) {
            *out_order_id = oid;
        }
        return cid;
    }
    return mm->submit_order(req);
}

long long MakeMarketStrategy::mmInstrumentNetPoFromPortfolioCache(const std::string& ax_symbol) {
    if (ax_symbol.empty()) {
        return 0;
    }
    auto opt = portfolio::PortfolioManager::getInstance().getPosition(ax_symbol);
    if (!opt.has_value() || !opt->isOpen()) {
        return 0;
    }
    const Quantity signed_qty = opt->isLong() ? opt->quantity : -opt->quantity;
    return static_cast<long long>(std::llround(signed_qty));
}

core::Quantity MakeMarketStrategy::sumRestingLegQtyForSymbol(const std::string& ax_symbol,
                                                             core::Side side) {
    if (ax_symbol.empty()) {
        return 0.0;
    }
    core::Quantity total = 0.0;
    for (const auto& sp : StrategyManager::getInstance().getAllStrategies()) {
        auto mm = std::dynamic_pointer_cast<MakeMarketStrategy>(sp);
        if (!mm || mm->mmAxSymbol() != ax_symbol) {
            continue;
        }
        std::lock_guard<std::mutex> lk(mm->tracked_orders_mutex_);
        const auto& vec = (side == core::Side::BUY) ? mm->tracked_bids_ : mm->tracked_asks_;
        for (const auto& t : vec) {
            if (t.state == MmLegState::ADOPTED || t.state == MmLegState::PLACE_PENDING) {
                const core::Quantity q = t.remaining_qty > 0.0 ? t.remaining_qty : t.qty;
                if (q > 0.0) {
                    total += q;
                }
            }
        }
    }
    return total;
}

void MakeMarketStrategy::mmEvictExchangeOidAcrossProductStacks(const std::string& ax_symbol,
                                                             const std::string& oid) {
    if (ax_symbol.empty() || oid.empty()) {
        return;
    }
    for (auto& mm_sp : collectMakeMarketStrategySharedPtrsOnAx(ax_symbol)) {
        auto* mm = mm_sp.get();
        {
            // C2: hold tracked_orders_mutex_ across BOTH the erase and the findTrackedByOrderId
            // probes (the helper has no internal lock by contract) so the legacy-id resets cannot
            // race a concurrent mutation of tracked_bids_/tracked_asks_ on the OM thread.
            std::lock_guard<std::mutex> lk(mm->tracked_orders_mutex_);
            auto erase_match = [&](std::vector<TrackedLeg>& vec) {
                vec.erase(std::remove_if(vec.begin(), vec.end(),
                                         [&](const TrackedLeg& t) { return t.exchange_oid == oid; }),
                          vec.end());
            };
            erase_match(mm->tracked_bids_);
            erase_match(mm->tracked_asks_);

            Side side_out = Side::BUY;
            if (mm->bid_order_id_ != 0 &&
                mm->findTrackedByOrderId(mm->bid_order_id_, &side_out) == nullptr) {
                mm->bid_order_id_ = 0;
                mm->last_bid_price_ = 0.0;
            }
            if (mm->ask_order_id_ != 0 &&
                mm->findTrackedByOrderId(mm->ask_order_id_, &side_out) == nullptr) {
                mm->ask_order_id_ = 0;
                mm->last_ask_price_ = 0.0;
            }
        }
        mm->refreshLegacyTopOfBookTrackingFromVectors();
    }
}

bool MakeMarketStrategy::mmVenueCancelOidForProductConverge(const std::string& ax_symbol,
                                                            const std::string& oid,
                                                            const char* reason_tag) {
    if (oid.empty() || ax_symbol.empty()) {
        return false;
    }
    const std::string owner_ax = mmDeskOwnerAxForExchangeOid(oid);
    if (!owner_ax.empty() && owner_ax != ax_symbol) {
        if (utils::Logger::isInitialized()) {
            Main().logger()->warn(
                "[MM_PRODUCT_CONVERGE] sym={} refuse venue cancel oid={} owner_ax={} reason={} "
                "— OID tracked on another instrument",
                ax_symbol,
                oid,
                owner_ax,
                reason_tag ? reason_tag : "");
        }
        return false;
    }
    api::HttpResponse cr;
    {
        nlohmann::json cancel_body = nlohmann::json::object();
        cancel_body["oid"] = oid;
        cr = Main().rest()->cancelOrderGateway(cancel_body.dump());
    }
    if (!cr.is_success) {
        cr = Main().rest()->cancelOrder(oid);
    }
    const bool ok = cr.is_success || mmIsBenignCancelFailure(cr);
    if (utils::Logger::isInitialized()) {
        Main().logger()->info(
            "[MM_PRODUCT_CONVERGE] venue_cancel ax={} oid={} http={} ok={} reason={}",
            ax_symbol,
            oid,
            cr.status_code,
            ok ? 1 : 0,
            reason_tag ? reason_tag : "");
    }
    if (ok) {
        mmEvictExchangeOidAcrossProductStacks(ax_symbol, oid);
    }
    return ok;
}

double MakeMarketStrategy::mmDeskStackDesiredPriceForSide(Side side) const {
    const Price legacy = (side == Side::BUY) ? last_bid_price_.load() : last_ask_price_.load();
    if (legacy > 0.0 && std::isfinite(legacy)) {
        return legacy;
    }
    const OrderId tid = (side == Side::BUY) ? bid_order_id_ : ask_order_id_;
    if (tid != 0) {
        std::lock_guard<std::mutex> lk(tracked_orders_mutex_);
        const auto& vec = (side == Side::BUY) ? tracked_bids_ : tracked_asks_;
        for (const auto& t : vec) {
            if (t.local_oid == tid && t.exchange_px > 0.0 && std::isfinite(t.exchange_px)) {
                return t.exchange_px;
            }
        }
    }
    return 0.0;
}

std::string MakeMarketStrategy::mmDeskStackExchangeOidForSide(Side side) const {
    const OrderId tid = (side == Side::BUY) ? bid_order_id_ : ask_order_id_;
    if (tid != 0) {
        std::lock_guard<std::mutex> lk(tracked_orders_mutex_);
        const auto& vec = (side == Side::BUY) ? tracked_bids_ : tracked_asks_;
        for (const auto& t : vec) {
            if (t.local_oid == tid && !t.exchange_oid.empty()) {
                return t.exchange_oid;
            }
        }
        auto& om = orders::OrderManager::getInstance();
        const auto op = om.getOrder(tid);
        if (op && !op->exchange_order_id.empty()) {
            return op->exchange_order_id;
        }
    }
    if (const auto ms = mmOptionalManualStack(); ms.has_value()) {
        const auto& st = *ms;
        if (side == Side::BUY && !st.bid_exchange_oid.empty()) {
            return st.bid_exchange_oid;
        }
        if (side == Side::SELL && !st.ask_exchange_oid.empty()) {
            return st.ask_exchange_oid;
        }
    }
    return {};
}

bool MakeMarketStrategy::mmStackTracksExchangeOid(const std::string& oid) const {
    if (oid.empty()) {
        return false;
    }
    if (mmDeskStackExchangeOidForSide(Side::BUY) == oid) {
        return true;
    }
    if (mmDeskStackExchangeOidForSide(Side::SELL) == oid) {
        return true;
    }
    std::lock_guard<std::mutex> lk(tracked_orders_mutex_);
    for (const auto& t : tracked_bids_) {
        if (t.exchange_oid == oid) {
            return true;
        }
    }
    for (const auto& t : tracked_asks_) {
        if (t.exchange_oid == oid) {
            return true;
        }
    }
    return false;
}

std::string MakeMarketStrategy::mmDeskOwnerAxForExchangeOid(const std::string& oid) {
    if (oid.empty()) {
        return {};
    }
    for (const auto& sp : StrategyManager::getInstance().getAllStrategies()) {
        auto mm = std::dynamic_pointer_cast<MakeMarketStrategy>(sp);
        if (!mm || mm->mmOrderRequestId().empty()) {
            continue;
        }
        if (mm->mmStackTracksExchangeOid(oid)) {
            return mm->mmAxSymbol();
        }
    }
    return {};
}

namespace {
bool mmVenueRowMatchesSide(const MmVenueOpenRow& row, Side side) {
    return (side == Side::BUY) ? row.is_buy : !row.is_buy;
}
}  // namespace

std::unordered_set<std::string> MakeMarketStrategy::mmBuildProtectOidsWithTieBreak(
    const std::vector<MakeMarketStrategy*>& desk_mms,
    const std::vector<MmVenueOpenRow>& venue_rows,
    bool at_cap,
    Side reduce_side) {
    std::unordered_set<std::string> protect;
    std::unordered_set<std::string> claimed;
    protect.reserve(desk_mms.size() * 2 + 4);
    auto& cfg = config::Config::getInstance();
    const double default_quote_tick =
        desk_mms.empty() ? 0.0
                         : desk_mms.front()->resolveAxQuoteTick(cfg.getMarketMakerPriceTick());

    auto pick_for_stack_side = [&](MakeMarketStrategy* mm, Side side) -> std::string {
        const std::string tracked_oid = mm->mmDeskStackExchangeOidForSide(side);
        const double desired_px = mm->mmDeskStackDesiredPriceForSide(side);
        const double quote_tick = mm->resolveAxQuoteTick(cfg.getMarketMakerPriceTick());
        const double px_tol =
            (quote_tick > 0.0) ? (quote_tick * 0.51)
                                 : ((default_quote_tick > 0.0) ? default_quote_tick * 0.51 : 1e-6);

        const MmVenueOpenRow* tracked_row = nullptr;
        std::vector<const MmVenueOpenRow*> candidates;
        for (const auto& row : venue_rows) {
            if (!mmVenueRowMatchesSide(row, side)) {
                continue;
            }
            if (claimed.count(row.oid) > 0) {
                continue;
            }
            candidates.push_back(&row);
            if (!tracked_oid.empty() && row.oid == tracked_oid) {
                tracked_row = &row;
            }
        }
        if (tracked_row != nullptr) {
            return tracked_row->oid;
        }
        if (desired_px > 0.0 && std::isfinite(desired_px)) {
            const MmVenueOpenRow* best = nullptr;
            double best_d = std::numeric_limits<double>::max();
            for (const MmVenueOpenRow* row : candidates) {
                if (!(row->price > 0.0) || !std::isfinite(row->price)) {
                    continue;
                }
                const double d = std::abs(row->price - desired_px);
                if (d < best_d) {
                    best_d = d;
                    best = row;
                }
            }
            if (best != nullptr && best_d <= px_tol) {
                return best->oid;
            }
        }
        return {};
    };

    if (at_cap) {
        for (auto* mm : desk_mms) {
            const std::string oid = pick_for_stack_side(mm, reduce_side);
            if (!oid.empty()) {
                protect.insert(oid);
                claimed.insert(oid);
            }
        }
        return protect;
    }
    for (auto* mm : desk_mms) {
        for (const Side side : {Side::BUY, Side::SELL}) {
            const std::string oid = pick_for_stack_side(mm, side);
            if (!oid.empty()) {
                protect.insert(oid);
                claimed.insert(oid);
            }
        }
    }
    return protect;
}

void MakeMarketStrategy::mmConvergeProductBookOnAx(const std::string& ax_symbol,
                                                    long long net_po,
                                                    long long cap_ll,
                                                    const char* reason,
                                                    MakeMarketStrategy* cycle_mutex_held) {
    if (ax_symbol.empty() || cap_ll <= 0 || !reason) {
        return;
    }
    // ENFORCE: the mover never diffs against the venue (this function GETs /open-orders).
    // Cap enforcement is snapshot-driven (mmEnforceCapFromSnapshotOnSymbol, which cancels
    // grow-side legs via enqueueRestCancelOnMover and never fetches) plus local effective()
    // gates. Defense-in-depth: every mover-reachable caller is also guarded, but this is the
    // single funnel for the venue-diff GET, so guard it here too. off/shadow unaffected.
    if (mmPbModeForSymbol(ax_symbol) == PbMode::Enforce) {
        return;
    }
    // Pin shared_ptrs for the whole converge REST window — unregister of a sibling stack
    // during venue fetch/cancel must not leave raw pointers dangling.
    std::vector<std::shared_ptr<MakeMarketStrategy>> mms_pin;
    mms_pin.reserve(8);
    for (const auto& sp : StrategyManager::getInstance().getAllStrategies()) {
        auto mm = std::dynamic_pointer_cast<MakeMarketStrategy>(sp);
        if (!mm || mm->mmAxSymbol() != ax_symbol) {
            continue;
        }
        mms_pin.push_back(mm);
    }
    std::sort(mms_pin.begin(), mms_pin.end());
    if (mms_pin.empty()) {
        return;
    }
    std::vector<MakeMarketStrategy*> mms;
    mms.reserve(mms_pin.size());
    for (const auto& sp : mms_pin) {
        mms.push_back(sp.get());
    }
    MakeMarketStrategy* ref_mm = nullptr;
    for (auto* mm : mms) {
        if (!mm->mmOrderRequestId().empty()) {
            ref_mm = mm;
            break;
        }
    }
    if (!ref_mm) {
        ref_mm = mms.front();
    }

    const bool lease_ok = mmAcquireProductPlacementLeaseForConverge(ref_mm);
    struct ConvergeLeaseRelease {
        MakeMarketStrategy* mm{nullptr};
        bool held{false};
        ~ConvergeLeaseRelease() {
            if (held && mm) {
                mmReleaseProductPlacementLease(mm);
            }
        }
    } lease_guard{ref_mm, lease_ok};
    if (!lease_ok) {
        if (net_po >= cap_ll) {
            (void)mmCancelTrackedLegAllStacksOnAx(ax_symbol, Side::BUY, cycle_mutex_held);
        }
        if (-net_po >= cap_ll) {
            (void)mmCancelTrackedLegAllStacksOnAx(ax_symbol, Side::SELL, cycle_mutex_held);
        }
        return;
    }

    auto stack_locks = mmAcquireAllStacksCycleLocks(ax_symbol, cycle_mutex_held);

    std::vector<MakeMarketStrategy*> desk_mms;
    desk_mms.reserve(mms.size());
    for (auto* mm : mms) {
        if (!mm->mmOrderRequestId().empty()) {
            desk_mms.push_back(mm);
        }
    }
    const int desk_stack_count = static_cast<int>(desk_mms.size());

    const bool at_cap = (std::llabs(net_po) >= cap_ll);
    const Side grow_side =
        (net_po > 0) ? Side::BUY : (net_po < 0 ? Side::SELL : Side::BUY);
    const Side reduce_side = (grow_side == Side::BUY) ? Side::SELL : Side::BUY;

    std::vector<MmVenueOpenRow> venue_rows;
    const bool venue_ok = mmFetchVenueOpenRowsForAx(ax_symbol, venue_rows);
    const std::unordered_set<std::string> protect_oids =
        venue_ok ? mmBuildProtectOidsWithTieBreak(desk_mms, venue_rows, at_cap, reduce_side)
                 : std::unordered_set<std::string>{};

    int venue_cancel_grow = 0;
    int venue_cancel_orphan = 0;
    int venue_skip_cross_ax = 0;

    if (venue_ok) {
        for (const auto& row : venue_rows) {
            const std::string owner_ax = MakeMarketStrategy::mmDeskOwnerAxForExchangeOid(row.oid);
            if (!owner_ax.empty() && owner_ax != ax_symbol) {
                ++venue_skip_cross_ax;
                if (utils::Logger::isInitialized()) {
                    Main().logger()->warn(
                        "[MM_PRODUCT_CONVERGE] sym={} skip venue cancel oid={} owner_ax={} "
                        "reason={} — oid tracked on another instrument",
                        ax_symbol,
                        row.oid,
                        owner_ax,
                        reason);
                }
                continue;
            }
            const bool row_is_buy = row.is_buy;
            const bool row_is_grow =
                at_cap && ((grow_side == Side::BUY) ? row_is_buy : !row_is_buy);
            const bool row_is_reduce =
                at_cap && ((reduce_side == Side::BUY) ? row_is_buy : !row_is_buy);

            if (row_is_grow) {
                if (mmVenueCancelOidForProductConverge(ax_symbol, row.oid, reason)) {
                    ++venue_cancel_grow;
                }
                continue;
            }
            if (row_is_reduce || !at_cap) {
                if (protect_oids.count(row.oid) == 0) {
                    if (mmVenueCancelOidForProductConverge(ax_symbol, row.oid, reason)) {
                        ++venue_cancel_orphan;
                    }
                }
            }
        }
    }

    int tracked_grow_cancelled = 0;
    if (at_cap) {
        if (net_po >= cap_ll) {
            tracked_grow_cancelled +=
                mmCancelTrackedLegAllStacksOnAx(ax_symbol, Side::BUY, cycle_mutex_held);
        }
        if (-net_po >= cap_ll) {
            tracked_grow_cancelled +=
                mmCancelTrackedLegAllStacksOnAx(ax_symbol, Side::SELL, cycle_mutex_held);
        }
        for (auto* mm : mms) {
            mm->mmResetLocalOrderTrackingOnSide(grow_side);
            mm->noteReduceOnlyState(net_po, cap_ll);
        }
    } else {
        for (auto* mm : mms) {
            mm->noteReduceOnlyState(net_po, cap_ll);
        }
    }

    if (utils::Logger::isInitialized()) {
        Main().logger()->info(
            "[MM_PRODUCT_CONVERGE] reason={} ax={} net_po={} max_po={} at_cap={} "
            "desk_stacks={} venue_rows={} venue_ok={} protect_oids={} "
            "venue_cancel_grow={} venue_cancel_orphan={} venue_skip_cross_ax={} "
            "tracked_grow_cancelled={} tie_break=tracked_then_desired_px — lease-gated venue diff",
            reason,
            ax_symbol,
            net_po,
            cap_ll,
            at_cap ? 1 : 0,
            desk_stack_count,
            venue_rows.size(),
            venue_ok ? 1 : 0,
            protect_oids.size(),
            venue_cancel_grow,
            venue_cancel_orphan,
            venue_skip_cross_ax,
            tracked_grow_cancelled);
    }
}

void MakeMarketStrategy::mmPullGrowSideOrdersAtOrPastCap(const std::string& ax_symbol,
                                                         long long net_po,
                                                         long long cap_ll,
                                                         const char* reason,
                                                         MakeMarketStrategy* cycle_mutex_held) {
    if (ax_symbol.empty() || cap_ll <= 0 || !reason) {
        return;
    }
    mmConvergeProductBookOnAx(ax_symbol, net_po, cap_ll, reason, cycle_mutex_held);
}

void MakeMarketStrategy::mmAssessTimCapAtFillInstant(const std::string& ax_symbol,
                                                     long long net_po_after_fill,
                                                     long long cap_ll,
                                                     long long resting_grow_qty_before_fill,
                                                     Quantity fill_qty,
                                                     const char* reason) {
    if (ax_symbol.empty() || cap_ll <= 0 || !reason) {
        return;
    }
    const long long abs_net = std::llabs(net_po_after_fill);
    if (abs_net <= cap_ll) {
        mmClearTimCapBurstLatch(ax_symbol);
        return;
    }
    const long long tim_fill_bound = cap_ll + std::max<long long>(0, resting_grow_qty_before_fill);
    const long long fill_lots = static_cast<long long>(std::llround(std::max(0.0, fill_qty)));
    if (!utils::Logger::isInitialized()) {
        if (abs_net <= tim_fill_bound) {
            std::lock_guard<std::mutex> lk(g_mm_tim_cap_burst_mu);
            mmTimCapBurstLatchFor(ax_symbol).acceptable_burst = true;
        }
        return;
    }
    if (abs_net <= tim_fill_bound) {
        {
            std::lock_guard<std::mutex> lk(g_mm_tim_cap_burst_mu);
            mmTimCapBurstLatchFor(ax_symbol).acceptable_burst = true;
        }
        Main().logger()->info(
            "[MM_CAP] BREACH_ACCEPTABLE reason={} ax={} net_po={} |net|={} max_po={} "
            "resting_grow_side_qty_before_fill={} fill_qty={} tim_fill_bound={} — Tim fill-instant: "
            "|net| may exceed max only up to max + qty that was resting when fills landed",
            reason,
            ax_symbol,
            net_po_after_fill,
            abs_net,
            cap_ll,
            resting_grow_qty_before_fill,
            fill_lots,
            tim_fill_bound);
    } else {
        mmClearTimCapBurstLatch(ax_symbol);
        Main().logger()->warn(
            "[MM_CAP] BREACH_UNEXPECTED reason={} ax={} net_po={} |net|={} max_po={} "
            "resting_grow_side_qty_before_fill={} fill_qty={} tim_fill_bound={} — exceeds Tim "
            "fill-instant bound (Joe: over max + what was on book at fill time)",
            reason,
            ax_symbol,
            net_po_after_fill,
            abs_net,
            cap_ll,
            resting_grow_qty_before_fill,
            fill_lots,
            tim_fill_bound);
    }
}

void MakeMarketStrategy::mmLogTimCapSteadyStateAfterPull(const std::string& ax_symbol,
                                                         long long net_po,
                                                         long long cap_ll,
                                                         const char* reason) {
    if (ax_symbol.empty() || cap_ll <= 0 || !reason) {
        return;
    }
    const long long abs_net = std::llabs(net_po);
    if (abs_net <= cap_ll) {
        mmClearTimCapBurstLatch(ax_symbol);
        return;
    }
    bool latched_acceptable = false;
    {
        std::lock_guard<std::mutex> lk(g_mm_tim_cap_burst_mu);
        auto it = g_mm_tim_cap_burst_by_ax.find(ax_symbol);
        if (it != g_mm_tim_cap_burst_by_ax.end()) {
            latched_acceptable = it->second.acceptable_burst;
        }
    }
    if (!utils::Logger::isInitialized()) {
        return;
    }
    if (latched_acceptable) {
        // Post-pull resting=0 is expected: resting converted to position. Reduce-only until |net|<=max.
        Main().logger()->info(
            "[MM_CAP] REDUCE_ONLY_OVER_CAP reason={} ax={} net_po={} |net|={} max_po={} — "
            "fill-time ACCEPTABLE burst latched; resting now 0 after pull (not re-checked vs "
            "max+resting); flatten via reduce side only",
            reason,
            ax_symbol,
            net_po,
            abs_net,
            cap_ll);
        return;
    }
    const core::Side grow_side = (net_po > 0) ? Side::BUY : Side::SELL;
    const long long resting_now = static_cast<long long>(
        std::llround(sumWorstCaseInflightLegQtyForSymbol(ax_symbol, grow_side, nullptr)));
    const long long tim_worst_bound = cap_ll + resting_now;
    Main().logger()->warn(
        "[MM_CAP] BREACH_UNEXPECTED reason={} ax={} net_po={} |net|={} max_po={} "
        "resting_grow_side_qty_now={} tim_worst_bound={} — exceeds Tim fill-instant bound with "
        "no prior ACCEPTABLE latch (Joe: phantom/stale/hidden size)",
        reason,
        ax_symbol,
        net_po,
        abs_net,
        cap_ll,
        resting_now,
        tim_worst_bound);
}

void MakeMarketStrategy::mmPruneCrossStackGrowSideToCap(const std::string& ax_symbol,
                                                        Side side,
                                                        long long net_po,
                                                        long long cap_ll,
                                                        MakeMarketStrategy* cycle_mutex_held) {
    if (ax_symbol.empty() || cap_ll <= 0) {
        return;
    }
    long long allowed_qty = 0;
    if (side == Side::BUY) {
        if (net_po < 0) {
            return;
        }
        allowed_qty = cap_ll - std::max<long long>(0, net_po);
    } else {
        if (net_po > 0) {
            return;
        }
        allowed_qty = cap_ll - std::max<long long>(0, -net_po);
    }
    if (allowed_qty <= 0) {
        (void)mmCancelTrackedLegAllStacksOnAx(ax_symbol, side, cycle_mutex_held);
        if (utils::Logger::isInitialized()) {
            Main().logger()->info(
                "[MM_ORDERS] cross_stack_grow_prune ax={} side={} net_po={} cap={} "
                "allowed_grow_qty=0 — cancelled all grow-side legs on product",
                ax_symbol,
                side == Side::BUY ? "BUY" : "SELL",
                net_po,
                cap_ll);
        }
        return;
    }

    struct LegRow {
        MakeMarketStrategy* mm{nullptr};
        core::OrderId local_oid{0};
        std::int64_t entered_ms{0};
        core::Quantity qty{0.0};
    };
    std::vector<LegRow> rows;
    rows.reserve(16);
    // Pin shared_ptrs for the lifetime of `rows` (which holds raw mm pointers used below) so a
    // concurrent sibling teardown cannot free a stack we later deref (UAF guard, 2026-06-11).
    auto mms_pin = collectMakeMarketStrategySharedPtrsOnAx(ax_symbol);
    for (auto& mm_sp : mms_pin) {
        auto* mm = mm_sp.get();
        std::lock_guard<std::mutex> lk(mm->tracked_orders_mutex_);
        const auto& vec = (side == Side::BUY) ? mm->tracked_bids_ : mm->tracked_asks_;
        for (const auto& t : vec) {
            if (t.state != MmLegState::ADOPTED && t.state != MmLegState::PLACE_PENDING &&
                !mmLegStateIsCancelInFlight(t.state)) {
                continue;
            }
            const core::Quantity q = t.remaining_qty > 0.0 ? t.remaining_qty : t.qty;
            if (q <= 0.0) {
                continue;
            }
            rows.push_back(
                LegRow{mm, t.local_oid, t.state_entered_steady_ms, q});
        }
    }
    if (rows.empty()) {
        return;
    }
    std::sort(rows.begin(), rows.end(), [](const LegRow& a, const LegRow& b) {
        if (a.local_oid != b.local_oid) {
            return a.local_oid > b.local_oid;
        }
        return a.entered_ms > b.entered_ms;
    });

    long long kept_qty = 0;
    int cancelled = 0;
    for (const LegRow& row : rows) {
        const long long q_ll = static_cast<long long>(std::llround(row.qty));
        if (q_ll <= 0) {
            continue;
        }
        if (kept_qty + q_ll <= allowed_qty) {
            kept_qty += q_ll;
            continue;
        }
        const OrderId tid =
            (side == Side::BUY) ? row.mm->bid_order_id_ : row.mm->ask_order_id_;
        if (tid != 0 && row.mm->mmCancelTrackedLegThisStackGateway(side) == 1) {
            row.mm->mmResetLocalOrderTrackingOnSide(side);
            ++cancelled;
        }
    }
    if (cancelled > 0 && utils::Logger::isInitialized()) {
        Main().logger()->warn(
            "[MM_ORDERS] cross_stack_grow_prune ax={} side={} net_po={} cap={} "
            "allowed_grow_qty={} kept_working_qty={} cancelled_stacks={} "
            "(newest local_oid wins; older grow-side legs removed)",
            ax_symbol,
            side == Side::BUY ? "BUY" : "SELL",
            net_po,
            cap_ll,
            allowed_qty,
            kept_qty,
            cancelled);
    }
}

core::Quantity MakeMarketStrategy::sumWorkingLegQtyForSymbol(
    const std::string& ax_symbol, core::Side side) {
    return sumRestingLegQtyForSymbol(ax_symbol, side);
}

core::Quantity MakeMarketStrategy::sumWorstCaseInflightLegQtyForSymbol(
    const std::string& ax_symbol,
    core::Side side,
    MakeMarketStrategy* exclude_own_cancel_pending_on_side) {
    if (ax_symbol.empty()) {
        return 0.0;
    }
    core::Quantity total = 0.0;
    for (const auto& sp : StrategyManager::getInstance().getAllStrategies()) {
        auto mm = std::dynamic_pointer_cast<MakeMarketStrategy>(sp);
        if (!mm || mm->mmAxSymbol() != ax_symbol) {
            continue;
        }
        std::lock_guard<std::mutex> lk(mm->tracked_orders_mutex_);
        const auto& vec = (side == core::Side::BUY) ? mm->tracked_bids_ : mm->tracked_asks_;
        for (const auto& t : vec) {
            // Worst-case exposure: resting, in-flight place, and cancel-not-yet-acked
            // (both CANCEL_PENDING and the fire-and-track CANCEL_SENT_UNCONFIRMED —
            // an unconfirmed cancel might still be live on the venue).
            if (t.state == MmLegState::ADOPTED || t.state == MmLegState::PLACE_PENDING ||
                mmLegStateIsCancelInFlight(t.state)) {
                // Only the classic ack-wait cancel may be excluded by a caller; a
                // fire-and-track unconfirmed leg is ALWAYS counted (pessimistic).
                if (exclude_own_cancel_pending_on_side == mm.get() &&
                    t.state == MmLegState::CANCEL_PENDING) {
                    continue;
                }
                const core::Quantity q =
                    t.remaining_qty > 0.0 ? t.remaining_qty : t.qty;
                if (q > 0.0) {
                    total += q;
                }
            }
        }
    }
    return total;
}

void MakeMarketStrategy::syncExchangeNetForAllMakeMarketOnSymbol(
    const std::string& ax_symbol,
    MakeMarketStrategy* cycle_mutex_held,
    bool enforce_cap_after) {
    if (ax_symbol.empty()) {
        return;
    }
    // ENFORCE: the shared book already carries venue truth (fills + 30s snapshots), so
    // the fill-path REST refresh + per-stack syncInventory are neutralized (no pf->refresh()).
    // Cap enforcement, if requested, runs from the book. off/shadow keep the legacy sync.
    if (mmPbModeForSymbol(ax_symbol) == PbMode::Enforce) {
        if (enforce_cap_after) {
            mmEnforceCapFromSnapshotOnSymbol(ax_symbol);
        }
        return;
    }
    // Collect and lock in fixed pointer order so L1–L3 cannot deadlock; skip `cycle_mutex_held` (caller
    // already holds that leg's `mm_cycle_mutex_`, e.g. fill path) or lock all.
    // mms_pin retains ownership for the whole locking window so a concurrent sibling teardown cannot
    // free a stack whose raw pointer / cycle mutex we hold (UAF guard, 2026-06-11).
    auto mms_pin = collectMakeMarketStrategySharedPtrsOnAx(ax_symbol);
    std::vector<MakeMarketStrategy*> mms;
    mms.reserve(mms_pin.size());
    for (auto& mm_sp : mms_pin) {
        mms.push_back(mm_sp.get());
    }
    if (mms.empty()) {
        return;
    }
    std::sort(mms.begin(), mms.end());
    // One REST round-trip before we pin all legs, so a slow exchange poll does not hold every stack’s cycle lock.
    Main().portfolio()->refresh();
    for (auto* mm : mms) {
        if (mm == cycle_mutex_held) {
            continue;
        }
        mm->mm_cycle_mutex_.lock();
    }
    for (auto* mm : mms) {
        mm->syncInventoryFromExchangePortfolio(true);
    }
    for (auto it = mms.rbegin(); it != mms.rend(); ++it) {
        if (*it == cycle_mutex_held) {
            continue;
        }
        (*it)->mm_cycle_mutex_.unlock();
    }
    if (enforce_cap_after) {
        MakeMarketStrategy::mmEnforceProductCapOnSymbolAfterSync(ax_symbol);
    }
}

void MakeMarketStrategy::mmEnforceProductCapOnSymbolAfterSync(const std::string& ax_symbol) {
    if (ax_symbol.empty()) {
        return;
    }
    // mms_pin retains ownership for the whole enforce window (UAF guard, 2026-06-11).
    auto mms_pin = collectMakeMarketStrategySharedPtrsOnAx(ax_symbol);
    std::vector<MakeMarketStrategy*> mms;
    mms.reserve(mms_pin.size());
    for (auto& mm_sp : mms_pin) {
        mms.push_back(mm_sp.get());
    }
    if (mms.empty()) {
        return;
    }
    MakeMarketStrategy* ref = mms.front();
    auto& cfg = config::Config::getInstance();
    const long long cap_ll = static_cast<long long>(ref->mmEffMaxPositionInt(cfg));
    if (cap_ll <= 0) {
        return;
    }
    const long long net_po = mmInstrumentNetPoFromPortfolioCache(ax_symbol);
    for (auto* mm : mms) {
        std::lock_guard<std::mutex> ps_lk(mm->position_state_mutex_);
        mm->position_state_.net_position_qty = static_cast<Quantity>(net_po);
    }
    mmConvergeProductBookOnAx(ax_symbol, net_po, cap_ll, "post_sync_enforce", nullptr);
    mmLogTimCapSteadyStateAfterPull(ax_symbol, net_po, cap_ll, "post_sync_enforce");
}

// =============================================================================
// PositionBook wiring (2026-07, shadow-first, config-revertible)
// =============================================================================
namespace {

MakeMarketStrategy::PbMode pbParseMode(const std::string& s, MakeMarketStrategy::PbMode dflt) {
    if (s == "off") return MakeMarketStrategy::PbMode::Off;
    if (s == "shadow") return MakeMarketStrategy::PbMode::Shadow;
    if (s == "enforce") return MakeMarketStrategy::PbMode::Enforce;
    return dflt;
}

const char* pbModeName(MakeMarketStrategy::PbMode m) {
    switch (m) {
        case MakeMarketStrategy::PbMode::Off:     return "OFF";
        case MakeMarketStrategy::PbMode::Shadow:  return "SHADOW";
        case MakeMarketStrategy::PbMode::Enforce: return "ENFORCE";
    }
    return "SHADOW";
}

struct PbModeConfig {
    MakeMarketStrategy::PbMode global{MakeMarketStrategy::PbMode::Shadow};
    bool global_from_config{false};
    std::unordered_map<std::string, MakeMarketStrategy::PbMode> per_symbol;
};

// Resolved ONCE and frozen for the process lifetime (like the mover sharding mode)
// so a mid-session config reload can never change position-tracking behavior under a
// running quote cycle. Revert story is "edit config, RESTART".
const PbModeConfig& pbModeConfig() {
    static const PbModeConfig cfg = [] {
        PbModeConfig c;
        try {
            auto& conf = config::Config::getInstance();
            c.global_from_config = conf.has("market_maker.position_book_mode");
            c.global = pbParseMode(
                conf.getString("market_maker.position_book_mode", "shadow"),
                MakeMarketStrategy::PbMode::Shadow);
            const auto sec = conf.getSection("market_maker.position_book_mode_per_symbol");
            if (sec.is_object()) {
                for (auto it = sec.begin(); it != sec.end(); ++it) {
                    if (it.value().is_string()) {
                        c.per_symbol[it.key()] =
                            pbParseMode(it.value().get<std::string>(), c.global);
                    }
                }
            }
        } catch (...) {
            // Fail SAFE: default shadow (logs only, no behavior change), no overrides.
            c.global = MakeMarketStrategy::PbMode::Shadow;
            c.global_from_config = false;
            c.per_symbol.clear();
        }
        return c;
    }();
    return cfg;
}

}  // namespace

MakeMarketStrategy::PbMode MakeMarketStrategy::mmPbGlobalMode() {
    return pbModeConfig().global;
}

MakeMarketStrategy::PbMode MakeMarketStrategy::mmPbModeForSymbol(const std::string& ax_symbol) {
    const auto& c = pbModeConfig();
    const auto it = c.per_symbol.find(ax_symbol);
    return (it == c.per_symbol.end()) ? c.global : it->second;
}

bool MakeMarketStrategy::mmPbEnabledAnySymbol() {
    const auto& c = pbModeConfig();
    if (c.global != PbMode::Off) {
        return true;
    }
    for (const auto& kv : c.per_symbol) {
        if (kv.second != PbMode::Off) {
            return true;
        }
    }
    return false;
}

bool MakeMarketStrategy::mmPbEnforceAnySymbol() {
    const auto& c = pbModeConfig();
    if (c.global == PbMode::Enforce) {
        return true;
    }
    for (const auto& kv : c.per_symbol) {
        if (kv.second == PbMode::Enforce) {
            return true;
        }
    }
    return false;
}

std::int64_t MakeMarketStrategy::mmVenueCacheMaxAgeMs() {
    // Frozen once (like the modes): a mover reader that finds a cache older than this
    // logs VENUE_CACHE_STALE and proceeds on LOCAL order state — it never fetches inline.
    static const std::int64_t v = [] {
        try {
            return static_cast<std::int64_t>(std::max(
                1000, config::Config::getInstance().getInt("api.venue_cache_max_age_ms", 30000)));
        } catch (...) {
            return static_cast<std::int64_t>(30000);
        }
    }();
    return v;
}

// -----------------------------------------------------------------------------
// Main-loop background refresher for the enforce-mode VenueOrdersCache.
// One GET /open-orders per ENFORCE symbol per interval; publishes into the cache
// and runs the own-OID-filtered [VENUE_COUNT_MISMATCH] regression check here,
// where the fresh symbol-wide data (all stacks) is available. Off/shadow symbols
// are never touched.
// -----------------------------------------------------------------------------
void MakeMarketStrategy::mmRefreshVenueOrdersCacheAllEnforced() {
    // Enumerate the distinct AX symbols that are in ENFORCE mode right now.
    std::set<std::string> enforce_syms;
    for (const auto& sp : StrategyManager::getInstance().getAllStrategies()) {
        auto mm = std::dynamic_pointer_cast<MakeMarketStrategy>(sp);
        if (!mm) {
            continue;
        }
        const std::string ax = mm->mmAxSymbol();
        if (ax.empty()) {
            continue;
        }
        if (mmPbModeForSymbol(ax) == PbMode::Enforce) {
            enforce_syms.insert(ax);
        }
    }
    if (enforce_syms.empty()) {
        return;
    }

    for (const std::string& ax : enforce_syms) {
        std::vector<MmVenueOpenRow> rows;
        if (!mmFetchVenueOpenRowsForAx(ax, rows)) {
            // GET failed — leave the previous snapshot in place; consumers will age it
            // out to VENUE_CACHE_STALE and proceed on local state. Never block the loop.
            continue;
        }

        // Publish into the cache (mover-thread reads consume this; never fetch inline).
        std::vector<VenueCacheRow> cache_rows;
        cache_rows.reserve(rows.size());
        for (const auto& r : rows) {
            cache_rows.push_back(VenueCacheRow{r.oid, r.is_buy, r.price, 0.0});
        }
        VenueOrdersCache::instance().publish(ax, std::move(cache_rows));

        // [VENUE_COUNT_MISMATCH] regression detector — own-OID filtered across ALL stacks
        // on this symbol. A venue row owned (tracked) by ANY sibling stack is accounted for;
        // only rows tracked by NOBODY are genuine orphans. This kills the multi-stack cry-wolf.
        auto mms = collectMakeMarketStrategySharedPtrsOnAx(ax);
        if (mms.empty() || !utils::Logger::isInitialized()) {
            continue;
        }
        std::unordered_set<std::string> owned_oids;
        std::size_t tracked_resting_total = 0;
        for (const auto& mm : mms) {
            std::lock_guard<std::mutex> lk(mm->tracked_orders_mutex_);
            auto scan = [&](const std::vector<TrackedLeg>& vec) {
                for (const auto& t : vec) {
                    if (t.state == MmLegState::FILLED || t.state == MmLegState::CANCELLED) {
                        continue;
                    }
                    if (!t.exchange_oid.empty()) {
                        owned_oids.insert(t.exchange_oid);
                        ++tracked_resting_total;
                    }
                }
            };
            scan(mm->tracked_bids_);
            scan(mm->tracked_asks_);
        }
        std::size_t orphan_rows = 0;
        for (const auto& r : rows) {
            if (!r.oid.empty() && owned_oids.find(r.oid) == owned_oids.end()) {
                ++orphan_rows;
            }
        }
        if (orphan_rows > 0) {
            Main().logger()->warn(
                "[VENUE_COUNT_MISMATCH] ax={} kind=VENUE_ORPHAN(untracked_by_any_stack) "
                "venue_open={} tracked_resting_all_stacks={} orphan_rows={} stacks={} — "
                "rows on the venue owned by no local stack (own-OID filtered)",
                ax, rows.size(), tracked_resting_total, orphan_rows, mms.size());
        }
    }
}

void MakeMarketStrategy::mmLogPositionBookBootBannerOnce() {
    // Retry-until-logger-ready one-shot: only consumes the guard once the Logger is
    // live, so the banner is never silently dropped during early startup.
    static std::atomic<bool> logged{false};
    if (logged.load(std::memory_order_acquire)) {
        return;
    }
    if (!utils::Logger::isInitialized()) {
        return;
    }
    bool expected = false;
    if (!logged.compare_exchange_strong(expected, true)) {
        return;
    }
    const auto& c = pbModeConfig();
    Main().logger()->info(
        "[POSITION_BOOK] mode={} source={}",
        pbModeName(c.global),
        c.global_from_config ? "config" : "code-default");
    for (const auto& kv : c.per_symbol) {
        Main().logger()->info(
            "[POSITION_BOOK] sym={} mode={} source=config",
            kv.first,
            pbModeName(kv.second));
    }
    // Pillar B — fire-and-track boot banner (one-shot, folded into the position-book
    // banner so it prints exactly once at startup). Only WARN when enabled, since it
    // loosens the post-cancel-confirm gating (transient exposure bound max_po+2x).
    auto& cfg = config::Config::getInstance();
    if (cfg.getBool("market_maker.place_before_cancel_ack", false)) {
        Main().logger()->warn(
            "[MM_ORDERS] place_before_cancel_ack=TRUE — re-add not gated on cancel confirm; "
            "transient exposure bound max_po+2x (operator/Tim approved). "
            "inflight_pessimistic_gate={} cancel_confirm_timeout_ms={}",
            cfg.getBool("market_maker.inflight_pessimistic_gate", false) ? "TRUE" : "FALSE",
            std::max(1, cfg.getInt("market_maker.cancel_confirm_timeout_ms", 3000)));
    } else {
        Main().logger()->info(
            "[MM_ORDERS] place_before_cancel_ack=FALSE — classic ack-wait cancel-replace "
            "(re-add gated on cancel confirm)");
    }
}

void MakeMarketStrategy::mmBindPositionBookIfNeeded() const {
    if (mmPbMode() == PbMode::Off) {
        return;
    }
    if (!position_book_) {
        position_book_ = PositionBook::forSymbol(mmAxSymbol());
    }
    mmLogPositionBookBootBannerOnce();
}

double MakeMarketStrategy::mmEffectiveNetPoQty() const {
    // ENFORCE (for this symbol): the book is THE truth. off/shadow: legacy local net
    // — so replacing any legacy reader with this call is behavior-neutral unless enforce.
    if (mmPbMode() == PbMode::Enforce) {
        auto b = position_book_ ? position_book_ : PositionBook::lookup(mmAxSymbol());
        if (b) {
            return b->effective();
        }
    }
    return position_state_.net_position_qty;
}

long long MakeMarketStrategy::mmEffectiveNetPoRounded() const {
    return static_cast<long long>(std::llround(mmEffectiveNetPoQty()));
}

void MakeMarketStrategy::mmOnFillToBook(Side side, Quantity qty) const {
    if (mmPbMode() == PbMode::Off) {
        return;
    }
    auto b = position_book_ ? position_book_ : PositionBook::forSymbol(mmAxSymbol());
    if (!b) {
        return;
    }
    const double signed_qty =
        (side == Side::BUY) ? static_cast<double>(qty) : -static_cast<double>(qty);
    b->onFill(signed_qty);
}

void MakeMarketStrategy::mmSeedPositionBookFromLocalNet() const {
    if (mmPbMode() == PbMode::Off) {
        return;
    }
    auto b = position_book_ ? position_book_ : PositionBook::forSymbol(mmAxSymbol());
    if (!b) {
        return;
    }
    double net = 0.0;
    {
        std::lock_guard<std::mutex> lk(position_state_mutex_);
        net = position_state_.net_position_qty;
    }
    b->seed(net);
}

long long MakeMarketStrategy::mmLegacyNetRoundedForSymbol(const std::string& ax_symbol) {
    auto mms = collectMakeMarketStrategySharedPtrsOnAx(ax_symbol);
    if (mms.empty()) {
        return 0;
    }
    return static_cast<long long>(std::llround(mms.front()->position_state_.net_position_qty));
}

void MakeMarketStrategy::mmApplyVenueSnapshotBatch(
    const std::unordered_map<std::string, double>& venue_by_symbol) {
    if (!mmPbEnabledAnySymbol()) {
        return;  // fully off -> caller should not even reach here; belt-and-suspenders
    }
    std::vector<std::pair<std::string, PositionBook::ApplyResult>> results;
    PositionBook::applySnapshotBatch(venue_by_symbol, &results);
    if (!utils::Logger::isInitialized()) {
        return;
    }
    auto& cfg = config::Config::getInstance();
    for (const auto& kv : results) {
        const std::string& sym = kv.first;
        const PositionBook::ApplyResult& r = kv.second;
        const PbMode mode = mmPbModeForSymbol(sym);
        if (mode == PbMode::Off) {
            continue;  // book should not exist for off symbols, but never log for them
        }

        // (5) Drift log — one greppable line per symbol.
        // Throttle (Fix 4): the FastPoller applies snapshots frequently, so an idle flat
        // book would emit one POSITION_SNAPSHOT per symbol on every poll (all-zero noise).
        // Rules: (a) if the snapshot or effective CHANGED for this symbol, ALWAYS log (that
        // is the drift evidence); (b) otherwise emit at most ONE liveness line per
        // snapshot_log_interval_ms across the WHOLE desk (a single shared idle gate, so N
        // idle symbols collapse to ~1 line/interval instead of N — ~12/min at 5s vs ~40+).
        bool log_snapshot = true;
        {
            static std::mutex s_snap_log_mu;
            struct SnapLogState {
                double snap{0.0};
                double eff{0.0};
                std::int64_t last_ms{0};
                bool inited{false};
            };
            static std::unordered_map<std::string, SnapLogState> s_snap_log;
            // Shared desk-wide idle-heartbeat gate: any logged line (changed or heartbeat)
            // refreshes it, so idle lines only appear after interval_ms of silence.
            static std::int64_t s_last_line_ms = INT64_MIN / 2;
            static const int interval_ms =
                std::max(0, cfg.getInt("market_maker.snapshot_log_interval_ms", 5000));
            const std::int64_t now_ms = mmSteadyMillis();
            std::lock_guard<std::mutex> lk(s_snap_log_mu);
            SnapLogState& st = s_snap_log[sym];
            const bool changed =
                !st.inited || st.snap != r.new_snapshot || st.eff != r.new_effective;
            const bool heartbeat_due = (now_ms - s_last_line_ms) >= interval_ms;
            log_snapshot = changed || heartbeat_due;
            if (log_snapshot) {
                st.snap = r.new_snapshot;
                st.eff = r.new_effective;
                st.last_ms = now_ms;
                st.inited = true;
                s_last_line_ms = now_ms;
            }
        }
        if (log_snapshot && mode == PbMode::Shadow) {
            const long long legacy = mmLegacyNetRoundedForSymbol(sym);
            Main().logger()->info(
                "[POSITION_SNAPSHOT] sym={} snapshot={:.4f} unaccounted_before_reset={:.4f} "
                "effective_before={:.4f} effective_after={:.4f} epoch={} legacy_net={} "
                "delta_vs_legacy={:.4f}",
                sym, r.new_snapshot, r.prev_unaccounted, r.prev_effective, r.new_effective,
                r.epoch, legacy, r.prev_effective - static_cast<double>(legacy));
        } else if (log_snapshot) {
            Main().logger()->info(
                "[POSITION_SNAPSHOT] sym={} snapshot={:.4f} unaccounted_before_reset={:.4f} "
                "effective_before={:.4f} effective_after={:.4f} epoch={}",
                sym, r.new_snapshot, r.prev_unaccounted, r.prev_effective, r.new_effective,
                r.epoch);
        }

        // (6) Cap evaluation on the post-snapshot effective.
        auto mms = collectMakeMarketStrategySharedPtrsOnAx(sym);
        if (mms.empty()) {
            continue;
        }
        const long long cap_ll = static_cast<long long>(mms.front()->mmEffMaxPositionInt(cfg));
        if (cap_ll <= 0) {
            continue;
        }
        const long long eff = static_cast<long long>(std::llround(r.new_effective));
        if (std::llabs(eff) < cap_ll) {
            if (mode == PbMode::Enforce) {
                // Below cap: still refresh reduce-only EXIT on all stacks from the book.
                for (auto& sp : mms) {
                    sp->noteReduceOnlyState(eff, cap_ll);
                }
            }
            continue;
        }
        const char* grow_side = (eff > 0) ? "BUY" : "SELL";
        if (mode == PbMode::Shadow) {
            // Log ONLY — the accuracy evidence for "would have enforced". No action.
            Main().logger()->info(
                "[POSITION_SNAPSHOT_CAP_SHADOW] sym={} effective={} cap={} would_set_reduce_only=1 "
                "would_pull_side={}",
                sym, eff, cap_ll, grow_side);
        } else {  // Enforce
            mmEnforceCapFromSnapshotOnSymbol(sym);
        }
    }
}

void MakeMarketStrategy::mmEnforceCapFromSnapshotOnSymbol(const std::string& ax_symbol) {
    if (ax_symbol.empty()) {
        return;
    }
    if (mmPbModeForSymbol(ax_symbol) != PbMode::Enforce) {
        return;  // enforce-only; shadow/off never act
    }
    auto mms_pin = collectMakeMarketStrategySharedPtrsOnAx(ax_symbol);
    if (mms_pin.empty()) {
        return;
    }
    auto& cfg = config::Config::getInstance();
    const long long cap_ll = static_cast<long long>(mms_pin.front()->mmEffMaxPositionInt(cfg));
    if (cap_ll <= 0) {
        return;
    }
    auto b = PositionBook::lookup(ax_symbol);
    if (!b) {
        return;
    }
    const long long net = static_cast<long long>(std::llround(b->effective()));

    // reduce-only ENTER/EXIT on every stack, read from the book (noteReduceOnlyState
    // handles both directions). This is the same signal the on-fill path sets.
    for (auto& sp : mms_pin) {
        sp->noteReduceOnlyState(net, cap_ll);
    }
    if (std::llabs(net) < cap_ll) {
        return;  // no breach — reduce-only cleared above, nothing to pull
    }

    // Breach: cancel every grow-side tracked leg on the product. Each cancel is routed
    // onto the symbol's mover (enqueueRestCancelOnMover) so the REST runs on the mover
    // thread, NOT this position-poll thread. Deliberately NOT mmConvergeProductBookOnAx
    // (that fetches open orders). LOCAL truth only.
    const Side grow = (net > 0) ? Side::BUY : Side::SELL;
    for (auto& sp : mms_pin) {
        std::vector<std::pair<OrderId, Side>> to_cancel;
        {
            std::lock_guard<std::mutex> lk(sp->tracked_orders_mutex_);
            auto& vec = (grow == Side::BUY) ? sp->tracked_bids_ : sp->tracked_asks_;
            for (auto& leg : vec) {
                if (leg.local_oid != 0 &&
                    (leg.state == MmLegState::ADOPTED ||
                     leg.state == MmLegState::PLACE_PENDING)) {
                    to_cancel.emplace_back(leg.local_oid, grow);
                }
            }
        }
        for (const auto& c : to_cancel) {
            sp->enqueueRestCancelOnMover(c.first, c.second, "snapshot_cap_enforce");
        }
    }
    if (utils::Logger::isInitialized()) {
        Main().logger()->info(
            "[POSITION_SNAPSHOT_CAP_ENFORCE] sym={} net={} cap={} pulled_side={} — reduce-only set, "
            "grow-side cancels enqueued on mover (no REST on poll thread)",
            ax_symbol, net, cap_ll, (grow == Side::BUY) ? "BUY" : "SELL");
    }
}

void MakeMarketStrategy::reconcileFromExchangeAfterOrphanFill(const std::string& fill_symbol) {
    auto& cfg = config::Config::getInstance();
    if (!cfg.isMarketMakerEnabled() || !isRunning() || !isEnabled()) {
        return;
    }
    bool match = false;
    if (!fill_symbol.empty() && fill_symbol == mmAxSymbol()) {
        match = true;
    }
    if (!match) {
        return;
    }
    // ENFORCE: orphan fills are absorbed by the next 30s snapshot (book is truth); skip
    // the synchronous REST inventory refresh + requote here. off/shadow keep the legacy path.
    if (mmPbEnforce()) {
        return;
    }
    const double pre_net = position_state_.net_position_qty;
    Main().logger()->info(
        "[STRATEGY:{}] Orphan exchange fill on {} — refreshing inventory from REST (no local order id; "
        "typical after cancel-all vs. a working exchange oid, or REST fill ahead of OM adopt).",
        getName(), fill_symbol);
    syncInventoryFromExchangePortfolio(true);
    constexpr double kNetEps = 1e-8;
    const double post_net = position_state_.net_position_qty;
    if (std::fabs(post_net - pre_net) < kNetEps) {
        Main().logger()->info(
            "[STRATEGY:{}] Orphan fill: REST NetPo unchanged ({:.8f}) — skip MM requote (no inventory delta; "
            "often a historic fill replay or cancel-all race). Requote runs when NetPo actually moves.",
            getName(),
            post_net);
        return;
    }
    Main().logger()->info(
        "[STRATEGY:{}] Orphan fill: REST NetPo {:.8f} → {:.8f} — requote (runFullMmQuoteCycle: max_reload, "
        "max_position, …)",
        getName(),
        pre_net,
        post_net);
    (void)config::Config::getInstance().reloadPrimaryConfigFromDisk();
    auto& cfg_or = config::Config::getInstance();
    if (!mmMarketMakerOrdersPlacementAllowed(cfg_or)) {
        return;
    }
    if (auto theo_opt = mmReadTransformedTheo()) {
        requoteFromTheo(*theo_opt, "fill_requote_orphan");
    }
}

void MakeMarketStrategy::notifyAllRegisteredOrphanExchangeFill(const std::string& fill_symbol) {
    for (const auto& sp : StrategyManager::getInstance().getAllStrategies()) {
        auto mm = std::dynamic_pointer_cast<MakeMarketStrategy>(sp);
        if (mm) {
            mm->reconcileFromExchangeAfterOrphanFill(fill_symbol);
        }
    }
}

void MakeMarketStrategy::reconcileFromExchangeAfterOrphanFillUnknownSymbol() {
    auto& cfg = config::Config::getInstance();
    if (!cfg.getBool("market_maker.orphan_fill_unknown_symbol_reconcile", true)) {
        return;
    }
    if (!cfg.isMarketMakerEnabled() || !isRunning() || !isEnabled()) {
        return;
    }
    // ENFORCE: book is truth; the 30s snapshot absorbs the fill. Skip REST refresh + requote.
    if (mmPbEnforce()) {
        return;
    }
    const double pre_net = position_state_.net_position_qty;
    syncInventoryFromExchangePortfolio();
    constexpr double kEps = 1e-8;
    if (std::fabs(position_state_.net_position_qty - pre_net) < kEps) {
        return;
    }
    Main().logger()->info(
        "[STRATEGY:{}] Orphan fill (symbol not in REST payload): NetPo {:.8f} -> {:.8f} on {} — "
        "requote (same gates as any MM cycle: max_reload, max_position, …)",
        getName(), pre_net, position_state_.net_position_qty, mmAxSymbol());
    (void)config::Config::getInstance().reloadPrimaryConfigFromDisk();
    auto& cfg_ou = config::Config::getInstance();
    if (!mmMarketMakerOrdersPlacementAllowed(cfg_ou)) {
        return;
    }
    if (auto theo_opt = mmReadTransformedTheo()) {
        requoteFromTheo(*theo_opt, "fill_requote_orphan");
    }
}

void MakeMarketStrategy::notifyAllRegisteredOrphanExchangeFillUnknownSymbol() {
    for (const auto& sp : StrategyManager::getInstance().getAllStrategies()) {
        auto mm = std::dynamic_pointer_cast<MakeMarketStrategy>(sp);
        if (mm) {
            mm->reconcileFromExchangeAfterOrphanFillUnknownSymbol();
        }
    }
}

void MakeMarketStrategy::mmFastMarketPullAllTrackedLegs(const std::string& reason) {
    // Global fast-market breaker fan-out. Called by FastMarketMonitor on the mover when HL SPX
    // trips. We snapshot the live strategies (getAllStrategies pins shared_ptrs), then per stack
    // enqueue a mover job that re-pins via getStrategy(name) — the committed lifetime pattern, so a
    // torn-down stack becomes a no-op instead of a UAF — and cancels BUY+SELL through the existing
    // mmCancelTrackedLegThisStackGateway. No new cancel path; cancels run on the one mover worker.
    for (const auto& sp : StrategyManager::getInstance().getAllStrategies()) {
        auto mm = std::dynamic_pointer_cast<MakeMarketStrategy>(sp);
        if (!mm) {
            continue;
        }
        const std::string ax = mm->mmAxSymbol();
        if (ax.empty()) {
            continue;
        }
        const std::string name = mm->getName();
        MmOrderMover::getInstance().enqueueForAx(ax, [name, reason]() {
            auto sp2 = StrategyManager::getInstance().getStrategy(name);
            auto mm2 = std::dynamic_pointer_cast<MakeMarketStrategy>(sp2);
            if (!mm2) {
                return;  // stack torn down between snapshot and drain — safe no-op
            }
            const int n_bid = mm2->mmCancelTrackedLegThisStackGateway(Side::BUY);
            const int n_ask = mm2->mmCancelTrackedLegThisStackGateway(Side::SELL);
            if ((n_bid > 0 || n_ask > 0) && utils::Logger::isInitialized()) {
                Main().logger()->warn(
                    "[FAST_MKT_BREAKER] pulled legs stack={} ax={} bid_cancelled={} ask_cancelled={} reason={}",
                    name, mm2->mmAxSymbol(), n_bid, n_ask, reason);
            }
        });
    }
}

int MakeMarketStrategy::mmDeskForceCancelStackGateway(const std::string& ax_symbol,
                                                      const std::string& stack_id,
                                                      const std::string& reason) {
    // Desk-driven targeted force-cancel using VENUE TRUTH (OrderManager / mm_orders.json), independent
    // of orders.json presence. This is the recovery path for the "orders on the exchange but the desk
    // cancel dropdown is empty" split (orders.json desired-state lost its stack while the leg still
    // rests on the venue): removing the stack from orders.json only makes C++ *stop moving* the stack
    // (see the desk_orders_json_stack_missing gate) — it does NOT cancel the resting legs. So the desk
    // posts (ax_symbol, stack_id) here and C++ cancels the legs it still tracks in OrderManager, which
    // (unlike Python) knows the exchange OIDs. Reuses the fast-market fan-out contract exactly: snapshot
    // strategies, then per match enqueue a mover job that re-pins by name (committed lifetime → a stack
    // torn down between snapshot and drain is a safe no-op) and cancels BUY+SELL via the existing
    // mmCancelTrackedLegThisStackGateway. No new cancel path; all venue ops run on the single mover
    // worker. Empty stack_id => every stack on ax_symbol (product-wide desk cancel).
    const std::string want_ax = mmDeskUpperAscii(mmDeskTrimStr(ax_symbol));
    const std::string want_sid = mmDeskTrimStr(stack_id);
    int dispatched = 0;
    for (const auto& sp : StrategyManager::getInstance().getAllStrategies()) {
        auto mm = std::dynamic_pointer_cast<MakeMarketStrategy>(sp);
        if (!mm) {
            continue;
        }
        const std::string ax = mm->mmAxSymbol();
        if (ax.empty()) {
            continue;
        }
        if (!want_ax.empty() && mmDeskUpperAscii(ax) != want_ax) {
            continue;
        }
        if (!want_sid.empty() && mmDeskTrimStr(mm->mmOrderRequestId()) != want_sid) {
            continue;
        }
        const std::string name = mm->getName();
        ++dispatched;
        MmOrderMover::getInstance().enqueueForAx(ax, [name, reason]() {
            auto sp2 = StrategyManager::getInstance().getStrategy(name);
            auto mm2 = std::dynamic_pointer_cast<MakeMarketStrategy>(sp2);
            if (!mm2) {
                return;  // stack torn down between snapshot and drain — safe no-op
            }
            const int n_bid = mm2->mmCancelTrackedLegThisStackGateway(Side::BUY);
            const int n_ask = mm2->mmCancelTrackedLegThisStackGateway(Side::SELL);
            if (utils::Logger::isInitialized()) {
                Main().logger()->warn(
                    "[DESK_FORCE_CANCEL] pulled legs stack={} ax={} bid_cancelled={} "
                    "ask_cancelled={} reason={}",
                    name, mm2->mmAxSymbol(), n_bid, n_ask, reason);
            }
        });
    }
    if (utils::Logger::isInitialized()) {
        Main().logger()->info(
            "[DESK_FORCE_CANCEL] dispatch ax='{}' stack_id='{}' matched={} reason={}",
            ax_symbol, stack_id, dispatched, reason);
    }
    return dispatched;
}

void MakeMarketStrategy::feedGuardianResetLocalState(const std::string& tag) {
    std::lock_guard<std::recursive_mutex> cycle_lock(mm_cycle_mutex_);
    Main().logger()->warn("[STRATEGY:{}] feed_guardian: MM local reset tag={}", getName(), tag);
    bid_order_id_ = 0;
    ask_order_id_ = 0;
    last_bid_price_ = 0.0;
    last_ask_price_ = 0.0;
    mm_desk_active_ = false;
    mm_pending_accepts_ = 0;
    mm_pending_accepts_last_change_ms_.store(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now().time_since_epoch())
            .count(),
        std::memory_order_relaxed);
    clearOpenOrderTracking();
}

void MakeMarketStrategy::feedGuardianResetAllRegistered(const std::string& tag) {
    for (const auto& sp : StrategyManager::getInstance().getAllStrategies()) {
        auto mm = std::dynamic_pointer_cast<MakeMarketStrategy>(sp);
        if (mm) {
            mm->feedGuardianResetLocalState(tag);
        }
    }
}

void MakeMarketStrategy::feedGuardianResetForDownFeeds(const std::string& tag) {
    auto& efm = marketdata::ExternalFeedManager::getInstance();
    for (const auto& sp : StrategyManager::getInstance().getAllStrategies()) {
        auto mm = std::dynamic_pointer_cast<MakeMarketStrategy>(sp);
        if (!mm) {
            continue;
        }
        const std::string& src = mm->mmTheoSource();
        if (src.empty()) {
            // Legacy ungated leg — fall back to "all" semantics so it doesn't get stuck.
            mm->feedGuardianResetLocalState(tag);
            continue;
        }
        if (!efm.isFeedUpForSource(src)) {
            mm->feedGuardianResetLocalState(tag);
        }
    }
}

void MakeMarketStrategy::ensureStartupQuotePair(const std::string& tag) {
    auto& cfg = config::Config::getInstance();
    // === FIX D companion ===
    // For legacy YAML strategies the gate "must have tracked legs to enforce a pair" makes sense —
    // C++ only moves AFTER the GUI seeds the first manual order. For desk-managed (`mm_req_*`)
    // stacks the seed IS the orders.json entry, not a manual GUI order; tracked legs may have been
    // erased by on_cancel after the strategy itself REST-cancelled them. Skip the early return
    // for desk-managed strategies whose desk JSON still owns the stack — the actual auto-place
    // decision happens further down (see `desk_recovery_eligible`).
    const bool desk_owned_active =
        mmIsDeskManaged() &&
        mmManualStackDeskSeeded() &&
        mmDeskOrdersJsonContainsThisStack();
    if (passive_until_first_manual_order_ || (!hasTrackedOrders() && !desk_owned_active)) {
        return;
    }

    // Per-reason throttled diagnostic — surfaces *why* the enforcer bailed so
    // misconfigurations don't masquerade as silence. Throttled to one log per
    // 10s per strategy.
    auto throttled_skip_log = [&](const std::string& reason) {
        const auto now_ms =
            std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now().time_since_epoch())
                .count();
        const auto last_ms = startup_enforce_skip_log_ms_.load();
        if (last_ms == 0 || (now_ms - last_ms) > 10000) {
            startup_enforce_skip_log_ms_.store(now_ms);
            Main().logger()->info(
                "[STRATEGY:{}] {}: skipping pair-enforce — reason={}",
                getName(), tag, reason);
        }
    };

    if (!cfg.isMarketMakerEnabled()) {
        throttled_skip_log("market_maker_disabled");
        return;
    }
    if (!isRunning() || !isEnabled()) {
        throttled_skip_log("strategy_not_running_or_disabled");
        return;
    }
    // Respect side-gating at product cap (reduce-only): when one side is
    // intentionally disabled, only require the side we currently want.
    const bool want_bid = shouldQuoteSide(Side::BUY);
    const bool want_ask = shouldQuoteSide(Side::SELL);
    // This stack only: another mm_req_* on the same AX symbol must not satisfy "have bid/ask" here.
    const bool have_bid =
        !want_bid || (bid_order_id_ != 0 && mmTrackedSideHasActiveLimitOrder(Side::BUY));
    const bool have_ask =
        !want_ask || (ask_order_id_ != 0 && mmTrackedSideHasActiveLimitOrder(Side::SELL));
    const bool missing_required_side = !(have_bid && have_ask);
    if (mm_pending_accepts_ > 0) {
        const auto now_ms =
            std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now().time_since_epoch())
                .count();
        const auto last_change_ms =
            mm_pending_accepts_last_change_ms_.load(std::memory_order_relaxed);
        const int stale_ms = std::max(500, cfg.getInt("market_maker.pending_accept_stale_ms", 5000));
        const long long age_ms = (last_change_ms > 0) ? (now_ms - last_change_ms) : -1;
        if (!missing_required_side) {
            // Working bid+ask already satisfy shouldQuoteSide — pending_accepts is stale (e.g. desk adopted
            // gateway rows without matching submit_order bookkeeping, or extra increments). Do not block
            // pair-enforce / recovery indefinitely on "awaiting_accept_pair_healthy".
            Main().logger()->info(
                "[STRATEGY:{}] MM pending_accepts={} cleared — pair healthy context={} "
                "(want_bid={} have_bid={} want_ask={} have_ask={})",
                getName(),
                mm_pending_accepts_,
                tag,
                want_bid ? 1 : 0,
                have_bid ? 1 : 0,
                want_ask ? 1 : 0,
                have_ask ? 1 : 0);
            mm_pending_accepts_ = 0;
            mm_pending_accepts_last_change_ms_.store(now_ms, std::memory_order_relaxed);
            {
                std::lock_guard<std::mutex> lk(mm_accept_pending_mutex_);
                mm_pending_accept_cycle_reason_.clear();
            }
        } else if (age_ms >= stale_ms) {
            Main().logger()->warn(
                "[STRATEGY:{}] MM_GATE gate=stale_awaiting_accept_autoclear context={} | pending_accepts={} "
                "age_ms={} stale_ms={} want_bid={} have_bid={} want_ask={} have_ask={} — clearing "
                "pending accepts to resume movement",
                getName(),
                tag,
                mm_pending_accepts_,
                age_ms,
                stale_ms,
                want_bid ? 1 : 0,
                have_bid ? 1 : 0,
                want_ask ? 1 : 0,
                have_ask ? 1 : 0);
            mm_pending_accepts_ = 0;
            mm_pending_accepts_last_change_ms_.store(now_ms, std::memory_order_relaxed);
            {
                std::lock_guard<std::mutex> lk(mm_accept_pending_mutex_);
                mm_pending_accept_cycle_reason_.clear();
            }
        } else {
            mmLogMmGateThrottled(
                "awaiting_accept_blocks_pair_enforce",
                tag,
                "pending_accepts=" + std::to_string(mm_pending_accepts_) +
                    " age_ms=" + std::to_string(age_ms) + " stale_ms=" + std::to_string(stale_ms) +
                    " missing_required_side=1"
                    " want_bid=" + std::to_string(want_bid ? 1 : 0) +
                    " have_bid=" + std::to_string(have_bid ? 1 : 0) +
                    " want_ask=" + std::to_string(want_ask ? 1 : 0) +
                    " have_ask=" + std::to_string(have_ask ? 1 : 0));
            throttled_skip_log("awaiting_accept_missing_required_side");
            return;
        }
    }

    // Per-leg feed gate: if this leg's theo_source is down, the per-leg block
    // already prevents quoting. ensureStartupQuotePair is a no-op then.
    if (!mm_theo_source_.empty()) {
        auto& efm = marketdata::ExternalFeedManager::getInstance();
        if (!efm.isFeedUpForSource(mm_theo_source_)) {
            throttled_skip_log("feed_down:" + mm_theo_source_);
            return;
        }
    }

    const bool instrument_cap_fixup_tag =
        (tag == "instrument_limits_update") || (tag == "instrument_cap_increased") ||
        (tag == "instrument_cap_decreased");
    if (have_bid && have_ask && !instrument_cap_fixup_tag) {
        return;
    }

    // === FIX D: DESK-MANAGED RECOVERY ===
    // Desk-driven stacks (`mm_req_*` + non-empty mm_order_request_id_) normally never auto-send
    // a missing bid/ask from here — operator seeds from the GUI; C++ moves once OIDs exist.
    //
    // EXCEPTION (recovery): if BOTH legs are absent (bid_order_id_=0 AND ask_order_id_=0) AND
    // orders.json STILL owns this stack (operator has NOT pulled it from the desk), the strategy
    // is stuck — usually because a brute-force theo-move REST-cancelled both legs and the place
    // step never ran (lock contention / hung worker / network timeout). Without recovery the
    // user's stack stays orderless until they manually re-seed, even though they explicitly
    // wanted it active. Place a fresh pair from current theo + skew.
    //
    // Live incident 2026-05-07 13:36:50: cycle 2 cancelled id=3+id=4 on EURUSD-PERP, then the
    // feed thread hung between cancel-ack and submit-place. mm_pair_enforce kept logging
    // "skipping pair-enforce — reason=manual_mm_no_auto_seed_from_pair_enforce" while the
    // venue had nothing for that stack. This branch fixes that.
    //
    // === DESK_RECOVERY RACE GUARD (2026-05-20 segfault + phantom-bid fix) ============
    // The 2026-05-20 production segfault and Joe's "3 offers, 4 bids" after-flatten
    // symptom both came from DESK_RECOVERY firing while the mover thread was in the
    // middle of a normal cancel-replace. A normal cycle transiently produces
    // `bid_order_id_=0 && ask_order_id_=0` between cancel-ack and place-submit-return,
    // and the old predicate happily matched that. DESK_RECOVERY would then:
    //   1. Clear tracked_bids_/tracked_asks_ from THIS thread while the mover holds /
    //      iterates them on its thread → memory corruption, segfault.
    //   2. Enqueue an extra full cycle behind the in-flight one → extra place that did
    //      not get a matching cancel → extra bid / extra ask visible to Joe.
    //
    // The new predicate adds three additional guards so DESK_RECOVERY only fires when
    // the stack is GENUINELY stuck (no cycle running, no cycle queued, no pending
    // accepts, AND no cycle activity in the last `quiet_ms`):
    //   - !mm_cycle_running_   : runFullMmQuoteCycle is NOT currently executing.
    //   - !mm_quote_cycle_pending_ : no queued cycle waiting to drain on the mover.
    //   - mm_pending_accepts_ == 0 : no place-ack is in flight for this stack.
    //   - last_cycle_activity_ms is at least `quiet_ms` ago (default 1500ms): even if
    //     all of the above are false, give the mover a healthy grace window so a
    //     between-callback gap doesn't trip recovery.
    const auto now_ms_dr =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now().time_since_epoch()).count();
    const std::int64_t last_activity_ms =
        mm_last_cycle_activity_ms_.load(std::memory_order_relaxed);
    const std::int64_t quiet_ms = std::max(
        500,
        cfg.getInt("market_maker.desk_recovery_quiet_ms", 1500));
    const bool cycle_quiet =
        (last_activity_ms == 0) ||
        ((now_ms_dr - last_activity_ms) >= quiet_ms);
    const bool desk_recovery_stuck_shape =
        mmIsDeskManaged() && bid_order_id_ == 0 && ask_order_id_ == 0 &&
        mmManualStackDeskSeeded() &&
        mmDeskOrdersJsonContainsThisStack();
    const std::int64_t recovery_backoff_until_ms =
        mm_desk_recovery_next_attempt_ms_.load(std::memory_order_relaxed);
    if (desk_recovery_stuck_shape && recovery_backoff_until_ms > now_ms_dr) {
        throttled_skip_log(
            "desk_recovery_backoff:lease_denied"
            " until_ms=" + std::to_string(recovery_backoff_until_ms) +
            " remaining_ms=" + std::to_string(recovery_backoff_until_ms - now_ms_dr) +
            " streak=" + std::to_string(mm_desk_recovery_lease_denied_streak_.load(
                                          std::memory_order_relaxed)));
        return;
    }
    const bool desk_recovery_eligible =
        desk_recovery_stuck_shape &&
        !mm_cycle_running_.load(std::memory_order_acquire) &&
        !mm_quote_cycle_pending_.load(std::memory_order_acquire) &&
        mm_pending_accepts_ == 0 &&
        cycle_quiet;
    const bool instrument_cap_missing_side_fixup =
        instrument_cap_fixup_tag && missing_required_side;
    if (mmIsDeskManaged() && !desk_recovery_eligible && !instrument_cap_missing_side_fixup) {
        // Distinguish "genuinely stuck, but waiting for quiet window" from "no recovery needed".
        if (bid_order_id_ == 0 && ask_order_id_ == 0 &&
            mmManualStackDeskSeeded() &&
            mmDeskOrdersJsonContainsThisStack()) {
            const long long age = (last_activity_ms > 0) ? (now_ms_dr - last_activity_ms) : -1;
            throttled_skip_log(
                "desk_recovery_inhibited:cycle_in_flight_or_quiet_not_elapsed"
                " cycle_running=" + std::to_string(mm_cycle_running_.load() ? 1 : 0) +
                " cycle_pending=" + std::to_string(mm_quote_cycle_pending_.load() ? 1 : 0) +
                " pending_accepts=" + std::to_string(mm_pending_accepts_) +
                " activity_age_ms=" + std::to_string(age) +
                " quiet_ms=" + std::to_string(quiet_ms));
        } else {
            throttled_skip_log("manual_mm_no_auto_seed_from_pair_enforce");
        }
        return;
    }
    if (desk_recovery_eligible) {
        Main().logger()->warn(
            "[STRATEGY:{}] {}: DESK_RECOVERY — both legs absent (bid_id=0 ask_id=0) but "
            "orders.json still owns stack_id={} ax={}. Placing fresh pair at current theo+skew. "
            "(typical cause: theo_move cancelled both legs then place-side hung on REST/lock)",
            getName(), tag, mmOrderRequestId(), mmAxSymbol());

        // === FIX E: PURGE STALE TRACKED ENTRIES BEFORE RECOVERY PLACE ===
        // Live evidence 2026-05-07 14:18:43.481 (`mm_req_EURUSD_PERP_24333b63`): Fix D fired the
        // DESK_RECOVERY warning, then `runFullMmQuoteCycle` immediately bailed with
        // `[MM_GATE gate=desk_exchange_orders_not_live ... bid_id=0 ask_id=0 — skipping move
        // (venue may still show working legs; adopt retries every ~2.5s)]`. The gate fired
        // because:
        //   1. The mid-cycle cancel-replace path in `runFullMmQuoteCycle` REST-cancels each leg
        //      via `mmCancelTrackedLegThisStackGateway` and then sets
        //      `bid_order_id_=0 / ask_order_id_=0`, but it never erases the corresponding
        //      `tracked_bids_/tracked_asks_` entries — they linger as zombie `ADOPTED` rows with
        //      the now-defunct exchange OIDs.
        //   2. `tryAdoptGatewayPlacedFromManualStack` then matches those zombies against the
        //      manual_stack snapshot (still pointing at the original gateway-placed OIDs that
        //      cycle 1's cancel just killed), short-circuits with
        //      "gateway-seed adopt skipped — already adopted", and returns `true`.
        //   3. `mmDeskTrackedOrdersAliveForMove()` immediately afterward returns false (because
        //      bid_order_id_=0 / ask_order_id_=0), `desk_periodic_move` is false for
        //      `mm_pair_enforce`, and the gate aborts the recovery place.
        //
        // The recovery contract is: orders.json owns this stack, both local handles are 0, no
        // alive legs on the venue we know of. Whatever the tracked vectors say, those rows
        // cannot be alive — purge them so:
        //   * `hasTrackedOrders()` returns false → `desk_recovery_no_legs` becomes true in
        //     `runFullMmQuoteCycle` and bypasses the early `passive_until_first_manual_order_ ||
        //     !hasTrackedOrders()` return.
        //   * `tryAdoptGatewayPlacedFromManualStack` cannot match zombie rows; it falls through
        //     to `applyMmDeskSeededJsonLocked` which reseeds correctly.
        //   * `mm_desk_active_=false` forces the next applyMmDeskSeededJsonLocked to treat this
        //     as a fresh adoption (the cancelled OIDs aren't ours anymore).
        {
            std::lock_guard<std::mutex> lk(tracked_orders_mutex_);
            tracked_bids_.clear();
            tracked_asks_.clear();
        }
        mm_desk_active_ = false;
        last_bid_price_ = 0.0;
        last_ask_price_ = 0.0;
    }
    // Legacy YAML `make_market_*`: default policy is manual venue placement first; C++ only moves
    // once OrderManager has at least one working id (or use fill_requote* / cpp_may_place_orders).
    // Desk recovery bypasses these gates — operator explicitly seeded via the desk; the gates
    // exist for legacy YAML quoters that wait for GUI-placed working orders before C++ may move.
    if (!desk_recovery_eligible) {
        if (cfg.getBool("market_maker.cpp_manual_quotes_only", true)) {
            throttled_skip_log("cpp_manual_quotes_only_no_pair_enforce_auto_place");
            return;
        }
        if (!cfg.getBool("market_maker.cpp_may_place_orders", false)) {
            throttled_skip_log("cpp_startup_no_autoplace_legacy_wait_gui");
            return;
        }
    }

    // Refuse to fight mm_orders_disabled / AX WS gate.
    if (!mmMarketMakerOrdersPlacementAllowed(cfg)) {
        throttled_skip_log("placement_blocked:disabled_or_strategy_off");
        return;
    }
    if (cfg.getMarketMakerCancelOnDisconnect() && !Main().websocket()->isConnected()) {
        throttled_skip_log("ws_disconnected");
        return;
    }

    if (!theo_provider_) {
        throttled_skip_log("no_theo_provider");
        return;
    }
    auto opt = mmReadTransformedTheo();
    if (!opt.has_value() || !std::isfinite(*opt) || *opt <= 0.0) {
        const auto now_ms =
            std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now().time_since_epoch())
                .count();
        const auto last_ms = startup_enforce_no_theo_log_ms_.load();
        if (last_ms == 0 || (now_ms - last_ms) > 10000) {
            startup_enforce_no_theo_log_ms_.store(now_ms);
            Main().logger()->warn(
                "[STRATEGY:{}] {}: feed up for theo_source='{}' but no theo yet for symbol='{}' — "
                "check market_maker.instruments[].theo_venue_symbol mapping. Will retry.",
                getName(),
                tag,
                mm_theo_source_.empty() ? std::string("(legacy)") : mm_theo_source_,
                mmTheoSymbol());
        }
        return;
    }

    Main().logger()->info(
        "[STRATEGY:{}] {}: missing required side(s) (want_bid={} have_bid={} want_ask={} have_ask={} bid_id={} ask_id={}) — forcing quote cycle at theo={:.6f}",
        getName(),
        tag,
        want_bid ? 1 : 0,
        have_bid ? 1 : 0,
        want_ask ? 1 : 0,
        have_ask ? 1 : 0,
        bid_order_id_,
        ask_order_id_,
        *opt);
    // Route the actual cancel/replace/place to the per-AX mover thread so we never
    // do REST on the mm_pair_enforce timer thread (or any other caller's thread).
    // The mover re-reads the freshest theo on its own thread, so enqueue is safe
    // even though *opt is captured by value here only for the diagnostic log above.
    enqueueQuoteCycleOnMover(tag);
}

void MakeMarketStrategy::ensureStartupQuotePairAllRegistered(const std::string& tag) {
    for (const auto& sp : StrategyManager::getInstance().getAllStrategies()) {
        auto mm = std::dynamic_pointer_cast<MakeMarketStrategy>(sp);
        if (!mm) {
            continue;
        }
        try {
            mm->ensureStartupQuotePair(tag);
        } catch (const std::exception& e) {
            Main().logger()->warn(
                "[STRATEGY:{}] ensureStartupQuotePair error: {}", mm->getName(), e.what());
        } catch (...) {
        }
    }
}

void MakeMarketStrategy::mmNotifyInstrumentMaxPositionChanged(const std::string& ax_symbol,
                                                              int old_cap,
                                                              int new_cap) {
    if (ax_symbol.empty() || old_cap == new_cap) {
        return;
    }
    auto& cfg = config::Config::getInstance();
    const int eff_new = new_cap > 0 ? new_cap : cfg.getMarketMakerMaxPosition();
    const long long cap_ll = static_cast<long long>(std::max(1, eff_new));

    long long net_po = 0;
    for (const auto& sp : StrategyManager::getInstance().getAllStrategies()) {
        auto mm = std::dynamic_pointer_cast<MakeMarketStrategy>(sp);
        if (!mm || mm->mmAxSymbol() != ax_symbol) {
            continue;
        }
        (void)mm->mmRefreshNetPositionFromPortfolioCache("instrument_cap_change");
        std::lock_guard<std::mutex> ps_lk(mm->position_state_mutex_);
        net_po = static_cast<long long>(std::llround(mm->position_state_.net_position_qty));
        break;
    }

    if (new_cap < old_cap || std::llabs(net_po) >= cap_ll) {
        mmPullGrowSideOrdersAtOrPastCap(
            ax_symbol,
            net_po,
            cap_ll,
            new_cap < old_cap ? "instrument_cap_decreased" : "instrument_cap_at_limit",
            nullptr);
    }

    const char* cycle_reason =
        (new_cap > old_cap) ? "instrument_cap_increased"
                            : (new_cap < old_cap) ? "instrument_cap_decreased" : "instrument_limits_update";

    int stacks = 0;
    for (const auto& sp : StrategyManager::getInstance().getAllStrategies()) {
        auto mm = std::dynamic_pointer_cast<MakeMarketStrategy>(sp);
        if (!mm || mm->mmAxSymbol() != ax_symbol) {
            continue;
        }
        if (!mm->isRunning() || !mm->isEnabled()) {
            continue;
        }
        mm->noteReduceOnlyState(net_po, cap_ll);
        mm->enqueueQuoteCycleOnMover(cycle_reason);
        ++stacks;
    }

    if (utils::Logger::isInitialized()) {
        Main().logger()->info(
            "[MM_CAP] instrument_max_position_changed ax={} old_max={} new_max={} net_po={} "
            "stacks_enqueued={} reason={}",
            ax_symbol,
            old_cap,
            new_cap,
            net_po,
            stacks,
            cycle_reason);
    }
}

void MakeMarketStrategy::mmNoteMmOrdersEnabledTransitionAfterConfigReload(core::Price theo_mid_hint) {
    std::lock_guard<std::recursive_mutex> cycle_lock(mm_cycle_mutex_);
    auto& cfg = config::Config::getInstance();
    const bool moe = cfg.getMarketMakerMmOrdersEnabledForSymbol(mmAxSymbol());
    if (mm_mm_orders_gate_config_seen_ && !mm_last_mm_orders_enabled_from_config_ && moe) {
        core::Price theo = theo_mid_hint;
        if (!(std::isfinite(theo) && theo > 0.0)) {
            if (auto opt = mmReadTransformedTheo()) {
                theo = *opt;
            }
        }
        if (std::isfinite(theo) && theo > 0.0 && cfg.isMarketMakerEnabled() && mmMarketMakerOrdersPlacementAllowed(cfg)) {
            if (cfg.getBool("market_maker.cpp_manual_quotes_only", true) && !mmIsDeskManaged()) {
                Main().logger()->info(
                    "[STRATEGY:{}] market_maker.mm_orders_enabled became true — skipping legacy auto-requote "
                    "(market_maker.cpp_manual_quotes_only=true; seed working limits from GUI or set false)",
                    getName());
            } else {
                Main().logger()->info(
                    "[STRATEGY:{}] market_maker.mm_orders_enabled became true — MM resume (theo={:.6f})",
                    getName(),
                    theo);
                requoteFromTheo(theo, "mm_orders_resume");
            }
        }
    }
    mm_last_mm_orders_enabled_from_config_ = moe;
    mm_mm_orders_gate_config_seen_ = true;
}

void MakeMarketStrategy::mmMaybeCancelOnMmOrdersDisabled(const config::Config& cfg) {
    std::lock_guard<std::recursive_mutex> cycle_lock(mm_cycle_mutex_);
    mmUpdateMmOrdersEnabledTransitionLocked(cfg);
}

void MakeMarketStrategy::mmUpdateMmOrdersEnabledTransitionLocked(const config::Config& cfg) {
    if (!cfg.getMarketMakerMmOrdersEnabledForSymbol(mmAxSymbol())) {
        if (mm_prev_mm_orders_enabled_) {
            mmCancelAllExchangeAndResetLocal("mm_orders_disabled");
        }
        mm_prev_mm_orders_enabled_ = false;
    } else {
        mm_prev_mm_orders_enabled_ = true;
    }
}

bool MakeMarketStrategy::mmMarketMakerOrdersPlacementAllowed(const config::Config& cfg) const {
    return cfg.isMarketMakerEnabled() && cfg.getMarketMakerMmOrdersEnabledForSymbol(mmAxSymbol());
}

bool MakeMarketStrategy::mmPlacementAllowedForSymbol(const config::Config& cfg) const {
    // Compose the two existing suppression helpers — no new inline condition. Placement is
    // allowed only when the stack is enabled + not held (mm_orders_enabled for this symbol)
    // AND not paused by the MWR / global fast-market breaker. Cancels are never gated here.
    return mmMarketMakerOrdersPlacementAllowed(cfg) && !mmMwrIsPausedNow();
}

void MakeMarketStrategy::onFeedUpdate(const marketdata::ExternalFeedQuote& quote) {
    // === WORKER-THREAD SURVIVAL GUARD ===
    // Live incident 2026-05-06: every MM strategy stopped emitting any logs after a desk reseed
    // race even though the process and feeds were healthy — the worker thread that runs
    // onFeedUpdate had stopped iterating and the existing try/catch in notifyCallbacks only
    // covered std::exception. Wrap the entire body so non-std exceptions and asserts cannot
    // silently kill the worker, AND record per-call elapsed time so a future "stuck for >1s"
    // callback shows up with a wall-clock duration.
    //
    // CONCURRENCY (2026-05-20 audit, fix C5): serialize onFeedUpdate on THIS strategy.
    // ExternalFeedManager runs multiple feed worker threads (poll_thread_,
    // aux_poll_thread_, mettraders_thread_). When a strategy is bound to a multi-theo
    // primary+auxiliary configuration two of those threads can deliver into the same
    // strategy concurrently. Held for the full function so internal state (last_theo_,
    // banner state, gate logs, enqueued cycle reasons) is consistent. Acquired BEFORE
    // the try block so it releases via lock_guard even if a non-std exception escapes.
    std::lock_guard<std::mutex> feed_cb_lk(mm_feed_callback_mutex_);
    const auto fu_t0 = std::chrono::steady_clock::now();
    try {
    {
        const std::string ax = mmAxSymbol();
        const std::int64_t now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                        fu_t0.time_since_epoch())
                                        .count();
        static std::mutex s_feed_watchdog_mu;
        static std::unordered_map<std::string, std::int64_t> s_last_feed_tick_ms_by_ax;
        constexpr std::int64_t kFeedStaleMs = 5000;
        {
            std::lock_guard<std::mutex> wlk(s_feed_watchdog_mu);
            const auto it = s_last_feed_tick_ms_by_ax.find(ax);
            if (it != s_last_feed_tick_ms_by_ax.end() && it->second > 0 &&
                (now_ms - it->second) >= kFeedStaleMs && utils::Logger::isInitialized()) {
                Main().logger()->warn(
                    "[MM_FEED_WATCHDOG] feed_stale ax={} strategy={} gap_ms={} — alert only (no cancel)",
                    ax,
                    getName(),
                    now_ms - it->second);
            }
            s_last_feed_tick_ms_by_ax[ax] = now_ms;
        }
    }
    std::size_t tracked_bids_snapshot = 0;
    std::size_t tracked_asks_snapshot = 0;
    {
        std::lock_guard<std::mutex> lk(tracked_orders_mutex_);
        tracked_bids_snapshot = tracked_bids_.size();
        tracked_asks_snapshot = tracked_asks_.size();
    }
    const bool desk_managed = mmIsDeskManaged();
    if (utils::Logger::isInitialized()) {
        Main().logger()->debug(
            "[CALL_TRACE:{}] {} entered: tracked_bids={} tracked_asks={} passive={} desk_stack={}",
            mmAxSymbol(),
            __func__,
            tracked_bids_snapshot,
            tracked_asks_snapshot,
            passive_until_first_manual_order_,
            desk_managed ? 1 : 0);
    }
    // Every external (e.g. Neon) theo tick: reload disk config first, then apply MM logic below.
    (void)config::Config::getInstance().reloadPrimaryConfigFromDisk();
    auto& cfg = config::Config::getInstance();
    mmMaybeCancelOnMmOrdersDisabled(cfg);

    // ── Per-leg theo transform (theo_scale or pricer-snapshot) at the feed boundary ──────
    // `quote.mid` is the RAW external venue mid (USD/JPY ≈ 159, SP500 index ≈ 7150, …). The
    // strategy quotes in the AX product's native price scale — typically the same as the
    // theo, but legs can configure `theo_scale` (e.g. SPY=0.1) or a per-stack pricer-snapshot
    // transform to bridge different scales. Apply once here so every downstream consumer in
    // this function (banner, MM math,
    // requote path, last_theo_ cache) sees the same units the strategy actually quotes in.
    //
    // The raw `quote.bid/ask/mid` are still emitted on the [MM_FEED] stdout line below — that
    // line is intentionally the venue's literal book, useful for diagnosing feed health.
    const Price feed_mid_transformed =
        (quote.valid && std::isfinite(quote.mid) && quote.mid > 0.0)
            ? mmTransformTheo(quote.mid)
            : 0.0;
    const bool feed_transformed_ok =
        std::isfinite(feed_mid_transformed) && feed_mid_transformed > 0.0;

    // Desk / mm_orders resume uses `theo_mid_hint`. All MM strategies receive the same broadcast
    // feed callback, so we must not treat another leg's mid (e.g. a commodity vs an FX pair) as
    // this leg's theo — only use the transformed feed mid when this quote targets our
    // `mmTheoSymbol()` (same rules as the rest of onFeedUpdate below).
    const bool quote_applies_to_leg =
        quote.valid &&
        (!cfg.isMultiTheoFeeds() || !quote.fix_symbol.empty()) &&
        (quote.fix_symbol.empty() ||
         marketdata::fixSymbolCanonical(quote.fix_symbol) ==
             marketdata::fixSymbolCanonical(mmTheoSymbol()));
    {
        Price transition_hint = last_theo_;
        if (quote_applies_to_leg && feed_transformed_ok) {
            transition_hint = feed_mid_transformed;
        }
        mmNoteMmOrdersEnabledTransitionAfterConfigReload(transition_hint);
    }
    if (!quote.valid) {
        if (cfg.isMarketMakerEnabled() && cfg.getMarketMakerCancelOnExternalFeedInvalid()) {
            const bool global = quote.fix_symbol.empty();
            const bool ours = !quote.fix_symbol.empty() &&
                marketdata::fixSymbolCanonical(quote.fix_symbol) ==
                    marketdata::fixSymbolCanonical(mmTheoSymbol());
            if (global || ours) {
                mmCancelAllExchangeAndResetLocal("mm_external_feed_invalid");
            }
        }
        return;
    }

    if (cfg.isMultiTheoFeeds() && quote.fix_symbol.empty()) {
        return;
    }
    if (!quote.fix_symbol.empty()) {
        if (marketdata::fixSymbolCanonical(quote.fix_symbol) !=
            marketdata::fixSymbolCanonical(mmTheoSymbol())) {
            return;
        }
    }

    // After this point, if the transform produced a non-positive value (e.g. scale * raw
    // underflow or snapshot formula producing a negative), the strategy has nothing valid to quote.
    if (!feed_transformed_ok) {
        return;
    }

    constexpr double kFeedStdoutEps = 1e-9;
    const bool quote_changed =
        !mm_feed_stdout_initialized_ ||
        std::fabs(quote.bid - last_mm_feed_stdout_bid_) > kFeedStdoutEps ||
        std::fabs(quote.ask - last_mm_feed_stdout_ask_) > kFeedStdoutEps;
    if (quote_changed) {
        mm_feed_stdout_initialized_ = true;
        last_mm_feed_stdout_bid_ = quote.bid;
        last_mm_feed_stdout_ask_ = quote.ask;

        const std::string feed_name = cfg.getString("external_feed.name", "external");
        const std::string feed_fallback = cfg.getExternalFeedProvider();
        const std::string feed_label = !mmTheoSource().empty() ? mmTheoSource() : feed_fallback;
        std::ostringstream line;
        line << std::fixed << std::setprecision(6);
        line << "[MM_FEED] " << feed_name;
        if (!feed_label.empty()) {
            line << " [" << feed_label << "]";
        }
        line << " bid=" << quote.bid << " ask=" << quote.ask << " mid=" << quote.mid << "\n";
        std::cout << line.str() << std::flush;
    }
    
    // Only modify orders when theo actually changes (avoid unnecessary API calls).
    // Compare the *transformed* feed mid against last_theo_ — last_theo_ is also stored in
    // transformed units below, so the epsilon-gate is consistent across legs (a 1e-8 epsilon
    // on a JPYUSD mid ≈ 156 — after snapshot transform — is meaningfully different from a 1e-8
    // epsilon on a $0.38 altcoin).
    constexpr double PRICE_CHANGE_EPSILON = 1e-8;
    double price_change = feed_mid_transformed - last_theo_;

    if (std::fabs(price_change) < PRICE_CHANGE_EPSILON) {
        return;  // No price change, skip
    }

    bool have_working_signal = hasTrackedOrders() || bid_order_id_ != 0 || ask_order_id_ != 0;
    // Desk-seeded but not yet adopted: a fresh mm_req_<AX>_<stack_id> strategy that just spawned
    // (passive=true, no tracked legs, no bid/ask ids) but holds a desk-seeded manual_stack with
    // gateway-placed OIDs. Without an explicit retry here, onFeedUpdate would skip the rest of
    // this function and the strategy would never adopt. Retry every ~2.5s.
    const auto ms_adopt = mmOptionalManualStack();
    if (desk_managed && !have_working_signal && ms_adopt.has_value() &&
        ms_adopt->desk_seeded &&
        (ms_adopt->placed_bid_price > 0.0 || ms_adopt->placed_ask_price > 0.0)) {
        const auto now_ms_ad = std::chrono::duration_cast<std::chrono::milliseconds>(
                                   std::chrono::steady_clock::now().time_since_epoch())
                                   .count();
        const std::int64_t last_try =
            mm_last_desk_adopt_attempt_ms_.load(std::memory_order_relaxed);
        if (now_ms_ad - last_try >= 2500) {
            mm_last_desk_adopt_attempt_ms_.store(now_ms_ad, std::memory_order_relaxed);
            const bool adopted_retry = tryAdoptGatewayPlacedFromManualStack();
            if (utils::Logger::isInitialized()) {
                Main().logger()->info(
                    "[MM_ORDERS] desk-managed adoption retry on feed update: strategy={} stack_id={} "
                    "result={} bid_oid='{}' ask_oid='{}' bid_px={:.6f} ask_px={:.6f}",
                    getName(),
                    mmOrderRequestId(),
                    adopted_retry ? "adopted" : "still_pending",
                    ms_adopt->bid_exchange_oid,
                    ms_adopt->ask_exchange_oid,
                    ms_adopt->placed_bid_price,
                    ms_adopt->placed_ask_price);
            }
            // Recompute working signal — adoption may have populated bid_order_id_/ask_order_id_.
            have_working_signal =
                hasTrackedOrders() || bid_order_id_ != 0 || ask_order_id_ != 0;
        }
    }
    // Desk-managed pairs are adopted externally — passive_until_first_manual_order_ would otherwise
    // gate them out of onFeedUpdate forever. Clear it here defensively whenever we have a working
    // signal under desk management.
    if (desk_managed && have_working_signal && passive_until_first_manual_order_) {
        passive_until_first_manual_order_ = false;
        if (utils::Logger::isInitialized()) {
            Main().logger()->info(
                "[MM_ORDERS] desk-managed: cleared passive_until_first_manual_order_ on feed update sym={} bid_id={} ask_id={}",
                mmAxSymbol(), bid_order_id_, ask_order_id_);
        }
    }
    const bool allow_order_actions = !(passive_until_first_manual_order_ || !have_working_signal);
    if (utils::Logger::isInitialized() && desk_managed && !allow_order_actions) {
        // Throttle this diagnostic to once per ~5s per strategy so it doesn't drown the log when
        // desk has no live pair (e.g. just after a full fill before the next desk place).
        static std::mutex s_gated_mu;
        static std::unordered_map<std::string, std::int64_t> s_gated_last_ms;
        const auto now_g = std::chrono::duration_cast<std::chrono::milliseconds>(
                               std::chrono::steady_clock::now().time_since_epoch())
                               .count();
        bool emit_g = false;
        {
            std::lock_guard<std::mutex> lk(s_gated_mu);
            auto& slot = s_gated_last_ms[getName()];
            if (now_g - slot >= 5000) {
                slot = now_g;
                emit_g = true;
            }
        }
        if (emit_g) {
            const auto ms_g = mmOptionalManualStack();
            Main().logger()->info(
                "[MM_ORDERS] desk-managed feed update gated: strategy={} sym={} passive={} "
                "have_working_signal={} bid_id={} ask_id={} desk_seeded={} placed_bid={:.6f} placed_ask={:.6f}",
                getName(),
                mmAxSymbol(),
                passive_until_first_manual_order_ ? 1 : 0,
                have_working_signal ? 1 : 0,
                bid_order_id_,
                ask_order_id_,
                (ms_g.has_value() && ms_g->desk_seeded) ? 1 : 0,
                ms_g.has_value() ? ms_g->placed_bid_price : 0.0,
                ms_g.has_value() ? ms_g->placed_ask_price : 0.0);
        }
    }
    if (utils::Logger::isInitialized()) {
        static std::mutex s_feed_route_mu;
        static std::unordered_map<std::string, std::pair<std::string, std::int64_t>> s_feed_route_last;
        const std::string sym = mmAxSymbol();
        const std::string route = desk_managed ? "state_machine" : "legacy_self";
        const std::int64_t now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                        std::chrono::steady_clock::now().time_since_epoch())
                                        .count();
        bool emit = false;
        {
            std::lock_guard<std::mutex> lk(s_feed_route_mu);
            auto& slot = s_feed_route_last[sym];
            if (slot.first != route || now_ms - slot.second >= 5000) {
                slot = {route, now_ms};
                emit = true;
            }
        }
        if (emit) {
            Main().logger()->debug(
                "[FEED_ROUTE] sym={} path={} desk_active={} mm_req_id='{}' tracked_bids={} tracked_asks={}",
                sym,
                route,
                mm_desk_active_ ? "true" : "false",
                mmOrderRequestId(),
                tracked_bids_snapshot,
                tracked_asks_snapshot);
        }
    }

    // === DETAILED LOGGING ===
    const int resting = cfg.getMarketMakerRestingDepthExtraTicks();
    const int eff_sp = mmEffBidSpreadTicks(cfg);
    int bid_w = eff_sp;
    int ask_w = eff_sp;
    const double quote_tick = mmEffResolvedQuoteTick(cfg);
    const bool quote_tick_ok = (quote_tick > 0.0 && std::isfinite(quote_tick));
    double basis = cfg.getMarketMakerBasis();

    // Apply per-stack pricer-snapshot transform here too — this is the BANNER computation that
    // the FEED PRICE CHANGE log shows, so it must agree with the actual quote-cycle math (which
    // also routes through `mmApplyPricerSnapshotTransform`). Otherwise the banner would say
    // "Target prices: BID=raw_theo±half" while the venue gets "newMidpoint±half" and the
    // operator would see two different numbers on screen and on the venue.
    Price adjusted_theo = mmApplyPricerSnapshotTransform(feed_mid_transformed) + basis;
    mmCapSpreadBpsToDeskPlacedLadder(adjusted_theo, bid_w, ask_w);
    Price new_bid = 0.0;
    Price new_ask = 0.0;
    if (quote_tick_ok) {
        Price raw_bid = 0.0;
        Price raw_ask = 0.0;
        computeInventorySkewedRawBidAsk(adjusted_theo, bid_w, ask_w, quote_tick, raw_bid, raw_ask);
        if (resting > 0) {
            raw_bid -= static_cast<double>(resting) * quote_tick;
            raw_ask += static_cast<double>(resting) * quote_tick;
        }
        finalizeMmPairOnTickGrid(raw_bid, raw_ask, bid_w, ask_w, quote_tick, new_bid, new_ask);
    } else {
        mmLogMmGateThrottled(
            "unresolved_quote_tick",
            "onFeedUpdate",
            "banner/target grid skipped for ax=" + mmAxSymbol() +
                " — fetch GET /instruments (ax_gateway_instruments_catalog) or set merged leg tick_size");
    }

    const bool first_theo = std::fabs(last_theo_) < 1e-12;
    const bool targets_changed =
        !feed_banner_targets_inited_ ||
        std::fabs(new_bid - last_feed_banner_target_bid_) > 1e-9 ||
        std::fabs(new_ask - last_feed_banner_target_ask_) > 1e-9;

    const int banner_min_ms = cfg.getMarketMakerFeedLogBannerMinIntervalMs();
    const auto now_banner = std::chrono::steady_clock::now();
    bool banner_debounce_ok =
        cfg.getMarketMakerFeedLogVerbose() || first_theo || banner_min_ms <= 0 ||
        !last_feed_banner_emit_at_.has_value() ||
        std::chrono::duration_cast<std::chrono::milliseconds>(now_banner - *last_feed_banner_emit_at_)
                .count() >= banner_min_ms;

    const bool log_feed_banner =
        have_working_signal &&
        (cfg.getMarketMakerFeedLogVerbose() || first_theo || (targets_changed && banner_debounce_ok));

    feed_banner_targets_inited_ = true;
    last_feed_banner_target_bid_ = new_bid;
    last_feed_banner_target_ask_ = new_ask;

    if (log_feed_banner) {
        last_feed_banner_emit_at_ = now_banner;

        Main().logger()->info("╔════════════════ FEED PRICE CHANGE ════════════════╗");
        Main().logger()->info(
            "║ strategy={} ax={} stack={}",
            getName(),
            mmAxSymbol(),
            [&] { const std::string r = mmOrderRequestId();
                  return r.empty()
                             ? (desk_managed ? std::string("(desk_active)") : std::string("(legacy)"))
                             : r; }());
        Main().logger()->info("║ Previous Theo: {:.6f}", last_theo_);
        // `New Theo` shows the post-transform theo so the displayed value matches the units the
        // strategy actually quotes in (e.g. JPYUSD-PERP ≈ 156 after the pricer-snapshot transform
        // is applied to a Neon USD/JPY feed ≈ 157). For diagnostics on the raw venue mid, see
        // the [MM_FEED] stdout line above.
        Main().logger()->info("║ New Theo:      {:.6f} (change: {:+.6f})", feed_mid_transformed, price_change);
        Main().logger()->info("║ Adjusted Theo: {:.6f} (basis: {:.6f})", adjusted_theo, basis);
        Main().logger()->info("║ ─────────────────────────────────────────────────");
        // bid_order_id_/ask_order_id_ are internal OM ids (monotonic per new submit), not “how many” orders.
        Main().logger()->info(
            "║ MM leg OrderManager ids (0=none; id grows each replace): bid={} ask={}",
            bid_order_id_,
            ask_order_id_);
        // Only show working-quote prices when an order id is live; last_* is set at submit-before-accept
        // and would misrepresent “current quote” while REST is in flight or after a reject.
        // CONCURRENCY (fix C3): explicit .load() — atomic<OrderId> + atomic<Price> would otherwise
        // make the ?: conditional ambiguous because both operands can convert in either direction.
        const Price disp_bid = (bid_order_id_.load(std::memory_order_acquire) != 0)
                                 ? last_bid_price_.load(std::memory_order_acquire)
                                 : Price{0.0};
        const Price disp_ask = (ask_order_id_.load(std::memory_order_acquire) != 0)
                                 ? last_ask_price_.load(std::memory_order_acquire)
                                 : Price{0.0};
        if (bid_order_id_ == 0 || ask_order_id_ == 0) {
            Main().logger()->info("║ Working quote:  BID={:.5f} ASK={:.5f} (0 = no live order on that side)",
                                  disp_bid, disp_ask);
        } else {
            Main().logger()->info("║ Working quote:  BID={:.5f} ASK={:.5f}", disp_bid, disp_ask);
        }
        Main().logger()->info("║ Target prices:  BID={:.5f} ASK={:.5f} (from latest theo + MM math)",
                              new_bid, new_ask);
        Main().logger()->info("╚════════════════════════════════════════════════════╝");
    }
    
    // Theo changed — optional cancel/replace (very chatty on high-frequency theo ticks when enabled).
    //
    // Do not require on_accept-tracked ids only. In real sessions the exchange can still have live
    // orders while our accept event is delayed/missed (temporary WS gap, startup race, or desk-seeded
    // adoption lag). `last_*_price_` is set at submit time, so include it as a "working intent" signal
    // to keep theo-driven reprice active instead of freezing until the next explicit timer reconcile.
    bool have_open_orders =
        (bid_order_id_ != 0 || ask_order_id_ != 0 || last_bid_price_ > 0.0 || last_ask_price_ > 0.0);
    bool have_remaining_bid = false;
    bool have_remaining_ask = false;
    Quantity snapshot_bid_filled = 0;
    Quantity snapshot_ask_filled = 0;
    
    {
        std::lock_guard<std::mutex> lock(fill_mutex_);
        snapshot_bid_filled = total_bid_filled_;
        snapshot_ask_filled = total_ask_filled_;
        have_remaining_bid =
            (original_bid_qty_ > 0 && snapshot_bid_filled + 1e-12 < original_bid_qty_);
        have_remaining_ask =
            (original_ask_qty_ > 0 && snapshot_ask_filled + 1e-12 < original_ask_qty_);
    }

    const bool requote_on_theo_move_cfg = cfg.getMarketMakerRequoteOnTheoMoveForSymbol(mmAxSymbol());
    const bool requote_on_theo_move = requote_on_theo_move_cfg;
    struct DeskOpenPairProbe {
        bool ok{false};
        bool has_pair{false};
        double bid_px{0.0};
        double ask_px{0.0};
        long long drift_ticks{0};
        int min_drift_ticks{1};
        double width_bps{0.0};
    };
    // === FREEZE FIX (race-free, no blocking) ===
    // BEFORE: every onFeedUpdate tick made a synchronous GET /open-orders call (200–800ms each)
    // inside the feed worker thread. With ~10 ticks/sec arriving from neon_fix and the REST RTT
    // serializing them, the feed thread was effectively frozen for 300–900ms windows even when
    // targets hadn't moved. That is the source of the "[FEED_DISPATCH_SLOW] elapsed_ms=357"
    // warnings the user has been seeing for days, and the apparent multi-second gaps in
    // [MM_SKEW] / order-move logs.
    //
    // AFTER: cache the venue probe per (ax|strategy) for `desk_probe_throttle_ms` (default
    // 2000ms). Between refreshes we reuse cached has_pair/bid_px/ask_px and only recompute
    // drift_ticks (cheap arithmetic) against the current feed mid + cached anchor. The anchor
    // update on drift>=min_drift still runs each tick, so the requote gate stays as responsive
    // as before. Net result: REST hit ~once every 2s instead of 10x/sec, and the feed worker
    // returns in <5ms in the steady state.
    struct DeskProbeCacheEntry {
        bool valid{false};
        bool has_pair{false};
        double bid_px{0.0};
        double ask_px{0.0};
        std::chrono::steady_clock::time_point last_fetch{};
    };
    static std::mutex s_desk_probe_cache_mu;
    static std::unordered_map<std::string, DeskProbeCacheEntry> s_desk_probe_cache;
    const int desk_probe_throttle_ms =
        std::max(0, cfg.getInt("market_maker.desk_probe_throttle_ms", 2000));
    auto probeDeskOpenPair = [&](double qt) {
        DeskOpenPairProbe probe;
        if (!desk_managed) {
            return probe;
        }
        if (!(qt > 0.0) || !std::isfinite(qt)) {
            return probe;
        }
        const int min_drift_ticks = std::max(1, mmEffMinTheoDriftTicksInt(cfg));
        probe.min_drift_ticks = min_drift_ticks;
        const double quote_tick = qt;
        const std::string cache_key = mmAxSymbol() + "|" + getName();
        const auto now_tp = std::chrono::steady_clock::now();
        bool need_fetch = true;
        DeskProbeCacheEntry cached{};
        {
            std::lock_guard<std::mutex> lk(s_desk_probe_cache_mu);
            auto it = s_desk_probe_cache.find(cache_key);
            if (it != s_desk_probe_cache.end() && it->second.valid) {
                const auto age_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                        now_tp - it->second.last_fetch).count();
                if (age_ms < desk_probe_throttle_ms) {
                    cached = it->second;
                    need_fetch = false;
                }
            }
        }
        if (!need_fetch) {
            // Reuse cached venue snapshot (no REST). drift_ticks is cheap to recompute against
            // the current anchor; anchor maintenance below is identical to the fresh-fetch path.
            probe.ok = true;
            probe.has_pair = cached.has_pair;
            probe.bid_px = cached.bid_px;
            probe.ask_px = cached.ask_px;
            probe.width_bps = (feed_mid_transformed > 0.0 && cached.has_pair)
                                  ? ((cached.ask_px - cached.bid_px) / feed_mid_transformed) * 10000.0
                                  : 0.0;
            static std::mutex s_pair_anchor_mu_cached;
            static std::unordered_map<std::string, double> s_anchor_by_key_cached;
            const std::string akey = mmAxSymbol() + "|" + getName();
            double anchor = feed_mid_transformed;
            {
                std::lock_guard<std::mutex> lk(s_pair_anchor_mu_cached);
                auto it = s_anchor_by_key_cached.find(akey);
                if (it == s_anchor_by_key_cached.end()) {
                    s_anchor_by_key_cached.emplace(akey, feed_mid_transformed);
                    anchor = feed_mid_transformed;
                } else {
                    anchor = it->second;
                }
            }
            probe.drift_ticks = static_cast<long long>(
                std::llabs(std::llround((feed_mid_transformed - anchor) / quote_tick)));
            if (probe.drift_ticks >= static_cast<long long>(min_drift_ticks)) {
                std::lock_guard<std::mutex> lk(s_pair_anchor_mu_cached);
                s_anchor_by_key_cached[akey] = feed_mid_transformed;
            }
            return probe;
        }
        // === FEED-THREAD NON-BLOCKING ===
        // Live evidence 2026-05-07 16:05:36.068 → 16:05:37.190: feed thread 1576137 logged
        // FEED PRICE CHANGE then sat for 1122 ms before emitting FEED_DISPATCH_SLOW. The gap
        // matches a single GET /open-orders RTT (881 ms+ as later logged from the mover for
        // the same endpoint) plus curl_mutex_ contention with the mover. The probe was the
        // last synchronous REST/curl point on the feed dispatch thread.
        //
        // We now refuse to do REST (or disk I/O — the registry write below was also on this
        // thread) here. When `need_fetch` is true we return probe.ok=false; the caller
        // (around line 3034) falls back to the local-drift gate using last_bid_price_ /
        // last_ask_price_, which are accurate for adopted desk-managed stacks. The mover
        // thread's mmReconcileTrackedAgainstVenue() already does GET /open-orders at the
        // start of every drain cycle to keep tracked_bids_/tracked_asks_ in sync with venue
        // truth, so we have NOT lost the venue-as-source-of-truth invariant.
        return probe;
        // [DEAD] legacy inline REST + disk-registry write — preserved as `if (false)` so the
        // historical anchor + width-registry logic survives as a single textual reference.
        if (false) {
        auto resp = Main().rest()->getOrdersGatewayRelative("/open-orders", {{"symbol", mmAxSymbol()}});
        if (!resp.is_success) {
            return probe;
        }
        probe.ok = true;
        nlohmann::json root = nlohmann::json::object();
        try {
            root = nlohmann::json::parse(resp.body);
        } catch (...) {
            return probe;
        }
        std::vector<nlohmann::json> rows;
        if (root.is_array()) {
            for (const auto& el : root) {
                if (el.is_object()) rows.push_back(el);
            }
        } else if (root.is_object()) {
            for (const char* k : {"orders", "open_orders", "data", "items"}) {
                if (root.contains(k) && root[k].is_array()) {
                    for (const auto& el : root[k]) {
                        if (el.is_object()) rows.push_back(el);
                    }
                    break;
                }
            }
        }
        double best_bid = 0.0;
        double best_ask = 0.0;
        for (const auto& row : rows) {
            std::string side_s;
            if (row.contains("d") && row["d"].is_string()) side_s = row["d"].get<std::string>();
            if (side_s.empty() && row.contains("side") && row["side"].is_string()) side_s = row["side"].get<std::string>();
            for (char& ch : side_s) ch = static_cast<char>(std::toupper(static_cast<unsigned char>(ch)));
            bool is_buy = (!side_s.empty() && (side_s[0] == 'B'));
            bool is_sell = (!side_s.empty() && (side_s[0] == 'S' || side_s[0] == 'A'));
            double px = 0.0;
            for (const char* pk : {"p", "price", "limit_price", "limitPrice"}) {
                if (!row.contains(pk)) continue;
                const auto& v = row[pk];
                if (v.is_number()) {
                    px = v.get<double>();
                    break;
                }
                if (v.is_string()) {
                    try {
                        px = std::stod(v.get<std::string>());
                    } catch (...) {
                    }
                    if (px > 0.0) break;
                }
            }
            if (!(px > 0.0)) continue;
            if (is_buy) {
                if (best_bid <= 0.0 || px > best_bid) best_bid = px;
            } else if (is_sell) {
                if (best_ask <= 0.0 || px < best_ask) best_ask = px;
            }
        }
        if (!(best_bid > 0.0 && best_ask > 0.0 && best_ask > best_bid)) {
            // Persist "no pair" result so the next 2s of cached lookups don't re-hit REST.
            std::lock_guard<std::mutex> lk(s_desk_probe_cache_mu);
            auto& slot = s_desk_probe_cache[cache_key];
            slot.valid = true;
            slot.has_pair = false;
            slot.bid_px = 0.0;
            slot.ask_px = 0.0;
            slot.last_fetch = now_tp;
            return probe;
        }
        probe.has_pair = true;
        probe.bid_px = best_bid;
        probe.ask_px = best_ask;
        probe.width_bps = (feed_mid_transformed > 0.0)
                              ? ((best_ask - best_bid) / feed_mid_transformed) * 10000.0
                              : 0.0;
        static std::mutex s_pair_anchor_mu;
        static std::unordered_map<std::string, double> s_anchor_by_key;
        const std::string key = mmAxSymbol() + "|" + getName();
        double anchor = feed_mid_transformed;
        {
            std::lock_guard<std::mutex> lk(s_pair_anchor_mu);
            auto it = s_anchor_by_key.find(key);
            if (it == s_anchor_by_key.end()) {
                s_anchor_by_key.emplace(key, feed_mid_transformed);
                anchor = feed_mid_transformed;
            } else {
                anchor = it->second;
            }
        }
        probe.drift_ticks = static_cast<long long>(
            std::llabs(std::llround((feed_mid_transformed - anchor) / quote_tick)));
        if (probe.drift_ticks >= static_cast<long long>(min_drift_ticks)) {
            std::lock_guard<std::mutex> lk(s_pair_anchor_mu);
            s_anchor_by_key[key] = feed_mid_transformed;
        }
        try {
            namespace fs = std::filesystem;
            const std::string reg_rel =
                cfg.getString("mm_desk.pair_width_registry_path", "logs/mm_pair_width_registry.json");
            fs::path reg_p(reg_rel.empty() ? "logs/mm_pair_width_registry.json" : reg_rel);
            if (!reg_p.is_absolute()) {
                reg_p = fs::current_path() / reg_p;
            }
            std::error_code ec;
            fs::create_directories(reg_p.parent_path(), ec);
            nlohmann::json reg = nlohmann::json::object();
            if (fs::is_regular_file(reg_p, ec)) {
                std::ifstream f(reg_p, std::ios::binary);
                if (f) {
                    try {
                        f >> reg;
                    } catch (...) {
                        reg = nlohmann::json::object();
                    }
                }
            }
            if (!reg.is_object()) reg = nlohmann::json::object();
            nlohmann::json item;
            item["symbol"] = mmAxSymbol();
            item["strategy"] = getName();
            item["stack_id"] = mmOrderRequestId();
            item["width_bps"] = probe.width_bps;
            item["min_drift_ticks"] = min_drift_ticks;
            item["last_theo"] = feed_mid_transformed;
            item["open_bid"] = best_bid;
            item["open_ask"] = best_ask;
            item["drift_ticks"] = probe.drift_ticks;
            reg[mmAxSymbol() + "|" + getName()] = std::move(item);
            std::ofstream o(reg_p, std::ios::trunc | std::ios::binary);
            if (o) {
                o << reg.dump();
            }
        } catch (...) {
        }
        // Cache the freshly-fetched venue snapshot so the next ≤desk_probe_throttle_ms feed
        // ticks reuse it without REST.
        {
            std::lock_guard<std::mutex> lk(s_desk_probe_cache_mu);
            auto& slot = s_desk_probe_cache[cache_key];
            slot.valid = true;
            slot.has_pair = probe.has_pair;
            slot.bid_px = probe.bid_px;
            slot.ask_px = probe.ask_px;
            slot.last_fetch = now_tp;
        }
        return probe;
        }  // end [DEAD] if (false) block
        return probe;
    };
    const DeskOpenPairProbe desk_probe = probeDeskOpenPair(quote_tick);
    // === CYCLE-LOCK GATE (2026-05-08) ============================================
    // Skip the drift-gate evaluation entirely when this pair has a cancel-replace
    // cycle in flight. Pre-fix: every feed tick during a 240-1000ms cycle
    // re-evaluated drift, occasionally racing the in-flight cancel/place pair
    // into a redundant cycle (phantom OIDs, reload-count churn) and ALWAYS
    // spamming `[MM_GATE] feed_theo_below_min_drift`. Post-fix: ZERO drift
    // evaluations between cancel REST sent and both legs adopted post-place.
    //
    // This skip applies ONLY to the drift-gate evaluation. Bookkeeping above
    // (theo cache, MM_SKEW logging, stats, last_theo_ update at the end of
    // onFeedUpdate) continues to run because it's idempotent and cheap. The
    // suppressed code is the per-tick decision to fire a NEW cycle.
    //
    // Multi-pair independence: each MM stack has its own MakeMarketStrategy
    // instance, so this gate is naturally per-pair. Pair A in CYCLE_PENDING
    // does NOT prevent Pair B's instance from evaluating drift.
    //
    // Cycle completion (mmMaybePostCycleRecheck, called from on_accept after
    // mm_pending_accepts_ hits 0) re-reads the current theo against the
    // just-placed prices once: if drift > min_drift, fires a new cycle
    // immediately ([CYCLE_RECHECK]); else logs [CYCLE_DONE] and resumes
    // normal feed-driven evaluation.
    const bool cycle_in_flight = isPairCycleInFlight();
    if (cycle_in_flight && allow_order_actions &&
        (desk_managed || requote_on_theo_move) &&
        (have_open_orders || have_remaining_bid || have_remaining_ask)) {
        // Would have evaluated the drift gate, but a cancel-replace is in flight.
        // Throttled to once per second per strategy via mmLogMmGateThrottled
        // (existing facility — same throttle key style as feed_theo_below_min_drift,
        // feed_theo_placement_blocked, etc.).
        mmLogMmGateThrottled(
            "cycle_lock_skip_drift",
            "onFeedUpdate",
            "[CYCLE_LOCK] sym=" + mmAxSymbol() +
                " stack=" + (mmOrderRequestIdOrLegacy()) +
                " pair has cycle in flight, skipping drift evaluation");
    }
    if (allow_order_actions &&
        (desk_managed || requote_on_theo_move) &&
        (have_open_orders || have_remaining_bid || have_remaining_ask) &&
        !cycle_in_flight) {
        int min_drift_ticks = mmEffMinTheoDriftTicksInt(cfg);
        // Reduce-only (|NetPo| >= max in practice): at most one add-side is quoted. Config often sets
        // min_theo_move_ticks=2 to damp two-sided churn; a single working reduce order would otherwise sit
        // stale until 2*tick drift. Re-peg that leg on 1-tick theo target moves instead.
        {
            const bool w_b = shouldQuoteSide(Side::BUY);
            const bool w_a = shouldQuoteSide(Side::SELL);
            if (w_b != w_a && min_drift_ticks > 0) {
                min_drift_ticks = 1;
            }
        }
        bool pass_min_move = quote_tick_ok;
        if (quote_tick_ok && desk_managed) {
            if (desk_probe.ok && desk_probe.has_pair) {
                pass_min_move = desk_probe.drift_ticks >= static_cast<long long>(desk_probe.min_drift_ticks);
            } else {
                // Never hard-block repricing on probe/parser misses. Fall back to local working-vs-target drift
                // so theo-based cancel-replace still moves pairs.
                pass_min_move = true;
                if (utils::Logger::isInitialized()) {
                    Main().logger()->info(
                        "[MM_ORDERS] open-orders probe unavailable for sym={} (ok={} has_pair={}) — "
                        "fallback to local drift gate",
                        mmAxSymbol(),
                        desk_probe.ok ? 1 : 0,
                        desk_probe.has_pair ? 1 : 0);
                }
            }
        }
        if (quote_tick_ok && min_drift_ticks > 0) {
            const double need = static_cast<double>(min_drift_ticks) * quote_tick;
            bool any_checked = false;
            bool any_exceeds = false;
            if (bid_order_id_ != 0 && last_bid_price_ > 0.0) {
                any_checked = true;
                if (std::fabs(last_bid_price_ - new_bid) + 1e-12 >= need) {
                    any_exceeds = true;
                }
            }
            if (ask_order_id_ != 0 && last_ask_price_ > 0.0) {
                any_checked = true;
                if (std::fabs(last_ask_price_ - new_ask) + 1e-12 >= need) {
                    any_exceeds = true;
                }
            }
            if (any_checked) {
                pass_min_move = any_exceeds;
            }
        }
        if (pass_min_move) {
            (void)config::Config::getInstance().reloadPrimaryConfigFromDisk();
            auto& cfg_ord = config::Config::getInstance();
            mmMaybeCancelOnMmOrdersDisabled(cfg_ord);
            if (mmMarketMakerOrdersPlacementAllowed(cfg_ord)) {
                if (desk_managed) {
                    // MM_TT: stamp tick-arrival at the moment we decide to requote so the
                    // cancel-first drain (processTrackedOrdersTheoMove) and the re-add place
                    // can report tick-to-cancel / tick-to-place. LOG-ONLY (no gating).
                    mm_tt_feed_arrival_ms_.store(mmSteadyMillis(), std::memory_order_relaxed);
                    mm_tt_cancel_sent_ms_.store(0, std::memory_order_relaxed);
                    // Always full pair cancel+replace on theo drift so bid/ask stay one width apart
                    // from the same theo snapshot (never reprice a single leg in isolation).
                    // Cancel-first: the drain routes desk "theo_move" through
                    // processTrackedOrdersTheoMove (immediate cancels, no gate before them);
                    // the gate/reconcile/NetPo checks gate ONLY the after-ack re-add.
                    enqueueQuoteCycleOnMover("theo_move");
                } else {
                    // Non-desk theo move: route through the legacy modify path. (The former
                    // `else if (false)` inline-place block — a feed-thread submit_order landmine —
                    // was deleted 2026-07: ALL placement now runs on the per-AX mover worker via
                    // enqueueQuoteCycleOnMover / runFullMmQuoteCycle. No submit_order on this thread.)
                    modifyQuotesAtTheo(feed_mid_transformed);
                }
            } else {
                Main().logger()->debug(
                    "[STRATEGY:{}] [FEED] theo_move order path skipped — config blocks MM orders "
                    "(mm_orders_enabled=false or strategy off)",
                    getName());
                mmLogMmGateThrottled(
                    "feed_theo_placement_blocked",
                    "onFeedUpdate",
                    "would theo_move but mmMarketMakerOrdersPlacementAllowed=false");
            }
        } else {
            if (desk_managed && utils::Logger::isInitialized()) {
                const double bid_diff = (bid_order_id_ != 0 && last_bid_price_ > 0.0)
                                            ? std::fabs(last_bid_price_ - new_bid)
                                            : 0.0;
                const double ask_diff = (ask_order_id_ != 0 && last_ask_price_ > 0.0)
                                            ? std::fabs(last_ask_price_ - new_ask)
                                            : 0.0;
                const long long bid_diff_ticks = (quote_tick > 0.0)
                                                     ? static_cast<long long>(std::llround(bid_diff / quote_tick))
                                                     : 0LL;
                const long long ask_diff_ticks = (quote_tick > 0.0)
                                                     ? static_cast<long long>(std::llround(ask_diff / quote_tick))
                                                     : 0LL;
                Main().logger()->info(
                    "[MM_ORDERS] desk drift gate not met: sym={} theo_drift_ticks={} min_drift={} "
                    "working_bid={:.6f} target_bid={:.6f} bid_diff_ticks={} "
                    "working_ask={:.6f} target_ask={:.6f} ask_diff_ticks={} "
                    "(targets unchanged after grid rounding — no cancel-replace needed)",
                    mmAxSymbol(),
                    desk_probe.ok ? desk_probe.drift_ticks : -1,
                    min_drift_ticks,
                    (bid_order_id_.load(std::memory_order_acquire) != 0 &&
                     last_bid_price_.load(std::memory_order_acquire) > 0.0)
                        ? last_bid_price_.load(std::memory_order_acquire)
                        : Price{0.0},
                    new_bid,
                    bid_diff_ticks,
                    (ask_order_id_.load(std::memory_order_acquire) != 0 &&
                     last_ask_price_.load(std::memory_order_acquire) > 0.0)
                        ? last_ask_price_.load(std::memory_order_acquire)
                        : Price{0.0},
                    new_ask,
                    ask_diff_ticks);
            }
            Main().logger()->debug(
                "[FEED] Quote drift vs theo |bid_act−bid_theo|={:.8f} |ask_act−ask_theo|={:.8f} < min {:.8f} "
                "({} ticks × AX quote_tick={}) — skip theo requote",
                (bid_order_id_ != 0 && last_bid_price_ > 0.0) ? std::fabs(last_bid_price_ - new_bid) : 0.0,
                (ask_order_id_ != 0 && last_ask_price_ > 0.0) ? std::fabs(last_ask_price_ - new_ask) : 0.0,
                static_cast<double>(min_drift_ticks) * quote_tick,
                min_drift_ticks,
                quote_tick);
            mmLogMmGateThrottled(
                "feed_theo_below_min_drift",
                "onFeedUpdate",
                "min_theo_move_ticks=" + std::to_string(min_drift_ticks) + " quote_tick=" + std::to_string(quote_tick));
        }
    } else if (!requote_on_theo_move && have_open_orders) {
        Main().logger()->debug(
            "[FEED] Theo moved (requote_on_theo_move=false) — resting quotes unchanged | bid_id={} ask_id={}",
            bid_order_id_, ask_order_id_);
        mmLogMmGateThrottled(
            "feed_theo_requote_disabled",
            "onFeedUpdate",
            "requote_on_theo_move=false for this AX symbol — resting quotes unchanged");
    } else {
        Main().logger()->debug(
            "[FEED] No open orders or remaining delta (bid_id={} ask_id={} bid_filled={} ask_filled={} "
            "orig_bid_qty={} orig_ask_qty={})",
            bid_order_id_,
            ask_order_id_,
            snapshot_bid_filled,
            snapshot_ask_filled,
            original_bid_qty_,
            original_ask_qty_);
    }

    // Keep cancel-ack reconciliation running for desk-managed stacks, but use the same
    // theo-move requote flow as main branch for both single and multi instrument.
    if (desk_managed) {
        maybeReconcileDeskCancelAckTimeouts();
    }

    // last_theo_ is stored in *transformed* units so subsequent comparisons (price_change above,
    // mmEffMinTheoDriftTicks gate, banner suppression) are unit-consistent with the orders
    // being placed.
    last_theo_ = feed_mid_transformed;

    // Slow-callback warning: a single onFeedUpdate that takes >250ms is a strong red flag (REST
    // contention, lock pile-up, or downstream stall). The historical incident took 0ms to log the
    // CALL_TRACE then never returned — without an upper bound the worker is blocked forever. We
    // can't kill the worker mid-call here, but we can surface the duration so the next stall is
    // immediately obvious.
    {
        const auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                    std::chrono::steady_clock::now() - fu_t0)
                                    .count();
        if (elapsed_ms >= 250 && utils::Logger::isInitialized()) {
            Main().logger()->warn(
                "[FEED_DISPATCH_SLOW] strategy={} sym={} onFeedUpdate elapsed_ms={} — "
                "investigate REST/lock contention",
                getName(), mmAxSymbol(), latencyDisplayMs(elapsed_ms));
        }
    }
    } catch (const std::exception& e) {
        if (utils::Logger::isInitialized()) {
            Main().logger()->error(
                "[FEED_DISPATCH_FAILED] strategy={} sym={} std::exception={} — worker survived",
                getName(), mmAxSymbol(), e.what());
        }
    } catch (...) {
        if (utils::Logger::isInitialized()) {
            Main().logger()->error(
                "[FEED_DISPATCH_FAILED] strategy={} sym={} ANY-THROW (non-std::exception) — worker survived",
                getName(), mmAxSymbol());
        }
    }
}

void MakeMarketStrategy::modifyQuotesAtTheo(Price new_theo_mid) {
    requoteFromTheo(new_theo_mid, "theo_move");
}

// =============================================================================
// Price Rounding
// =============================================================================

Price MakeMarketStrategy::roundToTick(Price price, double tick) const {
    if (tick <= 0) return price;
    return std::round(price / tick) * tick;
}

Price MakeMarketStrategy::roundBidDown(Price price, double tick) const {
    if (tick <= 0) return price;
    return std::floor(price / tick) * tick;
}

Price MakeMarketStrategy::roundAskUp(Price price, double tick) const {
    if (tick <= 0) return price;
    return std::ceil(price / tick) * tick;
}

void MakeMarketStrategy::finalizeMmPairOnTickGrid(Price raw_bid, Price raw_ask, int bid_spread_bps,
                                                   int ask_spread_bps, double quote_tick, Price& out_bid,
                                                   Price& out_ask) const {
    if (quote_tick <= 0.0) {
        out_bid = raw_bid;
        out_ask = raw_ask;
        return;
    }
    // Bid is floored to the tick grid (unchanged).
    out_bid = roundBidDown(raw_bid, quote_tick);
    // Ask is derived from the PLACED bid by a fixed integer number of ticks, so the spread can no
    // longer oscillate by ±1 tick from independent floor/ceil rounding (see Testing_Logs.txt).
    //
    // Tim spec 2026-06-12 ("Small change to math on order pricing"): the full width is the BID
    // side's bps offset DOUBLED — NOT (bid_offset + ask_offset). Steps:
    //   1. placed_bid = roundBidDown(raw_bid, tick)               (bid leg unchanged)
    //   2. full_width_offset = 2 * bid_offset                     (typed bps, doubled)
    //   3. full_width_ticks  = floor(full_width_offset / tick)    (ALWAYS round down)
    //   4. placed_ask        = placed_bid + full_width_ticks*tick
    // Recover the per-side bid offset from the symmetric raw span: raw_ask - raw_bid ==
    // bid_offset + ask_offset, so bid_offset = span * bid_bps / (bid_bps + ask_bps). For the
    // standard single-width desk config (bid_bps == ask_bps) this makes full_width_offset ==
    // (raw_ask - raw_bid) exactly, so placed prices are unchanged; it only diverges when the
    // operator sets asymmetric widths, where Tim wants the (bid) typed bps to drive the full
    // width. Minimum 1-tick spread preserved (same guard as before).
    const double full_span_px = raw_ask - raw_bid;  // == bid_offset + ask_offset
    const int bps_sum = bid_spread_bps + ask_spread_bps;
    const double full_width_offset =
        (bps_sum > 0)
            ? full_span_px * (2.0 * static_cast<double>(bid_spread_bps) / static_cast<double>(bps_sum))
            : full_span_px;  // == 2 * bid_offset
    const int full_width_ticks = std::max(1, static_cast<int>(full_width_offset / quote_tick));
    out_ask = out_bid + full_width_ticks * quote_tick;
}

// =============================================================================
// QO Validation
// =============================================================================

QOValidation MakeMarketStrategy::validateQuoteOrders(Price qo_bid, Price qo_ask, Price market_bid,
                                                       Price market_ask, bool check_crossing,
                                                       bool never_inside_spread) const {
    QOValidation result;

    if (check_crossing) {
        if (qo_bid > market_ask && market_ask > 0) {
            result.bid_would_cross = true;
            result.reason = "QO Bid " + std::to_string(qo_bid) + " > Market Ask " + std::to_string(market_ask);
        }
        if (qo_ask < market_bid && market_bid > 0) {
            result.ask_would_cross = true;
            if (!result.reason.empty()) {
                result.reason += "; ";
            }
            result.reason += "QO Ask " + std::to_string(qo_ask) + " < Market Bid " + std::to_string(market_bid);
        }
    }

    if (never_inside_spread && market_bid > 0 && market_ask > 0 && market_ask > market_bid) {
        if (qo_bid > market_bid) {
            result.bid_inside_spread = true;
            if (!result.reason.empty()) {
                result.reason += "; ";
            }
            result.reason +=
                "Inside spread bid: " + std::to_string(qo_bid) + " > Market Bid " + std::to_string(market_bid);
        }
        if (qo_ask < market_ask) {
            result.ask_inside_spread = true;
            if (!result.reason.empty()) {
                result.reason += "; ";
            }
            result.reason +=
                "Inside spread ask: " + std::to_string(qo_ask) + " < Market Ask " + std::to_string(market_ask);
        }
    }

    if (qo_bid >= qo_ask) {
        result.spread_inverted = true;
        if (!result.reason.empty()) {
            result.reason += "; ";
        }
        result.reason += "Inverted spread: Bid " + std::to_string(qo_bid) + " >= Ask " + std::to_string(qo_ask);
    }

    result.valid = !result.bid_would_cross && !result.ask_would_cross && !result.bid_inside_spread &&
                   !result.ask_inside_spread && !result.spread_inverted;

    if (result.valid) {
        result.reason = "Valid";
    }

    return result;
}

// =============================================================================
// Order Event Callbacks
// =============================================================================

void MakeMarketStrategy::registerMmOrderForReloadCountBump(const std::string& client_order_id,
                                                             const std::string& mm_cycle_reason) {
    if (client_order_id.empty() || mm_cycle_reason.empty()) {
        return;
    }
    std::lock_guard<std::mutex> lk(mm_accept_pending_mutex_);
    mm_pending_accept_cycle_reason_[client_order_id] = mm_cycle_reason;
}

void MakeMarketStrategy::on_accept(const OrderEventData& accept) {
    BaseStrategy::on_accept(accept);
    std::string cycle_reason;
    {
        std::lock_guard<std::mutex> lk(mm_accept_pending_mutex_);
        const auto it = mm_pending_accept_cycle_reason_.find(accept.client_order_id);
        if (it != mm_pending_accept_cycle_reason_.end()) {
            cycle_reason = it->second;
            mm_pending_accept_cycle_reason_.erase(it);
        }
    }
    auto& cfg = config::Config::getInstance();
    const bool fill_requote_accept = (cycle_reason.rfind("fill_requote", 0) == 0);
    // Fill-driven requotes bump `current_reload_count` once in processFill (per fill), not per accept,
    // so a bid+ask replace does not double-count and multi-stack products stay aligned.
    if (cycle_reason.rfind("fill_requote", 0) != 0 &&
        mmCycleReasonCountsTowardReloadLimit(cycle_reason, cfg.getMarketMakerReloadCyclesCountFillOnly())) {
        (void)cfg.bumpMarketMakerCurrentReloadCountForMmAccept(getName(), accept.client_order_id);
    }
    if (mm_pending_accepts_ > 0) {
        --mm_pending_accepts_;
        mm_pending_accepts_last_change_ms_.store(
            std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now().time_since_epoch())
                .count(),
            std::memory_order_relaxed);
    }
    mmNotifyProductPlacementGate(mmAxSymbol());

    // Calculate latency if we have the submit timestamp
    std::string latency_str;
    {
        std::lock_guard<std::mutex> lk(order_submit_times_mutex_);
        auto it = order_submit_times_.find(accept.client_order_id);
        if (it != order_submit_times_.end()) {
            auto t3_strategy_accept = std::chrono::steady_clock::now();
            auto total_latency_us = std::chrono::duration_cast<std::chrono::microseconds>(t3_strategy_accept - it->second).count();
            latency_str = " latency=" + std::to_string(latencyDisplayMsFromUs(total_latency_us)) + "ms";

            // === LATENCY SUMMARY for this order ===
            std::ostringstream ms;
            ms << std::fixed << std::setprecision(2) << latencyDisplayMsFromUs(total_latency_us);
            std::cout << "[LATENCY_TRACE] T3_STRATEGY_ACCEPT: order_id=" << accept.order_id
                      << " TOTAL_STRATEGY_LATENCY=" << latencyDisplayUs(total_latency_us) << "us (" << ms.str() << "ms)" << std::endl;

            order_submit_times_.erase(it);  // Clean up
        }
    }
    
    std::string exchange_oid;
    {
        auto op = orders::OrderManager::getInstance().getOrder(accept.order_id);
        if (op) {
            exchange_oid = op->exchange_order_id;
        }
    }
    const bool desk_managed = mmIsDeskManaged();
    bool matched_pending = false;
    std::int64_t place_ack_elapsed_ms = 0;
    OrderId fill_requote_deferred_cancel_oid{0};
    Side fill_requote_deferred_cancel_side{Side::BUY};
    {
        std::lock_guard<std::mutex> lk(tracked_orders_mutex_);
        Side out_side{};
        TrackedLeg* pending = findTrackedByPendingClientId(accept.client_order_id, &out_side);
        if (pending) {
            place_ack_elapsed_ms = mmSteadyMillis() - pending->state_entered_steady_ms;
            pending->local_oid = accept.order_id;
            pending->exchange_oid = exchange_oid;
            pending->exchange_px = accept.price;
            pending->remaining_qty = accept.quantity;
            pending->qty = accept.quantity;
            pending->pending_place_client_id.clear();
            matched_pending = true;
            if (utils::Logger::isInitialized()) {
                Main().logger()->info(
                    "[THEO_MOVE] place ack: leg={} oid={} px={} elapsed_ms={}",
                    out_side == Side::BUY ? "bid" : "ask",
                    exchange_oid.empty() ? std::to_string(static_cast<long long>(accept.order_id))
                                           : exchange_oid,
                    accept.price,
                    latencyDisplayMs(place_ack_elapsed_ms));
            }
            if (desk_managed && pending->cancel_after_opposite_settled) {
                pending->cancel_after_opposite_settled = false;
                pending->state = MmLegState::CANCEL_PENDING;
                pending->pending_cancel_intent = MmDeskCancelIntent::FillOpposite;
                pending->desk_cancel_timeout_retry_sent = false;
                pending->state_entered_steady_ms = mmSteadyMillis();
                fill_requote_deferred_cancel_oid = pending->local_oid;
                fill_requote_deferred_cancel_side = out_side;
            } else {
                pending->state = MmLegState::ADOPTED;
                pending->state_entered_steady_ms = mmSteadyMillis();
            }
        }
    }
    if (fill_requote_deferred_cancel_oid != 0) {
        const std::string opp_log =
            exchange_oid.empty() ? std::to_string(static_cast<long long>(fill_requote_deferred_cancel_oid))
                                 : exchange_oid;
        if (utils::Logger::isInitialized()) {
            Main().logger()->info("[FILL_REQUOTE] cancelling opposite leg (mover-queued): oid={}", opp_log);
        }
        // on_accept runs on the OM dispatch thread; the cancel REST goes to the AX worker so we
        // never block dispatch and any concurrent place/cancel for the same AX is serialised.
        enqueueRestCancelOnMover(
            fill_requote_deferred_cancel_oid, fill_requote_deferred_cancel_side, "fill_requote_defer_cancel");
    }
    if (!matched_pending) {
        addOrUpdateTrackedOrder(accept.order_id, exchange_oid, accept.side, accept.price, accept.quantity);
        Main().logger()->info(
            "[STRATEGY:{}] Adopted manual order oid={} side={} — now tracking and will move on theo",
            getName(),
            exchange_oid.empty() ? std::to_string(static_cast<long long>(accept.order_id)) : exchange_oid,
            sideToString(accept.side));
    }
    passive_until_first_manual_order_ = false;
    refreshLegacyTopOfBookTrackingFromVectors();
    if (const std::string req_id = mmOrderRequestId(); !req_id.empty()) {
        std::string bo;
        std::string ao;
        double bpx = 0.0;
        double apx = 0.0;
        int bq = 0;
        int aq = 0;
        {
            std::lock_guard<std::mutex> lk(tracked_orders_mutex_);
            if (tracked_bids_.size() == 1 && tracked_asks_.size() == 1 &&
                tracked_bids_.front().state == MmLegState::ADOPTED &&
                tracked_asks_.front().state == MmLegState::ADOPTED &&
                tracked_bids_.front().local_oid != 0 && tracked_asks_.front().local_oid != 0) {
                bo = tracked_bids_.front().exchange_oid;
                ao = tracked_asks_.front().exchange_oid;
                bpx = tracked_bids_.front().exchange_px;
                apx = tracked_asks_.front().exchange_px;
                bq = static_cast<int>(std::lround(tracked_bids_.front().qty));
                aq = static_cast<int>(std::lround(tracked_asks_.front().qty));
            }
        }
        if (!bo.empty() && !ao.empty()) {
            (void)mmDeskPatchOrdersJsonStackExchange(req_id, bo, ao, bpx, apx, bq, aq);
            if (utils::Logger::isInitialized()) {
                Main().logger()->info(
                    "[ADOPT] sym={} stack={} bid_oid={} ask_oid={} bid_px={} ask_px={}",
                    mmAxSymbol(),
                    req_id,
                    bo,
                    ao,
                    bpx,
                    apx);
                if (fill_requote_accept) {
                    Main().logger()->info(
                        "[FILL_REQUOTE] new pair adopted bid_oid={} ask_oid={}",
                        bo,
                        ao);
                }
            }
        }
    }

    // Stamp first-ack on the orders.json driver (one-shot, both sides). The
    // mm_orders.json writer in main.cpp uses this to know "AX confirmed at
    // least one side of the pair" before publishing the entry — matches the
    // user-confirmed contract that orders.json is only written on ack.
    if (!mm_first_accept_seen_.load()) {
        mm_first_accept_seen_.store(true);
        const auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
        mm_first_accept_ms_.store(static_cast<std::int64_t>(now_ms));
    }

    // === CYCLE-COMPLETION RE-CHECK (2026-05-08) ================================
    // If this accept brought us back to a fully-tracked state (no pending
    // accepts, no in-flight legs), re-check the drift gate ONCE against the
    // just-placed prices. Theo could have moved arbitrarily during the cycle
    // (the cycle-lock gate in onFeedUpdate suppressed every drift evaluation),
    // so the just-placed pair may already be stale. mmMaybePostCycleRecheck
    // emits [CYCLE_RECHECK] (and fires a new cycle) when drift is tripped, or
    // [CYCLE_DONE] (and resumes normal feed-driven evaluation) otherwise.
    mmMaybePostCycleRecheck();
}

void MakeMarketStrategy::on_fill(const OrderEventData& fill) {
    BaseStrategy::on_fill(fill);

    // Bookkeeping for the orders.json desk view: count + most recent fill id
    // so the GUI's per-stack row can show "fills=N · last_fill_id=…". Never
    // touches quoting math — the existing trackFill / processFill path is
    // unchanged below.
    mm_fills_count_.fetch_add(1, std::memory_order_relaxed);
    mm_last_fill_order_id_.store(fill.order_id, std::memory_order_relaxed);
    mm_last_fill_ms_.store(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count(),
        std::memory_order_relaxed);

    Quantity incremental = trackFill(fill, true);
    if (incremental > 0) {
        processFill(fill, incremental);
    }
}

void MakeMarketStrategy::on_partial_fill(const OrderEventData& fill) {
    BaseStrategy::on_partial_fill(fill);

    mm_fills_count_.fetch_add(1, std::memory_order_relaxed);
    mm_last_fill_order_id_.store(fill.order_id, std::memory_order_relaxed);
    mm_last_fill_ms_.store(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count(),
        std::memory_order_relaxed);

    Quantity incremental = trackFill(fill, false);
    if (incremental > 0) {
        processFill(fill, incremental);
    }
}

std::string MakeMarketStrategy::mmLastFillIdStr() const {
    const auto v = mm_last_fill_order_id_.load(std::memory_order_relaxed);
    return v == 0 ? std::string{} : std::to_string(static_cast<long long>(v));
}

Quantity MakeMarketStrategy::trackFill(const OrderEventData& fill, bool is_complete) {
    // === SIMPLIFIED FILL TRACKING ===
    // fill.filled_quantity is CUMULATIVE for that specific order
    // We track cumulative per order, and INCREMENT total by the delta
    // This avoids recalculating sums and is more robust
    
    // Single-threaded access (mutex protects all fill tracking)
    std::lock_guard<std::mutex> lock(fill_mutex_);
    
    bool is_current_bid = (fill.order_id == bid_order_id_);
    bool is_current_ask = (fill.order_id == ask_order_id_);
    
    // Select the right tracking variables based on side
    auto& fills_map = (fill.side == Side::BUY) ? bid_order_fills_ : ask_order_fills_;
    auto& total_filled = (fill.side == Side::BUY) ? total_bid_filled_ : total_ask_filled_;
    const char* side_name = (fill.side == Side::BUY) ? "BID" : "ASK";
    bool is_current = (fill.side == Side::BUY) ? is_current_bid : is_current_ask;
    
    // Get previous cumulative for this order
    Quantity prev_cumulative = 0;
    auto it = fills_map.find(fill.order_id);
    if (it != fills_map.end()) {
        prev_cumulative = it->second;
    }
    
    // Calculate INCREMENTAL fill (new cumulative - old cumulative)
    Quantity incremental = 0;
    if (fill.filled_quantity > prev_cumulative) {
        incremental = fill.filled_quantity - prev_cumulative;
        fills_map[fill.order_id] = fill.filled_quantity;
        total_filled += incremental;  // Increment by delta, not recalculate
    }

    if (incremental > 0) {
        std::cout << "[MM_FILL] " << (is_complete ? "COMPLETE" : "PARTIAL")
                  << " " << side_name
                  << " incremental_qty=" << incremental
                  << " cum_on_order=" << fill.filled_quantity
                  << " working_order_qty=" << fill.quantity
                  << " (next: MM_VERIFY after requote)\n"
                  << std::flush;
    }
    
    const Quantity orig_for_side =
        (fill.side == Side::BUY) ? original_bid_qty_ : original_ask_qty_;
    Quantity remaining =
        (orig_for_side > total_filled) ? (orig_for_side - total_filled) : 0;
    
    // Log fill details
    Main().logger()->info("╔════════════════════ {} ════════════════════╗", 
        is_complete ? "COMPLETE FILL" : "PARTIAL FILL");
    Main().logger()->info("║ Order ID: {} | is_current={} | Side: {}", 
        fill.order_id, is_current ? "YES" : "NO (old)", side_name);
    Main().logger()->info("║ prev_cum={} | new_cum={} | INCREMENTAL=+{}", 
        prev_cumulative, fill.filled_quantity, incremental);
    Main().logger()->info("║ Symbol: {} | Price: {:.6f}", 
        std::string(fill.symbol.data()), fill.avg_fill_price);
    Main().logger()->info("║ >>> {} TOTAL: filled={} of {} | remaining={} | orders_tracked={}", 
        side_name, total_filled, orig_for_side, remaining, fills_map.size());
    
    // Clear current order ID if fully filled
    if (is_current && (is_complete || fill.filled_quantity >= fill.quantity)) {
        Main().logger()->info("║ >>> Current {} fully filled, clearing order_id", side_name);
        if (fill.side == Side::BUY) {
            bid_order_id_ = 0;
        } else {
            ask_order_id_ = 0;
        }
    }
    Main().logger()->info("╚═══════════════════════════════════════════════════════════╝");
    return incremental;
}

void MakeMarketStrategy::on_cancel(const OrderEventData& cancel) {
    BaseStrategy::on_cancel(cancel);

    Side side = Side::BUY;
    bool desk_cancel_ack = false;
    bool orphan_fill_cascade = false;
    OrderId orphan_fill_opposite_oid = 0;
    Side orphan_fill_opposite_side = Side::BUY;
    bool orphan_fill_no_opposite = false;
    std::int64_t cancel_elapsed_ms = 0;
    std::string leg_name;
    std::string oid_for_log;
    MmDeskCancelIntent desk_cancel_intent = MmDeskCancelIntent::None;
    {
        std::unique_lock<std::mutex> lk(tracked_orders_mutex_);
        TrackedLeg* t = findTrackedByOrderId(cancel.order_id, &side);
        if (!t) {
            // FREEZE FIX (2026-05-08): refreshLegacyTopOfBookTrackingFromVectors() acquires
            // tracked_orders_mutex_ internally — calling it while we still hold lk on a
            // non-recursive std::mutex self-deadlocks the thread. The whole process then
            // piles up behind this strategy's mutex (every onFeedUpdate / on_cancel / on_accept
            // for this strategy) and goes silent. Live sample (PID 972, 2026-05-08 14:16:39)
            // showed exactly this: main, both feed dispatchers, and aux REST poll all stuck
            // on __psynch_mutexwait at the lock_guard ctor, with no visible holder because
            // the holder was the same main thread inside the inlined re-lock.
            //
            // The 'cancel for an unknown order' path is the common case for events fan-out:
            // when OrderManager publishes ORDER_CANCELLED, every subscribed strategy receives
            // it; only the strategy that owns the local_oid finds a TrackedLeg. The rest hit
            // this branch.
            lk.unlock();
            refreshLegacyTopOfBookTrackingFromVectors();
            return;
        }
        oid_for_log = t->exchange_oid.empty() ? std::to_string(static_cast<long long>(t->local_oid)) : t->exchange_oid;
        if (mmIsDeskManaged() && mmLegStateIsCancelInFlight(t->state)) {
            desk_cancel_ack = true;
            cancel_elapsed_ms = mmSteadyMillis() - t->state_entered_steady_ms;
            leg_name = (side == Side::BUY) ? "bid" : "ask";
            desk_cancel_intent = t->pending_cancel_intent;
            t->pending_cancel_intent = MmDeskCancelIntent::None;
            t->desk_cancel_timeout_retry_sent = false;
            if (desk_cancel_intent == MmDeskCancelIntent::DeskReseed) {
                lk.unlock();
                eraseTrackedByOrderId(cancel.order_id);
            } else {
                t->state = MmLegState::CANCELLED;
                t->state_entered_steady_ms = mmSteadyMillis();
                t->local_oid = 0;
                t->exchange_oid.clear();
            }
        } else if (mmIsDeskManaged() && t->state == MmLegState::ADOPTED) {
            // Adopted desk leg cancelled WITHOUT a CANCEL_PENDING from us. Typical source:
            // OM_GATEWAY reconcile (StartupSequence.cpp "venue has no open order for local
            // order_id …") fires `om.onOrderCancelled` 9–15s after the OID disappeared from
            // GET /open-orders. The OID can disappear because it was either CANCELLED or
            // FILLED on the venue — OM cannot distinguish the two. If the exchange position
            // is non-zero in the direction consistent with this leg being filled, treat it
            // as an ORPHAN FILL: synthesize the post-fill cascade by marking this leg FILLED,
            // moving the opposite ADOPTED leg into CANCEL_PENDING + FillOpposite, and
            // REST-cancelling it. The cancel-ack will land on the CANCEL_PENDING branch above
            // and fire tryDeskFillRequoteF4PlaceFreshPair → fresh skewed pair.
            //
            // Without this branch the strategy silently drops the leg, leaves the opposite
            // working on the venue forever, and never requotes — exactly the failure the
            // user observed (BUY filled before adoption → 9s later OM marks it CANCELLED →
            // SELL sat at 1.1803 untouched, no new pair).
            const long long net_po_ll =
                static_cast<long long>(std::llround(position_state_.net_position_qty));
            const bool dir_match =
                (side == Side::BUY && net_po_ll > 0) || (side == Side::SELL && net_po_ll < 0);
            if (dir_match) {
                // Match processFillDeskStack: keep local_oid/exchange_oid so the FILLED leg
                // stays identifiable for layout/logging until tryDeskFillRequoteF4PlaceFreshPair
                // wipes both vectors and pushes the new PLACE_PENDING entries.
                t->state = MmLegState::FILLED;
                t->state_entered_steady_ms = mmSteadyMillis();
                t->remaining_qty = 0.0;
                orphan_fill_cascade = true;
                leg_name = (side == Side::BUY) ? "bid" : "ask";
                orphan_fill_opposite_side = (side == Side::BUY) ? Side::SELL : Side::BUY;
                auto& opp_vec = (orphan_fill_opposite_side == Side::BUY)
                                    ? tracked_bids_
                                    : tracked_asks_;
                bool found_opposite = false;
                for (auto& o : opp_vec) {
                    if (o.state == MmLegState::ADOPTED && o.local_oid != 0) {
                        o.state = MmLegState::CANCEL_PENDING;
                        o.pending_cancel_intent = MmDeskCancelIntent::FillOpposite;
                        o.desk_cancel_timeout_retry_sent = false;
                        o.state_entered_steady_ms = mmSteadyMillis();
                        orphan_fill_opposite_oid = o.local_oid;
                        found_opposite = true;
                        break;
                    }
                }
                if (!found_opposite) {
                    // Layout for tryDeskFillRequoteF4PlaceFreshPair requires (bid_filled &&
                    // ask_cancelled) OR (ask_filled && bid_cancelled). Inject a synthetic
                    // CANCELLED entry on the opposite side so the layout gate passes.
                    TrackedLeg synth;
                    synth.state = MmLegState::CANCELLED;
                    synth.side = orphan_fill_opposite_side;
                    synth.state_entered_steady_ms = mmSteadyMillis();
                    opp_vec.push_back(synth);
                    orphan_fill_no_opposite = true;
                }
            } else {
                lk.unlock();
                eraseTrackedByOrderId(cancel.order_id);
            }
        } else {
            lk.unlock();
            eraseTrackedByOrderId(cancel.order_id);
        }
    }
    ++quotes_cancelled_;
    if (orphan_fill_cascade) {
        if (utils::Logger::isInitialized()) {
            Main().logger()->warn(
                "[ORPHAN_FILL_DETECTED] strategy={} sym={} leg={} oid={} net_po={:.0f} "
                "no_opposite_alive={} — adopted leg cancelled externally with NetPo direction "
                "consistent with a fill that arrived BEFORE strategy adoption; synthesizing "
                "post-fill cascade (cancel opposite + requote fresh skewed pair)",
                getName(),
                mmAxSymbol(),
                leg_name,
                oid_for_log,
                position_state_.net_position_qty,
                orphan_fill_no_opposite ? 1 : 0);
        }
        if (orphan_fill_opposite_oid != 0) {
            // on_cancel runs on the OM dispatch thread; route the orphan-fill opposite-leg
            // cancel REST through the AX worker to keep all order ops single-threaded per AX.
            enqueueRestCancelOnMover(
                orphan_fill_opposite_oid,
                orphan_fill_opposite_side,
                "orphan_fill_cancel_opposite");
        } else {
            // No live opposite leg — go straight to fresh-pair-place. Layout was satisfied
            // by the synthetic CANCELLED entry pushed above. Routed through the per-AX mover
            // so the place doesn't run on the OM dispatch thread (which is also where
            // on_cancel itself is invoked).
            enqueueFillRequoteOnMover();
        }
    } else if (desk_cancel_ack) {
        // CANCEL-FIRST WINDOW STAMP (2026-07-06): a cancel-first pair-cancel just
        // completed. Record it so the CANCEL-FIRST RE-ADD WINDOW GUARD in
        // applyMmDeskSeededJsonLocked can suppress a DESK_RECOVERY/runFull reconcile
        // that would otherwise re-adopt the just-killed orders.json oids (404 storm +
        // venue orphans) before the cancel-first re-add places the fresh pair. This is
        // deliberately NOT last_cancel_response_ms_ so the reconcile gate is unchanged.
        mm_last_cancel_first_ms_.store(mmSteadyMillis(), std::memory_order_release);
        if (utils::Logger::isInitialized()) {
            Main().logger()->info(
                "[THEO_MOVE] cancel ack: leg={} oid={} elapsed_ms={}",
                leg_name,
                oid_for_log,
                latencyDisplayMs(cancel_elapsed_ms));
        }
        if (desk_cancel_intent == MmDeskCancelIntent::FireAndTrack) {
            // Fire-and-track: the replacement pair was already placed at cancel-send
            // time. This confirmation ONLY resolves the old tombstone (done above →
            // CANCELLED). NEVER enqueue a re-add — doing so would double the pair.
            if (utils::Logger::isInitialized()) {
                Main().logger()->info(
                    "[MM_TT] sym={} stack={} phase=cancel_confirmed leg={} src=ack ack_mode=pre "
                    "elapsed_ms={}",
                    mmAxSymbol(), mmOrderRequestIdOrLegacy(), leg_name,
                    latencyDisplayMs(cancel_elapsed_ms));
            }
        } else if (desk_cancel_intent == MmDeskCancelIntent::FillOpposite) {
            enqueueFillRequoteOnMover();
        } else if (desk_cancel_intent == MmDeskCancelIntent::TheoMove) {
            // === CLASSIC cancel -> confirm -> replace (place_before_cancel_ack=false) =========
            // This leg's cancel is now CONFIRMED by the venue ACK (the cancel POST's own
            // response, not a query, not the poller). Log it in the MM_TT stream with
            // ack_mode=post so the full cancel -> cancel_confirmed -> replace sequence is
            // traceable per leg and distinguishable from fire-and-track (ack_mode=pre).
            // The fresh pair is placed ONLY after BOTH legs' cancels are confirmed, so no
            // replacement order can ever go out while a stale leg is still live at the venue.
            if (utils::Logger::isInitialized()) {
                Main().logger()->info(
                    "[MM_TT] sym={} stack={} phase=cancel_confirmed leg={} src=ack ack_mode=post "
                    "elapsed_ms={}",
                    mmAxSymbol(), mmOrderRequestIdOrLegacy(), leg_name,
                    latencyDisplayMs(cancel_elapsed_ms));
            }
            if (mmDeskPairCancelSettledForTheoReplace()) {
                if (utils::Logger::isInitialized()) {
                    Main().logger()->info(
                        "[MM_TT] sym={} stack={} phase=replace_enqueued "
                        "reason=theo_move_after_cancel_ack ack_mode=post — both legs' cancels "
                        "confirmed, placing fresh pair",
                        mmAxSymbol(), mmOrderRequestIdOrLegacy());
                }
                enqueueQuoteCycleOnMover("theo_move");
            } else if (utils::Logger::isInitialized()) {
                Main().logger()->info(
                    "[MM_TT] sym={} stack={} phase=replace_deferred leg={} "
                    "reason=awaiting_opposite_cancel_ack ack_mode=post — one leg still "
                    "cancel-in-flight, holding re-add until the pair is flat",
                    mmAxSymbol(), mmOrderRequestIdOrLegacy(), leg_name);
            }
        } else if (desk_cancel_intent != MmDeskCancelIntent::DeskReseed) {
            enqueueDeskTheoMoveAfterCancelAckOnMover(side, leg_name);
        }
    }
    refreshLegacyTopOfBookTrackingFromVectors();
}

bool MakeMarketStrategy::mmRejectReasonShouldPullPairedLeg(const OrderEventData& reject) {
    // Primary, reliable signal: the structured gateway error code.
    if (reject.error_code == core::ErrorCode::INSUFFICIENT_MARGIN) {
        return true;
    }
    // Fallback: some venues only surface the cause in the free-text message
    // (e.g. "margin breach", "insufficient margin"). Case-insensitive substring.
    std::string msg = reject.error_message;
    std::transform(msg.begin(), msg.end(), msg.begin(),
                   [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    if (msg.find("margin") != std::string::npos) {
        return true;
    }
    // FUTURE catch-all: add additional reject reasons here (e.g. balance, risk
    // limits) and the on_reject paired-pull behaviour applies to them uniformly.
    return false;
}

void MakeMarketStrategy::on_reject(const OrderEventData& reject) {
    BaseStrategy::on_reject(reject);
    if (!reject.client_order_id.empty()) {
        std::lock_guard<std::mutex> lk(mm_accept_pending_mutex_);
        mm_pending_accept_cycle_reason_.erase(reject.client_order_id);
    }
    if (mm_pending_accepts_ > 0) {
        --mm_pending_accepts_;
        mm_pending_accepts_last_change_ms_.store(
            std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now().time_since_epoch())
                .count(),
            std::memory_order_relaxed);
    }
    mmNotifyProductPlacementGate(mmAxSymbol());

    {
        std::lock_guard<std::mutex> lk(tracked_orders_mutex_);
        Side side{};
        if (TrackedLeg* t = findTrackedByOrderId(reject.order_id, &side)) {
            if (mmLegStateIsCancelInFlight(t->state)) {
                // A rejected cancel means the order is still live on the venue — restore
                // truth to ADOPTED. For fire-and-track this (transiently) leaves the old
                // ADOPTED leg alongside the already-placed replacement; the reduce-only /
                // cap machinery pulls the grow side within one fills-poll interval.
                const MmDeskCancelIntent saved_cancel_intent = t->pending_cancel_intent;
                t->state = MmLegState::ADOPTED;
                if (saved_cancel_intent == MmDeskCancelIntent::DeskReseed) {
                    t->pending_cancel_intent = MmDeskCancelIntent::DeskReseed;
                }
            }
            t->pending_place_client_id.clear();
        }
        if (TrackedLeg* p = findTrackedByPendingClientId(reject.client_order_id, &side)) {
            p->state = MmLegState::CANCELLED;
            p->pending_place_client_id.clear();
        }
    }
    if (reject.order_id == bid_order_id_) bid_order_id_ = 0;
    if (reject.order_id == ask_order_id_) ask_order_id_ = 0;
    // Submit path may set last_* before accept; if reject arrives with id mismatch (race) or zero
    // tracked ids, still drop stale "current quote" for this side so feed logs don't lie.
    if (reject.side == Side::BUY) {
        last_bid_price_ = 0.0;
    } else if (reject.side == Side::SELL) {
        last_ask_price_ = 0.0;
    }
    
    logStrategyDecision("ORDER_REJECTED", 
        "order_id=" + std::to_string(reject.order_id) + 
        " reason=" + reject.error_message);

    // Paired pull: a margin-style rejection means we must not be left showing a
    // one-sided quote (the desk is paid to show both sides together). Pull the
    // opposite (paired) leg too. The reject handler runs on the OrderManager
    // event-dispatch thread, so the cancel REST is routed through the per-AX
    // mover via enqueueRestCancelOnMover (never block this thread, and reuse the
    // UAF-safe re-resolve-by-name path). mmRejectReasonShouldPullPairedLeg is the
    // single catch-all extension point for which reject reasons trigger this.
    if (mmRejectReasonShouldPullPairedLeg(reject)) {
        const Side opposite_side = (reject.side == Side::BUY) ? Side::SELL : Side::BUY;
        // Only the rejected side's id was zeroed above; the opposite id is intact.
        // Acquire-load the atomic leg ids (same accessors the rest of the class uses).
        const OrderId opposite_oid =
            (opposite_side == Side::BUY) ? mmCurrentBidOrderId() : mmCurrentAskOrderId();
        const char* rejected_side_str = (reject.side == Side::BUY) ? "BUY" : "SELL";
        const char* opposite_side_str = (opposite_side == Side::BUY) ? "BUY" : "SELL";
        if (opposite_oid != 0) {
            logStrategyDecision("PAIRED_PULL_ON_REJECT",
                "rejected_side=" + std::string(rejected_side_str) +
                " reason=" + reject.error_message +
                " pulling_opposite_side=" + std::string(opposite_side_str) +
                " opposite_oid=" + std::to_string(opposite_oid) +
                " (margin-style reject — never leave a one-sided quote)");
            enqueueRestCancelOnMover(opposite_oid, opposite_side, "paired_pull_on_margin_reject");
        } else {
            logStrategyDecision("PAIRED_PULL_ON_REJECT",
                "rejected_side=" + std::string(rejected_side_str) +
                " reason=" + reject.error_message +
                " no live opposite leg to pull (already absent)");
        }
    }
}

// =============================================================================
// Fill Processing & Hedge Logic
// =============================================================================

bool MakeMarketStrategy::mmRouteOmGatewayVenueTruthMissing(
    OrderId local_oid,
    const std::string& exchange_oid,
    const std::string& sym) {
    if (local_oid == 0) {
        return false;
    }

    // Step 1: snapshot the leg's state + price/qty/side under the tracked-orders
    // mutex so the cancel-vs-fill decision races neither against on_accept (which
    // can flip PLACE_PENDING→ADOPTED) nor against deskTheoMove* (which arms
    // CANCEL_PENDING). We deliberately drop the lock before invoking
    // processFillDeskStack — that function takes the same mutex internally and
    // re-resolves the leg via findTrackedByOrderId.
    Side leg_side{Side::BUY};
    core::Price leg_px{0.0};
    core::Quantity leg_qty{0.0};
    MmLegState leg_state{MmLegState::IDLE};
    bool found = false;
    {
        std::lock_guard<std::mutex> lk(tracked_orders_mutex_);
        Side s{};
        TrackedLeg* t = findTrackedByOrderId(local_oid, &s);
        if (t) {
            found = true;
            leg_state = t->state;
            leg_side = s;
            leg_px = t->exchange_px;
            leg_qty = t->qty;
        }
    }
    if (!found) {
        return false;
    }

    // Step 2: distinguish fill from deliberate cancel via MmLegState.
    //   ADOPTED        → fill (we believed it was working on the venue).
    //   CANCEL_PENDING → our own cancel REST completed; existing on_cancel
    //                    cancel-ack branch handles it when the caller invokes
    //                    om.onOrderCancelled immediately after this.
    //   PLACE_PENDING  → silent place failure; warn and skip (on_reject /
    //                    on_cancel will tidy up).
    //   FILLED / CANCELLED / IDLE / PAUSED → already settled; skip.
    if (leg_state == MmLegState::PLACE_PENDING) {
        if (utils::Logger::isInitialized()) {
            Main().logger()->warn(
                "[FILL_DESK_SKIP] sym={} oid={} exch_oid={} state=PLACE_PENDING — leg missing from "
                "venue while a place was in flight; treating as silent place failure (NOT routing "
                "to fill handler). Existing on_reject/on_cancel cleanup will reset the leg.",
                sym,
                static_cast<long long>(local_oid),
                exchange_oid);
        }
        return false;
    }
    if (leg_state != MmLegState::ADOPTED) {
        return false;
    }

    // Step 3: ADOPTED + missing-from-venue → full fill. Synthesize an
    // OrderEventData from the captured leg state and route to the existing
    // private fill handler. Venue-truth doesn't expose partial fills via this
    // path (we only see the disappearance), so we always treat it as a full
    // fill (filled_quantity == quantity). avg_fill_price falls back to the
    // last-placed exchange price.
    events::OrderEventData synth{};
    synth.order_id = local_oid;
    synth.symbol = core::makeSymbol(sym);
    synth.side = leg_side;
    synth.order_type = core::OrderType::LIMIT;
    synth.status = core::OrderStatus::FILLED;
    synth.price = leg_px;
    synth.quantity = leg_qty;
    synth.filled_quantity = leg_qty;
    synth.avg_fill_price = leg_px;
    synth.error_code = core::ErrorCode::SUCCESS;
    synth.order_timestamp = std::chrono::duration_cast<core::Timestamp>(
        std::chrono::system_clock::now().time_since_epoch());

    if (utils::Logger::isInitialized()) {
        Main().logger()->info(
            "[FILL_DESK] sym={} side={} px={} qty={} stack={} exch_oid={} "
            "routed_from=om_gateway_venue_truth",
            sym,
            leg_side == Side::BUY ? "BUY" : "SELL",
            leg_px,
            leg_qty,
            mmOrderRequestIdOrLegacy(),
            exchange_oid);
    }

    processFillDeskStack(synth, leg_qty);
    return true;
}

void MakeMarketStrategy::processFillDeskStack(const OrderEventData& fill, Quantity incremental_fill_qty) {
    const bool full_fill = (fill.quantity > 0.0 && fill.filled_quantity + 1e-12 >= fill.quantity);
    Side matched_side{};
    struct OppCancel {
        OrderId oid{0};
        Side side{Side::BUY};
    };
    std::vector<OppCancel> opposite_cancel_ids;
    std::string leg_name;
    std::string oid_for_fill_log;
    bool position_updated_locally = false;
    {
        std::lock_guard<std::mutex> lk(tracked_orders_mutex_);
        TrackedLeg* matched = findTrackedByOrderId(fill.order_id, &matched_side);
        if (!matched) {
            return;
        }
        leg_name = (matched_side == Side::BUY) ? "bid" : "ask";
        oid_for_fill_log = !matched->exchange_oid.empty()
            ? matched->exchange_oid
            : std::to_string(static_cast<long long>(fill.order_id));
        if (!full_fill) {
            matched->remaining_qty = std::max<Quantity>(0.0, matched->qty - fill.filled_quantity);
            matched->exchange_px = fill.price > 0.0 ? fill.price : matched->exchange_px;
        } else {
            matched->state = MmLegState::FILLED;
            matched->remaining_qty = 0.0;
            matched->exchange_px = fill.avg_fill_price > 0.0 ? fill.avg_fill_price
                : (fill.price > 0.0 ? fill.price : matched->exchange_px);
            const Side opposite = (matched_side == Side::BUY) ? Side::SELL : Side::BUY;
            auto& opp_vec = (opposite == Side::BUY) ? tracked_bids_ : tracked_asks_;
            for (auto& o : opp_vec) {
                if (o.state == MmLegState::ADOPTED && o.local_oid != 0) {
                    const std::string opp_oid =
                        o.exchange_oid.empty() ? std::to_string(static_cast<long long>(o.local_oid))
                                               : o.exchange_oid;
                    if (utils::Logger::isInitialized()) {
                        Main().logger()->info("[FILL_REQUOTE] cancelling opposite leg: oid={}", opp_oid);
                    }
                    o.state = MmLegState::CANCEL_PENDING;
                    o.pending_cancel_intent = MmDeskCancelIntent::FillOpposite;
                    o.desk_cancel_timeout_retry_sent = false;
                    o.state_entered_steady_ms = mmSteadyMillis();
                    opposite_cancel_ids.push_back(OppCancel{o.local_oid, opposite});
                } else if (mmLegStateIsCancelInFlight(o.state) || o.state == MmLegState::PLACE_PENDING) {
                    o.cancel_after_opposite_settled = true;
                }
            }
        }
    }
    // === SILVER max_position breach fix (2026-05-21) =========================
    // Update position_state_.net_position_qty for the desk-managed synthesized
    // fill. Without this, fills routed through `mmRouteOmGatewayVenueTruthMissing`
    // (i.e., venue-truth-missing detections, including the new
    // CANCEL_REVEALED_FILL path) never bump the strategy's local NetPo. The
    // next runFullMmQuoteCycle then reads a stale NetPo, the cap-gate passes,
    // and another order goes out. The synth fill effectively becomes a
    // "phantom" until the next REST sync (3s throttle) overwrites NetPo from
    // venue truth. That window is exactly what allowed Joe & Tim's
    // XAG / SILVER positions to drift to |NetPo|=max_position+1.
    //
    // last_fill_time_ms_ is also updated so syncInventoryFromExchangePortfolio
    // throttles its REST overwrite for ~2s (the same way it does for normal
    // processFill), preventing a still-in-flight portfolio refresh from
    // clobbering the freshly-applied local fill with a now-stale snapshot.
    const std::string ax_sym_synth = mmAxSymbol();
    const long long resting_before_synth = static_cast<long long>(std::llround(
        sumWorstCaseInflightLegQtyForSymbol(ax_sym_synth, matched_side, nullptr)));
    {
        std::lock_guard<std::mutex> ps_lk(position_state_mutex_);
        position_state_.onAxFill(
            matched_side,
            incremental_fill_qty,
            fill.avg_fill_price > 0.0 ? fill.avg_fill_price
                                      : (fill.price > 0.0 ? fill.price : 0.0));
        position_updated_locally = true;
    }
    // PositionBook: mirror the signed fill into the shared per-symbol book. Runs on the
    // main-loop fill-poll thread (ORDER_FILLED is publishSync). No-op in off mode.
    mmOnFillToBook(matched_side, incremental_fill_qty);
    last_fill_time_ms_.store(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now().time_since_epoch())
            .count(),
        std::memory_order_release);
    {
        auto& synth_cfg = config::Config::getInstance();
        const long long cap_synth = static_cast<long long>(std::max(1, mmEffMaxPositionInt(synth_cfg)));
        // ENFORCE reads book effective(); off/shadow read legacy net (identical value).
        const long long net_synth = mmEffectiveNetPoRounded();
        mmPullGrowSideOrdersAtOrPastCap(
            ax_sym_synth, net_synth, cap_synth, "desk_synth_fill", this);
    }
    ++mm_desk_synth_fills_count_;
    MakeMarketStrategy::syncExchangeNetForAllMakeMarketOnSymbol(ax_sym_synth, this, true);
    {
        auto& synth_cfg2 = config::Config::getInstance();
        const long long cap_synth2 = static_cast<long long>(std::max(1, mmEffMaxPositionInt(synth_cfg2)));
        const long long net_after = mmInstrumentNetPoFromPortfolioCache(ax_sym_synth);
        mmAssessTimCapAtFillInstant(
            ax_sym_synth,
            net_after,
            cap_synth2,
            resting_before_synth,
            incremental_fill_qty,
            "desk_synth_fill");
    }

    if (utils::Logger::isInitialized()) {
        Main().logger()->info(
            "[FILL] sym={} leg={} oid={} qty={} px={} new_position={} (desk_synth_update={})",
            mmAxSymbol(),
            leg_name,
            oid_for_fill_log,
            incremental_fill_qty,
            fill.avg_fill_price,
            position_state_.net_position_qty,
            position_updated_locally ? "Y" : "N");
    }
    if (!full_fill) {
        refreshLegacyTopOfBookTrackingFromVectors();
        return;
    }
    // processFill runs on the OM dispatch thread; route opposite-leg cancels via the AX worker
    // so the cancel REST + the post-fill requote (enqueued separately below via tryDeskFill...)
    // serialise sequentially on the same thread, regardless of which fired first.
    for (const auto& c : opposite_cancel_ids) {
        enqueueRestCancelOnMover(c.oid, c.side, "fill_requote_cancel_opposite");
    }
    refreshLegacyTopOfBookTrackingFromVectors();
}

void MakeMarketStrategy::processFill(const OrderEventData& fill, Quantity incremental_fill_qty) {
    std::string symbol_str(fill.symbol.data());
    Side matched_side{};
    {
        std::lock_guard<std::mutex> lk(tracked_orders_mutex_);
        if (!findTrackedByOrderId(fill.order_id, &matched_side)) {
            return;  // Phase 3: ignore fills for non-tracked orders
        }
    }
    const bool desk_stack = mmIsDeskManaged();
    const std::string ax_sym = mmAxSymbol();
    // Tim fill-instant bound: resting on the filled side *before* this fill lands in NetPo.
    const long long resting_before_fill = static_cast<long long>(std::llround(
        sumWorstCaseInflightLegQtyForSymbol(ax_sym, fill.side, nullptr)));

    // Step 1: Calculate notional value of the incremental AX fill
    double fill_notional = fill.avg_fill_price * incremental_fill_qty;

    // Step 2: Update net position tally (in notional USD)
    // CONCURRENCY (fix C4): position_state_mutex_ serialises the fill write against
    // shouldQuoteSide / runFullMmQuoteCycle on the mover.
    {
        std::lock_guard<std::mutex> ps_lk(position_state_mutex_);
        position_state_.onAxFill(fill.side, incremental_fill_qty, fill.avg_fill_price);
    }
    // PositionBook: mirror the signed fill into the shared per-symbol book. Runs on the
    // main-loop fill-poll thread (ORDER_FILLED is publishSync). No-op in off mode.
    mmOnFillToBook(fill.side, incremental_fill_qty);
    last_fill_time_ms_.store(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now().time_since_epoch())
            .count(),
        std::memory_order_release);
    // Tim: on every fill (partial or full) pull grow side when |net|>=max — immediate pull on
    // local net, then exchange sync + enforce below reconciles venue truth and re-pulls if needed.
    {
        auto& fill_cfg_early = config::Config::getInstance();
        const long long cap_after_fill = static_cast<long long>(
            std::max(1, mmEffMaxPositionInt(fill_cfg_early)));
        long long net_po_after_fill = 0;
        {
            std::lock_guard<std::mutex> ps_lk(position_state_mutex_);
            // ENFORCE reads book effective(); off/shadow read legacy net (identical value).
            net_po_after_fill = mmEffectiveNetPoRounded();
        }
        mmPullGrowSideOrdersAtOrPastCap(
            mmAxSymbol(), net_po_after_fill, cap_after_fill, "process_fill", this);
    }
    // One exchange position per AX symbol: every L1/L2/L3 stack must share the same product net for
    // shouldQuoteSide / max_position. Other stacks do not receive this fill's processFill — force REST
    // for all MakeMarket strategies on this symbol.
    //
    // CONCURRENCY (2026-05-20 audit): pass `this` so the helper skips locking this strategy's
    // own `mm_cycle_mutex_`. processFill runs on the OM event thread (`publishSync` chain from
    // `OrderManager::onOrderFilled`). The MmOrderMover worker concurrently holds peer strategies'
    // `mm_cycle_mutex_` while running `runFullMmQuoteCycle` (REST inside the lock, can block for
    // hundreds of ms). Without this self-skip we acquire our own cycle mutex too, blocking other
    // fill handlers and serializing OM dispatch behind a long mover cycle. Even though processFill
    // does NOT itself hold mm_cycle_mutex_ today, passing `this` is the documented contract of
    // `syncExchangeNetForAllMakeMarketOnSymbol(ax, cycle_mutex_held)` and keeps the lock graph
    // minimal — and protects against future callers that DO hold the cycle mutex.
    MakeMarketStrategy::syncExchangeNetForAllMakeMarketOnSymbol(ax_sym, this, true);
    {
        auto& fill_cfg_assess = config::Config::getInstance();
        const long long cap_assess = static_cast<long long>(
            std::max(1, mmEffMaxPositionInt(fill_cfg_assess)));
        const long long net_after_sync = mmInstrumentNetPoFromPortfolioCache(ax_sym);
        mmAssessTimCapAtFillInstant(
            ax_sym,
            net_after_sync,
            cap_assess,
            resting_before_fill,
            incremental_fill_qty,
            "process_fill");
    }

    if (!desk_stack) {
        // Calculate hedge product notional for logging
        double hedge_product_notional = last_hedge_price_ * hedge_multiplier_;
        double contracts_equivalent = position_state_.hedgeContractsNeeded(hedge_product_notional);

        logStrategyDecision("AX_FILL_PROCESSED",
            "symbol=" + symbol_str +
            " side=" + std::string(sideToString(fill.side)) +
            " qty=" + std::to_string(incremental_fill_qty) +
            " price=" + std::to_string(fill.avg_fill_price) +
            " fill_notional=" + std::to_string(fill_notional) +
            " net_position_usd=" + std::to_string(position_state_.net_position_usd) +
            " hedge_equiv=" + std::to_string(contracts_equivalent) + " contracts");

        // Log detailed position state
        if (Main().config()->getBool("logging.log_position_updates", true)) {
            Main().logger()->info("[STRATEGY:{}] === POSITION UPDATE ===", getName());
            Main().logger()->info(
                "  AX Fill: {} {} @ {} = ${:.2f}",
                sideToString(fill.side),
                incremental_fill_qty,
                fill.avg_fill_price,
                fill_notional);
            Main().logger()->info("  Net Position (USD): ${:.2f}", position_state_.net_position_usd);
            Main().logger()->info("  Hedge Product: {} @ {:.4f}", hedge_symbol_, last_hedge_price_);
            Main().logger()->info("  Hedge Notional: ${:.2f}", hedge_product_notional);
            Main().logger()->info("  Contracts Equivalent: {:.2f}", contracts_equivalent);
            Main().logger()->info("  Threshold: ±{:.2f} contracts", hedge_threshold_ratio_);

            // Log to positions.csv
            std::string pos_side = (position_state_.net_position_qty >= 0) ? "BUY" : "SELL";
            Main().logger()->log_position(
                symbol_str,
                pos_side,
                std::fabs(position_state_.net_position_qty),
                fill.avg_fill_price,  // entry_price (approx)
                fill.avg_fill_price,  // mark_price = last fill price
                0.0,                  // unrealized_pnl
                0.0                   // realized_pnl
            );

            // Log to pnl.csv
            Main().logger()->log_pnl(
                symbol_str,
                0.0,  // realized (tracked separately)
                0.0,  // unrealized
                0.0,  // total
                0.0   // fees
            );
        }

        // Log to CSV
        if (Main().config()->getBool("logging.log_hedge_signals", true)) {
            double hedge_product_notional = last_hedge_price_ * hedge_multiplier_;
            double contracts_equivalent = position_state_.hedgeContractsNeeded(hedge_product_notional);
            Main().logger()->log_event(
                "POSITION_UPDATE",
                symbol_str,
                "net_usd=" + std::to_string(position_state_.net_position_usd) +
                    " hedge_equiv=" + std::to_string(contracts_equivalent));
        }
    }

    // Step 3: Decide whether to hedge
    auto hedge_intent = decideHedge(fill);

    // Step 4: Execute hedge (simulated for now)
    if (hedge_intent) {
        pending_hedges_.push_back(*hedge_intent);
        executeHedge(*hedge_intent);
    }

    if (desk_stack && utils::Logger::isInitialized()) {
        Main().logger()->info(
            "[FILL_REQUOTE_DECIDE] sym={} reason='using_main_requote_flow_after_fill' new_position={}",
            mmAxSymbol(),
            position_state_.net_position_qty);
    }

    const bool full_fill = (fill.quantity > 0.0 && fill.filled_quantity + 1e-12 >= fill.quantity);
    (void)config::Config::getInstance().reloadPrimaryConfigFromDisk();
    auto& fill_cfg = config::Config::getInstance();
    const bool gate_mm = mmMarketMakerOrdersPlacementAllowed(fill_cfg);
    const bool gate_max_po =
        shouldQuoteSide(Side::BUY, true) || shouldQuoteSide(Side::SELL, true);
    const int max_reload_cfg = mmEffMaxReloadCyclesInt(fill_cfg);
    const bool gate_reload = !(max_reload_cfg > 0 && mm_reload_cycles_used_ >= max_reload_cfg);
    auto theo_opt = mmReadTransformedTheo();
    const bool gate_theo = theo_opt.has_value();
    const bool placing_new_pair =
        full_fill && gate_mm && gate_max_po && gate_reload && gate_theo;
    Main().logger()->info(
        "[STRATEGY:{}] processFill: side={} qty={} fill_complete={} gates: mm_enabled={} max_po_ok={} reload_ok={} theo_ok={} -> placing_new_pair={}",
        getName(),
        sideToString(fill.side),
        incremental_fill_qty,
        full_fill ? 1 : 0,
        gate_mm ? 1 : 0,
        gate_max_po ? 1 : 0,
        gate_reload ? 1 : 0,
        gate_theo ? 1 : 0,
        placing_new_pair ? "yes" : "no");

    if (!full_fill) {
        std::lock_guard<std::mutex> lk(tracked_orders_mutex_);
        Side matched_side{};
        TrackedLeg* matched = findTrackedByOrderId(fill.order_id, &matched_side);
        if (matched) {
            matched->remaining_qty = std::max<Quantity>(0.0, matched->qty - fill.filled_quantity);
            matched->exchange_px = fill.price > 0.0 ? fill.price : matched->exchange_px;
        }
    }
    if (!full_fill) {
        refreshLegacyTopOfBookTrackingFromVectors();
        return;
    }

    // Full fill: cancel all opposite-side tracked orders and remove filled order.
    struct OppCancel {
        OrderId oid{0};
        Side side{Side::BUY};
    };
    std::vector<OppCancel> opposite_cancel_ids;
    bool erase_filled = false;
    {
        std::lock_guard<std::mutex> lk(tracked_orders_mutex_);
        Side matched_side{};
        TrackedLeg* matched = findTrackedByOrderId(fill.order_id, &matched_side);
        if (!matched) {
            return;
        }
        const Side opposite = (matched_side == Side::BUY) ? Side::SELL : Side::BUY;
        auto& opp_vec = (opposite == Side::BUY) ? tracked_bids_ : tracked_asks_;
        for (auto& t : opp_vec) {
            if (t.local_oid == 0 || t.state != MmLegState::ADOPTED) {
                continue;
            }
            t.state = MmLegState::CANCEL_PENDING;
            // Stamp the cancel intent so on_cancel routes the ack via
            // enqueueFillRequoteOnMover (single AX-wide inflight guard dedupes).
            // Without this stamp the default intent (`None`)
            // sends the ack down enqueueDeskTheoMoveAfterCancelAckOnMover,
            // which places a single replacement leg AFTER the fresh pair —
            // i.e. an extra bid (or ask) on top of the new pair. See the
            // 2026-05-21 GOLD requote incident: ask fill at 4544.2 produced
            // bid 13 + ask 14 (correct pair) plus orphan bid 15.
            t.pending_cancel_intent = MmDeskCancelIntent::FillOpposite;
            t.desk_cancel_timeout_retry_sent = false;
            t.state_entered_steady_ms = mmSteadyMillis();
            opposite_cancel_ids.push_back({t.local_oid, opposite});
        }
        erase_filled = true;
    }
    if (erase_filled) {
        eraseTrackedByOrderId(fill.order_id);
    }
    // Same reasoning as the partial-fill path above: route REST cancels through the AX
    // worker so OM dispatch never blocks on REST.
    for (const auto& c : opposite_cancel_ids) {
        enqueueRestCancelOnMover(c.oid, c.side, "fill_full_cancel_opposite");
    }
    refreshLegacyTopOfBookTrackingFromVectors();

    if (!placing_new_pair) {
        Main().logger()->warn(
            "[STRATEGY:{}] fill gate blocked: mm={} max_po_ok={} reload_ok={} theo_ok={} used={} max={}",
            getName(), gate_mm ? 1 : 0, gate_max_po ? 1 : 0, gate_reload ? 1 : 0, gate_theo ? 1 : 0,
            mm_reload_cycles_used_, max_reload_cfg);
        return;
    }
    // Tim: wait for opposite-leg cancel ack before re-quote — do not race fill_requote place
    // ahead of REST cancel completion (duplicate reduce-side rows at cap).
    if (opposite_cancel_ids.empty()) {
        enqueueFillRequoteOnMover();
    }
    passive_until_first_manual_order_ = false;
    refreshLegacyTopOfBookTrackingFromVectors();
    return;
    // Legacy single-pair fill-requote flow removed in favor of tracked-order lifecycle.
}

std::optional<HedgeIntent> MakeMarketStrategy::decideHedge(const OrderEventData& fill) {
    /**
     * Hedge Decision Logic:
     * 
     * 1. Calculate hedge product notional: price × multiplier
     *    (Configure hedge.initial_price and hedge.multiplier for your hedge instrument.)
     * 
     * 2. Calculate contracts equivalent: net_position_usd / hedge_notional
     * 
     * 3. If |contracts_equivalent| > threshold_ratio (default 0.5):
     *    - Hedge by trading round(contracts_equivalent) contracts
     *    - SELL if contracts_equivalent > +0.5 (we're long, need to reduce)
     *    - BUY if contracts_equivalent < -0.5 (we're short, need to reduce)
     */
    
    if (!auto_hedge_enabled_) {
        logStrategyDecision("HEDGE_DECISION", "auto_hedge disabled - no hedge");
        return std::nullopt;
    }
    if (hedge_symbol_.empty()) {
        logStrategyDecision("HEDGE_DECISION", "hedge.symbol unset - no hedge");
        return std::nullopt;
    }
    
    // Calculate hedge product notional value
    double hedge_product_notional = last_hedge_price_ * hedge_multiplier_;
    if (hedge_product_notional <= 0) {
        logStrategyDecision("HEDGE_DECISION", "invalid hedge notional - no hedge");
        return std::nullopt;
    }
    
    // Calculate how many hedge contracts our exposure represents
    double contracts_equivalent = position_state_.net_position_usd / hedge_product_notional;
    
    Main().logger()->debug("[STRATEGY:{}] Hedge decision: net_usd=${:.2f} / hedge_notional=${:.2f} = {:.2f} contracts",
        getName(), position_state_.net_position_usd, hedge_product_notional, contracts_equivalent);
    
    // Check if we exceed the threshold
    if (std::fabs(contracts_equivalent) > hedge_threshold_ratio_) {
        // We need to hedge!
        int contracts_to_hedge = static_cast<int>(std::round(contracts_equivalent));
        
        if (contracts_to_hedge == 0) {
            logStrategyDecision("HEDGE_DECISION", 
                "contracts_equiv=" + std::to_string(contracts_equivalent) + 
                " rounds to 0 - no hedge");
            return std::nullopt;
        }
        
        HedgeIntent intent;
        intent.symbol = hedge_symbol_;
        // If contracts_equivalent > 0 (we're long), SELL to reduce exposure
        // If contracts_equivalent < 0 (we're short), BUY to reduce exposure
        intent.side = (contracts_to_hedge > 0) ? Side::SELL : Side::BUY;
        intent.contracts = std::abs(contracts_to_hedge);
        intent.reference_price = last_hedge_price_;
        intent.triggering_fill_id = fill.order_id;
        intent.created_at = std::chrono::steady_clock::now();
        intent.executed = false;
        
        logStrategyDecision("HEDGE_TRIGGERED",
            "net_usd=" + std::to_string(position_state_.net_position_usd) +
            " contracts_equiv=" + std::to_string(contracts_equivalent) +
            " threshold=" + std::to_string(hedge_threshold_ratio_) +
            " → " + intent.toString());
        
        ++hedges_triggered_;
        return intent;
    }
    
    logStrategyDecision("HEDGE_DECISION",
        "contracts_equiv=" + std::to_string(contracts_equivalent) +
        " within threshold ±" + std::to_string(hedge_threshold_ratio_) + " - no hedge");
    
    return std::nullopt;
}

void MakeMarketStrategy::executeHedge(const HedgeIntent& hedge) {
    /**
     * SIMULATED HEDGE EXECUTION
     * 
     * For this demo version:
     * 1. Simulate a market order fill at current price (with small slippage)
     * 2. Log the trade as if it were real
     * 3. Update net position to include the hedge
     * 4. All downstream logic treats this as a real trade
     * 
     * Future implementation would connect to CME or another hedge venue and send real orders.
     */
    
    // Simulate fill price with small slippage (1 tick)
    double tick = 0.0001;  // Standard FX tick
    Price simulated_fill_price = hedge.reference_price;
    if (hedge.side == Side::BUY) {
        simulated_fill_price += tick;  // Buy at slight premium
    } else {
        simulated_fill_price -= tick;  // Sell at slight discount
    }
    
    // Calculate notional value of hedge trade
    double hedge_notional = hedge.contracts * simulated_fill_price * hedge_multiplier_;
    
    Main().logger()->info("[STRATEGY:{}] ╔═══════════════════════════════════════════════════╗", getName());
    Main().logger()->info("[STRATEGY:{}] ║           SIMULATED HEDGE TRADE                   ║", getName());
    Main().logger()->info("[STRATEGY:{}] ╚═══════════════════════════════════════════════════╝", getName());
    Main().logger()->info("[STRATEGY:{}]   Symbol: {}", getName(), hedge.symbol);
    Main().logger()->info("[STRATEGY:{}]   Side: {}", getName(), sideToString(hedge.side));
    Main().logger()->info("[STRATEGY:{}]   Contracts: {}", getName(), hedge.contracts);
    Main().logger()->info("[STRATEGY:{}]   Fill Price: {:.5f}", getName(), simulated_fill_price);
    Main().logger()->info("[STRATEGY:{}]   Notional Value: ${:.2f}", getName(), hedge_notional);
    Main().logger()->info("[STRATEGY:{}]   [SIMULATED - Would execute on {} in production]", getName(), hedge.symbol);
    
    // Record the hedge trade
    HedgeTrade trade;
    trade.symbol = hedge.symbol;
    trade.side = hedge.side;
    trade.contracts = hedge.contracts;
    trade.fill_price = simulated_fill_price;
    trade.notional_value = hedge_notional;
    trade.timestamp = std::chrono::steady_clock::now();
    trade.simulated = true;
    
    executed_hedges_.push_back(trade);
    
    // Update net position to include hedge
    // SELL reduces long exposure (negative notional), BUY reduces short exposure (positive notional)
    position_state_.onHedgeTrade(hedge.side, hedge.contracts, simulated_fill_price, hedge_multiplier_);
    
    // Log position after hedge
    double hedge_product_notional = last_hedge_price_ * hedge_multiplier_;
    double new_contracts_equiv = position_state_.hedgeContractsNeeded(hedge_product_notional);
    
    Main().logger()->info("[STRATEGY:{}]   ─────────────────────────────────────────────────", getName());
    Main().logger()->info("[STRATEGY:{}]   Position After Hedge:", getName());
    Main().logger()->info("[STRATEGY:{}]     Net Position (USD): ${:.2f}", getName(), position_state_.net_position_usd);
    Main().logger()->info("[STRATEGY:{}]     Contracts Equivalent: {:.2f}", getName(), new_contracts_equiv);
    Main().logger()->info("[STRATEGY:{}]   ─────────────────────────────────────────────────", getName());
    
    // Log to CSV for audit
    Main().logger()->log_event("HEDGE_EXECUTED", hedge.symbol,
        "side=" + std::string(sideToString(hedge.side)) +
        " contracts=" + std::to_string(hedge.contracts) +
        " fill_price=" + std::to_string(simulated_fill_price) +
        " notional=" + std::to_string(hedge_notional) +
        " new_net_usd=" + std::to_string(position_state_.net_position_usd) +
        "         simulated=true");
}

// =============================================================================
// Quote Update Logic
// =============================================================================

// Pricing mid = adjusted_theo (= newMidpoint + basis, where newMidpoint comes from the desk
// pricer-snapshot formula or raw theo when no anchors). Tick = market_maker.price_tick (AX).
// NetPo = long + / short −.
//
// Width semantics (desk spec 2026-05-12): bid_width_bps / ask_width_bps are PER-SIDE widths in
// basis points of pricing mid (1 bp = 0.0001 of mid). bid_offset = pricing_mid * bid_w_bps / 10000.
// When |NetPo| < max_position:
//   skew_tick_units = floor(|NetPo|/adjust_position) * sign(NetPo) * adjust_ticks
//   skew_price = skew_tick_units * Tick.
//   BidPx   = newMidpoint - newMidpoint*(bid_width_bps/10000) - skew_price
//   OfferPx = newMidpoint + newMidpoint*(ask_width_bps/10000) - skew_price
// Same skew subtracted from both; negative NetPo → skew < 0 → both prices rise; positive NetPo → both fall.
// If bid_w = ask_w = W: OfferPx − BidPx = 2 * newMidpoint * W / 10000 (i.e. total spread = 2*W bps)
// before tick-grid finalize (finalize anchors bid, sets ask = bid + span).
// When |NetPo| >= max_position: skew is 0 here; side gating uses shouldQuoteSide; leg qty is order_size (step).
// MM_SKEW_FORMULA (symmetric, 2026-05-21):
//   skew_tick_units = floor(|NetPo|/adjust_position) * sign(NetPo) * adjust_ticks  (when |NetPo| < max_position)
//   skew_price = skew_tick_units * quote_tick; same skew subtracted from bid and raw_ask chain.
void MakeMarketStrategy::computeInventorySkewedRawBidAsk(
    Price adjusted_theo, int bid_spread_bps, int ask_spread_bps, double quote_tick,
    Price& raw_bid, Price& raw_ask) const {

    auto& config = config::Config::getInstance();
    const int adjust_po = std::max(1, mmEffAdjustPositionInt(config));
    const int adjust_ticks = mmEffAdjustTicksInt(config);
    const int max_po = mmEffMaxPositionInt(config);

    // CONCURRENCY (fix C4): stable snapshot under position_state_mutex_.
    long long net_po = 0;
    {
        std::lock_guard<std::mutex> ps_lk(position_state_mutex_);
        // ENFORCE reads book effective(); off/shadow read legacy net (identical value).
        net_po = mmEffectiveNetPoRounded();
    }

    // Inventory skew (desk spec, symmetric long/short — 2026-05-21):
    //     skew_tick_units = floor(|NetPo| / adjust_position) * sign(NetPo) * adjust_ticks
    //     OfferPx = TheoMidpoint + half_spread - skew_price
    //     BidPx   = TheoMidpoint - half_spread - skew_price
    // Long (NetPo > 0)  → skew_price > 0 → both prices drop:
    //     • bid moves further from market → harder to fill (we don't want more longs)
    //     • ask moves closer to market    → easier to fill (we want to flatten)
    // Short (NetPo < 0) → skew_price < 0 → both prices rise (symmetric).
    //
    // NOTE — Symmetry fix: previously we used `floor(NetPo / adjust_position)` directly, which
    // (because std::floor rounds toward −∞) made any short inventory below `adjust_position`
    // jump straight to `-adjust_ticks` while the equivalent long position stayed at zero. e.g.
    // NetPo=-1, adjust_po=50000 → floor(-1/50000)=-1, skew_tick_units=-adjust_ticks; NetPo=+1
    // gives 0. Taking floor on |NetPo| and re-applying sign(NetPo) makes the two sides identical.
    //
    // CAP-SKEW POLICY (2026-05-21, aligned across C++ placement / Python desk preview /
    // MM_VERIFY / spec): when |NetPo| >= max_position, skew is **zero** here. At the cap,
    // `shouldQuoteSide` blocks the add side (BUY if NetPo>=+cap, SELL if NetPo<=-cap), so the
    // remaining (reduce) side rests at `mid ± half_spread` and any further inventory pressure
    // is handled by side gating + headroom-capped qty rather than by skewing the resting price.
    // Keeping skew on past the cap silently diverged from the verify printer and the Python
    // desk-side preview math, leaving operators looking at one set of numbers while C++ placed
    // at another — that is exactly the kind of money-leaking surprise we are eliminating.
    const long long abs_net_po = (net_po < 0) ? -net_po : net_po;
    const long long cap_ll = static_cast<long long>(std::max(1, max_po));
    const int net_po_sign = (net_po > 0) ? 1 : (net_po < 0 ? -1 : 0);
    double skew_tick_units = 0.0;
    if (abs_net_po < cap_ll) {
        skew_tick_units =
            std::floor(static_cast<double>(abs_net_po) / static_cast<double>(adjust_po)) *
            static_cast<double>(net_po_sign) *
            static_cast<double>(adjust_ticks);
    }

    const Price skew_price = skew_tick_units * quote_tick;
    // bps semantics (desk spec 2026-05-12): each side gets the FULL configured bps width.
    //     bid_offset_px = adjusted_theo * bid_spread_bps / 10000
    //     ask_offset_px = adjusted_theo * ask_spread_bps / 10000
    // With bid_w = ask_w = 20, bid sits at mid − mid*0.002 and ask at mid + mid*0.002 (total
    // spread = 40 bps of mid). This replaces the older "bps is total span / 2" convention so
    // the bps number on the desk UI matches the per-side price offset the operator expects.
    // Identical math for every instrument — no symbol-specific branching.
    const double bid_offset = adjusted_theo * (static_cast<double>(bid_spread_bps) / 10000.0);
    const double ask_offset = adjusted_theo * (static_cast<double>(ask_spread_bps) / 10000.0);
    raw_bid = adjusted_theo - bid_offset - skew_price;
    raw_ask = adjusted_theo + ask_offset - skew_price;

    // Throttled audit log — once per ~1.5s per strategy. Prints every input the formula uses so
    // the user can verify the skew is actually being computed (and pinpoint configs that
    // accidentally guarantee zero skew, e.g. adjust_position >> max_position).
    if (utils::Logger::isInitialized()) {
        static std::mutex s_skew_log_mu;
        static std::unordered_map<std::string, std::int64_t> s_skew_log_last_ms;
        const std::int64_t now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                        std::chrono::steady_clock::now().time_since_epoch())
                                        .count();
        bool emit = false;
        {
            std::lock_guard<std::mutex> lk(s_skew_log_mu);
            auto& slot = s_skew_log_last_ms[getName()];
            if (slot == 0 || now_ms - slot >= 1500) {
                slot = now_ms;
                emit = true;
            }
        }
        if (emit) {
            const bool config_skew_dead =
                (adjust_ticks <= 0) || (adjust_po >= max_po && max_po > 0 && adjust_ticks > 0);
            // Surface the pricer-snapshot transform inputs so the operator can verify the
            // newMidpoint = qs + qs*slope*((theo-ps)/ps) actually fired (qs/ps>0) or was
            // skipped (qs<=0 or ps<=0 → identity transform on theo).
            const double qs_dbg = mmEffQuoteSnapshot();
            const double ps_dbg = mmEffPricerSnapshot();
            const double sl_dbg = mmEffSlope();
            const bool xform_active = (qs_dbg > 0.0) && (ps_dbg > 0.0);
            Main().logger()->info(
                "[MM_SKEW] strategy={} sym={} net_po={} max_po={} adjust_position={} adjust_ticks={} "
                "quote_tick={:.6f} bid_w_bps={} ask_w_bps={} | "
                "skew_tick_units={:.3f} skew_price={:.6f} bid_offset={:.6f} ask_offset={:.6f} | "
                "adjusted_theo={:.6f} raw_bid={:.6f} raw_ask={:.6f} | "
                "pricer_xform={} qs={:.6f} ps={:.6f} slope={:.6f}{}",
                getName(),
                mmAxSymbol(),
                net_po,
                max_po,
                adjust_po,
                adjust_ticks,
                quote_tick,
                bid_spread_bps,
                ask_spread_bps,
                skew_tick_units,
                skew_price,
                bid_offset,
                ask_offset,
                adjusted_theo,
                raw_bid,
                raw_ask,
                xform_active ? "ON" : "OFF",
                qs_dbg,
                ps_dbg,
                sl_dbg,
                config_skew_dead
                    ? " WARN_SKEW_ALWAYS_ZERO=adjust_position>=max_position OR adjust_ticks<=0 — change orders.json"
                    : "");
        }
    }

#ifndef NDEBUG
    // 2*W invariant (desk spec 2026-05-12): when bid_w == ask_w == W, the placed pair
    // MUST be exactly 2 * adjusted_theo * W / 10000 apart BEFORE tick rounding (skew is
    // common-mode and cancels in the span). Catches any future regression that
    // re-introduces a half-spread divisor or skew that fails to cancel.
    if (bid_spread_bps == ask_spread_bps) {
        const double expected_span = 2.0 * adjusted_theo *
            static_cast<double>(bid_spread_bps) / 10000.0;
        const double actual_span = raw_ask - raw_bid;
        assert(std::abs(actual_span - expected_span) < 1e-9 &&
               "2*W invariant violated in symmetric-width path");
    }
#endif
}

void MakeMarketStrategy::computeInventorySkewedRawBidAsk(
    Price adjusted_theo, int spread_bps, double quote_tick,
    Price& raw_bid, Price& raw_ask) const {
    computeInventorySkewedRawBidAsk(adjusted_theo, spread_bps, spread_bps, quote_tick, raw_bid, raw_ask);
}

void MakeMarketStrategy::printMmVerifyStdout(
    const std::string& reason,
    Price theo_midpoint,
    Price adjusted_theo,
    int bid_width_bps,
    int ask_width_bps,
    double quote_tick,
    long long net_po_rounded,
    int max_po,
    int adjust_po,
    int adjust_ticks,
    double skew_tick_units,
    Price skew_price,
    Price raw_bid,
    Price raw_ask,
    Price bid_px,
    Price ask_px,
    Quantity qty_bid,
    Quantity qty_ask,
    bool want_bid,
    bool want_ask,
    bool placing_bid,
    bool placing_ask) const {
    auto& cfg = config::Config::getInstance();
    const std::string ext_name = cfg.getString("external_feed.name", "external");
    const std::string ext_prov = cfg.getExternalFeedProvider();
    const std::string prov_for_print = !mmTheoSource().empty() ? mmTheoSource() : ext_prov;

    // BPS-NATIVE VERIFIER (instrument-agnostic, desk spec 2026-05-12):
    // The actual pricing math in computeInventorySkewedRawBidAsk treats bid_w / ask_w as PER-SIDE
    // widths in basis points of pricing mid (1 bp = 0.0001 of adjusted_theo). Each side gets the
    // full bps offset — bid_offset_px = mid * bid_w / 10000, ask_offset_px = mid * ask_w / 10000.
    // Total spread = bid_offset + ask_offset = mid * (bid_w + ask_w) / 10000 bps.
    const int total_bps_both_sides = bid_width_bps + ask_width_bps;
    const double bid_offset_expected = adjusted_theo * (static_cast<double>(bid_width_bps) / 10000.0);
    const double ask_offset_expected = adjusted_theo * (static_cast<double>(ask_width_bps) / 10000.0);
    const double expected_raw_span_px = bid_offset_expected + ask_offset_expected;
    const double raw_span = raw_ask - raw_bid;
    const double placed_span_px =
        (std::isfinite(bid_px) && std::isfinite(ask_px)) ? (ask_px - bid_px) : 0.0;
    const double placed_span_ticks =
        (quote_tick > 0.0) ? (placed_span_px / quote_tick) : 0.0;

    std::cout << std::fixed << std::setprecision(6);
    std::cout << "\n========== MM_VERIFY [" << reason << "] ==========\n";
    std::optional<marketdata::ExternalFeedQuote> ext_q;
    {
        const std::string tsym = mmTheoSymbol();
        auto& efm = marketdata::ExternalFeedManager::getInstance();
        if (!tsym.empty()) {
            ext_q = efm.getQuoteForTheoSymbol(tsym);
        } else {
            ext_q = efm.getLastQuote();
        }
    }
    if (ext_q && ext_q->valid) {
        std::cout << "  [external " << ext_name;
        if (!prov_for_print.empty()) {
            std::cout << " / " << prov_for_print;
        }
        std::cout << "] TOB @ MM_VERIFY print (theo_symbol=" << mmTheoSymbol() << "): bid=" << ext_q->bid
                  << "  ask=" << ext_q->ask << "  mid=" << ext_q->mid << "\n";
    } else {
        std::cout << "  [external " << ext_name << "] (no valid cached bid/ask for this leg's theo_symbol yet)\n";
    }
    // Decompose adjusted_theo so the operator can see at a glance whether any delta vs theo_mid
    // came from the pricer-snapshot transform (qs/ps) or from an explicit basis offset. Previously
    // we labelled this line `adjusted_theo (+basis)`, which read as "the delta is basis" even when
    // basis was 0 and the entire shift came from the pricer-snapshot transform.
    const double xform_qs = mmEffQuoteSnapshot();
    const double xform_ps = mmEffPricerSnapshot();
    const double xform_slope = mmEffSlope();
    const bool xform_on = (xform_qs > 0.0) && (xform_ps > 0.0);
    const double new_midpoint = mmApplyPricerSnapshotTransform(theo_midpoint);
    const double basis_component = adjusted_theo - new_midpoint;
    const double xform_delta = new_midpoint - theo_midpoint;
    std::cout << "  theo_mid (this quote cycle)=" << theo_midpoint << "\n";
    std::cout << "  pricer-snapshot xform=" << (xform_on ? "ON" : "OFF")
              << "  qs=" << xform_qs << "  ps=" << xform_ps << "  slope=" << xform_slope
              << "  →  newMidpoint=" << new_midpoint
              << "  (xform_delta vs theo_mid=" << xform_delta << ")\n";
    std::cout << "  basis=" << basis_component
              << "  adjusted_theo = newMidpoint + basis = " << adjusted_theo << "\n";
    if (xform_on && std::fabs(xform_delta) > quote_tick * 0.5) {
        std::cout << "  NOTE: pricer-snapshot transform is shifting theo by " << xform_delta
                  << " (qs≠ps in orders.json). Set quote_snapshot==pricer_snapshot (or both <=0)"
                     " to disable.\n";
    }
    if (ext_q && ext_q->valid &&
        std::fabs(ext_q->mid - theo_midpoint) > quote_tick * 0.25) {
        std::cout << "  (cached TOB mid can trail cycle theo if FIX updated between timer and this line)\n";
    }
    std::cout << "  bid_width_bps=" << bid_width_bps
              << "  ask_width_bps=" << ask_width_bps
              << "  quote_tick=" << quote_tick << "\n";
    std::cout << "  NetPo~=" << net_po_rounded
              << "  max_po=" << max_po
              << "  adjust_po=" << adjust_po
              << "  adjust_ticks(config)=" << adjust_ticks << " (multiplier on skew floor)\n";
    std::cout << "  skew_tick_units = floor(|NetPo|/adjust_po)*sign(NetPo)*adjust_ticks"
                 " when |NetPo|<max_position, else 0: " << skew_tick_units << "\n";
    std::cout << "  skew_price = skew_tick_units * quote_tick = " << skew_price << "\n";
    std::cout << "[MM_FORMULA] pricing_mid = newMidpoint + basis (basis=0 => pricing_mid = newMidpoint)\n";
    std::cout << "[MM_FORMULA] newMidpoint = quote_snapshot + quote_snapshot * slope * "
                 "((theoMidpoint - pricer_snapshot) / pricer_snapshot)\n";
    std::cout << "[MM_FORMULA] side_offset = pricing_mid * bps / 10000  (1 bp = 0.0001 of mid; per side, full bps)\n";
    std::cout << "[MM_FORMULA] bid_offset = " << adjusted_theo << " * " << bid_width_bps << " / 10000 = "
              << bid_offset_expected << "    ask_offset = " << adjusted_theo << " * " << ask_width_bps << " / 10000 = "
              << ask_offset_expected << "\n";
    std::cout << "[MM_FORMULA] total_bps_both_sides = bid_w_bps + ask_w_bps = " << bid_width_bps << " + "
              << ask_width_bps << " = " << total_bps_both_sides
              << "    expected_raw_span_px = bid_offset + ask_offset = " << expected_raw_span_px << "\n";
    std::cout << "[MM_FORMULA] raw_bid = pricing_mid - bid_offset - skew ; raw_ask = pricing_mid + ask_offset - skew\n";
    std::cout << "[MM_FORMULA] raw_bid=" << raw_bid << "  raw_ask=" << raw_ask << "  raw_span_px=" << raw_span
              << "  (raw_ask shown for reference; the PLACED ask is derived from the bid below)\n";
    // ---- Ask derivation (Tim spec 2026-06-12): one leg priced normally, then add DOUBLE the
    // typed-bps offset, rounded ALWAYS DOWN to whole ticks. Logged step-by-step so it can be
    // vetted directly against the worked example in the 09-June feature request.
    const double full_width_offset = 2.0 * bid_offset_expected;  // 2 * bid_offset (typed bps doubled)
    const int full_width_ticks = (quote_tick > 0.0)
        ? std::max(1, static_cast<int>(full_width_offset / quote_tick))
        : 1;
    std::cout << "[MM_FORMULA] STEP 1 bid leg (unchanged): placed_bid = roundBidDown(raw_bid, tick) = roundBidDown("
              << raw_bid << ", " << quote_tick << ") = " << bid_px << "\n";
    std::cout << "[MM_FORMULA] STEP 2 full width = DOUBLE the typed-bps offset (NOT bid+ask): "
                 "full_width_offset = 2 * bid_offset = 2 * " << bid_offset_expected << " = " << full_width_offset
              << "\n";
    std::cout << "[MM_FORMULA] STEP 3 round width ALWAYS DOWN to whole ticks: "
                 "full_width_ticks = floor(full_width_offset / tick) = floor(" << full_width_offset << " / "
              << quote_tick << ") = " << full_width_ticks << "\n";
    std::cout << "[MM_FORMULA] STEP 4 ask = placed_bid + full_width_ticks * tick = " << bid_px << " + "
              << full_width_ticks << " * " << quote_tick << " = " << ask_px << "\n";
    std::cout << "[MM_FORMULA] placed bid_px=" << bid_px << "  ask_px=" << ask_px
              << "  placed_span_px=" << placed_span_px << "  placed_span_ticks=" << placed_span_ticks << "\n";
    std::cout << "[MM_FORMULA] qty_bid=" << qty_bid << "  qty_ask=" << qty_ask
              << "  (order_size step-rounded; max_position via side gating)\n";
    std::cout << "  want_bid=" << (want_bid ? "Y" : "N")
              << "  want_ask=" << (want_ask ? "Y" : "N") << "\n";
    if (placing_bid) {
        std::cout << "  -> ORDER BUY  price=" << bid_px << "  size=" << qty_bid << "\n";
    }
    if (placing_ask) {
        std::cout << "  -> ORDER SELL price=" << ask_px << "  size=" << qty_ask << "\n";
    }
    if (!placing_bid && !placing_ask) {
        std::cout << "  -> no orders placed (side pull or skip)\n";
    }
    std::cout << "==========================================\n";
    std::cout << std::defaultfloat << std::setprecision(6) << std::flush;
}

namespace {

constexpr const char kDeskAdoptClientPrefix[] = "desk-adopt-";

bool mmDeskAdoptClientOrderId(const std::string& client_order_id) {
    return client_order_id.rfind(kDeskAdoptClientPrefix, 0) == 0;
}

/** Venue OID embedded in `desk-adopt-<exchange_oid>` client ids from OrderManager::adoptExternalLimitOrder. */
std::string mmExchangeOidFromDeskAdoptClientId(const std::string& client_order_id) {
    if (!mmDeskAdoptClientOrderId(client_order_id)) {
        return {};
    }
    return client_order_id.substr(sizeof(kDeskAdoptClientPrefix) - 1);
}

std::string mmResolveExchangeOidForOrderEvent(const events::OrderEventData& data) {
    const std::string from_desk = mmExchangeOidFromDeskAdoptClientId(data.client_order_id);
    if (!from_desk.empty()) {
        return from_desk;
    }
    auto op = orders::OrderManager::getInstance().getOrder(data.order_id);
    if (op && !op->exchange_order_id.empty()) {
        return op->exchange_order_id;
    }
    return {};
}

}  // namespace

bool MakeMarketStrategy::orderEventMatchesStrategy(const events::OrderEventData& data,
                                                   events::EventType type) const {
    (void)type;

    // Ownership routing: never match by AX symbol alone (multi-stack mm_req_* share one symbol).
    // When clOrdID is present it wins over open_orders_ / local order_id (shared OM counter).
    if (!data.client_order_id.empty()) {
        if (clientOrderIdBelongsToThisStrategy(data.client_order_id)) {
            return true;
        }
        if (mmDeskAdoptClientOrderId(data.client_order_id)) {
            const std::string ex = mmExchangeOidFromDeskAdoptClientId(data.client_order_id);
            std::lock_guard<std::mutex> lk(tracked_orders_mutex_);
            for (const auto& t : tracked_bids_) {
                if (!t.exchange_oid.empty() && t.exchange_oid == ex) {
                    return true;
                }
            }
            for (const auto& t : tracked_asks_) {
                if (!t.exchange_oid.empty() && t.exchange_oid == ex) {
                    return true;
                }
            }
        }
        return false;
    }

    if (BaseStrategy::orderEventMatchesStrategy(data, type)) {
        return true;
    }

    const std::string exchange_oid = mmResolveExchangeOidForOrderEvent(data);
    if (!exchange_oid.empty()) {
        std::lock_guard<std::mutex> lk(tracked_orders_mutex_);
        for (const auto& t : tracked_bids_) {
            if (!t.exchange_oid.empty() && t.exchange_oid == exchange_oid) {
                return true;
            }
        }
        for (const auto& t : tracked_asks_) {
            if (!t.exchange_oid.empty() && t.exchange_oid == exchange_oid) {
                return true;
            }
        }
    }

    if (data.order_id == bid_order_id_.load(std::memory_order_acquire) ||
        data.order_id == ask_order_id_.load(std::memory_order_acquire)) {
        return true;
    }
    std::lock_guard<std::mutex> lk(tracked_orders_mutex_);
    for (const auto& t : tracked_bids_) {
        if (t.local_oid == data.order_id) {
            return true;
        }
    }
    for (const auto& t : tracked_asks_) {
        if (t.local_oid == data.order_id) {
            return true;
        }
    }
    return false;
}

bool MakeMarketStrategy::shouldQuoteSide(Side side, bool bypass_reload_limit) const {
    auto& cfg = config::Config::getInstance();
    const int max_po = mmEffMaxPositionInt(cfg);
    // CONCURRENCY (fix C4): stable snapshot under position_state_mutex_; this is the
    // cap-gate decision that historically allowed Joe's EURUSD 7×100 overshoot when
    // a concurrent fill arrived between the read and the place.
    long long net_ll = 0;
    {
        std::lock_guard<std::mutex> ps_lk(position_state_mutex_);
        // ENFORCE reads book effective(); off/shadow read legacy net (identical value).
        net_ll = mmEffectiveNetPoRounded();
    }
    const long long max_ll = static_cast<long long>(std::max(1, max_po));
    const int max_reload = mmEffMaxReloadCyclesInt(cfg);
    const bool reload_limit_hit =
        !bypass_reload_limit && (max_reload > 0 && mm_reload_cycles_used_ >= max_reload);

    // Reload cap reached: stop grow-side flow and only allow inventory-reducing orders.
    // If net is flat, both sides stay off until counters are reset/increased.
    if (reload_limit_hit) {
        if (side == Side::BUY) {
            return net_ll < 0;
        }
        return net_ll > 0;
    }

    // Tim spec (2026-05-26): placement gate uses net_po only — open/resting orders are excluded.
    // Reduce-only at cap: long -> SELL-only, short -> BUY-only. Grow side only while |net| < max.
    if (side == Side::BUY) {
        if (net_ll < 0) {
            return true;
        }
        return net_ll < max_ll;
    }
    if (net_ll > 0) {
        return true;
    }
    return static_cast<long long>(std::llabs(net_ll)) < max_ll;
}

std::string MakeMarketStrategy::mmAxSymbol() const {
    // Resolution order (per-leg ALWAYS wins over global, otherwise every
    // spawned strategy resolves to `market_maker.order_symbol` from the
    // global config and submits orders on the wrong market — rejected for
    // "Price Out of Bounds" / "Invalid Price Increment" when symbol and tick do not match.
    if (!mm_order_symbol_override_.empty()) {
        return mm_order_symbol_override_;
    }
    if (!mm_symbol_override_.empty()) {
        return mm_symbol_override_;
    }
    auto& cfg = config::Config::getInstance();
    std::string o = cfg.getMarketMakerOrderSymbol();
    if (!o.empty()) {
        return o;
    }
    return cfg.getMarketMakerSymbol();
}

bool MakeMarketStrategy::mmOrderManagerHasActiveBidAndAsk() const {
    return bid_order_id_ != 0 && ask_order_id_ != 0 && mmTrackedSideHasActiveLimitOrder(Side::BUY) &&
           mmTrackedSideHasActiveLimitOrder(Side::SELL);
}

bool MakeMarketStrategy::mmOrderManagerHasActiveLimitOnSide(Side side) const {
    const OrderId tid = (side == Side::BUY) ? bid_order_id_ : ask_order_id_;
    return tid != 0 && mmTrackedSideHasActiveLimitOrder(side);
}

bool MakeMarketStrategy::mmTrackedSideHasActiveLimitOrder(Side side) const {
    const OrderId tracked_id = (side == Side::BUY) ? bid_order_id_ : ask_order_id_;
    if (tracked_id == 0) {
        return false;
    }
    const auto op = orders::OrderManager::getInstance().getOrder(tracked_id);
    return op && op->isActive() && !op->isTerminal() && op->type == OrderType::LIMIT && op->side == side;
}

void MakeMarketStrategy::mmCancelAllExchangeAndResetLocal(const std::string& tag) {
    // Same mutex as runFullMmQuoteCycle — this mutates bid/ask id state + OM. Without
    // this, a feed-thread cancel (e.g. invalid theo) can race main-thread mm_startup
    // and corrupt memory (segfault) under multi-leg MM.
    std::lock_guard<std::recursive_mutex> cycle_lock(mm_cycle_mutex_);
    const std::string sym = mmAxSymbol();
    int mm_on_ax = 0;
    for (const auto& sp : StrategyManager::getInstance().getAllStrategies()) {
        auto mm = std::dynamic_pointer_cast<MakeMarketStrategy>(sp);
        if (mm && mm->mmAxSymbol() == sym) {
            ++mm_on_ax;
        }
    }
    Main().logger()->info(
        "[STRATEGY:{}] MM cancel symbol={} mm_stacks_on_product={} tag={}", getName(), sym, mm_on_ax, tag);
    if (mm_on_ax <= 1) {
        auto resp = Main().rest()->cancelAllOrders(sym);
        if (!resp.is_success) {
            Main().logger()->warn("[STRATEGY:{}] cancel-all HTTP {} body={}", getName(), resp.status_code,
                resp.error_message.empty() ? resp.body : resp.error_message);
        }
        // Single MM on this AX product: safe to purge local OM rows for the symbol.
        orders::OrderManager::getInstance().purgeNonTerminalOrdersForSymbol(sym);
    } else {
        // Multiple `mm_req_*` stacks share one symbol: never venue cancel-all — only this stack's pair.
        (void)mmCancelTrackedLegThisStackGateway(Side::BUY);
        (void)mmCancelTrackedLegThisStackGateway(Side::SELL);
    }
    clearOpenOrderTracking();
    bid_order_id_ = 0;
    ask_order_id_ = 0;
    last_bid_price_ = 0.0;
    last_ask_price_ = 0.0;
    mm_desk_active_ = false;
    resetMmSessionFillTracking(0.0, 0.0);
    mm_pending_accepts_ = 0;
    mm_pending_accepts_last_change_ms_.store(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now().time_since_epoch())
            .count(),
        std::memory_order_relaxed);
}

int MakeMarketStrategy::mmCancelTrackedLegThisStackGateway(Side side) {
    const OrderId tid = (side == Side::BUY) ? bid_order_id_ : ask_order_id_;
    if (tid == 0) return 1;

    // Get exchange OID + leg qty from tracked vectors first (most reliable).
    // The qty is captured here so that on a benign HTTP 404 cancel response
    // (which can mean "already filled" OR "already cancelled") we can compare
    // the venue position delta to the leg's qty and synthesize a fill instead
    // of silently evicting the leg. See the SILVER max_position breach fix
    // (2026-05-21 XAG/SILVER incidents) — the smoking gun was order 312 in
    // logMrinal/Example.txt @ 07:49:25.051 where a 404 was treated as benign
    // and the strategy missed an inbound fill, leaving NetPo stale; the next
    // cycle then placed a BUY 314 that pushed venue NetPo from 2 -> 3 over a
    // max_position=2 cap.
    std::string exch_oid;
    {
        std::lock_guard<std::mutex> lk(tracked_orders_mutex_);
        auto& vec = (side == Side::BUY) ? tracked_bids_ : tracked_asks_;
        for (auto& t : vec) {
            if (t.local_oid == tid && !t.exchange_oid.empty()) {
                exch_oid = t.exchange_oid;
                break;
            }
        }
    }

    if (exch_oid.empty()) {
        auto& om = orders::OrderManager::getInstance();
        const auto op = om.getOrder(tid);
        if (op && !op->exchange_order_id.empty()) {
            exch_oid = op->exchange_order_id;
        }
    }

    if (exch_oid.empty()) {
        Main().logger()->warn(
            "[STRATEGY:{}] cancel tracked leg: no exchange OID for "
            "side={} tid={} — treating as vacuous success",
            getName(), side == Side::BUY ? "BUY" : "SELL", tid);
        return 1;
    }

    Main().logger()->info(
        "[STRATEGY:{}] cancel tracked leg: sending REST cancel "
        "exch_oid={} side={} ax={}",
        getName(), exch_oid,
        side == Side::BUY ? "BUY" : "SELL", mmAxSymbol());

    auto is_network_or_timeout = [](const api::HttpResponse& r) {
        std::string msg = r.error_message.empty() ? r.body : r.error_message;
        for (char& c : msg) {
            c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
        }
        if (r.status_code == 408 || r.status_code == 504 || r.status_code == 0) {
            return true;
        }
        if (msg.find("timeout") != std::string::npos || msg.find("timed out") != std::string::npos) {
            return true;
        }
        if (msg.find("connection refused") != std::string::npos ||
            msg.find("connection reset") != std::string::npos ||
            msg.find("could not connect") != std::string::npos ||
            msg.find("network is unreachable") != std::string::npos) {
            return true;
        }
        return false;
    };
    // === FIX G: EVICT CANCELLED LEG FROM tracked_*_ ON SUCCESS ===
    // Live evidence 2026-05-07 14:18:43.481: cycle 1's brute-force cancel-replace called
    // `mmCancelTrackedLegThisStackGateway(BUY)` and `(SELL)` synchronously, both succeeded
    // (HTTP 200), then set `bid_order_id_=0 / ask_order_id_=0` — but the corresponding
    // ADOPTED rows in `tracked_bids_/tracked_asks_` were never touched. ~9 ms later
    // `mm_pair_enforce` saw `bid_id=0 ask_id=0`, fired Fix D's DESK_RECOVERY warning, then
    // immediately bailed at `desk_exchange_orders_not_live` because `tryAdoptGatewayPlacedFromManualStack`
    // matched the zombie ADOPTED rows against the stale manual_stack snapshot and short-circuited.
    //
    // Erasing the matched leg right after the venue confirms cancel keeps the local view
    // consistent with reality:
    //   - `bid_order_id_/ask_order_id_=0` (set by the caller after the cancel returns)
    //   - tracked vector no longer carries an "ADOPTED with this oid" row
    // The OM_GATEWAY direction-A reconcile still sees `om.getActiveOrders()` and emits the
    // local→CANCELLED sync as before; that path is independent of our tracked vectors.
    auto evict_oid_locally = [&](const std::string& oid_to_drop) {
        if (oid_to_drop.empty()) {
            return;
        }
        std::lock_guard<std::mutex> lk(tracked_orders_mutex_);
        auto erase_match = [&](std::vector<TrackedLeg>& vec) {
            vec.erase(std::remove_if(vec.begin(), vec.end(),
                                     [&](const TrackedLeg& t) { return t.exchange_oid == oid_to_drop; }),
                      vec.end());
        };
        erase_match(tracked_bids_);
        erase_match(tracked_asks_);
    };

    // Venue source-of-truth guard (2026-06-11; skip-on-absent REMOVED 2026-06-15): we NO LONGER
    // skip the cancel when the venue's last-known snapshot lacks this oid. A just-placed order may
    // not have propagated into /open-orders yet, so "absent from a fresh snapshot" does NOT mean
    // "already gone" — treating it that way evicted the leg locally without cancelling and leaked
    // orphans (fast cancel-replace on liquid instruments — verified across 3 prod logs). Refresh
    // venue truth for the NEXT decision, then ALWAYS send the real cancel below. A 404 for a
    // genuinely-gone order is handled as benign success in the loop's mmIsBenignCancelFailure path.
    {
        bool cache_fresh = false;
        if (!mmVenueTruthKnowsOid(exch_oid, cache_fresh)) {
            mmTriggerImmediateVenueReconcile();
        }
    }

    // LIFETIME CONTRACT (do not break): this cancel MUST stay SYNCHRONOUS on the mover thread.
    // The benign-404 / CANCEL_PENDING response handler below mutates tracked_*_ under
    // tracked_orders_mutex_; its safety relies on the shared_ptr pin held by the synchronous
    // mover lambda for this whole call (request AND response), NOT on a response-side isRunning()
    // check. Moving this onto the async RestClient::requestAsync path (detached thread) would let
    // the response handler run after strategy teardown and reintroduce a use-after-free.
    for (int attempt = 1; attempt <= 3; ++attempt) {
        // CANCEL-SPEED FIX: POST-first (~250ms) instead of DELETE-first (~1000ms). See the
        // long comment on the helper above (line ~170) for the empirical justification.
        api::HttpResponse cr;
        {
            nlohmann::json cancel_body = nlohmann::json::object();
            cancel_body["oid"] = exch_oid;
            cr = Main().rest()->cancelOrderGateway(cancel_body.dump());
        }
        if (!cr.is_success) {
            cr = Main().rest()->cancelOrder(exch_oid);
        }
        const std::string err_tail = cr.error_message.empty() ? cr.body : cr.error_message;
        if (cr.is_success) {
            Main().logger()->info(
                "[THEO_MOVE:{}] cancel result: oid={} http_status={} treating_as_success=true",
                mmAxSymbol(),
                exch_oid,
                cr.status_code);
            last_cancel_response_ms_.store(
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::steady_clock::now().time_since_epoch())
                    .count(),
                std::memory_order_release);
            evict_oid_locally(exch_oid);
            return 1;
        }
        if (mmIsBenignCancelFailure(cr)) {
            // === MAX-POSITION BREACH FIX (2026-05-22 Joe XAG breach, log
            //     `Max Position Breach.txt`) =====================================
            // 404 / "not found" / "cannot be cancelled" responses are AMBIGUOUS:
            // the venue is telling us the order is no longer open, but it does
            // NOT distinguish between (a) the order was cancelled and (b) the
            // order JUST FILLED and the matching fill notification is still
            // in-flight on the WebSocket fill stream.
            //
            // Previously we treated 404 as "truly cancelled": this branch
            // called `evict_oid_locally(exch_oid)` which removed the leg from
            // `tracked_bids_`/`tracked_asks_`. The very next thing the cycle
            // does is place a replacement leg, whose cap-gate projection reads
            // `sumWorkingLegQtyForSymbol` and gets 0 for our own just-evicted
            // qty — so even at the cap boundary the gate admits the new leg.
            // If the 404 was actually case (b), the old leg's fill arrives a
            // moment later and fills against the position the replacement is
            // also working into, pushing us 1 lot over the cap.
            //
            // Live evidence (`Max Position Breach.txt`, XAG-PERP, max_po=2):
            //   13:25:17.723  cancel 2368 → 404 benign → leg evicted
            //   13:25:17.772  bid 2370 placed (cap projection 1+0+1=2 ≤ 2)
            //   13:25:19.748  FILL_EVENT on 2368 (the "cancelled" leg) → 2
            //   13:25:21.818  FILL_EVENT on 2370 → 3   ←── BREACH
            //
            // Fix: do NOT evict on 404. Transition the leg to CANCEL_PENDING
            // (the state already used for "REST cancel sent, awaiting venue
            // confirmation") and update its `state_entered_steady_ms`.
            // `sumWorkingLegQtyForSymbol` already counts CANCEL_PENDING legs,
            // so the very next cap-gate check in the same cycle sees this leg
            // as "still potentially fillable" and refuses to admit a
            // boundary-breaching replacement. If the venue actually did fill
            // the order, the WebSocket fill notification arrives within the
            // next few hundred ms, `processFill[DeskStack]` matches by
            // local_oid (the leg is still in the vector!) and applies the fill
            // correctly. If the venue actually did cancel the order, the leg
            // sits in CANCEL_PENDING until `maybeReconcileDeskCancelAckTimeouts`
            // (called from runFullMmQuoteCycle / fill paths / timer paths)
            // checks GET /open-orders after `getMarketMakerCancelAckTimeoutMs`
            // elapsed and transitions to CANCELLED via the existing path.
            // `kMmCancelUncertainBudgetMs` is the cap-gate protection floor
            // (cap-gate guarantees to count this leg for at least this many
            // ms even if some future code path early-evicts).
            //
            // This restores Tim's original `CANCEL_REVEALED_FILL` intent
            // (cancel-vs-fill race protection) without the REST /positions
            // probe he objected to — we use only local state + the existing
            // fill stream + a short uncertainty window.
            Main().logger()->info(
                "[THEO_MOVE:{}] cancel result: oid={} http_status={} treating_as_success=true (benign) — "
                "leg kept in CANCEL_PENDING (cancel-vs-fill race protection, protection floor {}ms)",
                mmAxSymbol(), exch_oid, cr.status_code, kMmCancelUncertainBudgetMs);
            last_cancel_response_ms_.store(
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::steady_clock::now().time_since_epoch())
                    .count(),
                std::memory_order_release);
            // Mark the leg CANCEL_PENDING in place. This is intentionally NOT
            // an eviction: the cap-gate (`sumWorkingLegQtyForSymbol`) MUST keep
            // counting the qty until either a fill arrives or the sweep window
            // elapses. Holding `tracked_orders_mutex_` keeps this consistent
            // against concurrent `processFill` on the OM thread.
            const std::int64_t now_ms_cp = mmSteadyMillis();
            {
                std::lock_guard<std::mutex> lk(tracked_orders_mutex_);
                auto mark_match = [&](std::vector<TrackedLeg>& vec) {
                    for (auto& t : vec) {
                        if (t.exchange_oid == exch_oid) {
                            // FILLED is terminal — never reverse it from a stale 404 (the
                            // fill notification could already have raced ahead of us).
                            if (t.state != MmLegState::FILLED) {
                                t.state = MmLegState::CANCEL_PENDING;
                                t.state_entered_steady_ms = now_ms_cp;
                                t.desk_cancel_timeout_retry_sent = false;
                            }
                        }
                    }
                };
                mark_match(tracked_bids_);
                mark_match(tracked_asks_);
            }
            return 1;
        }
        if (is_network_or_timeout(cr)) {
            if (attempt < 3) {
                Main().logger()->warn(
                    "[STRATEGY:{}] cancel tracked leg network/timeout retry {}/3: oid={} side={} HTTP={} err={}",
                    getName(),
                    attempt,
                    exch_oid,
                    side == Side::BUY ? "BUY" : "SELL",
                    cr.status_code,
                    err_tail);
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
                continue;
            }
            Main().logger()->warn(
                "[THEO_MOVE:{}] cancel result: oid={} http_status={} treating_as_success=false (network/timeout)",
                mmAxSymbol(),
                exch_oid,
                cr.status_code);
            return 0;
        }
        Main().logger()->info(
            "[THEO_MOVE:{}] cancel result: oid={} http_status={} treating_as_success=true err={}",
            mmAxSymbol(),
            exch_oid,
            cr.status_code,
            err_tail.size() > 240 ? err_tail.substr(0, 240) + "..." : err_tail);
        evict_oid_locally(exch_oid);
        return 1;
    }
    return 0;
}

void MakeMarketStrategy::mmPullBothTrackedLegsAfterFillStall(const std::string& tag) {
    std::lock_guard<std::recursive_mutex> cycle_lock(mm_cycle_mutex_);
    Main().logger()->warn(
        "[STRATEGY:{}] MM_PULL_BOTH_LEGS tag={} ax={} bid_id={} ask_id={} — cancel survivor / stale pair",
        getName(),
        tag,
        mmAxSymbol(),
        static_cast<long long>(bid_order_id_),
        static_cast<long long>(ask_order_id_));
    (void)mmCancelTrackedLegThisStackGateway(Side::BUY);
    (void)mmCancelTrackedLegThisStackGateway(Side::SELL);
    bid_order_id_ = 0;
    ask_order_id_ = 0;
    last_bid_price_ = 0.0;
    last_ask_price_ = 0.0;
}

int MakeMarketStrategy::mmCancelTrackedLegAllStacksOnAx(const std::string& ax_symbol,
                                                        Side side,
                                                        MakeMarketStrategy* cycle_mutex_held) {
    if (ax_symbol.empty()) {
        return 0;
    }
    // mms_pin retains ownership for the whole cancel window so a concurrent sibling teardown cannot
    // free a stack whose cycle mutex we hold (UAF guard, 2026-06-11).
    auto mms_pin = collectMakeMarketStrategySharedPtrsOnAx(ax_symbol);
    std::vector<MakeMarketStrategy*> mms;
    mms.reserve(mms_pin.size());
    for (auto& mm_sp : mms_pin) {
        mms.push_back(mm_sp.get());
    }
    if (mms.empty()) {
        return 0;
    }
    std::sort(mms.begin(), mms.end());
    for (auto* mm : mms) {
        if (mm == cycle_mutex_held) {
            continue;
        }
        mm->mm_cycle_mutex_.lock();
    }
    int n = 0;
    for (auto* mm : mms) {
        const OrderId tid = (side == Side::BUY) ? mm->bid_order_id_ : mm->ask_order_id_;
        if (tid != 0 && mm->mmCancelTrackedLegThisStackGateway(side) == 1) {
            ++n;
        }
        mm->mmResetLocalOrderTrackingOnSide(side);
    }
    for (auto it = mms.rbegin(); it != mms.rend(); ++it) {
        if (*it == cycle_mutex_held) {
            continue;
        }
        (*it)->mm_cycle_mutex_.unlock();
    }
    return n;
}

void MakeMarketStrategy::resetMmSessionFillTracking(Quantity bid_leg_qty, Quantity ask_leg_qty) {
    std::lock_guard<std::mutex> lock(fill_mutex_);
    bid_order_fills_.clear();
    ask_order_fills_.clear();
    total_bid_filled_ = 0;
    total_ask_filled_ = 0;
    original_bid_qty_ = bid_leg_qty;
    original_ask_qty_ = ask_leg_qty;
}

double MakeMarketStrategy::resolveAxQuoteTick(double config_quote_tick) {
    const std::string sym = mmAxSymbol();
    auto& cfg = config::Config::getInstance();
    if (!sym.empty()) {
        const double from_catalog = cfg.getAxGatewayInstrumentTickSize(sym);
        if (from_catalog > 0.0 && std::isfinite(from_catalog)) {
            cached_ax_quote_tick_ = from_catalog;
            return from_catalog;
        }
    }
    double fallback = config_quote_tick;
    if (!std::isfinite(fallback) || fallback <= 0.0) {
        fallback = 0.0;
    }
    if (sym.empty()) {
        return fallback;
    }
    auto& mdm = marketdata::MarketDataManager::getInstance();
    auto m_opt = mdm.getMarket(sym);
    if (m_opt.has_value() && m_opt->min_price > 0.0) {
        cached_ax_quote_tick_ = m_opt->min_price;
        return m_opt->min_price;
    }
    const auto now = std::chrono::steady_clock::now();
    if (now >= mm_ax_quote_tick_next_refresh_) {
        mm_ax_quote_tick_next_refresh_ = now + std::chrono::seconds(45);
        mdm.refreshMarkets();
        m_opt = mdm.getMarket(sym);
        if (m_opt.has_value() && m_opt->min_price > 0.0) {
            cached_ax_quote_tick_ = m_opt->min_price;
            return m_opt->min_price;
        }
    }
    return fallback > 0.0 && std::isfinite(fallback) ? fallback : 0.0;
}

double MakeMarketStrategy::mmEffResolvedQuoteTick(const config::Config& cfg) {
    return resolveAxQuoteTick(mmEffPriceTick(cfg));
}

void MakeMarketStrategy::syncReloadLimitStateFromConfig() {
    auto& cfg = config::Config::getInstance();
    const int max_now = mmEffMaxReloadCyclesInt(cfg);
    const std::string nonce = cfg.getString("market_maker.reload_limit_reset_nonce", "");

    if (!mmOptionalManualStack().has_value()) {
        mm_reload_cycles_used_ = cfg.getMarketMakerCurrentReloadCount();
    }

    if (!mm_reload_limit_nonce_initialized_) {
        mm_reload_limit_nonce_seen_ = nonce;
        mm_reload_limit_nonce_initialized_ = true;
    } else if (nonce != mm_reload_limit_nonce_seen_) {
        mm_reload_limit_nonce_seen_ = nonce;
        mm_reload_cycles_used_ = 0;
        mm_reload_limit_engaged_ = false;
        cfg.persistPrimaryConfigMarketMakerCurrentReloadCount(0);
        Main().logger()->info(
            "[STRATEGY:{}] MM reload limit: current_reload_count cleared (reload_limit_reset_nonce changed) — "
            "quoting allowed again",
            getName());
    }

    if (mm_reload_limit_max_cfg_seen_ < 0) {
        mm_reload_limit_max_cfg_seen_ = max_now;
    } else if (max_now > mm_reload_limit_max_cfg_seen_) {
        mm_reload_cycles_used_ = 0;
        mm_reload_limit_engaged_ = false;
        cfg.persistPrimaryConfigMarketMakerCurrentReloadCount(0);
        Main().logger()->info(
            "[STRATEGY:{}] MM reload limit: current_reload_count cleared (max_reload_cycles increased {} → {})",
            getName(),
            mm_reload_limit_max_cfg_seen_,
            max_now);
        mm_reload_limit_max_cfg_seen_ = max_now;
    } else {
        mm_reload_limit_max_cfg_seen_ = max_now;
    }

    if (max_now == 0) {
        mm_reload_limit_engaged_ = false;
    } else if (mm_reload_cycles_used_ < max_now) {
        mm_reload_limit_engaged_ = false;
    }
}

void MakeMarketStrategy::mmLogMmGateThrottled(const std::string& gate, const std::string& context_reason,
                                              const std::string& extra_detail) {
    constexpr std::int64_t kThrottleMs = 8000;
    const auto now_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now().time_since_epoch())
            .count();
    {
        std::lock_guard<std::mutex> lk(mm_mm_gate_log_mutex_);
        if (gate == mm_mm_gate_last_tag_ && mm_mm_gate_last_log_ms_ != 0 &&
            (now_ms - mm_mm_gate_last_log_ms_) < kThrottleMs) {
            return;
        }
        mm_mm_gate_last_log_ms_ = now_ms;
        mm_mm_gate_last_tag_ = gate;
    }

    auto& cfg = config::Config::getInstance();
    const int max_reload_cfg = mmEffMaxReloadCyclesInt(cfg);
    const int reload_from_file = cfg.getMarketMakerCurrentReloadCount();
    const bool fill_only_reload = cfg.getMarketMakerReloadCyclesCountFillOnly();
    const bool accept_counts =
        mmCycleReasonCountsTowardReloadLimit(context_reason, fill_only_reload);
    bool feed_up = true;
    if (!mm_theo_source_.empty()) {
        feed_up = marketdata::ExternalFeedManager::getInstance().isFeedUpForSource(mm_theo_source_);
    }
    const bool ws_ok = !cfg.getMarketMakerCancelOnDisconnect() || Main().websocket()->isConnected();
    const bool mm_sym_on = cfg.getMarketMakerMmOrdersEnabledForSymbol(mmAxSymbol());
    std::string extra = extra_detail;
    if (!extra.empty()) {
        extra = " | " + extra;
    }
    if (gate.find("awaiting_accept_blocks_pair_enforce") != std::string::npos) {
        Main().logger()->warn(
            "[STRATEGY:{}] MM_GATE gate={} context={}{} | reload_local={} reload_cfg={} max_reload={} "
            "reload_engaged={} fill_only_reload_mode={} accept_counts_for_context={} pending_accepts={} "
            "bid_id={} ask_id={} last_bid_px={:.8g} last_ask_px={:.8g} ws_ok={} mm_orders_sym={} "
            "theo_src='{}' feed_up={} mm_enabled={}",
            getName(),
            gate,
            context_reason,
            extra,
            mm_reload_cycles_used_,
            reload_from_file,
            max_reload_cfg,
            mm_reload_limit_engaged_ ? 1 : 0,
            fill_only_reload ? 1 : 0,
            accept_counts ? 1 : 0,
            mm_pending_accepts_,
            bid_order_id_,
            ask_order_id_,
            last_bid_price_,
            last_ask_price_,
            ws_ok ? 1 : 0,
            mm_sym_on ? 1 : 0,
            mm_theo_source_,
            feed_up ? 1 : 0,
            cfg.isMarketMakerEnabled() ? 1 : 0);
    } else {
        Main().logger()->info(
            "[STRATEGY:{}] MM_GATE gate={} context={}{} | reload_local={} reload_cfg={} max_reload={} "
            "reload_engaged={} fill_only_reload_mode={} accept_counts_for_context={} pending_accepts={} "
            "bid_id={} ask_id={} last_bid_px={:.8g} last_ask_px={:.8g} ws_ok={} mm_orders_sym={} "
            "theo_src='{}' feed_up={} mm_enabled={}",
            getName(),
            gate,
            context_reason,
            extra,
            mm_reload_cycles_used_,
            reload_from_file,
            max_reload_cfg,
            mm_reload_limit_engaged_ ? 1 : 0,
            fill_only_reload ? 1 : 0,
            accept_counts ? 1 : 0,
            mm_pending_accepts_,
            bid_order_id_,
            ask_order_id_,
            last_bid_price_,
            last_ask_price_,
            ws_ok ? 1 : 0,
            mm_sym_on ? 1 : 0,
            mm_theo_source_,
            feed_up ? 1 : 0,
            cfg.isMarketMakerEnabled() ? 1 : 0);
    }
}

bool MakeMarketStrategy::mmTryApplyDeskInPlaceModifyOnly(
    const std::string& reason,
    Price theo_midpoint,
    Price adjusted_theo,
    Price bid,
    Price ask,
    Quantity qty_bid,
    Quantity qty_ask,
    bool do_bid,
    bool do_ask,
    bool want_bid,
    bool want_ask,
    bool reload_reduce_only_active,
    int bid_spread_bps,
    int ask_spread_bps,
    double quote_tick,
    long long net_po_rounded,
    int max_po_cfg,
    int adjust_po_cfg,
    int adjust_ticks_cfg,
    double skew_tick_units,
    Price skew_price,
    Price raw_bid,
    Price raw_ask) {
    if (!mmIsDeskManaged()) {
        return false;
    }
    if (!mmReasonAllowsDeskInPlaceModify(reason)) {
        return false;
    }
    if (reload_reduce_only_active) {
        return false;
    }
    const bool fill_driven_desk = (reason.rfind("fill_requote", 0) == 0);
    if (!fill_driven_desk) {
        return false;
    }
    if (!(want_bid && want_ask)) {
        return false;
    }
    if (!(do_bid && do_ask)) {
        // fill_requote only (early return above for non-fill). Pull both legs so no orphan survives
        // when BBO/qty blocks re-laying.
        (void)mmCancelTrackedLegThisStackGateway(Side::BUY);
        (void)mmCancelTrackedLegThisStackGateway(Side::SELL);
        bid_order_id_ = 0;
        ask_order_id_ = 0;
        last_bid_price_ = 0.0;
        last_ask_price_ = 0.0;
        Main().logger()->warn(
            "[STRATEGY:{}] fill_requote: desk in-place aborted (do_bid={} do_ask={}) — pulled both tracked "
            "legs (BBO/qty); no orphan survivor",
            getName(),
            do_bid ? 1 : 0,
            do_ask ? 1 : 0);
        // Never return true here: that would swallow runFullMmQuoteCycle and skip generic cancel+place
        // (older trees also logged a misleading MM_GATE for theo_move when qty rounded to 0).
        return false;
    }
    const bool desk_periodic_repricer =
        (reason == "theo_move" || reason == "timer_update" || reason == "timer_reconcile" ||
         reason == "mm_orders_resume");
    if (!mmDeskTrackedOrdersAliveForMove()) {
        if (!fill_driven_desk) {
            const bool have_tracked_pair = (bid_order_id_ != 0 && ask_order_id_ != 0);
            if (!(desk_periodic_repricer && have_tracked_pair)) {
                mmLogMmGateThrottled(
                    "desk_in_place_skip_no_adopted_pair",
                    reason,
                    "desk stack: no adopted bid+ask in OrderManager — skip (blank orders.json until desk seeds)");
                last_theo_ = theo_midpoint;
                last_theo_requote_anchor_ = theo_midpoint;
                last_theo_requote_anchor_inited_ = true;
                printMmVerifyStdout(
                    reason,
                    theo_midpoint,
                    adjusted_theo,
                    bid_spread_bps,
                    ask_spread_bps,
                    quote_tick,
                    net_po_rounded,
                    max_po_cfg,
                    adjust_po_cfg,
                    adjust_ticks_cfg,
                    skew_tick_units,
                    skew_price,
                    raw_bid,
                    raw_ask,
                    bid,
                    ask,
                    qty_bid,
                    qty_ask,
                    want_bid,
                    want_ask,
                    false,
                    false);
                return true;
            }
        }
    }

    auto& om = orders::OrderManager::getInstance();
    const Price px_eps = quote_tick * 0.25;
    bool placed_bid = false;
    bool placed_ask = false;

    bool bid_price_ok = false;
    bool ask_price_ok = false;
    bool bid_need_replace = false;
    bool ask_need_replace = false;
    if (do_bid && bid_order_id_ != 0) {
        const auto op = om.getOrder(bid_order_id_);
        if (op && op->isActive() && !op->exchange_order_id.empty()) {
            if (std::fabs(op->price - bid) <= px_eps) {
                bid_price_ok = true;
            } else {
                bid_need_replace = true;
            }
        } else if (desk_periodic_repricer || fill_driven_desk) {
            // Stale/missing OM row but we still hold tracked ids — attempt gateway cancel-replace so theo
            // moves are not frozen after accept lag or open-orders reconcile marking the row inactive.
            bid_need_replace = true;
        }
    }
    if (do_ask && ask_order_id_ != 0) {
        const auto op = om.getOrder(ask_order_id_);
        if (op && op->isActive() && !op->exchange_order_id.empty()) {
            if (std::fabs(op->price - ask) <= px_eps) {
                ask_price_ok = true;
            } else {
                ask_need_replace = true;
            }
        } else if (desk_periodic_repricer || fill_driven_desk) {
            ask_need_replace = true;
        }
    }

    if (fill_driven_desk || bid_need_replace || ask_need_replace) {
        std::lock_guard<std::mutex> ax_repricer_lk(mmDeskAxRepricerMutex(mmAxSymbol()));
        // Per-leg cancel: after a full fill one id is already 0 — do not require both cancels to "succeed"
        // on missing legs (that used to block fill_requote and left a single working orphan).
        const int cb = mmCancelTrackedLegThisStackGateway(Side::BUY);
        const int ca = mmCancelTrackedLegThisStackGateway(Side::SELL);
        if (cb != 1 || ca != 1) {
            Main().logger()->warn(
                "[STRATEGY:{}] desk cancel-replace: venue cancel incomplete bid_ok={} ask_ok={} ax={} — "
                "not placing replacements (avoids duplicate legs); check gateway / oid state",
                getName(),
                cb,
                ca,
                mmAxSymbol());
            placed_bid = bid_price_ok;
            placed_ask = ask_price_ok;
            if (fill_driven_desk) {
                // fill_requote must not leave the pre-fill survivor working if per-stack cancels failed.
                const std::string ax_scrub = mmAxSymbol();
                (void)mmCancelTrackedLegAllStacksOnAx(ax_scrub, Side::BUY, this);
                (void)mmCancelTrackedLegAllStacksOnAx(ax_scrub, Side::SELL, this);
                return false;
            }
        } else {
            bid_order_id_ = 0;
            ask_order_id_ = 0;
            last_bid_price_ = 0.0;
            last_ask_price_ = 0.0;

            int pending_inc = 0;  // count of legs actually reserved+submitted, for the log below
            const std::string sym_s = mmAxSymbol();

            const long long cap_ll_mod = static_cast<long long>(std::max(1, max_po_cfg));

            if (do_bid) {
                auto req_b = OrderRequest::limit_buy(sym_s, qty_bid, bid, "mm_bid");
                // Reserve BEFORE submit (mm_pending_accepts_ bumped inside) so the inline
                // accept dispatched during submit_order matches the leg and balances it.
                const auto cid_b = mmPlaceReservedLeg(Side::BUY, bid, qty_bid, req_b, cap_ll_mod, reason);
                if (!cid_b.empty()) {
                    last_bid_price_ = bid;
                    ++quotes_sent_;
                    placed_bid = true;
                    ++pending_inc;
                } else {
                    Main().logger()->warn(
                        "[STRATEGY:{}] desk cancel-replace: bid submit returned empty client id ax={}",
                        getName(),
                        mmAxSymbol());
                }
            }
            if (do_ask) {
                auto req_a = OrderRequest::limit_sell(sym_s, qty_ask, ask, "mm_ask");
                const auto cid_a = mmPlaceReservedLeg(Side::SELL, ask, qty_ask, req_a, cap_ll_mod, reason);
                if (!cid_a.empty()) {
                    last_ask_price_ = ask;
                    ++quotes_sent_;
                    placed_ask = true;
                    ++pending_inc;
                } else {
                    Main().logger()->warn(
                        "[STRATEGY:{}] desk cancel-replace: ask submit returned empty client id ax={}",
                        getName(),
                        mmAxSymbol());
                }
            }
            Main().logger()->info(
                "[STRATEGY:{}] desk cancel-replace: new limits bid={} ask={} pending+={} ax={}",
                getName(),
                placed_bid ? 1 : 0,
                placed_ask ? 1 : 0,
                pending_inc,
                mmAxSymbol());
        }
    } else {
        placed_bid = bid_price_ok;
        placed_ask = ask_price_ok;
    }

    if (!first_quote_logged_) {
        const std::string max_po_source =
            (mm_instrument_max_position_ > 0)
                ? (mm_max_position_source_.empty() ? std::string("instrument") : mm_max_position_source_)
                : std::string("global");
        Main().logger()->info(
            "[FIRST_QUOTE] stack={} instrument={} net_po={} max_po={} source={} want_bid={} want_ask={} "
            "(desk: cancel-replace path)",
            getName(),
            mmAxSymbol(),
            net_po_rounded,
            max_po_cfg,
            max_po_source,
            want_bid ? "Y" : "N",
            want_ask ? "Y" : "N");
        first_quote_logged_ = true;
    }

    printMmVerifyStdout(
        reason,
        theo_midpoint,
        adjusted_theo,
        bid_spread_bps,
        ask_spread_bps,
        quote_tick,
        net_po_rounded,
        max_po_cfg,
        adjust_po_cfg,
        adjust_ticks_cfg,
        skew_tick_units,
        skew_price,
        raw_bid,
        raw_ask,
        bid,
        ask,
        qty_bid,
        qty_ask,
        want_bid,
        want_ask,
        placed_bid && do_bid,
        placed_ask && do_ask);

    last_theo_ = theo_midpoint;
    last_theo_requote_anchor_ = theo_midpoint;
    last_theo_requote_anchor_inited_ = true;
    return true;
}

void MakeMarketStrategy::runFullMmQuoteCycle(Price theo_midpoint, const std::string& reason) {
    if (!isRunning() || !isEnabled()) {
        return;
    }
    // Venue source-of-truth guard: only an ACTIVE stack may run a cancel/replace cycle.
    // VENUE_ORPHANED / TEARDOWN / DEAD bail out cleanly (single source of truth).
    if (!mmWorkerMayProceed("run_full_quote_cycle")) {
        return;
    }
    // Desk fill-requote uses a dedicated path (wait for cancel ack, venue scrub at cap, AX dedupe).
    if (mmIsDeskManaged() && reason == "fill_requote") {
        tryDeskFillRequoteF4PlaceFreshPair();
        return;
    }
    const MmProductPlacementLeaseScope product_placement_lease(this);
    if (!product_placement_lease.leaseAcquired()) {
        mmLogMmGateThrottled(
            "product_placement_lease_denied",
            reason,
            "exclusive product lease not acquired — skip cycle (fail-safe)");
        return;
    }
    if (mmProductPlacementForceReleasedFor(mmAxSymbol(), getName()) || !isRunning()) {
        return;
    }
    const auto all_stacks_cycle_lock = mmAcquireAllStacksCycleLocks(mmAxSymbol(), this);
    mmSeedProductCycleNetSnapshot(this);

    // === DESK_RECOVERY RACE GUARD (2026-05-20 segfault fix) ==========================
    // Mark this strategy as "actively cycling" so the pair-enforce timer thread cannot
    // mistake a transient `bid_order_id_=0 && ask_order_id_=0` window (normal during
    // cancel-replace) for a stuck stack and fire DESK_RECOVERY. DESK_RECOVERY clearing
    // tracked_bids_/tracked_asks_ from the bg thread WHILE this function is mid-place on
    // the mover thread is what caused (a) the 2026-05-20 process crash and (b) the
    // "phantom bid reload after manual flatten" symptom Joe reported.
    //
    // RAII so we always clear the flag on any return path (incl. exceptions).
    struct CycleRunningGuard {
        std::atomic<bool>& flag;
        std::atomic<std::int64_t>& last_ms;
        explicit CycleRunningGuard(std::atomic<bool>& f, std::atomic<std::int64_t>& l)
            : flag(f), last_ms(l) {
            flag.store(true, std::memory_order_release);
            last_ms.store(
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::steady_clock::now().time_since_epoch()).count(),
                std::memory_order_relaxed);
        }
        ~CycleRunningGuard() {
            last_ms.store(
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::steady_clock::now().time_since_epoch()).count(),
                std::memory_order_relaxed);
            flag.store(false, std::memory_order_release);
        }
    };
    CycleRunningGuard cycle_running_guard(mm_cycle_running_, mm_last_cycle_activity_ms_);

    const bool fill_requote_reason = (reason.rfind("fill_requote", 0) == 0);
    // === FIX D companion ===
    // Desk-managed recovery (see ensureStartupQuotePair `desk_recovery_eligible`) can call
    // runFullMmQuoteCycle when both legs are absent AND tracked vectors are empty (on_cancel
    // erased them). Don't bail on `!hasTrackedOrders()` if the desk JSON still owns this stack
    // and there are no live order ids — that's the very state we are trying to recover from.
    const bool desk_recovery_no_legs =
        mmIsDeskManaged() &&
        bid_order_id_ == 0 && ask_order_id_ == 0 &&
        mmManualStackDeskSeeded() &&
        !hasTrackedOrders() &&
        mmDeskOrdersJsonContainsThisStack();
    if (!fill_requote_reason && !desk_recovery_no_legs &&
        (passive_until_first_manual_order_ || !hasTrackedOrders())) {
        last_theo_ = theo_midpoint;
        return;
    }

    (void)config::Config::getInstance().reloadPrimaryConfigFromDisk();
    auto& cfg = config::Config::getInstance();
    const bool desk_managed = mmIsDeskManaged();
    if (!cfg.isMarketMakerEnabled()) {
        mmLogMmGateThrottled("mm_market_maker_disabled", reason, "isMarketMakerEnabled=false");
        return;
    }
    mmUpdateMmOrdersEnabledTransitionLocked(cfg);
    if (!cfg.getMarketMakerMmOrdersEnabledForSymbol(mmAxSymbol())) {
        Main().logger()->debug(
            "[STRATEGY:{}] MM cycle skipped (market_maker.mm_orders_enabled=false) reason={}",
            getName(),
            reason);
        mmLogMmGateThrottled("mm_orders_disabled_for_symbol", reason,
                             "market_maker.mm_orders_enabled false for this AX symbol");
        return;
    }
    // === Global fast-market breaker (HL SPX) — suppress ALL placement during a global pause ===
    // Independent of this leg's per-instrument MWR enable state, so a single watched instrument's
    // volatility pulls quoting everywhere. PURE SUPPRESSION: cancels/reconcile still proceed (the
    // breach fan-out already pulled the legs); only NEW placement is gated here. Reads the single
    // shared atomic deadline set by FastMarketMonitor on the mover.
    if (FastMarketMonitor::isGloballyPausedNow()) {
        mmLogMmGateThrottled("fast_market_global_pause", reason,
                             "global fast-market breaker active — suppressing cycle placement");
        return;
    }
    // === MWR (Moving Window Range) volatility breaker — MOVER-THREAD-ONLY ===
    // Evaluated after the enable gates and BEFORE any desk gate / target computation /
    // cancel-replace, so a volatility spike pulls quoting regardless of desk state. All MWR
    // state lives in mm_mwr_ (plain members, mover-only). We also cache the resolved params
    // here every cycle so the 1 Hz sampler in mmDrainPendingQuoteCycle can run without re-
    // parsing config per feed tick.
    {
        const int mwr_size = mmEffMwrSizeTicks(cfg);
        const int mwr_win = mmEffMwrWindowSec(cfg);
        const int mwr_pull = mmEffMwrPullSec(cfg);
        mm_mwr_cfg_size_ticks_ = mwr_size;
        mm_mwr_cfg_window_sec_ = mwr_win;
        if (mwr_size > 0 && mwr_win > 0) {
            // Reuse the cycle's own tick resolver (leg tick_size → ax_gateway catalog →
            // global price_tick); same value the price math uses later, just read earlier.
            const double mwr_tick = mmEffResolvedQuoteTick(cfg);
            const auto mwr_now = std::chrono::steady_clock::now();
            if (!(mwr_tick > 0.0) || !std::isfinite(mwr_tick)) {
                if (!mm_mwr_tick_warn_logged_) {
                    mm_mwr_tick_warn_logged_ = true;
                    Main().logger()->warn(
                        "[MM_MWR_BREAKER] strategy={} ax={} skipped — unresolved quote_tick "
                        "(need leg tick_size or ax_gateway_instruments_catalog); not dividing",
                        getName(), mmAxSymbol());
                }
                // Do not divide / do not pause this cycle; fall through to normal flow.
            } else {
                mm_mwr_tick_warn_logged_ = false;
                const auto res = mm_mwr_.evaluate(mwr_now, mwr_tick, mwr_size, mwr_win, mwr_pull);
                if (res.just_resumed && utils::Logger::isInitialized()) {
                    Main().logger()->info("[MM_MWR_RESUME] strategy={} ax={}", getName(), mmAxSymbol());
                }
                if (res.action == MwrBreaker::Action::kPausedSkip) {
                    mm_mwr_paused_until_ms_snapshot_.store(res.paused_until_ms, std::memory_order_relaxed);
                    Main().logger()->debug(
                        "[MM_MWR_BREAKER] strategy={} ax={} paused — skipping cycle (reason={})",
                        getName(), mmAxSymbol(), reason);
                    return;
                }
                if (res.action == MwrBreaker::Action::kBreachPause) {
                    // Pull both tracked legs via the same per-stack gateway cancel the cycle
                    // uses everywhere else; then suppress until the pause expires.
                    (void)mmCancelTrackedLegThisStackGateway(Side::BUY);
                    (void)mmCancelTrackedLegThisStackGateway(Side::SELL);
                    mm_mwr_paused_until_ms_snapshot_.store(res.paused_until_ms, std::memory_order_relaxed);
                    Main().logger()->warn(
                        "[MM_MWR_BREAKER] strategy={} ax={} mwr_ticks={:.2f} size_ticks={} "
                        "window_sec={} paused_ms={}",
                        getName(), mmAxSymbol(), res.mwr_ticks, mwr_size, mwr_win,
                        res.paused_until_ms);
                    return;
                }
                // kProceed / kInsufficient / kInvalidTick → run the cycle normally.
            }
        }
    }
    // Desk (`mm_req_*`): move only while the stack row exists in orders.json (live/frozen) and
    // both adopted venue orders are still active in OrderManager — otherwise skip (cancel/GUI
    // removed stack, user flattened a side, or OIDs not yet adopted).
    if (desk_managed) {
        if (!mmDeskOrdersJsonContainsThisStack()) {
            mmLogMmGateThrottled(
                "desk_orders_json_stack_missing",
                reason,
                "stack_id=" + mmOrderRequestId() + " ax=" + mmAxSymbol() + " — skipping move");
            return;
        }
        if (!mmDeskTrackedOrdersAliveForMove()) {
            const auto now_ms_ad = std::chrono::duration_cast<std::chrono::milliseconds>(
                                       std::chrono::steady_clock::now().time_since_epoch())
                                       .count();
            const std::int64_t last_try = mm_last_desk_adopt_attempt_ms_.load(std::memory_order_relaxed);
            if (now_ms_ad - last_try >= 2500) {
                mm_last_desk_adopt_attempt_ms_.store(now_ms_ad, std::memory_order_relaxed);
                (void)tryAdoptGatewayPlacedFromManualStack();
            }
        }
        if (!mmDeskTrackedOrdersAliveForMove()) {
            // After a fill, one leg is cleared locally while the other may still be live — we must still
            // run fill_requote* to cancel the survivor and place a fresh pair (post_fill widen + skew).
            if (reason.rfind("fill_requote", 0) != 0) {
                const bool desk_periodic_move =
                    (reason == "theo_move" || reason == "timer_update" || reason == "timer_reconcile" ||
                     reason == "mm_orders_resume" || reason == "timer_repeg_one_leg");
                // === FIX F: ALLOW mm_pair_enforce DESK_RECOVERY TO BYPASS THIS GATE ===
                // `mm_pair_enforce` calls into `runFullMmQuoteCycle` only when
                // `ensureStartupQuotePair` has already determined `desk_recovery_eligible`
                // (bid_id=0 / ask_id=0 / orders.json owns stack / desk_seeded). At that point the
                // stack genuinely has nothing live on the venue we still own — Fix E above
                // wiped any zombie tracked rows so `mmDeskTrackedOrdersAliveForMove()` returning
                // false here isn't a "venue still has our legs" condition, it's the very
                // missing-pair state we're trying to recover from. Without this bypass the gate
                // re-fires every 2.5s, the warning logs but no place ever happens, and the
                // operator sees "DESK_RECOVERY" then "skipping move" forever.
                const bool desk_recovery_pair_enforce =
                    (reason.rfind("mm_pair_enforce", 0) == 0) &&
                    bid_order_id_ == 0 && ask_order_id_ == 0 &&
                    mmManualStackDeskSeeded() &&
                    mmDeskOrdersJsonContainsThisStack();
                if (!desk_periodic_move && !desk_recovery_pair_enforce) {
                    mmLogMmGateThrottled(
                        "desk_exchange_orders_not_live",
                        reason,
                        "bid_id=" + std::to_string(static_cast<long long>(bid_order_id_)) +
                            " ask_id=" + std::to_string(static_cast<long long>(ask_order_id_)) +
                            " — skipping move (venue may still show working legs; adopt retries every ~2.5s)");
                    return;
                }
                if (desk_recovery_pair_enforce) {
                    Main().logger()->warn(
                        "[STRATEGY:{}] DESK_RECOVERY bypass desk_exchange_orders_not_live: "
                        "reason={} stack_id={} ax={} — proceeding to place fresh pair",
                        getName(), reason, mmOrderRequestId(), mmAxSymbol());
                }
            }
        }
    }
    // Per-leg feed gate: if this leg's theo_source is down, do not place
    // quotes — but do not crash the strategy. The desk renders this as a
    // red "leg blocked: feed down" indicator (mm_blocked_by_feed_).
    if (!mm_theo_source_.empty()) {
        auto& efm = marketdata::ExternalFeedManager::getInstance();
        const bool up = efm.isFeedUpForSource(mm_theo_source_);
        const bool was_blocked = mm_blocked_by_feed_.exchange(!up);
        if (!up) {
            // Throttle log to once every 5s per leg to avoid log spam during
            // a sustained outage.
            const auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now().time_since_epoch()).count();
            const auto last = mm_last_blocked_log_ms_.load();
            if (!was_blocked || (now_ms - last) > 5000) {
                mm_last_blocked_log_ms_.store(now_ms);
                Main().logger()->warn(
                    "[STRATEGY:{}] MM cycle blocked — leg theo_source='{}' is down, "
                    "no quotes will be placed (reason={}). last_err={}",
                    getName(),
                    mm_theo_source_,
                    reason,
                    mmFeedBlockReason());
            }
            // Cancel any working MM orders on this leg so we don't leave
            // stale quotes if the feed flaps.
            mmCancelAllExchangeAndResetLocal("feed_down_" + mm_theo_source_);
            return;
        }
    }
    syncReloadLimitStateFromConfig();

    const bool fill_requote = (reason.rfind("fill_requote", 0) == 0);
    if (fill_requote) {
        mm_desk_active_ = false;
        mm_desk_gateway_seed_steady_ms_ = 0;
        // Always allow cancel+replace after a fill — stale submit counters must not block desk paths.
        mm_pending_accepts_ = 0;
        mm_pending_accepts_last_change_ms_.store(
            std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now().time_since_epoch())
                .count(),
            std::memory_order_relaxed);
        {
            std::lock_guard<std::mutex> lk(mm_accept_pending_mutex_);
            mm_pending_accept_cycle_reason_.clear();
        }
    }
    // Avoid immediate cancel-replace right after gateway-seed adopt (restart): operator limits stay until
    // quiet window elapses or a fill_requote* clears the anchor. Set quiet_ms=0 for legacy behavior.
    if (desk_managed && !fill_requote) {
        const int quiet_ms = cfg.getInt("market_maker.desk_gateway_seed_repricing_quiet_ms", 10000);
        if (quiet_ms > 0 && mm_desk_gateway_seed_steady_ms_ > 0) {
            const std::int64_t now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                            std::chrono::steady_clock::now().time_since_epoch())
                                            .count();
            const std::int64_t age = now_ms - mm_desk_gateway_seed_steady_ms_;
            if (age >= 0 && age < quiet_ms) {
                if (mmDeskReasonDeferredByGatewaySeedQuiet(reason)) {
                    mmLogMmGateThrottled(
                        "desk_gateway_seed_repricing_quiet",
                        reason,
                        "age_ms=" + std::to_string(age) + " quiet_ms=" + std::to_string(quiet_ms) +
                            " — skipping automatic desk cancel-replace until quiet elapses, "
                            "fill_requote*, or explicit Python desk sync (clears quiet)");
                    return;
                }
            }
        }
    }
    const bool manual_quotes_only = cfg.getBool("market_maker.cpp_manual_quotes_only", true);
    const bool legacy_mm = !desk_managed;
    const bool have_any_tracked = (bid_order_id_ != 0 || ask_order_id_ != 0);
    if (manual_quotes_only && legacy_mm && !fill_requote && !have_any_tracked) {
        mmLogMmGateThrottled(
            "cpp_manual_quotes_only_legacy_skip",
            reason,
            "market_maker.cpp_manual_quotes_only=true — YAML make_market_* does not auto-place until working "
            "limits exist (GUI seed) or a fill_requote* cycle; desk mm_req_* unchanged");
        return;
    }
    const bool cpp_may_place = cfg.getBool("market_maker.cpp_may_place_orders", false);
    if (!cpp_may_place && !fill_requote && legacy_mm) {
        mmLogMmGateThrottled(
            "cpp_autoplace_disabled_legacy_mm",
            reason,
            "C++ does not cancel+place for YAML make_market_* by default; use GUI/orders.json. "
            "Set market_maker.cpp_may_place_orders=true for legacy behavior.");
        return;
    }

    const int max_reload_cfg = mmEffMaxReloadCyclesInt(cfg);
    const bool reload_limit_hit = (max_reload_cfg > 0 && mm_reload_cycles_used_ >= max_reload_cfg);
    if (reload_limit_hit) {
        if (!mm_reload_limit_engaged_) {
            mm_reload_limit_engaged_ = true;
            logStrategyDecision(
                "MM_RELOAD_LIMIT",
                "reduce-only engaged — current_reload_count=" + std::to_string(mm_reload_cycles_used_) +
                    " max_reload_cycles=" + std::to_string(max_reload_cfg) +
                    " | only inventory-reducing side is allowed until counter reset/increase");
        }
        mmLogMmGateThrottled(
            "reload_limit_reduce_only_active",
            reason,
            "grow-side quotes suppressed while reload count >= max_reload_cycles");
    } else if (mm_reload_limit_engaged_) {
        mm_reload_limit_engaged_ = false;
        Main().logger()->info(
            "[STRATEGY:{}] MM reload limit: reduce-only disengaged (current_reload_count={} max_reload_cycles={})",
            getName(),
            mm_reload_cycles_used_,
            max_reload_cfg);
    }
    if (cfg.getMarketMakerCancelOnDisconnect() && !Main().websocket()->isConnected()) {
        mmLogMmGateThrottled("ws_disconnected_before_cycle", reason, "cancel_on_disconnect + AX WS down");
        mmCancelAllExchangeAndResetLocal("mm_ax_ws_disconnected");
        return;
    }

    std::string symbol = mmAxSymbol();
    const int qty_int = mmEffOrderSizeInt(cfg);
    Quantity quantity = static_cast<Quantity>(qty_int);
    if (quantity <= 0) {
        // STRICT: order size must come from the user-entered desk stack
        // (`orders.json` → `mm_req_*` stack). No runtime-config fallback. If we
        // got here either the strategy is a base `make_market_*` leg (no GUI
        // input) or the desk stack is malformed (missing positive `order_size`).
        mmLogMmGateThrottled("order_size_non_positive", reason,
                             "mmEffOrderSizeInt <= 0 — no user-entered order_size in desk stack "
                             "(no config fallback by design; check orders.json `stacks[].order_size`)");
        return;
    }
    // Throttled per-cycle diagnostic — shows the inputs the cycle is about to use
    // for price/qty so live runs can prove API-backed instrument config is the
    // source of truth at the call site (see MM_VERIFY block printed at the end).
    if (utils::Logger::isInitialized() && Main().logger()) {
        static thread_local std::int64_t last_log_ms = 0;
        const auto now_ms = mmSteadyMillis();
        if ((now_ms - last_log_ms) >= 5000) {
            last_log_ms = now_ms;
            const double diag_tick = mmEffResolvedQuoteTick(cfg);
            const auto ms_diag = mmOptionalManualStack();
            const double qs_log = ms_diag.has_value() ? ms_diag->quote_snapshot : 0.0;
            const double ps_log = ms_diag.has_value() ? ms_diag->pricer_snapshot : 0.0;
            const double sl_log = ms_diag.has_value() ? ms_diag->slope : 0.0;
            Main().logger()->info(
                "[STRATEGY:{}] MM_CYCLE_DIAG cycle={} sym={} theo_in={:.6f} basis={:.6f} "
                "theo_scale={:.6f} quote_snap={:.6f} pricer_snap={:.6f} slope={:.6f} "
                "quote_tick={:.6f} qty_int={} desk_managed={} pending_accepts={} bid_id={} ask_id={}",
                getName(),
                reason,
                symbol,
                theo_midpoint,
                cfg.getMarketMakerBasis(),
                mm_theo_scale_,
                qs_log,
                ps_log,
                sl_log,
                diag_tick,
                qty_int,
                mmIsDeskManaged() ? 1 : 0,
                mm_pending_accepts_,
                bid_order_id_,
                ask_order_id_);
        }
    }

    // === NetPo policy change (2026-05-21, Tim's spec) ============================
    // Removed: `syncInventoryFromExchangePortfolio()` + `mmRefreshNetPositionFromPortfolioCache()`
    //
    // Previously this cycle entry would overwrite `position_state_.net_position_qty`
    // from the Architect /positions REST endpoint on every quote cycle. The
    // 2026-05-21 SILVER incidents (XAG short 6 vs cap 5; XAG short 3 vs cap 2)
    // proved this is unsafe: Architect's /positions REST can lag /orders by
    // several seconds. A SELL leg can fill, vanish from open-orders, the
    // strategy issues a cancel, the venue replies 404 (benign), the cycle
    // refreshes /positions which still shows the pre-fill state, the cycle
    // concludes "flat" and places a fresh SELL — when the fresh SELL also
    // fills, NetPo blows past max_position.
    //
    // New policy:
    //   * Initial seed: `syncInventoryFromExchangePortfolio()` is called ONCE
    //     in `initialize()`, before any orders are placed (see
    //     `MakeMarketStrategy::initialize`). That single REST query
    //     establishes the starting NetPo.
    //   * Updates: only via `position_state_.onAxFill()` invoked from real
    //     fill events delivered by `OrderManager::onOrderFilled` (fed by
    //     `StartupSequence::pollExchangeFills`, default cadence now 1s).
    //   * Multi-stack: when a fill lands, `syncExchangeNetForAllMakeMarketOnSymbol()`
    //     STILL runs to propagate the venue truth to sibling stacks on the
    //     same symbol — that is the only sanctioned post-init REST overwrite,
    //     and it is fill-event-triggered, not cycle-time-triggered.
    //   * Cap correctness: the reconciliation gate immediately below blocks
    //     submits until a fill poll has completed AFTER the most recent
    //     cancel response, so a 404-cancel-that-was-actually-a-fill cannot
    //     hide from the cap-check.
    // =============================================================================

    // Tim reconcile gate: wait for /fills poll after our last cancel before place.
    mmWaitReconcileGateBeforePlace("run_full_mm_quote_cycle");

    // === VENUE-vs-LOCAL RECONCILE (2026-05-22, post SILVER overtrade) =========
    // The fill-poll gate above only guarantees "/fills was polled after our
    // cancel". It does NOT guarantee /fills actually delivered the fills.
    // In the 2026-05-22 00:00 SILVER overtrade, /fills returned 0 fills for
    // ~30s while the venue position grew to LONG 6 against `max_po=2`.
    // Cross-check the local NetPo against /positions REST truth here and
    // refuse to place if they disagree. See `mmReconcileVenueOrAbort` for
    // policy details.
    if (!mmReconcileVenueOrAbort("run_full_mm_quote_cycle")) {
        // Cancel-first invariant: if this re-add cycle aborts at the venue reconcile
        // AFTER the stale legs were already cancelled (cancel-first drain stamped
        // mm_tt_cancel_sent_ms_), we simply do not quote this cycle — the legs are
        // already flat on the venue, which is the intended safe state. Log it once and
        // clear the MM_TT stamps so the next cycle starts clean.
        const std::int64_t tt_cancel_ms =
            mm_tt_cancel_sent_ms_.load(std::memory_order_relaxed);
        if (tt_cancel_ms > 0 && utils::Logger::isInitialized()) {
            const std::int64_t tt_feed_ms =
                mm_tt_feed_arrival_ms_.load(std::memory_order_relaxed);
            Main().logger()->warn(
                "[MM_CYCLE_ABORT_AFTER_CANCEL] strategy={} sym={} reason={} "
                "feed_arrival_ms={} cancel_sent_ms={} — venue reconcile aborted the re-add; "
                "stale legs already cancelled → leaving flat (not quoting this cycle)",
                getName(), mmAxSymbol(), reason, tt_feed_ms, tt_cancel_ms);
        }
        mm_tt_feed_arrival_ms_.store(0, std::memory_order_relaxed);
        mm_tt_cancel_sent_ms_.store(0, std::memory_order_relaxed);
        return;
    }

    const int max_po_cfg = mmEffMaxPositionInt(cfg);
    const long long cap_ll = static_cast<long long>(std::max(1, max_po_cfg));
    const bool instrument_cap_cycle_reason =
        (reason == "instrument_cap_increased") || (reason == "instrument_cap_decreased") ||
        (reason == "instrument_limits_update");
    if (instrument_cap_cycle_reason) {
        (void)mmRefreshNetPositionFromPortfolioCache("instrument_cap_cycle");
    }
    // CONCURRENCY (fix C4): stable NetPo snapshot for Tim net-only side gates (`shouldQuoteSide`)
    // and post-cancel `refreshCapGatesBeforePlace` — without sync a concurrent processFill
    // onAxFill on the OM thread could race this read.
    long long net_po_rounded = 0;
    {
        std::lock_guard<std::mutex> ps_lk(position_state_mutex_);
        // ENFORCE reads book effective(); off/shadow read legacy net (identical value).
        net_po_rounded = mmEffectiveNetPoRounded();
    }
    // fill_requote*: always run full two-sided desk cancel-replace (post-fill widen + skew), even when
    // theo/timer churn has exhausted max_reload_cycles — otherwise mmTryApplyDeskInPlaceModifyOnly bails and
    // the generic desk submit path is intentionally disabled.
    const bool reload_reduce_only_active =
        (max_reload_cfg > 0 && mm_reload_cycles_used_ >= max_reload_cfg) && !fill_requote;

    bool want_bid_pre = shouldQuoteSide(Side::BUY, fill_requote);
    bool want_ask_pre = shouldQuoteSide(Side::SELL, fill_requote);
    const bool inventory_inside =
        (std::llabs(net_po_rounded) < cap_ll) && !reload_reduce_only_active;
    // Reduce-only state observability — logs ENTERED/EXITED on transitions (no-op when unchanged).
    // Sources `net_po_rounded` from `position_state_.net_position_qty`, which post-init mutates
    // only via real fill events (Tim's 2026-05-21 spec). No REST overwrite here.
    noteReduceOnlyState(net_po_rounded, cap_ll);

    // Never skip theo_move / mm_orders_resume / instrument cap refresh: resting bid+ask must be
    // cancel/replaced when theo moves enough (see min_theo_move_ticks), after mm_orders_enabled,
    // or when product max_position changes and all stacks must requote.
    const bool skip_if_two_sided =
        !fill_requote && !instrument_cap_cycle_reason && (reason != "mm_startup") &&
        (reason != "theo_move") && (reason != "mm_orders_resume") && (reason != "timer_repeg_one_leg");
    if (skip_if_two_sided && inventory_inside && want_bid_pre && want_ask_pre &&
        mmOrderManagerHasActiveBidAndAsk()) {
        Main().logger()->debug(
            "[STRATEGY:{}] MM full cycle skipped (active LIMIT bid+ask in OrderManager) reason={}",
            getName(),
            reason);
        mmLogMmGateThrottled(
            "skip_two_sided_order_manager_has_bid_and_ask",
            reason,
            "not theo_move/mm_startup/mm_orders_resume/timer_repeg_one_leg/instrument_cap — cycle suppressed");
        return;
    }

    const bool force_full_cancel = (reason == "mm_startup") || (reason == "mm_reload_limit_reached") ||
        (reason.rfind("mm_ax_ws_disconnected", 0) == 0);
    // `mm_req_*` desk stacks: per-stack gateway cancel-replace (below), never
    // `mmCancelAllExchangeAndResetLocal` on normal theo/timer — that clears `mm_desk_active_`.
    // NOTE: `desk_no_cpp_autoplace` must stay false for desk; the gate at the bottom of this
    // function returns before the place loop when true+two-sided, which would freeze all stacks.
    const bool desk_managed_stack = mmIsDeskManaged();
    const bool desk_no_cpp_autoplace = false;
    const bool inventory_driven_full_cancel = inventory_inside && !desk_managed_stack;
    const bool force_cancel_desk_safe =
        force_full_cancel &&
        !(desk_managed_stack && !fill_requote &&
          (reason == "mm_startup" || mmReasonAllowsDeskInPlaceModify(reason)));
    const bool use_full_cancel = force_cancel_desk_safe || inventory_driven_full_cancel;

    if (use_full_cancel) {
        mmCancelAllExchangeAndResetLocal(reason);
        // Recompute want_* after cancel-all + resetMmSessionFillTracking (net vs max only in shouldQuoteSide).
        want_bid_pre = shouldQuoteSide(Side::BUY, fill_requote);
        want_ask_pre = shouldQuoteSide(Side::SELL, fill_requote);
    } else {
        const std::string reduce_reason =
            reload_reduce_only_active ? std::string("reload_limit") : std::string("max_position");
        // |net| >= cap → reduce-only. Eagerly cancel any live grow-side order so we converge on
        // a single reduce-side quote per stack within one cycle (rather than waiting for the
        // next theo move / timer reconcile to pick this up).
        const std::string product_sym = mmAxSymbol();
        if (!want_bid_pre) {
            if (bid_order_id_ != 0 || mmTrackedSideHasActiveLimitOrder(Side::BUY)) {
                Main().logger()->info(
                    "[STRATEGY:{}] MM_REDUCE_ONLY cancelling grow-side BUY (reason={} net_po={} cap={} reload={}/{}) — only SELLs allowed",
                    getName(), reduce_reason, net_po_rounded, cap_ll, mm_reload_cycles_used_, max_reload_cfg);
            }
            (void)mmCancelTrackedLegAllStacksOnAx(product_sym, Side::BUY, this);
        }
        if (!want_ask_pre) {
            if (ask_order_id_ != 0 || mmTrackedSideHasActiveLimitOrder(Side::SELL)) {
                Main().logger()->info(
                    "[STRATEGY:{}] MM_REDUCE_ONLY cancelling grow-side SELL (reason={} net_po={} cap={} reload={}/{}) — only BUYs allowed",
                    getName(), reduce_reason, net_po_rounded, cap_ll, mm_reload_cycles_used_, max_reload_cfg);
            }
            (void)mmCancelTrackedLegAllStacksOnAx(product_sym, Side::SELL, this);
        }
    }

    const int eff_spread_pair = mmEffBidSpreadTicks(cfg);
    const int spread_bid_base = eff_spread_pair;
    const int spread_ask_base = eff_spread_pair;
    const int extra_bid = fill_extra_bid_ticks_;
    const int extra_ask = fill_extra_ask_ticks_;
    fill_extra_bid_ticks_ = 0;
    fill_extra_ask_ticks_ = 0;
    const int resting = cfg.getMarketMakerRestingDepthExtraTicks();
    // Spread inputs flow through the pricing pipeline as basis points of pricing mid (1 bp = 0.0001).
    // Names previously said "_ticks" but the values came from `mmEffBidSpreadTicks` which returns
    // bps (manual-stack `width_bps` or config `getMarketMakerBidSpreadTicks`, both bps despite the
    // legacy method/config names). Same units feed every downstream function for every instrument.
    int bid_spread_bps = spread_bid_base;
    int ask_spread_bps = spread_ask_base;

    const double basis = cfg.getMarketMakerBasis();
    double quote_tick = mmEffResolvedQuoteTick(cfg);
    if (!(quote_tick > 0.0) || !std::isfinite(quote_tick)) {
        mmLogMmGateThrottled(
            "unresolved_quote_tick",
            reason,
            "quote_tick unresolved for ax=" + mmAxSymbol() +
                " — need ax_gateway_instruments_catalog or merged leg tick_size");
        return;
    }

    const int adjust_po_cfg = std::max(1, mmEffAdjustPositionInt(cfg));
    const int adjust_ticks_cfg = mmEffAdjustTicksInt(cfg);
    // Skew (symmetric long/short, 2026-05-21):
    //   skew_tick_units = floor(|NetPo| / adjust_position) * sign(NetPo) * adjust_ticks
    // when |NetPo| < max_position. See computeInventorySkewedRawBidAsk() comment block for the
    // rationale: a plain `floor(NetPo/adjust_po)` rounds toward −∞ and therefore makes any
    // negative inventory below adjust_po skew immediately (e.g. NetPo=-1, adjust_po=50000
    // → -1*adjust_ticks), while the symmetric positive case stays at zero.
    double skew_tick_units = 0.0;
    if (std::llabs(net_po_rounded) < cap_ll) {
        const long long abs_net_po = std::llabs(net_po_rounded);
        const int net_po_sign = (net_po_rounded > 0) ? 1 : (net_po_rounded < 0 ? -1 : 0);
        skew_tick_units =
            std::floor(static_cast<double>(abs_net_po) / static_cast<double>(adjust_po_cfg)) *
            static_cast<double>(net_po_sign) *
            static_cast<double>(adjust_ticks_cfg);
    }

    const Price skew_price = skew_tick_units * quote_tick;
    const Price adjusted_theo = mmApplyPricerSnapshotTransform(theo_midpoint) + basis;
    mmCapSpreadBpsToDeskPlacedLadder(adjusted_theo, bid_spread_bps, ask_spread_bps);
    Price raw_bid = 0.0;
    Price raw_ask = 0.0;
    computeInventorySkewedRawBidAsk(adjusted_theo, bid_spread_bps, ask_spread_bps, quote_tick, raw_bid, raw_ask);
    const int extra_bid_ticks = resting + extra_bid;
    const int extra_ask_ticks = resting + extra_ask;
    if (extra_bid_ticks > 0) {
        raw_bid -= static_cast<double>(extra_bid_ticks) * quote_tick;
    }
    if (extra_ask_ticks > 0) {
        raw_ask += static_cast<double>(extra_ask_ticks) * quote_tick;
    }
    Price bid = 0.0;
    Price ask = 0.0;
    finalizeMmPairOnTickGrid(raw_bid, raw_ask, bid_spread_bps, ask_spread_bps, quote_tick, bid, ask);
    if (bid <= 0 || ask <= 0) {
        mmLogMmGateThrottled(
            "invalid_computed_bid_ask",
            reason,
            "bid=" + std::to_string(bid) + " ask=" + std::to_string(ask) + " theo_mid=" +
                std::to_string(theo_midpoint) + " quote_tick=" + std::to_string(quote_tick));
        return;
    }

    // === DRIFT-COALESCE NO-OP GUARD (2026-05-07 redundant-cycle fix) ====================
    // Symptom: every long mover-routed `theo_move` is followed by a SECOND `theo_move`
    // ~200ms later that cancel-replaces both legs at the EXACT SAME PRICES already on the
    // venue (live evidence: log 18:45:48–18:45:54 EURUSD-PERP placed bid=1.165500
    // ask=1.189100 in move N, then 200ms later move N+1 cancel-replaced to bid=1.165500
    // ask=1.189100 — pure waste, ~2.5s of mover time + 4 REST round trips).
    //
    // Why it happens:
    //   - Feed thread sees drift while move N is running (last_bid_price_ holds the
    //     pre-move N price for the entire 2-3s window between cancel start and place
    //     finish), enqueues `theo_move`.
    //   - `enqueueQuoteCycleOnMover` coalesces, but the pending flag is reset at the
    //     START of the next drain — so as soon as move N's drain begins running,
    //     subsequent feed updates flip the pending flag back on and queue move N+1.
    //   - By the time the mover gets to move N+1, last_bid_price_ has been updated
    //     by move N's place. The fresh theo + skew now rounds to the SAME tick-grid
    //     prices already on the venue. But runFullMmQuoteCycle has no idempotency
    //     check at this layer — it cancels both legs and places at the same prices.
    //
    // Fix: after computing the final tick-grid bid/ask, if both legs are alive locally
    // AND both new prices match `last_*_price_` within a tick-tolerance, this cycle is
    // a no-op. Update `last_theo_` (so the next drift-gate uses fresh theo) and return
    // without touching either leg.
    //
    // Reasons this guard skips: theo-driven repricing (theo_move, timer_*, mover_drain).
    // Reasons it does NOT skip (always proceed):
    //   - fill_requote*  (post-fill widen + skew change)
    //   - mm_pair_enforce / mm_startup / mm_orders_resume / mm_reload_limit_reached
    //     (recovery paths that need to re-place even at the same prices)
    //   - any reason where bid_order_id_ or ask_order_id_ is 0 (we don't have a live
    //     leg to compare against — must place).
    {
        const bool drift_driven_reason =
            (reason.rfind("theo_move", 0) == 0) ||
            (reason.rfind("timer_", 0) == 0) ||
            (reason == "mover_drain");
        const bool both_legs_alive =
            (bid_order_id_ != 0 && ask_order_id_ != 0 &&
             last_bid_price_ > 0.0 && last_ask_price_ > 0.0);
        if (drift_driven_reason && both_legs_alive && !fill_requote) {
            const double match_eps = std::max(1e-9, quote_tick * 0.5);
            const bool bid_unchanged = std::fabs(bid - last_bid_price_) < match_eps;
            const bool ask_unchanged = std::fabs(ask - last_ask_price_) < match_eps;
            if (bid_unchanged && ask_unchanged) {
                if (utils::Logger::isInitialized()) {
                    Main().logger()->info(
                        "[STRATEGY:{}] runFullMmQuoteCycle: drift-coalesce no-op reason={} "
                        "sym={} bid={:.6f} ask={:.6f} (=last_bid/last_ask) — feed-burst "
                        "during prior move queued a redundant theo_move, skip cancel+place",
                        getName(), reason, mmAxSymbol(), bid, ask);
                }
                last_theo_ = theo_midpoint;
                return;
            }
        }
    }

    bool want_bid = want_bid_pre;
    bool want_ask = want_ask_pre;
    if (!first_quote_logged_) {
        const std::string max_po_source =
            (mm_instrument_max_position_ > 0)
                ? (mm_max_position_source_.empty() ? std::string("instrument") : mm_max_position_source_)
                : std::string("global");
        Main().logger()->info(
            "[FIRST_QUOTE] stack={} instrument={} net_po={} max_po={} source={} want_bid={} want_ask={}",
            getName(),
            mmAxSymbol(),
            net_po_rounded,
            max_po_cfg,
            max_po_source,
            want_bid ? "Y" : "N",
            want_ask ? "Y" : "N");
        first_quote_logged_ = true;
    }

    const int order_step = mmEffOrderSizeStepInt(cfg);
    // Same step-rounded size on each active leg; max_position is side gating only (never asymmetric headroom).
    Quantity leg_sz = mmRoundQtyToStepDown(quantity, order_step);
    if (leg_sz <= 0.0 && quantity > 0.0) {
        // Last resort if step/config mismatch still floors to 0 (must never strand desk quotes).
        leg_sz = mmRoundQtyToStepDown(quantity, 1);
    }
    const Quantity qty_bid = want_bid ? leg_sz : 0.0;
    const Quantity qty_ask = want_ask ? leg_sz : 0.0;

    bool do_bid = want_bid;
    bool do_ask = want_ask;
    if (qty_bid <= 0.0) {
        do_bid = false;
    }
    if (qty_ask <= 0.0) {
        do_ask = false;
    }

    resetMmSessionFillTracking(qty_bid, qty_ask);

    std::string bbo_skip_bid_detail;
    std::string bbo_skip_ask_detail;
    const bool need_bbo = cfg.getMarketMakerValidateVsMarket() || cfg.getMarketMakerNeverInsideSpread();
    if (need_bbo) {
        const std::string ax_sym = mmAxSymbol();
        Price mkt_bid = 0.0;
        Price mkt_ask = 0.0;
        if (tryGetAxMarketBbo(ax_sym, mkt_bid, mkt_ask)) {
            QOValidation v = validateQuoteOrders(bid, ask, mkt_bid, mkt_ask, cfg.getMarketMakerValidateVsMarket(),
                                                   cfg.getMarketMakerNeverInsideSpread());
            if (!v.valid) {
                ++validation_failures_;
                Main().logger()->warn("[STRATEGY:{}] MM quote validation: {}", getName(), v.reason);
            }
            if (v.spread_inverted) {
                do_bid = false;
                do_ask = false;
                bbo_skip_bid_detail = v.reason;
                bbo_skip_ask_detail = v.reason;
            } else {
                if (v.bid_would_cross || v.bid_inside_spread) {
                    do_bid = false;
                    bbo_skip_bid_detail = v.reason;
                }
                if (v.ask_would_cross || v.ask_inside_spread) {
                    do_ask = false;
                    bbo_skip_ask_detail = v.reason;
                }
            }
        }
    }

    if (want_bid && !do_bid) {
        const std::string why = (qty_bid <= 0.0) ? std::string("qty_bid<=0")
                                  : (!bbo_skip_bid_detail.empty()) ? bbo_skip_bid_detail
                                                                 : std::string("side_gate_or_other");
        Main().logger()->warn(
            "[STRATEGY:{}] MM_PLACE_BID_SKIPPED cycle={} why={} qty_bid={} bid_px={:.6f} net_po={} max_po={} "
            "want_bid={} want_ask={} do_ask={} | qty_int={} order_step={} leg_sz={}",
            getName(),
            reason,
            why,
            qty_bid,
            bid,
            net_po_rounded,
            max_po_cfg,
            want_bid ? 1 : 0,
            want_ask ? 1 : 0,
            do_ask ? 1 : 0,
            qty_int,
            order_step,
            leg_sz);
    }
    if (want_ask && !do_ask) {
        const std::string why = (qty_ask <= 0.0) ? std::string("qty_ask<=0")
                                   : (!bbo_skip_ask_detail.empty()) ? bbo_skip_ask_detail
                                                                  : std::string("side_gate_or_other");
        Main().logger()->warn(
            "[STRATEGY:{}] MM_PLACE_ASK_SKIPPED cycle={} why={} qty_ask={} ask_px={:.6f} net_po={} max_po={} "
            "want_ask={} want_bid={} do_bid={} | qty_int={} order_step={} leg_sz={}",
            getName(),
            reason,
            why,
            qty_ask,
            ask,
            net_po_rounded,
            max_po_cfg,
            want_ask ? 1 : 0,
            want_bid ? 1 : 0,
            do_bid ? 1 : 0,
            qty_int,
            order_step,
            leg_sz);
    }

    if (mmTryApplyDeskInPlaceModifyOnly(
            reason,
            theo_midpoint,
            adjusted_theo,
            bid,
            ask,
            qty_bid,
            qty_ask,
            do_bid,
            do_ask,
            want_bid,
            want_ask,
            reload_reduce_only_active,
            bid_spread_bps,
            ask_spread_bps,
            quote_tick,
            net_po_rounded,
            max_po_cfg,
            adjust_po_cfg,
            adjust_ticks_cfg,
            skew_tick_units,
            skew_price,
            raw_bid,
            raw_ask)) {
        return;
    }
    if (desk_no_cpp_autoplace && want_bid && want_ask) {
        return;
    }

    const bool one_sided_inventory_mode = !use_full_cancel;
    auto& om = orders::OrderManager::getInstance();
    const Price px_eps = quote_tick * 0.25;

    if (std::llabs(net_po_rounded) >= cap_ll) {
        if (!mmEnsureAtCapReduceSideVenueClean(
                symbol, net_po_rounded, cap_ll, "pre_place_at_cap", this)) {
            mmLogMmGateThrottled(
                "pre_place_at_cap_venue_not_clean",
                reason,
                "excess reduce-side rows on venue — skip place this cycle");
            return;
        }
    }

    bool placed_bid = false;
    bool placed_ask = false;

    const bool force_pair_replace =
        (reason.rfind("theo_move", 0) == 0) || (reason == "timer_update") ||
        (reason == "instrument_cap_increased");

    auto refreshCapGatesBeforePlace = [&]() {
        {
            std::lock_guard<std::mutex> ps_lk(position_state_mutex_);
            // ENFORCE reads book effective(); off/shadow read legacy net (identical value).
            net_po_rounded = mmEffectiveNetPoRounded();
        }
        noteReduceOnlyState(net_po_rounded, cap_ll);
        want_bid = shouldQuoteSide(Side::BUY, fill_requote);
        want_ask = shouldQuoteSide(Side::SELL, fill_requote);
        const Quantity leg_sz_refresh = mmRoundQtyToStepDown(quantity, order_step);
        do_bid = want_bid && leg_sz_refresh > 0.0;
        do_ask = want_ask && leg_sz_refresh > 0.0;
    };

    if (do_bid) {
        bool kept_resting = false;
        if (!force_pair_replace && one_sided_inventory_mode && bid_order_id_ != 0) {
            const auto op = om.getOrder(bid_order_id_);
            if (op && op->isActive() && op->type == OrderType::LIMIT && std::fabs(op->price - bid) <= px_eps) {
                kept_resting = true;
                placed_bid = true;
            }
        }
        if (!kept_resting) {
            // Never submit without a verified cancel (or vacuous "nothing to cancel") for this stack/side.
            const int c_buy = mmCancelTrackedLegThisStackGateway(Side::BUY);
            mm_last_cycle_activity_ms_.store(
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::steady_clock::now().time_since_epoch()).count(),
                std::memory_order_relaxed);
            if (c_buy != 1) {
                Main().logger()->warn(
                    "[STRATEGY:{}] MM_PLACE_BID_BLOCKED: venue cancel incomplete reason={} ax={} bid_id={}",
                    getName(),
                    reason,
                    symbol,
                    static_cast<long long>(bid_order_id_));
            } else {
                bid_order_id_ = 0;
                last_bid_price_ = 0.0;

                refreshCapGatesBeforePlace();
                if (!do_bid) {
                    Main().logger()->info(
                        "[STRATEGY:{}] MM_PLACE_BID_SKIPPED_POST_CANCEL reason={} net_po={} max_po={} "
                        "— at cap after cancel (reduce-only)",
                        getName(),
                        reason,
                        net_po_rounded,
                        max_po_cfg);
                } else {
                auto req = OrderRequest::limit_buy(symbol, qty_bid, bid, "mm_bid");
                auto cid = mmPlaceReservedLeg(Side::BUY, bid, qty_bid, req, cap_ll, reason);
                mm_last_cycle_activity_ms_.store(
                    std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::steady_clock::now().time_since_epoch()).count(),
                    std::memory_order_relaxed);
                if (!cid.empty()) {
                    last_bid_price_ = bid;
                    ++quotes_sent_;
                    placed_bid = true;
                }
                }
            }
        }
    }
    if (do_ask) {
        bool kept_resting = false;
        if (!force_pair_replace && one_sided_inventory_mode && ask_order_id_ != 0) {
            const auto op = om.getOrder(ask_order_id_);
            if (op && op->isActive() && op->type == OrderType::LIMIT && std::fabs(op->price - ask) <= px_eps) {
                kept_resting = true;
                placed_ask = true;
            }
        }
        if (!kept_resting) {
            const int c_sell = mmCancelTrackedLegThisStackGateway(Side::SELL);
            mm_last_cycle_activity_ms_.store(
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::steady_clock::now().time_since_epoch()).count(),
                std::memory_order_relaxed);
            if (c_sell != 1) {
                Main().logger()->warn(
                    "[STRATEGY:{}] MM_PLACE_ASK_BLOCKED: venue cancel incomplete reason={} ax={} ask_id={}",
                    getName(),
                    reason,
                    symbol,
                    static_cast<long long>(ask_order_id_));
            } else {
                ask_order_id_ = 0;
                last_ask_price_ = 0.0;

                refreshCapGatesBeforePlace();
                if (!do_ask) {
                    Main().logger()->info(
                        "[STRATEGY:{}] MM_PLACE_ASK_SKIPPED_POST_CANCEL reason={} net_po={} max_po={} "
                        "— at cap after cancel (reduce-only)",
                        getName(),
                        reason,
                        net_po_rounded,
                        max_po_cfg);
                } else {
                auto req = OrderRequest::limit_sell(symbol, qty_ask, ask, "mm_ask");
                auto cid = mmPlaceReservedLeg(Side::SELL, ask, qty_ask, req, cap_ll, reason);
                mm_last_cycle_activity_ms_.store(
                    std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::steady_clock::now().time_since_epoch()).count(),
                    std::memory_order_relaxed);
                if (!cid.empty()) {
                    last_ask_price_ = ask;
                    ++quotes_sent_;
                    placed_ask = true;
                }
                }
            }
        }
    }

    // NOTE (accept-ordering race fix): mm_pending_accepts_ is now reserved BEFORE
    // each submit inside mmPlaceReservedLeg (and balanced by the inline/async
    // accept), so there is no post-submit counter bump here anymore.

    // MM_TT: one line per cancel-first requote when the fresh pair leaves the box, so Tim
    // can read tick_to_cancel and tick_to_place end-to-end. Only emitted when this cycle
    // was the re-add following a cancel-first drain (mm_tt_feed_arrival_ms_ still set).
    // LOG-ONLY; clears the stamps so the next requote starts clean.
    {
        const std::int64_t tt_feed_ms =
            mm_tt_feed_arrival_ms_.load(std::memory_order_relaxed);
        if (tt_feed_ms > 0 && (placed_bid || placed_ask) && utils::Logger::isInitialized()) {
            const std::int64_t tt_place_ms = mmSteadyMillis();
            const std::int64_t tt_cancel_ms =
                mm_tt_cancel_sent_ms_.load(std::memory_order_relaxed);
            Main().logger()->info(
                "[MM_TT] sym={} stack={} phase=place_sent reason={} placed_bid={} placed_ask={} "
                "feed_arrival_ms={} cancel_sent_ms={} place_sent_ms={} "
                "tick_to_cancel_ms={} tick_to_place_ms={} cancel_to_place_ms={}",
                mmAxSymbol(),
                mmOrderRequestIdOrLegacy(),
                reason,
                placed_bid ? 1 : 0,
                placed_ask ? 1 : 0,
                tt_feed_ms,
                tt_cancel_ms,
                tt_place_ms,
                (tt_cancel_ms > 0) ? (tt_cancel_ms - tt_feed_ms) : -1,
                tt_place_ms - tt_feed_ms,
                (tt_cancel_ms > 0) ? (tt_place_ms - tt_cancel_ms) : -1);
        }
        mm_tt_feed_arrival_ms_.store(0, std::memory_order_relaxed);
        mm_tt_cancel_sent_ms_.store(0, std::memory_order_relaxed);
    }

    printMmVerifyStdout(
        reason,
        theo_midpoint,
        adjusted_theo,
        bid_spread_bps,
        ask_spread_bps,
        quote_tick,
        net_po_rounded,
        max_po_cfg,
        adjust_po_cfg,
        adjust_ticks_cfg,
        skew_tick_units,
        skew_price,
        raw_bid,
        raw_ask,
        bid,
        ask,
        qty_bid,
        qty_ask,
        want_bid,
        want_ask,
        placed_bid,
        placed_ask);

    last_theo_ = theo_midpoint;
    last_theo_requote_anchor_ = theo_midpoint;
    last_theo_requote_anchor_inited_ = true;

    if (mmIsDeskManaged() && std::llabs(net_po_rounded) >= cap_ll) {
        mmMaybeConvergeAtCapAfterReplaceCycle(this, "post_replace_at_cap");
    }
}

// =============================================================================
// MmOrderMover plumbing — SINGLE-threaded GLOBAL executor for cancel/replace/place
// =============================================================================
//
// Why this exists (recap of the 2026-05-07 freeze investigation, log 14:48):
//   The feed-dispatch thread (1495887) called runFullMmQuoteCycle synchronously
//   on every drift-met tick. cancel + place serialized on `RestClient::curl_mutex_`
//   for ~230ms each, so a single move took 0.6–4 s of feed-thread blocking. Three
//   other threads (mm_pair_enforce 1496091, OM_GATEWAY reconciler 1495727,
//   desk_reseed_cancel from orders.json reconcile) concurrently mutated the same
//   stack's tracked vectors, producing zombie ADOPTED rows, double-cancel races,
//   and the "It froze again" symptom.
//
// Contract v2 (per user spec, 2026-05-07 evening — "remove multithreading"):
//   ALL cancel/replace/place runs on ONE global worker thread owned by MmOrderMover.
//   Different AX symbols no longer progress in parallel: a cancel-replace on
//   EURUSD-PERP completes before any work on JPYUSD-PERP starts. Latency goes up
//   under burst, correctness is absolute. The feed/timer/OM-event threads only set
//   a coalescing flag and post a drain lambda. The lambda re-reads the latest theo
//   on the mover thread (so coalesced requests still produce a fresh quote) and
//   calls the existing `runFullMmQuoteCycle` synchronously there.
//
// The `enqueueForAx(ax, ...)` API is preserved for call-site compatibility — the
// `ax` argument is now used only as a diagnostic label inside MmOrderMover (work
// is NOT routed by AX any more). All call sites here pass `mmAxSymbol()` for
// readable logs.
//
// Coalescing guarantees:
//   - feed thread can fire `enqueueQuoteCycleOnMover` 100 times in a row;
//     only one drain lambda is queued at a time.
//   - the latest-known reason is stored in `mm_quote_cycle_pending_reason_`
//     so the eventual log line credits the most recent caller (e.g. "theo_move"
//     instead of stale "timer_update").
//
// Lifetime safety:
//   - the lambda captures the strategy NAME, not `this`. A name lookup via
//     StrategyManager::getStrategy() returns nullptr if the strategy was torn
//     down (see desk strategy lifecycle in mmTearDownByStackId), so a stale
//     queued lambda becomes a no-op instead of UAF.

void MakeMarketStrategy::enqueueQuoteCycleOnMover(const std::string& reason) {
    {
        std::lock_guard<std::mutex> lk(mm_quote_cycle_pending_reason_mu_);
        mm_quote_cycle_pending_reason_ = reason;
    }
    const bool was_pending = mm_quote_cycle_pending_.exchange(true);
    if (was_pending) {
        return;
    }
    const std::string name = getName();
    const std::string ax = mmAxSymbol();
    if (ax.empty()) {
        // No AX yet (early init) — drop. The next live tick will retry.
        mm_quote_cycle_pending_.store(false);
        return;
    }
    MmOrderMover::getInstance().enqueueForAx(ax, [name]() {
        auto sp = StrategyManager::getInstance().getStrategy(name);
        auto mm = std::dynamic_pointer_cast<MakeMarketStrategy>(sp);
        if (!mm) {
            return;
        }
        mm->mmDrainPendingQuoteCycle();
    });
}

void MakeMarketStrategy::mmDrainPendingQuoteCycle() {
    if (!mm_quote_cycle_pending_.exchange(false)) {
        return;
    }
    std::string reason;
    {
        std::lock_guard<std::mutex> lk(mm_quote_cycle_pending_reason_mu_);
        reason = std::move(mm_quote_cycle_pending_reason_);
        mm_quote_cycle_pending_reason_.clear();
    }
    if (reason.empty()) {
        reason = "mover_drain";
    }
    if (!isRunning() || !isEnabled()) {
        return;
    }
    if (!mmWorkerMayProceed("quote_cycle_drain")) {
        return;
    }
    // === Proof instrumentation (per-mover-job venue-call counter) ================
    // Begin counting venue GET/POST/cancel issued on THIS thread for the duration
    // of this mover job. The RAII guard logs [MM_JOB_VENUE_CALLS] and ends counting
    // on EVERY exit path below (cancel-first return, no-theo return, normal place).
    // In enforce a theo-move drain must show gets=0 (cancel-drain: cancels=2,
    // place-drain: posts=2); gets>0 in enforce is a regression → WARN loudly.
    core::mmJobVenueBegin();
    struct MmJobVenueScope {
        MakeMarketStrategy* self;
        ~MmJobVenueScope() {
            const auto c = core::mmJobVenueSnapshot();
            core::mmJobVenueEnd();
            if (!utils::Logger::isInitialized()) return;
            const bool enforce = self->mmPbEnforce();
            // Only emit when the job actually touched the venue (avoids spamming a
            // line for every trivial no-op drain); always surface an enforce regression.
            if (c.gets == 0 && c.posts == 0 && c.cancels == 0) return;
            if (enforce && c.gets > 0) {
                Main().logger()->warn(
                    "[MM_JOB_VENUE_CALLS] sym={} gets={} posts={} cancels={} "
                    "— ENFORCE REGRESSION: venue GET on mover thread (expected gets=0)",
                    self->mmAxSymbol(), c.gets, c.posts, c.cancels);
            } else {
                Main().logger()->info(
                    "[MM_JOB_VENUE_CALLS] sym={} gets={} posts={} cancels={}",
                    self->mmAxSymbol(), c.gets, c.posts, c.cancels);
            }
        }
    } mm_job_venue_scope{this};
    // Venue-as-truth prelude: drop any tracked rows the venue no longer reports
    // BEFORE we compute targets / decide cancels. Otherwise a zombie ADOPTED row
    // (e.g. cancelled out-of-band, fill not yet propagated to OM, or a previous
    // cancel REST returned 200 but tracked vector wasn't evicted) can drive the
    // cycle into a wrong-cancel of a sibling stack's leg.
    mmReconcileTrackedAgainstVenue();
    auto theo_opt = mmReadTransformedTheo();
    if (!theo_opt.has_value() || !std::isfinite(*theo_opt) || *theo_opt <= 0.0) {
        // No theo yet — re-arm the flag so the next drain attempt picks it up.
        // (Don't recurse-enqueue here; the next feed/timer tick will do so.)
        return;
    }
    // === MWR volatility breaker: 1 Hz sample (MOVER-THREAD-ONLY; no lock / no atomic) ===
    // Cheap per-tick gate BEFORE any work so a disabled breaker costs ~nothing. The enabled/
    // window decision uses the params the gate (runFullMmQuoteCycle, below) cached on the
    // previous cycle — so we never re-parse config on a feed-driven drain. One-cycle warmup
    // (first ever drain has no cached params yet) is harmless.
    if (mm_mwr_cfg_size_ticks_ > 0 && mm_mwr_cfg_window_sec_ > 0) {
        const auto mwr_now = std::chrono::steady_clock::now();
        if (!mm_mwr_have_last_sample_ ||
            (mwr_now - mm_mwr_last_sample_ts_) >= std::chrono::milliseconds(1000)) {
            mm_mwr_have_last_sample_ = true;
            mm_mwr_last_sample_ts_ = mwr_now;
            mm_mwr_.push(mwr_now, static_cast<double>(*theo_opt), mm_mwr_cfg_window_sec_);
        }
    } else if (!mm_mwr_.empty()) {
        // Disabled at runtime → drop any stale window so a later re-enable starts clean.
        mm_mwr_.clear();
    }
    // === CANCEL-FIRST theo-move (client-approved 2026-07-03) ===================
    // For a desk-managed drift-driven requote, send the stale-leg cancels IMMEDIATELY
    // via the existing cancel-first path (processTrackedOrdersTheoMove → mover REST
    // cancel) with NO reconcile-gate (mmWaitReconcileGateBeforePlace) / venue-reconcile
    // (mmReconcileVenueOrAbort) / NetPo read before the cancel. The fresh pair is
    // re-added AFTER the cancel settles: on_cancel (TheoMove intent) re-enqueues
    // "theo_move" → this drain → processTrackedOrdersTheoMove now finds no ADOPTED legs
    // (both CANCELLED) and declines → runFullMmQuoteCycle places the pair with the
    // gate/reconcile/NetPo checks now correctly gating ONLY the re-add.
    //
    // This is the exact mechanism the timer path (timerReconcilePairQuotes →
    // processTrackedOrdersTheoMove "timer_update") already uses.
    //
    // CLIENT DIRECTIVE (2026-07-04, Tim/Mrinal): whenever the live theo has moved fast
    // enough to require a requote, the stale-leg cancel MUST leave the box FIRST — before
    // any inventory sync / reconcile-gate / venue-reconcile / NetPo read — in EVERY
    // scenario, NO MATTER WHAT enqueued this drain (theo_move, fill_requote,
    // instrument_cap_increased/decreased, recovery, mover_drain, …). We therefore attempt
    // the cancel-first path for ALL reasons, not just reason=="theo_move".
    //
    // This is safe because processTrackedOrdersTheoMove is fully self-gating on actual
    // drift: it only cancels ADOPTED legs whose target moved >= min_theo_move_ticks vs the
    // resting price, and `reason` is used solely in its log strings (never in a decision).
    // When the price has NOT moved past threshold (structural cap/fill/recovery cycles,
    // place-fresh, pending transitions, reduce-only full-cancel), it returns false and we
    // fall through to runFullMmQuoteCycle UNCHANGED — identical to prior behavior for those
    // cases. The fresh pair is always re-added AFTER the cancel-ack via runFullMmQuoteCycle,
    // which is where the inventory sync / reconcile-gate / NetPo cap check now correctly
    // gate ONLY the re-add (Tim: "delay the re-add, never the cancel").
    if (mmIsDeskManaged()) {
        // MM_TT: feed-driven "theo_move" already stamped mm_tt_feed_arrival_ms_ at
        // decision time in onFeedUpdate (true tick-to-cancel origin). For every OTHER
        // reason that now also runs cancel-first (fill_requote / instrument_cap_* /
        // recovery / mover_drain), there is no earlier feed-arrival event, and the atomic
        // may still hold a STALE stamp from a previous theo_move — which would make the
        // MM_TT tick_to_cancel_ms bogus. Re-stamp to the drain start here so the number is
        // meaningful (drain-start → cancel-sent) for these paths. LOG-ONLY; no gating.
        if (reason != "theo_move") {
            mm_tt_feed_arrival_ms_.store(mmSteadyMillis(), std::memory_order_relaxed);
            mm_tt_cancel_sent_ms_.store(0, std::memory_order_relaxed);
        }
        if (processTrackedOrdersTheoMove(*theo_opt, reason)) {
            // Stale legs cancelled first this drain (regardless of enqueue reason);
            // the re-add follows on the cancel-ack via runFullMmQuoteCycle.
            return;
        }
    }
    runFullMmQuoteCycle(*theo_opt, reason);
}

void MakeMarketStrategy::enqueuePairEnforceOnMover(const std::string& tag) {
    {
        std::lock_guard<std::mutex> lk(mm_pair_enforce_pending_tag_mu_);
        mm_pair_enforce_pending_tag_ = tag;
    }
    const bool was_pending = mm_pair_enforce_pending_.exchange(true);
    if (was_pending) {
        return;
    }
    const std::string name = getName();
    const std::string ax = mmAxSymbol();
    if (ax.empty()) {
        mm_pair_enforce_pending_.store(false);
        return;
    }
    MmOrderMover::getInstance().enqueueForAx(ax, [name]() {
        auto sp = StrategyManager::getInstance().getStrategy(name);
        auto mm = std::dynamic_pointer_cast<MakeMarketStrategy>(sp);
        if (!mm) {
            return;
        }
        mm->mmDrainPendingPairEnforce();
    });
}

void MakeMarketStrategy::mmDrainPendingPairEnforce() {
    if (!mm_pair_enforce_pending_.exchange(false)) {
        return;
    }
    std::string tag;
    {
        std::lock_guard<std::mutex> lk(mm_pair_enforce_pending_tag_mu_);
        tag = std::move(mm_pair_enforce_pending_tag_);
        mm_pair_enforce_pending_tag_.clear();
    }
    if (tag.empty()) {
        tag = "mm_pair_enforce";
    }
    if (!isRunning() || !isEnabled()) {
        return;
    }
    if (!mmWorkerMayProceed("pair_enforce_drain")) {
        return;
    }
    mmReconcileTrackedAgainstVenue();
    // ensureStartupQuotePair runs gates + diagnostic logging then enqueues a
    // quote cycle through this same mover (so downstream cancel/replace runs
    // here too, sequentially with any feed-thread move that snuck in).
    ensureStartupQuotePair(tag);
}

void MakeMarketStrategy::enqueueFillRequoteOnMover() {
    const bool was_pending = mm_fill_requote_pending_.exchange(true);
    if (was_pending) {
        return;
    }
    const std::string name = getName();
    const std::string ax = mmAxSymbol();
    if (ax.empty()) {
        mm_fill_requote_pending_.store(false);
        return;
    }
    MmOrderMover::getInstance().enqueueForAx(ax, [name]() {
        auto sp = StrategyManager::getInstance().getStrategy(name);
        auto mm = std::dynamic_pointer_cast<MakeMarketStrategy>(sp);
        if (!mm) {
            return;
        }
        mm->mmDrainPendingFillRequote();
    });
}

void MakeMarketStrategy::mmDrainPendingFillRequote() {
    if (!mm_fill_requote_pending_.exchange(false)) {
        return;
    }
    if (!isRunning() || !isEnabled()) {
        return;
    }
    if (!mmWorkerMayProceed("fill_requote_drain")) {
        return;
    }
    // Force ground truth: a fill is a state transition where any cached venue snapshot
    // is potentially wrong. Cancel-replace decisions here must be against the live book.
    mmReconcileTrackedAgainstVenue(/*force=*/true);
    tryDeskFillRequoteF4PlaceFreshPair();
}

void MakeMarketStrategy::enqueueDeskTheoMoveAfterCancelAckOnMover(Side side, std::string leg_name) {
    const std::string name = getName();
    const std::string ax = mmAxSymbol();
    if (ax.empty()) {
        return;
    }
    MmOrderMover::getInstance().enqueueForAx(ax,
        [name, side, leg_name = std::move(leg_name)]() {
            auto sp = StrategyManager::getInstance().getStrategy(name);
            auto mm = std::dynamic_pointer_cast<MakeMarketStrategy>(sp);
            if (!mm) {
                return;
            }
            mm->deskTheoMoveAfterCancelAckPlaceOneLeg(side, leg_name.c_str());
        });
}

void MakeMarketStrategy::enqueueRestCancelOnMover(OrderId local_order_id, Side side, std::string reason) {
    if (local_order_id == 0) {
        return;
    }
    const std::string name = getName();
    const std::string ax = mmAxSymbol();
    if (ax.empty()) {
        return;
    }
    MmOrderMover::getInstance().enqueueForAx(ax,
        [name, local_order_id, side, reason = std::move(reason)]() {
            auto sp = StrategyManager::getInstance().getStrategy(name);
            auto mm = std::dynamic_pointer_cast<MakeMarketStrategy>(sp);
            if (!mm) {
                return;
            }
            // mmSendRestCancelByOrderId lives in the anonymous namespace at the top of this
            // TU; calling it from here keeps the existing benign-failure / OM-update logic
            // intact (no behavioural change beyond which thread runs the REST call).
            (void)mmSendRestCancelByOrderId(local_order_id, side, mm->getName(), reason);
        });
}

void MakeMarketStrategy::enqueueRestCancelsConcurrentOnMover(
        std::vector<std::pair<OrderId, Side>> legs, std::string reason) {
    if (legs.empty()) {
        return;
    }
    const std::string name = getName();
    const std::string ax = mmAxSymbol();
    if (ax.empty()) {
        return;
    }
    // ONE mover job for the whole pair — the per-AX FIFO still serialises this job against
    // other order ops on the symbol, but the two cancels inside it fire concurrently.
    MmOrderMover::getInstance().enqueueForAx(ax,
        [name, legs = std::move(legs), reason = std::move(reason)]() {
            auto sp = StrategyManager::getInstance().getStrategy(name);
            auto mm = std::dynamic_pointer_cast<MakeMarketStrategy>(sp);
            if (!mm) {
                return;
            }
            mmSendRestCancelsConcurrentByOrderId(legs, mm->getName(), reason);
        });
}

namespace {

// Self-contained parsers — same shape as core/StartupSequence.cpp helpers
// (parseOpenOrdersBodyRows / openOrdersRowExchangeOid) but those live in a TU-local
// anonymous namespace so we mirror them here for the mover-thread venue-truth prelude.
void mmExtractOpenOrdersRows(const nlohmann::json& root,
                              std::vector<nlohmann::json>& out_rows) {
    out_rows.clear();
    if (root.is_array()) {
        for (const auto& el : root) {
            if (el.is_object()) {
                out_rows.push_back(el);
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
                    out_rows.push_back(el);
                }
            }
            return;
        }
    }
}

std::string mmExtractRowOid(const nlohmann::json& row) {
    static const char* kKeys[] = {"exchange_order_id", "oid", "order_id", "id", "orderId"};
    for (const char* k : kKeys) {
        if (!row.contains(k)) {
            continue;
        }
        const auto& v = row[k];
        if (v.is_string()) {
            std::string s = v.get<std::string>();
            // trim
            while (!s.empty() && (s.front() == ' ' || s.front() == '\t')) s.erase(s.begin());
            while (!s.empty() && (s.back() == ' ' || s.back() == '\t' ||
                                   s.back() == '\r' || s.back() == '\n')) {
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

}  // namespace

// ============================================================================
// Venue source-of-truth guard — stack state machine + venue-OID cache
// ============================================================================

bool MakeMarketStrategy::mmTransitionStackState(MmStackState to, const char* reason) {
    // Monotonic toward DEAD: DEAD is terminal; TEARDOWN may only advance to DEAD.
    // ACTIVE <-> VENUE_ORPHANED is the only reversible edge. CAS loop so concurrent
    // writers (venue poll thread vs desk teardown thread) converge deterministically.
    MmStackState from = mm_stack_state_.load(std::memory_order_acquire);
    for (;;) {
        if (from == to) {
            return false;
        }
        if (from == MmStackState::DEAD) {
            return false;  // terminal
        }
        if (from == MmStackState::TEARDOWN && to != MmStackState::DEAD) {
            return false;  // teardown only advances to DEAD
        }
        if (mm_stack_state_.compare_exchange_weak(from, to, std::memory_order_acq_rel,
                                                  std::memory_order_acquire)) {
            break;
        }
        // `from` reloaded by compare_exchange_weak — re-evaluate guards.
    }
    if (utils::Logger::isInitialized()) {
        Main().logger()->info(
            "[MM_STACK_STATE] stack_id={} old_state={} new_state={} reason={} thread_id={}",
            getName(), mmStackStateName(from), mmStackStateName(to),
            reason ? reason : "", mmThreadIdStr());
    }
    return true;
}

bool MakeMarketStrategy::mmWorkerMayProceed(const char* op) {
    const MmStackState st = mm_stack_state_.load(std::memory_order_acquire);
    if (st == MmStackState::ACTIVE) {
        return true;
    }
    if (utils::Logger::isInitialized()) {
        Main().logger()->warn(
            "[MM_STACK_STATE] worker bail-out stack_id={} state={} operation_skipped={} thread_id={}",
            getName(), mmStackStateName(st), op ? op : "", mmThreadIdStr());
    }
    return false;
}

void MakeMarketStrategy::mmUpdateVenueTruthCache(const std::unordered_set<std::string>& live_oids,
                                                 std::int64_t now_ms) {
    std::lock_guard<std::mutex> lk(mm_venue_truth_mu_);
    mm_venue_truth_oids_ = live_oids;
    mm_venue_truth_updated_ms_ = now_ms;
}

bool MakeMarketStrategy::mmVenueTruthKnowsOid(const std::string& oid, bool& cache_fresh) const {
    constexpr std::int64_t kVenueTruthTtlMs = 3000;  // == kReconcileTtlMs in the poll
    const std::int64_t now_ms = mmSteadyMillis();
    std::lock_guard<std::mutex> lk(mm_venue_truth_mu_);
    cache_fresh = (mm_venue_truth_updated_ms_ > 0) &&
                  (now_ms - mm_venue_truth_updated_ms_ < kVenueTruthTtlMs);
    return mm_venue_truth_oids_.count(oid) > 0;
}

void MakeMarketStrategy::mmTriggerImmediateVenueReconcile() {
    // Reset the throttle so the next mover drain re-polls VENUE_TRUTH unconditionally.
    mm_venue_reconcile_last_ms_.store(0, std::memory_order_release);
}

void MakeMarketStrategy::mmReconcileTrackedAgainstVenue(bool force) {
    // Skip if nothing is tracked locally — fresh strategies, mid-shutdown, etc.
    {
        std::lock_guard<std::mutex> lk(tracked_orders_mutex_);
        if (tracked_bids_.empty() && tracked_asks_.empty()) {
            return;
        }
    }

    const std::string ax = mmAxSymbol();
    if (ax.empty()) {
        return;
    }

    // ENFORCE: the mover issues ZERO venue GETs. Consume the main-loop VenueOrdersCache
    // (populated by mmRefreshVenueOrdersCacheAllEnforced) instead of the inline round-trip
    // below. off/shadow fall through to the legacy GET path — byte-for-byte unchanged.
    if (mmPbEnforce()) {
        mmReconcileTrackedAgainstVenueFromCache(ax);
        return;
    }

    // === THROTTLE (2026-05-07 freeze fix) =================================================
    // Pre-fix: every single mmDrainPendingQuoteCycle started with a synchronous
    // `GET /open-orders` REST round-trip (~150-250ms on a healthy session, much higher
    // when the venue is slow). User log 18:08:22.453→24.469 showed a 2.7s move where ~700ms
    // of the pre-amble was REST that didn't actually need to happen — the previous reconcile
    // only ran 0.5s ago and there were no fills/external cancels in between.
    //
    // Post-fix: cache the result for `kReconcileTtlMs`. Callers that genuinely need ground
    // truth (post-fill requote, paranoid recovery paths) pass `force=true` to bypass the
    // cache. Throttle window of 3s is tight enough that an out-of-band cancel will still be
    // caught within one cycle; loose enough to drop the 230ms tax from back-to-back drift moves.
    constexpr std::int64_t kReconcileTtlMs = 3000;
    const std::int64_t now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                    std::chrono::steady_clock::now().time_since_epoch())
                                    .count();
    if (!force) {
        const std::int64_t last = mm_venue_reconcile_last_ms_.load(std::memory_order_acquire);
        if (last > 0 && now_ms - last < kReconcileTtlMs) {
            // Cached — skip the REST. The previous successful reconcile already evicted
            // any zombies, and 3s is well below typical out-of-band cancel propagation.
            return;
        }
    }

    // We're already on the mover thread, so this REST round-trip is
    // latency we accept (per user spec: correctness over speed).
    auto resp = Main().rest()->getOrdersGatewayRelative("/open-orders", {{"symbol", ax}});
    if (!resp.is_success || resp.status_code < 200 || resp.status_code >= 300) {
        if (utils::Logger::isInitialized()) {
            Main().logger()->warn(
                "[VENUE_TRUTH] strategy={} sym={} GET /open-orders failed: HTTP {} — "
                "skipping tracked-vector reconcile this drain (will retry next drain)",
                getName(), ax, resp.status_code);
        }
        return;
    }

    nlohmann::json root;
    try {
        root = nlohmann::json::parse(resp.body);
    } catch (const std::exception& e) {
        if (utils::Logger::isInitialized()) {
            Main().logger()->warn(
                "[VENUE_TRUTH] strategy={} sym={} parse error: {} — skipping reconcile",
                getName(), ax, e.what());
        }
        return;
    }

    std::vector<nlohmann::json> rows;
    mmExtractOpenOrdersRows(root, rows);

    std::unordered_set<std::string> live_oids;
    live_oids.reserve(rows.size() * 2 + 8);
    int dropped_foreign_symbol = 0;
    int dropped_no_symbol = 0;
    for (const auto& r : rows) {
        if (mmExtractRowSymbol(r).empty()) {
            ++dropped_no_symbol;
            continue;
        }
        if (!mmOpenOrdersRowMatchesAx(ax, r)) {
            ++dropped_foreign_symbol;
            continue;
        }
        std::string oid = mmExtractRowOid(r);
        if (!oid.empty()) {
            live_oids.insert(std::move(oid));
        }
    }
    if ((dropped_foreign_symbol > 0 || dropped_no_symbol > 0) && utils::Logger::isInitialized()) {
        Main().logger()->warn(
            "[VENUE_TRUTH] strategy={} sym={} open-orders instrument filter "
            "dropped_foreign_symbol={} dropped_no_symbol={} — foreign/ambiguous rows ignored",
            getName(),
            ax,
            dropped_foreign_symbol,
            dropped_no_symbol);
    }

    mmApplyVenueTruthReconcile(ax, live_oids, now_ms, /*from_cache=*/false, rows.size(),
                               resp.body.size());
}

// -----------------------------------------------------------------------------
// ENFORCE-mode tracked-vs-venue reconcile: read the cache, never GET on the mover.
// -----------------------------------------------------------------------------
void MakeMarketStrategy::mmReconcileTrackedAgainstVenueFromCache(const std::string& ax) {
    const std::int64_t now_ms = VenueOrdersCache::nowSteadyMs();
    const VenueOrdersSnapshot snap = VenueOrdersCache::instance().get(ax);
    const std::int64_t age = snap.ageMs(now_ms);
    if (!snap.valid || age > mmVenueCacheMaxAgeMs()) {
        // Stale/missing → TRUST LOCAL order state. Never fetch inline, never evict on
        // stale data (better to keep a row for one interval than wrong-cancel). Log once
        // per cache-max-age window so a persistent staleness is visible but not spammy.
        const std::int64_t last = mm_venue_cache_stale_last_ms_.load(std::memory_order_acquire);
        if ((last == 0 || now_ms - last >= mmVenueCacheMaxAgeMs()) &&
            utils::Logger::isInitialized()) {
            mm_venue_cache_stale_last_ms_.store(now_ms, std::memory_order_release);
            Main().logger()->warn(
                "[VENUE_CACHE_STALE] strategy={} ax={} age_ms={} max_age_ms={} — proceeding on "
                "local order-state (no inline GET, no eviction)",
                getName(), ax, snap.valid ? age : static_cast<std::int64_t>(-1),
                mmVenueCacheMaxAgeMs());
        }
        return;
    }
    std::unordered_set<std::string> live_oids = snap.oidSet();
    // from_cache=true: skip the [VENUE_COUNT_MISMATCH] log (done in the refresher, own-OID
    // filtered) and skip the at-cap venue-diff GET (enforce cap = snapshot-driven). body_size
    // is unknown for a cache read → 0.
    mmApplyVenueTruthReconcile(ax, live_oids, now_ms, /*from_cache=*/true, snap.rows.size(), 0);
}

// -----------------------------------------------------------------------------
// Shared eviction/orphan tail. Identical for off/shadow (from_cache=false, via GET) and
// enforce (from_cache=true, via cache). The mismatch log + at-cap venue-diff GET are
// legacy-only.
// -----------------------------------------------------------------------------
void MakeMarketStrategy::mmApplyVenueTruthReconcile(
    const std::string& ax, const std::unordered_set<std::string>& live_oids, std::int64_t now_ms,
    bool from_cache, std::size_t rows_count, std::size_t body_size) {
    // Cache the last-known venue order state for this stack's symbol so the pre-outbound
    // guard (mmVenueTruthKnowsOid) can skip cancels for OIDs the venue no longer reports.
    mmUpdateVenueTruthCache(live_oids, now_ms);

    // [VENUE_COUNT_MISMATCH] regression detector (2026-06-15) — OBSERVABILITY ONLY, no behavior
    // change. After every venue-truth refresh, compare the venue's actual resting open-order count
    // for this AX against our tracked non-terminal legs. A venue EXCESS (more resting orders at the
    // venue than we track) is the signature of the order-leak this change closes — this is how we
    // confirm on the next live run that the leak is shut without eyeballing the book. LOCAL excess
    // is usually benign: a just-placed leg that has not propagated into /open-orders yet, or a dead
    // leg about to be evicted by the pass just below. We snapshot the counts before that eviction
    // so the raw divergence is visible. Logged loudly only on divergence to avoid steady-state spam.
    // ENFORCE (from_cache): this per-stack check is superseded by the own-OID-filtered symbol-wide
    // check in mmRefreshVenueOrdersCacheAllEnforced, so it is skipped here to avoid cry-wolf.
    if (!from_cache && utils::Logger::isInitialized()) {
        const std::size_t venue_open = live_oids.size();
        std::size_t tracked_non_terminal = 0;     // state not FILLED / CANCELLED
        std::size_t tracked_resting_at_venue = 0;  // non-terminal AND already has an exchange oid
        {
            std::lock_guard<std::mutex> lk(tracked_orders_mutex_);
            auto count_vec = [&](const std::vector<TrackedLeg>& vec) {
                for (const auto& t : vec) {
                    if (t.state == MmLegState::FILLED || t.state == MmLegState::CANCELLED) {
                        continue;  // terminal — not expected to rest at the venue
                    }
                    ++tracked_non_terminal;
                    if (!t.exchange_oid.empty()) {
                        ++tracked_resting_at_venue;
                    }
                }
            };
            count_vec(tracked_bids_);
            count_vec(tracked_asks_);
        }
        if (venue_open != tracked_resting_at_venue) {
            const char* kind = (venue_open > tracked_resting_at_venue)
                                   ? "VENUE_EXCESS(possible_orphan)"
                                   : "LOCAL_EXCESS(propagation_lag_or_pending_evict)";
            Main().logger()->warn(
                "[VENUE_COUNT_MISMATCH] strategy={} ax={} kind={} venue_open={} "
                "tracked_resting_at_venue={} tracked_non_terminal={} delta={}",
                getName(), ax, kind, venue_open, tracked_resting_at_venue, tracked_non_terminal,
                static_cast<long long>(static_cast<std::int64_t>(venue_open) -
                                       static_cast<std::int64_t>(tracked_resting_at_venue)));
        }
    }

    int evicted_bids = 0;
    int evicted_asks = 0;
    OrderId evicted_bid_local = 0;
    OrderId evicted_ask_local = 0;
    std::vector<std::string> evicted_oids;
    // === DEAD-SAFE PROPAGATION / ADOPTION GRACE (2026-07-14) ==============================
    // Live incident 2026-07-14 08:27:04→08:27:05 (`mm_req_EURUSD_PERP_803a7859`): a stack
    // adopted its exchange OIDs at spawn, then the very next venue-truth reconcile ~1s later
    // found live_oids NOT containing those OIDs (venue ack→/open-orders visibility lag, or a
    // cache snapshot that predated the adoption) and evicted BOTH legs. That flipped the stack
    // ACTIVE→VENUE_ORPHANED→(auto)ACTIVE ("venue_orphan_recovered") which re-placed a fresh
    // pair — leaving the original, still-live orders resting at the venue as untracked orphans
    // (VENUE_COUNT_MISMATCH venue_open held at 4 for minutes). Same mechanism is the most
    // likely cause of the JPY doubling.
    //
    // Fix: never evict a leg whose exchange_oid we learned within `evict_grace_ms`. A leg that
    // was genuinely rejected/cancelled transitions via its own ack/fill path (not this eviction),
    // and a genuinely-dead resting leg is still evicted once the grace elapses. This ONLY delays
    // venue-truth eviction of freshly-established legs — it can never keep a leg the venue has
    // truly dropped beyond the grace window, and it never places or cancels anything itself.
    const std::int64_t evict_grace_ms = static_cast<std::int64_t>(std::max(
        0, config::Config::getInstance().getInt("market_maker.venue_truth_evict_grace_ms", 4000)));
    int grace_protected = 0;
    {
        std::lock_guard<std::mutex> lk(tracked_orders_mutex_);

        auto evict_dead = [&](std::vector<TrackedLeg>& vec, int& counter, OrderId& any_local) {
            vec.erase(std::remove_if(vec.begin(), vec.end(), [&](const TrackedLeg& t) {
                if (t.exchange_oid.empty()) {
                    // pre-adoption (PLACE_PENDING with a client_id but no oid yet) — keep.
                    return false;
                }
                if (live_oids.count(t.exchange_oid) > 0) {
                    return false;
                }
                // Propagation/adoption grace: a just-established leg may not be in the venue
                // snapshot yet. Keep it until the grace elapses so we never orphan a live order.
                if (evict_grace_ms > 0 && t.state_entered_steady_ms > 0 &&
                    (now_ms - t.state_entered_steady_ms) < evict_grace_ms) {
                    ++grace_protected;
                    return false;
                }
                if (any_local == 0) {
                    any_local = t.local_oid;
                }
                evicted_oids.push_back(t.exchange_oid);
                ++counter;
                return true;
            }), vec.end());
        };

        evict_dead(tracked_bids_, evicted_bids, evicted_bid_local);
        evict_dead(tracked_asks_, evicted_asks, evicted_ask_local);
    }

    if (grace_protected > 0 && utils::Logger::isInitialized()) {
        static std::mutex s_grace_log_mu;
        static std::unordered_map<std::string, std::int64_t> s_grace_log_last_ms;
        bool emit = false;
        {
            std::lock_guard<std::mutex> lk(s_grace_log_mu);
            std::int64_t& last = s_grace_log_last_ms[getName()];
            if (last == 0 || now_ms - last >= 1000) {
                last = now_ms;
                emit = true;
            }
        }
        if (emit) {
            Main().logger()->info(
                "[VENUE_TRUTH] strategy={} sym={} eviction grace protected {} recently-established "
                "leg(s) (grace_ms={} live_oids={} src={}) — not orphaning freshly placed/adopted "
                "orders",
                getName(), ax, grace_protected, evict_grace_ms, live_oids.size(),
                from_cache ? "cache" : "rest");
        }
    }

    // Reset legacy single-leg handles if they pointed at an evicted leg. Without this,
    // hasTrackedOrders() returns false (vectors empty) but bid_order_id_/ask_order_id_
    // still hold a stale local id, which downstream gates (e.g. the `desk_exchange_orders_not_live`
    // check we audited under Fix F) read directly.
    if (evicted_bid_local != 0 && bid_order_id_ == evicted_bid_local) {
        bid_order_id_ = 0;
        last_bid_price_ = 0.0;
    }
    if (evicted_ask_local != 0 && ask_order_id_ == evicted_ask_local) {
        ask_order_id_ = 0;
        last_ask_price_ = 0.0;
    }
    refreshLegacyTopOfBookTrackingFromVectors();

    if ((evicted_bids + evicted_asks) > 0 && utils::Logger::isInitialized()) {
        Main().logger()->warn(
            "[VENUE_TRUTH] strategy={} sym={} evicted bids={} asks={} (live_oids={} "
            "rows={} body_size={} src={}) — venue lost track of these legs since last drain",
            getName(), ax, evicted_bids, evicted_asks,
            live_oids.size(), rows_count, body_size, from_cache ? "cache" : "rest");
    }

    // === VENUE-ORPHAN DETECTION (2026-06-11 venue source-of-truth guard) ===========
    // When the venue evicts ALL of this stack's tracked legs (live_oids=0), the engine's
    // local state is stale: workers must not keep operating on orders that no longer
    // exist. Mark the stack VENUE_ORPHANED immediately (single source of truth) so any
    // worker checking state bails out, without waiting for the next scheduled poll.
    // grace_protected==0 guard (2026-07-14): if the eviction grace kept a freshly
    // placed/adopted leg, the stack still owns a live order — it is NOT orphaned, so we
    // must not flip it VENUE_ORPHANED (which would trigger a duplicate re-place).
    if ((evicted_bids + evicted_asks) > 0 && live_oids.empty() && grace_protected == 0) {
        constexpr std::int64_t kOrphanDetectLogThrottleMs = 1000;
        // H2 (2026-06-11): bound VENUE_ORPHANED->ACTIVE auto-recovery so a persistently-empty or
        // rejecting venue cannot thrash the cancel/replace path indefinitely. Recovery requires
        // BOTH a minimum spacing between attempts AND a consecutive-attempt cap. Once capped the
        // stack is left VENUE_ORPHANED (workers stay bailed) until a healthy cycle (venue reports
        // live legs) resets the counter below.
        constexpr std::int64_t kOrphanRecoveryMinIntervalMs = 2000;
        constexpr int kOrphanRecoveryMaxConsecutive = 5;
        const std::int64_t last_recover =
            mm_venue_orphan_last_recover_ms_.load(std::memory_order_acquire);
        const int consec =
            mm_venue_orphan_consecutive_recoveries_.load(std::memory_order_acquire);
        const bool log_detect =
            (last_recover == 0) || (now_ms - last_recover >= kOrphanDetectLogThrottleMs);

        // Mark the stack orphaned (single source of truth) so any worker bails immediately.
        mmTransitionStackState(MmStackState::VENUE_ORPHANED, "venue_truth_evicted_all");

        if (log_detect && utils::Logger::isInitialized()) {
            std::string oid_csv;
            for (size_t i = 0; i < evicted_oids.size(); ++i) {
                if (i) oid_csv += ",";
                oid_csv += evicted_oids[i];
            }
            Main().logger()->warn(
                "[VENUE_TRUTH] venue-orphan detected stack_id={} evicted_oids=[{}]",
                getName(), oid_csv);
        }

        // Auto-recovery: a still-desired desk stack re-arms to ACTIVE so the existing
        // DESK_RECOVERY path re-places a fresh pair. The tracked vectors were just purged
        // by the eviction above, so recovery starts from a clean slate. A stack that is
        // NOT desired (heading for teardown) is not running, so still_desired is false and it
        // is left VENUE_ORPHANED for the reconcile loop to tear down.
        //
        // H2: recovery is attempted on every orphaned poll (not only the ACTIVE->ORPHANED edge)
        // so a stack throttled by the min-interval is retried once the interval elapses, and it is
        // bounded by both the min-interval spacing and the consecutive-attempt cap. Once the cap is
        // hit the stack stays VENUE_ORPHANED (no more cancel/replace churn) until a healthy cycle
        // (venue reports live legs, below) resets the counter.
        if (mmStackState() == MmStackState::VENUE_ORPHANED) {
            const bool still_desired = isRunning() && mmIsDeskManaged();
            const bool interval_ok =
                (last_recover == 0) || (now_ms - last_recover >= kOrphanRecoveryMinIntervalMs);
            const bool under_cap = consec < kOrphanRecoveryMaxConsecutive;
            if (still_desired && interval_ok && under_cap) {
                mm_venue_orphan_last_recover_ms_.store(now_ms, std::memory_order_release);
                mm_venue_orphan_consecutive_recoveries_.fetch_add(1, std::memory_order_acq_rel);
                mmTransitionStackState(MmStackState::ACTIVE, "venue_orphan_recovered");
            } else if (still_desired && log_detect && utils::Logger::isInitialized()) {
                Main().logger()->warn(
                    "[VENUE_TRUTH] venue-orphan recovery throttled stack_id={} consec={} cap={} "
                    "since_last_recover_ms={} min_interval_ms={} — staying VENUE_ORPHANED",
                    getName(), consec, kOrphanRecoveryMaxConsecutive,
                    (last_recover == 0 ? static_cast<std::int64_t>(-1) : (now_ms - last_recover)),
                    kOrphanRecoveryMinIntervalMs);
            }
        }
    } else if (!live_oids.empty()) {
        // Healthy cycle: the venue reports live legs for this symbol, so quoting is working
        // normally — reset the consecutive-recovery cap so a later transient orphaning gets a
        // fresh recovery allowance.
        if (mm_venue_orphan_consecutive_recoveries_.load(std::memory_order_acquire) != 0) {
            mm_venue_orphan_consecutive_recoveries_.store(0, std::memory_order_release);
        }
    }

    // At-cap venue-diff scrub — LEGACY (off/shadow) ONLY. In enforce the mover never GETs;
    // cap enforcement is snapshot-driven (mmEnforceCapFromSnapshotOnSymbol) + local effective()
    // gates, and mmMaybeConvergeAtCapIfVenueExcess is itself a no-op in enforce.
    if (!from_cache) {
        auto& cfg_vt = config::Config::getInstance();
        const long long net_po_vt = mmInstrumentNetPoFromPortfolioCache(ax);
        const long long cap_vt =
            static_cast<long long>(std::max(1, mmEffMaxPositionInt(cfg_vt)));
        if (cap_vt > 0 && std::llabs(net_po_vt) >= cap_vt && mmIsDeskManaged()) {
            std::vector<MmVenueOpenRow> venue_rows_vt;
            if (mmFetchVenueOpenRowsForAx(ax, venue_rows_vt)) {
                const Side reduce_side_vt = (net_po_vt > 0) ? Side::SELL : Side::BUY;
                const int reduce_on_venue = mmCountVenueRowsOnSide(venue_rows_vt, reduce_side_vt);
                if (reduce_on_venue > 1) {
                    mmMaybeConvergeAtCapIfVenueExcess(this, "venue_truth");
                }
            }
        }
    }

    // Refresh the throttle window only on a successful round-trip+parse — if anything
    // upstream returned early due to error, a follow-up call should still hit the venue.
    // (Cache reads don't use this throttle — the refresher cadence bounds freshness.)
    if (!from_cache) {
        mm_venue_reconcile_last_ms_.store(now_ms, std::memory_order_release);
    }
}

void MakeMarketStrategy::requoteFromTheo(Price /*theo_midpoint*/, const std::string& reason) {
    // Pre-mover: synchronous runFullMmQuoteCycle on the caller's thread.
    // Post-mover: hand off to the per-AX worker thread; the eventual drain
    // re-reads the freshest theo so coalesced calls still produce a fresh quote.
    enqueueQuoteCycleOnMover(reason);
}

void MakeMarketStrategy::timerReconcilePairQuotes(Price theo_midpoint) {
    auto& cfg = config::Config::getInstance();
    if (!cfg.isMarketMakerEnabled()) {
        return;
    }
    if (passive_until_first_manual_order_ || !hasTrackedOrders()) {
        return;
    }
    // If we have no working order IDs but pending is still positive (missed accept after cancel-all/fill,
    // reject ordering, etc.), clear stale pending so timer can call runFullMmQuoteCycle.
    const bool no_tracked_working = (bid_order_id_ == 0 && ask_order_id_ == 0);
    if (mm_pending_accepts_ > 0 && no_tracked_working) {
        Main().logger()->warn(
            "[STRATEGY:{}] MM timer_reconcile: pending_accepts={} but bid_id/ask_id are 0 — "
            "resetting pending to re-place quotes",
            getName(),
            mm_pending_accepts_);
        mm_pending_accepts_ = 0;
        mm_pending_accepts_last_change_ms_.store(
            std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now().time_since_epoch())
                .count(),
            std::memory_order_relaxed);
    }
    const bool want_bid = shouldQuoteSide(Side::BUY);
    const bool want_ask = shouldQuoteSide(Side::SELL);

    // Refresh reduce-only flag using the strategy's local NetPo (updated on fills + periodic
    // sync from runFullMmQuoteCycle). No fresh exchange poll here — the timer is hot-path and
    // shouldQuoteSide already gates on this same local value, so the flag follows the same truth.
    // CONCURRENCY (fix C4): take position_state_mutex_ so the snapshot used to set the
    // reduce-only flag cannot race a concurrent processFill onAxFill on the OM thread.
    {
        // ENFORCE reads book effective(); off/shadow read legacy net (identical value).
        const long long net_ll = mmEffectiveNetPoRounded();
        const long long cap_ll = static_cast<long long>(std::max(1, mmEffMaxPositionInt(cfg)));
        noteReduceOnlyState(net_ll, cap_ll);
    }

    // All cancel/replace branches below are now enqueued onto the per-AX mover
    // thread so the timer thread never does REST. The mover re-reads the freshest
    // theo when the lambda runs (so `theo_midpoint` arg here is informational only).
    if (want_bid && want_ask) {
        if (mmIsDeskManaged()) {
            // Desk stacks: cancel-ack reconcile only (no runFullMmQuoteCycle).
            // Removed `syncInventoryFromExchangePortfolio()` on 2026-05-21 per
            // Tim's spec — post-init the only sanctioned position-mutation
            // path is `position_state_.onAxFill()` driven by real fill events.
            // The periodic REST overwrite here would silently re-introduce
            // the same lag hazard that caused the SILVER 2026-05-21 breaches.
            maybeReconcileDeskCancelAckTimeouts();
            return;
        }
        if (bid_order_id_ == 0 || ask_order_id_ == 0) {
            if (!cfg.getBool("market_maker.cpp_manual_quotes_only", true)) {
                enqueueQuoteCycleOnMover("timer_reconcile");
            }
        }
        return;
    }
    if (!want_bid && bid_order_id_ != 0) {
        enqueueQuoteCycleOnMover("timer_pull_bid");
        return;
    }
    if (!want_ask && ask_order_id_ != 0) {
        enqueueQuoteCycleOnMover("timer_pull_ask");
        return;
    }
    if (want_bid && !want_ask && bid_order_id_ == 0) {
        enqueueQuoteCycleOnMover("timer_one_leg_bid");
        return;
    }
    if (!want_bid && want_ask && ask_order_id_ == 0) {
        enqueueQuoteCycleOnMover("timer_one_leg_ask");
        return;
    }
    // Reduce-only: we already have the correct side working — `timerReconcile` previously returned without
    // a runFull, so a resting reduce limit never tracked theo when `requote_on_timer` is false. Re-peg
    // here (no-op in runFull if price is unchanged, modulo kept_resting epsilon in one-sided mode).
    if (want_bid && !want_ask && bid_order_id_ != 0) {
        enqueueQuoteCycleOnMover("timer_repeg_one_leg");
        return;
    }
    if (!want_bid && want_ask && ask_order_id_ != 0) {
        enqueueQuoteCycleOnMover("timer_repeg_one_leg");
    }
    (void)theo_midpoint;
}

void MakeMarketStrategy::updateQuotes() {
    if (utils::Logger::isInitialized()) {
        std::size_t tb = 0;
        std::size_t ta = 0;
        {
            std::lock_guard<std::mutex> lk(tracked_orders_mutex_);
            tb = tracked_bids_.size();
            ta = tracked_asks_.size();
        }
        Main().logger()->debug(
            "[CALL_TRACE:{}] {} entered: tracked_bids={} tracked_asks={} passive={} desk_stack={}",
            mmAxSymbol(),
            __func__,
            tb,
            ta,
            passive_until_first_manual_order_,
            mmIsDeskManaged() ? 1 : 0);
    }
    (void)config::Config::getInstance().reloadPrimaryConfigFromDisk();
    auto& config = config::Config::getInstance();
    mmMaybeCancelOnMmOrdersDisabled(config);

    {
        Price transition_hint = last_theo_;
        if (auto opt = mmReadTransformedTheo()) {
            transition_hint = *opt;
        }
        mmNoteMmOrdersEnabledTransitionAfterConfigReload(transition_hint);
    }

    if (!theo_provider_) {
        logStrategyDecision("UPDATE_SKIPPED", "no theo provider");
        return;
    }

    if (!mmMarketMakerOrdersPlacementAllowed(config)) {
        mmLogMmGateThrottled("timer_update_placement_disallowed", "updateQuotes",
                             "mmMarketMakerOrdersPlacementAllowed=false");
        return;
    }
    if (config.getMarketMakerCancelOnDisconnect() && !Main().websocket()->isConnected()) {
        mmLogMmGateThrottled("timer_ws_disconnected", "updateQuotes", "cancel_on_disconnect + AX WS down");
        mmCancelAllExchangeAndResetLocal("mm_ax_ws_disconnected_timer");
        return;
    }

    maybePeriodicExchangePositionReconcile();

    if (mmIsDeskManaged()) {
        mmMaybePeriodicConvergeAtCap(this);
    }

    std::string theo_sym = mmTheoSymbol();

    auto theo_opt = mmReadTransformedTheo();
    if (!theo_opt) {
        Main().logger()->debug("[STRATEGY:{}] No theo for '{}' - skipping quote update", 
            getName(), theo_sym);
        mmLogMmGateThrottled("timer_no_transformed_theo", "updateQuotes",
                             "mmReadTransformedTheo() empty for symbol='" + theo_sym + "'");
        return;
    }

    Price theo_midpoint = *theo_opt;
    if (passive_until_first_manual_order_ || !hasTrackedOrders()) {
        return;
    }
    // === CYCLE-LOCK GATE (timer path) ============================================
    // Same semantics as the onFeedUpdate gate: skip drift-driven cycle eval while
    // a cancel-replace is in flight. processTrackedOrdersTheoMove may decide to
    // fire a new cycle, so we must not call it during an in-flight cycle. This
    // is independent of the onFeedUpdate gate — the timer fires at ~1Hz, and
    // a cycle that started off a recent feed tick will still be in flight when
    // the next timer tick arrives.
    if (isPairCycleInFlight()) {
        mmLogMmGateThrottled(
            "cycle_lock_skip_timer",
            "updateQuotes",
            "[CYCLE_LOCK] sym=" + mmAxSymbol() +
                " stack=" + (mmOrderRequestIdOrLegacy()) +
                " pair has cycle in flight, skipping timer drift evaluation");
        return;
    }
    if (mmIsDeskManaged()) {
        if (config.getMarketMakerRequoteOnTimer()) {
            requoteFromTheo(theo_midpoint, "timer_update");
        } else {
            timerReconcilePairQuotes(theo_midpoint);
        }
        return;
    }
    if (processTrackedOrdersTheoMove(theo_midpoint, "timer_update")) {
        return;
    }
    if (config.getMarketMakerRequoteOnTimer()) {
        requoteFromTheo(theo_midpoint, "timer_update");
    } else {
        timerReconcilePairQuotes(theo_midpoint);
    }
}

void MakeMarketStrategy::on_timer(DateInt date, TimeRack time_rack) {
    (void)date;
    (void)time_rack;
    updateQuotes();
}

bool MakeMarketStrategy::tryGetAxMarketBbo(const std::string& sym, Price& out_bid, Price& out_ask) const {
    if (!sym.empty() && market_provider_) {
        auto mq = market_provider_(sym);
        if (mq && mq->valid && mq->bid > 0 && mq->ask > 0 && mq->ask > mq->bid) {
            out_bid = mq->bid;
            out_ask = mq->ask;
            return true;
        }
    }
    if (sym.empty()) {
        return false;
    }
    auto bbo = marketdata::MarketDataManager::getInstance().getBBO(sym);
    if (!bbo.has_value()) {
        return false;
    }
    out_bid = bbo->first.price;
    out_ask = bbo->second.price;
    return out_bid > 0 && out_ask > 0 && out_ask > out_bid;
}

namespace {

using DeskJson = nlohmann::json;

std::string deskJsonOid(const DeskJson& j) {
    static const char* keys[] = {"oid", "exchange_order_id", "order_id", "id", "orderId"};
    for (const char* k : keys) {
        if (!j.contains(k)) {
            continue;
        }
        const auto& v = j[k];
        if (v.is_string()) {
            std::string s = v.get<std::string>();
            const auto start = s.find_first_not_of(" \t\r\n");
            if (start == std::string::npos) {
                continue;
            }
            const auto end = s.find_last_not_of(" \t\r\n");
            s = s.substr(start, end - start + 1);
            if (!s.empty()) {
                return s;
            }
        } else if (v.is_number_integer()) {
            return std::to_string(v.get<std::int64_t>());
        }
    }
    if (j.contains("data") && j["data"].is_object()) {
        std::string nested = deskJsonOid(j["data"]);
        if (!nested.empty()) {
            return nested;
        }
    }
    return {};
}

std::optional<bool> rowSideIsBuy(const DeskJson& row) {
    if (row.contains("d") && row["d"].is_string()) {
        const std::string d = row["d"].get<std::string>();
        if (!d.empty()) {
            const char c = static_cast<char>(std::toupper(static_cast<unsigned char>(d[0])));
            if (c == 'B') {
                return true;
            }
            if (c == 'S') {
                return false;
            }
        }
    }
    if (row.contains("side") && row["side"].is_string()) {
        std::string s = row["side"].get<std::string>();
        for (char& ch : s) {
            ch = static_cast<char>(std::toupper(static_cast<unsigned char>(ch)));
        }
        if (s == "BUY" || s == "B") {
            return true;
        }
        if (s == "SELL" || s == "S" || s == "ASK") {
            return false;
        }
    }
    return std::nullopt;
}

double deskRowPrice(const DeskJson& row) {
    static const char* keys[] = {"p", "price", "limit_price", "limitPrice"};
    for (const char* k : keys) {
        if (!row.contains(k)) {
            continue;
        }
        const auto& v = row[k];
        if (v.is_number()) {
            return v.get<double>();
        }
        if (v.is_string()) {
            try {
                return std::stod(v.get<std::string>());
            } catch (...) {
            }
        }
    }
    return 0.0;
}

void parseOpenOrdersBody(const DeskJson& root, std::vector<DeskJson>* out_rows) {
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

}  // namespace

void MakeMarketStrategy::maybePeriodicExchangePositionReconcile() {
    auto& cfg = config::Config::getInstance();
    const double interval_sec = cfg.getDouble("market_maker.exchange_position_reconcile_sec", 3.5);
    if (!(interval_sec > 0.0)) {
        mm_last_position_reconcile_valid_ = false;
        return;
    }
    const auto now = std::chrono::steady_clock::now();
    if (mm_last_position_reconcile_valid_) {
        const double elapsed =
            std::chrono::duration<double>(now - mm_last_position_reconcile_tp_).count();
        if (elapsed < interval_sec) {
            return;
        }
    }
    mm_last_position_reconcile_tp_ = now;
    mm_last_position_reconcile_valid_ = true;

    std::lock_guard<std::recursive_mutex> cycle_lock(mm_cycle_mutex_);

    const std::string sym = mmAxSymbol();
    const int max_po = mmEffMaxPositionInt(cfg);
    if (sym.empty() || max_po <= 0) {
        return;
    }

    // Exchange truth + product-wide grow prune (same path as post-fill sync).
    syncExchangeNetForAllMakeMarketOnSymbol(sym, this, true);
    // mmEnforceProductCapOnSymbolAfterSync already ran inside sync; log if still past cap.
    const long long net_po = static_cast<long long>(
        std::llround(position_state_.net_position_qty));
    const long long max_ll = static_cast<long long>(max_po);
    if (std::llabs(net_po) <= max_ll) {
        return;
    }
    if (utils::Logger::isInitialized()) {
        Main().logger()->warn(
            "[STRATEGY:{}] MM position reconcile: |net|={} still > max_position={} on {} "
            "after exchange sync + grow prune — venue may need manual flatten",
            getName(),
            net_po,
            max_po,
            sym);
    }
}

bool MakeMarketStrategy::mmDeskReconcileAdoptStack(const architect::config::MarketMakerManualStack& stack) {
    const std::string cur_req = mmOrderRequestId();
    if (cur_req == stack.id) {
        bool has_inflight = false;
        bool has_non_adopted_tracked = false;
        {
            std::lock_guard<std::mutex> lk(tracked_orders_mutex_);
            auto scan_inflight = [](const std::vector<TrackedLeg>& vec) {
                for (const auto& t : vec) {
                    if (mmLegStateIsCancelInFlight(t.state) || t.state == MmLegState::PLACE_PENDING) {
                        return true;
                    }
                }
                return false;
            };
            auto scan_non_adopted = [](const std::vector<TrackedLeg>& vec) {
                for (const auto& t : vec) {
                    if (t.state != MmLegState::ADOPTED && t.state != MmLegState::IDLE &&
                        t.state != MmLegState::PAUSED) {
                        return true;
                    }
                }
                return false;
            };
            has_inflight = scan_inflight(tracked_bids_) || scan_inflight(tracked_asks_);
            has_non_adopted_tracked =
                scan_non_adopted(tracked_bids_) || scan_non_adopted(tracked_asks_);
        }
        if (has_inflight || has_non_adopted_tracked) {
            if (utils::Logger::isInitialized()) {
                Main().logger()->info(
                    "[MM_ORDERS] adopt skipped — already bound stack_id={} (state transition in-flight)",
                    stack.id);
            }
            return true;
        }
    }
    if (mm_desk_active_ && cur_req == stack.id) {
        auto side_matches = [&](const std::vector<TrackedLeg>& vec,
                                const std::string& expected_oid,
                                double expected_px) {
            if (expected_oid.empty() && !(expected_px > 0.0)) {
                return true;
            }
            for (const auto& t : vec) {
                if (t.state != MmLegState::ADOPTED || t.local_oid == 0) {
                    continue;
                }
                const bool oid_ok = !expected_oid.empty() && t.exchange_oid == expected_oid;
                const bool px_ok =
                    (expected_px > 0.0) ? (std::fabs(t.exchange_px - expected_px) <= 1e-6) : true;
                if (oid_ok && px_ok) {
                    return true;
                }
            }
            return false;
        };
        bool bid_match = false;
        bool ask_match = false;
        {
            std::lock_guard<std::mutex> lk(tracked_orders_mutex_);
            bid_match = side_matches(tracked_bids_, stack.bid_exchange_oid, stack.placed_bid_price);
            ask_match = side_matches(tracked_asks_, stack.ask_exchange_oid, stack.placed_ask_price);
        }
        if (bid_match && ask_match) {
            if (utils::Logger::isInitialized()) {
                Main().logger()->info(
                    "[MM_ORDERS] adopt skipped — already bound to stack_id={} with matching OIDs and prices",
                    stack.id);
            }
            return true;
        }
    }
    // Multi-pair safety: this strategy is already bound to a DIFFERENT desk stack and is actively
    // managing its legs. Re-adopting onto the same instance would clobber the prior stack's
    // bookkeeping (tracked_bids_/tracked_asks_, exchange OIDs, last_bid_price_/last_ask_price_),
    // leaving the prior stack's exchange orders orphaned. Reject the re-adopt and surface a clear
    // error so the caller (mmSpawnFromOrdersStack) logs the spawn-skipped message.
    //
    // To run multiple desk stacks on the same AX symbol, set
    //   market_maker.allow_multi_mm_per_ax=true
    // so each stack spawns its own mm_req_<AX>_<stack_id> strategy instance with independent
    // tracked legs and per-pair width/min_drift.
    if (mm_desk_active_ && !cur_req.empty() && cur_req != stack.id) {
        if (utils::Logger::isInitialized()) {
            Main().logger()->warn(
                "[MM_ORDERS] adopt rejected — strategy={} ax={} already bound to stack_id={} "
                "(refusing to clobber). To run multiple desk stacks on the same AX, set "
                "market_maker.allow_multi_mm_per_ax=true so each stack gets its own mm_req_* "
                "strategy. Incoming stack_id={} ignored.",
                getName(), mmAxSymbol(), cur_req, stack.id);
        }
        return false;
    }
    setMmOrderRequestId(stack.id);
    setOptionalManualStack(stack);
    setMmDeskActive(true);
    // Adopted desk stacks are externally placed — there is no per-strategy "manual order" to wait for.
    // Without this, allow_order_actions stays false and onFeedUpdate silently skips theo-move.
    passive_until_first_manual_order_ = false;
    if (utils::Logger::isInitialized()) {
        auto& cfg_log = config::Config::getInstance();
        const int eff = mmEffMinTheoDriftTicksInt(cfg_log);
        const auto ms_log = mmOptionalManualStack();
        const char* src = (ms_log.has_value() &&
                           ms_log->min_theo_move_ticks_to_requote > 0)
                              ? "per-pair (orders.json)"
                              : "desk default (mm_desk.default_min_drift_ticks)";
        Main().logger()->info(
            "[MM_ORDERS] desk gate config strategy={} stack_id={} eff_min_drift_ticks={} source='{}'",
            getName(), stack.id, eff, src);
    }
    return tryAdoptGatewayPlacedFromManualStack();
}

void MakeMarketStrategy::mmDeskReconcileClearStackBinding() {
    (void)mmDeskShutdownLogAndCancelTrackedLegs(nullptr);
    nlohmann::json j;
    j["symbol"] = mmAxSymbol();
    j["mm_desk_active"] = false;
    (void)applyMmDeskSeededJsonLocked(j, false, false);
    setMmOrderRequestId("");
    setOptionalManualStack(std::nullopt);
    setMmDeskActive(false);
}

bool MakeMarketStrategy::tryAdoptGatewayPlacedFromManualStack() {
    (void)config::Config::getInstance().reloadPrimaryConfigFromDisk();
    // Runs on feed thread (via onFeedUpdate) AND desk-bg thread. Take ONE locked snapshot
    // of the desk-config so a concurrent desk-bg replace can't tear the strings we read below.
    const auto ms = mmOptionalManualStack();
    const std::string req_id = mmOrderRequestId();
    if (!ms.has_value() || req_id.empty() || !ms->desk_seeded) {
        return false;
    }
    const auto& s = *ms;
    if (!(s.placed_bid_price > 0.0 || s.placed_ask_price > 0.0)) {
        return false;
    }
    // Idempotency guard: the spawn-time bootstrap call from main.cpp can race the feed-driven
    // adoption retry inside ``onFeedUpdate``. If the feed path adopts first, this strategy
    // already has tracked_bids_/tracked_asks_ in ``ADOPTED`` state matching the manual_stack OIDs
    // and prices. Falling through to ``applyMmDeskSeededJsonLocked`` would queue a
    // ``DeskReseed`` cancel against those same legs (lines 5867–5902) and kill the freshly
    // adopted pair. Same check that ``mmDeskReconcileAdoptStack`` already does for the base
    // quoter path; replicated here for the spawn path.
    if (mm_desk_active_) {
        auto side_matches = [&](const std::vector<TrackedLeg>& vec,
                                const std::string& expected_oid,
                                double expected_px) {
            if (expected_oid.empty() && !(expected_px > 0.0)) {
                return true;
            }
            for (const auto& t : vec) {
                if (t.state != MmLegState::ADOPTED || t.local_oid == 0) {
                    continue;
                }
                const bool oid_ok = !expected_oid.empty() && t.exchange_oid == expected_oid;
                const bool px_ok =
                    (expected_px > 0.0) ? (std::fabs(t.exchange_px - expected_px) <= 1e-6) : true;
                if (oid_ok && px_ok) {
                    return true;
                }
            }
            return false;
        };
        bool bid_match = false;
        bool ask_match = false;
        {
            std::lock_guard<std::mutex> lk(tracked_orders_mutex_);
            bid_match = side_matches(tracked_bids_, s.bid_exchange_oid, s.placed_bid_price);
            ask_match = side_matches(tracked_asks_, s.ask_exchange_oid, s.placed_ask_price);
        }
        if (bid_match && ask_match) {
            if (utils::Logger::isInitialized()) {
                Main().logger()->info(
                    "[MM_ORDERS] gateway-seed adopt skipped — already adopted strategy={} stack_id={} "
                    "bid_oid='{}' ask_oid='{}' bid_px={:.6f} ask_px={:.6f}",
                    getName(), req_id, s.bid_exchange_oid, s.ask_exchange_oid,
                    s.placed_bid_price, s.placed_ask_price);
            }
            return true;
        }
    }
    nlohmann::json j;
    j["symbol"] = mmAxSymbol();
    j["mm_desk_active"] = true;
    j["bid_exchange_oid"] = s.bid_exchange_oid;
    j["ask_exchange_oid"] = s.ask_exchange_oid;
    j["bid_price"] = s.placed_bid_price;
    j["ask_price"] = s.placed_ask_price;
    // STRICT: order size only from the user-entered desk stack. Refuse to adopt
    // the gateway-placed pair if the stack ships no positive size — that's a
    // malformed desk seed and substituting a config default has historically
    // produced wrong-size requotes (e.g. quote 100 when user submitted 10).
    if (!(s.order_size > 0)) {
        if (utils::Logger::isInitialized() && Main().logger()) {
            Main().logger()->warn(
                "[MM_ORDERS] desk gateway-seed adopt refused — strategy={} stack_id={} "
                "user-entered order_size missing/<=0 in orders.json (no config fallback)",
                getName(), req_id);
        }
        return false;
    }
    const int osz = s.order_size;
    j["quantity"] = static_cast<double>(osz);
    // Per-side qty falls back to the per-stack `order_size` ONLY when the
    // matching `placed_*_qty` was not recorded by the desk for that side. This
    // is still a user-entered value (same stack), not a config default.
    const int bq =
        s.placed_bid_qty > 0 ? s.placed_bid_qty : (s.placed_bid_price > 0.0 ? osz : 0);
    const int aq =
        s.placed_ask_qty > 0 ? s.placed_ask_qty : (s.placed_ask_price > 0.0 ? osz : 0);
    j["bid_quantity"] = static_cast<double>(bq);
    j["ask_quantity"] = static_cast<double>(aq);
    return applyMmDeskSeededJsonLocked(j, false, true);
}

bool MakeMarketStrategy::mmDeskTrackedOrdersAliveForMove() const {
    if (bid_order_id_ == 0 && ask_order_id_ == 0) {
        return false;
    }
    if (bid_order_id_ != 0 && !mmTrackedSideHasActiveLimitOrder(Side::BUY)) {
        return false;
    }
    if (ask_order_id_ != 0 && !mmTrackedSideHasActiveLimitOrder(Side::SELL)) {
        return false;
    }
    return true;
}

bool MakeMarketStrategy::mmDeskOrdersJsonContainsThisStack() const {
    const std::string ax = mmAxSymbol();
    const std::string sid = mmOrderRequestId();
    if (ax.empty() || sid.empty()) {
        return false;
    }
    auto& cfg = config::Config::getInstance();
    namespace fs = std::filesystem;
    const std::string live_rel = cfg.getString("mm_desk.mm_orders_config_path", "logs/orders.json");
    const fs::path live_p = mmDeskResolveLogsRelPathStr(live_rel, "logs/orders.json");
    const std::string frozen_rel =
        cfg.getString("mm_desk.orders_config_frozen_copy_path", "logs/orders.json.frozen_cpp");
    const fs::path frozen_p = mmDeskResolveLogsRelPathStr(frozen_rel, "logs/orders.json.frozen_cpp");
    const fs::path p = g_mm_orders_reconcile_from_frozen_copy ? frozen_p : live_p;
    std::error_code ec;
    if (!fs::is_regular_file(p, ec)) {
        return false;
    }
    std::string raw;
    {
        std::ifstream f(p, std::ios::binary);
        if (!f) {
            return false;
        }
        std::ostringstream ss;
        ss << f.rdbuf();
        raw = ss.str();
    }
    if (raw.empty()) {
        return false;
    }
    try {
        const nlohmann::json doc = nlohmann::json::parse(raw);
        return mmDeskDocContainsStackForAx(doc, ax, sid);
    } catch (...) {
        // FAIL-SAFE on a TRANSIENT corrupt read. A parse error here means orders.json was
        // caught mid-publish (e.g. a racing writer). The old behaviour returned false, which
        // told every stack "you are no longer in the desk config" — so after any theo-move
        // cancelled a pair, the re-add was gated (desk_orders_json_stack_missing) and all
        // resting orders silently drained. A malformed read is NOT evidence a stack was
        // removed (an intentional removal writes a well-formed {"stacks":[...]} that parses
        // fine and correctly reports absent). Treat unparseable as "assume still present" so
        // we keep quoting on the last known-good desired state until the file is readable
        // again. This never places an extra order: placement remains gated by the lease and
        // existing tracked legs. Throttle the warning to once/10s/stack.
        static std::mutex s_warn_mu;
        static std::unordered_map<std::string, std::int64_t> s_last_warn_ms;
        const std::int64_t now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                        std::chrono::steady_clock::now().time_since_epoch())
                                        .count();
        bool do_warn = false;
        {
            std::lock_guard<std::mutex> lk(s_warn_mu);
            auto& last = s_last_warn_ms[sid];
            if (now_ms - last >= 10000) {
                last = now_ms;
                do_warn = true;
            }
        }
        if (do_warn) {
            Main().logger()->warn(
                "[MM_ORDERS] orders.json unparseable while checking stack ownership "
                "(ax={} stack={}) — FAIL-SAFE: assuming stack still present, NOT gating "
                "(transient corrupt read; keeps resting orders instead of draining them)",
                ax, sid);
        }
        return true;
    }
}

bool MakeMarketStrategy::applyMmDeskSeededJsonLocked(
    const architect::config::json& j, bool require_desk_sync, bool stamp_first_ack_from_adopted) {
    (void)config::Config::getInstance().reloadPrimaryConfigFromDisk();
    // Reachable from the feed thread (onFeedUpdate → tryAdopt…). Snapshot the desk id once
    // for the logging below rather than reading the raw member across the desk-bg writer.
    const std::string req_id = mmOrderRequestId();
    auto& cfg = config::Config::getInstance();
    if (!cfg.isMarketMakerEnabled()) {
        return false;
    }
    if (require_desk_sync && !cfg.getMarketMakerDeskSyncEnabled()) {
        return false;
    }
    if (!cfg.getMarketMakerMmOrdersEnabledForSymbol(mmAxSymbol())) {
        Main().logger()->info(
            "[STRATEGY:{}] desk seeded sync ignored (market_maker.mm_orders_enabled=false)", getName());
        return false;
    }
    const std::string sym_sig = j.value("symbol", "");
    const std::string sym_mm = mmAxSymbol();
    if (!sym_sig.empty() && sym_sig != sym_mm) {
        Main().logger()->warn(
            "[STRATEGY:{}] desk seeded sym mismatch sig={} mm={}", getName(), sym_sig, sym_mm);
        return false;
    }

    std::lock_guard<std::recursive_mutex> cycle_lock(mm_cycle_mutex_);

    const bool active = j.value("mm_desk_active", true);
    if (!active) {
        orders::OrderManager::getInstance().purgeNonTerminalOrdersForSymbol(sym_mm);
        bid_order_id_ = 0;
        ask_order_id_ = 0;
        last_bid_price_ = 0.0;
        last_ask_price_ = 0.0;
        clearOpenOrderTracking();
        mm_desk_active_ = false;
        mm_desk_stack_paused_ = false;
        mm_pending_accepts_ = 0;
        mm_desk_gateway_seed_steady_ms_ = 0;
        mm_pending_accepts_last_change_ms_.store(
            std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now().time_since_epoch())
                .count(),
            std::memory_order_relaxed);
        Main().logger()->info("[STRATEGY:{}] desk sync: mm_desk_active=false, purged local MM orders", getName());
        return true;
    }
    // STRICT: order quantities are user-entered values carried into this JSON by the
    // desk (orders.json `mm_req_*` stack). No fallback to `market_maker.quantity` —
    // that's a runtime-config default that has no relation to what the user typed
    // for THIS stack. If neither `quantity` nor a per-side `bid_quantity`/`ask_quantity`
    // is present and positive, refuse the seeded sync entirely.
    Quantity qty_cfg = 0.0;
    if (j.contains("quantity") && j["quantity"].is_number()) {
        qty_cfg = static_cast<Quantity>(j["quantity"].get<double>());
    }
    Quantity qty_bid_cfg = qty_cfg;
    Quantity qty_ask_cfg = qty_cfg;
    if (j.contains("bid_quantity") && j["bid_quantity"].is_number()) {
        qty_bid_cfg = static_cast<Quantity>(j["bid_quantity"].get<double>());
    }
    if (j.contains("ask_quantity") && j["ask_quantity"].is_number()) {
        qty_ask_cfg = static_cast<Quantity>(j["ask_quantity"].get<double>());
    }
    if (qty_cfg <= 0.0 && qty_bid_cfg <= 0.0 && qty_ask_cfg <= 0.0) {
        if (utils::Logger::isInitialized() && Main().logger()) {
            Main().logger()->warn(
                "[STRATEGY:{}] desk seeded sync refused — no user-entered quantity "
                "(quantity/bid_quantity/ask_quantity all missing or <=0; no config fallback)",
                getName());
        }
        return false;
    }

    // Avoid re-adopting from orders.json while a prior desk cancel-replace has submitted limits but
    // accept has not arrived yet (bid_order_id_/ask_order_id_ still 0). Otherwise we bind new OIDs from
    // JSON while venue rows from the in-flight submit are still working → duplicate stacks on the book.
    if (mm_pending_accepts_ > 0 && bid_order_id_ == 0 && ask_order_id_ == 0) {
        Main().logger()->info(
            "[STRATEGY:{}] desk seeded sync deferred stack_id={} pending_accepts={} "
            "(await venue ack for in-flight submit before re-adopt)",
            getName(),
            req_id,
            mm_pending_accepts_);
        return false;
    }

    if (!require_desk_sync) {
        if (isPairCycleInFlight()) {
            Main().logger()->info(
                "[STRATEGY:{}] gateway-seed adopt deferred stack_id={} — cycle_in_flight",
                getName(),
                req_id);
            return false;
        }
        if (mmIsAxFillRequoteInflight(sym_mm)) {
            Main().logger()->info(
                "[STRATEGY:{}] gateway-seed adopt deferred stack_id={} — fill_requote_inflight ax={}",
                getName(),
                req_id,
                sym_mm);
            return false;
        }
        bool leg_churn = false;
        {
            std::lock_guard<std::mutex> lk(tracked_orders_mutex_);
            const auto pending = [](const std::vector<TrackedLeg>& vec) {
                return std::any_of(vec.begin(), vec.end(), [](const TrackedLeg& t) {
                    return mmLegStateIsCancelInFlight(t.state) ||
                           t.state == MmLegState::PLACE_PENDING;
                });
            };
            leg_churn = pending(tracked_bids_) || pending(tracked_asks_);
        }
        if (leg_churn) {
            Main().logger()->info(
                "[STRATEGY:{}] gateway-seed adopt deferred stack_id={} — cancel/place pending on stack",
                getName(),
                req_id);
            return false;
        }
    }

    // === STRATEGY-OWNS-LEGS GUARD (2026-05-07 cascading-moves fix) =========================
    // Pre-fix history: this site originally tore down on every orders.json reconcile pass.
    // v1 fix added an OID-match idempotency check; v2 confirmed quiet-timer was rearmed.
    // BUT a deeper bug remained: orders.json is written *after* the strategy places (by the
    // reconciler in main.cpp / the python desk client) so a snapshot read by the next
    // reconcile pass is by construction at least one cycle behind the strategy's tracked
    // OIDs. With the v1 OID-match guard, every post-move reconcile would see a mismatch
    // (JSON has prev-cycle OIDs, OM has current-cycle OIDs) and fall through to teardown.
    //
    // The teardown then:
    //   1. Queues `desk_reseed_cancel` REST cancels for our currently-alive legs on the mover
    //   2. `om.detachLocalOrderById(...)` IMMEDIATELY — so when the queued cancel later fires,
    //      `om.getOrder(local_id)` returns nullptr and the cancel is skipped. The exchange
    //      orders STAY ALIVE on the venue forever (orphan legs).
    //   3. Re-adopts the JSON's stale OIDs as new local rows.
    //
    // Net effect (visible in user log 18:30:50–18:31:13 EURUSD-PERP):
    //   • Move 1 places DPG/DPR. tracked = DPG/DPR.
    //   • Reconcile 1ms later: JSON has DME/DMN (prev cycle). Mismatch → teardown.
    //     DPG/DPR get detached (cancel never executes). tracked = DME/DMN.
    //   • Move 2 cancels DME/DMN, places DSD/DSM. DPG/DPR still alive on venue.
    //   • Reconcile: JSON has DPG/DPR (now caught up to move 1). Mismatch → teardown.
    //     DSD/DSM get detached. tracked = DPG/DPR.
    //   • Move 3 cancels DPG/DPR, places DV6/DVE. DSD/DSM still alive on venue.
    //   • …two interleaved tracks of orphan orders pile up; user sees "two pairs moving".
    //
    // Fix: orders.json is authoritative ONLY for initial seeding. Once the strategy has any
    // alive leg in OM, IT owns its tracked state — the JSON is informational. A reconcile
    // pass with the strategy already running must be a no-op regardless of OID mismatch.
    //
    // Operator-driven explicit desk sync (require_desk_sync=true) still falls through so
    // manual price changes from the python desk client take effect immediately.
    if (active && !require_desk_sync) {
        auto& om = orders::OrderManager::getInstance();
        auto cur_bid = bid_order_id_ != 0 ? om.getOrder(bid_order_id_) : nullptr;
        auto cur_ask = ask_order_id_ != 0 ? om.getOrder(ask_order_id_) : nullptr;
        const bool bid_alive = cur_bid && !cur_bid->isTerminal();
        const bool ask_alive = cur_ask && !cur_ask->isTerminal();
        if (bid_alive || ask_alive) {
            // Strategy already owns at least one live leg. Trust it.
            if (utils::Logger::isInitialized()) {
                Main().logger()->debug(
                    "[STRATEGY:{}] applyMmDeskSeededJsonLocked: strategy owns legs "
                    "(bid_alive={} ask_alive={} json_bid_oid='{}' json_ask_oid='{}') — "
                    "no-op (orders.json is lagging informational snapshot)",
                    getName(),
                    bid_alive ? 1 : 0,
                    ask_alive ? 1 : 0,
                    j.value("bid_exchange_oid", ""),
                    j.value("ask_exchange_oid", ""));
            }
            return true;
        }
    }

    // === CANCEL-FIRST RE-ADD WINDOW GUARD (2026-07-06, Joe "continuous cancel rejects") =====
    // Cancel-first (processTrackedOrdersTheoMove) sends the stale-leg cancels IMMEDIATELY and
    // defers the fresh-pair re-add to the cancel-ack. A clean cancel evicts the leg locally
    // (evict_oid_locally on HTTP 200; benign 404 keeps it CANCEL_PENDING) so for a brief window
    // the stack owns NO live leg — which defeats the owns-legs guard above. If a reconcile pass
    // lands in that window it tears down and re-adopts the STALE oids still in orders.json (a
    // lagging snapshot that still lists the just-cancelled oids), then cancels those dead oids
    // (HTTP 404 storm) and detaches the real replacements (venue orphans). This is the exact
    // interleaved-orphan bug the owns-legs guard was built to prevent, reopened by cancel-first.
    //
    // Fix: while a cancel has completed very recently (within the cancel-ack timeout budget +
    // margin) and the re-add has not yet re-established a live leg, treat this exactly like
    // "strategy owns legs": no-op and let the cancel-first re-add place the fresh pair. This
    // aligns with the design intent ("orders.json is authoritative ONLY for initial seeding").
    // Operator-driven desk sync (require_desk_sync) still falls through so manual price/size
    // changes from the python desk apply immediately. Initial seed / restart re-adopt are
    // unaffected: they have no recent cancel, so both stamps are stale/zero.
    //
    // COVERAGE FIX (2026-07-06, Joe multi-stack XAU/SPY 404 storm): the cancel-first pair
    // cancel acks through on_cancel (desk_cancel_ack) which stamps mm_last_cancel_first_ms_,
    // NOT last_cancel_response_ms_ (only the synchronous runFull "cancel tracked leg" REST
    // result stamps that one). The DESK_RECOVERY→runFull re-add lands in the naked window
    // opened by the cancel-first cancel, so keying the guard on last_cancel_response_ms_
    // alone missed it and every re-add re-adopted the just-killed oids (404 + orphans). Take
    // the most recent of BOTH signals so the guard fires for the cancel-first window too.
    if (active && !require_desk_sync) {
        const std::int64_t cancel_ms =
            std::max(last_cancel_response_ms_.load(std::memory_order_acquire),
                     mm_last_cancel_first_ms_.load(std::memory_order_acquire));
        if (cancel_ms > 0) {
            const std::int64_t age_ms = mmSteadyMillis() - cancel_ms;
            const std::int64_t window_ms =
                static_cast<std::int64_t>(cfg.getMarketMakerCancelAckTimeoutMs()) + 2000;
            if (age_ms >= 0 && age_ms < window_ms) {
                if (utils::Logger::isInitialized() && Main().logger()) {
                    Main().logger()->info(
                        "[STRATEGY:{}] gateway-seed adopt skipped — cancel-first re-add window "
                        "(cancel {}ms ago < {}ms; orders.json oids are stale) stack_id={}",
                        getName(),
                        age_ms,
                        window_ms,
                        req_id);
                }
                return true;
            }
        }
    }

    // Capture pre-teardown active state for the quiet-timer arm decision below.
    // The quiet window's purpose is to give the operator a 10s grace after FIRST adoption
    // (e.g. binary restart re-adopts gateway-placed legs); arming it on every reconcile
    // turns it into an indefinite freeze (see comment block above + user log 17:54:*).
    const bool was_desk_active_at_entry = mm_desk_active_;

    // Multi-stack desk: never purge all EURUSD-PERP (etc.) adopts — that kills sibling mm_req_* rows and
    // breaks mmDeskTrackedOrdersAliveForMove / gateway modify. Drop only this strategy's prior OIDs.
    // IMPORTANT: detachLocalOrderById only removes local OM rows; it does NOT cancel at the venue.
    // Always REST-cancel previous stack legs through TrackedLeg CANCEL_PENDING (DeskReseed), then
    // detach local OM rows — avoids bypassing pending_cancel_intent / reconcile timeouts.
    {
        const OrderId prev_bid = bid_order_id_;
        const OrderId prev_ask = ask_order_id_;
        struct SeedCx {
            OrderId oid{0};
            Side side{Side::BUY};
        };
        std::vector<SeedCx> desk_reseed_cancels;
        {
            std::lock_guard<std::mutex> lk(tracked_orders_mutex_);
            auto queue_reseed = [&](std::vector<TrackedLeg>& vec) {
                for (auto& t : vec) {
                    if (t.state != MmLegState::ADOPTED || t.local_oid == 0) {
                        continue;
                    }
                    t.state = MmLegState::CANCEL_PENDING;
                    t.pending_cancel_intent = MmDeskCancelIntent::DeskReseed;
                    t.desk_cancel_timeout_retry_sent = false;
                    t.state_entered_steady_ms = mmSteadyMillis();
                    desk_reseed_cancels.push_back(SeedCx{t.local_oid, t.side});
                }
            };
            queue_reseed(tracked_bids_);
            queue_reseed(tracked_asks_);
        }
        // Route desk_reseed_cancel through the per-AX mover so the mm_desk_bg thread
        // (running reconcileMmReqStrategiesFromOrdersJson) never blocks on REST. Local
        // CANCEL_PENDING state was already set above under tracked_orders_mutex_.
        for (const auto& c : desk_reseed_cancels) {
            enqueueRestCancelOnMover(c.oid, c.side, "desk_reseed_cancel");
        }
        auto& om = orders::OrderManager::getInstance();
        if (prev_bid != 0) {
            om.detachLocalOrderById(prev_bid);
        }
        if (prev_ask != 0) {
            om.detachLocalOrderById(prev_ask);
        }
    }
    bid_order_id_ = 0;
    ask_order_id_ = 0;
    last_bid_price_ = 0.0;
    last_ask_price_ = 0.0;
    clearOpenOrderTracking();

    std::string bid_oid = j.value("bid_exchange_oid", "");
    std::string ask_oid = j.value("ask_exchange_oid", "");
    double bid_px = 0.0;
    double ask_px = 0.0;
    if (j.contains("bid_price") && j["bid_price"].is_number()) {
        bid_px = j["bid_price"].get<double>();
    }
    if (j.contains("ask_price") && j["ask_price"].is_number()) {
        ask_px = j["ask_price"].get<double>();
    }

    const double tick = mmEffResolvedQuoteTick(cfg);
    const double eps = (tick > 0.0 && std::isfinite(tick)) ? tick * 0.25 : 1e-9;

    const bool need_bid = bid_oid.empty() && bid_px > 0.0;
    const bool need_ask = ask_oid.empty() && ask_px > 0.0;
    if (need_bid || need_ask) {
        if (mmPbEnforce()) {
            // ENFORCE: resolve missing oids from the VenueOrdersCache — never GET here (this
            // adopt path is reachable on the mover via runFullMmQuoteCycle). If the cache is
            // stale/missing, skip resolution this cycle; adopt retries when the cache is fresh.
            const VenueOrdersSnapshot snap = VenueOrdersCache::instance().get(sym_mm);
            if (!snap.valid || snap.ageMs(VenueOrdersCache::nowSteadyMs()) > mmVenueCacheMaxAgeMs()) {
                if (utils::Logger::isInitialized()) {
                    Main().logger()->warn(
                        "[VENUE_CACHE_STALE] strategy={} ax={} desk-adopt oid resolve skipped — "
                        "will retry next cycle when cache is fresh",
                        getName(), sym_mm);
                }
            } else {
                for (const auto& row : snap.rows) {  // cache rows are already this-symbol filtered
                    if (row.oid.empty() || !(row.price > 0.0) || !std::isfinite(row.price)) {
                        continue;
                    }
                    if (need_bid && row.is_buy && bid_oid.empty() &&
                        std::fabs(row.price - bid_px) <= eps) {
                        bid_oid = row.oid;
                    }
                    if (need_ask && !row.is_buy && ask_oid.empty() &&
                        std::fabs(row.price - ask_px) <= eps) {
                        ask_oid = row.oid;
                    }
                }
            }
        } else {
            auto resp = Main().rest()->getOrdersGatewayRelative("/open-orders", {{"symbol", sym_mm}});
            if (!resp.is_success) {
                Main().logger()->warn("[STRATEGY:{}] desk sync: GET open-orders failed HTTP {}", getName(),
                    resp.status_code);
            } else {
                const nlohmann::json root = resp.parseJson();
                std::vector<nlohmann::json> rows;
                parseOpenOrdersBody(root, &rows);
                for (const auto& row : rows) {
                    if (!mmOpenOrdersRowMatchesAx(sym_mm, row)) {
                        continue;
                    }
                    const auto side_buy = rowSideIsBuy(row);
                    if (!side_buy.has_value()) {
                        continue;
                    }
                    const double px = deskRowPrice(row);
                    if (!(px > 0.0) || !std::isfinite(px)) {
                        continue;
                    }
                    const std::string oid = deskJsonOid(row);
                    if (oid.empty()) {
                        continue;
                    }
                    if (need_bid && *side_buy && bid_oid.empty() && std::fabs(px - bid_px) <= eps) {
                        bid_oid = oid;
                    }
                    if (need_ask && !*side_buy && ask_oid.empty() && std::fabs(px - ask_px) <= eps) {
                        ask_oid = oid;
                    }
                }
            }
        }
    }

    resetMmSessionFillTracking(
        qty_bid_cfg > 0.0 ? qty_bid_cfg : qty_cfg, qty_ask_cfg > 0.0 ? qty_ask_cfg : qty_cfg);
    mm_pending_accepts_ = 0;
    mm_pending_accepts_last_change_ms_.store(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now().time_since_epoch())
            .count(),
        std::memory_order_relaxed);

    auto& om = orders::OrderManager::getInstance();
    constexpr UserId kDeskUser = 0;

    if (!bid_oid.empty() && bid_px > 0.0 && qty_bid_cfg > 0.0) {
        const OrderId id = om.adoptExternalLimitOrder(
            kDeskUser, sym_mm, Side::BUY, bid_px, qty_bid_cfg, qty_bid_cfg, bid_oid, "");
        if (id != 0) {
            noteAdoptedWorkingOrder(id, sym_mm);
            bid_order_id_ = id;
            last_bid_price_ = bid_px;
        }
    }
    if (!ask_oid.empty() && ask_px > 0.0 && qty_ask_cfg > 0.0) {
        const OrderId id = om.adoptExternalLimitOrder(
            kDeskUser, sym_mm, Side::SELL, ask_px, qty_ask_cfg, qty_ask_cfg, ask_oid, "");
        if (id != 0) {
            noteAdoptedWorkingOrder(id, sym_mm);
            ask_order_id_ = id;
            last_ask_price_ = ask_px;
        }
    }
    {
        std::lock_guard<std::mutex> lk(tracked_orders_mutex_);
        tracked_bids_.clear();
        tracked_asks_.clear();

        if (bid_order_id_ != 0) {
            TrackedLeg tb;
            tb.local_oid = bid_order_id_;
            tb.exchange_oid = bid_oid;
            tb.exchange_px = bid_px;
            tb.qty = qty_bid_cfg;
            tb.remaining_qty = qty_bid_cfg;
            tb.side = Side::BUY;
            tb.state = MmLegState::ADOPTED;
            tb.state_entered_steady_ms = mmSteadyMillis();
            tracked_bids_.push_back(tb);
        }
        if (ask_order_id_ != 0) {
            TrackedLeg ta;
            ta.local_oid = ask_order_id_;
            ta.exchange_oid = ask_oid;
            ta.exchange_px = ask_px;
            ta.qty = qty_ask_cfg;
            ta.remaining_qty = qty_ask_cfg;
            ta.side = Side::SELL;
            ta.state = MmLegState::ADOPTED;
            ta.state_entered_steady_ms = mmSteadyMillis();
            tracked_asks_.push_back(ta);
        }
    }

    mm_desk_active_ = (bid_order_id_ != 0 || ask_order_id_ != 0);
    if (!req_id.empty() && mm_desk_active_) {
        mm_desk_stack_paused_ = false;
    }
    double theo_sig = 0.0;
    if (j.contains("theo_mid") && j["theo_mid"].is_number()) {
        theo_sig = j["theo_mid"].get<double>();
        if (std::isfinite(theo_sig) && theo_sig > 0.0) {
            last_theo_ = theo_sig;
            last_theo_requote_anchor_ = theo_sig;
            last_theo_requote_anchor_inited_ = true;
        }
    }

    if (stamp_first_ack_from_adopted) {
        if (bid_order_id_ != 0 || ask_order_id_ != 0) {
            mm_first_accept_seen_.store(true, std::memory_order_relaxed);
            const std::int64_t now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                              std::chrono::system_clock::now().time_since_epoch())
                                          .count();
            mm_first_accept_ms_.store(now_ms, std::memory_order_relaxed);
        } else {
            return false;
        }
    }

    // === QUIET-TIMER ARM POLICY (2026-05-07 — DISABLED) =====================================
    //
    // History:
    //   v1: armed on every reconcile pass → 10s gate + 5s reconcile = permanent block.
    //   v2: armed only on inactive→active transition. Looked correct, but ANY path that
    //       flipped `mm_desk_active_=false` (fill_requote @5622, feed_guardian @2152,
    //       desk recovery @2386, manual cleanup @4797) would arm the timer on the very next
    //       reconcile because `was_desk_active_at_entry` would now be false. User log
    //       18:22:33.567 caught this: gate fired with age=7918ms even though the strategy
    //       had been running for tens of seconds with healthy legs.
    //
    // v3 (this): NEVER arm the timer from this path. The
    //   `market_maker.desk_gateway_seed_repricing_quiet_ms` config still exists for
    //   backward compatibility, but `mm_desk_gateway_seed_steady_ms_` will only be set by:
    //     - explicit Python desk-sync clearing (line 7495 below) — sets to 0
    //     - explicit Python desk-sync arming via a future call site if reintroduced
    //     - active=false branch at the top of this function — sets to 0
    //   In other words: the gate is now opt-in only via Python desk integration. Steady-state
    //   operation never blocks moves on it.
    //
    //   The original feature intent (10s grace on binary restart so the operator's freshly
    //   placed orders aren't immediately moved by the strategy) is acknowledged but the
    //   feature was structurally incompatible with normal `mm_desk_active_=false` transitions
    //   used elsewhere in the codebase. If we ever want it back, the right shape is a
    //   "process-start steady_ms" timestamp set ONCE in `Strategy::onInit`, never reset by
    //   any subsequent state machine, and a separate `armed_once_` flag.
    if (require_desk_sync) {
        // Explicit Python desk sync — operator wants moves now, not a quiet window.
        mm_desk_gateway_seed_steady_ms_ = 0;
    }
    (void)was_desk_active_at_entry;  // intentionally unused under v3

    if (require_desk_sync) {
        Main().logger()->info(
            "[STRATEGY:{}] desk signal (Python) applied: sym={} bid_oid={} ask_oid={} bid_px={:.6f} ask_px={:.6f} "
            "qty_bid={:.2f} qty_ask={:.2f} quantity_field={:.2f} theo_mid_sig={:.6f} desk_active={}",
            getName(), sym_mm, bid_oid, ask_oid, bid_px, ask_px, qty_bid_cfg, qty_ask_cfg, qty_cfg, theo_sig,
            mm_desk_active_);
    } else {
        Main().logger()->info(
            "[STRATEGY:{}] orders.json gateway-seed adopt: sym={} bid_oid={} ask_oid={} bid_px={:.6f} "
            "ask_px={:.6f} qty_bid={:.2f} qty_ask={:.2f} theo_sig={:.6f} desk_active={}",
            getName(), sym_mm, bid_oid, ask_oid, bid_px, ask_px, qty_bid_cfg, qty_ask_cfg, theo_sig, mm_desk_active_);
    }

    return true;
}

void MakeMarketStrategy::armDeskAutorepriceForAllDeskStacksOnAx(const std::string& ax_symbol) {
    (void)ax_symbol;
}

bool MakeMarketStrategy::applyMmDeskSignalJson(const std::string& json_body) {
    (void)config::Config::getInstance().reloadPrimaryConfigFromDisk();
    nlohmann::json j;
    try {
        j = nlohmann::json::parse(json_body);
    } catch (...) {
        return false;
    }

    const std::string sym_sig = j.value("symbol", "");
    const std::string sym_mm = mmAxSymbol();
    if (!sym_sig.empty() && sym_sig != sym_mm) {
        Main().logger()->warn("[STRATEGY:{}] desk signal symbol mismatch sig={} mm={}", getName(), sym_sig,
            sym_mm);
        return false;
    }
    return applyMmDeskSeededJsonLocked(j, true, false);
}

int MakeMarketStrategy::mmDeskShutdownLogAndCancelTrackedLegs(
    std::vector<std::pair<std::string, std::string>>* out_pending_cancel_oid_by_ax) {
    if (!mmHasOrderRequestId()) {
        return 0;
    }
    struct Item {
        std::string exch_oid;
        OrderId local_id{0};
        Side side{Side::BUY};
    };
    std::vector<Item> items;
    {
        std::lock_guard<std::mutex> lk(tracked_orders_mutex_);
        auto collect = [&](const std::vector<TrackedLeg>& vec) {
            for (const auto& t : vec) {
                if (t.state != MmLegState::ADOPTED && !mmLegStateIsCancelInFlight(t.state) &&
                    t.state != MmLegState::PLACE_PENDING) {
                    continue;
                }
                // Phantom leg: no exchange oid AND no local order id means there is nothing on the
                // venue to cancel (e.g. a PLACE_PENDING slot whose accept never arrived). Skip it
                // entirely so we don't emit the misleading "cancelling leg ... oid=0" line or count
                // a cancel that is never sent.
                if (t.exchange_oid.empty() && t.local_oid == 0) {
                    continue;
                }
                const std::string oid_log =
                    t.exchange_oid.empty() ? std::to_string(static_cast<long long>(t.local_oid)) : t.exchange_oid;
                if (utils::Logger::isInitialized()) {
                    Main().logger()->info("[SHUTDOWN] cancelling leg ax={} oid={}", mmAxSymbol(), oid_log);
                }
                items.push_back(Item{t.exchange_oid, t.local_oid, t.side});
            }
        };
        collect(tracked_bids_);
        collect(tracked_asks_);
    }
    auto& om = orders::OrderManager::getInstance();
    int n = 0;
    for (const auto& it : items) {
        std::string track_oid = it.exch_oid;
        if (track_oid.empty() && it.local_id != 0) {
            if (const auto op = om.getOrder(it.local_id)) {
                track_oid = op->exchange_order_id;
            }
        }
        if (!it.exch_oid.empty()) {
            // CANCEL-SPEED FIX: shutdown path also uses POST-first to drain quickly.
            api::HttpResponse cr;
            {
                nlohmann::json cancel_body = nlohmann::json::object();
                cancel_body["oid"] = it.exch_oid;
                cr = Main().rest()->cancelOrderGateway(cancel_body.dump());
            }
            if (!cr.is_success) {
                cr = Main().rest()->cancelOrder(it.exch_oid);
            }
            (void)cr;
            ++n;
        } else if (it.local_id != 0) {
            (void)mmSendRestCancelByOrderId(it.local_id, it.side, getName(), "shutdown_cancel");
            ++n;
        }
        if (out_pending_cancel_oid_by_ax && !track_oid.empty()) {
            out_pending_cancel_oid_by_ax->emplace_back(mmAxSymbol(), track_oid);
        }
    }
    mm_pending_accepts_ = 0;
    mm_pending_accepts_last_change_ms_.store(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now().time_since_epoch())
            .count(),
        std::memory_order_relaxed);
    {
        std::lock_guard<std::mutex> lk(mm_accept_pending_mutex_);
        mm_pending_accept_cycle_reason_.clear();
    }
    mmNotifyProductPlacementGate(mmAxSymbol());
    return n;
}

void MakeMarketStrategy::deskShutdownCancelPollTruncateOrdersJson(const std::string& shutdown_reason,
                                                                  std::int64_t& out_cancel_poll_ms) {
    out_cancel_poll_ms = 0;
    const auto t0 = std::chrono::steady_clock::now();
    std::vector<std::string> ax_list;
    int n_legs = 0;
    std::vector<std::pair<std::string, std::string>> pending_shutdown_cancel_oid;
    for (const auto& sp : StrategyManager::getInstance().getAllStrategies()) {
        auto mm = std::dynamic_pointer_cast<MakeMarketStrategy>(sp);
        if (!mm || mm->mmOrderRequestId().empty()) {
            continue;
        }
        ax_list.push_back(mm->mmAxSymbol());
        n_legs += mm->mmDeskShutdownLogAndCancelTrackedLegs(&pending_shutdown_cancel_oid);
    }
    std::sort(ax_list.begin(), ax_list.end());
    ax_list.erase(std::unique(ax_list.begin(), ax_list.end()), ax_list.end());
    std::string ax_join;
    for (std::size_t i = 0; i < ax_list.size(); ++i) {
        if (i) {
            ax_join += ",";
        }
        ax_join += ax_list[i];
    }
    if (utils::Logger::isInitialized()) {
        Main().logger()->info("[SHUTDOWN] cancelling {} legs: ax_list=[{}]", n_legs, ax_join);
    }

    auto& cfg = config::Config::getInstance();
    const int budget_ms = cfg.getMarketMakerShutdownCancelTimeoutMs();
    auto& rest = *Main().rest();
    while (!pending_shutdown_cancel_oid.empty()) {
        const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                                 std::chrono::steady_clock::now() - t0)
                                 .count();
        if (elapsed >= budget_ms) {
            if (utils::Logger::isInitialized()) {
                Main().logger()->warn(
                    "[SHUTDOWN] timeout: {} legs still open after {}ms — proceeding",
                    static_cast<int>(pending_shutdown_cancel_oid.size()),
                    static_cast<int>(latencyDisplayMs(elapsed)));
            }
            break;
        }
        const auto r = rest.getOrders();
        if (r.is_success) {
            for (auto it = pending_shutdown_cancel_oid.begin(); it != pending_shutdown_cancel_oid.end();) {
                if (!mmRestOrdersBodyContainsExchangeOid(r.body, it->second)) {
                    if (utils::Logger::isInitialized()) {
                        Main().logger()->info(
                            "[SHUTDOWN] cancel ack: ax={} oid={} elapsed_ms={}",
                            it->first,
                            it->second,
                            static_cast<int>(latencyDisplayMs(elapsed)));
                    }
                    it = pending_shutdown_cancel_oid.erase(it);
                } else {
                    ++it;
                }
            }
        }
        if (pending_shutdown_cancel_oid.empty()) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }

    // Escalation: if we timed out with legs still resting, do NOT silently abandon them. Fire a
    // per-symbol cancel_all for every AX that still has an unconfirmed leg so we are far less
    // likely to leave live orders on the venue. This is a belt before the unscoped cancel_all
    // sweep that runs later in executeShutdown; both are cheap and idempotent.
    if (!pending_shutdown_cancel_oid.empty()) {
        std::vector<std::string> residual_ax;
        residual_ax.reserve(pending_shutdown_cancel_oid.size());
        for (const auto& pr : pending_shutdown_cancel_oid) {
            residual_ax.push_back(pr.first);
        }
        std::sort(residual_ax.begin(), residual_ax.end());
        residual_ax.erase(std::unique(residual_ax.begin(), residual_ax.end()), residual_ax.end());
        for (const auto& ax : residual_ax) {
            try {
                const auto r = rest.cancelAllOrders(ax);
                if (utils::Logger::isInitialized()) {
                    Main().logger()->warn(
                        "[SHUTDOWN] escalate cancel_all ax={} http_ok={} — legs unconfirmed after "
                        "poll timeout",
                        ax,
                        r.is_success ? 1 : 0);
                }
            } catch (const std::exception& e) {
                if (utils::Logger::isInitialized()) {
                    Main().logger()->warn("[SHUTDOWN] escalate cancel_all ax={} threw: {}", ax, e.what());
                }
            } catch (...) {
                if (utils::Logger::isInitialized()) {
                    Main().logger()->warn("[SHUTDOWN] escalate cancel_all ax={} threw unknown", ax);
                }
            }
        }
        if (utils::Logger::isInitialized()) {
            Main().logger()->warn(
                "[SHUTDOWN] residual {} legs unconfirmed after escalation — venue cancel_all sweep "
                "will run next",
                static_cast<int>(pending_shutdown_cancel_oid.size()));
        }
    }

    (void)mmWriteOrdersJsonEmptyAtConfigPath();
    const auto t1 = std::chrono::steady_clock::now();
    out_cancel_poll_ms = std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();
    if (utils::Logger::isInitialized()) {
        if (pending_shutdown_cancel_oid.empty()) {
            Main().logger()->info("[SHUTDOWN] all cancels acked in {}ms — orders.json cleared",
                                  latencyDisplayMs(out_cancel_poll_ms));
        }
    }

    const std::string logd = Main().getLogDirectory();
    if (!logd.empty()) {
        namespace fs = std::filesystem;
        nlohmann::json exitj;
        exitj["shutdown_reason"] = shutdown_reason;
        exitj["cancel_poll_elapsed_ms"] = latencyDisplayMs(out_cancel_poll_ms);
        exitj["unix_time_ms"] = static_cast<std::int64_t>(
            std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch())
                .count());
        std::error_code ec;
        fs::create_directories(logd, ec);
        const fs::path p = fs::path(logd) / "last_process_exit.json";
        const std::string tmp = p.string() + ".tmp";
        try {
            std::ofstream ofs(tmp, std::ios::trunc | std::ios::binary);
            if (ofs) {
                ofs << exitj.dump(2) << "\n";
            }
        } catch (...) {
        }
        fs::rename(tmp, p, ec);
    }
}

// =============================================================================
// Logging
// =============================================================================

void MakeMarketStrategy::logStrategyDecision(const std::string& decision, 
                                              const std::string& details) {
    auto& config = config::Config::getInstance();
    
    if (config.getBool("logging.log_strategy_decisions", true)) {
        Main().logger()->info("[STRATEGY:{}] {}: {}", getName(), decision, details);
        Main().logger()->log_event("STRATEGY", getName(), decision + ": " + details);
    }
}

} // namespace strategy
} // namespace architect
