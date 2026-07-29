/**
 * @file main.cpp
 * @brief 25-Step Simulation Runner using the Architect Platform Core
 * 
 * Usage: ./trading_client <config_path> <simulation_date>
 * 
 * Arguments:
 *   config_path     - Path to JSON configuration file
 *   simulation_date - Date for simulation in YYYYMMDD format (e.g., 20260130)
 * 
 * Example:
 *   ./trading_client config/default_config.json 20260130
 * 
 * 25-Step Framework:
 *   Steps 1-6:   Core System Initialization
 *   Steps 7-12:  API & Connectivity
 *   Steps 13-17: Market Data Infrastructure
 *   Steps 18-21: Trading Infrastructure
 *   Steps 22-23: Pre-Trade Verification
 *   Step 24:     Trading Loop (runs until SIGINT/SIGTERM/SIGHUP)
 *   Step 25:     Graceful Shutdown
 * 
 * Output:
 *   Creates logs/{date}/ directory with:
 *     - {date}.platform.log    (main log)
 *     - {date}.config_*.json   (configuration)
 *     - {date}.*.csv           (structured data logs)
 */

#include <iostream>
#include <string>
#include <csignal>
#include <cstdlib>
#include <cstdint>
#include <cmath>
#include <cctype>
#include <cerrno>
#include <exception>
#include <thread>
#include <set>
#include <atomic>
#include <chrono>
#include <unordered_map>
#include <unordered_set>
#include <sstream>
#include <optional>
#include <algorithm>
#include <functional>  // std::hash (single-instance lock key)
#include <unistd.h>   // getpid, write, STDERR_FILENO (POSIX)
#include <fcntl.h>    // open, O_CREAT (single-instance lock)
#include <sys/file.h> // flock, LOCK_EX (single-instance lock)
#include <mutex>
#include <signal.h>   // sigaltstack, sigaction, SA_ONSTACK
#include <cstring>
#include <string.h>   // strsignal(3) — main thread only; not in async signal path

#include <nlohmann/json.hpp>

// Platform Core
#include "core/Platform.h"
#include "core/StartupSequence.h"
#include "strategy/MakeMarketStrategy.h"
#include "strategy/mm_orders_reconcile_shared.h"
#include "strategy/Strategy.h"
#include "config/Config.h"
#include "marketdata/ExternalFeedManager.h"

#include <filesystem>
#include <fstream>

using namespace architect;

namespace {

std::string makeMmStrategyNameForLeg(const std::string& ax, std::size_t n_legs) {
    if (n_legs <= 1) {
        return "make_market";
    }
    std::string s = "make_market_";
    for (char c : ax) {
        if (std::isalnum(static_cast<unsigned char>(c))) {
            s.push_back(c);
        } else {
            s.push_back('_');
        }
    }
    return s;
}

// Desk stacks may encode spread as width_bps / width_ticks / width; JSON numbers are
// sometimes floats. Read flexibly so width_bps is never silently dropped to 0 (which
// forces the global MM spread fallback in MakeMarketStrategy).
int mmJsonIntFromNumber(const nlohmann::json& j, const std::string& key) {
    if (!j.contains(key)) {
        return 0;
    }
    const auto& v = j.at(key);
    if (v.is_number_integer()) {
        return v.get<int>();
    }
    if (v.is_number_float()) {
        return static_cast<int>(std::llround(v.get<double>()));
    }
    if (v.is_string()) {
        try {
            return std::stoi(v.get_ref<const std::string&>());
        } catch (const std::exception&) {
            return 0;
        }
    }
    return 0;
}

int mmOrdersJsonStackWidthBps(const nlohmann::json& s) {
    auto read_width = [](const nlohmann::json& o) -> int {
        int w = mmJsonIntFromNumber(o, "width_bps");
        if (w > 0) {
            return w;
        }
        w = mmJsonIntFromNumber(o, "width_ticks");
        if (w > 0) {
            return w;
        }
        w = mmJsonIntFromNumber(o, "width");
        return w > 0 ? w : 0;
    };
    int w = read_width(s);
    if (w > 0) {
        return w;
    }
    if (s.contains("params") && s["params"].is_object()) {
        w = read_width(s["params"]);
    }
    return w > 0 ? w : 0;
}

/** Per-stack contracts: root first, then legacy desk ``params`` object (same pattern as width). */
int mmOrdersJsonStackOrderSize(const nlohmann::json& s) {
    int q = mmJsonIntFromNumber(s, "order_size");
    if (q > 0) {
        return q;
    }
    if (s.contains("params") && s["params"].is_object()) {
        q = mmJsonIntFromNumber(s["params"], "order_size");
    }
    return q > 0 ? q : 0;
}

int mmOrdersJsonStackOrderSizeStep(const nlohmann::json& s) {
    int st = mmJsonIntFromNumber(s, "order_size_step");
    if (st <= 0) {
        st = mmJsonIntFromNumber(s, "order_step");
    }
    if (st > 0) {
        return st;
    }
    if (s.contains("params") && s["params"].is_object()) {
        st = mmJsonIntFromNumber(s["params"], "order_size_step");
        if (st <= 0) {
            st = mmJsonIntFromNumber(s["params"], "order_step");
        }
    }
    return st > 0 ? st : 0;
}

/** Desk mm_req stacks must never use unrelated global ``market_maker.width`` / spread_ticks. */
void mmDeskCoerceStackSpreadWidth(const std::string& ax,
                                  architect::config::MarketMakerManualStack& stack,
                                  core::Platform& main) {
    if (stack.id.empty()) {
        return;
    }
    if (stack.width_bps > 0) {
        stack.width_ticks = stack.width_bps;
        return;
    }
    auto& cfg = architect::config::Config::getInstance();
    const int inst_w = cfg.getMarketMakerDefaultWidthBpsForAx(ax);
    const int floor_w = cfg.getMarketMakerDeskWidthEmergencyFloorBps();
    const int chosen = inst_w > 0 ? inst_w : floor_w;
    if (main.logger()) {
        main.logger()->warn(
            "[MM_ORDERS] desk stack width missing/zero stack_id={} ax={} — using width_bps={} (source={})",
            stack.id,
            ax,
            chosen,
            inst_w > 0 ? std::string("instrument_default") : std::string("desk_width_emergency_floor"));
    }
    stack.width_bps = chosen;
    stack.width_ticks = chosen;
}

std::string sanitizeNameToken(const std::string& in) {
    std::string s;
    for (char c : in) {
        if (std::isalnum(static_cast<unsigned char>(c))) {
            s.push_back(c);
        } else {
            s.push_back('_');
        }
    }
    if (s.empty()) {
        return "s";
    }
    if (s.size() > 40) {
        s.resize(40);
    }
    return s;
}

void attachMmProviders(core::Platform& main, const std::shared_ptr<strategy::MakeMarketStrategy>& mm) {
    mm->set_theo_provider([&main](const std::string& sym) {
        return main.externalFeed()->getTheoPrice(sym);
    });
    mm->set_market_quote_provider([&main](const std::string& sym) -> std::optional<strategy::MarketQuote> {
        auto tick = main.marketdata()->getTick(sym);
        if (!tick) {
            return std::nullopt;
        }
        strategy::MarketQuote quote;
        quote.bid = tick->bid_price;
        quote.ask = tick->ask_price;
        quote.valid = (quote.bid > 0 && quote.ask > 0);
        quote.timestamp = std::chrono::steady_clock::now();
        return quote;
    });
    mm->add_timer(0);
    mm->registerFeedCallback();
    main.strategyManager()->registerStrategy(mm);
}

/**
 * @brief Deterministic name for the per-leg "base" MM strategy.
 *
 * `makeMmStrategyNameForLeg` returns the same string at startup and on every
 * reconcile pass so we can detect whether the base strategy for a leg already
 * exists in the StrategyManager.
 */
std::string mmBaseStrategyNameForLeg(const std::string& ax, std::size_t n_legs) {
    return makeMmStrategyNameForLeg(ax, n_legs);
}

/**
 * @brief Atomically write per-feed health + per-leg trade-gate status to a
 *        JSON file the desk watches.
 *
 * The desk reads `logs/mm_feed_health.json` (path overridable by config
 * `desk.mm_feed_health_path`) every snapshot and renders the three pills
 * (Mettraders / Hyperliquid / Neon) plus a per-leg "blocked: feed down" indicator.
 * The schema is intentionally flat so the Python side can `json.load` it
 * without binding to any C++ type.
 */
void writeFeedHealthSnapshot(core::Platform& main) {
    namespace fs = std::filesystem;
    auto& cfg = config::Config::getInstance();
    const std::string rel = cfg.getString("mm_desk.mm_feed_health_path", "logs/mm_feed_health.json");
    fs::path out_path(rel);
    if (!out_path.is_absolute()) {
        out_path = fs::current_path() / out_path;
    }
    std::error_code ec;
    fs::create_directories(out_path.parent_path(), ec);

    auto& efm = marketdata::ExternalFeedManager::getInstance();
    const auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();

    nlohmann::json j;
    j["updated_ms"] = now_ms;
    j["enabled"] = efm.isEnabled();
    j["running"] = efm.isRunning();

    auto pack_feed = [&](const std::string& key,
                         bool up,
                         std::int64_t last_ms,
                         const std::string& err) {
        nlohmann::json fj;
        fj["up"] = up;
        fj["last_ok_ms"] = last_ms;
        fj["age_ms"] = (last_ms > 0 ? (now_ms - last_ms) : -1);
        fj["last_error"] = err;
        j[key] = std::move(fj);
    };
    pack_feed("mettraders",  efm.isMettradersUp(),  efm.lastMettradersOkMs(),  efm.lastMettradersError());
    pack_feed("hyperliquid", efm.isHlUp(),   efm.lastHlOkMs(),   efm.lastHlError());
    pack_feed("neon",        efm.isNeonUp(), efm.lastNeonOkMs(), efm.lastNeonError());

    const int up_count =
        (efm.isMettradersUp() ? 1 : 0) + (efm.isHlUp() ? 1 : 0) + (efm.isNeonUp() ? 1 : 0);
    j["up_count"] = up_count;

    // Per-leg block status (one entry per registered MM strategy).
    nlohmann::json legs = nlohmann::json::array();
    if (auto* sm = main.strategyManager()) {
        for (const auto& s : sm->getAllStrategies()) {
            auto mm = std::dynamic_pointer_cast<strategy::MakeMarketStrategy>(s);
            if (!mm) continue;
            nlohmann::json lj;
            lj["name"] = mm->getName();
            lj["ax_symbol"] = mm->mmAxSymbol();
            lj["theo_source"] = mm->mmTheoSource();
            lj["blocked_by_feed"] = mm->mmIsBlockedByFeed();
            lj["block_reason"] = mm->mmFeedBlockReason();
            legs.push_back(std::move(lj));
        }
    }
    j["legs"] = std::move(legs);

    const std::string tmp_str = out_path.string() + ".tmp";
    {
        std::ofstream f(tmp_str, std::ios::trunc | std::ios::binary);
        if (!f) return;
        f << j.dump();
        f.flush();
    }
    fs::rename(tmp_str, out_path, ec);
}

/**
 * @brief Deterministic strategy name for an orders.json-driven manual order.
 *
 * Includes a sanitized prefix of the request UUID so two stacks on the same
 * AX product can run side-by-side without name collisions, and so the desk's
 * cancel flow can find the strategy by (ax_symbol, request_id) without a
 * full scan.
 */
std::string mmRequestStrategyName(const std::string& ax, const std::string& request_id) {
    std::string id_short = sanitizeNameToken(request_id);
    if (id_short.size() > 8) id_short.resize(8);
    if (id_short.empty()) id_short = "req";
    return std::string("mm_req_") + sanitizeNameToken(ax) + "_" + id_short;
}

/**
 * @brief Resolve a config-relative or absolute path against the working
 *        directory. Mirrors the convention used by mm_feed_health.json so
 *        Python and C++ agree on where the file lives.
 */
std::filesystem::path resolveLogsRelPath(const std::string& rel,
                                         const std::string& fallback) {
    namespace fs = std::filesystem;
    const std::string s = rel.empty() ? fallback : rel;
    fs::path p(s);
    if (!p.is_absolute()) {
        p = fs::current_path() / p;
    }
    return p;
}

/**
 * @brief Spawn one MakeMarketStrategy for one stack of one product from
 *        `orders.json`.
 *
 * `orders.json` is the desk's desired-state config (Python sole writer):
 *   {
 *     "products": {
 *       "PAIR-PERP": {
 *         "theo_source": "neon_fix",
 *         "theo_venue_symbol": "CCY/PAIR",
 *         "stacks": [ {"id": "...", "width_ticks": 4, ...}, ... ]
 *       }
 *     }
 *   }
 *
 * For each (product, stack) we synthesise a `MarketMakerManualStack` and a
 * fake `MarketMakerInstrumentLeg` and register a `MakeMarketStrategy` named
 * `mmRequestStrategyName(ax, stack.id)`. The math (theo-move requote, fill
 * reload, max-position guard) is unchanged — this path only varies the
 * source of stack params.
 */
/**
 * Hot-apply resolved per-product limits (`max_position`, `max_reload_cycles`)
 * to every running MM strategy on that AX so base + mm_req stacks share one risk view.
 */
struct MmResolvedLimits {
    int max_position{0};
    std::string max_position_source{"global"};
    int max_reload_cycles{0};
    std::string max_reload_source{"global"};
};

void applyDeskProductLimits(
    core::Platform& main,
    const std::unordered_map<std::string, MmResolvedLimits>& limits_by_ax) {
    auto* sm = main.strategyManager();
    if (!sm || limits_by_ax.empty()) {
        return;
    }
    std::unordered_map<std::string, std::pair<int, int>> ax_max_po_change;
    for (const auto& strat : sm->getAllStrategies()) {
        auto mm = std::dynamic_pointer_cast<strategy::MakeMarketStrategy>(strat);
        if (!mm) {
            continue;
        }
        const std::string& ax = mm->mmAxSymbol();
        auto it = limits_by_ax.find(ax);
        if (it == limits_by_ax.end()) {
            continue;
        }
        const int cur_po = mm->mmInstrumentMaxPosition();
        const int target_po = it->second.max_position > 0 ? it->second.max_position : 0;
        const std::string po_source =
            it->second.max_position_source.empty() ? std::string("global") : it->second.max_position_source;
        bool changed = false;
        if (target_po != cur_po) {
            if (!ax_max_po_change.count(ax)) {
                ax_max_po_change[ax] = {cur_po, target_po};
            }
            mm->setMmInstrumentMaxPosition(target_po, po_source);
            changed = true;
        }

        const int cur_reload = mm->mmInstrumentMaxReloadCycles();
        const int target_reload = it->second.max_reload_cycles > 0 ? it->second.max_reload_cycles : 0;
        const std::string reload_source =
            it->second.max_reload_source.empty() ? std::string("global") : it->second.max_reload_source;
        if (target_reload != cur_reload) {
            mm->setMmInstrumentMaxReloadCycles(target_reload, reload_source);
            changed = true;
        }
        if (changed) {
            main.logger()->info(
                "[MM_ORDERS] desk instrument limits hot-update strategy={} ax={} max_position:{}->{}({}) max_reload:{}->{}({}) — quote check",
                mm->getName(), ax,
                cur_po, target_po, po_source,
                cur_reload, target_reload, reload_source);
        }
    }
    for (const auto& [ax, delta] : ax_max_po_change) {
        strategy::MakeMarketStrategy::mmNotifyInstrumentMaxPositionChanged(ax, delta.first, delta.second);
    }
}

void mmSpawnFromOrdersStack(core::Platform& main,
                            events::DateInt date_int,
                            const std::string& ax,
                            const std::string& theo_src,
                            const std::string& theo_venue,
                            const std::string& reference_fix_symbol,
                            const std::string& order_symbol,
                            const architect::config::MarketMakerManualStack& stack,
                            int leg_max_position,
                            const std::string& leg_max_position_source,
                            int leg_max_reload_cycles,
                            const std::string& leg_max_reload_source,
                            double leg_tick_size,
                            double leg_theo_scale) {
    if (ax.empty() || theo_src.empty() || stack.id.empty()) {
        main.logger()->warn(
            "[MM_ORDERS] orders.json stack rejected — missing ax/theo_source/id (ax='{}', src='{}', id='{}')",
            ax, theo_src, stack.id);
        return;
    }
    auto* sm = main.strategyManager();
    if (!sm) return;
    const std::string name = mmRequestStrategyName(ax, stack.id);
    if (auto existing = sm->getStrategy(name)) {
        // Already running — but the desk may have just upgraded the row (desk_seeded false→true,
        // newly placed_bid_price/placed_ask_price/exchange_oids). Refresh the strategy's local
        // manual_stack snapshot and retry adoption so the gateway-placed pair gets tracked.
        // Without this refresh, the strategy holds whatever snapshot it had at spawn time and the
        // adopt retry in onFeedUpdate keeps reading stale (empty) prices forever.
        if (auto mm_existing = std::dynamic_pointer_cast<strategy::MakeMarketStrategy>(existing)) {
            mm_existing->setOptionalManualStack(stack);
            if (stack.desk_seeded &&
                (stack.placed_bid_price > 0.0 || stack.placed_ask_price > 0.0)) {
                const bool adopted_now = mm_existing->mmDeskBootstrapAdopt();
                main.logger()->info(
                    "[MM_ORDERS] reconcile refresh+adopt strategy={} stack_id={} desk_seeded=1 "
                    "result={} bid_oid='{}' ask_oid='{}' bid_px={:.6f} ask_px={:.6f}",
                    name, stack.id, adopted_now ? "adopted" : "deferred",
                    stack.bid_exchange_oid, stack.ask_exchange_oid,
                    stack.placed_bid_price, stack.placed_ask_price);
            }
        }
        return;
    }
    auto& spawn_cfg = config::Config::getInstance();
    // Multi-pair coexistence is the default: each desk stack on an AX spawns its own
    // mm_req_<AX>_<stack_id> strategy with independent tracked legs and per-pair width/min_drift.
    // Setting market_maker.allow_multi_mm_per_ax=false reverts to the legacy "single quoter per AX,
    // additional stacks adopt into the base make_market_<AX>" behavior.
    if (!spawn_cfg.getBool("market_maker.allow_multi_mm_per_ax", true)) {
        std::shared_ptr<strategy::MakeMarketStrategy> existing_for_ax;
        const auto mm_insts = spawn_cfg.getMarketMakerInstruments();
        const std::size_t n_mm_legs = mm_insts.empty() ? std::size_t{1} : mm_insts.size();
        const std::string base_mm_name = mmBaseStrategyNameForLeg(ax, n_mm_legs);
        if (auto strat = sm->getStrategy(base_mm_name)) {
            existing_for_ax = std::dynamic_pointer_cast<strategy::MakeMarketStrategy>(strat);
            if (existing_for_ax && existing_for_ax->mmAxSymbol() != ax) {
                existing_for_ax.reset();
            }
        }
        if (!existing_for_ax) {
            for (const auto& strat : sm->getAllStrategies()) {
                auto mm = std::dynamic_pointer_cast<strategy::MakeMarketStrategy>(strat);
                if (!mm || mm->mmAxSymbol() != ax) {
                    continue;
                }
                if (mm->getName().rfind("mm_req_", 0) == 0) {
                    continue;
                }
                existing_for_ax = std::move(mm);
                break;
            }
        }
        if (existing_for_ax) {
            const bool adopted = existing_for_ax->mmDeskReconcileAdoptStack(stack);
            if (adopted) {
                main.logger()->info(
                    "[MM_ORDERS] desk stack adopted into existing quoter strategy={} stack_id={} "
                    "bid_oid={} ask_oid={} bid_px={:.6f} ask_px={:.6f}",
                    existing_for_ax->getName(),
                    stack.id,
                    stack.bid_exchange_oid.empty() ? std::string("(none)") : stack.bid_exchange_oid,
                    stack.ask_exchange_oid.empty() ? std::string("(none)") : stack.ask_exchange_oid,
                    stack.placed_bid_price,
                    stack.placed_ask_price);
                existing_for_ax->ensureStartupQuotePair("desk_stack_adopted_into_base_quoter");
            } else {
                static std::unordered_set<std::string> mm_spawn_skip_logged;
                const std::string log_key = ax + '\x1f' + stack.id;
                if (mm_spawn_skip_logged.insert(log_key).second) {
                    main.logger()->debug(
                        "[MM_ORDERS] spawn skipped name={} ax={} stack_id={} — "
                        "market_maker.allow_multi_mm_per_ax=false and this ax already has quoter {}. "
                        "Extra stacks share one exchange position; set market_maker.allow_multi_mm_per_ax=true "
                        "to run multiple desk stacks on the same AX. (Desk adopt did not apply; check "
                        "desk_seeded, gateway OIDs, and prices in orders.json.)",
                        name,
                        ax,
                        stack.id,
                        existing_for_ax->getName());
                }
            }
            return;
        }
    }
    auto mm = std::make_shared<strategy::MakeMarketStrategy>(name);
    // For products not in market_maker.instruments[] the desk sends the
    // theo venue symbol on the request — we plumb it as the leg's
    // reference_fix_symbol so getTheoPrice() looks it up on the right feed.
    // If the user didn't supply one we fall back to theo_venue.
    const std::string ref = !reference_fix_symbol.empty() ? reference_fix_symbol
                          : !theo_venue.empty()           ? theo_venue
                                                          : ax;
    mm->setMmInstrumentLeg(ax, order_symbol.empty() ? ax : order_symbol, ref);
    mm->setMmTheoSource(theo_src);
    mm->setMmOrderRequestId(stack.id);
    mm->setOptionalManualStack(stack);
    // Product-level cap (preferred). 0 = no per-leg override; strategy falls
    // through to global market_maker.max_position.
    if (leg_max_position > 0) {
        const std::string src = leg_max_position_source.empty() ? std::string("instrument")
                                                                 : leg_max_position_source;
        mm->setMmInstrumentMaxPosition(leg_max_position, src);
    }
    if (leg_max_reload_cycles > 0) {
        const std::string src = leg_max_reload_source.empty() ? std::string("instrument")
                                                               : leg_max_reload_source;
        mm->setMmInstrumentMaxReloadCycles(leg_max_reload_cycles, src);
    }
    if (leg_tick_size > 0.0) {
        mm->setMmInstrumentTick(leg_tick_size);
    }
    if (leg_theo_scale > 0.0) {
        mm->setMmTheoScale(leg_theo_scale);
    }
    attachMmProviders(main, mm);
    try {
        mm->initialize(date_int);
        mm->start();
    } catch (const std::exception& e) {
        main.logger()->warn("[MM_ORDERS] spawn failed name={} stack_id={} err={}", name, stack.id, e.what());
        try { sm->unregisterStrategy(name); } catch (...) {}
        return;
    }
    // Adopt the desk-seeded gateway-placed pair NOW. Without this, the fresh strategy stays
    // PASSIVE (initialize() sets passive_until_first_manual_order_=true) and onFeedUpdate skips
    // early because have_working_signal=false. The periodic adoption retry inside
    // runFullMmQuoteCycle is unreachable from that state, so the strategy would otherwise sit
    // forever in "PASSIVE, waiting for manual orders" while the desk-placed orders rest on the
    // exchange un-tracked.
    if (stack.desk_seeded && (stack.placed_bid_price > 0.0 || stack.placed_ask_price > 0.0)) {
        const bool adopted_now = mm->mmDeskBootstrapAdopt();
        main.logger()->info(
            "[MM_ORDERS] spawn-time adoption strategy={} stack_id={} desk_seeded=1 result={} "
            "bid_oid='{}' ask_oid='{}' bid_px={:.6f} ask_px={:.6f}",
            name, stack.id, adopted_now ? "adopted" : "deferred",
            stack.bid_exchange_oid, stack.ask_exchange_oid,
            stack.placed_bid_price, stack.placed_ask_price);
    }
    const int eff_cap =
        leg_max_position > 0 ? leg_max_position : config::Config::getInstance().getMarketMakerMaxPosition();
    const int eff_max_reload =
        leg_max_reload_cycles > 0 ? leg_max_reload_cycles
                                  : config::Config::getInstance().getMarketMakerMaxReloadCycles();
    const std::string cap_src = leg_max_position > 0
                                    ? (leg_max_position_source.empty() ? std::string("instrument")
                                                                       : leg_max_position_source)
                                    : std::string("global");
    const std::string reload_src = leg_max_reload_cycles > 0
                                       ? (leg_max_reload_source.empty() ? std::string("instrument")
                                                                        : leg_max_reload_source)
                                       : std::string("global");
    main.logger()->info(
        "[MM_ORDERS] Spawned strategy name={} ax={} stack_id={} theo_source={} venue='{}' "
        "width_bps={} width_ticks={} order_size={} max_position={} (src={}) max_reload_cycles={} (src={})",
        name, ax, stack.id, theo_src, theo_venue,
        stack.width_bps, stack.width_ticks, stack.order_size, eff_cap, cap_src, eff_max_reload, reload_src);
}

/**
 * @brief Tear down the strategy bound to a stack id (cancel + unregister).
 *
 * For ``mm_req_*`` desk-only strategies, this function REST-cancels every tracked
 * leg via ``mmDeskShutdownLogAndCancelTrackedLegs`` BEFORE calling stop()+unregister.
 * Note: ``BaseStrategy::stop()`` itself does NOT cancel orders — it only flips
 * ``running_=false``. Skipping the explicit cancel here would orphan the live legs
 * on the exchange; if either side then gets hit, the on_fill → cancel-opposite +
 * requote-with-skew chain has no actor to run, so the position grows uncovered.
 *
 * When the desk stack was adopted into the config-registered ``make_market_*`` quoter,
 * the same stack_id is cleared in place via ``mmDeskReconcileClearStackBinding``:
 * venue cancels for tracked desk legs, desk binding cleared, base strategy stays
 * registered.
 */
namespace {
std::mutex g_mm_teardown_mu;
std::unordered_set<std::string> g_mm_teardown_in_progress;

std::string mmTeardownKey(const std::string& ax, const std::string& stack_id) {
    return ax + '\x1e' + stack_id;
}
}  // namespace

void mmTearDownByStackId(core::Platform& main, const std::string& ax,
                         const std::string& stack_id) {
    auto* sm = main.strategyManager();
    if (!sm || stack_id.empty() || ax.empty()) {
        return;
    }
    const std::string teardown_key = mmTeardownKey(ax, stack_id);
    {
        std::lock_guard<std::mutex> lk(g_mm_teardown_mu);
        if (!g_mm_teardown_in_progress.insert(teardown_key).second) {
            main.logger()->warn(
                "[MM_ORDERS] desk stack teardown already in progress ax={} stack_id={}",
                ax,
                stack_id);
            return;
        }
    }
    struct TeardownInProgressGuard {
        std::string key;
        ~TeardownInProgressGuard() {
            std::lock_guard<std::mutex> lk(g_mm_teardown_mu);
            g_mm_teardown_in_progress.erase(key);
        }
    } teardown_guard{teardown_key};

    const std::string mm_req_name = mmRequestStrategyName(ax, stack_id);
    auto target = sm->getStrategy(mm_req_name);
    std::string registered_name = mm_req_name;
    if (!target) {
        for (const auto& strat : sm->getAllStrategies()) {
            auto mm = std::dynamic_pointer_cast<strategy::MakeMarketStrategy>(strat);
            if (!mm || mm->mmAxSymbol() != ax || mm->mmOrderRequestId() != stack_id) {
                continue;
            }
            target = strat;
            registered_name = mm->getName();
            break;
        }
    }
    if (!target) {
        return;
    }
    if (registered_name.rfind("mm_req_", 0) == 0) {
        // Venue source-of-truth guard (2026-06-11): flip the stack to TEARDOWN so any
        // mover lambda still resolving this stack by name bails out before touching the
        // venue, and so the venue-orphan poll path cannot mark it ACTIVE again mid-teardown.
        // Both the venue poll and this desk teardown converge on the same state machine.
        if (auto mm_state = std::dynamic_pointer_cast<strategy::MakeMarketStrategy>(target)) {
            mm_state->mmTransitionStackState(strategy::MmStackState::TEARDOWN, "desk_teardown");
        }
        // Stop new feed/mover cycles before waiting on the lease — queued mover work re-resolves
        // by name and bails when !isRunning().
        try {
            target->shutdown();
        } catch (...) {
        }
        if (auto mm = std::dynamic_pointer_cast<strategy::MakeMarketStrategy>(target)) {
            try {
                mm->mmTeardownUnblockProductPlacementLease();
            } catch (...) {
            }
        }
        // Wait for the mover thread to drop the product placement lease before destroying
        // this strategy — otherwise MmProductPlacementLeaseScope release can UAF (2026-06-05 JPY).
        strategy::MakeMarketStrategy::mmWaitForStrategyProductPlacementLeaseRelease(
            ax, registered_name, 5000);
        // CRITICAL: BaseStrategy::stop() does NOT cancel exchange orders — it only flips
        // running_=false. Without an explicit cancel pass here the strategy's tracked
        // bid/ask sit on the venue as orphans and any subsequent fill arrives at a
        // strategy that no longer exists, so the on_fill → cancel-opposite + requote-with-skew
        // chain never runs. We REST-cancel every tracked leg first, then unregister.
        int n_cancelled = 0;
        if (auto mm = std::dynamic_pointer_cast<strategy::MakeMarketStrategy>(target)) {
            try {
                n_cancelled = mm->mmDeskShutdownLogAndCancelTrackedLegs(nullptr);
            } catch (...) {
                // mmDeskShutdownLogAndCancelTrackedLegs already logs per-leg; swallow.
            }
        }
        try {
            sm->unregisterStrategy(registered_name);
        } catch (...) {
        }
        // Venue source-of-truth guard (2026-06-11): the stack is unregistered — mark DEAD.
        // The object is still pinned alive by the local `target` shared_ptr until scope exit,
        // so this transition is safe and records the final lifecycle edge.
        if (auto mm_dead = std::dynamic_pointer_cast<strategy::MakeMarketStrategy>(target)) {
            mm_dead->mmTransitionStackState(strategy::MmStackState::DEAD, "unregistered");
        }
        // CONCURRENCY (2026-05-20 audit, fix C7): drain in-flight EventManager callbacks before
        // releasing the local `target` shared_ptr. dispatchEvent() copies subscribers under
        // shared_lock then invokes them WITHOUT the lock held (EventManager.cpp:195-201). A
        // dispatch that started before our teardownSubscriptions() may still be mid-call on
        // another thread. We hold `target` alive here so [this]-captures remain valid, sleep
        // briefly to let those calls finish, then return (last shared_ptr drops on scope exit).
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        main.logger()->info(
            "[MM_ORDERS] desk stack torn down: REST-cancelled {} tracked leg(s) and unregistered "
            "name={} stack_id={}",
            n_cancelled, registered_name, stack_id);
        return;
    }
    auto mm = std::dynamic_pointer_cast<strategy::MakeMarketStrategy>(target);
    if (!mm) {
        return;
    }
    mm->mmDeskReconcileClearStackBinding();
    main.logger()->info(
        "[MM_ORDERS] desk stack removed from orders.json — venue legs cancelled + desk binding cleared on "
        "strategy={} stack_id={}",
        registered_name,
        stack_id);
}

/**
 * @brief Reconcile running MM strategies against the desired-state
 *        `orders.json` (Python-written).
 *
 * Runs every ~1s on the desk background thread (and once at startup, which
 * gives us restart recovery for free — every stack present in the file gets
 * a fresh strategy). Spawn unconditional of feed health: the per-leg trade
 * gate already prevents quoting when the bound feed is down, and the desk
 * shows the stack as `blocked_by_feed=true` until it recovers.
 *
 * Reconciliation rule:
 *   - desired = set of (ax_symbol, stack_id) read from orders.json
 *   - running = every `mm_req_*` MakeMarketStrategy currently registered
 *   - for s in desired \ running:  spawn it.
 *   - for s in running \ desired:  stop + unregister it (cancel-all on AX).
 *
 * The user's "C++ must always check existence in orders.json before placing
 * a theo-based order" requirement falls out of this directly: a strategy is
 * only registered while its stack_id is in the file, so any place call has
 * already passed the existence check at the previous reconcile tick.
 */
/**
 * Multiple MakeMarketStrategy instances on the same AX symbol share one exchange
 * position and one visible order set per side; each strategy still issues its own
 * cancel-all / replace cycles — a common misconfiguration when a base
 * `make_market_*` leg and one or more `mm_req_*` desk stacks are both active.
 */
void warnMultipleMmOnSameAx(core::Platform& main) {
    auto* sm = main.strategyManager();
    if (!sm) {
        return;
    }
    std::unordered_map<std::string, int> count_by_ax;
    std::unordered_map<std::string, std::vector<std::string>> names_by_ax;
    for (const auto& s : sm->getAllStrategies()) {
        auto mm = std::dynamic_pointer_cast<strategy::MakeMarketStrategy>(s);
        if (!mm) {
            continue;
        }
        const std::string ax = mm->mmAxSymbol();
        if (ax.empty()) {
            continue;
        }
        ++count_by_ax[ax];
        names_by_ax[ax].push_back(mm->getName());
    }
    static std::unordered_set<std::string> noted_ax;
    auto& cfg_local = config::Config::getInstance();
    const bool multi_allowed = cfg_local.getBool("market_maker.allow_multi_mm_per_ax", true);
    for (const auto& [ax, n] : count_by_ax) {
        if (n < 2) {
            continue;
        }
        if (!noted_ax.insert(ax).second) {
            continue;
        }
        std::ostringstream names;
        for (std::size_t i = 0; i < names_by_ax[ax].size(); ++i) {
            if (i > 0) {
                names << ", ";
            }
            names << names_by_ax[ax][i];
        }
        if (multi_allowed) {
            // Multi-pair on the same AX is now the default. Treat this as informational, not a
            // warning. Per-stack cancels (mmCancelTrackedLegThisStackGateway) ensure each strategy
            // only operates on its own exchange OIDs, so siblings do not race each other.
            main.logger()->info(
                "[MM_ORDERS] multi-pair active on ax={}: {} strategies ({}). Each tracks its own "
                "legs and moves per its own width/min_drift; shared net exchange position is "
                "reconciled via syncInventoryFromExchangePortfolio.",
                ax,
                n,
                names.str());
        } else {
            main.logger()->warn(
                "[MM_ORDERS] {} MakeMarketStrategy instances on ax={} ({}) with "
                "market_maker.allow_multi_mm_per_ax=false. Each issues its own cancel-all and "
                "pair-enforce cycles which can race the others' working orders. Either remove "
                "extra stacks from orders.json, drop the duplicate base instrument leg, or set "
                "market_maker.allow_multi_mm_per_ax=true to enable per-stack mm_req_* quoters.",
                n,
                ax,
                names.str());
        }
    }
}

namespace {

std::uint64_t g_mm_orders_freeze_last_seq = 0;

/**
 * Desk-driven freeze: Python writes mm_desk.orders_config_freeze_signal_path as
 * {"active":true|false,"seq":<monotonic>}. On each new seq, C++ either copies live
 * orders.json to orders_config_frozen_copy_path (active) or resumes reading the live file.
 * While active, reconcileOrdersConfig reads the frozen copy so theo-based moves do not race
 * a half-written live orders.json during "Add order" UI flows.
 */
void mmOrdersJsonMaybeRefreshFreezeCopy(core::Platform& main) {
    namespace fs = std::filesystem;
    auto& cfg = config::Config::getInstance();
    const std::string freeze_rel =
        cfg.getString("mm_desk.orders_config_freeze_signal_path", "logs/mm_orders_freeze.json");
    const fs::path freeze_p = resolveLogsRelPath(freeze_rel, "logs/mm_orders_freeze.json");
    const std::string live_rel = cfg.getString("mm_desk.mm_orders_config_path", "logs/orders.json");
    const fs::path live_p = resolveLogsRelPath(live_rel, "logs/orders.json");
    const std::string copy_rel =
        cfg.getString("mm_desk.orders_config_frozen_copy_path", "logs/orders.json.frozen_cpp");
    const fs::path frozen_p = resolveLogsRelPath(copy_rel, "logs/orders.json.frozen_cpp");

    std::error_code ec;
    if (!fs::is_regular_file(freeze_p, ec)) {
        if (g_mm_orders_reconcile_from_frozen_copy) {
            g_mm_orders_reconcile_from_frozen_copy = false;
            main.logger()->info(
                "[MM_ORDERS] orders.json freeze signal missing — reconcile reads live orders.json again");
        }
        return;
    }
    std::string raw;
    {
        std::ifstream f(freeze_p, std::ios::binary);
        if (!f) {
            return;
        }
        std::ostringstream ss;
        ss << f.rdbuf();
        raw = ss.str();
    }
    if (raw.empty()) {
        return;
    }
    try {
        const auto j = nlohmann::json::parse(raw);
        const bool active = j.value("active", false);
        std::uint64_t seq = 0;
        if (j.contains("seq")) {
            if (j["seq"].is_number_unsigned()) {
                seq = j["seq"].get<std::uint64_t>();
            } else if (j["seq"].is_number_integer()) {
                const auto iv = j["seq"].get<std::int64_t>();
                if (iv > 0) {
                    seq = static_cast<std::uint64_t>(iv);
                }
            }
        }
        if (seq == 0) {
            return;
        }
        if (seq == g_mm_orders_freeze_last_seq) {
            return;
        }
        g_mm_orders_freeze_last_seq = seq;
        if (active) {
            if (fs::is_regular_file(live_p, ec)) {
                fs::create_directories(frozen_p.parent_path(), ec);
                fs::copy_file(live_p, frozen_p, fs::copy_options::overwrite_existing);
            } else {
                fs::create_directories(frozen_p.parent_path(), ec);
                std::ofstream o(frozen_p, std::ios::trunc | std::ios::binary);
                if (o) {
                    o << R"({"version":1,"updated_ms":0,"products":{}})";
                }
            }
            g_mm_orders_reconcile_from_frozen_copy = true;
            main.logger()->info(
                "[MM_ORDERS] orders.json viewer-freeze seq={} — reconcile uses frozen snapshot {}",
                seq,
                frozen_p.string());
        } else {
            g_mm_orders_reconcile_from_frozen_copy = false;
            main.logger()->info(
                "[MM_ORDERS] orders.json viewer-freeze cleared seq={} — reconcile uses live {}",
                seq,
                live_p.string());
        }
    } catch (const std::exception& e) {
        main.logger()->warn("[MM_ORDERS] freeze signal parse error: {} ({})", e.what(), freeze_p.string());
    }
}

}  // namespace

// Set by reconcileOrdersConfig's MASS-TEARDOWN GUARDRAIL to demand a confirming
// re-read on the next mm_desk_bg tick (bypassing the mtime cache), even when the
// file has not changed. Both this function and reconcileMmReqStrategiesFromOrdersJson
// run on the single mm_desk_bg_thread, so a plain file-static bool is race-free.
static bool s_reconcile_force_reread = false;

void reconcileOrdersConfig(core::Platform& main, events::DateInt date_int) {
    namespace fs = std::filesystem;
    mmOrdersJsonMaybeRefreshFreezeCopy(main);
    auto& cfg = config::Config::getInstance();
    const std::string rel = cfg.getString("mm_desk.mm_orders_config_path",
                                          "logs/orders.json");
    const fs::path live_p = resolveLogsRelPath(rel, "logs/orders.json");
    const std::string frozen_rel =
        cfg.getString("mm_desk.orders_config_frozen_copy_path", "logs/orders.json.frozen_cpp");
    const fs::path frozen_p = resolveLogsRelPath(frozen_rel, "logs/orders.json.frozen_cpp");
    const fs::path p = g_mm_orders_reconcile_from_frozen_copy ? frozen_p : live_p;

    nlohmann::json doc = nlohmann::json::object();
    std::error_code ec;
    if (fs::exists(p, ec)) {
        std::string raw;
        {
            std::ifstream f(p, std::ios::binary);
            if (f) {
                std::ostringstream ss;
                ss << f.rdbuf();
                raw = ss.str();
            }
        }
        if (!raw.empty()) {
            try {
                doc = nlohmann::json::parse(raw);
            } catch (const std::exception& e) {
                main.logger()->warn("[MM_ORDERS] orders.json parse error: {} (size={}) — skipping reconcile pass",
                                    e.what(), raw.size());
                return;
            }
        }
    }

    // Desk-authorised intent marker (written by the GUI clear/remove-all path). When
    // present it means the shrink is deliberate, so the mass-teardown guardrail below
    // must NOT defer for a confirming read — the emergency pull-all is applied at once.
    std::string desk_intent;
    if (doc.is_object() && doc.contains("intent") && doc["intent"].is_string()) {
        desk_intent = doc["intent"].get<std::string>();
    }

    // Pre-index the configured legs by ax_symbol so we can read leg-level overrides
    // (max_position / tick_size / theo_scale) without re-walking the list per stack.
    // orders.json may override individual fields for ad-hoc one-offs.
    //
    // `mm_legs` must outlive `inst_leg_by_ax` pointers.
    const auto mm_legs = cfg.getMarketMakerInstruments();
    struct LegOverrides {
        int    max_position{0};
        int    max_reload_cycles{0};
        double tick_size{0.0};
        double theo_scale{0.0};
    };
    std::unordered_map<std::string, LegOverrides> leg_overrides_by_ax;
    std::unordered_map<std::string, const config::MarketMakerInstrumentLeg*> inst_leg_by_ax;
    for (const auto& L : mm_legs) {
        if (L.ax_symbol.empty()) continue;
        LegOverrides ov;
        ov.max_position = L.max_position;
        ov.max_reload_cycles = L.max_reload_cycles;
        ov.tick_size    = L.tick_size;
        ov.theo_scale   = L.theo_scale;
        leg_overrides_by_ax[L.ax_symbol] = ov;
        inst_leg_by_ax[L.ax_symbol]      = &L;
    }

    // Build desired set + spawn anything missing from top-level `stacks[]` (desk-owned).
    // (Legacy `products[ax].stacks[]` is kept in sync by the desk for UI; reconcile iterates root only.)
    std::set<std::pair<std::string, std::string>> desired;  // (ax_symbol, stack_id)
    std::unordered_map<std::string, MmResolvedLimits> desk_resolved_limits_by_ax;
    // Product-level max_position = max(max_position) across every desk stack on that AX.
    // Per-stack values in orders.json may differ; the exchange has one net position per
    // symbol, so all mm_req_* strategies on the same AX must share the highest configured cap.
    std::unordered_map<std::string, int> instrument_max_po_by_ax;
    std::size_t root_stacks_count = 0;
    if (doc.is_object() && doc.contains("stacks") && doc["stacks"].is_array()) {
        root_stacks_count = doc["stacks"].size();
    }
    main.logger()->info("[ORDERS_JSON_READ] iterating root_stacks found {} entries (source={})",
                        root_stacks_count,
                        g_mm_orders_reconcile_from_frozen_copy ? std::string("frozen_cpp") : std::string("live"));
    if (doc.is_object() && doc.contains("stacks") && doc["stacks"].is_array()) {
        for (const auto& s : doc["stacks"]) {
            if (!s.is_object()) {
                continue;
            }
            const std::string ax = s.value("ax_symbol", std::string{});
            const std::string id = s.value("stack_id", s.value("id", std::string{}));
            if (ax.empty() || id.empty()) {
                continue;
            }
            desired.emplace(ax, id);

            const config::MarketMakerInstrumentLeg* cfg_leg = nullptr;
            {
                const auto itl = inst_leg_by_ax.find(ax);
                if (itl != inst_leg_by_ax.end()) {
                    cfg_leg = itl->second;
                }
            }
            std::string theo_src = s.value("theo_source", std::string{});
            std::string theo_venue = s.value("theo_venue_symbol", std::string{});
            std::string ref_fix = s.value("reference_fix_symbol", std::string{});
            std::string ord_sym = s.value("order_symbol", std::string{});
            if (cfg_leg) {
                theo_src = cfg_leg->theo_source;
                theo_venue = cfg_leg->theo_venue_symbol;
                if (!cfg_leg->reference_fix_symbol.empty()) {
                    ref_fix = cfg_leg->reference_fix_symbol;
                }
                ord_sym = cfg_leg->order_symbol.empty() ? ax : cfg_leg->order_symbol;
            } else if (ord_sym.empty()) {
                ord_sym = ax;
            }

            LegOverrides leg_ov;
            {
                auto it = leg_overrides_by_ax.find(ax);
                if (it != leg_overrides_by_ax.end()) {
                    leg_ov = it->second;
                }
            }
            int leg_max_po = 0;
            std::string leg_max_po_source = "global";
            int leg_max_reload = 0;
            std::string leg_max_reload_source = "global";
            const int stack_max_po = static_cast<int>(s.value("max_position", s.value("max_po", 0)));
            const int stack_max_reload =
                static_cast<int>(s.value("max_reload_cycles", s.value("max_reload", 0)));
            if (stack_max_po > 0) {
                leg_max_po = stack_max_po;
                leg_max_po_source = "override";
            } else if (leg_ov.max_position > 0) {
                leg_max_po = leg_ov.max_position;
                leg_max_po_source = "instrument";
            }
            if (stack_max_reload > 0) {
                leg_max_reload = stack_max_reload;
                leg_max_reload_source = "override";
            } else if (leg_ov.max_reload_cycles > 0) {
                leg_max_reload = leg_ov.max_reload_cycles;
                leg_max_reload_source = "instrument";
            }
            if (leg_max_po > 0) {
                instrument_max_po_by_ax[ax] = std::max(instrument_max_po_by_ax[ax], leg_max_po);
            }
            if (leg_max_reload > 0) {
                auto& lim_slot = desk_resolved_limits_by_ax[ax];
                lim_slot.max_reload_cycles =
                    std::max(lim_slot.max_reload_cycles, leg_max_reload);
                if (lim_slot.max_reload_source.empty() ||
                    lim_slot.max_reload_cycles == leg_max_reload) {
                    lim_slot.max_reload_source = leg_max_reload_source;
                }
            }

            double leg_tick = s.value("tick_size", s.value("tick", 0.0));
            if (leg_tick <= 0.0) {
                leg_tick = leg_ov.tick_size;
            }
            double leg_scale = s.value("theo_scale", 0.0);
            if (leg_scale <= 0.0) {
                leg_scale = leg_ov.theo_scale;
            }

            architect::config::MarketMakerManualStack stack;
            stack.id = id;
            const int w_bps = mmOrdersJsonStackWidthBps(s);
            stack.width_bps = w_bps;
            stack.width_ticks = w_bps;
            stack.order_size = mmOrdersJsonStackOrderSize(s);
            const int ostep = mmOrdersJsonStackOrderSizeStep(s);
            if (ostep > 0) {
                stack.order_size_step = ostep;
            } else if (cfg_leg && cfg_leg->order_size_step > 0) {
                stack.order_size_step = cfg_leg->order_size_step;
            }
            stack.max_position = static_cast<int>(s.value("max_position", 0));
            stack.adjust_position = static_cast<int>(s.value("adjust_position", 0));
            stack.adjust_ticks = static_cast<int>(s.value("adjust_ticks", 0));
            stack.min_theo_move_ticks_to_requote = static_cast<int>(s.value(
                "min_theo_move_ticks_to_requote", s.value("min_drift_ticks", 0)));
            stack.max_reload_cycles = static_cast<int>(s.value("max_reload_cycles", 0));
            if (s.contains("desk_seeded") && s["desk_seeded"].is_boolean()) {
                stack.desk_seeded = s["desk_seeded"].get<bool>();
            }
            if (s.contains("bid_exchange_oid") && s["bid_exchange_oid"].is_string()) {
                stack.bid_exchange_oid = s["bid_exchange_oid"].get<std::string>();
            }
            if (s.contains("ask_exchange_oid") && s["ask_exchange_oid"].is_string()) {
                stack.ask_exchange_oid = s["ask_exchange_oid"].get<std::string>();
            }
            if (s.contains("bid_price") && s["bid_price"].is_number()) {
                stack.placed_bid_price = s["bid_price"].get<double>();
            }
            if (s.contains("ask_price") && s["ask_price"].is_number()) {
                stack.placed_ask_price = s["ask_price"].get<double>();
            }
            if (s.contains("bid_qty") && s["bid_qty"].is_number_integer()) {
                stack.placed_bid_qty = s["bid_qty"].get<int>();
            } else if (s.contains("bid_quantity") && s["bid_quantity"].is_number()) {
                stack.placed_bid_qty = static_cast<int>(s["bid_quantity"].get<double>());
            }
            if (s.contains("ask_qty") && s["ask_qty"].is_number_integer()) {
                stack.placed_ask_qty = s["ask_qty"].get<int>();
            } else if (s.contains("ask_quantity") && s["ask_quantity"].is_number()) {
                stack.placed_ask_qty = static_cast<int>(s["ask_quantity"].get<double>());
            }
            // Per-stack pricer-snapshot linear transform. Must be parsed here on the
            // root `stacks[]` path (the live reconcile source) — the legacy
            // `products[].stacks[]` parser in Config.cpp also reads these, but the
            // engine only reconciles from the root array. Missing this read silently
            // disables the transform and the strategy quotes off raw theo, which on a
            // venue with a different price scale leads to immediate cross-the-book
            // taker fills.
            if (s.contains("quote_snapshot") && s["quote_snapshot"].is_number()) {
                stack.quote_snapshot = s["quote_snapshot"].get<double>();
            }
            if (s.contains("pricer_snapshot") && s["pricer_snapshot"].is_number()) {
                stack.pricer_snapshot = s["pricer_snapshot"].get<double>();
            }
            if (s.contains("slope") && s["slope"].is_number()) {
                stack.slope = s["slope"].get<double>();
            }
            stack.mm_move_enabled = true;

            mmDeskCoerceStackSpreadWidth(ax, stack, main);

            const int inst_max_po =
                [&]() -> int {
                    auto it = instrument_max_po_by_ax.find(ax);
                    if (it != instrument_max_po_by_ax.end() && it->second > 0) {
                        return it->second;
                    }
                    return leg_max_po;
                }();
            const std::string inst_max_po_source =
                (inst_max_po > 0 && inst_max_po != leg_max_po) ? std::string("instrument_aggregate")
                                                                 : leg_max_po_source;
            mmSpawnFromOrdersStack(main, date_int, ax, theo_src, theo_venue, ref_fix, ord_sym, stack,
                                   inst_max_po, inst_max_po_source, leg_max_reload, leg_max_reload_source,
                                   leg_tick, leg_scale);
        }
    }

    // Also honour products[ax].max_position from orders.json (desk may have raised the
    // instrument cap without every stack row carrying the new value yet).
    if (doc.is_object() && doc.contains("products") && doc["products"].is_object()) {
        for (const auto& [ax_key, prod_val] : doc["products"].items()) {
            if (!prod_val.is_object()) {
                continue;
            }
            const int prod_mp = static_cast<int>(prod_val.value("max_position", 0));
            if (prod_mp > 0) {
                instrument_max_po_by_ax[ax_key] = std::max(instrument_max_po_by_ax[ax_key], prod_mp);
            }
        }
    }
    for (const auto& [ax_sym, inst_mp] : instrument_max_po_by_ax) {
        if (inst_mp <= 0) {
            continue;
        }
        auto& lim = desk_resolved_limits_by_ax[ax_sym];
        lim.max_position = inst_mp;
        lim.max_position_source = "instrument_aggregate";
    }

    applyDeskProductLimits(main, desk_resolved_limits_by_ax);

    // Tear down anything running that no longer appears in the file.
    if (auto* sm = main.strategyManager()) {
        std::vector<std::pair<std::string, std::string>> to_remove;  // (ax, stack_id)
        int running_mm_req = 0;
        for (const auto& s : sm->getAllStrategies()) {
            auto mm = std::dynamic_pointer_cast<strategy::MakeMarketStrategy>(s);
            if (!mm) continue;
            const std::string& rid = mm->mmOrderRequestId();
            if (rid.empty()) continue;  // base/leg strategy, not orders.json-driven
            ++running_mm_req;
            if (desired.count({mm->mmAxSymbol(), rid}) == 0) {
                to_remove.emplace_back(mm->mmAxSymbol(), rid);
            }
        }
        // Stable, sorted list of the stacks this pass would remove (used both for the
        // change log and as the mass-teardown confirmation fingerprint).
        std::string removed;
        {
            std::vector<std::string> ids;
            ids.reserve(to_remove.size());
            for (const auto& [ax_r, id_r] : to_remove) {
                ids.push_back(ax_r + ":" + id_r);
            }
            std::sort(ids.begin(), ids.end());
            for (const auto& s : ids) {
                if (!removed.empty()) {
                    removed += ",";
                }
                removed += s;
            }
        }
        static std::optional<std::size_t> s_last_reconcile_desired_count;
        static std::optional<int> s_last_reconcile_running_mm_req;
        const std::size_t desired_count = desired.size();
        const bool count_changed =
            !s_last_reconcile_desired_count || *s_last_reconcile_desired_count != desired_count ||
            !s_last_reconcile_running_mm_req || *s_last_reconcile_running_mm_req != running_mm_req ||
            !to_remove.empty();
        if (count_changed && utils::Logger::isInitialized()) {
            main.logger()->warn(
                "[MM_RECONCILE_STACK_COUNT_CHANGE] desired={} running_mm_req={} removing={} "
                "removed_stacks={}",
                desired_count,
                running_mm_req,
                to_remove.size(),
                removed.empty() ? std::string("(none)") : removed);
        }
        s_last_reconcile_desired_count = desired_count;
        s_last_reconcile_running_mm_req = running_mm_req;

        // === MASS-TEARDOWN GUARDRAIL (2026-07-14) ==================================
        // "Orders wiped out of nowhere": a viewer-freeze cleared onto a live orders.json
        // that had momentarily shrunk to ~1 stack, so this reconcile tore down every
        // OTHER running stack (desk_teardown -> cancel its bid+ask). Guard: when a single
        // pass would remove >= mass_min stacks, DEFER on first sighting and only proceed
        // if a second, immediately-forced re-read yields the IDENTICAL removal set. A
        // transient / partial / stale file (or a freeze<->live race) recovers on the
        // confirming read and the teardown is cancelled; a genuine desk removal (persists
        // across both reads) still proceeds ~1s later. Small edits (add / single remove)
        // are never gated. Complements the desk-side mass-shrink write guard.
        auto& cfg_guard = config::Config::getInstance();
        const bool guard_enabled =
            cfg_guard.getBool("mm_desk.reconcile_mass_teardown_guard", true);
        const int mass_min =
            std::max(2, cfg_guard.getInt("mm_desk.reconcile_mass_teardown_min", 2));
        // A desk-authorised remove-all/clear-all bypasses the defer: the shrink is
        // deliberate, so tear down immediately (no confirming-read delay).
        const bool intent_remove_all =
            (desk_intent == "remove_all" || desk_intent == "clear_all");
        const bool is_mass =
            static_cast<int>(to_remove.size()) >= mass_min && !intent_remove_all;
        if (intent_remove_all && !to_remove.empty() && utils::Logger::isInitialized()) {
            main.logger()->warn(
                "[MM_RECONCILE_MASS_TEARDOWN_INTENT] desk intent='{}' — authorised removal of "
                "{} stack(s), bypassing the confirm-read defer",
                desk_intent, to_remove.size());
        }
        static std::string s_pending_mass_teardown_fp;
        if (guard_enabled && is_mass) {
            const std::string fp = std::to_string(desired_count) + "#" +
                                   std::to_string(running_mm_req) + "|" + removed;
            if (s_pending_mass_teardown_fp != fp) {
                // First sighting → do NOT tear down; force a confirming re-read next tick.
                s_pending_mass_teardown_fp = fp;
                s_reconcile_force_reread = true;
                if (utils::Logger::isInitialized()) {
                    main.logger()->warn(
                        "[MM_RECONCILE_MASS_TEARDOWN_DEFERRED] desired={} running_mm_req={} "
                        "would_remove={} removed_stacks={} — suspicious desired-state shrink; "
                        "NOT tearing down this pass, awaiting a second confirming read "
                        "(re-submit stacks now if this was accidental)",
                        desired_count, running_mm_req, to_remove.size(),
                        removed.empty() ? std::string("(none)") : removed);
                }
                warnMultipleMmOnSameAx(main);
                return;  // skip teardown until confirmed
            }
            // Second consecutive identical observation → confirmed; fall through to teardown.
            if (utils::Logger::isInitialized()) {
                main.logger()->warn(
                    "[MM_RECONCILE_MASS_TEARDOWN_CONFIRMED] desired={} running_mm_req={} "
                    "removing={} removed_stacks={} — shrink persisted across two reads; "
                    "proceeding with teardown",
                    desired_count, running_mm_req, to_remove.size(),
                    removed.empty() ? std::string("(none)") : removed);
            }
            s_pending_mass_teardown_fp.clear();
        } else if (!s_pending_mass_teardown_fp.empty()) {
            // A previously-deferred mass teardown no longer applies (file recovered or the
            // removal set fell below the threshold) → cancel it.
            if (utils::Logger::isInitialized()) {
                main.logger()->warn(
                    "[MM_RECONCILE_MASS_TEARDOWN_RECOVERED] prior deferred mass teardown "
                    "cancelled (now removing={} desired={}) — live desired-state recovered",
                    to_remove.size(), desired_count);
            }
            s_pending_mass_teardown_fp.clear();
        }

        for (const auto& [ax, id] : to_remove) {
            mmTearDownByStackId(main, ax, id);
        }
    }

    warnMultipleMmOnSameAx(main);
}

/**
 * @brief Runtime poller path for desk-driven ``mm_req_*`` stacks (spawn / re-seed / tear-down).
 *
 * Invoked from the ~1 Hz ``mm_desk_bg_thread`` in ``main()`` after ``mmOrdersJsonMaybeRefreshFreezeCopy``.
 * Calls ``reconcileOrdersConfig`` only when the active reconcile file (live vs frozen snapshot)
 * last-write time changes, so config reload + GUI snapshots still run every second without re-parsing JSON.
 */
void reconcileMmReqStrategiesFromOrdersJson(core::Platform& main, events::DateInt date_int) {
    namespace fs = std::filesystem;
    static std::optional<fs::file_time_type> s_orders_json_mtime;
    static std::optional<bool> s_reconcile_read_frozen{};
    static bool s_had_orders_file{false};

    auto& cfg = config::Config::getInstance();
    const std::string rel = cfg.getString("mm_desk.mm_orders_config_path", "logs/orders.json");
    const fs::path live_p = resolveLogsRelPath(rel, "logs/orders.json");
    const std::string frozen_rel =
        cfg.getString("mm_desk.orders_config_frozen_copy_path", "logs/orders.json.frozen_cpp");
    const fs::path frozen_p = resolveLogsRelPath(frozen_rel, "logs/orders.json.frozen_cpp");
    const bool read_frozen = g_mm_orders_reconcile_from_frozen_copy;
    const fs::path p = read_frozen ? frozen_p : live_p;

    if (s_reconcile_read_frozen != read_frozen) {
        s_reconcile_read_frozen = read_frozen;
        s_orders_json_mtime.reset();
    }

    std::error_code ec;
    if (!fs::is_regular_file(p, ec)) {
        if (s_had_orders_file) {
            s_had_orders_file = false;
            s_orders_json_mtime.reset();
            reconcileOrdersConfig(main, date_int);
        }
        return;
    }

    const auto mtime = fs::last_write_time(p, ec);
    if (ec) {
        return;
    }

    s_had_orders_file = true;
    // The mass-teardown guardrail defers a suspicious shrink and asks for one confirming
    // re-read even if the file's mtime is unchanged — honour that here.
    const bool force_reread = s_reconcile_force_reread;
    s_reconcile_force_reread = false;
    if (!force_reread && s_orders_json_mtime.has_value() && *s_orders_json_mtime == mtime) {
        return;
    }
    s_orders_json_mtime = mtime;
    reconcileOrdersConfig(main, date_int);
}

/**
 * @brief Atomically write the live state of every orders.json-driven MM
 *        strategy to `mm_orders.json` (the GUI's read-only view).
 *
 * `orders.json` carries the *desired* params (Python-owned). This file is
 * the *live* projection (C++-owned) keyed by the same `stack_id` so the
 * desk can join the two in the UI: per stack we surface the live AX bid /
 * ask order_ids, latest prices/qtys, blocked-by-feed flag, fills counter
 * and the most recent fill identifier. An entry only appears once the
 * strategy has seen at least one AX accept (`mmHasFirstAcceptHappened`)
 * so a reject doesn't leave a phantom row in the GUI.
 */
void writeMmOrdersActiveSnapshot(core::Platform& main) {
    namespace fs = std::filesystem;
    auto& cfg = config::Config::getInstance();
    const std::string rel = cfg.getString("mm_desk.mm_orders_active_path",
                                          "logs/mm_orders.json");
    const fs::path out = resolveLogsRelPath(rel, "logs/mm_orders.json");
    std::error_code ec;
    fs::create_directories(out.parent_path(), ec);

    const auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();

    nlohmann::json arr = nlohmann::json::array();
    if (auto* sm = main.strategyManager()) {
        for (const auto& s : sm->getAllStrategies()) {
            auto mm = std::dynamic_pointer_cast<strategy::MakeMarketStrategy>(s);
            if (!mm) continue;
            const std::string& rid = mm->mmOrderRequestId();
            if (rid.empty()) continue;
            if (!mm->mmHasFirstAcceptHappened()) continue;

            nlohmann::json e;
            // `request_id` field name is preserved for the desk's existing JS;
            // semantically it is the orders.json `stack_id` for this row.
            e["request_id"]            = rid;
            e["stack_id"]              = rid;
            e["strategy_name"]         = mm->getName();
            e["ax_symbol"]             = mm->mmAxSymbol();
            // HOLD ACK (Phase 3, per-instrument-cancel-all): the engine's LIVE effective
            // per-instrument gate for this ax_symbol, derived from the same in-memory config
            // that drives placement. Combined with the top-level `config_mtime_ms` below, the
            // desk can prove the engine has OBSERVED a hold it wrote (D5 step 2) before it
            // fires the venue cancel-all — closing the in-flight-place orphan race. Read-only
            // projection field; nothing on the order path consumes it.
            e["mm_orders_enabled_effective"] =
                cfg.isMarketMakerEnabled() &&
                cfg.getMarketMakerMmOrdersEnabledForSymbol(mm->mmAxSymbol());
            e["order_symbol"]          = mm->mmOrderSymbolOverride();
            e["reference_fix_symbol"]  = mm->mmReferenceFixOverride();
            e["theo_venue_symbol"]     = mm->mmReferenceFixOverride();
            e["theo_source"]           = mm->mmTheoSource();
            e["blocked_by_feed"]       = mm->mmIsBlockedByFeed();
            e["block_reason"]          = mm->mmFeedBlockReason();
            e["first_ack_ms"]          = mm->mmFirstAcceptMs();
            // Fill bookkeeping — the "identifier when fills received" the
            // user asked for is `last_fill_id` (AX order_id of the last
            // partial/complete fill, decimal string).
            e["fills_count"]           = static_cast<long long>(mm->mmFillsCount());
            e["last_fill_id"]          = mm->mmLastFillIdStr();
            e["last_fill_ms"]          = mm->mmLastFillMs();
            nlohmann::json params = nlohmann::json::object();
            if (const auto& st = mm->mmOptionalManualStack()) {
                params["id"]                              = st->id;
                params["width_ticks"]                     = st->width_ticks;
                params["order_size"]                      = st->order_size;
                params["max_position"]                    = st->max_position;
                params["adjust_position"]                 = st->adjust_position;
                params["adjust_ticks"]                    = st->adjust_ticks;
                params["min_theo_move_ticks_to_requote"]  = st->min_theo_move_ticks_to_requote;
                params["max_reload_cycles"]               = st->max_reload_cycles;
            }
            e["params"] = std::move(params);

            nlohmann::json sides;
            sides["bid"] = {{"order_id", static_cast<long long>(mm->mmCurrentBidOrderId())},
                            {"price",    mm->mmLastBidPrice()},
                            {"qty",      mm->mmLastBidQty()}};
            sides["ask"] = {{"order_id", static_cast<long long>(mm->mmCurrentAskOrderId())},
                            {"price",    mm->mmLastAskPrice()},
                            {"qty",      mm->mmLastAskQty()}};
            e["sides"] = std::move(sides);
            arr.push_back(std::move(e));
        }
    }

    nlohmann::json doc;
    doc["updated_ms"] = now_ms;
    // Epoch-ms mtime of the primary config the engine's live in-memory state was last loaded
    // from. The desk waits for this to reach (>=) the mtime of its own hold write AND for the
    // instrument's `mm_orders_enabled_effective` to read false before issuing cancelAllOrders.
    doc["config_mtime_ms"] = cfg.lastPrimaryReloadFileMtimeMs();
    doc["orders"]     = std::move(arr);

    const std::string tmp = out.string() + ".tmp";
    {
        std::ofstream f(tmp, std::ios::trunc | std::ios::binary);
        if (!f) return;
        f << doc.dump();
        f.flush();
    }
    fs::rename(tmp, out, ec);
}

static constexpr const char* kLastExitBreadcrumbName = "last_process_exit.json";

static std::string describeOsSignalForLogs(int signum) {
    const char* t = strsignal(signum);
    if (t && t[0]) {
        return std::string(t);
    }
    return "signum=" + std::to_string(signum);
}

static void writeLastExitBreadcrumb(const std::string& log_dir,
                                    const nlohmann::json& j,
                                    std::string& err_out) {
    err_out.clear();
    if (log_dir.empty()) {
        return;
    }
    namespace fs = std::filesystem;
    std::error_code ec;
    fs::create_directories(log_dir, ec);
    const fs::path p = fs::path(log_dir) / kLastExitBreadcrumbName;
    const std::string tmp = p.string() + ".tmp";
    {
        std::ofstream f(tmp, std::ios::trunc | std::ios::binary);
        if (!f) {
            err_out = "open failed: " + tmp;
            return;
        }
        f << j.dump(2) << "\n";
    }
    fs::rename(tmp, p, ec);
    if (ec) {
        err_out = ec.message();
    }
}

static void tryLogPreviousExitBreadcrumb(core::Platform& main) {
    try {
        const std::string d = main.getLogDirectory();
        if (d.empty()) {
            return;
        }
        const std::filesystem::path p = std::filesystem::path(d) / kLastExitBreadcrumbName;
        if (!std::filesystem::exists(p)) {
            return;
        }
        std::ifstream f(p, std::ios::binary);
        if (!f) {
            return;
        }
        std::ostringstream ss;
        ss << f.rdbuf();
        nlohmann::json j = nlohmann::json::parse(ss.str());
        if (j.is_object()) {
            main.logger()->warn(
                "[LIFECYCLE] Previous run left {} — event={} signum={} at_ms={} (correlate with that run's full log)",
                p.string(),
                j.value("event", std::string{}),
                j.value("signum", 0),
                j.value("unix_time_ms", int64_t{0}));
        }
    } catch (...) {
        // best-effort only
    }
}

static void writeShutdownBreadcrumbForSignal(core::Platform& main, int signum) {
    nlohmann::json j;
    j["event"]         = "shutdown_signal";
    j["signum"]        = signum;
    j["strsignal"]     = describeOsSignalForLogs(signum);
    j["pid"]           = static_cast<int64_t>(getpid());
    j["unix_time_ms"]  = static_cast<int64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count());
    std::string e;
    writeLastExitBreadcrumb(main.getLogDirectory(), j, e);
    if (!e.empty() && main.logger()) {
        main.logger()->warn("[LIFECYCLE] could not write last_process_exit.json: {}", e);
    }
}

static void writeAbnormalExitBreadcrumb(const std::string& log_dir, const char* event, const char* detail) {
    nlohmann::json j;
    j["event"]        = event;
    j["detail"]       = detail ? detail : "";
    j["pid"]          = static_cast<int64_t>(getpid());
    j["unix_time_ms"] = static_cast<int64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count());
    std::string e;
    writeLastExitBreadcrumb(log_dir, j, e);
    (void)e;
}

/**
 * std::terminate (e.g. uncaught exception in a worker thread) — log to stderr and leave a
 * JSON breadcrumb in logs/ (when log dir is known) for post-mortem.
 */
static std::string g_terminate_breadcrumb_log_dir;
static void installStdTerminateHandler() {
    const char* self = "main.cpp:installStdTerminateHandler";
    (void)self;
    std::set_terminate([]() noexcept {
        const char* head = "\n[FATAL] std::terminate — uncaught exception in a thread or noexcept violation\n";
        (void)::write(STDERR_FILENO, head, std::strlen(head));
        try {
            if (std::current_exception()) {
                std::rethrow_exception(std::current_exception());
            }
        } catch (const std::exception& e) {
            const char* w = e.what();
            if (w) {
                (void)::write(STDERR_FILENO, w, std::strlen(w));
            }
        } catch (...) {
            (void)::write(STDERR_FILENO, " (non-std exception)\n", 22);
        }
        (void)::write(STDERR_FILENO, "\n", 1);
        if (!g_terminate_breadcrumb_log_dir.empty()) {
            writeAbnormalExitBreadcrumb(g_terminate_breadcrumb_log_dir, "std_terminate", "see stderr");
        }
        std::abort();
    });
}

void lifecycleSetLogDirForTerminateBreadcrumb(const std::string& d) {
    g_terminate_breadcrumb_log_dir = d;
}

void lifecycleOnTradingLoopExited(core::Platform& main, int last_signum) {
    if (main.logger()) {
        if (last_signum != 0) {
            main.logger()->info(
                "[LIFECYCLE] trading loop stopped: signum={} ({})",
                last_signum,
                describeOsSignalForLogs(last_signum));
        } else {
            main.logger()->warn(
                "[LIFECYCLE] trading loop returned with shutdown but signum=0 — check for other causes");
        }
    }
    if (last_signum != 0) {
        writeShutdownBreadcrumbForSignal(main, last_signum);
    }
}

} // namespace

// Global flag for shutdown signal (false = keep running, true = shutdown requested)
std::atomic<bool> g_shutdown_requested{false};

/** Set by the signal handler (async): which signal requested shutdown. 0 = not set. */
std::atomic<int> g_last_shutdown_signum{0};

/**
 * Async-signal-safe: do not use iostream, malloc-heavy calls, or Logger here.
 * (Logging std::cout from a handler can deadlock and look like a random process death.)
 */
void signalHandler(int signum) {
    g_last_shutdown_signum.store(signum, std::memory_order_release);
    g_shutdown_requested.store(true, std::memory_order_release);
}

// =============================================================================
// CRASH CANCEL-ALL WATCHDOG (2026-05-20 audit, fix C6)
// =============================================================================
//
// Background: SIGSEGV / SIGABRT / SIGFPE / SIGBUS previously left venue orders
// orphaned because the only graceful cancel happened in executeShutdown, which
// the crashing process never reaches. With multi-million-dollar exposure this
// is unacceptable.
//
// Design: a pre-armed background thread (`g_crash_watchdog_thread`) sits on
// an atomic flag. The fatal-signal handler:
//   1. records the signum (async-signal-safe atomic store)
//   2. wakes the watchdog (atomic store)
//   3. sleeps up to ~5s waiting on `g_crash_cleanup_done_` (nanosleep is
//      async-signal-safe)
//   4. restores the default disposition and re-raises so the OS can produce a
//      core dump
//
// The watchdog thread runs OUTSIDE the signal handler with full C++ runtime
// available — it invokes `Main().rest()->cancelAllOrders(std::nullopt)` which
// goes through the thread-safe CurlMultiManager (see audit #4 for the
// transport-level analysis). This is best-effort: if the heap is corrupted
// the REST call may itself crash, but in 90%+ of crashes (null deref in
// strategy code, divide-by-zero in pricing, etc.) the rest of the process
// is still well-formed enough to send a single HTTPS POST.
//
// IMPORTANT: this does NOT replace a Python/OS watchdog that should ALSO
// monitor the C++ PID — if the kernel kills us with SIGKILL there is no
// chance to clean up from inside the process. That extra layer is tracked
// as a follow-up.
std::atomic<bool> g_crash_cleanup_requested{false};
std::atomic<bool> g_crash_cleanup_done{false};
std::atomic<int>  g_crash_signum{0};
std::thread       g_crash_watchdog_thread;
std::atomic<bool> g_crash_watchdog_stop{false};

void fatalSignalHandler(int signum);

static void crashWatchdogLoop() {
    while (!g_crash_watchdog_stop.load(std::memory_order_acquire)) {
        if (g_crash_cleanup_requested.load(std::memory_order_acquire)) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    if (g_crash_watchdog_stop.load(std::memory_order_acquire) &&
        !g_crash_cleanup_requested.load(std::memory_order_acquire)) {
        return;
    }

    const int signum = g_crash_signum.load(std::memory_order_acquire);
    try {
        auto& main = core::Main();
        if (main.logger()) {
            main.logger()->warn(
                "[CRASH_WATCHDOG] fatal signal={} ({}) — attempting venue REST cancel-all best-effort "
                "before re-raising for core dump",
                signum, describeOsSignalForLogs(signum));
            main.logger()->flush();
        }
        if (main.rest()) {
            try {
                (void)main.rest()->cancelAllOrders(std::nullopt);
            } catch (...) {
            }
        }
        if (main.logger()) {
            const std::string log_dir = main.getLogDirectory();
            if (!log_dir.empty()) {
                writeAbnormalExitBreadcrumb(log_dir, "fatal_signal_cancel_all", describeOsSignalForLogs(signum).c_str());
            }
            main.logger()->warn("[CRASH_WATCHDOG] REST cancel-all attempt complete; releasing signal handler");
            main.logger()->flush();
        }
    } catch (...) {
    }
    g_crash_cleanup_done.store(true, std::memory_order_release);
}

/**
 * Fatal-signal handler. Async-signal-safe: atomic stores + nanosleep + raise +
 * write to STDERR_FILENO only.
 */
void fatalSignalHandler(int signum) {
    int expected = 0;
    if (!g_crash_signum.compare_exchange_strong(expected, signum,
                                                std::memory_order_acq_rel,
                                                std::memory_order_relaxed)) {
        const char* msg = "[CRASH_WATCHDOG] re-entry — restoring default handler immediately\n";
        (void)::write(STDERR_FILENO, msg, std::strlen(msg));
        std::signal(signum, SIG_DFL);
        std::raise(signum);
        return;
    }
    const char* preamble = "\n[CRASH_WATCHDOG] fatal signal received — invoking REST cancel-all watchdog (max 5s wait)\n";
    (void)::write(STDERR_FILENO, preamble, std::strlen(preamble));
    g_crash_cleanup_requested.store(true, std::memory_order_release);
    for (int i = 0; i < 50; ++i) {
        if (g_crash_cleanup_done.load(std::memory_order_acquire)) {
            break;
        }
        struct timespec ts{0, 100 * 1000 * 1000};
        (void)nanosleep(&ts, nullptr);
    }
    const char* tail = "[CRASH_WATCHDOG] re-raising signal for default handler / core dump\n";
    (void)::write(STDERR_FILENO, tail, std::strlen(tail));
    std::signal(signum, SIG_DFL);
    std::raise(signum);
}

namespace {
alignas(16) char g_crash_alt_stack[64 * 1024];

void installCrashAltSignalStack() {
    stack_t ss{};
    ss.ss_sp = g_crash_alt_stack;
    ss.ss_size = sizeof(g_crash_alt_stack);
    ss.ss_flags = 0;
    if (sigaltstack(&ss, nullptr) != 0) {
        const char* msg = "[CRASH_WATCHDOG] sigaltstack install failed — handler may not run on stack overflow\n";
        (void)::write(STDERR_FILENO, msg, std::strlen(msg));
    }
}

void installFatalSignalHandlersOnAltStack() {
    struct sigaction sa{};
    sa.sa_handler = fatalSignalHandler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = SA_ONSTACK;
    const int fatal_sigs[] = {SIGSEGV, SIGABRT, SIGFPE, SIGBUS, SIGILL};
    for (int sig : fatal_sigs) {
        (void)sigaction(sig, &sa, nullptr);
    }
}
}  // namespace

static void installCrashCancelHandler() {
    installCrashAltSignalStack();
    g_crash_watchdog_thread = std::thread(crashWatchdogLoop);
    installFatalSignalHandlersOnAltStack();
}

static void teardownCrashCancelHandler() {
    g_crash_watchdog_stop.store(true, std::memory_order_release);
    if (g_crash_watchdog_thread.joinable()) {
        g_crash_watchdog_thread.join();
    }
}

void printUsage(const char* binary) {
    std::cout << R"(
╔══════════════════════════════════════════════════════════════════════╗
║              ARCHITECT PLATFORM CORE - Trading Simulator             ║
╚══════════════════════════════════════════════════════════════════════╝

Usage: )" << binary << R"( <config_path> <simulation_date>

Arguments:
  config_path      Path to JSON configuration file
  simulation_date  Date for simulation (YYYYMMDD format)

Example:
  )" << binary << R"( config/default_config.json 20260206

25-Step Framework:
  Steps 1-6:   Core System Initialization
  Steps 7-12:  API & Connectivity  
  Steps 13-17: Market Data Infrastructure
  Steps 18-21: Trading Infrastructure
  Steps 22-23: Pre-Trade Verification
  Step 24:     Trading Loop (Ctrl+C to exit)
  Step 25:     Graceful Shutdown

Output:
  Creates logs/{date}/ with platform logs and CSV data files.

)";
}

/**
 * @brief Register the market maker strategy
 */
void registerStrategy(core::Platform& main, const std::string& simulation_date) {
    if (!main.config()->isMarketMakerEnabled()) {
        main.logger()->info("Market maker strategy disabled in config");
        return;
    }

    events::DateInt date_int = 0;
    try {
        date_int = static_cast<events::DateInt>(std::stoll(simulation_date));
    } catch (...) {}

    if (main.config()->hasMarketMakerInstrumentList()) {
        const auto legs = main.config()->getMarketMakerInstruments();
        if (legs.empty()) {
            main.logger()->warn("market_maker.instruments is non-empty in config check but parsed empty — skipping MM");
            return;
        }
        const std::size_t n = legs.size();
        for (const auto& leg : legs) {
            const std::string name = mmBaseStrategyNameForLeg(leg.ax_symbol, n);
            auto mm = std::make_shared<strategy::MakeMarketStrategy>(name);
            mm->setMmInstrumentLeg(leg.ax_symbol, leg.order_symbol, leg.reference_fix_symbol);
            const std::string ts = main.config()->getResolvedTheoSourceForLeg(leg);
            mm->setMmTheoSource(ts);
            // If the leg ships a `manual_stacks[0]` in default_config.json, treat it
            // as a per-product override of the top-level market_maker scalars
            // (order_size / width / max_position / adjust_*). Without this, every
            // a non-FX leg silently inherits market_maker.order_size sized for another
            // product and can bust margin on SPX/XAU/WTI/XAG sandbox-side.
            std::optional<architect::config::MarketMakerManualStack> seed_stack = std::nullopt;
            if (!leg.manual_stacks.empty()) {
                seed_stack = leg.manual_stacks.front();
            }
            if (seed_stack.has_value() && seed_stack->order_size_step <= 0 && leg.order_size_step > 0) {
                seed_stack->order_size_step = leg.order_size_step;
            }
            mm->setOptionalManualStack(seed_stack);
            if (leg.order_size_step > 0) {
                mm->setMmInstrumentOrderSizeStep(leg.order_size_step);
            }
            // Apply leg-level (product-level) max_position cap if configured. When > 0 this
            // overrides any per-stack max_position AND the global market_maker.max_position
            // for THIS strategy AND every per-stack mm_req_* spawned for the same AX symbol —
            // so two stacks on the same AX symbol can no longer collectively breach the cap.
            if (leg.max_position > 0) {
                mm->setMmInstrumentMaxPosition(leg.max_position, "instrument");
            }
            if (leg.max_reload_cycles > 0) {
                mm->setMmInstrumentMaxReloadCycles(leg.max_reload_cycles, "instrument");
            }
            // Per-leg quote tick (AX /instruments tick_size). Required because XAU=0.1, XAG=0.01,
            // JPYUSD=1e-6, etc. — using market_maker.price_tick globally produces invalid prices.
            if (leg.tick_size > 0.0) {
                mm->setMmInstrumentTick(leg.tick_size);
            }
            // Legacy multiplicative theo scale (e.g. AX SPY-PERP vs Hyperliquid xyz:SP500 — divide
            // by 10). The pricer-snapshot transform handles general cross-scale bridging.
            if (leg.theo_scale > 0.0) {
                mm->setMmTheoScale(leg.theo_scale);
            }
            attachMmProviders(main, mm);
            const auto& s_log = seed_stack;
            main.logger()->info(
                "Market maker (base) registered name={} ax={} ref_fix={} theo_source={}"
                " stack_override={} order_size={} width={} max_position={} max_reload_cycles={}",
                name, leg.ax_symbol,
                leg.reference_fix_symbol.empty() ? "(config theo)" : leg.reference_fix_symbol,
                ts.empty() ? "(none)" : ts,
                s_log.has_value() ? s_log->id : std::string("(global)"),
                s_log.has_value() ? s_log->order_size : main.config()->getMarketMakerQuantity(),
                s_log.has_value() ? s_log->width_ticks : main.config()->getMarketMakerSpreadTicks(),
                s_log.has_value() ? s_log->max_position : main.config()->getMarketMakerMaxPosition(),
                leg.max_reload_cycles > 0 ? leg.max_reload_cycles : main.config()->getMarketMakerMaxReloadCycles());
        }
    } else {
        auto mm = std::make_shared<strategy::MakeMarketStrategy>();
        attachMmProviders(main, mm);
        main.logger()->info("Market maker strategy registered (single-leg):");
        main.logger()->info("  Symbol: {}", main.config()->getMarketMakerSymbol());
        main.logger()->info("  Theo Symbol: {}", main.config()->getMarketMakerTheoSymbol());
    }

    main.strategyManager()->initializeAll(date_int);

    // First pass over orders.json — gives us restart recovery for free:
    // every stack present in the file at boot becomes a running strategy
    // here, before the trading loop starts. Subsequent passes run on the
    // bg thread (~1 Hz). If the file is missing/empty this is a no-op.
    try {
        reconcileOrdersConfig(main, date_int);
    } catch (const std::exception& e) {
        main.logger()->warn("[MM_ORDERS] initial orders.json reconcile error: {}", e.what());
    }

    main.logger()->info("  Spread: {} ticks", main.config()->getMarketMakerSpreadTicks());
    main.logger()->info("  Quote Tick: {}", main.config()->getMarketMakerPriceTick());
    main.logger()->info("  Quantity: {}", main.config()->getMarketMakerQuantity());
    main.logger()->info("  Update Interval: {}s", main.config()->getMarketMakerUpdateIntervalSec());
}

// Held for the whole process lifetime; the OS drops the flock automatically on any
// exit (clean OR crash), so there is no stale-pidfile problem to clean up.
static int g_single_instance_lock_fd = -1;

/**
 * @brief Acquire an exclusive single-instance lock keyed to this config's orders.json.
 *
 * ROOT-CAUSE FIX for "orders pulled automatically": a second `trading_client` started
 * (or shut down) against the same desired-state file wipes `orders.json` to empty via
 * its own startup clean-slate / shutdown truncate, and the running instance then
 * reconciles to `desired=0` and tears down every live stack. This lock makes that
 * impossible: the second instance can never begin startup, so it never wipes the file.
 *
 * The lock is an flock on a temp file whose name is derived from the RESOLVED
 * orders.json path, so two instances sharing the same desired-state collide even if
 * launched from different working dirs. MUST be called before `executeStartup()`
 * (which performs the clean-slate wipe) — on contention we return false and main
 * exits immediately WITHOUT touching orders.json or the venue.
 *
 * Fails OPEN (returns true) only if the lock file itself cannot be created (infra
 * error), but fails CLOSED (returns false) on genuine contention.
 */
static bool acquireSingleInstanceLock(const std::string& config_path,
                                      std::string& out_lock_path,
                                      std::string& out_reason) {
    namespace fs = std::filesystem;
    std::string orders_rel = "logs/orders.json";
    try {
        std::ifstream f(config_path, std::ios::binary);
        if (f) {
            nlohmann::json cfg;
            f >> cfg;
            if (cfg.is_object() && cfg.contains("mm_desk") && cfg["mm_desk"].is_object()) {
                const auto& md = cfg["mm_desk"];
                if (md.contains("mm_orders_config_path") &&
                    md["mm_orders_config_path"].is_string()) {
                    orders_rel = md["mm_orders_config_path"].get<std::string>();
                }
            }
        }
    } catch (...) {
        // fall back to the default relative path — still a stable key per install
    }

    std::error_code ec;
    fs::path op(orders_rel);
    if (!op.is_absolute()) {
        op = fs::absolute(op, ec);
    }
    const std::string key = op.lexically_normal().string();
    const std::size_t h = std::hash<std::string>{}(key);
    fs::path lock_dir = fs::temp_directory_path(ec);
    if (ec) {
        lock_dir = fs::path("/tmp");
    }
    const fs::path lock_path = lock_dir / ("architect_trading_client_" + std::to_string(h) + ".lock");
    out_lock_path = lock_path.string();

    const int fd = ::open(lock_path.c_str(), O_CREAT | O_RDWR, 0644);
    if (fd < 0) {
        out_reason = std::string("could not open lock file (") + std::strerror(errno) +
                     ") — proceeding without single-instance protection";
        return true;  // fail-open on infra error
    }
    if (::flock(fd, LOCK_EX | LOCK_NB) != 0) {
        out_reason = "another trading_client instance already holds the lock for this orders.json";
        ::close(fd);
        return false;  // fail-closed on genuine contention
    }
    (void)::ftruncate(fd, 0);
    const std::string pid_line = std::to_string(getpid()) + "\n";
    (void)::write(fd, pid_line.data(), pid_line.size());
    g_single_instance_lock_fd = fd;  // intentionally leaked for the process lifetime
    return true;
}

/**
 * @brief Main entry point - 25-Step Framework
 */
int main(int argc, char* argv[]) {
    // Parse command line arguments
    if (argc < 3) {
        printUsage(argv[0]);
        return 1;
    }
    
    std::string binary_path = argv[0];
    std::string config_path = argv[1];
    std::string simulation_date = argv[2];
    
    // Setup signal handlers
    std::signal(SIGINT, signalHandler);
    std::signal(SIGTERM, signalHandler);
    std::signal(SIGHUP, signalHandler);
    (void)std::signal(SIGPIPE, SIG_IGN);

    installStdTerminateHandler();

    // CONCURRENCY (2026-05-20 audit, fix C6): install fatal-signal handlers BEFORE
    // any strategy thread runs so a crash from the very first feed/mover tick still
    // triggers the cancel-all watchdog. Tear down after the graceful shutdown path
    // completes so the watchdog never re-orphans a clean exit.
    installCrashCancelHandler();

    // Create simulation parameters
    core::SimulationParams params;
    params.binary_path = binary_path;
    params.config_path = config_path;
    params.simulation_date = simulation_date;
    
    // Quick parameter validation before starting framework
    std::string validation_error = params.validate();
    if (!validation_error.empty()) {
        std::cerr << "ERROR: " << validation_error << std::endl;
        printUsage(argv[0]);
        return 1;
    }

    // SINGLE-INSTANCE LOCK — must run BEFORE executeStartup()'s orders.json clean-slate
    // wipe. If another instance already owns this config's orders.json, abort now WITHOUT
    // touching the file or the venue (a second instance's wipe is the root cause of the
    // "orders pulled automatically" incidents).
    {
        std::string lock_path;
        std::string lock_reason;
        if (!acquireSingleInstanceLock(config_path, lock_path, lock_reason)) {
            std::cerr << "ERROR: refusing to start — " << lock_reason << "\n"
                      << "       lock: " << lock_path << "\n"
                      << "       Another trading_client is already running against this "
                         "orders.json. Stop it first; this instance is exiting WITHOUT "
                         "modifying orders.json or cancelling any orders."
                      << std::endl;
            return 1;
        }
        if (!lock_reason.empty()) {
            std::cerr << "WARN: single-instance lock: " << lock_reason << std::endl;
        }
    }

    // Get startup sequence orchestrator
    auto& startup = core::Startup();
    
    int exit_code = 0;
    
    try {
        // =====================================================================
        // STEPS 1-23: Startup Sequence
        // =====================================================================
        startup.executeStartup(params);
        
        // =====================================================================
        // Register Strategy (after startup complete)
        // =====================================================================
        auto& main = core::Main();
        lifecycleSetLogDirForTerminateBreadcrumb(main.getLogDirectory());
        tryLogPreviousExitBreadcrumb(main);
        main.logger()->info("[LIFECYCLE] pid={}", static_cast<int64_t>(getpid()));
        registerStrategy(main, simulation_date);

        events::DateInt date_int_bg = 0;
        try {
            date_int_bg = static_cast<events::DateInt>(std::stoll(simulation_date));
        } catch (...) {}

        // Background loop: reload primary config (so per-leg "base" MM
        // picks up edits to market_maker.* without a restart), reconcile
        // running strategies against the desk-owned `orders.json`, publish
        // the live `mm_orders.json` projection for the GUI, and refresh
        // the feed-health pill file. Each step is independently wrapped
        // so one transient failure (e.g. a bad config reload, a stray
        // half-written orders.json from the desk) doesn't take down the
        // others.
        std::thread mm_desk_bg_thread;
        std::atomic<bool> reconcile_stop{false};
        if (main.config()->isMarketMakerEnabled()) {
            mm_desk_bg_thread = std::thread([&main, date_int_bg, &reconcile_stop]() {
                while (!g_shutdown_requested.load(std::memory_order_acquire) &&
                       !reconcile_stop.load(std::memory_order_acquire)) {
                    try {
                        (void)config::Config::getInstance().reloadPrimaryConfigFromDisk();
                    } catch (const std::exception& e) {
                        main.logger()->warn("[MM_ORDERS] config reload error: {}", e.what());
                    } catch (...) {
                    }
                    try {
                        mmOrdersJsonMaybeRefreshFreezeCopy(main);
                    } catch (const std::exception& e) {
                        main.logger()->warn("[MM_ORDERS] orders.json freeze refresh error: {}", e.what());
                    } catch (...) {
                    }
                    try {
                        reconcileMmReqStrategiesFromOrdersJson(main, date_int_bg);
                    } catch (const std::exception& e) {
                        main.logger()->warn("[MM_ORDERS] orders.json reconcile error: {}", e.what());
                    } catch (...) {
                        main.logger()->warn("[MM_ORDERS] orders.json reconcile unknown error");
                    }
                    // Stale pending-accept cleanup only (no auto place): desk seeds bid+ask;
                    // C++ moves them on theo once OIDs are adopted.
                    try {
                        strategy::MakeMarketStrategy::ensureStartupQuotePairAllRegistered(
                            "mm_pair_enforce");
                    } catch (const std::exception& e) {
                        main.logger()->warn("[MM_ORDERS] pair-enforce error: {}", e.what());
                    } catch (...) {
                    }
                    try {
                        writeMmOrdersActiveSnapshot(main);
                    } catch (const std::exception& e) {
                        main.logger()->warn("[MM_ORDERS] active-snapshot error: {}", e.what());
                    } catch (...) {
                    }
                    try {
                        writeFeedHealthSnapshot(main);
                    } catch (const std::exception& e) {
                        main.logger()->warn("feed-health snapshot error: {}", e.what());
                    } catch (...) {
                    }
                    for (int i = 0; i < 10; ++i) {
                        if (g_shutdown_requested.load(std::memory_order_acquire) ||
                            reconcile_stop.load(std::memory_order_acquire)) {
                            break;
                        }
                        std::this_thread::sleep_for(std::chrono::milliseconds(100));
                    }
                }
            });
        }

        main.logger()->info("╔══════════════════════════════════════════════════════════════════╗");
        main.logger()->info("║                   PLATFORM READY FOR TRADING                     ║");
        main.logger()->info("╚══════════════════════════════════════════════════════════════════╝");
        main.logger()->info("Press Ctrl+C to initiate graceful shutdown.");
        main.logger()->info("Log directory: {}", main.getLogDirectory());
        
        // Display feed status
        const auto& feed = main.getFeedStatus();
        if (feed.is_live) {
            main.logger()->warn("*** LIVE TRADING MODE - Orders will execute on real market! ***");
        } else if (feed.is_paper) {
            main.logger()->info("Paper trading mode - real prices, simulated orders");
        } else if (feed.is_simulation) {
            main.logger()->info("Simulation mode - historical replay");
        }
        
        // =====================================================================
        // STEP 24: Trading Loop (blocks until SIGINT/SIGTERM)
        // =====================================================================
        startup.executeTradingLoop(g_shutdown_requested);

        lifecycleOnTradingLoopExited(
            main, g_last_shutdown_signum.load(std::memory_order_acquire));

        reconcile_stop.store(true, std::memory_order_release);
        if (mm_desk_bg_thread.joinable()) {
            mm_desk_bg_thread.join();
        }

    } catch (const core::StartupException& e) {
        // Startup failed at a specific step
        std::cerr << "\n" << std::endl;
        std::cerr << "╔══════════════════════════════════════════════════════════════════╗" << std::endl;
        std::cerr << "║                      STARTUP SEQUENCE FAILED                      ║" << std::endl;
        std::cerr << "╚══════════════════════════════════════════════════════════════════╝" << std::endl;
        std::cerr << "Step " << e.getStepNumber() << " [" << e.getStepName() << "] failed:" << std::endl;
        std::cerr << "  " << e.what() << std::endl;
        std::cerr << std::endl;
        
        exit_code = e.getStepNumber();  // Return step number as exit code
        
    } catch (const std::exception& e) {
        std::cerr << "FATAL: Unexpected error: " << e.what() << std::endl;
        exit_code = 99;
    }
    
    // =========================================================================
    // STEP 25: Graceful Shutdown
    // =========================================================================
    std::string shutdown_reason = (exit_code == 0) 
        ? "User requested shutdown (SIGINT/SIGTERM)"
        : "Startup failure at step " + std::to_string(exit_code);
    
    auto shutdown_status = startup.executeShutdown(shutdown_reason);

    // CONCURRENCY (fix C6): graceful shutdown reached — stop the crash watchdog
    // BEFORE returning so static destructors don't race the watchdog thread.
    teardownCrashCancelHandler();

    // Final output
    std::cout << std::endl;
    if (shutdown_status.isGraceful()) {
        std::cout << "✓ Graceful shutdown complete." << std::endl;
    } else {
        std::cout << "⚠ Shutdown completed with warnings." << std::endl;
    }
    
    if (exit_code == 0) {
        std::cout << "Logs saved to: logs/" << simulation_date << "/" << std::endl;
    }
    
    return exit_code;
}
