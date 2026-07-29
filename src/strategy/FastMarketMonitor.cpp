#include "strategy/FastMarketMonitor.h"

#include "strategy/MmOrderMover.h"
#include "strategy/MakeMarketStrategy.h"
#include "config/Config.h"
#include "marketdata/ExternalFeedManager.h"
#include "utils/Logger.h"

#include <chrono>
#include <string>

namespace architect {
namespace strategy {

FastMarketMonitor& FastMarketMonitor::getInstance() {
    static FastMarketMonitor instance;
    return instance;
}

void FastMarketMonitor::enqueueTick() {
    // Post onto the single global mover worker so sample+evaluate is naturally serialized with
    // all other order ops (finding A). The ax label is for diagnostics only.
    MmOrderMover::getInstance().enqueueForAx("__fast_market__", []() {
        FastMarketMonitor::getInstance().onMoverTick();
    });
}

void FastMarketMonitor::onMoverTick() {
    using clock = std::chrono::steady_clock;
    auto& cfg = config::Config::getInstance();

    // GLOBAL config (NOT per-leg templates). Re-read every tick so a GUI edit
    // (market_maker.fast_market.*) is picked up live on the next mtime-gated reload.
    const std::string symbol = cfg.getString("market_maker.fast_market.hl_spx_symbol", "");
    const double tick = cfg.getDouble("market_maker.fast_market.tick_size", 0.0);
    const int size_ticks = cfg.getInt("market_maker.fast_market.size_of_move_ticks", 0);
    const int window_sec = cfg.getInt("market_maker.fast_market.time_of_move_sec", 0);
    int pull_sec = cfg.getInt("market_maker.fast_market.pull_sec", 10);
    if (pull_sec <= 0) {
        pull_sec = 10;
    }
    // Explicit on/off switch (default ON) so the breaker can be turned off from the desk
    // WITHOUT blanking the symbol — the symbol is preserved for the next re-enable.
    const bool enabled = cfg.getBool("market_maker.fast_market.enabled", true);

    const bool config_disabled = FastMarketCore::configDisabledFromConfig(symbol.empty(), enabled);
    const clock::time_point now = clock::now();

    // ============================ FAIL-OPEN DECISION POINT ============================
    // SINGLE place to flip the HL-staleness posture. DEFAULT = FAIL-OPEN: if the HL feed is
    // down/stale we do NOT trip (keep quoting). Harry must confirm open vs closed; to make it
    // fail-closed, force `feed_stale` to additionally pull all markets here instead of returning.
    auto& efm = marketdata::ExternalFeedManager::getInstance();
    const std::int64_t now_wall_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                         std::chrono::system_clock::now().time_since_epoch())
                                         .count();
    const std::int64_t last_hl_ok = efm.lastHlOkMs();
    const bool feed_stale = !efm.isHlUp() || last_hl_ok == 0 ||
                            (now_wall_ms - last_hl_ok) > FastMarketCore::kHlStaleMs;
    // =================================================================================

    bool have_valid_mid = false;
    double mid = 0.0;
    if (!config_disabled && !feed_stale && tick > 0.0 && size_ticks > 0 && window_sec > 0) {
        const auto q = efm.getQuoteForTheoSymbol(symbol);
        if (q.has_value() && q->valid) {
            double m = q->mid;
            if (!(m > 0.0) && q->bid > 0.0 && q->ask > 0.0) {
                m = 0.5 * (q->bid + q->ask);
            }
            if (m > 0.0 && m == m /*not NaN*/) {
                mid = m;
                have_valid_mid = true;
            }
        }
    }

    const auto tr = core_.evaluateTick(now, config_disabled, feed_stale, have_valid_mid, mid,
                                       tick, size_ticks, window_sec, pull_sec);

    using Action = FastMarketCore::TickResult::Action;
    switch (tr.action) {
        case Action::kDisabled:
            if (!disabled_log_edge_) {
                disabled_log_edge_ = true;
                if (utils::Logger::isInitialized()) {
                    utils::Logger::getInstance().info(
                        "[FAST_MKT_BREAKER] disabled (symbol='{}' tick_size={} size_of_move_ticks={} "
                        "time_of_move_sec={}) — not watching",
                        symbol, tick, size_ticks, window_sec);
                }
            }
            return;
        default:
            disabled_log_edge_ = false;
            break;
    }

    // === Liveness heartbeat (ADDITIVE, log-only) =====================================
    // The breaker is otherwise SILENT in its steady non-breach states (kProceed /
    // kInsufficient / kDropInvalid / kPausedSkip) and only warns on kFailOpen (10s) or
    // kBreachPause (on breach). That silence makes an armed-but-quiet monitor look dead.
    // Emit a throttled heartbeat for every non-disabled state so an operator can confirm
    // it is actually watching (symbol, current MWR ticks, sample count, staleness, pause).
    // Throttle via market_maker.fast_market.heartbeat_sec (default 30; 0 disables).
    {
        const int hb_sec = std::max(0, cfg.getInt("market_maker.fast_market.heartbeat_sec", 30));
        if (hb_sec > 0 && utils::Logger::isInitialized() &&
            (now_wall_ms - heartbeat_last_ms_) >= static_cast<std::int64_t>(hb_sec) * 1000) {
            heartbeat_last_ms_ = now_wall_ms;
            const char* st =
                (tr.action == Action::kFailOpen)    ? "FAIL_OPEN"    // stale HL/self → not tripping
              : (tr.action == Action::kDropInvalid) ? "NO_MID"       // watch symbol not ingesting
              : (tr.action == Action::kInsufficient)? "WARMING"      // <2 samples in window yet
              : (tr.action == Action::kBreachPause) ? "BREACH"
              : (tr.action == Action::kPausedSkip)  ? "PAUSED"
                                                    : "OK";          // kProceed
            utils::Logger::getInstance().info(
                "[FAST_MKT_BREAKER] heartbeat state={} symbol={} mwr_ticks={:.2f} size_ticks={} "
                "window_sec={} pull_sec={} samples={} have_mid={} feed_stale={} self_stale={} "
                "paused_until_ms={} now_ms={}",
                st, symbol, tr.mwr_ticks, size_ticks, window_sec, pull_sec,
                core_.sampleCount(), have_valid_mid ? 1 : 0, feed_stale ? 1 : 0,
                tr.self_stale ? 1 : 0, pausedUntilMs(), now_wall_ms);
        }
    }

    if (tr.action == Action::kFailOpen) {
        if ((now_wall_ms - fail_open_warn_last_ms_) > 10000 && utils::Logger::isInitialized()) {
            fail_open_warn_last_ms_ = now_wall_ms;
            utils::Logger::getInstance().warn(
                "[FAST_MKT_BREAKER] HL feed/self stale (hl_up={} self_stale={} last_hl_ok_ms={}) "
                "— FAIL-OPEN: not tripping, quoting continues",
                efm.isHlUp(), tr.self_stale, last_hl_ok);
        }
        return;
    }

    if (tr.action == Action::kBreachPause && tr.just_paused) {
        // ONE structured log on the unpaused->paused transition.
        if (utils::Logger::isInitialized()) {
            utils::Logger::getInstance().warn(
                "[FAST_MKT_BREAKER] HL_SPX BREACH symbol={} mwr_ticks={:.2f} size_ticks={} "
                "window_sec={} pull_sec={} paused_until_ms={} — pulling ALL markets",
                symbol, tr.mwr_ticks, size_ticks, window_sec, pull_sec, tr.paused_until_ms);
        }
        // Fan out a cancel-only job to every live MM strategy (committed lifetime + cancel path).
        MakeMarketStrategy::mmFastMarketPullAllTrackedLegs("fast_market_breach");
    }
}

}  // namespace strategy
}  // namespace architect
