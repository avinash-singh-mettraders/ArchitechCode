#pragma once

/**
 * @file Config.h
 * @brief Configuration management system with JSON support
 * 
 * Provides a hierarchical, thread-safe configuration system that supports:
 * - JSON file loading
 * - Environment variable overrides
 * - Runtime configuration updates
 * - Type-safe accessors
 */

#include <string>
#include <vector>
#include <map>
#include <mutex>
#include <shared_mutex>
#include <optional>
#include <functional>
#include <nlohmann/json.hpp>

namespace architect {
namespace config {

using json = nlohmann::json;
using ConfigChangeCallback = std::function<void(const std::string& key, const json& old_value, const json& new_value)>;

/**
 * Per-stack MM params: each becomes a separate MakeMarketStrategy + bid/ask that moves with theo
 * (see main.cpp registration when manual_stacks is non-empty for a leg).
 */
struct MarketMakerManualStack {
    std::string id;
    int width_bps{0};
    int width_ticks{0};  ///< Legacy compatibility alias; mirrors width_bps.
    int order_size{0};
    int order_size_step{0};  ///< Per-stack lot step. >0 = override global market_maker.order_size_step.
    /// Not read for MM net gating; use `MarketMakerInstrumentLeg::max_position` or global `market_maker.max_position`.
    int max_position{0};
    int adjust_position{0};
    int adjust_ticks{0};
    int min_theo_move_ticks_to_requote{0};
    int max_reload_cycles{0};

    /**
     * Pricer-snapshot linear transform (per-stack). Lets the desk quote an AX product whose
     * native price level differs from the live theo's price level, by mapping theo around two
     * snapshot anchors plus a slope (= elasticity ratio). Spec from the desk (2026-05-12):
     *
     *     theoMidpoint = raw external feed mid (CME, HL, or Neon — *not* invert/scale-transformed)
     *     newMidpoint  = quote_snapshot
     *                  + quote_snapshot * slope * ((theoMidpoint - pricer_snapshot) / pricer_snapshot)
     *
     *     bid_offset_px = newMidpoint * bid_width_bps / 10000   (per side, full bps)
     *     ask_offset_px = newMidpoint * ask_width_bps / 10000
     *     skew_units = Floor(|NetPo|/AdjustPo) * Sign(NetPo) * AdjustTicks   (symmetric)
     *     BidPx   = newMidpoint - bid_offset_px - skew_units * Tick
     *     OfferPx = newMidpoint + ask_offset_px - skew_units * Tick
     *     (Each side then rounded to the nearest tick — bid down, ask up.)
     *
     * When the transform is active (qs>0 AND ps>0), MakeMarketStrategy::mmTransformTheo
     * SKIPS the legacy per-leg theo_scale — the snapshot anchors already carry the scale
     * relationship, so applying scale first would feed a pre-mangled mid into the formula.
     * (invert_theo was REMOVED 2026-05-12; use the snapshot transform for cross-scale legs.)
     *
     * Examples:
     *   - USD/JPY HL as theo (157.198), Japan AX leg with qs=156.20119, ps=157.197, slope=1
     *       → newMidpoint = 156.20119 + 156.20119*1*((157.198-157.197)/157.197) ≈ 156.202184
     *   - SPY AX quoted off S&P500 HL: qs=726.48, ps=7280.7, slope=1.0; theo=7351.9
     *       → newMidpoint = 726.48 + 726.48*1*((7351.9-7280.7)/7280.7) ≈ 733.5845
     *
     * Disabled (transform skipped, raw theo passes through unchanged) when:
     *   - pricer_snapshot <= 0  (the divisor would blow up, or operator left it blank)
     *   - quote_snapshot  <= 0  (no anchor on the AX side; transform is meaningless)
     * `slope` defaults to 1.0 (linear pass-through with coherent snapshots) and may be 0
     * (clamps newMidpoint to quote_snapshot — i.e. fixed midpoint).
     */
    double quote_snapshot{0.0};
    double pricer_snapshot{0.0};
    double slope{1.0};

    /**
     * True when the desk has successfully placed the pair on the order gateway and persisted this
     * stack; C++ skips the usual startup `cancelAll`+replace for `mm_req_*` and instead adopts
     * OIDs into OrderManager. Always false for legacy `orders.json` rows that were C++-placed
     * only, so their startup flatten-on-init behavior is preserved.
     */
    bool desk_seeded{false};

    // Placed leg metadata (so the spawned `mm_req_*` strategy can adopt the exchange OIDs
    // instead of cancel-on-start, which would kill the pre-placed orders).
    std::string bid_exchange_oid;
    std::string ask_exchange_oid;
    double placed_bid_price{0.0};
    double placed_ask_price{0.0};
    int placed_bid_qty{0};
    int placed_ask_qty{0};

    /**
     * When false, C++ does not cancel/replace this stack from theo (orders stay cancelled or rest
     * until resumed). Default true. Product-level ``mm_move_all_enabled`` in orders.json AND this
     * flag must be true for quotes to move.
     */
    bool mm_move_enabled{true};
};

/**
 * One AX leg + reference theo when using market_maker.instruments[].
 * Optional theo_source picks theo venue per product (Mettraders, Hyperliquid, Neon FIX) when
 * isMultiTheoFeeds() is true. Optional theo_venue_symbol = venue-native id (e.g. silver-usdc on Hyperliquid).
 * When ``manual_stacks`` is non-empty, C++ registers one strategy per stack; otherwise one strategy per leg.
 */
struct MarketMakerInstrumentLeg {
    std::string ax_symbol;
    std::string order_symbol;
    std::string reference_fix_symbol;
    std::string theo_source;
    std::string theo_venue_symbol;
    /**
     * Product-level max_position cap (absolute units of `ax_symbol`).
     * 
     * The MM gate is `|net_position_for_ax_symbol| < max_position` and is shared by every
     * strategy trading this leg (legacy per-leg + every per-stack `mm_req_*`). Each strategy
     * reads the same exchange-aggregated net via `syncInventoryFromExchangePortfolio()`, so
     * setting the cap on the leg makes the cap truly product-wide rather than per-stack.
     * 
     * Resolution order in `MakeMarketStrategy::mmEffMaxPositionInt` (per-stack `manual_stacks[].max_position`
     * is NOT used for the net gate — only leg + global — so L1/L2/L3 on one product always share one cap):
     *   1. leg.max_position (this field, when > 0)  ← product-level (set this for multi-stack)
     *   2. config.market_maker.max_position  (global default)
     */
    int max_position{0};
    /**
     * Product-level max_reload_cycles cap (0 = global fallback).
     *
     * Resolution order mirrors max_position for multi-stack consistency:
     *   1. orders.json products[ax].max_reload_cycles (runtime override)
     *   2. market_maker.instruments[].max_reload_cycles (instrument default)
     *   3. market_maker.max_reload_cycles (global default)
     */
    int max_reload_cycles{0};

    /**
     * Per-leg quote tick size in price units of `ax_symbol`. > 0 overrides the global
     * `market_maker.price_tick` for this leg only — required because AX `/instruments` exposes
     * very different ticks per product (FX majors vs metals vs inverse FX). Using the
     * wrong tick produces invalid prices that the gateway rejects (or silently rounds in sandbox).
     * Resolution: leg.tick_size > 0 → use it; else fall through to `market_maker.price_tick`.
     */
    double tick_size{0.0};

    /**
     * Multiplicative scale applied to the raw theo. > 0 enables; 0 = no scaling (treated as 1.0).
     * Use for proxy-instrument sizing — e.g. `SPY-PERP` (ETF, ~$715) priced off Hyperliquid
     * `xyz:SP500` (index, ~$7150) needs `theo_scale = 0.1`. For general cross-scale legs (e.g.
     * AX JPYUSD-PERP off USD/JPY feed) use the pricer-snapshot transform instead.
     */
    double theo_scale{0.0};

    /**
     * When >0, used as the spread width (bps units; same as manual_stack.width_bps) for
     * desk ``mm_req_*`` strategies if ``orders.json`` omits stack width — instead of falling
     * back to unrelated global ``market_maker.width`` / ``spread_ticks`` (which caused EURUSD
     * desk pairs to jump to a fat default spread). Alias JSON key: ``instrument_width_bps``.
     */
    int default_width_bps{0};

    /**
     * Venue contract lot step for this AX (integer contracts). When >0, merged into each
     * `manual_stacks[]` entry that omits `order_size_step`, and into desk `orders.json` stacks
     * for this ax when the stack omits step — so one product-level value replaces per-stack copy.
     * JSON aliases: `order_step`, `contract_step`.
     */
    int order_size_step{0};

    /**
     * MWR (Moving Window Range) volatility breaker — per-leg operator config.
     * Disabled unless BOTH mwr_size_ticks>0 AND mwr_window_sec>0.
     *   mwr_size_ticks : range threshold in ticks; (px_max-px_min)/tick > this → pull + pause.
     *   mwr_window_sec : rolling lookback window for the range, in seconds.
     *   mwr_pull_sec   : pause/pull duration in seconds; 0 = fall back to mwr_window_sec.
     */
    int mwr_size_ticks{0};
    int mwr_window_sec{0};
    int mwr_pull_sec{0};

    std::vector<MarketMakerManualStack> manual_stacks;
};

/**
 * @brief Thread-safe, hierarchical configuration manager
 */
class Config {
public:
    /**
     * @brief Get the singleton instance
     */
    static Config& getInstance();
    
    // Prevent copying
    Config(const Config&) = delete;
    Config& operator=(const Config&) = delete;
    
    // ==========================================================================
    // Loading & Saving
    // ==========================================================================
    
    /**
     * @brief Load configuration from a JSON file
     * @param filepath Path to the configuration file
     * @return true if successful
     */
    bool loadFromFile(const std::string& filepath);

    /**
     * @brief Load main JSON, then merge credentials.local.json and external_feed.local.json when present
     *        in the same directory (gitignored overlays for secrets).
     */
    bool loadFromFileWithOptionalOverlays(const std::string& filepath);

    /**
     * Re-merge the primary JSON file plus optional overlays from disk into the in-memory singleton.
     * Uses the path last successfully passed to loadFromFileWithOptionalOverlays; no-op if unset.
     * Intended for live MM so GUI edits to default_config.json apply on the next quote cycle.
     *
     * Throttling: a full reload (open + parse 24KB + merge_patch under unique_lock) is expensive
     * — multi-strategy benchmarks on 2026-05-06 showed 5+ MM workers serializing on this single
     * call inside `MakeMarketStrategy::onFeedUpdate` and producing 250–600ms `[FEED_DISPATCH_SLOW]`
     * lines even when no REST was issued. This function therefore skips the on-disk re-read when
     * BOTH (a) the primary file's mtime is unchanged since the last successful reload AND (b) the
     * last reload completed within `min_interval_ms` ago. Pass min_interval_ms=0 to force the
     * reload (used by config-mutating paths that need their own write back-read).
     */
    bool reloadPrimaryConfigFromDisk(int min_interval_ms = 750);

    /**
     * @brief Epoch-ms of the primary-config file mtime the CURRENT in-memory config was last
     *        loaded from (0 until the first successful reload). Stamped into the desk-facing
     *        mm_orders.json projection so the desk can prove the engine's effective
     *        per-instrument gate reflects a config at least as new as the desk's own write.
     */
    long long lastPrimaryReloadFileMtimeMs() const;
    
    /**
     * @brief Load configuration from a JSON string
     * @param json_str JSON string to parse
     * @return true if successful
     */
    bool loadFromString(const std::string& json_str);
    
    /**
     * @brief Merge additional configuration (non-destructive)
     * @param config JSON object to merge
     */
    void merge(const json& config);
    
    /**
     * @brief Save current configuration to file
     * @param filepath Path to save to
     * @return true if successful
     */
    bool saveToFile(const std::string& filepath) const;
    
    /**
     * @brief Reset to default configuration
     */
    void reset();
    
    // ==========================================================================
    // Typed Accessors
    // ==========================================================================
    
    template<typename T>
    T get(const std::string& key, const T& default_value = T{}) const;
    
    std::string getString(const std::string& key, const std::string& default_value = "") const;
    int getInt(const std::string& key, int default_value = 0) const;
    double getDouble(const std::string& key, double default_value = 0.0) const;
    bool getBool(const std::string& key, bool default_value = false) const;
    std::vector<std::string> getStringArray(const std::string& key) const;
    
    /**
     * @brief Get a nested configuration section
     */
    json getSection(const std::string& key) const;
    
    /**
     * @brief Check if a key exists
     */
    bool has(const std::string& key) const;
    
    // ==========================================================================
    // Setters
    // ==========================================================================
    
    template<typename T>
    void set(const std::string& key, const T& value);
    
    // ==========================================================================
    // API Configuration Shortcuts
    // ==========================================================================
    
    std::string getRestEndpoint() const;
    std::string getWebSocketEndpoint() const;
    std::string getApiKey() const;
    std::string getApiSecret() const;
    std::string getSessionToken() const;
    
    // ==========================================================================
    // Performance Configuration
    // ==========================================================================
    
    std::size_t getEventQueueSize() const;
    std::size_t getOrderQueueSize() const;
    std::size_t getMarketDataQueueSize() const;
    std::size_t getWorkerThreads() const;
    
    // ==========================================================================
    // Trading Configuration
    // ==========================================================================
    
    int getMaxOrdersPerSecond() const;
    int getMaxRequestsPerSecond() const;
    double getDefaultSlippage() const;
    std::vector<std::string> getWatchlist() const;
    /** Watchlist plus market_maker order symbol when MM is enabled (deduped). Used for WS MD subscriptions. */
    std::vector<std::string> getMarketDataSubscriptionSymbols() const;

    /** True when market_maker.instruments is a non-empty array (multi-leg MM). */
    bool hasMarketMakerInstrumentList() const;
    /** Parsed legs; empty if instruments absent/empty (use legacy symbol / theo_symbol). */
    std::vector<MarketMakerInstrumentLeg> getMarketMakerInstruments() const;

    /**
     * Parse Architect ``GET {api.rest_endpoint}/instruments`` (OpenAPI ``GetInstrumentsResponse``:
     * ``{ "instruments": [ Instrument, ... ] }``; ``tick_size`` and ``minimum_order_size`` are often
     * JSON **strings**). Attach slim tradeable catalog to ``ax_gateway_instruments_catalog``, merge
     * tick and minimum size into enabled ``market_maker`` legs (and legacy ``price_tick`` /
     * ``order_size_step``), and write snapshot JSON (path from ``startup.ax_gateway_instruments_snapshot_path``).
     * When ``startup.persist_ax_gateway_instruments_to_primary_config`` is true, also rewrites the
     * loaded primary config file so merged legs + catalog survive on disk.
     */
    bool applyAxGatewayInstrumentsHttpBody(const std::string& http_body,
                                           bool strict_match_configured_symbols,
                                           std::string& err);

    /** From ``ax_gateway_instruments_catalog`` (startup ``GET …/instruments``). 0 if unknown. */
    double getAxGatewayInstrumentTickSize(const std::string& ax_symbol) const;
    /** From catalog; 0 if unknown (caller may fall back to ``market_maker.order_size_step``). */
    int getAxGatewayInstrumentMinimumOrderSize(const std::string& ax_symbol) const;

    /**
     * From ``market_maker.instruments[]`` for this AX / order symbol (after /instruments merge).
     * 0 if no matching leg or tick unset. Legacy single-symbol MM: matches global symbol/order_symbol.
     */
    double getMarketMakerInstrumentTickForAxSymbol(const std::string& ax_symbol) const;
    /** Same for ``order_size_step`` / aliases on the matched leg. */
    int getMarketMakerInstrumentOrderSizeStepForAxSymbol(const std::string& ax_symbol) const;

    /** MWR volatility-breaker params from the matched ``market_maker.instruments[]`` leg; 0 if unset. */
    int getMarketMakerInstrumentMwrSizeTicksForAxSymbol(const std::string& ax_symbol) const;
    int getMarketMakerInstrumentMwrWindowSecForAxSymbol(const std::string& ax_symbol) const;
    int getMarketMakerInstrumentMwrPullSecForAxSymbol(const std::string& ax_symbol) const;

    /** True when more than one external theo source is in use, or market_maker.multi_theo is set. */
    bool isMultiTheoFeeds() const;
    /**
     * Resolved theo source for a leg: leg.theo_source if set, else external_feed.provider.
     * Normalized: neon_fix, hyperliquid, mettraders, (other passthrough as lowercase alnum+underscore).
     */
    std::string getResolvedTheoSourceForLeg(const MarketMakerInstrumentLeg& leg) const;

    /** Mettraders websocket feed configuration for legs with theo_source mettraders. */
    bool getMettradersFeedEnabled() const;
    std::string getMettradersFeedUrl() const;
    /**
     * FIX-style symbol that the Mettraders WS quote stream belongs to (e.g.
     * "GOLD"). The wire format `{"ts":..,"bid":..,"ask":..}` carries no
     * symbol of its own, so the producer needs a config-supplied tag to
     * route published quotes into `quotes_by_canonical_` under the same key
     * the consuming MM legs already resolve via `reference_fix_symbol`.
     * Default: "GOLD".
     */
    std::string getMettradersFeedFixSymbol() const;
    
    // ==========================================================================
    // Logging Configuration
    // ==========================================================================
    
    std::string getLogLevel() const;
    std::string getLogFile() const;
    bool isConsoleLogging() const;
    bool isFileLogging() const;
    
    // ==========================================================================
    // Feed Configuration
    // ==========================================================================
    
    /**
     * @brief Get feed mode string ("auto", "live", "paper", "simulation", "backtest")
     */
    std::string getFeedMode() const;
    
    /**
     * @brief Get feed source string ("websocket_live", "rest_polling", "historical_file", etc.)
     */
    std::string getFeedSource() const;
    
    /**
     * @brief Get default subscription level ("L1", "L2", "L3")
     */
    std::string getDefaultSubscriptionLevel() const;
    
    /**
     * @brief Get L2 order book depth
     */
    int getL2Depth() const;
    
    /**
     * @brief Check if feed should be verified on startup
     */
    bool shouldVerifyFeedOnStartup() const;
    
    /**
     * @brief Check if historical replay is enabled
     */
    bool isHistoricalReplayEnabled() const;
    
    /**
     * @brief Get historical data path
     */
    std::string getHistoricalDataPath() const;
    
    /**
     * @brief Get historical replay speed multiplier
     */
    double getReplaySpeed() const;
    
    /**
     * @brief Get WebSocket channel configuration
     */
    json getWebSocketChannelConfig(const std::string& channel) const;
    
    // ==========================================================================
    // External feed (theo pricing; neon_fix or generic REST bookTicker)
    // ==========================================================================
    
    bool isExternalFeedEnabled() const;
    /** e.g. neon_fix, or any non-FIX value selects REST polling when rest_url is set */
    std::string getExternalFeedProvider() const;
    std::string getExternalFeedRestUrl() const;
    /** When true, use /api/v3/ticker/bookTicker (spot-style); when false, /fapi/v1/ticker/bookTicker. */
    bool getExternalFeedRestUsesSpotTickerPath() const;
    std::string getExternalFeedSymbol() const;
    std::string getExternalFeedDisplaySymbol() const;
    int getExternalFeedPollIntervalMs() const;
    /** After this many consecutive REST/FIX quote errors, invalidate external theo and notify MM (pull quotes). 0 = never auto-invalidate from errors. */
    int getExternalFeedConsecutiveErrorsBeforeInvalidate() const;
    bool getExternalFeedWriteDeskDepthCache() const;
    std::string getExternalFeedDeskDepthCachePath() const;
    int getExternalFeedDeskDepthLevels() const;
    int getExternalFeedDeskDepthWriteIntervalMs() const;
    // FIX quote session (Neon / Integral via stunnel)
    std::string getExternalFeedFixHost() const;
    int getExternalFeedFixPort() const;
    std::string getExternalFeedFixSenderCompId() const;
    std::string getExternalFeedFixTargetCompId() const;
    std::string getExternalFeedFixDeliverToCompId() const;
    std::string getExternalFeedFixSenderSubId() const;
    std::string getExternalFeedFixUsername() const;
    std::string getExternalFeedFixPassword() const;
    int getExternalFeedFixHeartBtInt() const;
    /** FIX 265: 0=full refresh (Neon default), 1=incremental, -1=omit tag. */
    int getExternalFeedFixMdUpdateType() const;
    /** Key = FIX tag number as string, value = field value (Neon ProductNotSet: often 167). */
    std::vector<std::pair<std::string, std::string>> getExternalFeedFixMdInstrumentExtra() const;
    std::vector<std::pair<std::string, std::string>> getExternalFeedFixLogonExtra() const;
    std::vector<std::pair<std::string, std::string>> getExternalFeedFixMdRequestRootExtra() const;
    bool getExternalFeedFixMdSnapshotOnly() const;
    /** FIX 264: 0 = request full book (venue-dependent), 1 = top of book only. */
    int getExternalFeedFixMdMarketDepth() const;
    std::vector<std::string> getExternalFeedFixMdSymbols() const;
    bool getExternalFeedFixStunnelAutostart() const;
    std::string getExternalFeedFixStunnelConfig() const;
    std::string getExternalFeedFixStunnelExecutable() const;
    int getExternalFeedFixStunnelStartTimeoutMs() const;
    std::string getExternalFeedFixStunnelLogFile() const;
    
    // ==========================================================================
    // Market maker strategy
    // ==========================================================================
    
    bool isMarketMakerEnabled() const;
    std::string getMarketMakerSymbol() const;
    /** If set, use this symbol for place_order (else use PAIR-PERP per API doc). */
    std::string getMarketMakerOrderSymbol() const;
    /** Contract size step for place_order q (e.g. 100 for GBPUSD-PERP). Must be positive multiple of this. */
    int getMarketMakerOrderSizeStep() const;
    double getMarketMakerQuantity() const;
    /** @deprecated Use getMarketMakerSpreadTicks() instead */
    int getMarketMakerSpreadBps() const;
    /** Client-facing half-width in basis points (bps) around theo midpoint. */
    int getMarketMakerSpreadTicks() const;
    /** Per-side width in bps; defaults to getMarketMakerSpreadTicks() unless bid_width_bps / ask_width_bps set. */
    int getMarketMakerBidSpreadTicks() const;
    int getMarketMakerAskSpreadTicks() const;
    /**
     * Per-instrument spread width (bps) for desk stacks when the stack row has width 0/missing.
     * Resolution: ``instruments[ax].default_width_bps`` (or alias ``instrument_width_bps``),
     * else first ``manual_stacks[0].width_bps`` on that leg. Returns 0 if the AX is unknown.
     */
    int getMarketMakerDefaultWidthBpsForAx(const std::string& ax_symbol) const;
    /**
     * Last-resort width (bps) for desk ``mm_req_*`` when neither stack nor instrument supplies
     * a positive width. Default 6; minimum 1. Never uses legacy global bid spread for desk.
     */
    int getMarketMakerDeskWidthEmergencyFloorBps() const;
    double getMarketMakerBasis() const;
    std::string getMarketMakerTheoSymbol() const;
    int getMarketMakerUpdateIntervalSec() const;
    /** Tick size for the QUOTING market (where we place orders) */
    double getMarketMakerPriceTick() const;
    /** Tick size for the PRICING market (where we get theo from) - may differ from quote tick */
    double getMarketMakerPricingTick() const;
    bool getMarketMakerPaperSimulateFills() const;
    
    /** Max |net contracts| for inventory skew; beyond this skew is disabled (symmetric width only). */
    int getMarketMakerMaxPosition() const;
    /** Divisor NetPo in inventory skew (with adjust_ticks as multiplier on the floor). */
    int getMarketMakerAdjustPosition() const;
    /** Multiplier on floor(|NetPo|/adjust_position)*sign(NetPo) for skew tick units (min 1). */
    int getMarketMakerAdjustTicks() const;
    
    /** 
     * @brief When true, reload full quantity when a side is fully filled
     * Only reloads if the other side is NOT partially filled (to prevent asymmetric positions)
     */
    bool getMarketMakerReloadQty() const;

    /** When true, cancel/replace on every external theo tick (high churn). Default false = fill-driven + seed only. */
    bool getMarketMakerRequoteOnTheoMove() const;
    /**
     * Symbol-scoped override for `requote_on_theo_move` from `market_maker.instruments[]`.
     * Falls back to global `market_maker.requote_on_theo_move` when no per-leg value exists.
     */
    bool getMarketMakerRequoteOnTheoMoveForSymbol(const std::string& ax_symbol) const;
    /**
     * When false, C++ does not place or adopt MM orders (feed theo_move, timer, fill reload, desk signal).
     * Desk "Cancel ALL" sets this false in default_config.json. Default true.
     */
    bool getMarketMakerMmOrdersEnabled() const;
    /**
     * Symbol-scoped override for `mm_orders_enabled` from `market_maker.instruments[]`.
     * Falls back to global `market_maker.mm_orders_enabled` when no per-leg value exists.
     */
    bool getMarketMakerMmOrdersEnabledForSymbol(const std::string& ax_symbol) const;
    /**
     * Minimum |Δtheo| in pricing_tick units before a theo-driven requote runs (vs last successful MM anchor).
     * Clamped to [1, 1_000_000] at read time. Default 1 in JSON.
     */
    int getMarketMakerMinTheoMoveTicksToRequote() const;
    /** Ms to wait for cancel ack on a desk MM leg before REST reconcile (default 3000). */
    int getMarketMakerCancelAckTimeoutMs() const;
    /** Shutdown: max ms to poll GET /orders until flat before truncating orders.json (default 5000). */
    int getMarketMakerShutdownCancelTimeoutMs() const;
    /** When true, timer may cancel/replace when prices move. Default false = timer only seeds missing sides + side-pull cancels. */
    bool getMarketMakerRequoteOnTimer() const;
    /**
     * When true and |round(NetPo)| >= max_position: do not run runFullMmQuoteCycle for theo_move or timer_update
     * (avoids cancel/replace churn on every external theo tick while at inventory cap). Fill-driven and
     * timer_reconcile / one-leg seed paths still run. Default true.
     */
    bool getMarketMakerSuppressPeriodicTheoRequoteWhenAtInventoryCap() const;
    /**
     * When true, log the FEED PRICE CHANGE banner on every tiny theo move.
     * Default false = banner only on first theo or when rounded target bid/ask would change (less noise on FIX).
     */
    bool getMarketMakerFeedLogVerbose() const;
    /** Min ms between FEED PRICE CHANGE banners when targets jitter; 0 = no debounce. Default 1500. */
    int getMarketMakerFeedLogBannerMinIntervalMs() const;
    /** Extra half-spread ticks on the side that was fully filled (bid hit -> wider bid; ask hit -> wider ask). 0 = off. */
    int getMarketMakerPostFillExtraTicks() const;

    /** Extra ticks applied to BOTH bid and ask half-spreads vs theo (rest farther from mid; reduces immediate fills). */
    int getMarketMakerRestingDepthExtraTicks() const;

    /** When true, do not place a side that would cross AX best bid/ask (requires BBO from WS or market_quote provider). */
    bool getMarketMakerValidateVsMarket() const;
    /**
     * When true, do not rest inside the AX BBO spread: bid must be <= best bid, ask must be >= best ask
     * (join touch or behind; no price improvement between bid and ask). Requires BBO when enabled.
     */
    bool getMarketMakerNeverInsideSpread() const;
    /** When true, cancel MM quotes if Architect WebSocket is not connected (see market_maker.cancel_on_disconnect in JSON). */
    bool getMarketMakerCancelOnDisconnect() const;
    /** When true, cancel MM quotes when external feed publishes an invalid/stale quote (after consecutive errors, etc.). */
    bool getMarketMakerCancelOnExternalFeedInvalid() const;
    /** Stop placing new MM orders after this many counted reload cycles (0 = unlimited). Default 25. */
    int getMarketMakerMaxReloadCycles() const;
    /**
     * Exchange-accept counter for MM reload limit (persisted in primary `default_config.json` under
     * `market_maker.current_reload_count`). Only C++ increments on strategy exchange accepts; desk orders do not.
     */
    int getMarketMakerCurrentReloadCount() const;
    /**
     * Writes `market_maker.current_reload_count` into the primary config file (not overlays) and updates
     * the in-memory singleton. No-op file write if `primary_config_path_` is unset (in-memory only).
     */
    bool persistPrimaryConfigMarketMakerCurrentReloadCount(int count);
    /**
     * Increments `current_reload_count` in the primary config when `client_order_id` belongs to this MM strategy.
     * Skipped when `max_reload_cycles` is 0 (unlimited).
     * Fill-driven `fill_requote*` cycles bump via `bumpMarketMakerCurrentReloadCountForInstrumentFill` in
     * `processFill` instead (one count per qualifying fill). This path covers non-fill cycle reasons per
     * `reload_cycles_count_fill_only` / `mmCycleReasonCountsTowardReloadLimit`.
     */
    bool bumpMarketMakerCurrentReloadCountForMmAccept(const std::string& strategy_name,
                                                      const std::string& client_order_id);
    /**
     * Increments persisted `market_maker.current_reload_count` once per fill event (instrument-global counter).
     * Used instead of per-accept bumps for `fill_requote*` so a two-leg replace does not double-count.
     */
    bool bumpMarketMakerCurrentReloadCountForInstrumentFill();
    /**
     * When true (default), only fill-driven MM accepts bump `current_reload_count` (fill_requote*, mm_*_reload).
     * When false, every MM accept bumps except theo_move / timer_update. Implemented in MakeMarketStrategy::on_accept.
     */
    bool getMarketMakerReloadCyclesCountFillOnly() const;

    /** When true, trading loop reloads config and syncs MM orders from Python MM Live Desk signal file. */
    bool getMarketMakerDeskSyncEnabled() const;
    /** Path to JSON written by Python after desk apply (default logs/mm_desk_signal.json). */
    std::string getMarketMakerDeskSignalPath() const;
    // ==========================================================================
    // Hedge (on fill, send to external URL; recipient can place hedge order)
    // ==========================================================================
    
    bool isHedgeEnabled() const;
    std::string getHedgeWebhookUrl() const;
    
    // ==========================================================================
    // Callbacks
    // ==========================================================================
    
    void onConfigChange(ConfigChangeCallback callback);
    
    // ==========================================================================
    // Raw Access
    // ==========================================================================
    
    const json& raw() const { return config_; }
    
private:
    Config();
    ~Config() = default;
    
    void initDefaults();
    json getValue(const std::string& key) const;
    void setValue(const std::string& key, const json& value);
    void notifyChange(const std::string& key, const json& old_val, const json& new_val);
    /** Internal: persist `current_reload_count` to primary JSON (used under reload-counter mutex in Config.cpp). */
    bool persistPrimaryConfigMarketMakerCurrentReloadCountLocked(int clamped);
    
    json config_;
    mutable std::shared_mutex mutex_;
    std::vector<ConfigChangeCallback> change_callbacks_;
    /** Last filepath passed to loadFromFileWithOptionalOverlays (for reloadPrimaryConfigFromDisk). */
    std::string primary_config_path_;
};

// =============================================================================
// Template Implementations
// =============================================================================

template<typename T>
T Config::get(const std::string& key, const T& default_value) const {
    try {
        json val = getValue(key);
        if (val.is_null()) return default_value;
        return val.get<T>();
    } catch (...) {
        return default_value;
    }
}

template<typename T>
void Config::set(const std::string& key, const T& value) {
    json old_val = getValue(key);

    {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        setValue(key, json(value));
    }
    
    notifyChange(key, old_val, json(value));
}

} // namespace config
} // namespace architect
