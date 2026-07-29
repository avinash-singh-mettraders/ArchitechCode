#pragma once

/**
 * @file MakeMarketStrategy.h
 * @brief Make a market (bid + ask) on AX using theo from the configured external feed.
 *
 * Strategy Logic (Base Case):
 * 1. Confirm live feeds are up (including external pricing feed)
 * 2. Calculate theo midpoint from external feed
 * 3. Calculate QO bid/ask: width in ticks from theo, minus inventory skew (adjust_* × net); max_position only side-pull at cap
 * 4. Validate: QO Bid <= market's front offer (won't cross)
 * 5. Validate: QO Ask >= market's front bid (won't cross)
 * 6. If valid, submit bid and ask limit orders
 * 7. On theo change, recalculate and replace orders if moved > tick
 * 8. On fill, trigger hedge logic (placeholder for now)
 * 9. Track net position and directional exposure
 * 10. Reload filled order to full size
 */

#include "strategy/Strategy.h"
#include "strategy/MwrBreaker.h"
#include "strategy/FastMarketMonitor.h"
#include "strategy/PositionBook.h"
#include "config/Config.h"
#include "core/Types.h"
#include "marketdata/ExternalFeedManager.h"
#include "marketdata/ExternalFeedTypes.h"
#include <functional>
#include <optional>
#include <shared_mutex>
#include <chrono>
#include <unordered_map>
#include <unordered_set>
#include <cmath>
#include <limits>
#include <mutex>
#include <atomic>
#include <cstdint>

namespace architect {
namespace strategy {

using TheoProvider = std::function<std::optional<core::Price>(const std::string&)>;

/**
 * @brief Market data for QO validation (bid/ask from exchange)
 */
struct MarketQuote {
    core::Price bid{0.0};
    core::Price ask{0.0};
    bool valid{false};
    std::chrono::steady_clock::time_point timestamp;
};

using MarketQuoteProvider = std::function<std::optional<MarketQuote>(const std::string&)>;

/**
 * @brief Hedge trade record (simulated or real)
 */
struct HedgeTrade {
    std::string symbol;                      // Hedge product symbol (e.g., CME future root)
    core::Side side;
    int contracts{0};                        // Number of contracts (integer)
    core::Price fill_price{0.0};
    double notional_value{0.0};              // contracts * fill_price * multiplier
    std::chrono::steady_clock::time_point timestamp;
    bool simulated{true};                    // True = fake hedge for demo
    
    std::string toString() const {
        return "HedgeTrade{symbol=" + symbol + 
               ", side=" + std::string(core::sideToString(side)) +
               ", contracts=" + std::to_string(contracts) +
               ", fill_price=" + std::to_string(fill_price) +
               ", notional=" + std::to_string(notional_value) +
               ", simulated=" + (simulated ? "YES" : "NO") + "}";
    }
};

/**
 * @brief Hedge intent (before execution)
 */
struct HedgeIntent {
    std::string symbol;
    core::Side side;                    // Opposite of fill side
    int contracts{0};                   // Number of hedge contracts
    core::Price reference_price;
    core::OrderId triggering_fill_id;
    std::chrono::steady_clock::time_point created_at;
    bool executed{false};
    
    std::string toString() const {
        return "HedgeIntent{symbol=" + symbol + 
               ", side=" + std::string(core::sideToString(side)) +
               ", contracts=" + std::to_string(contracts) +
               ", ref_price=" + std::to_string(reference_price) + "}";
    }
};

/** Per-leg state for desk-driven cancel-replace (single bid + ask per stack). */
enum class MmLegState : std::int8_t {
    IDLE = 0,
    ADOPTED,
    CANCEL_PENDING,
    CANCELLED,
    PLACE_PENDING,
    FILLED,
    PAUSED,
    // Fire-and-track (place_before_cancel_ack=true, enforce only): the cancel REST has
    // been SENT but not yet confirmed, AND the replacement pair was placed without
    // waiting for the ack. Semantically identical to CANCEL_PENDING for every in-flight /
    // exposure / tombstone / resolution purpose (see mmLegStateIsCancelInFlight) — the
    // ONLY difference is that the re-add already fired. Resolved by (a) the async cancel
    // result, (b) a fill event, or (c) absence from the next VenueOrdersCache snapshot.
    CANCEL_SENT_UNCONFIRMED
};

/**
 * True when a leg has a cancel in flight (ack not yet observed). Covers BOTH the
 * classic ack-wait state (CANCEL_PENDING) and the fire-and-track state
 * (CANCEL_SENT_UNCONFIRMED). Every site that treats CANCEL_PENDING as
 * "in-flight / counts as worst-case exposure / tombstone blocks re-adopt /
 * eligible for cancel-ack resolution" must use this predicate so the two states
 * behave identically — the fire-and-track state differs ONLY in that the re-add
 * has already been placed.
 */
inline bool mmLegStateIsCancelInFlight(MmLegState st) {
    return st == MmLegState::CANCEL_PENDING || st == MmLegState::CANCEL_SENT_UNCONFIRMED;
}

/**
 * Routes cancel-ack (WS/REST/reconcile) to theo-move replace vs fill-requote opposite cancel.
 * FireAndTrack: the replacement pair was ALREADY placed at cancel-send time
 * (place_before_cancel_ack), so the cancel confirmation must only resolve the
 * old leg's tombstone and NEVER trigger a (second) re-add.
 */
enum class MmDeskCancelIntent : std::int8_t {
    None = 0, TheoMove = 1, FillOpposite = 2, DeskReseed = 3, FireAndTrack = 4 };

/**
 * Stack-level lifecycle state machine (2026-06-11 venue source-of-truth guard).
 *
 * Single source of truth for whether a mover-thread worker may operate on this
 * stack's venue orders. The VENUE_TRUTH poll and the desk teardown path both
 * converge on this atomic — a worker only proceeds when state == ACTIVE.
 *
 *   ACTIVE          normal operation
 *   VENUE_ORPHANED  venue lost our orders (live_oids=0); reconciling / re-arming
 *   TEARDOWN        desk-side teardown in progress (mmTearDownByStackId)
 *   DEAD            unregistered; memory will be freed
 *
 * Transitions are monotonic toward DEAD: TEARDOWN may only advance to DEAD, and
 * DEAD is terminal. ACTIVE <-> VENUE_ORPHANED is the only reversible edge (a
 * transient venue glitch auto-recovers; a still-desired stack re-arms).
 */
enum class MmStackState : std::int8_t {
    ACTIVE = 0,
    VENUE_ORPHANED,
    TEARDOWN,
    DEAD
};

/** Human-readable label for MmStackState (logging). */
inline const char* mmStackStateName(MmStackState s) {
    switch (s) {
        case MmStackState::ACTIVE:         return "ACTIVE";
        case MmStackState::VENUE_ORPHANED: return "VENUE_ORPHANED";
        case MmStackState::TEARDOWN:       return "TEARDOWN";
        case MmStackState::DEAD:           return "DEAD";
    }
    return "UNKNOWN";
}

/** One resting row from GET /open-orders for product-level converge venue diff. */
struct MmVenueOpenRow {
    std::string oid;
    bool is_buy{false};
    double price{0.0};
};

struct TrackedLeg {
    MmLegState state{MmLegState::IDLE};
    std::string exchange_oid;
    /** Local OrderManager id (monotonic). */
    core::OrderId local_oid{0};
    core::Side side{core::Side::BUY};
    /** Last exchange working price for this leg. */
    core::Price exchange_px{0.0};
    core::Quantity qty{0.0};
    core::Quantity remaining_qty{0.0};
    std::int64_t state_entered_steady_ms{0};
    std::string pending_place_client_id;
    /** F3: opposite leg filled while this leg was not ADOPTED — cancel once state allows. */
    bool cancel_after_opposite_settled{false};
    MmDeskCancelIntent pending_cancel_intent{MmDeskCancelIntent::None};
    /** After cancel-ack timeout: one extra REST cancel if GET /orders still shows the order resting. */
    bool desk_cancel_timeout_retry_sent{false};
};

/**
 * @brief Net position and exposure tracking (all in notional USD)
 * 
 * Per the requirement: Track everything in notional USD because
 * AX perp and hedge product have different notional values.
 * 
 * Example: configure hedge.multiplier and hedge.initial_price so that
 *   hedge_notional = initial_price × multiplier matches your hedge contract's USD per contract.
 */
struct PositionState {
    // Primary tracking: Net position in notional USD
    double net_position_usd{0.0};            // + = long exposure, - = short exposure
    
    // Secondary tracking (for reference)
    core::Quantity net_position_qty{0.0};    // Net quantity (AX fills only)
    double total_traded_notional{0.0};       // Absolute sum of all trades
    std::uint64_t ax_fills_count{0};         // Count of AX fills
    std::uint64_t hedge_trades_count{0};     // Count of hedge trades
    
    /**
     * @brief Record an AX fill (updates net position in USD)
     */
    void onAxFill(core::Side side, core::Quantity qty, core::Price price) {
        double notional = qty * price;
        double signed_notional = (side == core::Side::BUY) ? notional : -notional;
        
        net_position_usd += signed_notional;
        net_position_qty += (side == core::Side::BUY) ? qty : -qty;
        total_traded_notional += notional;
        ++ax_fills_count;
    }
    
    /**
     * @brief Record a hedge trade (updates net position in USD)
     * @param contracts Number of contracts (positive)
     * @param fill_price Price of hedge product
     * @param multiplier Contract multiplier (venue-specific; from hedge.multiplier)
     * @param side BUY or SELL
     */
    void onHedgeTrade(core::Side side, int contracts, core::Price fill_price, double multiplier) {
        double notional = contracts * fill_price * multiplier;
        double signed_notional = (side == core::Side::BUY) ? notional : -notional;
        
        net_position_usd += signed_notional;
        total_traded_notional += notional;
        ++hedge_trades_count;
    }
    
    /**
     * @brief Calculate how many hedge contracts are needed
     * @param hedge_product_notional Notional value of 1 hedge contract
     * @return Number of contracts (can be negative for short)
     */
    double hedgeContractsNeeded(double hedge_product_notional) const {
        if (hedge_product_notional <= 0) return 0;
        return net_position_usd / hedge_product_notional;
    }
    
    bool isLongExposure() const { return net_position_usd > 0; }
    bool isShortExposure() const { return net_position_usd < 0; }
    bool isFlat() const { return std::fabs(net_position_usd) < 0.01; }
};

/**
 * @brief Quote Order validation result
 */
struct QOValidation {
    bool valid{false};
    bool bid_would_cross{false};
    bool ask_would_cross{false};
    /** True when bid is strictly above AX best bid but below best ask (improves bid without crossing). */
    bool bid_inside_spread{false};
    /** True when ask is strictly below AX best ask but above best bid (improves ask without crossing). */
    bool ask_inside_spread{false};
    bool spread_inverted{false};
    std::string reason;
};

class MakeMarketStrategy;

/**
 * RAII bundle for the per-AX cross-stack cycle lock (2026-06-11 venue source-of-truth
 * guard). Holds a `shared_ptr` to every locked stack so the locks — and the
 * `mm_cycle_mutex_` they reference — cannot outlive the strategy objects. This closes
 * a use-after-free where the orders.json reconcile thread could free a sibling stack
 * while `runFullMmQuoteCycle` still held that stack's cycle mutex.
 *
 * Member order is load-bearing: `owners` is declared BEFORE `locks`, so on destruction
 * `locks` (declared later) is destroyed FIRST — every mutex is unlocked before the
 * corresponding object's refcount is dropped.
 */
struct MmAllStacksCycleLock {
    std::vector<std::shared_ptr<MakeMarketStrategy>> owners;
    std::vector<std::unique_lock<std::recursive_mutex>> locks;
};

class MakeMarketStrategy : public BaseStrategy {
public:
    MakeMarketStrategy();
    explicit MakeMarketStrategy(const std::string& name);
    ~MakeMarketStrategy() override;

    // =========================================================================
    // Configuration
    // =========================================================================
    
    void set_theo_provider(TheoProvider fn) { theo_provider_ = std::move(fn); }
    void set_market_quote_provider(MarketQuoteProvider fn) { market_provider_ = std::move(fn); }

    /** Multi-leg MM: AX symbol, optional gateway order_symbol, Neon FIX 55 (or REST key) for theo. Empty strings = use global config. */
    void setMmInstrumentLeg(std::string ax_symbol, std::string order_symbol, std::string reference_fix_symbol);

    /**
     * @brief Product-level (leg-level) `max_position` cap, shared across every stack of this AX symbol.
     *
     * Set once at strategy construction from `MarketMakerInstrumentLeg::max_position`. When > 0 it
     * overrides any per-stack `max_position` AND the global `market_maker.max_position` — the gate
     * becomes "for this AX product, |sum of long − sum of short| < this cap", which is exactly what
     * the user means by "max position is at the product level". Pass 0 to disable the override.
     */
    void setMmInstrumentMaxPosition(int max_po, std::string source = "instrument") {
        if (max_po > 0) {
            mm_instrument_max_position_ = max_po;
            mm_max_position_source_ = std::move(source);
            if (mm_max_position_source_.empty()) {
                mm_max_position_source_ = "instrument";
            }
            return;
        }
        mm_instrument_max_position_ = 0;
        mm_max_position_source_.clear();
    }
    [[nodiscard]] int mmInstrumentMaxPosition() const { return mm_instrument_max_position_; }

    /**
     * @brief Product-level (leg-level) `max_reload_cycles` cap, shared across every stack of this AX symbol.
     *
     * Set once at strategy construction from `MarketMakerInstrumentLeg::max_reload_cycles` and/or
     * orders.json product override. When > 0 it overrides per-stack values and global
     * `market_maker.max_reload_cycles` for this AX product. Pass 0 to disable the override.
     */
    void setMmInstrumentMaxReloadCycles(int max_reload, std::string source = "instrument") {
        if (max_reload > 0) {
            mm_instrument_max_reload_cycles_ = max_reload;
            mm_max_reload_source_ = std::move(source);
            if (mm_max_reload_source_.empty()) {
                mm_max_reload_source_ = "instrument";
            }
            return;
        }
        mm_instrument_max_reload_cycles_ = 0;
        mm_max_reload_source_.clear();
    }
    [[nodiscard]] int mmInstrumentMaxReloadCycles() const { return mm_instrument_max_reload_cycles_; }

    /**
     * @brief Per-leg quote tick (price units of the AX symbol).
     *
     * AX `/instruments` exposes very different ticks per product (FX majors vs metals vs
     * inverse FX, …). Using the global `market_maker.price_tick` for every leg produces
     * invalid prices on most products. Pass > 0 here to override; 0 = fall through to global.
     */
    void setMmInstrumentTick(double t) { mm_instrument_tick_ = (t > 0.0 ? t : 0.0); }
    [[nodiscard]] double mmInstrumentTick() const { return mm_instrument_tick_; }

    /**
     * Per-leg contract lot step from `market_maker.instruments[].order_size_step`. When >0 and
     * the active manual stack omits `order_size_step`, this is used (before global/desk fallbacks).
     */
    void setMmInstrumentOrderSizeStep(int s) { mm_instrument_order_size_step_ = (s > 0 ? s : 0); }
    [[nodiscard]] int mmInstrumentOrderSizeStep() const { return mm_instrument_order_size_step_; }

    /**
     * @brief Theo scale (legacy multiplicative transform; superseded by pricer-snapshot xform).
     *
     * Applied to the raw mid from `theo_provider_(theo_venue_symbol)` ONLY when the desk
     * pricer-snapshot transform is NOT active for this strategy:
     *   theo := raw
     *   if (mm_theo_scale_ > 0.0)   theo := theo * mm_theo_scale_
     *
     * When the operator has typed (quote_snapshot > 0 AND pricer_snapshot > 0) into the active
     * manual stack, MakeMarketStrategy::mmTransformTheo SHORT-CIRCUITS and returns the raw mid
     * unchanged — the snapshot formula carries the scale relationship between pricer and quote
     * markets.
     *
     * `invert_theo` was REMOVED (2026-05-12) — it was a recurring footgun that turned a 157
     * USD/JPY mid into a 0.006 quote scale and back. The pricer-snapshot transform is the only
     * supported way to bridge different price scales between feed and AX product.
     */
    void setMmTheoScale(double s) { mm_theo_scale_ = (s > 0.0 ? s : 0.0); }
    [[nodiscard]] double mmTheoScale() const { return mm_theo_scale_; }

    /**
     * @brief Bind this leg to one of the external feeds for trade-gating.
     *
     * Accepts the resolved theo source string ("mettraders"|"hyperliquid"|"neon_fix").
     * When the bound feed is reported down by `ExternalFeedManager::isFeedUpForSource`,
     * `runFullMmQuoteCycle` short-circuits without quoting and exposes the
     * blocked status via `mmIsBlockedByFeed()` / `mmFeedBlockReason()` so the
     * desk can flag the leg in red. Empty string = no gating (legacy behavior).
     */
    void setMmTheoSource(std::string theo_source) { mm_theo_source_ = std::move(theo_source); }
    [[nodiscard]] const std::string& mmTheoSource() const { return mm_theo_source_; }
    [[nodiscard]] bool mmIsBlockedByFeed() const { return mm_blocked_by_feed_.load(); }
    [[nodiscard]] std::string mmFeedBlockReason() const;

    /**
     * @brief Reduce-only mode (max_position cap reached at the product level).
     *
     * Becomes true the moment `|net_position_for_this_AX| >= mmEffMaxPositionInt(cfg)`.
     * While true:
     *   - The grow-side quote is suppressed (`shouldQuoteSide` returns false).
     *   - Any live grow-side order is cancelled at the next `runFullMmQuoteCycle` /
     *     `timerReconcilePairQuotes` tick (per-side cancel branch — not full-cancel,
     *     because `inventory_inside == false`).
     *   - The reduce side stays alive — fills there bring the position back inside the
     *     cap, at which point `shouldQuoteSide` re-permits both sides and the strategy
     *     resumes pair quoting on its next cycle.
     *
     * For a product with N active stacks, this means N single-side reduce-only orders
     * at the cap (instead of 2N pair quotes), exactly as the user's spec describes —
     * since every stack reads the same exchange-aggregated NetPo and the same leg cap.
     *
     * Side semantics of `mmReduceOnlySide()`:
     *   - 0 = NONE       (inside the cap; pair-quote as normal)
     *   - 1 = BUY-ONLY   (we are short ≥ cap; only BUYs are allowed, to flatten)
     *   - 2 = SELL-ONLY  (we are long  ≥ cap; only SELLs are allowed, to flatten)
     */
    [[nodiscard]] bool mmIsReduceOnly() const { return mm_reduce_only_active_.load(); }
    [[nodiscard]] int  mmReduceOnlySide() const { return mm_reduce_only_side_.load(); }

    /**
     * @brief Durable handle for the mm_orders.json flow.
     *
     * When this strategy is spawned from a `mm_order_requests.json` row the
     * caller stamps the request UUID here. The C++ side later uses it to look
     * the strategy up on cancel and to write the matching entry into
     * `mm_orders.json` once the AX gateway acks the first place.
     */
    // CONCURRENCY: mm_order_request_id_ is written by the desk background thread
    // (adopt/clear) and read on mover/feed/OM/timer threads. A std::string racing a
    // reader can free-then-read its buffer → segfault. Guard writes with a unique
    // lock and hand out COPIES to readers under a shared lock. mmIsDeskManaged()'s
    // hot-path emptiness check reads a lock-free atomic kept in sync here.
    void setMmOrderRequestId(std::string id) {
        std::unique_lock<std::shared_mutex> lk(mm_desk_cfg_mu_);
        mm_is_desk_managed_.store(!id.empty(), std::memory_order_release);
        mm_order_request_id_ = std::move(id);
    }
    [[nodiscard]] std::string mmOrderRequestId() const {
        std::shared_lock<std::shared_mutex> lk(mm_desk_cfg_mu_);
        return mm_order_request_id_;
    }
    /** Single-lock read of the id for logging: returns "(legacy)" when unset. Avoids the
     *  empty()?...:id TOCTOU of two separate accessor calls. */
    [[nodiscard]] std::string mmOrderRequestIdOrLegacy() const {
        std::shared_lock<std::shared_mutex> lk(mm_desk_cfg_mu_);
        return mm_order_request_id_.empty() ? std::string("(legacy)") : mm_order_request_id_;
    }
    /** Lock-free "has a non-empty order request id" (mirrors mm_order_request_id_ emptiness). */
    [[nodiscard]] bool mmHasOrderRequestId() const {
        return mm_is_desk_managed_.load(std::memory_order_acquire);
    }

    static void armDeskAutorepriceForAllDeskStacksOnAx(const std::string& ax_symbol);

    /**
     * Mirrors of `bid_order_id_` / `ask_order_id_` etc. for the desk's
     * `mm_orders.json` snapshot.
     *
     * CONCURRENCY (2026-05-20 audit, fix C3): backing fields are
     * `std::atomic<OrderId>` because they are read on the feed worker thread
     * (onFeedUpdate logging / state inspection), the MmOrderMover worker
     * (cycle planning), and the OrderManager event-dispatch thread
     * (on_accept / on_reject / on_cancel write), and also from the desk
     * snapshot reader. Lock-free reads via `.load(memory_order_acquire)` to
     * pair with `.store(memory_order_release)` on the writers, ensuring any
     * tracked-leg state mutated under `tracked_orders_mutex_` BEFORE the id
     * publication is visible to a reader that sees the new id.
     */
    [[nodiscard]] OrderId mmCurrentBidOrderId() const { return bid_order_id_.load(std::memory_order_acquire); }
    [[nodiscard]] OrderId mmCurrentAskOrderId() const { return ask_order_id_.load(std::memory_order_acquire); }

    /**
     * @brief OM_GATEWAY venue-truth disappearance hook (called from
     *        StartupSequence.cpp when /open-orders no longer reports a
     *        tracked exchange OID).
     *
     * Distinguishes fill from deliberate cancel by reading the tracked leg's
     * `MmLegState` under `tracked_orders_mutex_`:
     *   - `ADOPTED` (we believed it was working) → treat as FULL FILL.
     *     Synthesizes an `OrderEventData` from the leg's last placed price and
     *     qty, emits `[FILL_DESK]`, and calls `processFillDeskStack` (which
     *     marks this leg `FILLED`, cancels the opposite ADOPTED leg, and
     *     enqueues the post-fill requote cascade).
     *   - `CANCEL_PENDING` → our own cancel REST completed; NOT a fill. The
     *     existing cancel-ack branch in `on_cancel` will fire when the caller
     *     subsequently invokes `om.onOrderCancelled`.
     *   - `PLACE_PENDING` → silent place failure; logs a warning and skips.
     *   - Any other state → already settled; no-op.
     *
     * The caller MUST still invoke `om.onOrderCancelled(local_oid)` after this
     * returns. For the fill case the leg has already transitioned to
     * `MmLegState::FILLED` inside `processFillDeskStack`, so `on_cancel` falls
     * through to its erase branch (no double-fire). For the non-fill case
     * `on_cancel` runs as before.
     *
     * @return true if a `[FILL_DESK]` route fired, false otherwise.
     */
    bool mmRouteOmGatewayVenueTruthMissing(OrderId local_oid,
                                           const std::string& exchange_oid,
                                           const std::string& sym);

    [[nodiscard]] core::Price mmLastBidPrice() const { return last_bid_price_; }
    [[nodiscard]] core::Price mmLastAskPrice() const { return last_ask_price_; }
    [[nodiscard]] core::Quantity mmLastBidQty() const { return original_bid_qty_; }
    [[nodiscard]] core::Quantity mmLastAskQty() const { return original_ask_qty_; }
    /** True once at least one place ack has been observed for this strategy. */
    [[nodiscard]] bool mmHasFirstAcceptHappened() const { return mm_first_accept_seen_.load(); }
    [[nodiscard]] std::int64_t mmFirstAcceptMs() const { return mm_first_accept_ms_.load(); }

    /**
     * Total fill events observed (partial + complete). Read by the desk for
     * the "fills received" counter — bumps on every on_fill / on_partial_fill
     * regardless of side. Cheap counter, never reset.
     */
    [[nodiscard]] std::uint64_t mmFillsCount() const { return mm_fills_count_.load(); }
    /**
     * Count of fills synthesized by the desk-managed venue-truth-missing path
     * (`mmRouteOmGatewayVenueTruthMissing` → `processFillDeskStack`). These
     * bypass `OrderManager::onOrderFilled` so they are NOT counted in
     * `OrderManager::Stats::total_fills` / HEARTBEAT `fills=`. Surfaced
     * separately by HEARTBEAT as `desk_synth_fills=` so triage cannot be
     * misled by `fills=0` while venue truth shows fills occurring (see the
     * SILVER 2026-05-21 incident in `Example.txt`).
     */
    [[nodiscard]] std::uint64_t mmDeskSynthFillsCount() const {
        return mm_desk_synth_fills_count_.load();
    }
    /** AX order_id from the most recent fill (decimal string). Empty until first fill. */
    [[nodiscard]] std::string mmLastFillIdStr() const;
    /** Steady-clock ms at the most recent fill. 0 = none yet. */
    [[nodiscard]] std::int64_t mmLastFillMs() const { return mm_last_fill_ms_.load(); }

    /** AX gateway order symbol (where orders go) — typically equals ax_symbol. Empty = use config. */
    [[nodiscard]] const std::string& mmOrderSymbolOverride() const { return mm_order_symbol_override_; }
    /** Reference theo symbol (FIX 55 / venue id) supplied via setMmInstrumentLeg(...). Empty = use config. */
    [[nodiscard]] const std::string& mmReferenceFixOverride() const { return mm_theo_symbol_override_; }
    /**
     * Optional stack params (width / order_size / max_position / …) for orders.json snapshot.
     *
     * CONCURRENCY: returns a COPY under a shared lock. `mm_manual_stack_` is replaced
     * wholesale by the desk background thread at runtime (adopt/clear) while mover,
     * feed, timer and OM threads read it; it embeds std::strings, so a lock-free read
     * racing the writer could deref freed string buffers → segfault. Callers must use
     * the returned snapshot (never re-read the member across the operation). Binding it
     * to `const auto&` is fine — the temporary's lifetime is extended.
     */
    [[nodiscard]] std::optional<architect::config::MarketMakerManualStack> mmOptionalManualStack() const {
        std::shared_lock<std::shared_mutex> lk(mm_desk_cfg_mu_);
        return mm_manual_stack_;
    }
    /** Thread-safe "manual stack present AND desk_seeded" check (guarded read of a single flag). */
    [[nodiscard]] bool mmManualStackDeskSeeded() const {
        std::shared_lock<std::shared_mutex> lk(mm_desk_cfg_mu_);
        return mm_manual_stack_.has_value() && mm_manual_stack_->desk_seeded;
    }
    /**
     * When set, this strategy is one row of ``market_maker.instruments[].manual_stacks[]``; quote math uses
     * stack width/order_size/max/… instead of global market_maker.*.
     */
    void setOptionalManualStack(const std::optional<architect::config::MarketMakerManualStack>& stack);

    /**
     * orders.json reconcile (legacy ``allow_multi_mm_per_ax=false`` path only): adopt a desk-seeded
     * stack into this config-registered ``make_market_*`` quoter instead of spawning ``mm_req_*``.
     * Binds ``stack.id`` and applies the same gateway-seed path as
     * ``tryAdoptGatewayPlacedFromManualStack``.
     *
     * NOTE: ``market_maker.allow_multi_mm_per_ax`` defaults to ``true`` — under that default the
     * spawn path creates a separate ``mm_req_<AX>_<stack_id>`` strategy per desk stack, so each
     * pair has independent ``tracked_bids_``/``tracked_asks_``, ``mm_manual_stack_``, and per-pair
     * width/min_drift gates. Refuses to clobber an already-bound stack (returns false) when an
     * incoming ``stack.id`` differs from the currently bound one, to keep the legacy single-quoter
     * path safe.
     */
    [[nodiscard]] bool mmDeskReconcileAdoptStack(const architect::config::MarketMakerManualStack& stack);

    /** orders.json reconcile: stack row removed — REST-cancel tracked desk legs, clear desk fields; strategy stays registered. */
    void mmDeskReconcileClearStackBinding();
    /** Reconcile helper for adopted base quoters: explicit desk-route state flip. */
    void setMmDeskActive(bool active) { mm_desk_active_ = active; }
    /**
     * Spawn-time adoption entry for fresh ``mm_req_<AX>_<stack_id>`` strategies (called from
     * ``mmSpawnFromOrdersStack`` after start). Without this, the strategy stays PASSIVE and the
     * desk-placed pair is never tracked. Safe to call repeatedly; idempotent and gated by
     * ``mm_manual_stack_->desk_seeded`` and ``placed_*_price > 0``.
     */
    bool mmDeskBootstrapAdopt() { return tryAdoptGatewayPlacedFromManualStack(); }

    /** Register for direct feed updates (bypasses timer for ultra-low latency) */
    void registerFeedCallback();

    /** After startup, align strategy inventory with exchange portfolio (crash / restart safety). */
    void initialize(DateInt date) override;
    
    // =========================================================================
    // Event Callbacks
    // =========================================================================
    
    void on_timer(DateInt date, TimeRack time_rack) override;
    void on_accept(const OrderEventData& accept) override;
    void on_fill(const OrderEventData& fill) override;
    void on_partial_fill(const OrderEventData& fill) override;
    void on_cancel(const OrderEventData& cancel) override;
    void on_reject(const OrderEventData& reject) override;
    
    // =========================================================================
    // State Access
    // =========================================================================
    
    const PositionState& getPositionState() const { return position_state_; }
    const std::vector<HedgeIntent>& getPendingHedges() const { return pending_hedges_; }

    /** AX venue symbol for orders (per-leg when market_maker.instruments is used). */
    std::string mmAxSymbol() const;

    /**
     * REST fill referenced an exchange order_id not in OrderManager; if symbol matches MM config,
     * sync NetPo from exchange and requote (same path as fill-driven reload).
     */
    void reconcileFromExchangeAfterOrphanFill(const std::string& fill_symbol);
    static void notifyAllRegisteredOrphanExchangeFill(const std::string& fill_symbol);

    /**
     * Orphan REST fill with no usable symbol: refresh this leg's exchange position; if NetPo changed vs
     * pre-refresh, sync and requote (covers gateway payloads missing symbol / order already gone from
     * open-orders after a full fill).
     */
    void reconcileFromExchangeAfterOrphanFillUnknownSymbol();
    static void notifyAllRegisteredOrphanExchangeFillUnknownSymbol();

    /**
     * Global fast-market breaker fan-out: pull tracked legs on EVERY live MM strategy/AX.
     * Snapshots getAllStrategies() and, per stack, enqueues a mover job that re-pins the strategy
     * via getStrategy(name) (committed lifetime pattern) and cancels BUY+SELL through the existing
     * mmCancelTrackedLegThisStackGateway — no new cancel path. Called by FastMarketMonitor on the
     * mover when the HL SPX breaker trips. Static so it can drive cancels across all stacks.
     */
    static void mmFastMarketPullAllTrackedLegs(const std::string& reason);

    /**
     * Desk-driven targeted force-cancel using VENUE TRUTH (OrderManager / mm_orders.json), independent
     * of orders.json presence. Recovery path for the "orders rest on the exchange but the desk cancel
     * dropdown is empty" split: the desk removes the stack from orders.json (its desired state) and
     * posts (ax_symbol, stack_id) for the C++ loop to drain, so C++ cancels the still-live venue legs
     * it tracks for that stack. Same mover-serialized, committed-lifetime re-pin pattern as
     * mmFastMarketPullAllTrackedLegs (cancels run on the one mover worker; a stack torn down between
     * snapshot and drain is a safe no-op). Matches on ax_symbol (case-insensitive) and, when non-empty,
     * stack_id; an empty stack_id targets every stack on ax_symbol. Returns the number of matching live
     * strategies dispatched.
     */
    static int mmDeskForceCancelStackGateway(const std::string& ax_symbol,
                                             const std::string& stack_id,
                                             const std::string& reason);

    /** After account-wide feed guardian cancel-all: reset MM working ids without another REST cancel. */
    void feedGuardianResetLocalState(const std::string& tag);
    static void feedGuardianResetAllRegistered(const std::string& tag);

    /**
     * SIGINT/SIGTERM path: REST-cancel all desk stack legs, poll GET /orders, truncate orders.json,
     * log [SHUTDOWN] lines. Call before StrategyManager::stopAll while REST/OM still usable.
     */
    static void deskShutdownCancelPollTruncateOrdersJson(const std::string& shutdown_reason,
                                                         std::int64_t& out_cancel_poll_ms);
    /** Per strategy (X2): log each tracked desk leg OID and issue REST cancel. */
    int mmDeskShutdownLogAndCancelTrackedLegs(
        std::vector<std::pair<std::string, std::string>>* out_pending_cancel_oid_by_ax = nullptr);
    /** Per-leg variant: only reset MM strategies whose theo_source feed is currently down
     *  (per `ExternalFeedManager::isFeedUpForSource`). Legs bound to healthy feeds keep quoting. */
    static void feedGuardianResetForDownFeeds(const std::string& tag);

    bool adoptBootstrappedTrackedOrder(core::OrderId order_id,
                                       const std::string& exchange_oid,
                                       core::Side side,
                                       core::Price price,
                                       core::Quantity qty);

    /**
     * Startup pair-guarantee (legacy YAML `make_market_*` only when market_maker.cpp_manual_quotes_only
     * is false and cpp_may_place_orders is true): if this leg's theo_source feed is up and a theo is
     * available, ensure at least one active bid AND ask exists in OrderManager —
     * otherwise force `runFullMmQuoteCycle(theo, tag)`.
     *
     * Desk `mm_req_*`: never auto-places missing sides here (operator seeds OIDs in orders.json / GUI).
     * Default config: cpp_manual_quotes_only=true also disables legacy auto-place from this path.
     *
     * No-op when feed down, mm_orders_disabled, AX WS down with cancel_on_disconnect, or the pair already exists.
     */
    void ensureStartupQuotePair(const std::string& tag);
    static void ensureStartupQuotePairAllRegistered(const std::string& tag);

    /**
     * Instrument-level max_position hot-update: every mm_req_* / make_market_* on `ax_symbol`
     * gets a mover cycle. Cap increase → two-sided requote; cap decrease or |net|>=new cap →
     * product-wide grow-side pull then per-stack reduce-only cycle.
     */
    static void mmNotifyInstrumentMaxPositionChanged(const std::string& ax_symbol,
                                                     int old_cap,
                                                     int new_cap);

    /** Reload is assumed done. Cancels all MM orders when mm_orders_enabled transitions false (takes mm_cycle_mutex). */
    void mmMaybeCancelOnMmOrdersDisabled(const config::Config& cfg);
    /** Caller must hold mm_cycle_mutex (same as runFullMmQuoteCycle). */
    void mmUpdateMmOrdersEnabledTransitionLocked(const config::Config& cfg);
    /** After reload + gates: strategy on and mm_orders_enabled. */
    bool mmMarketMakerOrdersPlacementAllowed(const config::Config& cfg) const;

    /**
     * Single source of truth for "may this stack place a NEW order right now?".
     * ORs every placement suppression that already exists, reusing the existing helpers
     * (no new/duplicated condition): the per-instrument HOLD + strategy-off gate
     * (`mmMarketMakerOrdersPlacementAllowed`, which is `market_maker.mm_orders_enabled`
     * for this ax_symbol) AND the MWR volatility / global fast-market pause
     * (`mmMwrIsPausedNow`). Cancels/reconciles are deliberately NOT gated by this — only
     * placement. Every place site routes through this so the per-instrument cancel-all
     * HOLD is exhaustive (Phase 2, per-instrument-cancel-all). Pure predicate; no I/O.
     */
    bool mmPlacementAllowedForSymbol(const config::Config& cfg) const;

    /** Sync working bid/ask from MM Live Desk signal (Python apply); adopts exchange OIDs into OrderManager. */
    bool applyMmDeskSignalJson(const std::string& json_body);

protected:
    bool orderEventMatchesStrategy(const events::OrderEventData& data, events::EventType type) const override;

private:
    [[nodiscard]] bool mmIsDeskManaged() const;
    void addOrUpdateTrackedOrder(core::OrderId order_id,
                                 const std::string& exchange_oid,
                                 core::Side side,
                                 core::Price price,
                                 core::Quantity qty);
    bool hasTrackedOrders() const;
    bool processTrackedOrdersTheoMove(core::Price theo_midpoint, const std::string& reason);
    void maybeReconcileDeskCancelAckTimeouts();
    void refreshLegacyTopOfBookTrackingFromVectors();
    /** GET /orders body: true if any row matches exchange oid `want_oid`. */
    static bool mmRestOrdersBodyContainsExchangeOid(const std::string& body, const std::string& want_oid);

    /**
     * @brief True if this strategy's pair has a cancel-replace cycle in flight.
     *
     * Returns true when ANY of the following hold:
     *   - `mm_pending_accepts_ > 0` (we have outstanding place_acks expected)
     *   - any tracked bid/ask leg is in `MmLegState::CANCEL_PENDING`
     *   - any tracked bid/ask leg is in `MmLegState::PLACE_PENDING`
     *
     * Used by `onFeedUpdate` and the timer-driven theo-move path to skip the
     * drift-gate evaluation while a cycle is mid-flight. The theo can move
     * arbitrarily during the cycle and the strategy ignores it FOR THIS PAIR
     * — see `mmMaybePostCycleRecheck` for the cycle-completion re-evaluation
     * that fires once the cycle finishes.
     *
     * Multi-pair independence is automatic: each MM stack has its own
     * `MakeMarketStrategy` instance, so this predicate is per-pair.
     *
     * Locking: takes `tracked_orders_mutex_` internally (the same mutex that
     * guards leg state transitions), so the snapshot is consistent.
     */
    [[nodiscard]] bool isPairCycleInFlight() const;

    /**
     * @brief Cycle-completion hook — called from `on_accept` after the place-ack
     *        for the second leg of a cancel-replace cycle has landed.
     *
     * Re-reads the current theo, recomputes target bid/ask, and compares
     * against the just-placed `last_bid_price_`/`last_ask_price_`. If the
     * drift gate (`min_theo_move_ticks_to_requote × quote_tick`) is tripped
     * — i.e. the theo moved enough during the cycle that we'd want to
     * re-fire — logs `[CYCLE_RECHECK]` and enqueues a new cycle through
     * `enqueueQuoteCycleOnMover`. Otherwise logs `[CYCLE_DONE]` and lets
     * normal feed-driven evaluation resume (which is no longer suppressed
     * by `isPairCycleInFlight()` because the cycle just completed).
     *
     * Must be called AFTER the leg has transitioned to `MmLegState::ADOPTED`
     * and `mm_pending_accepts_` has been decremented.
     */
    void mmMaybePostCycleRecheck();
    void deskTheoMoveAfterCancelAckPlaceOneLeg(core::Side side, const char* leg_name);
    /**
     * Pillar B — fire-and-track pair place (place_before_cancel_ack=true, ENFORCE only).
     * Called from processTrackedOrdersTheoMove immediately AFTER the pair cancels are
     * enqueued, without waiting for the cancel acks. Reads effective() ONCE for the
     * whole reduce-only decision, then places the fresh pair at the given targets via
     * mmPlaceReservedLeg (whose per-side effective()-driven cap gate is the final guard).
     * Stamps [MM_TT] phase=place_sent ack_mode=pre. Assumes the caller holds mm_cycle_mutex_.
     *
     * When market_maker.async_batch_submit is ON and `batch_cancel_ids` is non-empty, the pair
     * cancels are NOT enqueued on the mover ahead of this call — instead the cancels + the fresh
     * places are fired together in ONE curl-multi batch (Platform::placeAndCancelConcurrent,
     * ~1 RTT for the whole cancel-replace cycle). The SELF-TRADE GUARD (new bid >= old ask, or
     * new ask <= old bid, using `old_bid_px`/`old_ask_px`) forces a safe fallback on big theo
     * jumps: cancel the pair (await), THEN place the fresh pair (~2 RTT, only when a fresh leg
     * could cross a still-live old leg). `old_bid_px`/`old_ask_px` are the working prices of the
     * resting legs being cancelled (0 = none on that side).
     */
    void mmFireAndTrackPlacePair(core::Price tgt_bid, core::Price tgt_ask,
                                 core::Price theo_mid, const std::string& reason,
                                 const std::vector<core::OrderId>& batch_cancel_ids = {},
                                 core::Price old_bid_px = 0.0,
                                 core::Price old_ask_px = 0.0);
    void tryDeskFillRequoteF4PlaceFreshPair();
    void processFillDeskStack(const events::OrderEventData& fill, core::Quantity incremental_fill_qty);
    TrackedLeg* findTrackedByOrderId(core::OrderId order_id, core::Side* side_out = nullptr);
    TrackedLeg* findTrackedByPendingClientId(const std::string& client_order_id, core::Side* side_out = nullptr);
    void eraseTrackedByOrderId(core::OrderId order_id);

    bool tryAdoptGatewayPlacedFromManualStack();
    /** Desk (`mm_req_*`): stack row must still exist in orders.json (live or frozen snapshot). */
    [[nodiscard]] bool mmDeskOrdersJsonContainsThisStack() const;
    /** Desk: both tracked venue OIDs must be active LIMIT rows before cancel/replace moves. */
    [[nodiscard]] bool mmDeskTrackedOrdersAliveForMove() const;
    /** No ADOPTED/CANCEL_PENDING legs — safe to run full pair `theo_move` after cancels. */
    [[nodiscard]] bool mmDeskPairCancelSettledForTheoReplace() const;
    bool applyMmDeskSeededJsonLocked(
        const architect::config::json& j, bool require_desk_sync, bool stamp_first_ack_from_adopted);

    /**
     * Desk stacks (`mm_req_*`): when Python has seeded gateway OIDs in orders.json and both legs are adopted
     * here, theo/timer cycles cancel both venue legs then submit fresh limits (cancel-replace). The order gateway
     * does not accept modify for these OIDs (HTTP 404). Blank/missing OIDs: earlier gates skip; C++ does not seed.
     */
    [[nodiscard]] bool mmTryApplyDeskInPlaceModifyOnly(
        const std::string& reason,
        core::Price theo_midpoint,
        core::Price adjusted_theo,
        core::Price bid,
        core::Price ask,
        core::Quantity qty_bid,
        core::Quantity qty_ask,
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
        core::Price skew_price,
        core::Price raw_bid,
        core::Price raw_ask);

    // =========================================================================
    // Core Logic
    // =========================================================================
    
    /**
     * @brief Round price to nearest tick (inward rounding for safety)
     */
    core::Price roundToTick(core::Price price, double tick) const;
    
    /**
     * @brief Round bid down and ask up to nearest tick
     */
    core::Price roundBidDown(core::Price price, double tick) const;
    core::Price roundAskUp(core::Price price, double tick) const;
    /**
     * Snap raw bid/ask to the tick grid: bid floored, ask ceiled (with ask >= bid + 1 tick).
     * The bid_spread_bps / ask_spread_bps params are accepted for caller-side symmetry with the
     * pricing pipeline but currently unused — placement does NOT force a fixed span; the actual
     * placed_span_px = roundAskUp(raw_ask,tick) - roundBidDown(raw_bid,tick) and may differ from
     * the raw bps-derived span by up to ~1 tick.
     */
    void finalizeMmPairOnTickGrid(core::Price raw_bid, core::Price raw_ask, int bid_spread_bps,
                                  int ask_spread_bps, double quote_tick, core::Price& out_bid,
                                  core::Price& out_ask) const;
    
    /**
     * @brief Validate QO vs AX BBO: optional no-cross, optional never-inside-spread (join or behind touch).
     */
    QOValidation validateQuoteOrders(core::Price qo_bid, core::Price qo_ask, core::Price market_bid,
                                      core::Price market_ask, bool check_crossing,
                                      bool never_inside_spread) const;
    
    /**
     * @brief Main quote update logic (called on timer)
     */
    void updateQuotes();

    /**
     * On market_maker.exchange_position_reconcile_sec cadence (default ~3.5s): refresh NetPo from exchange REST.
     * If |NetPo| >= max_position, cancel all open orders on the MM symbol for the side that *adds* to inventory
     * (long at cap → BUYs; short at cap → SELLs) via order-gateway open-orders + cancel_order — catches Python/GUI
     * placements not reflected in OrderManager until poll.
     */
    void maybePeriodicExchangePositionReconcile();
    
    /**
     * @brief Per-side offsets (in basis points of pricing mid) from adjusted theo, plus inventory skew when |NetPo| < max_position.
     *
     * Pricing mid = adjusted_theo (newMidpoint + basis; basis 0 matches "TheoMidpoint" in the product formula,
     * where `newMidpoint` is the desk pricer-snapshot transform applied to the raw theo feed mid).
     * Spread inputs are PER-SIDE widths in basis points of pricing mid (1 bp = 0.0001 of mid):
     *   bid_offset_px = adjusted_theo * bid_spread_bps / 10000
     *   ask_offset_px = adjusted_theo * ask_spread_bps / 10000
     * Total spread = bid_offset_px + ask_offset_px = adjusted_theo * (bid_w + ask_w) / 10000.
     * skew_tick_units = floor(|NetPo|/adjust_position) * sign(NetPo) * adjust_ticks (symmetric);
     * skew_price = skew_tick_units * quote_tick (zero when |NetPo| >= max_position); same skew
     * subtracted from both sides.
     *   raw_bid = adjusted_theo - bid_offset_px - skew_price
     *   raw_ask = adjusted_theo + ask_offset_px - skew_price
     * Then snapped to the tick grid in finalizeMmPairOnTickGrid (bid floored, ask ceiled, independently).
     * NetPo uses net_position_qty (+ long, - short).
     */
    void computeInventorySkewedRawBidAsk(Price adjusted_theo, int bid_spread_bps, int ask_spread_bps,
                                         double quote_tick, Price& raw_bid, Price& raw_ask) const;
    void computeInventorySkewedRawBidAsk(Price adjusted_theo, int spread_bps, double quote_tick,
                                         Price& raw_bid, Price& raw_ask) const;
    
    /**
     * @brief Cancel/replace both sides at new theo (only if market_maker.requote_on_theo_move is true).
     */
    void modifyQuotesAtTheo(core::Price new_theo_mid);
    
    /**
     * @brief External feed tick — logs target vs working quote; optional theo-driven requote from config.
     */
    void onFeedUpdate(const marketdata::ExternalFeedQuote& quote);
    
    /**
     * @brief Process a fill and trigger hedge logic (placeholder)
     */
    void processFill(const OrderEventData& fill, core::Quantity incremental_fill_qty);
    
    /**
     * @brief Decide whether to hedge based on current position (PLACEHOLDER)
     */
    std::optional<HedgeIntent> decideHedge(const OrderEventData& fill);
    
    /**
     * @brief Execute hedge (PLACEHOLDER - logs intent only for now)
     */
    void executeHedge(const HedgeIntent& hedge);
    
    /**
     * @brief Log strategy decision for audit
     */
    void logStrategyDecision(const std::string& decision, const std::string& details);
    
    /**
     * @brief Track fill for delta calculation (called by both on_fill and on_partial_fill)
     */
    core::Quantity trackFill(const OrderEventData& fill, bool is_complete);
    void requoteFromTheo(core::Price theo_midpoint, const std::string& reason);
    /**
     * Canonical MM cycle: sync portfolio → cancel stale MM orders (cancel-all when |net| < max_position,
     * else only the add-side legs) → place bid/ask per shouldQuoteSide at configured order_size (per-leg
     * sizes match; cap is gating not asymmetric qty shrink). Preserves the reducing-side resting order at
     * the same formula price when past the cap (unless mm_startup / disconnect / reload halt).
     */
    void runFullMmQuoteCycle(core::Price theo_midpoint, const std::string& reason);
    std::string mmTheoSymbol() const;
    /** True if OrderManager has at least one active LIMIT bid and one active LIMIT ask for mmAxSymbol(). */
    bool mmOrderManagerHasActiveBidAndAsk() const;
    /** True if OrderManager has at least one active LIMIT on `side` for mmAxSymbol(). */
    bool mmOrderManagerHasActiveLimitOnSide(core::Side side) const;
    /** True if tracked bid_order_id_/ask_order_id_ currently points to an active LIMIT order on that side. */
    bool mmTrackedSideHasActiveLimitOrder(core::Side side) const;
    void mmCancelAllExchangeAndResetLocal(const std::string& tag);
    /** Per full MM cycle: clear cumulative fill maps and set per-leg target sizes for fill / reload logic. */
    void resetMmSessionFillTracking(core::Quantity bid_leg_qty, core::Quantity ask_leg_qty);
    /** When requote_on_timer is false: only run full cycle if a needed leg is missing or inventory pull requires it. */
    void timerReconcilePairQuotes(core::Price theo_midpoint);
    /** BUY/SELL for next MM placement: reduce side always; add side only while net is strictly inside ±max_position and reload limit is not hit. */
    bool shouldQuoteSide(core::Side side, bool bypass_reload_limit = false) const;
    /** Stdout trace for manual verification (external bid/ask, theo, skew, orders). */
    /**
     * Set position_state_ net from exchange: PortfolioManager::refresh() then getPosition (REST is source of truth).
     * @param force If true, skip the post-fill debounce that blocks REST (use so L1/L2/L3 on the same AX symbol
     *        all see identical product net after any stack’s fill).
     */
    void syncInventoryFromExchangePortfolio(bool force = false);

    /**
     * Refresh inventory from REST for every MakeMarketStrategy on `ax_symbol`. Call after any fill on the product
     * so L1–L3 share one exchange-aggregated net; otherwise sibling stacks can have stale `position_state_` and
     * disagree on shouldQuoteSide / max cap.
     * @param cycle_mutex_held If non-null, that leg already holds its `mm_cycle_mutex_` (e.g. caller inside
     *        `runFullMmQuoteCycle`); the implementation locks other same-symbol instances in a fixed order only.
     */
    static void syncExchangeNetForAllMakeMarketOnSymbol(
        const std::string& ax_symbol,
        MakeMarketStrategy* cycle_mutex_held = nullptr,
        bool enforce_cap_after = false);

    /**
     * Resting leg qty on `side` across all stacks (ADOPTED + PLACE_PENDING only).
     * Not used for Tim breach logging — see sumWorstCaseInflightLegQtyForSymbol.
     * Placement gates use net_po only (open orders excluded from the add-side check).
     */
    static core::Quantity sumRestingLegQtyForSymbol(const std::string& ax_symbol, core::Side side);

    /** @deprecated alias — same as sumRestingLegQtyForSymbol. */
    static core::Quantity sumWorkingLegQtyForSymbol(const std::string& ax_symbol, core::Side side);

    /**
     * Grow-side qty still on the book (ADOPTED + PLACE_PENDING + CANCEL_PENDING).
     * Tim acceptable-breach bound: |net| may exceed max_po by at most this total when
     * resting orders fill together. Not used for placement gates (net-only check).
     */
    static core::Quantity sumWorstCaseInflightLegQtyForSymbol(
        const std::string& ax_symbol,
        core::Side side,
        MakeMarketStrategy* exclude_own_cancel_pending_on_side = nullptr);

    /** Signed instrument net from portfolio cache (post-refresh). */
    static long long mmInstrumentNetPoFromPortfolioCache(const std::string& ax_symbol);

public:
    // =========================================================================
    // PositionBook wiring (2026-07, shadow-first, config-revertible)
    // =========================================================================
    // Mode switch: market_maker.position_book_mode = off | shadow | enforce
    // (default shadow), with optional market_maker.position_book_mode_per_symbol
    // override map. off = byte-for-byte legacy. shadow = book runs + logs only,
    // NO behavior change. enforce = local truth (legacy hot-path REST neutralized,
    // readers on effective(), snapshot cap enforcement acts).
    enum class PbMode { Off, Shadow, Enforce };

    /** Resolve mode for a symbol (per-symbol override else global). Cached. */
    static PbMode mmPbModeForSymbol(const std::string& ax_symbol);
    /** Global (non-per-symbol) mode. Cached. */
    static PbMode mmPbGlobalMode();
    /** True if global mode != off OR any per-symbol override != off. */
    static bool mmPbEnabledAnySymbol();
    /** True if global mode == enforce OR any per-symbol override == enforce. */
    static bool mmPbEnforceAnySymbol();
    /** One-shot boot banner (global line + one line per per-symbol override). */
    static void mmLogPositionBookBootBannerOnce();

    // =========================================================================
    // Enforce-mode VenueOrdersCache (2026-07): ZERO venue GETs on the mover.
    // Venue reconciliation still happens, but on the MAIN LOOP, on a cadence,
    // feeding VenueOrdersCache which the mover reads (never populates).
    // =========================================================================
    /**
     * Main-loop-only background refresher. For every AX symbol currently in
     * ENFORCE mode: one GET /open-orders, publish rows into VenueOrdersCache,
     * and run the [VENUE_COUNT_MISMATCH] regression check here (own-OID filtered
     * across all stacks on the symbol, so siblings no longer read as orphans).
     * Off/shadow symbols are never fetched — legacy behavior is byte-for-byte.
     * Call ONLY from StartupSequence::executeTradingLoop (main loop thread).
     */
    static void mmRefreshVenueOrdersCacheAllEnforced();

    /** Config: max cache age before a mover reader treats it as stale (default 30000ms). */
    static std::int64_t mmVenueCacheMaxAgeMs();

    /** This strategy's resolved mode (by its AX symbol). */
    PbMode mmPbMode() const { return mmPbModeForSymbol(mmAxSymbol()); }
    bool mmPbEnforce() const { return mmPbMode() == PbMode::Enforce; }
    bool mmPbShadowOrEnforce() const { return mmPbMode() != PbMode::Off; }

    /** Bind (lazily create) this strategy's shared per-symbol PositionBook. No-op in off. */
    void mmBindPositionBookIfNeeded() const;
    /** The bound book (may be null in off mode / before bind). */
    std::shared_ptr<PositionBook> mmPositionBook() const { return position_book_; }

    /**
     * THE position for gates. In ENFORCE (for this symbol) returns the book's
     * effective(); in off/shadow returns the legacy position_state_.net_position_qty
     * — so switching a reader to this is behavior-neutral unless enforce is on.
     */
    double mmEffectiveNetPoQty() const;
    long long mmEffectiveNetPoRounded() const;

    /** Feed a signed fill into the bound book (both fill handlers). No-op in off. */
    void mmOnFillToBook(core::Side side, core::Quantity qty) const;
    /** Seed the bound book from the current local net (called once post-init sync). */
    void mmSeedPositionBookFromLocalNet() const;

    /**
     * Host for the 30s snapshot refresh, called from StartupSequence::pollExchangePositions
     * on a refresh tick (main-loop thread) with the venue net per symbol from the SAME
     * getPositions() response (no new REST). Applies the snapshot batch, emits the
     * [POSITION_SNAPSHOT] drift log (+ legacy delta in shadow), and runs cap evaluation
     * (shadow: [POSITION_SNAPSHOT_CAP_SHADOW] log only; enforce: real enforcement).
     */
    static void mmApplyVenueSnapshotBatch(
        const std::unordered_map<std::string, double>& venue_by_symbol);

    /**
     * Enforce-only: after a snapshot, if |effective()| >= cap for `ax_symbol`, set
     * reduce-only on all stacks and enqueue a grow-side pull on that symbol's mover.
     * LOCAL only — no REST, and deliberately NOT mmConvergeProductBookOnAx.
     */
    static void mmEnforceCapFromSnapshotOnSymbol(const std::string& ax_symbol);

    /** Legacy net (llround position_state_.net_position_qty) for any stack on a symbol; 0 if none. */
    static long long mmLegacyNetRoundedForSymbol(const std::string& ax_symbol);

private:

    /** Tim spec: when |net| >= max_po on a side, cancel all grow-side legs on that product. */
    static void mmPullGrowSideOrdersAtOrPastCap(const std::string& ax_symbol,
                                                long long net_po,
                                                long long cap_ll,
                                                const char* reason,
                                                MakeMarketStrategy* cycle_mutex_held);

    /**
     * Phase-A product book converge: hold placement lease, GET /open-orders (venue truth),
     * cancel grow-side (at cap) or orphan/excess legs (below cap), then sync tracked pull.
     * Single writer during converge — stacks must not place until lease is released.
     */
    static void mmConvergeProductBookOnAx(const std::string& ax_symbol,
                                          long long net_po,
                                          long long cap_ll,
                                          const char* reason,
                                          MakeMarketStrategy* cycle_mutex_held = nullptr);

    /** Lease-gated venue diff while |net| >= cap (15s timer + every 4 replace cycles). */
    static void mmMaybePeriodicConvergeAtCap(MakeMarketStrategy* caller);
    static void mmMaybeConvergeAtCapAfterReplaceCycle(MakeMarketStrategy* caller,
                                                      const char* reason);

    /** Tim reconcile gate: wait for /fills poll after last cancel response before place. */
    void mmWaitReconcileGateBeforePlace(const char* context) const;

    /** One fill-requote placement pass per AX (processFill + cancel-ack paths). */
    static bool mmAcquireAxFillRequoteInflight(const std::string& ax_symbol);
    static void mmReleaseAxFillRequoteInflight(const std::string& ax_symbol);
    [[nodiscard]] static bool mmIsAxFillRequoteInflight(const std::string& ax_symbol);

    /**
     * At cap: cancel tracked + venue reduce-side rows until open-orders shows <=1 reduce row.
     * Returns false if venue still has excess reduce-side rows (caller must not place).
     */
    static bool mmEnsureAtCapReduceSideVenueClean(const std::string& ax_symbol,
                                                  long long net_po,
                                                  long long cap_ll,
                                                  const char* reason,
                                                  MakeMarketStrategy* cycle_mutex_held);

    /** Immediate converge when venue row count exceeds protected OID set at cap. */
    static void mmMaybeConvergeAtCapIfVenueExcess(MakeMarketStrategy* caller,
                                                 const char* reason_tag);

    /**
     * Tim fill-instant bound: |net_after| <= max + resting_qty on the filled side *before* this
     * fill (not post-pull steady state). Latches ACCEPTABLE burst on the AX for steady-state logs.
     */
    static void mmAssessTimCapAtFillInstant(const std::string& ax_symbol,
                                            long long net_po_after_fill,
                                            long long cap_ll,
                                            long long resting_grow_qty_before_fill,
                                            core::Quantity fill_qty,
                                            const char* reason);

    /**
     * After pull / post-sync: do not re-check max+resting (resting already converted to position).
     * Logs REDUCE_ONLY_OVER_CAP if a fill-time ACCEPTABLE burst is unwinding; else Joe-class UNEXPECTED.
     */
    static void mmLogTimCapSteadyStateAfterPull(const std::string& ax_symbol,
                                                long long net_po,
                                                long long cap_ll,
                                                const char* reason);

    /**
     * Multi-stack grow-side prune (net + open-order headroom). Not used on Tim placement paths
     * (2026-05-26: placement is net-only; pull on fill when |net|>=max). Retained for tests/tools.
     */
    static void mmPruneCrossStackGrowSideToCap(const std::string& ax_symbol,
                                               core::Side side,
                                               long long net_po,
                                               long long cap_ll,
                                               MakeMarketStrategy* cycle_mutex_held);

    /** After REST sync: pull grow-side legs when |net| >= max_po; log breach assessment. */
    static void mmEnforceProductCapOnSymbolAfterSync(const std::string& ax_symbol);

    /**
     * Lock every same-AX `mm_cycle_mutex_` (sorted) for the duration of `runFullMmQuoteCycle`.
     * Retains a `shared_ptr` to each locked stack (see `MmAllStacksCycleLock`) so a concurrent
     * teardown cannot free a sibling whose cycle mutex is held.
     */
    static MmAllStacksCycleLock mmAcquireAllStacksCycleLocks(
        const std::string& ax_symbol, MakeMarketStrategy* self_fallback);

    /**
     * Read the live AX exchange position from the portfolio cache and overwrite
     * `position_state_.net_position_qty/usd` so the cap gate / `shouldQuoteSide` see exchange truth
     * rather than the local fill-tally. Required because fills can arrive without an OM order_id
     * match (taker fill on submit, missed WS event), in which case `processFill` early-returns and
     * the local NetPo never advances even though the exchange position grows. Returns the signed
     * qty observed (>0 long, <0 short, 0 if no open position).
     */
    long long mmRefreshNetPositionFromPortfolioCache(const char* call_site = "");

    /** Drop tracked quote state for one side (OrderManager ids are handled separately). */
    void mmResetLocalOrderTrackingOnSide(core::Side side);

    /** AX venue BBO for non-cross check: optional market_provider_, else MarketDataManager WS book. */
    bool tryGetAxMarketBbo(const std::string& sym, core::Price& out_bid, core::Price& out_ask) const;
    /** Apply max_reload_cycles / reload_limit_reset_nonce changes; may clear counter and disengage halt. */
    void syncReloadLimitStateFromConfig();

    /**
     * Throttled INFO when MM stalls (early return from quote cycle, fill path, timer, or feed theo gate).
     * Same `gate` is logged at most once per ~8s per strategy instance; a different `gate` logs immediately.
     */
    void mmLogMmGateThrottled(const std::string& gate, const std::string& context_reason,
                            const std::string& extra_detail = {});

    /**
     * After `reloadPrimaryConfigFromDisk`: when `market_maker.mm_orders_enabled` goes false→true (e.g. account
     * cancel-all paused MM, user re-enabled via desk), run one `mm_orders_resume` cycle — feed `theo_move` does not
     * run on a flat book.
     */
    void mmNoteMmOrdersEnabledTransitionAfterConfigReload(core::Price theo_mid_hint);

    /**
     * Architect GET /markets → Market.min_price for the MM AX symbol (quote tick). Used only when
     * ``ax_gateway_instruments_catalog`` and merged leg ticks do not supply a tick. Returns 0.0
     * when nothing authoritative is available — never substitute a fake default tick.
     */
    double resolveAxQuoteTick(double config_quote_tick);

    /**
     * Final quote tick: ``resolveAxQuoteTick(mmEffPriceTick(cfg))`` (catalog + merged leg + MDM).
     * Returns <=0 when nothing authoritative is configured — callers must not quote with a fake tick.
     */
    double mmEffResolvedQuoteTick(const config::Config& cfg);

    int mmEffBidSpreadTicks(const config::Config& cfg) const;
    /**
     * Historical cap that lowered each side's spread_bps to the per-side bps implied by the
     * desk-placed bid/ask vs `adjusted_theo`. DISABLED 2026-05-19 — see implementation comment
     * for the ratchet bug (placed_*_price tracks the latest quoted prices, not the original
     * desk-seeded ladder, so `std::min` + `floor()` monotonically narrowed the spread on every
     * theo move). Retained as a no-op shim so the existing call sites do not need to change;
     * any future re-enable MUST anchor against the original (seed-time) placed prices, not the
     * live mover-overwritten ones, and use round-to-nearest semantics.
     */
    void mmCapSpreadBpsToDeskPlacedLadder(core::Price adjusted_theo, int& bid_spread_bps, int& ask_spread_bps) const;
    /**
     * Effective per-leg order size in contracts. STRICT user-input source-of-truth:
     * returns the desk stack's ``order_size`` (what the user typed into the GUI
     * and was persisted into ``orders.json``) when this strategy is desk-managed
     * (`mm_req_<ax>_<stack_id>`); returns 0 otherwise.
     *
     * No fallback to ``market_maker.order_size`` / ``market_maker.quantity``: those
     * are runtime config defaults bound to no particular user intent, and using
     * them as a fallback in past versions caused C++ to ``move`` orders at the
     * wrong size (e.g. quote 100 when the user submitted 10). When this returns 0,
     * the cycle gate in ``runFullMmQuoteCycle`` (``qty_int <= 0``) refuses to
     * place or move orders.
     */
    int mmEffOrderSizeInt(const config::Config& cfg) const;
    /**
     * Per-stack `order_size_step`, else `mm_instrument_order_size_step_` (from instruments[]),
     * else desk-managed → 1, else global `market_maker.order_size_step`.
     * Step is capped by effective `order_size` so step-rounding never floors leg qty to zero.
     */
    int mmEffOrderSizeStepInt(const config::Config& cfg) const;
    int mmEffMaxPositionInt(const config::Config& cfg) const;
    /**
     * Recompute reduce-only state from the freshly-synced NetPo and the effective cap, log
     * transitions ENTERED/EXITED exactly once per edge, and update the atomic flags read by
     * `mmIsReduceOnly()` / `mmReduceOnlySide()`. Call right after
     * `syncInventoryFromExchangePortfolio()` so the state reflects the exchange truth.
     *
     * @param net_po_rounded   Signed product net position (long long-rounded to integer units).
     * @param cap_ll           Effective max_position cap (>= 1, already lower-bounded).
     */
    void noteReduceOnlyState(long long net_po_rounded, long long cap_ll);

    /**
     * Effective per-leg quote tick (no invented defaults). Resolution order:
     *   1) `mm_instrument_tick_` when > 0
     *   2) merged ``market_maker.instruments[]`` tick for this AX symbol
     *   3) ``ax_gateway_instruments_catalog`` (GET /instruments at startup)
     *   4) ``market_maker.price_tick`` (global; default 0 until merge or explicit config)
     */
    double mmEffPriceTick(const config::Config& cfg) const;

    /**
     * Apply per-leg theo transformation to a raw mid: legacy `theo_scale` multiplier (if > 0)
     * when no pricer-snapshot anchors are configured, otherwise pass-through (the snapshot
     * formula carries the scale change). Returns 0.0 if the input is non-finite or non-positive.
     *
     * invert_theo was REMOVED (2026-05-12) — there is no per-leg `1/raw_mid` step anywhere on
     * this path. Cross-scale legs (e.g. JPYUSD-PERP off Neon USD/JPY) must use the
     * pricer-snapshot transform (`quote_snapshot`, `pricer_snapshot`, `slope`).
     */
    core::Price mmTransformTheo(core::Price raw_mid) const;

    /**
     * Fetch the raw mid from `theo_provider_(mmTheoSymbol())` and apply `mmTransformTheo`.
     * Returns `std::nullopt` when the provider returns nothing OR when the transformed value is
     * non-positive / non-finite. Every quote-cycle call site reads through this helper so the
     * `theo_scale` knob is honored uniformly without each site having to remember to apply it.
     */
    std::optional<core::Price> mmReadTransformedTheo() const;
    int mmEffAdjustPositionInt(const config::Config& cfg) const;
    int mmEffAdjustTicksInt(const config::Config& cfg) const;
    int mmEffMinTheoDriftTicksInt(const config::Config& cfg) const;
    /** Product-level override (when set) else global market_maker.max_reload_cycles. */
    int mmEffMaxReloadCyclesInt(const config::Config& cfg) const;

    /**
     * MWR (Moving Window Range) volatility-breaker params resolved for this strategy's AX.
     * Resolution is leg (market_maker.instruments[]) → optional global (market_maker.mwr_*).
     * Params are per-leg by design (see MarketMakerInstrumentLeg); there is no per-stack tier.
     * MWR is enabled only when mmEffMwrSizeTicks()>0 AND mmEffMwrWindowSec()>0.
     */
    int mmEffMwrSizeTicks(const config::Config& cfg) const;
    int mmEffMwrWindowSec(const config::Config& cfg) const;
    int mmEffMwrPullSec(const config::Config& cfg) const;

    /**
     * MWR pause predicate — cheap deadline compare(s). Returns true while EITHER breaker is paused:
     *   (1) this leg's per-instrument MWR (mover-only mm_mwr_ deadline), OR
     *   (2) the GLOBAL fast-market breaker (HL SPX) — a single shared atomic deadline.
     * Either pausing suppresses placement (pure suppression). It does NOT evaluate(), push a
     * sample, parse config, or call any mmEffMwr* getter — just deadline reads, no allocation.
     * Used by the runFullMmQuoteCycle gate (global half) and both Item G placement gates
     * (fill-requote, after-cancel-ack re-peg). When both breakers are disabled, neither deadline
     * is set, so this returns false (no suppression).
     */
    bool mmMwrIsPausedNow() const {
        if (std::chrono::steady_clock::now() < mm_mwr_.pausedUntil()) {
            return true;  // per-instrument breaker
        }
        return FastMarketMonitor::isGloballyPausedNow();  // global fast-market breaker
    }

    /**
     * Per-stack pricer-snapshot transform anchors (see MarketMakerManualStack docstring for the
     * formula). Read from `mm_manual_stack_` so each `mm_req_*` quotes off its own desk-input
     * snapshots; legacy strategies (no manual_stack) get the disabled defaults (snapshots=0,
     * slope=1) so the transform is a no-op and behavior is unchanged.
     */
    double mmEffQuoteSnapshot() const;
    double mmEffPricerSnapshot() const;
    double mmEffSlope() const;
    /**
     * Apply the desk's pricer-snapshot linear transform to a raw theo midpoint:
     *   newMidpoint = quote_snapshot
     *               + quote_snapshot * slope * ((theo - pricer_snapshot) / pricer_snapshot)
     * Returns the input `theo` unchanged when the transform is disabled (pricer_snapshot<=0
     * OR quote_snapshot<=0 OR transformed value is non-finite/non-positive — failsafe so a
     * misconfigured snapshot can't push the strategy into negative or NaN price territory).
     * Every site that derives `adjusted_theo` from a raw midpoint routes through this so the
     * formula lives in exactly one place.
     */
    core::Price mmApplyPricerSnapshotTransform(core::Price theo) const;

    void printMmVerifyStdout(const std::string& reason,
                             core::Price theo_midpoint,
                             core::Price adjusted_theo,
                             int bid_width_bps,
                             int ask_width_bps,
                             double quote_tick,
                             long long net_po_rounded,
                             int max_po,
                             int adjust_po,
                             int adjust_ticks,
                             double skew_tick_units,
                             core::Price skew_price,
                             core::Price raw_bid,
                             core::Price raw_ask,
                             core::Price bid_px,
                             core::Price ask_px,
                             core::Quantity qty_bid,
                             core::Quantity qty_ask,
                             bool want_bid,
                             bool want_ask,
                             bool placing_bid,
                             bool placing_ask) const;

    // =========================================================================
    // State
    // =========================================================================
    
    TheoProvider theo_provider_;
    MarketQuoteProvider market_provider_;

    std::string mm_symbol_override_;
    std::string mm_order_symbol_override_;
    std::string mm_theo_symbol_override_;
    /** Non-empty: one `manual_stacks` row; overrides global market_maker scalars in quote / reload math.
     *  CONCURRENCY: guarded by mm_desk_cfg_mu_ (desk-bg writer vs mover/feed/OM/timer readers).
     *  Read cross-thread ONLY via mmOptionalManualStack() (returns a locked copy). Direct member
     *  access is permitted solely on the desk-bg thread that owns the writes. */
    std::optional<architect::config::MarketMakerManualStack> mm_manual_stack_;
    // Shared (readers-writer) lock for the desk-config fields written by the desk background
    // thread and read on the mover/feed/OM/timer threads: mm_manual_stack_ + mm_order_request_id_.
    mutable std::shared_mutex mm_desk_cfg_mu_;
    // Lock-free mirror of "mm_order_request_id_ is non-empty" for the very hot mmIsDeskManaged()
    // check; kept in sync inside setMmOrderRequestId().
    std::atomic<bool> mm_is_desk_managed_{false};

    /**
     * Leg-level (product-level) `max_position` cap — the only per-product override in `mmEffMaxPositionInt`;
     * `manual_stacks[].max_position` is not used for gating. Must be set (or rely on global) so L1–L3 share one cap.
     */
    int mm_instrument_max_position_{0};
    /** Source tag for `mm_instrument_max_position_`: "instrument" or "override" (empty = global fallback). */
    std::string mm_max_position_source_;
    /** Leg-level (product-level) max_reload_cycles shared by all stacks on one AX symbol. 0 = global fallback. */
    int mm_instrument_max_reload_cycles_{0};
    /** Source tag for `mm_instrument_max_reload_cycles_`: "instrument" or "override" (empty = global fallback). */
    std::string mm_max_reload_source_;
    /** One-shot per initialize(): emit first-cycle cap/side decision once for quick boot diagnostics. */
    bool first_quote_logged_{false};

    /** Per-leg tick (price units of `mmAxSymbol()`). 0 = fall through to `market_maker.price_tick`. */
    double mm_instrument_tick_{0.0};
    /** Per-leg contract step from instruments[]. 0 = not set (use stack / global / desk rules). */
    int mm_instrument_order_size_step_{0};
    /** Legacy multiplicative scale applied to raw theo (e.g. SP500 → SPY = 0.1). 0 = no scaling. */
    double mm_theo_scale_{0.0};

    // Active order tracking
    //
    // CONCURRENCY (2026-05-20 audit, fix C3): bid_order_id_ / ask_order_id_ are
    // touched by feed worker (onFeedUpdate logging), MmOrderMover worker (cycle
    // bookkeeping), and OrderManager event-dispatch thread (on_accept/on_reject).
    // Previously plain OrderId; reads/writes were unsynchronized. Made
    // std::atomic<OrderId> with acquire/release pairing so a reader who sees
    // a freshly-published id is guaranteed to also see the tracked-leg row
    // that was inserted under `tracked_orders_mutex_` immediately before.
    std::atomic<OrderId> bid_order_id_{0};
    std::atomic<OrderId> ask_order_id_{0};
    // last_*_price_ / last_theo_ are scalar mirrors used for logging and gating.
    // Read on feed/mover/OM threads, written on mover. atomic<double> keeps
    // single-store/load tear-free (C++20).
    std::atomic<core::Price> last_bid_price_{0.0};
    std::atomic<core::Price> last_ask_price_{0.0};
    std::atomic<core::Price> last_theo_{0.0};
    // CONCURRENCY: read/written by feed, OM dispatch (on_accept) and mover threads.
    // Atomic so those accesses never tear or race (relaxed is fine — it gates behaviour,
    // not memory ordering of other state).
    std::atomic<bool> passive_until_first_manual_order_{true};
    mutable std::mutex tracked_orders_mutex_;
    std::vector<TrackedLeg> tracked_bids_;
    std::vector<TrackedLeg> tracked_asks_;

    /**
     * CONCURRENCY (2026-05-20 audit, fix C5): serializes onFeedUpdate on this
     * strategy. ExternalFeedManager runs multiple feed worker threads
     * (poll_thread_, aux_poll_thread_, mettraders_thread_); when a strategy
     * is bound to a primary + auxiliary theo source (multi-theo configuration),
     * publishQuote from two threads can deliver into THIS strategy's
     * onFeedUpdate concurrently. Without this lock the body would mutate
     * `last_theo_`, enqueue overlapping cycles, and corrupt the desk's
     * "feed -> theo -> requote" state machine. Released automatically on
     * function exit / exception via std::lock_guard.
     */
    mutable std::mutex mm_feed_callback_mutex_;

    std::chrono::steady_clock::time_point mm_ax_quote_tick_next_refresh_{};
    double cached_ax_quote_tick_{0.0};
    /** Last theo mid at which we ran a successful `runFullMmQuoteCycle` (any reason) — baseline for min-theo-move gating. */
    core::Price last_theo_requote_anchor_{0.0};
    bool last_theo_requote_anchor_inited_{false};

    /** Suppress repeated identical [MM_FEED] lines when the external feed sends unchanged TOB. */
    core::Price last_mm_feed_stdout_bid_{0.0};
    core::Price last_mm_feed_stdout_ask_{0.0};
    bool mm_feed_stdout_initialized_{false};

    /** Last target bid/ask shown in FEED PRICE CHANGE banner (suppress repeat banners when only mid jitters). */
    bool feed_banner_targets_inited_{false};
    core::Price last_feed_banner_target_bid_{0.0};
    core::Price last_feed_banner_target_ask_{0.0};
    std::optional<std::chrono::steady_clock::time_point> last_feed_banner_emit_at_;
    
    // Filled quantity tracking for delta calculation
    // Track fills across ALL orders (including cancelled ones that still receive fills)
    // Protected by fill_mutex_ for thread safety
    mutable std::mutex fill_mutex_;
    std::unordered_map<OrderId, core::Quantity> bid_order_fills_;  // order_id -> MAX cumulative filled seen
    std::unordered_map<OrderId, core::Quantity> ask_order_fills_;  // order_id -> MAX cumulative filled seen
    core::Quantity total_bid_filled_{0};    // Sum of all bid fills (computed from bid_order_fills_)
    core::Quantity total_ask_filled_{0};    // Sum of all ask fills (computed from ask_order_fills_)
    core::Quantity original_bid_qty_{0};    // Last-placed bid leg size (inventory-capped)
    core::Quantity original_ask_qty_{0};    // Last-placed ask leg size (inventory-capped)
    
    // Position tracking (all in notional USD)
    //
    // CONCURRENCY (2026-05-20 audit, fix C4): `position_state_` is mutated by
    // processFill on the OrderManager event-dispatch thread, by
    // syncInventoryFromExchangePortfolio on the MmOrderMover worker thread,
    // and by syncExchangeNetForAllMakeMarketOnSymbol on either thread (it
    // also writes to PEER strategies' position_state_ — see that function).
    // It is read by shouldQuoteSide / cap-projection on the mover thread.
    //
    // Without sync the cap-gate could observe a half-updated net position
    // (matters most for max_position; this is precisely what allowed Joe's
    // 2026-05-19 EURUSD 7×100-lot overshoot to be possible in theory). We
    // do NOT make net_position_qty atomic<double> because PositionState is
    // a value-semantic struct (exposed by const ref via getPositionState).
    // Instead we serialize fill writes and cap-gate reads through this
    // mutex; logging/diagnostic reads remain unsynchronized (worst case is
    // a stale display value, never a wrong order).
    mutable std::mutex position_state_mutex_;
    PositionState position_state_;

    /**
     * Shared per-AX-symbol PositionBook (single source of truth for net position
     * in enforce mode; shadow accuracy witness otherwise). Bound lazily via
     * mmBindPositionBookIfNeeded(); null in off mode. Sibling stacks on the same
     * symbol share ONE book (registry keyed by AX symbol). See PositionBook.h.
     */
    mutable std::shared_ptr<PositionBook> position_book_;

    /** Suppress duplicate inventory REST INFO / [MM_SYNC] lines when qty and ref price are unchanged. */
    double inventory_sync_last_logged_net_{std::numeric_limits<double>::quiet_NaN()};
    double inventory_sync_last_logged_ref_{std::numeric_limits<double>::quiet_NaN()};
    
    // Hedge configuration
    std::string hedge_symbol_;               // From hedge.symbol (empty = no hedge intents)
    double hedge_multiplier_{1.0};           // From hedge.multiplier
    double hedge_threshold_ratio_{0.5};      // Hedge when |net_pos| > ratio * hedge_notional
    bool auto_hedge_enabled_{false};
    core::Price last_hedge_price_{0.0};      // Last known hedge product price
    
    // Hedge tracking
    std::vector<HedgeIntent> pending_hedges_;
    std::vector<HedgeTrade> executed_hedges_;
    
    // Statistics
    std::uint64_t quotes_sent_{0};
    std::uint64_t quotes_cancelled_{0};
    [[maybe_unused]] std::uint64_t quotes_modified_{0};
    std::uint64_t validation_failures_{0};
    std::uint64_t hedges_triggered_{0};
    
    // Order latency tracking (client_order_id -> submit timestamp).
    // CONCURRENCY: written by the mover worker thread (at submit) and read/erased by the
    // OrderManager dispatch thread (in on_accept). Concurrent unordered_map mutation from
    // two threads corrupts the table (rehash), so every access takes order_submit_times_mutex_.
    mutable std::mutex order_submit_times_mutex_;
    std::unordered_map<std::string, std::chrono::steady_clock::time_point> order_submit_times_;
    
    // Feed callback registration
    int feed_callback_id_{-1};

    /** One-shot widen on the side that completed a fill (consumed in requoteFromTheo). */
    int fill_extra_bid_ticks_{0};
    int fill_extra_ask_ticks_{0};

    /**
     * Avoid timer reconcile while HTTP accepts for the last cycle are still in flight (local ids not yet set).
     *
     * CONCURRENCY (2026-05-20 audit, fix C2): atomic<int> because this counter is incremented
     * from the MmOrderMover worker thread (cycle place paths) and decremented from the
     * OrderManager event-dispatch thread (on_accept / on_reject). The 2026-05-19 incidents
     * (phantom 1-lot reload after silver flatten; "pending_accepts==0" gate firing while an
     * accept was in flight) trace to torn reads on this field. All read/write ops use the
     * atomic operators; format-string and std::to_string call sites explicitly `.load()`.
     */
    std::atomic<int> mm_pending_accepts_{0};
    /** Steady-clock ms since epoch when `mm_pending_accepts_` last changed (for stale awaiting_accept auto-clear). */
    std::atomic<std::int64_t> mm_pending_accepts_last_change_ms_{0};

    /** Python desk placed quotes; suppress timer/theo full cycles until fill_requote clears this. */
    // CONCURRENCY: toggled by feed / mover / cancel paths and read widely; atomic.
    std::atomic<bool> mm_desk_active_{false};
    /** F4: max_position / max_reload blocked fresh pair — cleared on next desk re-adopt for this stack. */
    bool mm_desk_stack_paused_{false};

    /** Last `market_maker.mm_orders_enabled` after config reload — detect true resume after account cancel pause. */
    bool mm_last_mm_orders_enabled_from_config_{true};
    bool mm_mm_orders_gate_config_seen_{false};

    /** Last time exchange position + open-order reconcile ran (steady clock). */
    std::chrono::steady_clock::time_point mm_last_position_reconcile_tp_{};
    bool mm_last_position_reconcile_valid_{false};

    /** Steady-clock ms since epoch at last processFill onAxFill; 0 = no fill yet. Used to skip REST overwrite briefly. */
    std::atomic<std::int64_t> last_fill_time_ms_{0};

    /**
     * Count of fills synthesized by the desk-managed venue-truth-missing path
     * (`mmRouteOmGatewayVenueTruthMissing` → `processFillDeskStack`). These
     * fills bypass `OrderManager::onOrderFilled` so they are NOT counted in
     * `OrderManager::Stats::total_fills` / the HEARTBEAT `fills=` field.
     * Surfaced separately so "fills=0 for 7 minutes" in HEARTBEAT cannot be
     * trusted as "no fills happened" — see the SILVER 2026-05-21 incident
     * where 2 venue fills occurred but `fills=0` throughout the log.
     */
    std::atomic<std::uint64_t> mm_desk_synth_fills_count_{0};

    /**
     * Steady-clock millis of the most recent cancel response from the venue
     * (whether 2xx success or benign-404). Read by the cycle-time
     * reconciliation gate to enforce Tim's rule (2026-05-21): after we
     * cancel an order, do not place a fresh order on this stack until at
     * least one full fill-poll has *completed* (i.e. its `last_poll_end_ms`
     * is strictly greater than this stamp). If the venue dropped the cancel
     * AND posted a fill for the same order in the same window, that fill
     * must be visible in the next poll before we are allowed to re-submit.
     * Without this gate, the strategy would speculatively re-quote on a
     * stale local NetPo and breach max_position. Zero = no cancel ever.
     */
    std::atomic<std::int64_t> last_cancel_response_ms_{0};

    /**
     * Steady-clock ms of the most recent CANCEL-FIRST cancel ack
     * (processTrackedOrdersTheoMove → on_cancel desk_cancel_ack path).
     *
     * Distinct from `last_cancel_response_ms_`, which is stamped ONLY by the
     * synchronous runFull "cancel tracked leg" REST result. The cancel-first
     * path acks through on_cancel and never touches `last_cancel_response_ms_`,
     * so the CANCEL-FIRST RE-ADD WINDOW GUARD in applyMmDeskSeededJsonLocked
     * could not see the naked window opened by a cancel-first pair-cancel and
     * a DESK_RECOVERY/runFull reconcile landing in it would re-adopt the
     * just-killed orders.json oids (404 storm + venue orphans). This atomic
     * gives the guard a signal for the cancel-first window without altering
     * the reconcile-gate semantics that key off `last_cancel_response_ms_`.
     * Zero = no cancel-first cancel has ever completed.
     */
    std::atomic<std::int64_t> mm_last_cancel_first_ms_{0};

    /**
     * Cross-check the strategy's local NetPo against the venue's REST
     * positions truth right before placement. Returns `true` if the
     * caller may proceed to place orders, `false` if the cycle must
     * abort (mismatch detected or refresh failed).
     *
     * Added 2026-05-22 after the SILVER overtrade where the REST /fills
     * poller returned 0 fills for ~30s while the venue position grew to
     * +6 against a `max_po=2` cap. Tim's "fills-only" model breaks down
     * exactly when /fills is unreliable; layering a synchronous
     * /positions read on top closes that gap. Side effect: when local
     * disagrees with venue we adopt the more-pessimistic (larger
     * magnitude) value into `position_state_.net_position_qty` so the
     * next cycle starts from the truer NetPo.
     */
    bool mmReconcileVenueOrAbort(const char* reason);

    /** Serialize full cancel/replace cycles — timer + fill handlers can run on different event workers. Recursive so feed/timer paths can run transition cancels then re-enter runFullMmQuoteCycle on the same thread. */
    mutable std::recursive_mutex mm_cycle_mutex_;

    /**
     * Maps client_order_id → runFullMmQuoteCycle cycle reason for reload-count policy.
     * @see on_accept — only some reasons bump market_maker.current_reload_count (fill-driven, not theo/timer).
     *
     * Cancel only this stack's tracked LIMIT on `side` (gateway oid + OM). Returns 0 or 1.
     * Caller must hold this strategy's `mm_cycle_mutex_` when used from `runFullMmQuoteCycle` / fill paths.
     */
    /** @return 1 if this stack/side is clear for a new submit (nothing to cancel, terminal local row, gateway ok, or benign 404); 0 on hard cancel failure. */
    int mmCancelTrackedLegThisStackGateway(core::Side side);
    /**
     * Reject-reason classifier for the paired-pull behaviour: when one leg is
     * rejected for one of these reasons we also pull the opposite (paired) leg,
     * because the desk is obligated to show both sides together — a one-sided
     * quote is not acceptable.
     *
     * Currently triggers on margin breaches (ErrorCode::INSUFFICIENT_MARGIN, or
     * a "margin" substring when the venue only puts the reason in free text).
     * Deliberately written as a single catch-all extension point: add further
     * reject reasons here and the paired-pull in on_reject applies uniformly.
     */
    static bool mmRejectReasonShouldPullPairedLeg(const events::OrderEventData& reject);
    /** Venue-cancel both tracked legs and clear local bid/ask ids (never leave a one-legged book after a fill when we cannot re-quote). */
    void mmPullBothTrackedLegsAfterFillStall(const std::string& tag);
    /**
     * Product-level: cancel the tracked LIMIT on `side` for every MM strategy on `ax_symbol` (at most one per stack).
     * Locks peer `mm_cycle_mutex_` in address order; `cycle_mutex_held` already holds its mutex (if non-null).
     */
    static int mmCancelTrackedLegAllStacksOnAx(const std::string& ax_symbol, core::Side side,
                                             MakeMarketStrategy* cycle_mutex_held);

    /** Product converge: drop `oid` from every stack's tracked vectors and legacy ids. */
    static void mmEvictExchangeOidAcrossProductStacks(const std::string& ax_symbol,
                                                      const std::string& oid);

    /** Venue cancel + local eviction for lease-gated product converge. */
    static bool mmVenueCancelOidForProductConverge(const std::string& ax_symbol,
                                                   const std::string& oid,
                                                   const char* reason_tag);

    /** Best exchange oid for a side on this desk stack (tracked top-of-book or orders.json seed). */
    [[nodiscard]] std::string mmDeskStackExchangeOidForSide(core::Side side) const;

    /** True if this desk stack tracks `oid` on bid or ask (tracked vectors or orders.json seed). */
    [[nodiscard]] bool mmStackTracksExchangeOid(const std::string& oid) const;

    /** AX symbol of the desk stack that tracks `oid`, or empty if none. */
    static std::string mmDeskOwnerAxForExchangeOid(const std::string& oid);

    /** Working price the stack is targeting on `side` (last placed / tracked leg). */
    [[nodiscard]] double mmDeskStackDesiredPriceForSide(core::Side side) const;

    static std::unordered_set<std::string> mmBuildProtectOidsWithTieBreak(
        const std::vector<MakeMarketStrategy*>& desk_mms,
        const std::vector<MmVenueOpenRow>& venue_rows,
        bool at_cap,
        core::Side reduce_side);

    void registerMmOrderForReloadCountBump(const std::string& client_order_id, const std::string& mm_cycle_reason);
    std::unordered_map<std::string, std::string> mm_pending_accept_cycle_reason_;
    mutable std::mutex mm_accept_pending_mutex_;

    /** Last-seen market_maker.mm_orders_enabled for transition cancel (→ false ⇒ cancel-all once). */
    bool mm_prev_mm_orders_enabled_{true};

    /** Circuit breaker: after max_reload_cycles counted events, stop placing MM orders (session). */
    int mm_reload_cycles_used_{0};
    /** After limit hit: cancel-all once, then skip placement until config reset (no repeated cancel-all). */
    bool mm_reload_limit_engaged_{false};
    int mm_reload_limit_max_cfg_seen_{-1};
    std::string mm_reload_limit_nonce_seen_;
    bool mm_reload_limit_nonce_initialized_{false};

    /** Resolved theo source for this leg ("mettraders"|"hyperliquid"|"neon_fix"); empty = legacy ungated. */
    std::string mm_theo_source_;
    /** True when the bound feed is currently down — surfaces to the desk's per-leg blocked indicator. */
    std::atomic<bool> mm_blocked_by_feed_{false};
    /** Throttle for "leg blocked: feed down" log lines (steady_clock ms). */
    std::atomic<std::int64_t> mm_last_blocked_log_ms_{0};

    /**
     * Reduce-only state — see `mmIsReduceOnly()` docstring. Updated each MM cycle by
     * `noteReduceOnlyState(...)` from the synced exchange NetPo and `mmEffMaxPositionInt`.
     * - mm_reduce_only_active_ : true when |NetPo| >= max_position
     * - mm_reduce_only_side_   : 0=None, 1=BuyOnly (was short ≥ cap), 2=SellOnly (was long ≥ cap)
     * Both are atomic so the desk-side snapshot writer can read them without a lock.
     */
    std::atomic<bool> mm_reduce_only_active_{false};
    std::atomic<int>  mm_reduce_only_side_{0};

    // =========================================================================
    // MWR (Moving Window Range) volatility breaker — MOVER-THREAD-ONLY STATE.
    // =========================================================================
    // Every member below is plain (no mutex, no atomic) and is touched ONLY from the
    // MmOrderMover worker thread: sampling in mmDrainPendingQuoteCycle and the gate in
    // runFullMmQuoteCycle. Do NOT read/write these from onFeedUpdate or any feed thread.
    //
    // mm_mwr_ holds the rolling sample window + pause state (see MwrBreaker).
    // mm_mwr_cfg_* cache the resolved per-leg params (written by the gate each cycle) so the
    // 1 Hz sampler can decide enabled/window without re-parsing config on every feed tick —
    // when disabled this keeps sampling a true zero-cost no-op.
    MwrBreaker mm_mwr_;
    int  mm_mwr_cfg_size_ticks_{0};
    int  mm_mwr_cfg_window_sec_{0};
    bool mm_mwr_have_last_sample_{false};
    std::chrono::steady_clock::time_point mm_mwr_last_sample_ts_{};
    bool mm_mwr_tick_warn_logged_{false};
    // OPTIONAL read-only mirror of paused_until for the web desk ONLY (write-on-change).
    // The decision logic must NEVER read this; it reads the plain MwrBreaker state.
    std::atomic<std::int64_t> mm_mwr_paused_until_ms_snapshot_{0};

    /** Durable handle for mm_orders.json. Empty = base/leg strategy not driven by the request file. */
    std::string mm_order_request_id_;

    /** Flips true on the first observed place ack (any side); never resets. Drives mm_orders.json staleness. */
    std::atomic<bool> mm_first_accept_seen_{false};
    /** Steady-clock ms at the first place ack (for mm_orders.json freshness display). */
    std::atomic<std::int64_t> mm_first_accept_ms_{0};

    /** Cumulative fill events (partial + complete). Bumped in on_fill/on_partial_fill. */
    std::atomic<std::uint64_t> mm_fills_count_{0};
    /** AX order id from the most recent fill, stored as integer; 0 = none yet. */
    std::atomic<OrderId> mm_last_fill_order_id_{0};
    /** System-clock ms at the most recent fill. */
    std::atomic<std::int64_t> mm_last_fill_ms_{0};

    /** Throttle for `ensureStartupQuotePair` "no theo yet" warnings (~10s per leg). */
    std::atomic<std::int64_t> startup_enforce_no_theo_log_ms_{0};
    /** Throttle for per-reason `ensureStartupQuotePair` early-return diagnostics (~10s per leg). */
    std::atomic<std::int64_t> startup_enforce_skip_log_ms_{0};

    /** Desk: throttle `tryAdoptGatewayPlacedFromManualStack` when OM/venue drift (~2.5s per stack). */
    std::atomic<std::int64_t> mm_last_desk_adopt_attempt_ms_{0};

    /**
     * After orders.json gateway-seed adopt (!require_desk_sync), steady_clock ms since epoch when adopt
     * completed. While within `market_maker.desk_gateway_seed_repricing_quiet_ms`, periodic repricing
     * (theo_move / timer_update / timer_reconcile) is skipped so C++ does not immediately cancel-replace
     * operator-placed limits on process start. Cleared on fill_requote* or explicit desk sync signal.
     * 0 = no active quiet window.
     */
    std::int64_t mm_desk_gateway_seed_steady_ms_{0};

    /** Serialize throttled MM_GATE diagnostics (feed vs runFull may share the process). */
    mutable std::mutex mm_mm_gate_log_mutex_;
    std::int64_t mm_mm_gate_last_log_ms_{0};
    std::string mm_mm_gate_last_tag_;

    // ==========================================================================
    // MmOrderMover integration (single-thread GLOBAL executor — v2 2026-05-07)
    // ==========================================================================
    //
    // All cancel/replace/place REST work across every strategy and every AX symbol
    // is serialized through ONE global worker thread owned by MmOrderMover. The
    // feed-dispatch thread / timer thread / OM-event thread compute targets locally
    // and call `enqueueQuoteCycleOnMover` (or one of the more specific helpers below).
    // Each helper sets a per-strategy pending flag + reason and posts a lambda to the
    // mover; when the mover thread reaches it, the lambda calls back into
    // `mmDrain<XYZ>OnMover` which reads the latest theo, reconciles tracked rows
    // against the venue's `/open-orders`, then runs the synchronous cycle.
    //
    // Different AX symbols do NOT progress in parallel any more (v1 had per-AX
    // workers; user requirement on 2026-05-07 was "remove multithreading from
    // moving orders"). Cancels/places for EURUSD-PERP and JPYUSD-PERP run in
    // strict posting order on the same thread. Latency is acceptable; correctness
    // is mandatory.
    //
    // Coalescing: each pending flag uses compare-exchange so only the first
    // unsatisfied caller posts a lambda. Subsequent callers piggy-back on the
    // already-queued action and the lambda re-reads the latest theo when it runs.

    std::atomic<bool> mm_quote_cycle_pending_{false};
    mutable std::mutex mm_quote_cycle_pending_reason_mu_;
    std::string mm_quote_cycle_pending_reason_;

    std::atomic<bool> mm_fill_requote_pending_{false};

    std::atomic<bool> mm_pair_enforce_pending_{false};
    mutable std::mutex mm_pair_enforce_pending_tag_mu_;
    std::string mm_pair_enforce_pending_tag_;

    /**
     * True while `runFullMmQuoteCycle` is actively executing on the mover thread.
     * The pair-enforce timer thread MUST NOT trigger DESK_RECOVERY while this is set:
     * a normal cancel-replace transiently produces `bid_order_id_=0 && ask_order_id_=0`,
     * and racing the pair-enforce path against the in-flight place caused (a) phantom
     * extra bid/ask placements after a fill ("3 offers, 4 bids" symptom Joe reported),
     * and (b) the 2026-05-20 segfault that left venue orders unmanaged (DESK_RECOVERY
     * cleared `tracked_bids_/tracked_asks_` from the bg thread while the mover thread
     * was mid-place, corrupting state and crashing the process).
     */
    std::atomic<bool> mm_cycle_running_{false};

    /**
     * Steady-clock ms timestamp of the most recent cancel/replace/place activity inside
     * `runFullMmQuoteCycle` (or its helpers). DESK_RECOVERY requires this to be at least
     * `mm_desk_recovery_quiet_ms_` old before it is allowed to fire, so a momentary
     * `bid_order_id_=0 && ask_order_id_=0` window (cancel-acked but place-not-yet-fired)
     * cannot be misinterpreted as "stack is stuck".
     */
    std::atomic<std::int64_t> mm_last_cycle_activity_ms_{0};

    // === CANCEL-FIRST theo-move latency instrumentation (MM_TT, 2026-07-03) =====
    // Monotonic (steady-clock ms) timestamps so Tim can measure tick-to-cancel and
    // tick-to-place across the cancel-first requote (cancels leave on the feed drain;
    // the fresh pair re-adds after the cancel-ack). LOG-ONLY: these never gate any
    // decision. `mm_tt_feed_arrival_ms_` is stamped when a desk theo_move requote is
    // triggered; `mm_tt_cancel_sent_ms_` when processTrackedOrdersTheoMove enqueues the
    // cancels. Both are cleared when the fresh pair is placed OR the re-add aborts
    // after the cancel (MM_CYCLE_ABORT_AFTER_CANCEL).
    std::atomic<std::int64_t> mm_tt_feed_arrival_ms_{0};
    std::atomic<std::int64_t> mm_tt_cancel_sent_ms_{0};

    /** Exponential backoff after DESK_RECOVERY fails product_placement_lease_denied. */
    std::atomic<std::int64_t> mm_desk_recovery_next_attempt_ms_{0};
    std::atomic<int> mm_desk_recovery_lease_denied_streak_{0};

    // Throttle caches for per-drain REST hits — without these, every
    // `mmDrainPendingQuoteCycle` does GET /open-orders + portfolio refresh + portfolio cache
    // refresh = ~700ms of pure REST before even starting the cancel/replace. With min_drift=1
    // and a typical 2-cancel + 2-place sequence, that pre-amble is the single biggest
    // contributor to perceived "slow move" latency in user-facing logs (2.7s observed).
    // Atomics so the feed thread can read them cheaply for "should I bother enqueuing" gates,
    // though today only the mover thread uses them.
    std::atomic<std::int64_t> mm_venue_reconcile_last_ms_{0};
    std::atomic<std::int64_t> mm_inventory_sync_last_ms_{0};
    /** Enforce-mode VENUE_CACHE_STALE log throttle (steady ms of last emission). */
    std::atomic<std::int64_t> mm_venue_cache_stale_last_ms_{0};

    /** Per-strategy guard so reentrant timer/feed paths don't post a lambda while one is running. */
    std::atomic<bool> mm_mover_in_action_{false};

    // ==========================================================================
    // Venue source-of-truth guard (2026-06-11)
    // ==========================================================================
    /** Stack lifecycle state machine — single source of truth for worker bail-out. */
    std::atomic<MmStackState> mm_stack_state_{MmStackState::ACTIVE};

    /**
     * Last-known venue order state, cached from the VENUE_TRUTH poll
     * (`mmReconcileTrackedAgainstVenue`). `mm_venue_truth_oids_` is the set of
     * exchange OIDs the venue reported open for this stack's symbol on the most
     * recent successful poll; `mm_venue_truth_updated_ms_` is the steady_clock ms
     * of that update. Used by the pre-outbound check to skip cancels for OIDs the
     * venue no longer knows about. Guarded by `mm_venue_truth_mu_`.
     */
    mutable std::mutex mm_venue_truth_mu_;
    std::unordered_set<std::string> mm_venue_truth_oids_;
    std::int64_t mm_venue_truth_updated_ms_{0};

    /**
     * Throttle so a persistently-empty / rejecting venue cannot thrash
     * VENUE_ORPHANED<->ACTIVE (and the cancel/replace churn that follows). Auto-recovery is
     * gated on BOTH a minimum spacing (`now - last_recover >= kOrphanRecoveryMinIntervalMs`) and a
     * consecutive-attempt cap (`mm_venue_orphan_consecutive_recoveries_ < kOrphanRecoveryMaxConsecutive`).
     * `mm_venue_orphan_last_recover_ms_` is the steady_clock ms of the last ACTIVE recovery;
     * `mm_venue_orphan_consecutive_recoveries_` counts recoveries since the last healthy cycle and
     * is reset to 0 whenever the venue reports live legs for this symbol (quoting normally).
     */
    std::atomic<std::int64_t> mm_venue_orphan_last_recover_ms_{0};
    std::atomic<int> mm_venue_orphan_consecutive_recoveries_{0};

public:
    /** Product-wide placement serialization: sum of `mm_pending_accepts_` on this AX. */
    static int mmSumProductPendingAcceptsOnAx(const std::string& ax_symbol);

    /** True if any stack on this AX has PLACE_PENDING / CANCEL_PENDING / pending accepts. */
    static bool mmAnyPlacementActivityOnAx(const std::string& ax_symbol);

    /** Wake threads waiting on the per-AX product placement gate (call after place-ack). */
    static void mmNotifyProductPlacementGate(const std::string& ax_symbol);

    /** @return false on timeout — caller must not place (no force-take of a held lease). */
    [[nodiscard]] static bool mmAcquireProductPlacementLease(MakeMarketStrategy* mm);
    static void mmReleaseProductPlacementLease(MakeMarketStrategy* mm);
    /** Safe from mover thread after strategy teardown — resolves live `mm` by name when possible. */
    static void mmReleaseProductPlacementLeaseByName(const std::string& ax_symbol,
                                                     const std::string& strategy_name);
    /** Block teardown until `strategy_name` no longer holds the per-AX placement lease (or timeout). */
    static void mmWaitForStrategyProductPlacementLeaseRelease(const std::string& ax_symbol,
                                                              const std::string& strategy_name,
                                                              int timeout_ms = 5000);

    /** Teardown prep: drop local pending so lease release does not spin wait_budget_ms. */
    void mmTeardownUnblockProductPlacementLease();

    /** Venue-aggregated NetPo for cap gate: syncs every stack on `ax_symbol` then reads. */
    static long long mmProductNetPoForCapGate(const std::string& ax_symbol,
                                              MakeMarketStrategy* cycle_mutex_held);

    /** NetPo at HTTP submit: cycle REST snapshot + local mutex refresh (no per-leg REST). */
    static long long mmProductNetForCapGateAtSubmit(MakeMarketStrategy* mm);

    /** One REST portfolio sync per exclusive placement session (not per submit). */
    static void mmSeedProductCycleNetSnapshot(MakeMarketStrategy* cycle_mutex_held);
    static void mmClearProductCycleNetSnapshot();

    // ==========================================================================
    // Venue source-of-truth guard — stack state machine + venue-OID cache
    // ==========================================================================
    /** Current lifecycle state of this stack (lock-free read). */
    [[nodiscard]] MmStackState mmStackState() const noexcept {
        return mm_stack_state_.load(std::memory_order_acquire);
    }

    /**
     * Transition the stack to `to`, enforcing monotonic progress toward DEAD
     * (TEARDOWN -> DEAD only; DEAD terminal; ACTIVE<->VENUE_ORPHANED reversible).
     * Logs `stack_id + old_state + new_state + reason + thread_id` on a real
     * change. @return true if the state actually changed.
     */
    bool mmTransitionStackState(MmStackState to, const char* reason);

    /**
     * Worker bail-out gate. Returns true only when state == ACTIVE. Otherwise
     * logs `stack_id + state + operation_skipped + thread_id` and returns false.
     * Every mover-thread entry point calls this before touching venue orders.
     */
    bool mmWorkerMayProceed(const char* op);

    /** Replace the cached last-known venue OID set (called by the VENUE_TRUTH poll). */
    void mmUpdateVenueTruthCache(const std::unordered_set<std::string>& live_oids,
                                 std::int64_t now_ms);

    /**
     * @return true if `oid` is in the last-known venue state. `cache_fresh` is set
     * true when that cache was refreshed within the reconcile TTL (3s); a stale
     * cache means the caller should reconcile before trusting an absence.
     */
    [[nodiscard]] bool mmVenueTruthKnowsOid(const std::string& oid, bool& cache_fresh) const;

    /** Force the next mover drain to re-poll VENUE_TRUTH (resets the throttle). */
    void mmTriggerImmediateVenueReconcile();

    [[nodiscard]] bool mmIsQuoteCycleRunning() const {
        return mm_cycle_running_.load(std::memory_order_acquire);
    }
    [[nodiscard]] std::int64_t mmLastCycleActivityMs() const {
        return mm_last_cycle_activity_ms_.load(std::memory_order_relaxed);
    }

    /** Strict bid/ask id 0/0 — partial one-leg stacks must not skip product-quiet. */
    [[nodiscard]] bool mmIsFullyDegradedForLeaseBypass() const {
        return bid_order_id_ == 0 && ask_order_id_ == 0;
    }

    static void mmScheduleDeskRecoveryLeaseBackoff(MakeMarketStrategy* mm);
    static void mmClearDeskRecoveryLeaseBackoff(MakeMarketStrategy* mm);

    // When defer_wire is true the terminal submit uses submit_order_no_dispatch (no inline
    // HTTP); the created OrderManager id is returned via out_order_id so the caller can fire
    // the wire in a concurrent batch. Default false = existing synchronous submit.
    static std::string mmSubmitOrderWithProductCapGate(MakeMarketStrategy* mm,
                                                       const OrderRequest& req,
                                                       core::Side side,
                                                       core::Quantity qty,
                                                       long long cap_ll,
                                                       const std::string& reason,
                                                       bool defer_wire = false,
                                                       OrderId* out_order_id = nullptr);

    // === ACCEPT-ORDERING RACE FIX (2026-07-09) ================================
    // ORDER_ACCEPTED is dispatched SYNCHRONOUSLY, inline, on the submitting thread
    // from inside submit_order() (see Strategy.cpp handleOrderEvent — "arrives
    // before submit_order returns"). Any path that registered its PLACE_PENDING
    // leg or bumped mm_pending_accepts_ AFTER the submit call therefore raced its
    // own accept: the inline on_accept saw pending==0 (skipped its decrement) and
    // found no matching leg, so the post-submit bump/registration got stuck,
    // pinning isPairCycleInFlight()==true forever (the JPY ce60e217 lock).
    //
    // Fix: reserve the pending-accept slot + register the PLACE_PENDING leg with a
    // pre-generated client_order_id BEFORE submit, so the inline (or later async)
    // accept matches and balances it. Set req.client_order_id to the returned id.
    // If the submit does not go out (gate refused / REST failure → empty cid),
    // call mmUnreservePendingPlace() to undo the reservation. Both take
    // tracked_orders_mutex_ internally; call withOUT holding it.
    std::string mmReservePendingPlace(core::Side side, core::Price px, core::Quantity qty);
    void mmUnreservePendingPlace(core::Side side, const std::string& client_order_id);

    // Reserve-before-submit wrapper used by every quote/replace/fill-requote place
    // site. Registers the PLACE_PENDING leg, the reload-count reason, and the submit
    // timestamp under the pre-generated client id BEFORE calling submit, then submits
    // through the product cap gate. On success returns the client id (the inline or
    // async accept balances mm_pending_accepts_ and flips the leg to ADOPTED). On
    // failure it fully rolls back the reservation and returns "". `req` is taken by
    // value; the caller only sets side/qty/price/symbol on it.
    // defer_wire/out_order_id: see mmSubmitOrderWithProductCapGate. When deferring, the leg is
    // reserved + created but NOT fired; the caller batches the wire place. Reservation rollback
    // on gate refusal is unchanged.
    std::string mmPlaceReservedLeg(core::Side side, core::Price px, core::Quantity qty,
                                   OrderRequest req, long long cap_ll, const std::string& reason,
                                   bool defer_wire = false, OrderId* out_order_id = nullptr);

    static void mmMaybeStaleClearProductPendingAccepts(const std::string& ax_symbol);

    /** RAII: exclusive per-AX placement lease + drain place-acks before release. */
    class MmProductPlacementLeaseScope {
    public:
        explicit MmProductPlacementLeaseScope(MakeMarketStrategy* mm);
        ~MmProductPlacementLeaseScope();
        MmProductPlacementLeaseScope(const MmProductPlacementLeaseScope&) = delete;
        MmProductPlacementLeaseScope& operator=(const MmProductPlacementLeaseScope&) = delete;
        [[nodiscard]] bool leaseAcquired() const { return lease_acquired_; }

    private:
        MakeMarketStrategy* mm_{nullptr};
        std::string ax_label_;
        std::string strategy_name_;
        bool outer_{false};
        bool lease_acquired_{false};
    };

    /**
     * Coalesced enqueue: set the pending-quote-cycle flag and post a drain lambda
     * to the global mover thread if not already pending. The caller passes the
     * reason that will be logged when the lambda actually runs.
     *
     * Safe to call from feed thread, timer thread, OM dispatch thread, etc.
     * Returns immediately; cancel + place run later on the mover thread.
     */
    void enqueueQuoteCycleOnMover(const std::string& reason);

    /**
     * Coalesced enqueue for the `mm_pair_enforce` recovery path. The lambda will
     * call `ensureStartupQuotePair(tag)` on the mover thread so cancel + place
     * doesn't race the feed thread.
     */
    void enqueuePairEnforceOnMover(const std::string& tag);

    /**
     * Coalesced enqueue for the post-fill requote cascade. The lambda will call
     * `tryDeskFillRequoteF4PlaceFreshPair()` on the mover thread.
     */
    void enqueueFillRequoteOnMover();

    /**
     * Non-coalescing enqueue for the per-leg place-after-cancel-ack path.
     * Each call posts a fresh lambda; this is rare (one per cancel-ack), so
     * pile-up is not a concern.
     */
    void enqueueDeskTheoMoveAfterCancelAckOnMover(core::Side side, std::string leg_name);

    /**
     * Post a single REST cancel for `local_order_id` to the global mover thread.
     * Used by every site that previously called `mmSendRestCancelByOrderId`
     * directly on a non-mover thread (timer thread, OM dispatch thread, mm_desk_bg
     * thread). The actual REST call runs on the mover thread; sequential ordering
     * with other cancels/places is preserved by the worker's FIFO queue (no AX
     * scoping any more — strict posting-order globally).
     *
     * The local in-memory state mutation that often precedes the cancel (e.g.
     * setting `MmLegState::CANCEL_PENDING` while holding `tracked_orders_mutex_`)
     * MUST stay synchronous on the caller's thread before this enqueue, because
     * subsequent cancel-ack handling depends on that state being visible.
     */
    void enqueueRestCancelOnMover(core::OrderId local_order_id, core::Side side, std::string reason);

    /**
     * Concurrent sibling of `enqueueRestCancelOnMover`: post a SINGLE mover job that fires
     * ALL of a stack's stale-leg cancels together in one curl-multi batch (~1 venue RTT)
     * instead of N serially-queued single-cancel jobs (~N RTT). Each `legs` entry is
     * (local_order_id, side).
     *
     * DEAD-SAFE / semantics-preserving: the batch job performs the exact same per-leg
     * pre-flight validation, venue-truth guard, "cancel send" logging, POST-gateway
     * (+DELETE fallback) transport and `OrderManager::cancelOrder(req)` bookkeeping as the
     * serial path — so the event-driven cancel-ack -> replace flow is byte-identical; ONLY
     * the HTTP transport is parallelised. A leg whose cancel hard-fails is left
     * CANCEL_PENDING for the existing async cancel-timeout reconcile (never dropped, never
     * re-placed here). Cancelling our own resting orders can never create or cross an order.
     */
    void enqueueRestCancelsConcurrentOnMover(
        std::vector<std::pair<core::OrderId, core::Side>> legs, std::string reason);

private:
    void mmDrainPendingQuoteCycle();
    void mmDrainPendingPairEnforce();
    void mmDrainPendingFillRequote();

    /**
     * Venue-as-truth prelude. Called at the top of every mover-thread drain.
     *
     * Issues `GET /open-orders` for `mmAxSymbol()`, builds the set of live exchange
     * oids, and evicts every `tracked_bids_/tracked_asks_` entry whose
     * `exchange_oid` is non-empty AND not present in that set. Also clears
     * `bid_order_id_/ask_order_id_` if they were pointing at an evicted leg.
     *
     * No-op when local tracked vectors are empty (fresh strategy → nothing to
     * reconcile, skip the REST hit). On REST failure or parse failure, logs a
     * warning and returns; we don't speculatively delete tracked rows when we
     * can't see the venue (better to leak a row for one cycle than wrong-cancel).
     *
     * Latency: one GET /open-orders per drain (~150-250 ms). Acceptable per
     * user spec ("it's okay to have latency and correctness when moving order"),
     * but throttled to ~3s per strategy via `mm_venue_reconcile_last_ms_` so
     * back-to-back theo_move drains don't pay the 230ms REST tax twice. Pass
     * `force=true` from `enqueueFillRequoteOnMover` (where ground truth is
     * mandatory) — see callers.
     */
    void mmReconcileTrackedAgainstVenue(bool force = false);

    /**
     * ENFORCE-mode variant of the tracked-vs-venue reconcile: consumes the
     * main-loop VenueOrdersCache instead of issuing a GET on the mover. If the
     * cache is missing/stale (> mmVenueCacheMaxAgeMs()) it logs VENUE_CACHE_STALE
     * once per interval and RETURNS, trusting local order-state (no eviction on
     * stale data). Otherwise it runs the identical eviction/orphan logic via
     * mmApplyVenueTruthReconcile with the cache's live oids.
     */
    void mmReconcileTrackedAgainstVenueFromCache(const std::string& ax);

    /**
     * Shared tail of the tracked-vs-venue reconcile, given the venue's live oid
     * set. `from_cache` selects enforce (cache) vs legacy (GET) behavior: the
     * [VENUE_COUNT_MISMATCH] log and the at-cap venue-diff GET run ONLY on the
     * legacy path (from_cache=false); enforce's mismatch check lives in the
     * main-loop refresher and enforce cap enforcement is snapshot-driven.
     */
    void mmApplyVenueTruthReconcile(const std::string& ax,
                                    const std::unordered_set<std::string>& live_oids,
                                    std::int64_t now_ms,
                                    bool from_cache,
                                    std::size_t rows_count,
                                    std::size_t body_size);
};

} // namespace strategy
} // namespace architect
