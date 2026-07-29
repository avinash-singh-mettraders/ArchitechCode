#pragma once

/**
 * @file NeonFixFeed.h
 * @brief FIX 4.4 quote session (Integral / Neon-style) for external theo (TOB or full book per md_market_depth).
 */

#include "marketdata/ExternalFeedTypes.h"
#include <atomic>
#include <functional>
#include <string>
#include <utility>
#include <vector>

namespace architect {
namespace marketdata {

struct NeonFixSettings {
    std::string host{"127.0.0.1"};
    int port{14508};
    std::string sender_comp_id;
    std::string target_comp_id;
    std::string deliver_to_comp_id;
    std::string sender_sub_id;
    std::string username;
    std::string password;
    int heart_bt_int{30};
    /** FIX tag 55 symbol that drives theo (must match MD responses). */
    std::string pricing_symbol;
    /** All FIX 55 symbols to subscribe (snapshot + incremental). */
    std::vector<std::string> md_symbols;
    /**
     * FIX tag 264 MarketDepth on MD request: 0 = full book (venue-dependent), 1 = top of book only.
     * Use 0 when you need multiple price levels (e.g. desk depth JSON); some venues reject 0.
     */
    int md_market_depth{1};
    /**
     * FIX tag 265 MDUpdateType in Market Data Request: 0 = full refresh, 1 = incremental.
     * Neon/Integral often rejects 1 (MDUpdateTypeNotSupported); default 0.
     * Set to -1 to omit tag 265 entirely if your venue requires that.
     */
    int md_update_type{0};
    /** Extra FIX tags after each 55 in the NoRelatedSym block; emitter orders 460 before 167, then remaining tags by tag number. */
    std::vector<std::pair<std::string, std::string>> md_instrument_extra;
    /** Tags appended after 553/554/141 on Logon (Neon ProductNotSet may require a product/subscription tag here). */
    std::vector<std::pair<std::string, std::string>> logon_extra;
    /** Tags on MD request after 263/264/265 and before 146 (message-level). */
    std::vector<std::pair<std::string, std::string>> md_request_root_extra;
    /** If true: 263=0 snapshot only, omit 265 (workaround for strict venues). */
    bool md_snapshot_only{false};
};

/**
 * Blocking loop: TCP connect, Logon, MarketDataRequest, process W/X until running=false.
 * Invokes sink on bid/ask updates for pricing_symbol; on_error for disconnect/reject/logout text.
 * depth_emit (optional): sorted bid/ask ladders from the in-memory book (full depth when md_market_depth==0).
 */
void runNeonFixFeed(
    const NeonFixSettings& settings,
    std::atomic<bool>& running,
    const std::function<void(const ExternalFeedQuote&)>& sink,
    const std::function<void(std::string)>& on_error,
    const std::function<void(const std::vector<std::pair<double, double>>&, const std::vector<std::pair<double, double>>&)>&
        depth_emit = {},
    const std::function<void(bool fix_session_logged_on)>& on_fix_session_state = {});

} // namespace marketdata
} // namespace architect
