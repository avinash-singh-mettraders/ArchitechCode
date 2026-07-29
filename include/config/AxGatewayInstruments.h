#pragma once

#include <nlohmann/json.hpp>
#include <optional>
#include <string>

namespace architect {
namespace config {

/** One row from ``ax_gateway_instruments_catalog`` (Architect ``/instruments``). */
struct AxGatewayCatalogQuote {
    double tick_size{0.0};
    int minimum_order_size{1};
};

/**
 * Look up ``tick_size`` / ``minimum_order_size`` in the slim catalog array
 * (``config["ax_gateway_instruments_catalog"]``). Symbol match ignores spaces,
 * hyphens, underscores (same rules as merge).
 */
std::optional<AxGatewayCatalogQuote> lookupAxGatewayCatalogQuote(const nlohmann::json& catalog,
                                                                 const std::string& ax_symbol);

/**
 * Parse Architect ``GET {rest_endpoint}/instruments`` JSON (OpenAPI
 * ``GetInstrumentsResponse``: object with required ``instruments`` array of
 * ``Instrument`` rows). Live gateway returns ``tick_size`` and ``minimum_order_size``
 * as **strings** (e.g. ``"0.01"``, ``"1"``); this merge accepts numbers or strings.
 * Also accepts a top-level JSON array of instrument objects (legacy / proxy).
 * Merge authoritative ``tick_size`` and ``minimum_order_size`` (→ ``order_size_step``)
 * into ``market_maker`` config, and attach the slim tradeable catalog under
 * ``ax_gateway_instruments_catalog`` on ``config_root``.
 *
 * @param strict_match_configured_symbols  When true, every symbol referenced by
 *        enabled MM config (instruments[] ax/order_symbol or legacy symbol/order_symbol)
 *        must resolve with a positive gateway tick.
 * @param out_tradeable_count  If non-null, set to the number of tradeable rows with tick>0.
 * @return false on hard parse failure or strict validation failure (err set).
 */
bool mergeAxGatewayInstrumentsIntoConfig(nlohmann::json& config_root,
                                         const std::string& http_body,
                                         bool strict_match_configured_symbols,
                                         std::string& err,
                                         int* out_tradeable_count = nullptr);

/** Write ``catalog`` JSON (pretty) to path; creates parent directories. */
bool writeAxGatewayInstrumentsSnapshot(const std::string& path, const nlohmann::json& catalog);

} // namespace config
} // namespace architect
