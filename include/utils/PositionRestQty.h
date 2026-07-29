#pragma once

/**
 * Parse signed position quantity from GET /positions rows.
 *
 * Source of truth (Architect api-gateway OpenAPI): each item in `positions` has required
 * `signed_quantity` (int64, positive = long, negative = short). See:
 * https://docs.architect.exchange/api-reference/portfolio-management/get-positions
 *
 * Fallback keys support older/alternate payloads and nested shapes.
 */

#include <cmath>
#include <nlohmann/json.hpp>

namespace architect {
namespace utils {

inline double jsonScalarToDouble(const nlohmann::json& v) {
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

/**
 * Signed quantity: positive = long, negative = short. Returns 0 if absent or flat.
 */
inline double signedPositionQtyFromRestRow(const nlohmann::json& j, int depth = 0) {
    constexpr int kMaxDepth = 5;
    if (!j.is_object() || depth > kMaxDepth) {
        return 0.0;
    }

    // OpenAPI Position: signed_quantity (required)
    for (const char* k : {"signed_quantity", "signedQuantity"}) {
        if (j.contains(k)) {
            return jsonScalarToDouble(j.at(k));
        }
    }

    static const char* kQtyKeys[] = {
        "open_quantity",
        "open_qty",
        "openQty",
        "net_position",
        "netPosition",
        "net_qty",
        "netQty",
        "positionAmt",
        "position_qty",
        "positionQty",
        "quantity",
        "qty",
        "q",
        "position",
        "size",
        "contracts",
        "contract_qty",
        "contractQty",
        "base_position",
        "basePosition",
        "base_qty",
        "baseQty",
        "pos",
    };

    for (const char* key : kQtyKeys) {
        if (!j.contains(key)) {
            continue;
        }
        const auto& v = j.at(key);
        if (v.is_object()) {
            const double inner = signedPositionQtyFromRestRow(v, depth + 1);
            if (std::abs(inner) > 1e-15) {
                return inner;
            }
            continue;
        }
        const double x = jsonScalarToDouble(v);
        if (std::abs(x) > 1e-15) {
            return x;
        }
    }

    static const char* kWrapKeys[] = {
        "position", "pos", "info", "details", "data", "snapshot", "aggregate", "summary"};
    for (const char* key : kWrapKeys) {
        if (!j.contains(key) || !j.at(key).is_object()) {
            continue;
        }
        const double x = signedPositionQtyFromRestRow(j.at(key), depth + 1);
        if (std::abs(x) > 1e-15) {
            return x;
        }
    }

    if (j.contains("long_qty") || j.contains("short_qty")) {
        const double lq = j.contains("long_qty") ? jsonScalarToDouble(j.at("long_qty")) : 0.0;
        const double sq = j.contains("short_qty") ? jsonScalarToDouble(j.at("short_qty")) : 0.0;
        const double net = lq - sq;
        if (std::abs(net) > 1e-15) {
            return net;
        }
    }

    return 0.0;
}

/** OpenAPI Position: signed_notional (string or number), USD notional (sign matches side). */
inline double signedNotionalFromRestRow(const nlohmann::json& j) {
    if (!j.is_object()) {
        return 0.0;
    }
    for (const char* k : {"signed_notional", "signedNotional"}) {
        if (j.contains(k)) {
            return jsonScalarToDouble(j.at(k));
        }
    }
    return 0.0;
}

} // namespace utils
} // namespace architect
