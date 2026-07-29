#include "config/AxGatewayInstruments.h"

#include <cmath>
#include <cctype>
#include <fstream>
#include <filesystem>
#include <unordered_map>
#include <optional>

namespace architect {
namespace config {

using json = nlohmann::json;

namespace fs = std::filesystem;

namespace {

std::string trim_sym(std::string s) {
    while (!s.empty() && std::isspace(static_cast<unsigned char>(s.front()))) {
        s.erase(s.begin());
    }
    while (!s.empty() && std::isspace(static_cast<unsigned char>(s.back()))) {
        s.pop_back();
    }
    return s;
}

std::string upper_sym(const std::string& s) {
    std::string o;
    o.reserve(s.size());
    for (unsigned char c : s) {
        o.push_back(static_cast<char>(std::toupper(c)));
    }
    return o;
}

/** Match desk: ignore spaces, hyphens, underscores for symbol equality. */
std::string norm_ax_sym(const std::string& s) {
    std::string o;
    o.reserve(s.size());
    for (unsigned char c : s) {
        if (c == ' ' || c == '-' || c == '_') {
            continue;
        }
        o.push_back(static_cast<char>(std::toupper(c)));
    }
    return o;
}

double json_to_positive_double(const json& v) {
    try {
        if (v.is_number_float()) {
            return v.get<double>();
        }
        if (v.is_number_integer()) {
            return static_cast<double>(v.get<std::int64_t>());
        }
        if (v.is_string()) {
            return std::stod(v.get<std::string>());
        }
    } catch (...) {}
    return 0.0;
}

int json_to_positive_int_at_least_one(const json& v) {
    try {
        if (v.is_number_integer()) {
            const int x = v.get<int>();
            return x > 0 ? x : 1;
        }
        if (v.is_number_float()) {
            const double d = v.get<double>();
            if (!std::isfinite(d) || d <= 0.0) {
                return 1;
            }
            return std::max(1, static_cast<int>(std::ceil(d - 1e-9)));
        }
        if (v.is_string()) {
            const double d = std::stod(v.get<std::string>());
            if (!std::isfinite(d) || d <= 0.0) {
                return 1;
            }
            return std::max(1, static_cast<int>(std::ceil(d - 1e-9)));
        }
    } catch (...) {}
    return 1;
}

/** AX ``Instrument.is_tradeable`` may be absent (treat as tradeable), bool, 0/1, or string. */
bool instrument_row_is_tradeable(const json& item) {
    if (!item.contains("is_tradeable")) {
        return true;
    }
    const auto& v = item["is_tradeable"];
    if (v.is_boolean()) {
        return v.get<bool>();
    }
    if (v.is_number_integer()) {
        return v.get<std::int64_t>() != 0;
    }
    if (v.is_number_float()) {
        return std::fabs(v.get<double>()) >= 1e-12;
    }
    if (v.is_string()) {
        std::string s = trim_sym(v.get<std::string>());
        for (std::size_t i = 0; i < s.size(); ++i) {
            s[i] = static_cast<char>(std::tolower(static_cast<unsigned char>(s[i])));
        }
        if (s.empty()) {
            return true;
        }
        if (s == "0" || s == "false" || s == "no" || s == "off") {
            return false;
        }
    }
    return true;
}

/** Prefer OpenAPI field names; ``tick`` / ``price_tick`` / ``min_price`` for compat with older rows. */
double extract_instrument_tick_size(const json& item) {
    static const char* kTick[] = {"tick_size", "tick", "price_tick", "min_price"};
    for (const char* k : kTick) {
        if (!item.contains(k) || item[k].is_null()) {
            continue;
        }
        const double t = json_to_positive_double(item[k]);
        if (t > 0.0 && std::isfinite(t)) {
            return t;
        }
    }
    return 0.0;
}

/** OpenAPI ``minimum_order_size`` (string); fallbacks match ``/markets``-style rows. */
int extract_instrument_minimum_order_size(const json& item) {
    static const char* kMin[] = {"minimum_order_size", "min_order_size", "min_quantity", "min_qty"};
    for (const char* k : kMin) {
        if (!item.contains(k) || item[k].is_null()) {
            continue;
        }
        return json_to_positive_int_at_least_one(item[k]);
    }
    return 1;
}

std::string extract_instrument_symbol(const json& item) {
    if (item.contains("symbol") && item["symbol"].is_string()) {
        return trim_sym(item["symbol"].get<std::string>());
    }
    if (item.contains("s") && item["s"].is_string()) {
        return trim_sym(item["s"].get<std::string>());
    }
    return {};
}

struct GatewayRow {
    double tick_size{0.0};
    int min_order{1};
};

void index_row(std::unordered_map<std::string, GatewayRow>& by_key,
               const std::string& canonical_sym,
               const GatewayRow& row) {
    const std::string t = trim_sym(canonical_sym);
    if (t.empty()) {
        return;
    }
    by_key[t] = row;
    by_key[upper_sym(t)] = row;
    const std::string nk = norm_ax_sym(t);
    if (!nk.empty()) {
        by_key[nk] = row;
    }
}

const GatewayRow* lookup_row(const std::unordered_map<std::string, GatewayRow>& by_key,
                             const std::string& sym) {
    const std::string t = trim_sym(sym);
    if (t.empty()) {
        return nullptr;
    }
    for (const auto& k : {t, upper_sym(t), norm_ax_sym(t)}) {
        if (k.empty()) {
            continue;
        }
        const auto it = by_key.find(k);
        if (it != by_key.end() && it->second.tick_size > 0.0 && std::isfinite(it->second.tick_size)) {
            return &it->second;
        }
    }
    return nullptr;
}

void apply_row_to_mm_leg_json(json& leg, const GatewayRow& row) {
    leg["tick_size"] = row.tick_size;
    // Alias keys used elsewhere in config / docs
    leg["price_tick"] = row.tick_size;
    leg["order_size_step"] = row.min_order;
    if (leg.contains("manual_stacks") && leg["manual_stacks"].is_array()) {
        for (auto& st : leg["manual_stacks"]) {
            if (!st.is_object()) {
                continue;
            }
            st["order_size_step"] = row.min_order;
        }
    }
}

bool mm_json_enabled(const json& mm) {
    if (!mm.is_object() || !mm.contains("enabled") || !mm["enabled"].is_boolean()) {
        return false;
    }
    return mm["enabled"].get<bool>();
}

void collect_mm_symbols_from_config(const json& root, std::vector<std::string>& out_syms) {
    if (!root.contains("market_maker") || !root["market_maker"].is_object()) {
        return;
    }
    const auto& mm = root["market_maker"];
    if (!mm_json_enabled(mm)) {
        return;
    }
    if (mm.contains("instruments") && mm["instruments"].is_array() && !mm["instruments"].empty()) {
        for (const auto& el : mm["instruments"]) {
            if (!el.is_object()) {
                continue;
            }
            std::string ax;
            if (el.contains("symbol") && el["symbol"].is_string()) {
                ax = el["symbol"].get<std::string>();
            } else if (el.contains("ax_symbol") && el["ax_symbol"].is_string()) {
                ax = el["ax_symbol"].get<std::string>();
            }
            std::string os;
            if (el.contains("order_symbol") && el["order_symbol"].is_string()) {
                os = el["order_symbol"].get<std::string>();
            }
            ax = trim_sym(ax);
            os = trim_sym(os);
            if (!ax.empty()) {
                out_syms.push_back(ax);
            }
            if (!os.empty() && os != ax) {
                out_syms.push_back(os);
            }
        }
        return;
    }
    // Legacy single-symbol MM
    std::string sym = mm.value("symbol", std::string{});
    std::string ord = mm.value("order_symbol", std::string{});
    sym = trim_sym(sym);
    ord = trim_sym(ord);
    if (!sym.empty()) {
        out_syms.push_back(sym);
    }
    if (!ord.empty() && ord != sym) {
        out_syms.push_back(ord);
    }
}

std::optional<AxGatewayCatalogQuote> lookup_ax_gateway_catalog_quote(const json& catalog,
                                                                     const std::string& sym) {
    const std::string needle = norm_ax_sym(sym);
    if (needle.empty() || !catalog.is_array()) {
        return std::nullopt;
    }
    for (const auto& item : catalog) {
        if (!item.is_object()) {
            continue;
        }
        std::string row_sym = extract_instrument_symbol(item);
        if (row_sym.empty()) {
            continue;
        }
        if (norm_ax_sym(row_sym) != needle) {
            continue;
        }
        const double tick = extract_instrument_tick_size(item);
        if (!(tick > 0.0) || !std::isfinite(tick)) {
            return std::nullopt;
        }
        AxGatewayCatalogQuote q;
        q.tick_size = tick;
        q.minimum_order_size = extract_instrument_minimum_order_size(item);
        return q;
    }
    return std::nullopt;
}

} // namespace

std::optional<AxGatewayCatalogQuote> lookupAxGatewayCatalogQuote(const json& catalog,
                                                                 const std::string& ax_symbol) {
    return lookup_ax_gateway_catalog_quote(catalog, ax_symbol);
}

bool writeAxGatewayInstrumentsSnapshot(const std::string& path, const json& catalog) {
    try {
        const fs::path p(path);
        if (p.has_parent_path()) {
            fs::create_directories(p.parent_path());
        }
        std::ofstream out(p, std::ios::binary | std::ios::trunc);
        if (!out) {
            return false;
        }
        out << catalog.dump(2);
        return static_cast<bool>(out);
    } catch (...) {
        return false;
    }
}

bool mergeAxGatewayInstrumentsIntoConfig(json& root,
                                         const std::string& http_body,
                                         bool strict_match_configured_symbols,
                                         std::string& err,
                                         int* out_tradeable_count) {
    if (out_tradeable_count) {
        *out_tradeable_count = 0;
    }
    err.clear();
    json parsed;
    try {
        parsed = json::parse(http_body);
    } catch (const std::exception& e) {
        err = std::string("JSON parse error: ") + e.what();
        return false;
    }
    json rows = json::array();
    if (parsed.is_object() && parsed.contains("instruments") && parsed["instruments"].is_array()) {
        rows = parsed["instruments"];
    } else if (parsed.is_object() && parsed.contains("data") && parsed["data"].is_object() &&
               parsed["data"].contains("instruments") && parsed["data"]["instruments"].is_array()) {
        rows = parsed["data"]["instruments"];
    } else if (parsed.is_array()) {
        rows = std::move(parsed);
    } else {
        err = "instruments response: expected {\"instruments\":[...]} (OpenAPI GetInstrumentsResponse), "
              "optional {\"data\":{\"instruments\":[...]}}, or a top-level array of instrument objects";
        return false;
    }

    std::unordered_map<std::string, GatewayRow> by_key;
    by_key.reserve(rows.size() * 3u + 8u);
    json catalog_tradeable = json::array();
    int tradeable = 0;

    for (const auto& item : rows) {
        if (!item.is_object()) {
            continue;
        }
        if (!instrument_row_is_tradeable(item)) {
            continue;
        }
        const std::string sym = extract_instrument_symbol(item);
        if (sym.empty()) {
            continue;
        }
        const double tick = extract_instrument_tick_size(item);
        if (!(tick > 0.0) || !std::isfinite(tick)) {
            continue;
        }
        const int mino = extract_instrument_minimum_order_size(item);
        GatewayRow row;
        row.tick_size = tick;
        row.min_order = mino;
        index_row(by_key, sym, row);
        json slim = json::object();
        slim["symbol"] = sym;
        slim["tick_size"] = tick;
        slim["minimum_order_size"] = mino;
        if (item.contains("multiplier") && !item["multiplier"].is_null()) {
            slim["multiplier"] = item["multiplier"];
        }
        if (item.contains("price_scale") && !item["price_scale"].is_null()) {
            slim["price_scale"] = item["price_scale"];
        }
        catalog_tradeable.push_back(std::move(slim));
        ++tradeable;
    }

    if (out_tradeable_count) {
        *out_tradeable_count = tradeable;
    }
    root["ax_gateway_instruments_catalog"] = std::move(catalog_tradeable);

    if (tradeable <= 0) {
        err = "no tradeable instruments with positive tick_size in /instruments";
        return false;
    }

    // --- merge into market_maker.* (only when MM enabled) ---
    if (root.contains("market_maker") && root["market_maker"].is_object()) {
        json& mm = root["market_maker"];
        if (mm_json_enabled(mm)) {
            if (mm.contains("instruments") && mm["instruments"].is_array() && !mm["instruments"].empty()) {
                for (auto& el : mm["instruments"]) {
                    if (!el.is_object()) {
                        continue;
                    }
                    std::string ax;
                    if (el.contains("symbol") && el["symbol"].is_string()) {
                        ax = el["symbol"].get<std::string>();
                    } else if (el.contains("ax_symbol") && el["ax_symbol"].is_string()) {
                        ax = el["ax_symbol"].get<std::string>();
                    }
                    std::string ord = ax;
                    if (el.contains("order_symbol") && el["order_symbol"].is_string()) {
                        ord = el["order_symbol"].get<std::string>();
                    }
                    const GatewayRow* r = lookup_row(by_key, ax);
                    if (!r) {
                        r = lookup_row(by_key, ord);
                    }
                    if (r) {
                        apply_row_to_mm_leg_json(el, *r);
                    }
                }
            } else {
                // Legacy: single product under market_maker.symbol
                std::string sym = mm.value("symbol", std::string{});
                std::string ord = mm.value("order_symbol", std::string{});
                const GatewayRow* r = lookup_row(by_key, sym);
                if (!r) {
                    r = lookup_row(by_key, ord);
                }
                if (r) {
                    mm["price_tick"] = r->tick_size;
                    mm["order_size_step"] = r->min_order;
                }
            }
        }
    }

    if (strict_match_configured_symbols) {
        std::vector<std::string> required;
        collect_mm_symbols_from_config(root, required);
        std::vector<std::string> missing;
        for (const auto& s : required) {
            if (lookup_row(by_key, s) == nullptr) {
                missing.push_back(s);
            }
        }
        if (!missing.empty()) {
            err = "MM symbols missing from gateway /instruments (tick): ";
            for (std::size_t i = 0; i < missing.size(); ++i) {
                if (i) {
                    err += ", ";
                }
                err += missing[i];
            }
            return false;
        }
    }

    return true;
}

} // namespace config
} // namespace architect
