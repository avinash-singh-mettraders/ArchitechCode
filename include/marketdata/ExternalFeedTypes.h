#pragma once

#include "core/Types.h"
#include <chrono>
#include <cctype>
#include <string>
#include <string_view>

namespace architect {
namespace marketdata {

/** Uppercase alnum-only form for matching FIX 55 variants (e.g. USD/JPY vs USDJPY). */
inline std::string fixSymbolCanonical(std::string_view sym) {
    std::string o;
    o.reserve(sym.size());
    for (unsigned char ch : sym) {
        if (ch == '/' || ch == '-' || ch == ' ' || ch == '_') {
            continue;
        }
        o.push_back(static_cast<char>(std::toupper(ch)));
    }
    return o;
}

struct ExternalFeedQuote {
    core::Price bid{0.0};
    core::Price ask{0.0};
    core::Price mid{0.0};
    std::chrono::steady_clock::time_point updated_at;
    bool valid{false};
    /** FIX tag 55 wire value when from neon_fix; empty for legacy REST (keyed by external_feed.symbol). */
    std::string fix_symbol;
};

} // namespace marketdata
} // namespace architect
