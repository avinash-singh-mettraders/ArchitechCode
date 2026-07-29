// =============================================================================
// REGRESSION — per-AX open-orders instrument filter (6×5 desk scale)
// =============================================================================
//
// Production bug class: mixed GET /open-orders books treated foreign or
// symbol-less rows as belonging to the requested AX → orphan scrub cancelled
// another metal's legs.
//
// Contract mirrored from MakeMarketStrategy.cpp (mmExtractRowSymbol /
// mmOpenOrdersRowMatchesAx). Reject empty symbol; reject foreign symbol.

#include "test_helpers.h"

#include <nlohmann/json.hpp>

#include <string>
#include <unordered_map>
#include <vector>

namespace {

std::string extractRowSymbol(const nlohmann::json& row) {
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

std::string upperAscii(std::string s) {
    for (char& c : s) {
        c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
    }
    return s;
}

bool openOrdersRowMatchesAx(const std::string& ax_symbol, const nlohmann::json& row) {
    if (ax_symbol.empty()) {
        return false;
    }
    const std::string row_sym = extractRowSymbol(row);
    if (row_sym.empty()) {
        return false;
    }
    return upperAscii(row_sym) == upperAscii(ax_symbol);
}

struct FilterStats {
    int kept{0};
    int dropped_foreign{0};
    int dropped_no_symbol{0};
};

FilterStats filterRowsForAx(const std::string& ax, const std::vector<nlohmann::json>& rows) {
    FilterStats st;
    for (const auto& row : rows) {
        if (extractRowSymbol(row).empty()) {
            ++st.dropped_no_symbol;
            continue;
        }
        if (!openOrdersRowMatchesAx(ax, row)) {
            ++st.dropped_foreign;
            continue;
        }
        ++st.kept;
    }
    return st;
}

bool venueCancelAllowed(const std::string& ax,
                        const std::string& oid,
                        const std::unordered_map<std::string, std::string>& oid_owner_ax) {
    if (oid.empty() || ax.empty()) {
        return false;
    }
    const auto it = oid_owner_ax.find(oid);
    if (it != oid_owner_ax.end() && !it->second.empty() && it->second != ax) {
        return false;
    }
    return true;
}

}  // namespace

static void test_foreign_and_no_symbol_rows() {
    const std::vector<nlohmann::json> rows = {
        {{"symbol", "XAU-PERP"}, {"oid", "O-XAU-1"}},
        {{"symbol", "XAG-PERP"}, {"oid", "O-XAG-1"}},
        {{"oid", "O-NO-SYM"}},
        {{"s", "xau-perp"}, {"oid", "O-XAU-2"}},
    };
    const auto xau = filterRowsForAx("XAU-PERP", rows);
    TX_EQ(xau.kept, 2);
    TX_EQ(xau.dropped_foreign, 1);
    TX_EQ(xau.dropped_no_symbol, 1);

    const auto xag = filterRowsForAx("XAG-PERP", rows);
    TX_EQ(xag.kept, 1);
    TX_EQ(xag.dropped_foreign, 2);
    TX_EQ(xag.dropped_no_symbol, 1);
    tx::report_pass("foreign_and_no_symbol_rows");
}

static void test_six_instruments_isolated() {
    const char* axes[] = {"EURUSD-PERP", "JPYUSD-PERP", "XAU-PERP",
                          "XAG-PERP",  "SPY-PERP",    "WTIOIL-PERP"};
    std::vector<nlohmann::json> book;
    for (const char* ax : axes) {
        for (int i = 0; i < 10; ++i) {
            book.push_back({{"symbol", ax}, {"oid", std::string(ax) + "-B" + std::to_string(i)}});
            book.push_back({{"symbol", ax}, {"oid", std::string(ax) + "-A" + std::to_string(i)}});
        }
    }
    book.push_back({{"oid", "O-AMBIGUOUS"}});

    for (const char* ax : axes) {
        const auto st = filterRowsForAx(ax, book);
        TX_EQ(st.kept, 20);
        TX_EQ(st.dropped_foreign, 100);
        TX_EQ(st.dropped_no_symbol, 1);
    }
    tx::report_pass("six_instruments_isolated");
}

static void test_refuse_cross_ax_venue_cancel() {
    std::unordered_map<std::string, std::string> owners{
        {"O-XAU-1", "XAU-PERP"},
        {"O-XAG-1", "XAG-PERP"},
    };
    TX_REQUIRE(venueCancelAllowed("XAU-PERP", "O-XAU-1", owners));
    TX_REQUIRE(!venueCancelAllowed("XAG-PERP", "O-XAU-1", owners));
    TX_REQUIRE(!venueCancelAllowed("XAU-PERP", "O-XAG-1", owners));
    TX_REQUIRE(venueCancelAllowed("XAG-PERP", "O-XAG-1", owners));
    tx::report_pass("refuse_cross_ax_venue_cancel");
}

int main() {
    std::printf("[SUITE] instrument_isolation_filter\n");
    test_foreign_and_no_symbol_rows();
    test_six_instruments_isolated();
    test_refuse_cross_ax_venue_cancel();
    return tx::finish("instrument_isolation_filter");
}
