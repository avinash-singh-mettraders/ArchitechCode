// =============================================================================
// REGRESSION — multi-stack mm_req_* order-event ownership routing
// =============================================================================
//
// Root cause: MakeMarketStrategy::orderEventMatchesStrategy matched any
// ORDER_* on the shared AX symbol, so N desk stacks stole each other's
// accepts/fills and corrupted bid_id/ask_id and net position.
//
// This test models three stacks on one AX with the same routing contract as
// production (anchored client_order_id, desk-adopt venue OID, local/venue map).
// Must stay in sync with MakeMarketStrategy::orderEventMatchesStrategy.

#include "test_helpers.h"

#include <cstdint>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace {

constexpr const char kDeskAdoptPrefix[] = "desk-adopt-";

bool anchoredClientOrderIdMatch(const std::string& cid, const std::string& strategy_name) {
    if (cid.empty() || strategy_name.empty()) {
        return false;
    }
    if (cid.size() < strategy_name.size()) {
        return false;
    }
    if (cid.compare(0, strategy_name.size(), strategy_name) != 0) {
        return false;
    }
    if (cid.size() == strategy_name.size()) {
        return true;
    }
    return cid[strategy_name.size()] == '_';
}

bool deskAdoptClientOrderId(const std::string& cid) {
    return cid.rfind(kDeskAdoptPrefix, 0) == 0;
}

std::string exchangeOidFromDeskAdopt(const std::string& cid) {
    if (!deskAdoptClientOrderId(cid)) {
        return {};
    }
    return cid.substr(sizeof(kDeskAdoptPrefix) - 1);
}

struct TrackedLeg {
    std::uint64_t    local_oid{0};
    std::string      exchange_oid;
    std::string      pending_place_client_id;
    bool             place_pending{false};
};

struct StackModel {
    std::string                       name;
    std::unordered_set<std::uint64_t> open_orders;
    std::uint64_t                     bid_oid{0};
    std::uint64_t                     ask_oid{0};
    std::vector<TrackedLeg>           bids;
    std::vector<TrackedLeg>           asks;
    int                               net_po{0};
};

struct OrderEvent {
    std::uint64_t order_id{0};
    std::string   client_order_id;
    std::string   exchange_oid;  // OM lookup when client_order_id empty
};

// Mirrors production routing (no symbol-only match).
bool orderEventMatchesStack(const StackModel& s, const OrderEvent& ev) {
    if (!ev.client_order_id.empty()) {
        if (anchoredClientOrderIdMatch(ev.client_order_id, s.name)) {
            return true;
        }
        if (deskAdoptClientOrderId(ev.client_order_id)) {
            const std::string ex = exchangeOidFromDeskAdopt(ev.client_order_id);
            for (const auto& t : s.bids) {
                if (!t.exchange_oid.empty() && t.exchange_oid == ex) {
                    return true;
                }
            }
            for (const auto& t : s.asks) {
                if (!t.exchange_oid.empty() && t.exchange_oid == ex) {
                    return true;
                }
            }
        }
        return false;
    }
    if (s.open_orders.count(ev.order_id) != 0) {
        return true;
    }
    if (!ev.exchange_oid.empty()) {
        for (const auto& t : s.bids) {
            if (!t.exchange_oid.empty() && t.exchange_oid == ev.exchange_oid) {
                return true;
            }
        }
        for (const auto& t : s.asks) {
            if (!t.exchange_oid.empty() && t.exchange_oid == ev.exchange_oid) {
                return true;
            }
        }
    }
    if (ev.order_id == s.bid_oid || ev.order_id == s.ask_oid) {
        return true;
    }
    for (const auto& t : s.bids) {
        if (t.local_oid == ev.order_id) {
            return true;
        }
    }
    for (const auto& t : s.asks) {
        if (t.local_oid == ev.order_id) {
            return true;
        }
    }
    return false;
}

void dispatchAccept(StackModel& owner, const OrderEvent& ev, bool is_bid) {
    owner.open_orders.insert(ev.order_id);
    TrackedLeg leg;
    leg.local_oid = ev.order_id;
    leg.exchange_oid = ev.exchange_oid;
    if (is_bid) {
        owner.bid_oid = ev.order_id;
        owner.bids.push_back(leg);
    } else {
        owner.ask_oid = ev.order_id;
        owner.asks.push_back(leg);
    }
}

void dispatchFill(StackModel& owner, const OrderEvent& ev, int qty_delta) {
    owner.net_po += qty_delta;
    owner.open_orders.erase(ev.order_id);
}

void anchored_prefix_no_cross_stack_steal() {
    const std::string short_stack = "mm_req_XAG_PERP_eca8";
    const std::string long_stack = "mm_req_XAG_PERP_eca823da";
    TX_REQUIRE(!anchoredClientOrderIdMatch(long_stack + "_20260523_000001", short_stack));
    TX_REQUIRE(anchoredClientOrderIdMatch(short_stack + "_20260523_000001", short_stack));
    TX_REQUIRE(anchoredClientOrderIdMatch(long_stack + "_20260523_000001", long_stack));
}

void three_stacks_no_cross_accept_or_fill() {
    std::vector<StackModel> stacks = {
        {"mm_req_XAG_PERP_2aa2c626", {}, 0, 0, {}, {}, 0},
        {"mm_req_XAG_PERP_eca823da", {}, 0, 0, {}, {}, 0},
        {"mm_req_XAG_PERP_d321e08b", {}, 0, 0, {}, {}, 0},
    };

    struct Place {
        std::size_t stack_idx;
        std::uint64_t order_id;
        std::string   client_order_id;
        std::string   exchange_oid;
        bool          bid;
    };
    const std::vector<Place> places = {
        {0, 101, "mm_req_XAG_PERP_2aa2c626_20260523_000001", "O-STACK0-BID", true},
        {0, 102, "mm_req_XAG_PERP_2aa2c626_20260523_000002", "O-STACK0-ASK", false},
        {1, 201, "mm_req_XAG_PERP_eca823da_20260523_000001", "O-STACK1-BID", true},
        {1, 202, "mm_req_XAG_PERP_eca823da_20260523_000002", "O-STACK1-ASK", false},
        {2, 301, "mm_req_XAG_PERP_d321e08b_20260523_000001", "O-STACK2-BID", true},
        {2, 302, "mm_req_XAG_PERP_d321e08b_20260523_000002", "O-STACK2-ASK", false},
    };

    for (const auto& p : places) {
        OrderEvent ev{p.order_id, p.client_order_id, p.exchange_oid};
        for (std::size_t i = 0; i < stacks.size(); ++i) {
            if (!orderEventMatchesStack(stacks[i], ev)) {
                continue;
            }
            if (i != p.stack_idx) {
                TX_REQUIRE(false);  // sibling must not match before accept
            }
        }
        dispatchAccept(stacks[p.stack_idx], ev, p.bid);
    }

    TX_EQ(stacks[0].bid_oid, 101u);
    TX_EQ(stacks[0].ask_oid, 102u);
    TX_EQ(stacks[1].bid_oid, 201u);
    TX_EQ(stacks[1].ask_oid, 202u);
    TX_EQ(stacks[2].bid_oid, 301u);
    TX_EQ(stacks[2].ask_oid, 302u);
    // Sibling stacks must not have adopted another stack's local order_id as top-of-book.
    TX_REQUIRE(stacks[1].bid_oid != 101u);
    TX_REQUIRE(stacks[2].ask_oid != 102u);

    // Fill without clOrdID (venue dropped) — route by exchange_oid only to owner.
    {
        OrderEvent fill{101, "", "O-STACK0-BID"};
        int owners = 0;
        for (auto& s : stacks) {
            if (orderEventMatchesStack(s, fill)) {
                dispatchFill(s, fill, 1);
                ++owners;
            }
        }
        TX_EQ(owners, 1);
        TX_EQ(stacks[0].net_po, 1);
        TX_EQ(stacks[1].net_po, 0);
        TX_EQ(stacks[2].net_po, 0);
    }

    // Desk-adopt client id routes by embedded venue OID, not symbol.
    stacks[1].bids.clear();
    stacks[1].asks.clear();
    stacks[1].bid_oid = 0;
    stacks[1].ask_oid = 0;
    TrackedLeg adopted;
    adopted.local_oid = 401;
    adopted.exchange_oid = "O-ADOPT-BID";
    stacks[1].bids.push_back(adopted);
    stacks[1].open_orders.insert(401);

    OrderEvent desk_fill{401, "desk-adopt-O-ADOPT-BID", "O-ADOPT-BID"};
    int adopt_owners = 0;
    for (auto& s : stacks) {
        if (orderEventMatchesStack(s, desk_fill)) {
            dispatchFill(s, desk_fill, -1);
            ++adopt_owners;
        }
    }
    TX_EQ(adopt_owners, 1);
    TX_EQ(stacks[1].net_po, -1);
    TX_EQ(stacks[0].net_po, 1);
}

void shared_order_id_counter_still_isolated() {
    // Same local order_id on two stacks (shared OM counter) must not cross-route.
    std::vector<StackModel> stacks = {
        {"mm_req_XAG_PERP_aaa", {7}, 7, 0, {{7, "O-A-BID", ""}}, {}, 0},
        {"mm_req_XAG_PERP_bbb", {7}, 0, 7, {}, {{7, "O-B-ASK", ""}}, 0},
    };
    OrderEvent foreign_fill{7, "mm_req_XAG_PERP_bbb_20260523_000099", "O-B-ASK"};
    TX_REQUIRE(!orderEventMatchesStack(stacks[0], foreign_fill));
    TX_REQUIRE(orderEventMatchesStack(stacks[1], foreign_fill));
}

}  // namespace

int main() {
    std::printf("=== multi-stack mm_req_* order-event ownership ===\n");
    TX_RUN(anchored_prefix_no_cross_stack_steal);
    TX_RUN(three_stacks_no_cross_accept_or_fill);
    TX_RUN(shared_order_id_counter_still_isolated);
    return tx::finish("test_mm_order_event_ownership");
}
