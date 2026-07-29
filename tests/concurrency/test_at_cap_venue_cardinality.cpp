// At-cap converge: immediate scrub when reduce-side venue rows > protected OIDs.

#include "test_helpers.h"

#include <string>

namespace {

bool shouldImmediateConvergeAtCap(int reduce_side_rows, int protect_oid_count) {
    return reduce_side_rows > std::max(1, protect_oid_count);
}

bool mayPlaceReduceSideAtCap(int reduce_side_rows_after_scrub) {
    return reduce_side_rows_after_scrub <= 1;
}

void scenario_excess_rows_trigger_converge() {
    TX_REQUIRE(shouldImmediateConvergeAtCap(3, 1));
    TX_REQUIRE(!shouldImmediateConvergeAtCap(1, 1));
}

void scenario_place_blocked_until_single_row() {
    TX_REQUIRE(mayPlaceReduceSideAtCap(1));
    TX_REQUIRE(!mayPlaceReduceSideAtCap(3));
}

}  // namespace

int main() {
    std::printf("=== At-cap venue cardinality ===\n");
    TX_RUN(scenario_excess_rows_trigger_converge);
    TX_RUN(scenario_place_blocked_until_single_row);
    return tx::finish("test_at_cap_venue_cardinality");
}
