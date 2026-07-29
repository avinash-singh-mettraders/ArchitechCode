# =============================================================================
# Concurrency regression tests (2026-05-20 audit)
# =============================================================================
#
# Built only when -DBUILD_TESTS=ON. Each test is a standalone binary with
# its own main() — no GoogleTest/Catch2 dependency. They probe the same
# atomic / mutex primitives the strategy now uses and assert the production
# invariant that was being violated for each of Tim Izzo's three reports.
#
# Run via:
#   cmake -S . -B build -DBUILD_TESTS=ON
#   cmake --build build --target concurrency_tests
#   ctest --test-dir build --output-on-failure -R concurrency_

set(CONCURRENCY_TEST_SOURCES
    concurrency/test_max_position_no_breach.cpp
    concurrency/test_desk_recovery_guard.cpp
    concurrency/test_crash_watchdog.cpp
    concurrency/test_atomic_pending_accepts.cpp
    concurrency/test_feed_callback_and_order_ids.cpp
    concurrency/test_skew_symmetry.cpp
    concurrency/test_cancel_revealed_fill_no_breach.cpp
    concurrency/test_silver_breach_replay.cpp
    concurrency/test_reconcile_gate.cpp
    concurrency/test_mm_order_event_ownership.cpp
    concurrency/test_instrument_isolation_filter.cpp
    concurrency/test_at_cap_venue_cardinality.cpp
    concurrency/test_venue_orphan_bailout.cpp
    concurrency/test_triple_divergence_teardown.cpp
    concurrency/test_feed_callback_uaf.cpp
    concurrency/test_paired_pull_on_reject.cpp
    concurrency/test_venue_truth_always_cancel.cpp
    # --- Cancel-first theo-move reorder (2026-07-03, client-approved) ---------
    concurrency/test_cancel_precedes_gate.cpp
    concurrency/test_canceled_order_filled_blocks_place.cpp
    concurrency/test_cycle_abort_after_cancel_leaves_flat.cpp
    concurrency/test_mwr_pause_cancels_still_go.cpp
    # --- Parallel per-instrument movers + accept-ordering (2026-07-09) ---------
    concurrency/test_mover_shard_accept_race.cpp
    # --- Fire-and-track cancel-replace (place_before_cancel_ack, 2026-07-12) ---
    concurrency/test_fire_and_track.cpp
    # --- Async batch place: accept-counter integrity (Option B, 2026-07-13) -----
    concurrency/test_batch_place_accept_ordering.cpp
    # --- Combined cancel+place batch + self-trade guard (2026-07-13) ------------
    concurrency/test_combined_cancel_place_batch.cpp
    # --- Venue-truth eviction propagation/adoption grace (JPY/EURUSD dup, 2026-07-14) ---
    concurrency/test_venue_truth_evict_grace.cpp
    # --- Concurrent pair-cancel (theo-move latency fix, 2026-07-14) --------------
    concurrency/test_concurrent_cancels.cpp
    # --- orders.json mass-teardown guardrail + incident replay (2026-07-15) ------
    concurrency/test_orders_json_mass_teardown_guard.cpp
    # --- Per-instrument HOLD: mm_orders_enabled gates every place site (2026-07-23) ---
    concurrency/test_per_instrument_hold.cpp
)

# ThreadSanitizer: build the concurrency tests with -fsanitize=thread to prove the
# modelled contracts (per-AX serialization, reserve-before-submit, guarded fill
# stream) are data-race-free.  Enable with:
#   cmake -S . -B build -DBUILD_TESTS=ON -DENABLE_TSAN=ON
#   cmake --build build --target concurrency_tests
#   ctest --test-dir build --output-on-failure -R concurrency_
option(ENABLE_TSAN "Build concurrency tests under ThreadSanitizer" OFF)

add_custom_target(concurrency_tests)

foreach(src ${CONCURRENCY_TEST_SOURCES})
    get_filename_component(exe_name ${src} NAME_WE)
    add_executable(${exe_name} ${src})
    target_include_directories(${exe_name} PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/concurrency
    )
    # These tests intentionally model the concurrency contract in isolation —
    # no link against strategy_lib so they remain fast and self-contained.
    target_link_libraries(${exe_name} PRIVATE Threads::Threads)
    if(exe_name STREQUAL "test_instrument_isolation_filter")
        target_link_libraries(${exe_name} PRIVATE nlohmann_json::nlohmann_json)
    endif()
    target_compile_features(${exe_name} PRIVATE cxx_std_20)
    # Tests probe race windows; never -O0 because timings change too much,
    # and never -O3 LTO because that can elide our race witnesses. Use -O2.
    target_compile_options(${exe_name} PRIVATE -O2 -g)
    if(ENABLE_TSAN)
        target_compile_options(${exe_name} PRIVATE -fsanitize=thread -fno-omit-frame-pointer)
        target_link_options(${exe_name} PRIVATE -fsanitize=thread)
    endif()
    add_test(NAME concurrency_${exe_name} COMMAND ${exe_name})
    # TSan runs are slower; give the sharded/fill-stream test more headroom.
    set_tests_properties(concurrency_${exe_name} PROPERTIES TIMEOUT 120)
    add_dependencies(concurrency_tests ${exe_name})
endforeach()

# -----------------------------------------------------------------------------
# REAL MmOrderMover: per-symbol isolation + boot-mode banner (2026-07-12)
# -----------------------------------------------------------------------------
# Unlike the isolated models above, this one links the REAL production singleton
# (src/strategy/MmOrderMover.cpp) plus the real Config + Logger so it exercises
# the actual shardingEnabled() default and the boot banner. config_lib/utils_lib
# are base modules (no strategy/api deps), so the link stays light.
add_executable(test_mover_per_symbol_banner
    concurrency/test_mover_per_symbol_banner.cpp
    ${CMAKE_SOURCE_DIR}/src/strategy/MmOrderMover.cpp)
target_include_directories(test_mover_per_symbol_banner PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/concurrency
    ${CMAKE_SOURCE_DIR}/include)
target_link_libraries(test_mover_per_symbol_banner PRIVATE
    config_lib utils_lib Threads::Threads)
target_compile_features(test_mover_per_symbol_banner PRIVATE cxx_std_20)
target_compile_options(test_mover_per_symbol_banner PRIVATE -O2 -g)
if(ENABLE_TSAN)
    target_compile_options(test_mover_per_symbol_banner PRIVATE -fsanitize=thread -fno-omit-frame-pointer)
    target_link_options(test_mover_per_symbol_banner PRIVATE -fsanitize=thread)
endif()
add_test(NAME concurrency_test_mover_per_symbol_banner COMMAND test_mover_per_symbol_banner)
set_tests_properties(concurrency_test_mover_per_symbol_banner PROPERTIES TIMEOUT 60)
add_dependencies(concurrency_tests test_mover_per_symbol_banner)

# -----------------------------------------------------------------------------
# PositionBook — component unit/concurrency tests + shadow-first wiring contracts
# -----------------------------------------------------------------------------
# PositionBook.h is a standalone, STL-only production header, so this test
# includes the REAL component (not a model) and needs the project include dir.
# The wiring-contract cases model the off/shadow/enforce mode-guard semantics
# from MakeMarketStrategy.cpp in isolation (the repo's established test idiom).
add_executable(test_position_book
    concurrency/test_position_book.cpp)
target_include_directories(test_position_book PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/concurrency
    ${CMAKE_SOURCE_DIR}/include)
target_link_libraries(test_position_book PRIVATE Threads::Threads)
target_compile_features(test_position_book PRIVATE cxx_std_20)
target_compile_options(test_position_book PRIVATE -O2 -g)
if(ENABLE_TSAN)
    target_compile_options(test_position_book PRIVATE -fsanitize=thread -fno-omit-frame-pointer)
    target_link_options(test_position_book PRIVATE -fsanitize=thread)
endif()
add_test(NAME concurrency_test_position_book COMMAND test_position_book)
set_tests_properties(concurrency_test_position_book PROPERTIES TIMEOUT 120)
add_dependencies(concurrency_tests test_position_book)

# -----------------------------------------------------------------------------
# Enforce mode: ZERO venue reads on the mover thread (2026-07)
# -----------------------------------------------------------------------------
# Includes the REAL standalone headers core/MmJobVenueCalls.h (per-job venue
# counter) and strategy/VenueOrdersCache.h (main-loop-populated cache), and
# models the mover-job / refresher / mismatch semantics from MakeMarketStrategy
# in isolation. Header-only under test -> just needs the project include dir.
add_executable(test_enforce_zero_venue_reads
    concurrency/test_enforce_zero_venue_reads.cpp)
target_include_directories(test_enforce_zero_venue_reads PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/concurrency
    ${CMAKE_SOURCE_DIR}/include)
target_link_libraries(test_enforce_zero_venue_reads PRIVATE Threads::Threads)
target_compile_features(test_enforce_zero_venue_reads PRIVATE cxx_std_20)

# -----------------------------------------------------------------------------
# Combined cancel-replace: wire cancel fires EXACTLY ONCE per leg (2026-07-13)
# -----------------------------------------------------------------------------
# Models Platform::dispatchCancelResponses + the ORDER_CANCELLED wire-cancel
# handler using the REAL per-job counter header core/MmJobVenueCalls.h — proving
# a combined pair job reports cancels=2 (not the pre-fix 4). Needs the project
# include dir for the header (same as test_enforce_zero_venue_reads).
add_executable(test_combined_cancel_no_double_fire
    concurrency/test_combined_cancel_no_double_fire.cpp)
target_include_directories(test_combined_cancel_no_double_fire PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/concurrency
    ${CMAKE_SOURCE_DIR}/include)
target_link_libraries(test_combined_cancel_no_double_fire PRIVATE Threads::Threads)
target_compile_features(test_combined_cancel_no_double_fire PRIVATE cxx_std_20)
target_compile_options(test_combined_cancel_no_double_fire PRIVATE -O2 -g)
if(ENABLE_TSAN)
    target_compile_options(test_combined_cancel_no_double_fire PRIVATE -fsanitize=thread -fno-omit-frame-pointer)
    target_link_options(test_combined_cancel_no_double_fire PRIVATE -fsanitize=thread)
endif()
add_test(NAME concurrency_test_combined_cancel_no_double_fire COMMAND test_combined_cancel_no_double_fire)
set_tests_properties(concurrency_test_combined_cancel_no_double_fire PROPERTIES TIMEOUT 60)
add_dependencies(concurrency_tests test_combined_cancel_no_double_fire)
target_compile_options(test_enforce_zero_venue_reads PRIVATE -O2 -g)
if(ENABLE_TSAN)
    target_compile_options(test_enforce_zero_venue_reads PRIVATE -fsanitize=thread -fno-omit-frame-pointer)
    target_link_options(test_enforce_zero_venue_reads PRIVATE -fsanitize=thread)
endif()
add_test(NAME concurrency_test_enforce_zero_venue_reads COMMAND test_enforce_zero_venue_reads)
set_tests_properties(concurrency_test_enforce_zero_venue_reads PROPERTIES TIMEOUT 120)
add_dependencies(concurrency_tests test_enforce_zero_venue_reads)

# -----------------------------------------------------------------------------
# FastPoller — continuous ms fills+positions poller (Pillar C, 2026-07-12)
# -----------------------------------------------------------------------------
# Links the REAL production component (src/core/FastPoller.cpp), which is
# deliberately decoupled from RestClient (fetch/push callbacks), so the test
# drives it with a mock venue. Proves back-to-back cadence, interleave, FIFO
# handoff, the rate governor (429 backoff + recovery), respawn-then-fallback,
# and saturation isolation (run under -DENABLE_TSAN=ON for Tests §6).
add_executable(test_fast_poller
    concurrency/test_fast_poller.cpp
    ${CMAKE_SOURCE_DIR}/src/core/FastPoller.cpp)
target_include_directories(test_fast_poller PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/concurrency
    ${CMAKE_SOURCE_DIR}/include)
target_link_libraries(test_fast_poller PRIVATE Threads::Threads)
target_compile_features(test_fast_poller PRIVATE cxx_std_20)
target_compile_options(test_fast_poller PRIVATE -O2 -g)
if(ENABLE_TSAN)
    target_compile_options(test_fast_poller PRIVATE -fsanitize=thread -fno-omit-frame-pointer)
    target_link_options(test_fast_poller PRIVATE -fsanitize=thread)
endif()
add_test(NAME concurrency_test_fast_poller COMMAND test_fast_poller)
set_tests_properties(concurrency_test_fast_poller PROPERTIES TIMEOUT 120)
add_dependencies(concurrency_tests test_fast_poller)

# -----------------------------------------------------------------------------
# Product placement lease quiet-gate bypass (2026-07-12)
# -----------------------------------------------------------------------------
# Exercises the pure decision predicates in include/strategy/MmProductLeasePolicy.h
# that production (mmAcquireProductPlacementLease /
# mmTryAtomicClaimProductPlacementLeaseLocked) now routes through: fire-and-track
# grants while its own legs are CANCEL_SENT_UNCONFIRMED; a different owner still
# denies; off/shadow gate byte-identical; non-mover callers never get the bypass.
add_executable(test_product_lease_bypass
    concurrency/test_product_lease_bypass.cpp)
target_include_directories(test_product_lease_bypass PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/concurrency
    ${CMAKE_SOURCE_DIR}/include)
target_link_libraries(test_product_lease_bypass PRIVATE Threads::Threads)
target_compile_features(test_product_lease_bypass PRIVATE cxx_std_20)
target_compile_options(test_product_lease_bypass PRIVATE -O2 -g)
if(ENABLE_TSAN)
    target_compile_options(test_product_lease_bypass PRIVATE -fsanitize=thread -fno-omit-frame-pointer)
    target_link_options(test_product_lease_bypass PRIVATE -fsanitize=thread)
endif()
add_test(NAME concurrency_test_product_lease_bypass COMMAND test_product_lease_bypass)
set_tests_properties(concurrency_test_product_lease_bypass PROPERTIES TIMEOUT 120)
add_dependencies(concurrency_tests test_product_lease_bypass)

# =============================================================================
# Tests
# =============================================================================
# Self-contained logic tests. These deliberately do NOT link the platform
# (core_lib); they exercise pure, dependency-free logic headers directly so they
# build/run fast and stay decoupled from the full trading engine.

# --- MWR (Moving Window Range) volatility breaker ---------------------------
add_executable(mwr_breaker_test mwr_breaker_test.cpp)
target_include_directories(mwr_breaker_test PRIVATE ${CMAKE_SOURCE_DIR}/include)
set_target_properties(mwr_breaker_test PROPERTIES
    CXX_STANDARD 20
    CXX_STANDARD_REQUIRED ON
    # The breaker's debug owner-thread assert is part of what test 6 documents;
    # keep this TU's own LTO off so the tiny test links quickly regardless of the
    # top-level Release/-flto setting.
    INTERPROCEDURAL_OPTIMIZATION OFF)
find_package(Threads REQUIRED)
target_link_libraries(mwr_breaker_test PRIVATE Threads::Threads)

add_test(NAME mwr_breaker_test COMMAND mwr_breaker_test)

# -----------------------------------------------------------------------------
# Desk-side Python integrity suites (registered in ctest, 2026-07-23)
# -----------------------------------------------------------------------------
# Dependency-free stdlib scripts (each has its own main() returning non-zero on
# failure). Registering them here stops them rotting outside the suite:
#   * orders.json write integrity (gen-CAS / non-clobber / mass-shrink guard)
#   * default_config.json write integrity (atomic RMW / key-drop guard)  [Phase 1]
#   * per-instrument cancel-all + HOLD (D5 ordering / retry / NOT CLEAN)  [Phase 4]
find_program(PYTHON3_EXECUTABLE NAMES python3 python)
if(PYTHON3_EXECUTABLE)
    foreach(py_test
            test_orders_json_integrity
            test_default_config_integrity
            test_instrument_cancel_all
            test_fast_market_config)
        add_test(NAME desk_python_${py_test}
                 COMMAND ${PYTHON3_EXECUTABLE}
                         ${CMAKE_SOURCE_DIR}/tools/${py_test}.py
                 WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})
        set_tests_properties(desk_python_${py_test} PROPERTIES TIMEOUT 120)
    endforeach()
else()
    message(WARNING "python3 not found — desk Python integrity tests NOT registered in ctest")
endif()
