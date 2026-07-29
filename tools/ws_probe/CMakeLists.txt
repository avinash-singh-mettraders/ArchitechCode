# =============================================================================
# ws_probe — standalone AX WebSocket order-entry probe.
#
# Deliberately NOT linked into trading_client / core_lib / any engine target.
# It reuses only the vendored networking libraries (websocketpp + standalone
# Asio + OpenSSL, libcurl, nlohmann_json) that the top-level build already
# fetched — it does not depend on any src/ module.
# =============================================================================

add_executable(ws_probe
    main.cpp
    probe.cpp
    live_ws.cpp
    rest.cpp
)

target_include_directories(ws_probe PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${asio_SOURCE_DIR}/asio/include
    ${websocketpp_SOURCE_DIR}
)

target_compile_definitions(ws_probe PRIVATE
    ASIO_STANDALONE
)

target_compile_features(ws_probe PRIVATE cxx_std_20)

# websocketpp 0.8.2 does pointer arithmetic that trips this Clang warning; the
# repo already suppresses it for marketdata_lib for the same reason.
target_compile_options(ws_probe PRIVATE
    $<$<OR:$<CXX_COMPILER_ID:AppleClang>,$<CXX_COMPILER_ID:Clang>>:-Wno-null-pointer-subtraction>
)

target_link_libraries(ws_probe PRIVATE
    nlohmann_json::nlohmann_json
    CURL::libcurl
    OpenSSL::SSL
    OpenSSL::Crypto
    Threads::Threads
)

# Dry-run self-tests run in ctest (only when the test suite is enabled). The
# live modes are NEVER invoked from ctest — they require --yes + real env vars.
if(BUILD_TESTS)
    add_test(NAME ws_probe_dry_run COMMAND ws_probe --dry-run)
    set_tests_properties(ws_probe_dry_run PROPERTIES TIMEOUT 120)
endif()
