# MarketData Module CMakeLists.txt

add_library(marketdata_lib STATIC
    MarketDataManager.cpp
    ExternalFeedManager.cpp
    NeonFixFeed.cpp
)

target_include_directories(marketdata_lib PUBLIC
    ${PLATFORM_INCLUDE_DIR}
    ${asio_SOURCE_DIR}/asio/include
    ${websocketpp_SOURCE_DIR}
)

target_compile_definitions(marketdata_lib PUBLIC
    ASIO_STANDALONE
)

target_compile_options(marketdata_lib PRIVATE
    $<$<OR:$<CXX_COMPILER_ID:AppleClang>,$<CXX_COMPILER_ID:Clang>>:-Wno-null-pointer-subtraction>
)

target_link_libraries(marketdata_lib PUBLIC
    config_lib
    events_lib
    orders_lib
    api_lib
    utils_lib
    nlohmann_json::nlohmann_json
    Threads::Threads
    OpenSSL::SSL
    OpenSSL::Crypto
)
