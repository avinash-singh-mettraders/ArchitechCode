# Core Module CMakeLists.txt

add_library(core_lib STATIC
    Platform.cpp
    StartupSequence.cpp
    FastPoller.cpp
)

target_include_directories(core_lib PUBLIC
    ${PLATFORM_INCLUDE_DIR}
)

target_link_libraries(core_lib PUBLIC
    nlohmann_json::nlohmann_json
    fmt::fmt
    config_lib
    utils_lib
    events_lib
    orders_lib
    user_lib
    api_lib
    marketdata_lib
    portfolio_lib
    strategy_lib
    Threads::Threads
)
