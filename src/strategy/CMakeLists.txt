# Strategy module - Predictors, Strategies, and Timer System
add_library(strategy_lib STATIC
    Predictor.cpp
    Strategy.cpp
    TimerSystem.cpp
    mm_orders_reconcile_shared.cpp
    MakeMarketStrategy.cpp
    MmOrderMover.cpp
    FastMarketMonitor.cpp
)

target_include_directories(strategy_lib PUBLIC
    ${CMAKE_SOURCE_DIR}/include
    ${CMAKE_SOURCE_DIR}/include/strategy
    ${CMAKE_SOURCE_DIR}/include/core
    ${CMAKE_SOURCE_DIR}/include/events
    ${CMAKE_SOURCE_DIR}/include/orders
    ${CMAKE_SOURCE_DIR}/include/utils
)

target_link_libraries(strategy_lib PUBLIC
    config_lib
    events_lib
    orders_lib
    portfolio_lib
    utils_lib
    pthread
)

# Require C++20
target_compile_features(strategy_lib PUBLIC cxx_std_20)
