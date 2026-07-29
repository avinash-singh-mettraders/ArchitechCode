# Portfolio Module CMakeLists.txt

add_library(portfolio_lib STATIC
    PortfolioManager.cpp
)

target_include_directories(portfolio_lib PUBLIC
    ${PLATFORM_INCLUDE_DIR}
)

target_link_libraries(portfolio_lib PUBLIC
    events_lib
    orders_lib
    api_lib
    marketdata_lib
    Threads::Threads
)
