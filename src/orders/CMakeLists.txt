# Orders Module CMakeLists.txt

add_library(orders_lib STATIC
    Order.cpp
    OrderManager.cpp
    OrderBook.cpp
)

target_include_directories(orders_lib PUBLIC
    ${PLATFORM_INCLUDE_DIR}
)

target_link_libraries(orders_lib PUBLIC
    events_lib
    Threads::Threads
)
