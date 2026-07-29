# Events Module CMakeLists.txt

add_library(events_lib STATIC
    EventManager.cpp
)

target_include_directories(events_lib PUBLIC
    ${PLATFORM_INCLUDE_DIR}
)

target_link_libraries(events_lib PUBLIC
    config_lib
    Threads::Threads
)
