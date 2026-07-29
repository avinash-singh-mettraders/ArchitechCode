# User Module CMakeLists.txt

add_library(user_lib STATIC
    UserManager.cpp
)

target_include_directories(user_lib PUBLIC
    ${PLATFORM_INCLUDE_DIR}
)

target_link_libraries(user_lib PUBLIC
    config_lib
    events_lib
    Threads::Threads
)
