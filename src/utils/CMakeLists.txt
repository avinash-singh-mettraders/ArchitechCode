# Utils Module CMakeLists.txt

add_library(utils_lib STATIC
    Logger.cpp
)

target_include_directories(utils_lib PUBLIC
    ${PLATFORM_INCLUDE_DIR}
)

target_link_libraries(utils_lib PUBLIC
    spdlog::spdlog
    Threads::Threads
)

# C++17 filesystem support
if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU" AND CMAKE_CXX_COMPILER_VERSION VERSION_LESS 9.0)
    target_link_libraries(utils_lib PUBLIC stdc++fs)
endif()
