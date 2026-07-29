# Examples CMakeLists.txt

add_executable(trading_client main.cpp)

target_link_libraries(trading_client PRIVATE
    platform_core
)

target_include_directories(trading_client PRIVATE
    ${PLATFORM_INCLUDE_DIR}
)

# Copy config file to build directory
configure_file(
    ${CMAKE_SOURCE_DIR}/config/default_config.json
    ${CMAKE_CURRENT_BINARY_DIR}/config.json
    COPYONLY
)
