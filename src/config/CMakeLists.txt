# Config Module CMakeLists.txt

add_library(config_lib STATIC
    Config.cpp
    AxGatewayInstruments.cpp
)

target_include_directories(config_lib PUBLIC
    ${PLATFORM_INCLUDE_DIR}
)

target_link_libraries(config_lib PUBLIC
    nlohmann_json::nlohmann_json
)
