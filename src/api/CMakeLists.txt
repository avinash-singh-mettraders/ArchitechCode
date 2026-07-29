# API Module CMakeLists.txt

add_library(api_lib STATIC
    CurlMultiManager.cpp
    RestClient.cpp
    WebSocketClient.cpp
)

target_include_directories(api_lib PUBLIC
    ${PLATFORM_INCLUDE_DIR}
)

target_link_libraries(api_lib PUBLIC
    config_lib
    events_lib
    user_lib
    nlohmann_json::nlohmann_json
    CURL::libcurl
    OpenSSL::SSL
    OpenSSL::Crypto
    Threads::Threads
)
