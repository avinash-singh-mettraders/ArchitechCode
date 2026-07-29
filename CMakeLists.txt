cmake_minimum_required(VERSION 3.20)
project(ArchitectPlatformCore VERSION 1.0.0 LANGUAGES CXX)

# =============================================================================
# Build Configuration
# =============================================================================
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Build type configuration
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE)
endif()

# =============================================================================
# Output Directories Configuration
# =============================================================================
# Binary output directory: bin/ax_bin (parallel to src)
set(AX_BIN_DIR "${CMAKE_SOURCE_DIR}/bin/ax_bin")

# Create directories if they don't exist
file(MAKE_DIRECTORY "${CMAKE_SOURCE_DIR}/bin/make")
file(MAKE_DIRECTORY "${AX_BIN_DIR}")
file(MAKE_DIRECTORY "${AX_BIN_DIR}/lib")

# Set output directories for all targets
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "${AX_BIN_DIR}")
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY "${AX_BIN_DIR}/lib")
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY "${AX_BIN_DIR}/lib")

# Also set per-configuration directories (for multi-config generators like Xcode)
foreach(CONFIG_TYPE ${CMAKE_CONFIGURATION_TYPES})
    string(TOUPPER ${CONFIG_TYPE} CONFIG_TYPE_UPPER)
    set(CMAKE_RUNTIME_OUTPUT_DIRECTORY_${CONFIG_TYPE_UPPER} "${AX_BIN_DIR}")
    set(CMAKE_LIBRARY_OUTPUT_DIRECTORY_${CONFIG_TYPE_UPPER} "${AX_BIN_DIR}/lib")
    set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY_${CONFIG_TYPE_UPPER} "${AX_BIN_DIR}/lib")
endforeach()

# =============================================================================
# Compiler Flags for Ultra-Fast Performance
# =============================================================================
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -Wpedantic")
set(CMAKE_CXX_FLAGS_DEBUG "-g -O0 -DDEBUG")
set(CMAKE_CXX_FLAGS_RELEASE "-O3 -DNDEBUG -march=native -mtune=native -flto")
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "-O2 -g -DNDEBUG")

# Enable link-time optimization for release builds
if(CMAKE_BUILD_TYPE STREQUAL "Release")
    set(CMAKE_INTERPROCEDURAL_OPTIMIZATION TRUE)
endif()

# =============================================================================
# Options
# =============================================================================
option(BUILD_TESTS "Build unit tests" OFF)
option(BUILD_EXAMPLES "Build example applications" ON)
option(ENABLE_SANITIZERS "Enable address and undefined behavior sanitizers" OFF)
option(USE_SYSTEM_LIBS "Use system-installed libraries instead of fetching" OFF)

if(ENABLE_SANITIZERS)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address,undefined")
    set(CMAKE_LINKER_FLAGS "${CMAKE_LINKER_FLAGS} -fsanitize=address,undefined")
endif()

# =============================================================================
# Dependencies
# =============================================================================
include(FetchContent)

# nlohmann/json for JSON parsing
FetchContent_Declare(
    json
    GIT_REPOSITORY https://github.com/nlohmann/json.git
    GIT_TAG v3.11.3
)

# fmt for string formatting (used by core and other modules).
# fmt 10.x fails to compile its own sources with Apple Clang + C++20 (consteval / FMT_STRING);
# pin fmt 9.1 for the fmt::fmt target.
FetchContent_Declare(
    fmt
    GIT_REPOSITORY https://github.com/fmtlib/fmt.git
    GIT_TAG 9.1.0
)

# spdlog: link against the same fmt 9.x as above (bundled fmt in spdlog is fmt 10.x and hits
# Apple Clang + C++20 consteval issues). SPDLOG_USE_STD_FORMAT is off: libc++ often lacks
# __cpp_lib_format >= 202207L, so spdlog would fall back to string_view as the "format string" type.
FetchContent_Declare(
    spdlog
    GIT_REPOSITORY https://github.com/gabime/spdlog.git
    GIT_TAG v1.12.0
)

# websocketpp + standalone Asio for external WSS feeds.
FetchContent_Declare(
    asio
    GIT_REPOSITORY https://github.com/chriskohlhoff/asio.git
    GIT_TAG asio-1-30-2
)
FetchContent_Declare(
    websocketpp
    GIT_REPOSITORY https://github.com/zaphoyd/websocketpp.git
    GIT_TAG 0.8.2
)

set(SPDLOG_USE_STD_FORMAT OFF CACHE BOOL "" FORCE)
set(SPDLOG_FMT_EXTERNAL ON CACHE BOOL "spdlog: use FetchContent fmt (9.x)" FORCE)

FetchContent_MakeAvailable(json fmt spdlog asio)

# websocketpp is header-only; populate sources without adding its legacy CMake project.
FetchContent_GetProperties(websocketpp)
if(NOT websocketpp_POPULATED)
    FetchContent_Populate(websocketpp)
endif()

# -----------------------------------------------------------------------------
# Patch websocketpp 0.8.2 headers for GCC 13+ / clang-16+ (CWG 1602).
# -----------------------------------------------------------------------------
# zaphoyd/websocketpp has been unmaintained since 2020. Its 0.8.2 ctor/dtor
# declarations use template-id-in-injected-class-name syntax such as
#   ~endpoint<connection,config>() {}
#   basic<concurrency,names>(level c, std::ostream * out)
# That has always been non-standard (see CWG 1602) and GCC 13+ now errors
# instead of warning, breaking the build on modern Ubuntu / WSL toolchains.
# We can't upgrade the package (no newer release exists), so we patch the two
# offending headers in-place. The replacements are idempotent — a second
# configure run finds nothing to change and is a no-op.
#
# Touches:
#   _deps/websocketpp-src/websocketpp/endpoint.hpp        (line 112: ~endpoint)
#   _deps/websocketpp-src/websocketpp/logger/basic.hpp    (lines 61..100: ctors + dtor)
function(_patch_websocketpp_header path)
    if(NOT EXISTS "${path}")
        return()
    endif()
    file(READ "${path}" _orig)
    set(_patched "${_orig}")
    # Order matters: handle the "~basic<concurrency,names>(" dtor variant first
    # via the broader substring rule below — string(REPLACE) is whole-substring,
    # not regex, and "basic<concurrency,names>(" is a strict suffix of the dtor
    # form so one replace fixes both. Same trick for endpoint (dtor only).
    string(REPLACE "endpoint<connection,config>("  "endpoint("  _patched "${_patched}")
    string(REPLACE "basic<concurrency,names>("    "basic("     _patched "${_patched}")
    if(NOT "${_patched}" STREQUAL "${_orig}")
        file(WRITE "${path}" "${_patched}")
        message(STATUS "Patched websocketpp header for GCC 13+/clang-16+: ${path}")
    endif()
endfunction()

_patch_websocketpp_header("${websocketpp_SOURCE_DIR}/websocketpp/endpoint.hpp")
_patch_websocketpp_header("${websocketpp_SOURCE_DIR}/websocketpp/logger/basic.hpp")

# Apple Clang + C++20: fmt still sets FMT_HAS_CONSTEVAL via the __cpp_consteval branch while
# consteval format-string parsing remains broken; predefine empty FMT_CONSTEVAL so core.h
# skips that block (no FMT_HAS_CONSTEVAL) and basic_format_string stays constexpr-safe.
if(TARGET fmt)
    target_compile_definitions(fmt PUBLIC "FMT_CONSTEVAL=")
    # fmt 9.x `operator"" _a` triggers -Wdeprecated-literal-operator on Apple Clang when TUs
    # include format.h (directly or via spdlog). INTERFACE so dependents inherit the suppression.
    target_compile_options(
        fmt INTERFACE
        $<$<OR:$<CXX_COMPILER_ID:AppleClang>,$<CXX_COMPILER_ID:Clang>>:-Wno-deprecated-literal-operator>)
endif()

# Find required system libraries
find_package(Threads REQUIRED)
find_package(CURL REQUIRED)
find_package(OpenSSL REQUIRED)

# =============================================================================
# Include directories
# =============================================================================
set(PLATFORM_INCLUDE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/include)

# =============================================================================
# Add subdirectories (modules) - Order matters for dependencies!
# =============================================================================
# Base modules (no internal dependencies)
add_subdirectory(src/utils)
add_subdirectory(src/config)
add_subdirectory(src/events)

# Mid-level modules
add_subdirectory(src/orders)
add_subdirectory(src/user)
add_subdirectory(src/api)

# High-level modules
add_subdirectory(src/marketdata)
add_subdirectory(src/portfolio)

# Strategy module (predictors, strategies, timer system)
add_subdirectory(src/strategy)

# Core/Platform (depends on all others)
add_subdirectory(src/core)

# =============================================================================
# Main Library - combines all modules
# =============================================================================
add_library(platform_core INTERFACE)
target_link_libraries(platform_core INTERFACE
    core_lib
)

# =============================================================================
# Examples
# =============================================================================
if(BUILD_EXAMPLES)
    add_subdirectory(examples)
endif()

# =============================================================================
# Tests
# =============================================================================
if(BUILD_TESTS)
    enable_testing()
    add_subdirectory(tests)
endif()

# =============================================================================
# Standalone tools (NOT linked into trading_client / core_lib)
# =============================================================================
# AX WebSocket order-entry probe. Independent binary; reuses only the vendored
# networking libs. Its --dry-run self-test registers into ctest when BUILD_TESTS.
option(BUILD_WS_PROBE "Build the standalone AX WS order-entry probe" ON)
if(BUILD_WS_PROBE)
    add_subdirectory(tools/ws_probe)
endif()

# =============================================================================
# Installation
# =============================================================================
install(DIRECTORY include/ DESTINATION include)
# Note: Installation of INTERFACE libraries doesn't require EXPORT
install(TARGETS platform_core
    LIBRARY DESTINATION lib
    ARCHIVE DESTINATION lib
    RUNTIME DESTINATION bin
    INCLUDES DESTINATION include
)

# =============================================================================
# Package Configuration
# =============================================================================
include(CMakePackageConfigHelpers)
write_basic_package_version_file(
    "${CMAKE_CURRENT_BINARY_DIR}/platform_core-config-version.cmake"
    VERSION ${PROJECT_VERSION}
    COMPATIBILITY SameMajorVersion
)

# Note: Export is disabled for now as it requires all linked libraries to be exported
# export(EXPORT platform_core-targets
#     FILE "${CMAKE_CURRENT_BINARY_DIR}/platform_core-targets.cmake"
#     NAMESPACE platform_core::
# )

message(STATUS "===========================================")
message(STATUS "Architect Platform Core Configuration")
message(STATUS "===========================================")
message(STATUS "Build type: ${CMAKE_BUILD_TYPE}")
message(STATUS "C++ Standard: ${CMAKE_CXX_STANDARD}")
message(STATUS "Build tests: ${BUILD_TESTS}")
message(STATUS "Build examples: ${BUILD_EXAMPLES}")
message(STATUS "-------------------------------------------")
message(STATUS "Output Directories:")
message(STATUS "  Binaries: ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}")
message(STATUS "  Libraries: ${CMAKE_LIBRARY_OUTPUT_DIRECTORY}")
message(STATUS "  Archives: ${CMAKE_ARCHIVE_OUTPUT_DIRECTORY}")
message(STATUS "-------------------------------------------")
message(STATUS "Build instructions:")
message(STATUS "  cd bin/make && cmake ../.. && make -j16")
message(STATUS "===========================================")
