// Lightweight, dependency-free assertion macros for the concurrency tests.
// We deliberately avoid pulling in GoogleTest / Catch2 because none are
// already wired into the build and the user wants a fast push decision.
// Each test binary defines its own `int main()` and uses these macros.
#pragma once

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <string>

namespace tx {

inline std::atomic<int> g_failures{0};

inline void report_pass(const char* name) {
    std::printf("  [PASS] %s\n", name);
}

inline void report_fail(const char* file, int line, const char* expr, const std::string& detail) {
    std::printf("  [FAIL] %s:%d %s  (%s)\n", file, line, expr, detail.c_str());
    g_failures.fetch_add(1, std::memory_order_relaxed);
}

inline int finish(const char* suite_name) {
    const int n = g_failures.load(std::memory_order_relaxed);
    if (n == 0) {
        std::printf("[SUITE OK] %s — all checks passed\n", suite_name);
        return 0;
    }
    std::printf("[SUITE FAIL] %s — %d failure(s)\n", suite_name, n);
    return 1;
}

}  // namespace tx

#define TX_REQUIRE(expr)                                                                   \
    do {                                                                                   \
        if (!(expr)) {                                                                     \
            ::tx::report_fail(__FILE__, __LINE__, #expr, "expected true");                 \
            return;                                                                        \
        }                                                                                  \
    } while (0)

#define TX_EQ(actual, expected)                                                            \
    do {                                                                                   \
        const auto _a = (actual);                                                          \
        const auto _e = (expected);                                                        \
        if (!(_a == _e)) {                                                                 \
            ::tx::report_fail(__FILE__, __LINE__, #actual " == " #expected,                \
                              "actual=" + std::to_string(_a) +                             \
                                  " expected=" + std::to_string(_e));                      \
            return;                                                                        \
        }                                                                                  \
    } while (0)

#define TX_LE(actual, bound)                                                               \
    do {                                                                                   \
        const auto _a = (actual);                                                          \
        const auto _b = (bound);                                                           \
        if (!(_a <= _b)) {                                                                 \
            ::tx::report_fail(__FILE__, __LINE__, #actual " <= " #bound,                   \
                              "actual=" + std::to_string(_a) +                             \
                                  " bound=" + std::to_string(_b));                         \
            return;                                                                        \
        }                                                                                  \
    } while (0)

#define TX_RUN(fn)                                                                         \
    do {                                                                                   \
        std::printf("[RUN ] %s\n", #fn);                                                   \
        fn();                                                                              \
        ::tx::report_pass(#fn);                                                            \
    } while (0)
