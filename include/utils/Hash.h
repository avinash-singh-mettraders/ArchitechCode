#pragma once

/**
 * @file Hash.h
 * @brief High-performance hash functions for trading data structures
 * 
 * Provides specialized hash functions for:
 * - Fixed-size Symbol arrays
 * - Order IDs
 * - String views
 */

#include "core/Types.h"
#include <cstdint>
#include <cstring>
#include <string_view>
#include <functional>

namespace architect {
namespace utils {

/**
 * @brief FNV-1a hash implementation (fast, good distribution)
 */
struct FNV1aHash {
    static constexpr std::uint64_t FNV_OFFSET = 14695981039346656037ULL;
    static constexpr std::uint64_t FNV_PRIME = 1099511628211ULL;
    
    [[nodiscard]] static constexpr std::uint64_t hash(const char* data, std::size_t len) noexcept {
        std::uint64_t hash = FNV_OFFSET;
        for (std::size_t i = 0; i < len; ++i) {
            hash ^= static_cast<std::uint64_t>(static_cast<unsigned char>(data[i]));
            hash *= FNV_PRIME;
        }
        return hash;
    }
    
    [[nodiscard]] static std::uint64_t hash(std::string_view sv) noexcept {
        return hash(sv.data(), sv.size());
    }
};

/**
 * @brief Hash functor for Symbol (std::array<char, N>)
 */
struct SymbolHash {
    [[nodiscard]] std::size_t operator()(const core::Symbol& sym) const noexcept {
        // Hash only until null terminator for efficiency
        const char* data = sym.data();
        std::size_t len = 0;
        while (len < core::MAX_SYMBOL_LENGTH && data[len] != '\0') {
            ++len;
        }
        return FNV1aHash::hash(data, len);
    }
};

/**
 * @brief Equality functor for Symbol
 */
struct SymbolEqual {
    [[nodiscard]] bool operator()(const core::Symbol& a, const core::Symbol& b) const noexcept {
        // Compare as C strings (stop at null)
        return std::strcmp(a.data(), b.data()) == 0;
    }
};

/**
 * @brief Hash functor for string_view (avoids std::hash overhead)
 */
struct StringViewHash {
    [[nodiscard]] std::size_t operator()(std::string_view sv) const noexcept {
        return FNV1aHash::hash(sv);
    }
};

/**
 * @brief Simple identity hash for integer types (no hashing needed)
 */
template<typename T>
struct IdentityHash {
    [[nodiscard]] constexpr std::size_t operator()(T value) const noexcept {
        return static_cast<std::size_t>(value);
    }
};

/**
 * @brief Compile-time string hash (for switch statements on strings)
 */
[[nodiscard]] constexpr std::uint64_t constexpr_hash(const char* str) noexcept {
    std::uint64_t hash = FNV1aHash::FNV_OFFSET;
    while (*str) {
        hash ^= static_cast<std::uint64_t>(static_cast<unsigned char>(*str++));
        hash *= FNV1aHash::FNV_PRIME;
    }
    return hash;
}

/**
 * @brief User-defined literal for compile-time string hashing
 * Usage: switch(hash(str)) { case "BUY"_hash: ... }
 */
[[nodiscard]] constexpr std::uint64_t operator""_hash(const char* str, std::size_t) noexcept {
    return constexpr_hash(str);
}

/**
 * @brief Runtime string hash matching the compile-time version
 */
[[nodiscard]] inline std::uint64_t runtime_hash(const char* str) noexcept {
    return constexpr_hash(str);
}

[[nodiscard]] inline std::uint64_t runtime_hash(std::string_view sv) noexcept {
    return FNV1aHash::hash(sv);
}

} // namespace utils
} // namespace architect

// Specialization for std::hash to use with standard containers
namespace std {
    template<>
    struct hash<architect::core::Symbol> {
        [[nodiscard]] std::size_t operator()(const architect::core::Symbol& sym) const noexcept {
            return architect::utils::SymbolHash{}(sym);
        }
    };
}
