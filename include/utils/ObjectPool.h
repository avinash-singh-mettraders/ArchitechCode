#pragma once

/**
 * @file ObjectPool.h
 * @brief Lock-free object pool for high-performance allocation
 * 
 * Provides O(1) allocation and deallocation with zero heap allocation
 * after initial pool creation.
 */

#include <array>
#include <atomic>
#include <memory>
#include <cstdint>
#include <new>
#include <type_traits>

namespace architect {
namespace utils {

/**
 * @brief Fixed-size lock-free object pool
 * 
 * Uses a free list with atomic operations for thread-safe allocation.
 * Objects are stored in contiguous memory for cache efficiency.
 * 
 * @tparam T Object type (must be default constructible)
 * @tparam POOL_SIZE Maximum number of objects
 */
template<typename T, std::size_t POOL_SIZE = 4096>
class ObjectPool {
    static_assert(std::is_default_constructible_v<T>, "T must be default constructible");
    
    struct alignas(64) Node {  // Cache-line aligned
        T object;
        std::atomic<Node*> next;
        bool in_use;
        
        Node() : next(nullptr), in_use(false) {}
    };
    
public:
    ObjectPool() : free_list_(nullptr), allocated_(0), high_water_mark_(0) {
        // Initialize free list
        for (std::size_t i = 0; i < POOL_SIZE; ++i) {
            nodes_[i].next.store(&nodes_[(i + 1) % POOL_SIZE], std::memory_order_relaxed);
            nodes_[i].in_use = false;
        }
        nodes_[POOL_SIZE - 1].next.store(nullptr, std::memory_order_relaxed);
        free_list_.store(&nodes_[0], std::memory_order_release);
    }
    
    ~ObjectPool() = default;
    
    // Non-copyable, non-movable
    ObjectPool(const ObjectPool&) = delete;
    ObjectPool& operator=(const ObjectPool&) = delete;
    ObjectPool(ObjectPool&&) = delete;
    ObjectPool& operator=(ObjectPool&&) = delete;
    
    /**
     * @brief Allocate an object from the pool
     * @return Pointer to object, or nullptr if pool exhausted
     */
    [[nodiscard]] T* allocate() noexcept {
        Node* node = nullptr;
        Node* next = nullptr;
        
        do {
            node = free_list_.load(std::memory_order_acquire);
            if (!node) {
                return nullptr;  // Pool exhausted
            }
            next = node->next.load(std::memory_order_relaxed);
        } while (!free_list_.compare_exchange_weak(node, next,
                    std::memory_order_release, std::memory_order_relaxed));
        
        node->in_use = true;
        std::size_t current = ++allocated_;
        
        // Update high water mark
        std::size_t hwm = high_water_mark_.load(std::memory_order_relaxed);
        while (current > hwm && 
               !high_water_mark_.compare_exchange_weak(hwm, current,
                    std::memory_order_relaxed, std::memory_order_relaxed)) {
        }
        
        return &node->object;
    }
    
    /**
     * @brief Return an object to the pool
     * @param ptr Pointer previously returned by allocate()
     */
    void deallocate(T* ptr) noexcept {
        if (!ptr) return;
        
        // Find the node containing this object
        Node* node = reinterpret_cast<Node*>(
            reinterpret_cast<char*>(ptr) - offsetof(Node, object)
        );
        
        // Verify it's from this pool
        if (node < &nodes_[0] || node >= &nodes_[POOL_SIZE]) {
            return;  // Not from this pool
        }
        
        node->in_use = false;
        --allocated_;
        
        // Push onto free list
        Node* old_head = nullptr;
        do {
            old_head = free_list_.load(std::memory_order_acquire);
            node->next.store(old_head, std::memory_order_relaxed);
        } while (!free_list_.compare_exchange_weak(old_head, node,
                    std::memory_order_release, std::memory_order_relaxed));
    }
    
    /**
     * @brief Construct object in-place with arguments
     */
    template<typename... Args>
    [[nodiscard]] T* construct(Args&&... args) {
        T* ptr = allocate();
        if (ptr) {
            new (ptr) T(std::forward<Args>(args)...);
        }
        return ptr;
    }
    
    /**
     * @brief Destroy object and return to pool
     */
    void destroy(T* ptr) noexcept {
        if (ptr) {
            ptr->~T();
            deallocate(ptr);
        }
    }
    
    // === Statistics ===
    
    [[nodiscard]] std::size_t size() const noexcept { 
        return allocated_.load(std::memory_order_relaxed); 
    }
    
    [[nodiscard]] std::size_t capacity() const noexcept { 
        return POOL_SIZE; 
    }
    
    [[nodiscard]] std::size_t available() const noexcept { 
        return POOL_SIZE - allocated_.load(std::memory_order_relaxed); 
    }
    
    [[nodiscard]] std::size_t highWaterMark() const noexcept { 
        return high_water_mark_.load(std::memory_order_relaxed); 
    }
    
    [[nodiscard]] bool empty() const noexcept { 
        return allocated_.load(std::memory_order_relaxed) == 0; 
    }
    
    [[nodiscard]] bool full() const noexcept { 
        return allocated_.load(std::memory_order_relaxed) >= POOL_SIZE; 
    }
    
private:
    std::array<Node, POOL_SIZE> nodes_;
    std::atomic<Node*> free_list_;
    std::atomic<std::size_t> allocated_;
    std::atomic<std::size_t> high_water_mark_;
};

/**
 * @brief Smart pointer that returns object to pool on destruction
 */
template<typename T, std::size_t POOL_SIZE>
class PoolPtr {
public:
    using Pool = ObjectPool<T, POOL_SIZE>;
    
    PoolPtr() noexcept : ptr_(nullptr), pool_(nullptr) {}
    PoolPtr(T* ptr, Pool* pool) noexcept : ptr_(ptr), pool_(pool) {}
    
    ~PoolPtr() {
        reset();
    }
    
    // Move only
    PoolPtr(const PoolPtr&) = delete;
    PoolPtr& operator=(const PoolPtr&) = delete;
    
    PoolPtr(PoolPtr&& other) noexcept : ptr_(other.ptr_), pool_(other.pool_) {
        other.ptr_ = nullptr;
        other.pool_ = nullptr;
    }
    
    PoolPtr& operator=(PoolPtr&& other) noexcept {
        if (this != &other) {
            reset();
            ptr_ = other.ptr_;
            pool_ = other.pool_;
            other.ptr_ = nullptr;
            other.pool_ = nullptr;
        }
        return *this;
    }
    
    void reset() noexcept {
        if (ptr_ && pool_) {
            pool_->destroy(ptr_);
            ptr_ = nullptr;
        }
    }
    
    [[nodiscard]] T* get() const noexcept { return ptr_; }
    [[nodiscard]] T* operator->() const noexcept { return ptr_; }
    [[nodiscard]] T& operator*() const noexcept { return *ptr_; }
    [[nodiscard]] explicit operator bool() const noexcept { return ptr_ != nullptr; }
    
    T* release() noexcept {
        T* tmp = ptr_;
        ptr_ = nullptr;
        pool_ = nullptr;
        return tmp;
    }
    
private:
    T* ptr_;
    Pool* pool_;
};

/**
 * @brief Ring buffer for fixed-size queue operations
 * 
 * Single-producer single-consumer lock-free queue.
 */
template<typename T, std::size_t SIZE>
class RingBuffer {
    static_assert((SIZE & (SIZE - 1)) == 0, "SIZE must be power of 2");
    
public:
    RingBuffer() : head_(0), tail_(0) {}
    
    [[nodiscard]] bool push(const T& item) noexcept {
        std::size_t head = head_.load(std::memory_order_relaxed);
        std::size_t next = (head + 1) & (SIZE - 1);
        
        if (next == tail_.load(std::memory_order_acquire)) {
            return false;  // Full
        }
        
        buffer_[head] = item;
        head_.store(next, std::memory_order_release);
        return true;
    }
    
    [[nodiscard]] bool push(T&& item) noexcept {
        std::size_t head = head_.load(std::memory_order_relaxed);
        std::size_t next = (head + 1) & (SIZE - 1);
        
        if (next == tail_.load(std::memory_order_acquire)) {
            return false;  // Full
        }
        
        buffer_[head] = std::move(item);
        head_.store(next, std::memory_order_release);
        return true;
    }
    
    [[nodiscard]] bool pop(T& item) noexcept {
        std::size_t tail = tail_.load(std::memory_order_relaxed);
        
        if (tail == head_.load(std::memory_order_acquire)) {
            return false;  // Empty
        }
        
        item = std::move(buffer_[tail]);
        tail_.store((tail + 1) & (SIZE - 1), std::memory_order_release);
        return true;
    }
    
    [[nodiscard]] bool empty() const noexcept {
        return head_.load(std::memory_order_relaxed) == tail_.load(std::memory_order_relaxed);
    }
    
    [[nodiscard]] std::size_t size() const noexcept {
        std::size_t head = head_.load(std::memory_order_relaxed);
        std::size_t tail = tail_.load(std::memory_order_relaxed);
        return (head - tail) & (SIZE - 1);
    }
    
    [[nodiscard]] std::size_t capacity() const noexcept {
        return SIZE - 1;
    }
    
private:
    alignas(64) std::array<T, SIZE> buffer_;
    alignas(64) std::atomic<std::size_t> head_;
    alignas(64) std::atomic<std::size_t> tail_;
};

} // namespace utils
} // namespace architect
