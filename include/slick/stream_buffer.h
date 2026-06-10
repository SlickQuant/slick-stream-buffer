/********************************************************************************
 * Copyright (c) 2026 Slick Quant LLC
 * All rights reserved
 *
 * This file is part of the SlickStreamBuffer. Redistribution and use in source
 * and binary forms, with or without modification, are permitted exclusively
 * under the terms of the MIT license which is available at
 * https://github.com/SlickQuant/slick-stream-buffer/blob/main/LICENSE
 *
 ********************************************************************************/

#pragma once

// Prevent Windows min/max macros from conflicting with std::numeric_limits
// This must be defined BEFORE any Windows headers (including those from slick-shm)
#if defined(_MSC_VER) || defined(_WIN32)
#ifndef NOMINMAX
#define NOMINMAX
#endif
#endif

#include <slick/shm/shared_memory.hpp>

// Undef Windows min/max macros that slick-shm may have pulled in
#if defined(_WIN32) || defined(_MSC_VER)
#ifdef max
#undef max
#endif
#ifdef min
#undef min
#endif
#endif

#include <cstdint>
#include <cstddef>
#include <cstring>
#include <atomic>
#include <stdexcept>
#include <string>
#include <cassert>
#include <thread>
#include <chrono>
#include <limits>
#include <new>
#include <utility>

#if defined(_MSC_VER) && (defined(_M_IX86) || defined(_M_X64))
#include <immintrin.h>
#endif

#ifndef SLICK_STREAM_BUFFER_ENABLE_LOSS_DETECTION
#if !defined(NDEBUG)
#define SLICK_STREAM_BUFFER_ENABLE_LOSS_DETECTION 1
#else
#define SLICK_STREAM_BUFFER_ENABLE_LOSS_DETECTION 0
#endif
#endif

#ifndef SLICK_STREAM_BUFFER_ENABLE_CPU_RELAX
#define SLICK_STREAM_BUFFER_ENABLE_CPU_RELAX 1
#endif

namespace slick {

/**
 * @brief A lock-free single-producer multi-consumer byte stream buffer with optional shared memory support.
 *
 * The producer side exposes a beast::flat_buffer-like interface (prepare/commit/consume/data/size)
 * so that network bytes can be written directly into the ring (e.g. by boost::asio/boost::beast via
 * the dynamic_stream_buffer adapter). consume(n) does not discard bytes - it PUBLISHES them to
 * consumers as one discrete message record. Each consumer owns a monotonic cursor and reads whole
 * messages zero-copy as (pointer, length) pairs.
 *
 * Caveats:
 * - Producer methods (prepare/commit/consume/data/size/reset) must be called from a single thread.
 * - The queue is lossy: if the producer outruns a consumer by more than the control ring or data
 *   ring size, the consumer skips ahead and the loss is counted (see loss_count()). A consumer
 *   lapped mid-read may observe torn record fields or bytes - same caveat as SlickQueue.
 * - The data-lap validity check in read() is best-effort: bytes can still be overwritten after the
 *   check while the caller is using them, and records whose ring positions fall inside a wrap
 *   relocation gap may be reported lost even though their bytes were not overwritten.
 * - prepare() may relocate the committed-but-unconsumed region; pointers previously returned by
 *   data() or prepare() are invalidated (same as flat_buffer reallocation semantics).
 * - A single message (one consume() call) is limited to < 4 GiB.
 */
class SlickStreamBuffer {
    static constexpr uint64_t kInvalidSeq = std::numeric_limits<uint64_t>::max();

#if defined(__cpp_lib_hardware_interference_size)
    static constexpr std::size_t cacheline_size = std::hardware_destructive_interference_size;
#else
    static constexpr std::size_t cacheline_size = 64;
#endif

    struct record {
        std::atomic<uint64_t> seq{ kInvalidSeq };  // record sequence number, kInvalidSeq = unpublished
        uint64_t offset = 0;                       // monotonic data offset of the first byte
        uint32_t length = 0;                       // record length in bytes
        uint32_t pad0 = 0;
        uint64_t pad1 = 0;                         // pad to 32 bytes
    };
    static_assert(sizeof(record) == 32, "record must be 32 bytes");

    // geometry
    uint64_t capacity_ = 0;          // data ring size in bytes, power of 2
    uint64_t mask_ = 0;              // capacity_ - 1
    uint32_t control_size_ = 0;      // number of records in the control ring, power of 2
    uint32_t control_mask_ = 0;      // control_size_ - 1
    uint8_t* data_ = nullptr;
    record* control_ = nullptr;

    // shared cursors (point into the shm header, or at the *_local_ members below)
    std::atomic<uint64_t>* committed_ = nullptr;     // monotonic end of committed bytes
    std::atomic<uint64_t>* consumed_ = nullptr;      // monotonic publish boundary
    std::atomic<uint64_t>* next_seq_ = nullptr;      // next record sequence number
    std::atomic<uint64_t>* reserve_end_ = nullptr;   // monotonic high-water of the prepared region

    // producer-private
    uint64_t prepared_size_ = 0;     // size of the live prepare() region, 0 if none

    alignas(cacheline_size) std::atomic<uint64_t> committed_local_{ 0 };
    alignas(cacheline_size) std::atomic<uint64_t> consumed_local_{ 0 };
    alignas(cacheline_size) std::atomic<uint64_t> next_seq_local_{ 0 };
    alignas(cacheline_size) std::atomic<uint64_t> reserve_end_local_{ 0 };
#if SLICK_STREAM_BUFFER_ENABLE_LOSS_DETECTION
    alignas(cacheline_size) std::atomic<uint64_t> loss_count_{ 0 };
#endif
    bool own_ = false;
    bool use_shm_ = false;
    slick::shm::shared_memory shm_;   // RAII wrapper for shared memory
    void* lpvMem_ = nullptr;          // Cached data pointer
    std::string shm_name_;            // Stored for cleanup

    // Shared memory layout constants
    //
    // The shared memory segment is organized as follows:
    //
    // [HEADER: 64 bytes]
    //   Offset 0-7   (8 bytes):  std::atomic<uint64_t> committed_   - monotonic end of committed bytes
    //   Offset 8-15  (8 bytes):  std::atomic<uint64_t> consumed_    - monotonic publish boundary
    //   Offset 16-23 (8 bytes):  std::atomic<uint64_t> next_seq_    - next record sequence number
    //   Offset 24-31 (8 bytes):  std::atomic<uint64_t> reserve_end_ - prepared-region high-water mark
    //   Offset 32-39 (8 bytes):  capacity_ - data ring size in bytes (uint64_t)
    //   Offset 40-43 (4 bytes):  control_size_ - control ring record count (uint32_t)
    //   Offset 44-47 (4 bytes):  header_magic - layout/version marker
    //   Offset 48-51 (4 bytes):  init_state - atomic init state (0=uninit,2=init,3=ready)
    //   Offset 52-63 (12 bytes): PADDING - reserved for future use
    //
    // [CONTROL RING: sizeof(record) * control_size_]
    //
    // [DATA RING: capacity_ bytes]
    //
    static constexpr uint32_t HEADER_SIZE = 64;
    static constexpr uint32_t COMMITTED_OFFSET = 0;
    static constexpr uint32_t CONSUMED_OFFSET = 8;
    static constexpr uint32_t NEXT_SEQ_OFFSET = 16;
    static constexpr uint32_t RESERVE_END_OFFSET = 24;
    static constexpr uint32_t CAPACITY_OFFSET = 32;
    static constexpr uint32_t CONTROL_SIZE_OFFSET = 40;
    static constexpr uint32_t HEADER_MAGIC_OFFSET = 44;
    static constexpr uint32_t INIT_STATE_OFFSET = 48;
    static constexpr uint32_t HEADER_MAGIC = 0x53534231; // 'SSB1'
    static constexpr uint32_t INIT_STATE_UNINITIALIZED = 0;
    static constexpr uint32_t INIT_STATE_INITIALIZING = 2;
    static constexpr uint32_t INIT_STATE_READY = 3;

    static constexpr bool is_power_of_two(uint64_t value) noexcept {
        return value != 0 && ((value & (value - 1)) == 0);
    }

public:
    /**
     * @brief The published message record as consumers will see it, returned by consume().
     *
     * Evaluates to false when nothing was published (zero-length consume). The data pointer
     * stays valid until the producer laps this part of the ring.
     */
    struct published_record {
        uint64_t sequence = kInvalidSeq;  ///< record sequence number (consumer cursor value)
        const uint8_t* data = nullptr;    ///< message bytes, pointing into the ring
        uint32_t length = 0;              ///< message length in bytes
        explicit operator bool() const noexcept { return data != nullptr; }
    };

    /**
     * @brief Construct a new SlickStreamBuffer
     *
     * @param capacity Data ring size in bytes, must be a power of 2.
     * @param control_size Number of message records in the control ring, must be a power of 2.
     * @param shm_name The name of the shared memory segment. If nullptr, the buffer uses local memory.
     *
     * @throws std::runtime_error if shared memory allocation fails.
     * @throws std::invalid_argument if capacity or control_size is not a power of 2.
     */
    SlickStreamBuffer(uint64_t capacity, uint32_t control_size, const char* const shm_name = nullptr)
        : capacity_(capacity)
        , mask_(capacity ? capacity - 1 : 0)
        , control_size_(control_size)
        , control_mask_(control_size ? control_size - 1 : 0)
        , own_(shm_name == nullptr)
        , use_shm_(shm_name != nullptr)
    {
        if (!is_power_of_two(capacity_)) {
            throw std::invalid_argument("capacity must be power of 2");
        }
        if (!is_power_of_two(control_size_)) {
            throw std::invalid_argument("control_size must be power of 2");
        }
        if (shm_name) {
            allocate_shm_data(shm_name, false);
        } else {
            committed_ = &committed_local_;
            consumed_ = &consumed_local_;
            next_seq_ = &next_seq_local_;
            reserve_end_ = &reserve_end_local_;
            data_ = new uint8_t[capacity_];
            control_ = new record[control_size_];
        }
    }

    /**
     * @brief Open an existing SlickStreamBuffer in shared memory
     *
     * @param shm_name The name of the shared memory segment.
     *
     * @throws std::runtime_error if shared memory allocation fails or the segment does not exist.
     */
    explicit SlickStreamBuffer(const char* const shm_name)
        : own_(false)
        , use_shm_(true)
    {
        allocate_shm_data(shm_name, true);
    }

    SlickStreamBuffer(const SlickStreamBuffer&) = delete;
    SlickStreamBuffer& operator=(const SlickStreamBuffer&) = delete;

    virtual ~SlickStreamBuffer() noexcept {
        if (use_shm_) {
            // slick-shm RAII handles unmapping and closing automatically
            // Only need to explicitly remove on POSIX if we're the owner
#if !defined(_MSC_VER)
            if (own_ && shm_.is_valid() && !shm_name_.empty()) {
                slick::shm::shared_memory::remove(shm_name_.c_str());
            }
#endif
        } else {
            delete[] data_;
            data_ = nullptr;
            delete[] control_;
            control_ = nullptr;
        }
    }

    /**
     * @brief Check if the buffer owns the memory
     * @return true if the buffer owns the memory, false otherwise
     */
    bool own_buffer() const noexcept { return own_; }

    /**
     * @brief Check if the buffer uses shared memory
     * @return true if the buffer uses shared memory, false otherwise
     */
    bool use_shm() const noexcept { return use_shm_; }

    /**
     * @brief Get the data ring capacity in bytes
     */
    uint64_t capacity() const noexcept { return capacity_; }

    /**
     * @brief Get the number of records in the control ring
     */
    uint32_t control_size() const noexcept { return control_size_; }

    /**
     * @brief Get the number of records skipped due to overwrite (debug-only if enabled).
     * @return Count of skipped records observed by this instance.
     */
    uint64_t loss_count() const noexcept {
#if SLICK_STREAM_BUFFER_ENABLE_LOSS_DETECTION
        return loss_count_.load(std::memory_order_relaxed);
#else
        return 0;
#endif
    }

    /**
     * @brief Get the initial reading cursor for a late-joining consumer.
     *
     * A consumer starting at this cursor only sees records published after the call.
     */
    uint64_t initial_reading_index() const noexcept {
        return next_seq_->load(std::memory_order_relaxed);
    }

    // ------------------------------------------------------------------
    // Producer side (single thread only)
    // ------------------------------------------------------------------

    /**
     * @brief Get a contiguous writable region of n bytes.
     *
     * May relocate the committed-but-unconsumed region to keep it contiguous when the ring wraps;
     * pointers previously returned by data() or prepare() are invalidated.
     *
     * @return Pointer to the writable region and its size (== n).
     * @throws std::length_error if size() + n exceeds capacity().
     */
    std::pair<uint8_t*, std::size_t> prepare(std::size_t n) {
        if (n == 0) [[unlikely]] {
            prepared_size_ = 0;
            return { nullptr, 0 };
        }
        uint64_t committed = committed_->load(std::memory_order_relaxed);
        const uint64_t consumed = consumed_->load(std::memory_order_relaxed);
        const uint64_t unconsumed = committed - consumed;
        if (unconsumed + n > capacity_) [[unlikely]] {
            throw std::length_error("SlickStreamBuffer::prepare exceeds capacity");
        }

        uint64_t pos = committed & mask_;
        if (pos + n > capacity_) {
            // Not enough contiguous space at the end of the ring: relocate the unconsumed
            // region to the ring start and jump the monotonic cursors to the next capacity
            // boundary. The relocated bytes are unpublished, so no consumer references them.
            const uint64_t base = committed + (capacity_ - pos);  // next multiple of capacity_
            // Mark the bytes about to be clobbered (ring [0, unconsumed + n)) as reserved
            // BEFORE writing, so consumers can detect the data lap.
            bump_reserve_end(base + unconsumed + n);
            if (unconsumed != 0) {
                const uint64_t src = consumed & mask_;
                if (src != 0) {
                    std::memmove(data_, data_ + src, unconsumed);  // ranges can overlap
                }
            }
            consumed_->store(base, std::memory_order_relaxed);
            committed = base + unconsumed;
            committed_->store(committed, std::memory_order_relaxed);
            pos = unconsumed;
        } else {
            // asio writes into the region before commit(), so reserve it now.
            bump_reserve_end(committed + n);
        }
        prepared_size_ = n;
        return { data_ + pos, n };
    }

    /**
     * @brief Move n bytes from the prepared region into the committed (readable) region.
     *
     * n is clamped to the size of the prepared region; any remainder stays prepared.
     */
    void commit(std::size_t n) noexcept {
        if (n > prepared_size_) {
            n = prepared_size_;
        }
        // consumers never read committed_, relaxed is sufficient
        committed_->store(committed_->load(std::memory_order_relaxed) + n, std::memory_order_relaxed);
        prepared_size_ -= n;
    }

    /**
     * @brief Publish the first n committed bytes to consumers as ONE message record.
     *
     * n is clamped to size(); a zero-length consume publishes nothing.
     *
     * @return The record as consumers will see it (sequence, data pointer, length), or an
     *         empty record (data == nullptr) if nothing was published.
     */
    published_record consume(std::size_t n) noexcept {
        const uint64_t consumed = consumed_->load(std::memory_order_relaxed);
        const uint64_t avail = committed_->load(std::memory_order_relaxed) - consumed;
        if (n > avail) {  // clamp like flat_buffer: consuming more than size() consumes everything
            n = avail;
        }
        if (n == 0) {
            return {};
        }
        assert(n <= std::numeric_limits<uint32_t>::max() && "a single message must be < 4 GiB");

        // Advance next_seq_ BEFORE publishing the record so that every live record always has
        // seq < next_seq_ (consumers use seq >= next_seq_ to detect reset()).
        const uint64_t seq = next_seq_->load(std::memory_order_relaxed);
        next_seq_->store(seq + 1, std::memory_order_release);

        record& r = control_[seq & control_mask_];
        // Best-effort: invalidate the slot before rewriting its fields so most concurrent
        // readers of a lapped slot observe "not ready" instead of torn fields. This does not
        // fully close the torn-read race (same caveat as SlickQueue).
        r.seq.store(kInvalidSeq, std::memory_order_relaxed);
        r.offset = consumed;
        r.length = static_cast<uint32_t>(n);
        r.seq.store(seq, std::memory_order_release);  // publication edge, pairs with consumer acquire

        consumed_->store(consumed + n, std::memory_order_relaxed);
        return { seq, data_ + (consumed & mask_), static_cast<uint32_t>(n) };
    }

    /**
     * @brief Pointer to the committed-but-unconsumed region (always contiguous).
     */
    const uint8_t* data() const noexcept {
        return data_ + (consumed_->load(std::memory_order_relaxed) & mask_);
    }

    /**
     * @brief Number of committed-but-unconsumed bytes.
     */
    std::size_t size() const noexcept {
        return static_cast<std::size_t>(committed_->load(std::memory_order_relaxed) -
                                        consumed_->load(std::memory_order_relaxed));
    }

    // ------------------------------------------------------------------
    // Consumer side
    // ------------------------------------------------------------------

    /**
     * @brief Read the next published message.
     * @param cursor Reference to the consumer's reading cursor (record sequence number),
     *               updated past the returned record.
     * @return Pointer to the message bytes and length, or (nullptr, 0) if no message is available.
     */
    std::pair<const uint8_t*, uint32_t> read(uint64_t& cursor) noexcept {
        for (;;) {
            record& r = control_[cursor & control_mask_];
            const uint64_t seq = r.seq.load(std::memory_order_acquire);

            if (seq != kInvalidSeq && seq >= next_seq_->load(std::memory_order_relaxed)) [[unlikely]] {
                // the buffer has been reset
                cursor = 0;
                continue;
            }

            if (seq == kInvalidSeq || seq < cursor) {
                // record not published yet
                return { nullptr, 0 };
            }

            if (seq > cursor) {
                // producer lapped the control ring; skip to the record in this slot
#if SLICK_STREAM_BUFFER_ENABLE_LOSS_DETECTION
                loss_count_.fetch_add(seq - cursor, std::memory_order_relaxed);
#endif
                cursor = seq;
            }

            // fields are ordered by the acquire load of seq
            const uint64_t offset = r.offset;
            const uint32_t length = r.length;

            // The control ring and data ring lap independently: a slot can hold a valid seq
            // while its bytes were already overwritten by prepare(). Detect via the prepared
            // high-water mark (best-effort). Checking the first byte suffices because
            // clobbering proceeds in offset order.
            if (reserve_end_->load(std::memory_order_relaxed) - offset > capacity_) [[unlikely]] {
#if SLICK_STREAM_BUFFER_ENABLE_LOSS_DETECTION
                loss_count_.fetch_add(1, std::memory_order_relaxed);
#endif
                ++cursor;
                cpu_relax();
                continue;
            }

            cursor = seq + 1;
            return { data_ + (offset & mask_), length };
        }
    }

    /**
     * @brief Read the most recently published message without a cursor.
     * @return Pointer to the message bytes and length, or (nullptr, 0) if none is available.
     *
     * May transiently return (nullptr, 0) while the newest record is being published or
     * after it has been lapped.
     */
    std::pair<const uint8_t*, uint32_t> read_last() noexcept {
        const uint64_t next = next_seq_->load(std::memory_order_acquire);
        if (next == 0) {
            return { nullptr, 0 };
        }
        const uint64_t seq = next - 1;
        record& r = control_[seq & control_mask_];
        if (r.seq.load(std::memory_order_acquire) != seq) {
            return { nullptr, 0 };
        }
        const uint64_t offset = r.offset;
        const uint32_t length = r.length;
        if (reserve_end_->load(std::memory_order_relaxed) - offset > capacity_) [[unlikely]] {
            return { nullptr, 0 };
        }
        return { data_ + (offset & mask_), length };
    }

    /**
     * @brief Reset the buffer, invalidating all existing data
     *
     * Note: This function is not thread-safe and should be called when no other threads
     * or processes are accessing the buffer.
     */
    void reset() noexcept {
        if (use_shm_) {
            control_ = new ((uint8_t*)lpvMem_ + HEADER_SIZE) record[control_size_];
        } else {
            delete[] control_;
            control_ = new record[control_size_];
        }
        committed_->store(0, std::memory_order_relaxed);
        consumed_->store(0, std::memory_order_relaxed);
        reserve_end_->store(0, std::memory_order_relaxed);
        next_seq_->store(0, std::memory_order_release);
        prepared_size_ = 0;
#if SLICK_STREAM_BUFFER_ENABLE_LOSS_DETECTION
        loss_count_.store(0, std::memory_order_relaxed);
#endif
    }

private:
    void bump_reserve_end(uint64_t end) noexcept {
        // monotonic max - a re-prepare with a smaller n must not move the high-water back
        if (reserve_end_->load(std::memory_order_relaxed) < end) {
            reserve_end_->store(end, std::memory_order_release);
        }
    }

    static inline void cpu_relax() noexcept {
#if SLICK_STREAM_BUFFER_ENABLE_CPU_RELAX
#if defined(_MSC_VER) && (defined(_M_IX86) || defined(_M_X64))
        _mm_pause();
#elif defined(__i386__) || defined(__x86_64__)
        __builtin_ia32_pause();
#elif defined(__aarch64__) || defined(__arm__)
        __asm__ __volatile__("yield" ::: "memory");
#else
        std::this_thread::yield();
#endif
#else
        (void)0;
#endif
    }

    bool wait_for_shared_memory_ready(std::atomic<uint32_t>* init_state) const noexcept {
        constexpr int kMaxWaitMs = 2000;
        for (int i = 0; i < kMaxWaitMs; ++i) {
            if (init_state->load(std::memory_order_acquire) == INIT_STATE_READY) {
                return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        return false;
    }

    void map_existing(uint8_t* base) {
        const uint32_t magic = *reinterpret_cast<uint32_t*>(base + HEADER_MAGIC_OFFSET);
        if (magic != HEADER_MAGIC) {
            throw std::runtime_error("Shared memory header magic mismatch - not a SlickStreamBuffer segment");
        }

        const uint64_t capacity = *reinterpret_cast<uint64_t*>(base + CAPACITY_OFFSET);
        const uint32_t control_size = *reinterpret_cast<uint32_t*>(base + CONTROL_SIZE_OFFSET);
        if (!is_power_of_two(capacity) || !is_power_of_two(control_size)) {
            throw std::runtime_error("Shared memory geometry is invalid");
        }

        if (capacity_ != 0 && (capacity != capacity_ || control_size != control_size_)) {
            throw std::runtime_error("Shared memory geometry mismatch. Expected capacity " +
                std::to_string(capacity_) + "/control_size " + std::to_string(control_size_) +
                " but got " + std::to_string(capacity) + "/" + std::to_string(control_size));
        }

        capacity_ = capacity;
        mask_ = capacity_ - 1;
        control_size_ = control_size;
        control_mask_ = control_size_ - 1;

        committed_ = reinterpret_cast<std::atomic<uint64_t>*>(base + COMMITTED_OFFSET);
        consumed_ = reinterpret_cast<std::atomic<uint64_t>*>(base + CONSUMED_OFFSET);
        next_seq_ = reinterpret_cast<std::atomic<uint64_t>*>(base + NEXT_SEQ_OFFSET);
        reserve_end_ = reinterpret_cast<std::atomic<uint64_t>*>(base + RESERVE_END_OFFSET);
        control_ = reinterpret_cast<record*>(base + HEADER_SIZE);
        data_ = base + HEADER_SIZE + sizeof(record) * control_size_;
    }

    void allocate_shm_data(const char* const shm_name, bool open_only) {
        shm_name_ = shm_name;  // Store for destructor cleanup

        if (open_only) {
            // Opener constructor - open existing only
            try {
                shm_ = slick::shm::shared_memory(
                    shm_name,
                    slick::shm::open_existing,
                    slick::shm::access_mode::read_write
                );
            } catch (const slick::shm::shared_memory_error& e) {
                throw std::runtime_error(std::string("Failed to open shared memory: ") + e.what());
            }

            lpvMem_ = shm_.data();
            if (!lpvMem_) {
                throw std::runtime_error("Failed to map shared memory");
            }

            auto* base = reinterpret_cast<uint8_t*>(lpvMem_);
            auto* init_state = reinterpret_cast<std::atomic<uint32_t>*>(base + INIT_STATE_OFFSET);
            if (!wait_for_shared_memory_ready(init_state)) {
                throw std::runtime_error("Timed out waiting for shared memory initialization");
            }

            map_existing(base);

        } else {
            // Creator constructor - create or open
            const size_t total_size = HEADER_SIZE + sizeof(record) * control_size_ + capacity_;

            try {
                shm_ = slick::shm::shared_memory(
                    shm_name,
                    total_size,
                    slick::shm::open_or_create,
                    slick::shm::access_mode::read_write
                );
            } catch (const slick::shm::shared_memory_error& e) {
                throw std::runtime_error(std::string("Failed to create/open shared memory: ") + e.what());
            }

            lpvMem_ = shm_.data();
            if (!lpvMem_) {
                throw std::runtime_error("Failed to map shared memory");
            }

            auto* base = reinterpret_cast<uint8_t*>(lpvMem_);
            auto* init_state = reinterpret_cast<std::atomic<uint32_t>*>(base + INIT_STATE_OFFSET);

            uint32_t expected = INIT_STATE_UNINITIALIZED;
            bool we_are_creator = init_state->compare_exchange_strong(
                expected, INIT_STATE_INITIALIZING, std::memory_order_acq_rel);

            if (we_are_creator) {
                // Initialize as creator
                own_ = true;

                committed_ = new (base + COMMITTED_OFFSET) std::atomic<uint64_t>(0);
                consumed_ = new (base + CONSUMED_OFFSET) std::atomic<uint64_t>(0);
                next_seq_ = new (base + NEXT_SEQ_OFFSET) std::atomic<uint64_t>(0);
                reserve_end_ = new (base + RESERVE_END_OFFSET) std::atomic<uint64_t>(0);

                *reinterpret_cast<uint64_t*>(base + CAPACITY_OFFSET) = capacity_;
                *reinterpret_cast<uint32_t*>(base + CONTROL_SIZE_OFFSET) = control_size_;
                *reinterpret_cast<uint32_t*>(base + HEADER_MAGIC_OFFSET) = HEADER_MAGIC;

                control_ = new (base + HEADER_SIZE) record[control_size_];
                data_ = base + HEADER_SIZE + sizeof(record) * control_size_;

                init_state->store(INIT_STATE_READY, std::memory_order_release);

            } else {
                // Opened existing - validate geometry against the requested values
                own_ = false;

                if (!wait_for_shared_memory_ready(init_state)) {
                    throw std::runtime_error("Timed out waiting for shared memory initialization");
                }

                map_existing(base);
            }
        }
    }
};

}
