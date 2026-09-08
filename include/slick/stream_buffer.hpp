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
#include <thread>
#include <chrono>
#include <limits>
#include <new>
#include <utility>
#include <concepts>
#include <type_traits>

// The SLICK_STREAM_BUFFER_ENABLE_* macros were replaced by the Traits parameter of read()
// (see slick::read_traits). A macro cannot express per-call configuration, and because these
// changed the body - and, for loss detection, the layout - of a header-only class, two
// translation units compiled with different values silently violated the ODR: sizeof
// disagreed by one cacheline across a -DNDEBUG boundary. Warn rather than error so upgrading
// does not break a build that still passes -D; the macro itself has no effect any more.
#define SLICK_STREAM_BUFFER_STR_(x) #x
#define SLICK_STREAM_BUFFER_STR(x) SLICK_STREAM_BUFFER_STR_(x)
#if defined(_MSC_VER)
// The __FILE__(__LINE__): prefix makes the warning clickable in the VS problem list.
#define SLICK_STREAM_BUFFER_DEPRECATED_MACRO(msg) \
    __pragma(message(__FILE__ "(" SLICK_STREAM_BUFFER_STR(__LINE__) "): warning: " msg))
#else
#define SLICK_STREAM_BUFFER_DEPRECATED_MACRO(msg) _Pragma(SLICK_STREAM_BUFFER_STR(GCC warning msg))
#endif

#ifdef SLICK_STREAM_BUFFER_ENABLE_LOSS_DETECTION
SLICK_STREAM_BUFFER_DEPRECATED_MACRO("SLICK_STREAM_BUFFER_ENABLE_LOSS_DETECTION is ignored; it was "
                                     "replaced by slick::read_traits::count_loss. See README.")
#endif
#ifdef SLICK_STREAM_BUFFER_ENABLE_RESET_DETECTION
SLICK_STREAM_BUFFER_DEPRECATED_MACRO("SLICK_STREAM_BUFFER_ENABLE_RESET_DETECTION is ignored; it was "
                                     "replaced by slick::read_traits::detect_reset. See README.")
#endif
#ifdef SLICK_STREAM_BUFFER_ENABLE_CPU_RELAX
SLICK_STREAM_BUFFER_DEPRECATED_MACRO("SLICK_STREAM_BUFFER_ENABLE_CPU_RELAX is ignored; the pause hint "
                                     "was removed - it sat in read()'s catch-up loop, which makes "
                                     "forward progress rather than waiting, and measurably increased "
                                     "loss. Put a pause in your own poll loop instead. See README.")
#endif

namespace slick {

/**
 * @brief Compile-time configuration for SlickStreamBuffer::read().
 *
 * Derive and override what you need:
 * @code
 *     struct counting : slick::read_traits {
 *         static constexpr bool count_loss = true;
 *     };
 *     auto [data, len] = buf.read<counting>(cursor);
 * @endcode
 *
 * The configuration rides on read() rather than on the class so that every configuration is
 * still the same slick::stream_buffer type: a differently-configured buffer stays passable to
 * anything taking stream_buffer&, shared_ptr<stream_buffer>, or dynamic_buffer<stream_buffer>.
 * Being a template argument, it is also part of the mangled name, so two translation units
 * that disagree cannot silently share one definition the way the old macros did.
 */
struct read_traits {
    /// Resynchronize a consumer whose cursor outlived a reset() that restarted the numbering.
    /// On by default: it is one acquire load of next_seq_ per record, which measured free next
    /// to the rest of read() as long as count_loss is off (see bench/). Turn it off if you never
    /// call reset() and want that load gone on a platform where it may not be free - it is a
    /// real ldar on ARM.
    static constexpr bool detect_reset = true;

    /// Accumulate loss_count() as records are skipped. Off by default: it costs ~20% of read()
    /// on MSVC even in a run that loses nothing - the atomic RMW never executes, but it stays in
    /// the function body and bloats the read loop. Turn it on when diagnosing loss.
    static constexpr bool count_loss = false;
};

/**
 * @brief Requirements on the Traits parameter of read().
 *
 * Requiring exactly bool (not merely convertible to bool) rejects a wrong type at the point of
 * use with a readable constraint failure. It cannot catch a misspelled override in a derived
 * traits struct: the inherited member stays visible and silently keeps the base value.
 */
template<typename Traits>
concept read_traits_type = requires {
    requires std::same_as<std::remove_cv_t<decltype(Traits::detect_reset)>, bool>;
    requires std::same_as<std::remove_cv_t<decltype(Traits::count_loss)>, bool>;
};

/**
 * @brief A lock-free single-producer multi-consumer byte stream buffer with optional shared memory support.
 *
 * The producer side exposes a beast::flat_buffer-like interface (prepare/commit/consume/data/size)
 * so that network bytes can be written directly into the ring (e.g. by boost::asio/boost::beast via
 * the slick::dynamic_buffer adapter from the slick-dynamic-buffer repo). consume(n) does not
 * discard bytes - it PUBLISHES them to consumers as one discrete message record. Each consumer
 * owns a monotonic cursor and reads whole messages zero-copy as (pointer, length) pairs.
 *
 * Caveats:
 * - Producer methods (prepare/commit/consume/data/size/reset) must be called from a single thread.
 * - The queue is lossy: if the producer outruns a consumer by more than the control ring or data
 *   ring size, the consumer skips ahead; the loss is counted only for reads made with a Traits
 *   whose count_loss is true (see slick::read_traits and loss_count()). A consumer
 *   lapped mid-read may observe torn record fields or bytes - same caveat as SlickQueue.
 * - The data-lap validity check in read() is best-effort: bytes can still be overwritten after the
 *   check while the caller is using them, and records whose ring positions fall inside a wrap
 *   relocation gap may be reported lost even though their bytes were not overwritten.
 * - prepare() may relocate the committed-but-unconsumed region; pointers previously returned by
 *   data() or prepare() are invalidated (same as flat_buffer reallocation semantics).
 * - A single message (one consume() call) is limited to < 4 GiB; consume() throws
 *   std::length_error rather than publishing a truncated record length.
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
    uint64_t committed_cursor_ = 0;  // producer-local shadow of committed_
    uint64_t consumed_cursor_ = 0;   // producer-local shadow of consumed_
    uint64_t next_seq_cursor_ = 0;   // producer-local shadow of next_seq_
    uint64_t reserve_end_cursor_ = 0;// producer-local shadow of reserve_end_

    alignas(cacheline_size) std::atomic<uint64_t> committed_local_{ 0 };
    alignas(cacheline_size) std::atomic<uint64_t> consumed_local_{ 0 };
    alignas(cacheline_size) std::atomic<uint64_t> next_seq_local_{ 0 };
    alignas(cacheline_size) std::atomic<uint64_t> reserve_end_local_{ 0 };
    // Always declared, never conditional: guarding a member behind a macro made sizeof depend
    // on -DNDEBUG, which is an ODR violation waiting to corrupt a mixed build. Whether it is
    // *updated* is read()s Traits parameter.
    alignas(cacheline_size) std::atomic<uint64_t> loss_count_{ 0 };
    bool own_ = false;
    bool use_shm_ = false;
    slick::shm::shared_memory shm_;   // RAII wrapper for shared memory
    void* lpvMem_ = nullptr;          // Cached data pointer
    std::string shm_name_;            // Stored for cleanup

    // Shared memory layout constants
    //
    // The hot counters share one cache line deliberately. Splitting the producer-private
    // counters (committed_, consumed_) away from the ones consumers poll (next_seq_,
    // reserve_end_) was tried and measured no faster at 3 or 7 consumers: next_seq_ is the
    // publication signal, so the producer writes that line on every message no matter what,
    // and the line already ping-pongs once per message. Folding committed_/consumed_ into the
    // same transfer is free, while splitting them adds a second dirty line per message. See
    // bench/ - re-measure there before rearranging this.
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

    /// Bytes a segment of this geometry occupies, computed in uint64_t on purpose: with a 32-bit
    /// size_t the sum wraps, and a wrapped total would size a tiny mapping that then passes every
    /// size check against its own truncated value. The sum itself cannot overflow - a
    /// power-of-two capacity is at most 2^63 and the control ring at most 2^37 bytes.
    static constexpr uint64_t total_bytes(uint64_t capacity, uint32_t control_size) noexcept {
        return static_cast<uint64_t>(HEADER_SIZE) +
            static_cast<uint64_t>(control_size) * sizeof(record) + capacity;
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
     * Only a segment this process actually creates is initialized. If @p shm_name already exists,
     * this attaches to it (own_buffer() is then false) and throws unless it is a SlickStreamBuffer
     * segment with matching geometry - a name that collides with another application's segment is
     * never overwritten.
     *
     * @throws std::runtime_error if shared memory allocation fails, or the existing segment is not
     *         a compatible SlickStreamBuffer segment.
     * @throws std::invalid_argument if capacity or control_size is not a power of 2.
     * @throws std::length_error if the segment would not fit in a std::size_t on this build (only
     *         reachable on a 32-bit build, where a 4 GiB capacity is a valid power of two).
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
        if (total_bytes(capacity_, control_size_) > std::numeric_limits<std::size_t>::max()) {
            // A 4 GiB capacity is a valid power of two that a 32-bit build cannot address.
            throw std::length_error("capacity plus control ring exceeds the address space of this build");
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
     * @brief Unlink a shared memory segment by name - the stale-segment recovery step
     *
     * A creator that dies part-way through initializing a segment leaves it stuck in the
     * INITIALIZING state, and on POSIX the name outlives the process. Every later creator and
     * opener then fails construction (with a message naming this call) because there is no
     * portable way to tell a dead creator from a slow one: a PID stamped in the header is
     * meaningless across PID namespaces and unreliable under PID reuse. Recovery is therefore an
     * explicit decision the application makes, typically once at startup before creating:
     *
     * @code
     * slick::stream_buffer::remove("my_stream");   // clear anything a previous run left behind
     * slick::stream_buffer buf(1 << 20, 4096, "my_stream");
     * @endcode
     *
     * The same call clears a stale but fully initialized segment left by a crashed producer,
     * which would otherwise be attached with its dead cursors intact.
     *
     * @param shm_name The name of the shared memory segment.
     * @return true if the segment was removed.
     *
     * @warning Only call this when no process is using the segment. On POSIX the name is
     *          unlinked immediately, so the next creator gets a fresh segment while any process
     *          still mapped to the old one keeps reading the orphaned copy. On Windows this is a
     *          no-op returning true - a section there disappears once the last handle closes.
     */
    static bool remove(const char* const shm_name) noexcept {
        return slick::shm::shared_memory::remove(shm_name);
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
     * @brief Get the number of records skipped due to overwrite.
     * @return Count of skipped records observed by this instance, counting only reads made with
     *         a Traits whose count_loss is true - so 0 under the default slick::read_traits.
     */
    uint64_t loss_count() const noexcept {
        return loss_count_.load(std::memory_order_relaxed);
    }

    /**
     * @brief Get the initial reading cursor for a late-joining consumer.
     *
     * A consumer starting at this cursor only sees records published after the call.
     */
    uint64_t initial_reading_index() const noexcept {
        return next_seq_->load(std::memory_order_acquire);
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
            return { data_ + (committed_cursor_ & mask_), 0 };
        }
        const uint64_t unconsumed = committed_cursor_ - consumed_cursor_;
        if (n > capacity_ - unconsumed) [[unlikely]] {
            throw std::length_error("SlickStreamBuffer::prepare exceeds capacity");
        }

        uint64_t pos = committed_cursor_ & mask_;
        if (pos + n > capacity_) {
            // Not enough contiguous space at the end of the ring: relocate the unconsumed
            // region to the ring start and jump the monotonic cursors to the next capacity
            // boundary. The relocated bytes are unpublished, so no consumer references them.
            const uint64_t base = committed_cursor_ + (capacity_ - pos);  // next multiple of capacity_
            // Mark the bytes about to be clobbered (ring [0, unconsumed + n)) as reserved
            // BEFORE writing, so consumers can detect the data lap.
            bump_reserve_end(base + unconsumed + n);
            if (unconsumed != 0) {
                const uint64_t src = consumed_cursor_ & mask_;
                if (src != 0) {
                    std::memmove(data_, data_ + src, unconsumed);  // ranges can overlap
                }
            }
            consumed_cursor_ = base;
            consumed_->store(consumed_cursor_, std::memory_order_relaxed);
            committed_cursor_ = base + unconsumed;
            committed_->store(committed_cursor_, std::memory_order_relaxed);
            pos = unconsumed;
        } else {
            // asio writes into the region before commit(), so reserve it now.
            bump_reserve_end(committed_cursor_ + n);
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
        committed_cursor_ += n;
        committed_->store(committed_cursor_, std::memory_order_relaxed);
        prepared_size_ -= n;
    }

    /**
     * @brief Publish the first n committed bytes to consumers as ONE message record.
     *
     * n is clamped to size(); a zero-length consume publishes nothing.
     *
     * @return The record as consumers will see it (sequence, data pointer, length), or an
     *         empty record (data == nullptr) if nothing was published.
     * @throws std::length_error if the clamped length does not fit a record's 32-bit length
     *         field (a single message must be < 4 GiB). The check runs before any state
     *         changes, so nothing is published and the bytes stay readable - publish them as
     *         several smaller records instead.
     */
    published_record consume(std::size_t n) {
        const uint64_t avail = committed_cursor_ - consumed_cursor_;
        // uint64_t throughout: on a 32-bit size_t the clamp itself would truncate avail.
        const uint64_t len = static_cast<uint64_t>(n) < avail ? static_cast<uint64_t>(n) : avail;
        if (len == 0) {
            return {};
        }
        if (len > std::numeric_limits<uint32_t>::max()) [[unlikely]] {
            // record::length is 32 bits. Truncating here would publish a wrong length and hand
            // consumers a short read of a long message, so this must never be a Debug-only assert.
            throw std::length_error("SlickStreamBuffer::consume exceeds the 4 GiB record limit");
        }

        // Advance next_seq_ BEFORE publishing the record so that every live record always has
        // seq < next_seq_ (consumers use seq >= next_seq_ to detect reset()).
        // The value comes from the producer-private shadow: only the producer writes it, so
        // re-loading the shared atomic would just risk a miss on a line consumers keep pulling.
        const uint64_t seq = next_seq_cursor_++;
        next_seq_->store(next_seq_cursor_, std::memory_order_release);

        record& r = control_[seq & control_mask_];
        // Best-effort: invalidate the slot before rewriting its fields so most concurrent
        // readers of a lapped slot observe "not ready" instead of torn fields. This does not
        // fully close the torn-read race (same caveat as SlickQueue).
        r.seq.store(kInvalidSeq, std::memory_order_relaxed);
        r.offset = consumed_cursor_;
        r.length = static_cast<uint32_t>(len);
        r.seq.store(seq, std::memory_order_release);  // publication edge, pairs with consumer acquire

        const uint64_t consumed = consumed_cursor_;
        consumed_cursor_ += len;
        consumed_->store(consumed_cursor_, std::memory_order_relaxed);
        return { seq, data_ + (consumed & mask_), static_cast<uint32_t>(len) };
    }

    /**
     * @brief Discard the committed-but-unconsumed bytes and any prepared region
     *        without publishing them.
     *
     * Use this when the byte stream is interrupted mid-message (e.g. a websocket
     * disconnect after a partial read): the bytes already committed for the incomplete
     * message must not be prepended to the data of the next connection. discard()
     * itself does not publish a record, but older published records remain subject
     * to the normal lossy overwrite semantics if a prepared region already wrapped
     * over their bytes.
     *
     * Producer-side only: call from the producer thread, with no read operation
     * outstanding on the buffer. A stale commit() after discard() commits nothing.
     */
    void discard() noexcept {
        committed_cursor_ = consumed_cursor_;
        // consumers never read committed_, relaxed is sufficient
        committed_->store(committed_cursor_, std::memory_order_relaxed);
        prepared_size_ = 0;
    }

    /**
     * @brief Pointer to the committed-but-unconsumed region (always contiguous).
     */
    const uint8_t* data() const noexcept {
        return data_ + (consumed_cursor_ & mask_);
    }

    /**
     * @brief Number of committed-but-unconsumed bytes.
     */
    std::size_t size() const noexcept {
        return static_cast<std::size_t>(committed_cursor_ - consumed_cursor_);
    }

    // ------------------------------------------------------------------
    // Consumer side
    // ------------------------------------------------------------------

    /**
     * @brief Read the next published message.
     * @tparam Traits Per-call configuration, see slick::read_traits. Its defaults resynchronize
     *                after a reset() but do not accumulate loss_count(); derive to change that.
     * @param cursor Reference to the consumer's reading cursor (record sequence number),
     *               updated past the returned record.
     * @return Pointer to the message bytes and length, or (nullptr, 0) if no message is available.
     */
    template<read_traits_type Traits = read_traits>
    std::pair<const uint8_t*, uint32_t> read(uint64_t& cursor) noexcept {
        for (;;) {
            record& r = control_[cursor & control_mask_];
            const uint64_t seq = r.seq.load(std::memory_order_acquire);

            if constexpr (Traits::detect_reset) {
                // A published seq is always < next_seq_, so seq >= next_seq_ can only mean
                // reset() restarted the numbering under a consumer that was idle at the time.
                if (seq != kInvalidSeq && seq >= next_seq_->load(std::memory_order_acquire)) [[unlikely]] {
                    cursor = 0;
                    continue;
                }
            }

            if (seq == kInvalidSeq || seq < cursor) {
                // record not published yet
                return { nullptr, 0 };
            }

            if (seq > cursor) {
                // producer lapped the control ring; skip to the record in this slot
                if constexpr (Traits::count_loss) {
                    loss_count_.fetch_add(seq - cursor, std::memory_order_relaxed);
                }
                cursor = seq;
            }

            // fields are ordered by the acquire load of seq
            const uint64_t offset = r.offset;
            const uint32_t length = r.length;

            // The control ring and data ring lap independently: a slot can hold a valid seq
            // while its bytes were already overwritten by prepare(). Detect via the prepared
            // high-water mark (best-effort). Checking the first byte suffices because
            // clobbering proceeds in offset order.
            // Relaxed: this load publishes nothing. The bytes were made visible by the acquire
            // on r.seq above, and no barrier can stop the producer from clobbering them after
            // the check - hence "best-effort".
            if (reserve_end_->load(std::memory_order_relaxed) - offset > capacity_) [[unlikely]] {
                if constexpr (Traits::count_loss) {
                    loss_count_.fetch_add(1, std::memory_order_relaxed);
                }
                // No pause hint here: this loop skips forward every iteration rather than
                // waiting on anything, so backing off only widens the gap it is closing.
                ++cursor;
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
        committed_cursor_ = 0;
        consumed_cursor_ = 0;
        next_seq_cursor_ = 0;
        reserve_end_cursor_ = 0;
        prepared_size_ = 0;
        loss_count_.store(0, std::memory_order_relaxed);
    }

private:
    void bump_reserve_end(uint64_t end) noexcept {
        // monotonic max - a re-prepare with a smaller n must not move the high-water back
        // Compared against the producer-private shadow, so the common no-op case costs no
        // shared load at all.
        if (reserve_end_cursor_ < end) {
            reserve_end_cursor_ = end;
            reserve_end_->store(end, std::memory_order_release);
        }
    }

    /// Throw unless the mapped segment is at least @p required bytes. A segment this process did
    /// not create can be any size, so every header read must be size-checked before it happens.
    void require_mapped_size(uint64_t required) const {
        const uint64_t mapped = static_cast<uint64_t>(shm_.size());
        if (mapped < required) {
            throw std::runtime_error("Shared memory segment is too small - need " +
                std::to_string(required) + " bytes but mapped " + std::to_string(mapped));
        }
    }

    /// Wait for the creator to publish INIT_STATE_READY, throwing if it never arrives. Fails fast
    /// when the magic slot already holds a foreign value: a SlickStreamBuffer creator only ever
    /// writes HEADER_MAGIC there, so anything else non-zero means the name collides with another
    /// application's segment. The caller must have size-checked the header first.
    void wait_for_shared_memory_ready(uint8_t* base) const {
        auto* init_state = reinterpret_cast<std::atomic<uint32_t>*>(base + INIT_STATE_OFFSET);
        auto* magic = reinterpret_cast<std::atomic<uint32_t>*>(base + HEADER_MAGIC_OFFSET);
        constexpr auto kMaxWait = std::chrono::milliseconds(2000);
        // Deadline-based: a sleep_for(1ms) rounds up to the OS timer granularity (~15.6 ms on
        // Windows), so counting iterations would stretch the wait by an order of magnitude.
        const auto deadline = std::chrono::steady_clock::now() + kMaxWait;
        for (;;) {
            if (init_state->load(std::memory_order_acquire) == INIT_STATE_READY) {
                return;
            }
            const uint32_t observed = magic->load(std::memory_order_relaxed);
            if (observed != 0 && observed != HEADER_MAGIC) {
                throw std::runtime_error("Shared memory header magic mismatch - not a SlickStreamBuffer segment");
            }
            if (std::chrono::steady_clock::now() >= deadline) {
                throw std::runtime_error(stale_segment_message(
                    init_state->load(std::memory_order_acquire)));
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    }

    /// Explain a segment that never reached INIT_STATE_READY and name the recovery step. There is
    /// no portable way to tell a creator that is still working from one that died mid-init - a
    /// stamped PID is meaningless across PID namespaces and unreliable under PID reuse - so
    /// recovery is deliberately an explicit operator action via remove().
    std::string stale_segment_message(uint32_t state) const {
        const std::string recovery = " Once no process is using it, clear it with "
            "slick::stream_buffer::remove(\"" + shm_name_ + "\") and construct again.";
        if (state == INIT_STATE_INITIALIZING) {
            return "Shared memory segment '" + shm_name_ + "' is stuck in the INITIALIZING state - "
                "a creator died part-way through initializing it." + recovery;
        }
        return "Timed out waiting for shared memory '" + shm_name_ + "' to be initialized - the "
            "segment exists but no SlickStreamBuffer creator ever initialized it, so it may belong "
            "to another application." + recovery;
    }

    void map_existing(uint8_t* base) {
        require_mapped_size(HEADER_SIZE);

        const uint32_t magic = reinterpret_cast<std::atomic<uint32_t>*>(
            base + HEADER_MAGIC_OFFSET)->load(std::memory_order_acquire);
        if (magic != HEADER_MAGIC) {
            throw std::runtime_error("Shared memory header magic mismatch - not a SlickStreamBuffer segment");
        }

        const uint64_t capacity = *reinterpret_cast<uint64_t*>(base + CAPACITY_OFFSET);
        const uint32_t control_size = *reinterpret_cast<uint32_t*>(base + CONTROL_SIZE_OFFSET);
        if (!is_power_of_two(capacity) || !is_power_of_two(control_size)) {
            throw std::runtime_error("Shared memory geometry is invalid");
        }

        // total_bytes() is overflow-safe here because the power-of-two checks above bound both
        // values, so a bogus header geometry cannot wrap the sum past the mapped size.
        if (total_bytes(capacity, control_size) > static_cast<uint64_t>(shm_.size())) {
            throw std::runtime_error("Shared memory segment is smaller than the geometry its header declares");
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
        committed_cursor_ = committed_->load(std::memory_order_relaxed);
        consumed_cursor_ = consumed_->load(std::memory_order_relaxed);
        next_seq_cursor_ = next_seq_->load(std::memory_order_relaxed);
        reserve_end_cursor_ = reserve_end_->load(std::memory_order_relaxed);
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
            require_mapped_size(HEADER_SIZE);
            wait_for_shared_memory_ready(base);

            map_existing(base);

        } else {
            // Creator constructor - create or open
            // The constructor already rejected a geometry that does not fit in a size_t.
            const std::size_t total_size = static_cast<std::size_t>(total_bytes(capacity_, control_size_));

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

            // Only the process that actually created the segment may initialize it. The
            // init_state slot cannot carry that authority: a foreign segment that happens to
            // share our name is zero-filled too, so a CAS on it would win and overwrite bytes
            // we do not own before the magic is ever checked. is_creator() is OS-backed
            // (O_CREAT|O_EXCL on POSIX, ERROR_ALREADY_EXISTS on Windows) and is the only
            // trustworthy answer.
            if (!shm_.is_creator()) {
                // The segment pre-existed us - attach to it read/write without touching a byte
                // until map_existing() has validated its magic, geometry and size.
                own_ = false;

                require_mapped_size(HEADER_SIZE);
                wait_for_shared_memory_ready(base);

                map_existing(base);
                return;
            }

            // Initialize as creator
            own_ = true;
            require_mapped_size(total_size);

            auto* init_state = reinterpret_cast<std::atomic<uint32_t>*>(base + INIT_STATE_OFFSET);
            uint32_t expected = INIT_STATE_UNINITIALIZED;
            if (!init_state->compare_exchange_strong(
                    expected, INIT_STATE_INITIALIZING, std::memory_order_acq_rel)) {
                // A freshly created segment is zero-filled by the OS, so this cannot happen
                // unless the mapping is not what the platform reported it to be.
                throw std::runtime_error("Newly created shared memory segment is already initialized");
            }

            committed_ = new (base + COMMITTED_OFFSET) std::atomic<uint64_t>(0);
            consumed_ = new (base + CONSUMED_OFFSET) std::atomic<uint64_t>(0);
            next_seq_ = new (base + NEXT_SEQ_OFFSET) std::atomic<uint64_t>(0);
            reserve_end_ = new (base + RESERVE_END_OFFSET) std::atomic<uint64_t>(0);

            *reinterpret_cast<uint64_t*>(base + CAPACITY_OFFSET) = capacity_;
            *reinterpret_cast<uint32_t*>(base + CONTROL_SIZE_OFFSET) = control_size_;
            new (base + HEADER_MAGIC_OFFSET) std::atomic<uint32_t>(HEADER_MAGIC);

            control_ = new (base + HEADER_SIZE) record[control_size_];
            data_ = base + HEADER_SIZE + sizeof(record) * control_size_;

            init_state->store(INIT_STATE_READY, std::memory_order_release);
        }
    }
};

/// Preferred snake_case spelling of SlickStreamBuffer, matching the slick::stream_buffer
/// CMake target and the Boost-style naming of companion types such as slick::dynamic_buffer.
/// SlickStreamBuffer remains available for backward compatibility.
using stream_buffer = SlickStreamBuffer;

}
