# SlickStreamBuffer

[![C++20](https://img.shields.io/badge/C%2B%2B-20-blue.svg)](https://en.cppreference.com/w/cpp/20)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Header-only](https://img.shields.io/badge/header--only-yes-brightgreen.svg)](#installation)
[![Lock-free](https://img.shields.io/badge/concurrency-lock--free-orange.svg)](#architecture)

SlickStreamBuffer is a header-only C++ library that provides a lock-free,
single-producer multi-consumer (SPMC) byte stream buffer built on a ring buffer.
Incoming bytes (e.g. from a network socket) are written directly into the ring,
and publishing a complete message to consumer threads — or other processes via
shared memory — requires **zero copies**.

A Boost.Asio `DynamicBuffer` adapter over this buffer is available separately:
[slick-dynamic-buffer](https://github.com/SlickQuant/slick-dynamic-buffer).

## How it works

The producer side exposes the familiar dynamic-buffer interface
(`prepare` / `commit` / `consume` / `data` / `size`), with one twist:

- `prepare(n)` returns a contiguous writable region — received bytes are written there
- `commit(n)` moves bytes into the readable area — the app parses them in place
- `consume(n)` does **not** discard bytes: it **publishes** them to consumers as
  **one discrete message record**

Each consumer owns an independent monotonic cursor and reads whole messages
zero-copy as `(pointer, length)` pairs — the broadcast pattern of
[SlickQueue](https://github.com/SlickQuant/slick-queue), applied to a byte stream.

```
 network ────────▶ prepare/commit ──▶ [ data ring ] ──consume(n)──▶ record {offset, len}
                                                                        │
                                              consumer A (own cursor) ◀─┤  zero-copy reads
                                              consumer B (own cursor) ◀─┤  (threads or
                                              process C (shared memory)◀┘   processes)
```

## Features

- **Lock-free** single-producer / multi-consumer broadcast
- **Zero-copy fan-out** of received network data to threads and processes
- **Header-only**
- **Shared memory support** for inter-process communication
- **Cross-platform** — Windows, Linux, macOS
- Modern **C++20**

## Requirements

- C++20 compatible compiler (the CMake target requires `cxx_std_20`, so linking
  `slick::stream_buffer` raises a consumer's standard automatically)
- CMake 3.21 or newer
- [slick-shm](https://github.com/SlickQuant/slick-shm) (fetched automatically when not installed)

## Installation

Header-only. Add the `include` directory to your include path:

```cpp
#include <slick/stream_buffer.hpp>
```

### Using vcpkg

```bash
vcpkg install slick-stream-buffer
```

Then in your `CMakeLists.txt`:

```cmake
find_package(slick-stream-buffer CONFIG REQUIRED)
target_link_libraries(your_target PRIVATE slick::stream_buffer)
```

### Using CMake FetchContent

```cmake
include(FetchContent)

set(BUILD_SLICK_STREAM_BUFFER_TESTS OFF CACHE BOOL "" FORCE)
FetchContent_Declare(
    slick-stream-buffer
    GIT_REPOSITORY https://github.com/SlickQuant/slick-stream-buffer.git
    GIT_TAG v2.0.0 # See https://github.com/SlickQuant/slick-stream-buffer/releases for latest version
)
FetchContent_MakeAvailable(slick-stream-buffer)

target_link_libraries(your_target PRIVATE slick::stream_buffer)
```

## Usage

### Producer: receive bytes, publish on message boundaries

```cpp
#include <slick/stream_buffer.hpp>

// 64 MB data ring, 64K message records; named -> shared memory, nullptr -> local
slick::stream_buffer stream(1ull << 26, 1u << 16, "market_data");

for (;;) {
    auto [ptr, size] = stream.prepare(64 * 1024);
    std::size_t n = receive_bytes(ptr, size);   // e.g. read from a socket
    stream.commit(n);

    // parse the readable area; publish every complete package
    while (std::size_t package_size = find_complete_package(stream.data(), stream.size())) {
        stream.consume(package_size);   // publishes one record - no copy
    }
}
```

### Consumers: independent cursors, zero-copy reads

```cpp
// same process: share the slick::stream_buffer instance with the producer
// another process:
slick::stream_buffer stream("market_data");

uint64_t cursor = stream.initial_reading_index();   // or 0 to replay history
for (;;) {
    auto [data, length] = stream.read(cursor);
    if (data == nullptr) continue;          // nothing new yet - a pause/yield hint belongs here
    handle_package(data, length);           // points directly into the ring
}
```

### Minimal end-to-end example

```cpp
#include <slick/stream_buffer.hpp>

slick::stream_buffer buf(1024, 16);         // capacity bytes, record count (both pow2)

auto [ptr, sz] = buf.prepare(5);
std::memcpy(ptr, "hello", 5);
buf.commit(5);
buf.consume(5);                              // publish "hello" as one record

uint64_t cursor = 0;
auto [data, length] = buf.read(cursor);      // -> "hello", 5
```

## API Overview

The class is `slick::SlickStreamBuffer`; `slick::stream_buffer` is a type alias for it
and the preferred spelling, matching the `slick::stream_buffer` CMake target. Both name
the same type.

### Constructors

```cpp
stream_buffer(uint64_t capacity, uint32_t control_size);                       // local memory
stream_buffer(uint64_t capacity, uint32_t control_size, const char* shm_name); // shm creator
stream_buffer(const char* shm_name);                                           // shm opener
```

`capacity` is the data ring size in bytes; `control_size` is the number of message
records the control ring holds. Both must be powers of 2. Size `control_size` to the
number of messages (not bytes) a slow consumer may lag behind.

The whole segment — header, control ring and data ring — must also fit in a `size_t`. On a
32-bit build a 4 GiB capacity is a valid power of two that cannot be addressed, so the
constructor throws `std::length_error` rather than truncating the size.

The shm creator only initializes a segment it actually created. If the name is already
taken, it attaches to the existing segment instead (`own_buffer()` is then `false`) and
throws unless that segment is a SlickStreamBuffer with matching geometry — a colliding
name never gets overwritten.

### Producer methods (single thread only)

- `std::pair<uint8_t*, size_t> prepare(size_t n)` — contiguous writable region; throws `std::length_error` if `size() + n > capacity()`
- `void commit(size_t n)` — make n prepared bytes readable
- `published_record consume(size_t n)` — publish the first n readable bytes as one message
  record; returns the record exactly as consumers will see it
  (`{sequence, data, length}`, evaluates to `false` if nothing was published); throws
  `std::length_error` if the clamped length would not fit a record's 32-bit length field
- `void discard()` — drop the readable bytes and any prepared region **without publishing**;
  this starts the next connection cleanly but older published records still follow the
  normal lossy overwrite semantics
- `void reset()` — drop every record and restart the sequence numbering. Not thread-safe: call it
  only when no other thread or process is touching the buffer. Consumers whose cursors outlive a
  reset resynchronize on their next `read()` (see Read traits)
- `const uint8_t* data()` / `size_t size()` — the readable (committed, unconsumed) region

### Consumer methods

- `std::pair<const uint8_t*, uint32_t> read<Traits = read_traits>(uint64_t& cursor)` — next message, or `(nullptr, 0)`
- `std::pair<const uint8_t*, uint32_t> read_last()` — most recently published message
- `uint64_t initial_reading_index()` — cursor for late joiners (skip history)
- `uint64_t loss_count()` — messages skipped due to overwrite, counting only reads made with a traits whose `count_loss` is true (see Read traits)

### Read traits

`read()` takes a compile-time configuration parameter:

```cpp
struct slick::read_traits {
    static constexpr bool detect_reset = true;   // resync a cursor that outlived a reset()
    static constexpr bool count_loss   = false;  // accumulate loss_count()
};
```

Ordinary callers write `buf.read(cursor)` and never name a traits type. Loss counting is opt-in
because it is not free; derive when you need it:

```cpp
struct counting : slick::read_traits { static constexpr bool count_loss = true; };
auto [data, len] = buf.read<counting>(cursor);
```

So `loss_count()` stays 0 unless you read through traits that ask for it — records are still
skipped correctly, only the counter is silent.

Why `count_loss` costs anything when nothing is lost: the atomic increment never executes, but
it stays in `read()`'s body and bloats the read loop. On MSVC that measured ~20% of `read()` in
a run that lost nothing. `detect_reset` is a genuine hot-path load, but with `count_loss` off it
measured free, which is why it stays on by default.

A function pointer set by a config call was measured as an alternative — it lets every call site
stay `buf.read(cursor)` — and cost ~32%: the indirect call blocks inlining into the consumer's
polling loop, so the cursor spills to memory every iteration. That is more than the features it
would be switching, hence the template.

The configuration rides on `read()` rather than on the class, so every configuration is still
the same `slick::stream_buffer` type and stays passable to anything taking `stream_buffer&`,
`shared_ptr<stream_buffer>`, or `dynamic_buffer<stream_buffer>`.

The old `SLICK_STREAM_BUFFER_ENABLE_LOSS_DETECTION`, `_RESET_DETECTION` and `_CPU_RELAX`
macros are gone. They changed the body — and, for loss detection, `sizeof` — of a header-only
class, so two translation units built with different values silently violated the ODR. Passing
them now produces a compiler warning and has no effect.

### Accessors

- `uint64_t capacity()` / `uint32_t control_size()` — the configured geometry
- `bool own_buffer()` — this instance created the buffer, rather than attaching to an existing one
- `bool use_shm()` — backed by shared memory rather than local memory

### Shared-memory lifecycle

- `static bool remove(const char* shm_name)` — unlink a segment by name (POSIX; a no-op on
  Windows, where a section dies with its last handle)

A crashed creator can leave a segment behind. On POSIX the name survives the process, so:

- if it died **part-way through initialization**, the segment is stuck in the `INITIALIZING`
  state and every later creator and opener fails construction after a 2-second wait;
- if it died **after initialization**, the segment is intact and gets attached with the dead
  producer's cursors still in it.

Neither is recovered automatically: nothing portable can distinguish a dead creator from a slow
one — a PID stamped in the header means nothing across PID namespaces and is unreliable under
PID reuse — so taking a segment over is an explicit decision. The wedged-segment error names the
step. Call `remove()` once at startup, when the application knows no other process is using the
segment:

```cpp
slick::stream_buffer::remove("my_stream");           // clear anything a previous run left
slick::stream_buffer buf(1 << 20, 4096, "my_stream");
```

On POSIX `remove()` unlinks immediately: the next creator gets a fresh segment, while any process
still mapped to the old one keeps reading the orphaned copy.

## Important Constraints

**Single producer.** All producer methods must be called from one thread. Consumers
are lock-free and independent.

**Lossy semantics.** The producer never blocks. If it laps a slow consumer — by more
than `control_size` messages or `capacity` bytes — the consumer skips ahead and the
loss is counted only if you asked for it. Size the rings so this cannot happen in normal
operation; to verify, read with a traits whose `count_loss` is true (see Read traits) and
check `loss_count()`.

**Pointer invalidation.** `prepare()` may relocate the readable region to keep it
contiguous when the ring wraps; pointers previously returned by `data()`/`prepare()`
are invalidated. Message pointers returned by `read()` stay valid until the producer
laps that part of the ring.

**Record granularity.** Every `consume(n)` call produces exactly one consumer-visible
record. If a protocol layer consumes incrementally, records correspond to those
increments; call `consume()` yourself on package boundaries when you need strict
framing.

**Disconnects mid-message.** If the connection drops after a partial message was
committed, the leftover readable bytes are invalid for the next connection. Call
`discard()` before reconnecting so the partial bytes are not prepended to the new
connection's data. `discard()` does not publish a record, but slow consumers can
still lose older published records if the producer already wrapped a prepared
region over those ring bytes.

**Message size** is limited to < 4 GiB per record: `record::length` is 32 bits, so `consume()`
throws `std::length_error` rather than publishing a truncated length. It throws before touching
any state, so the bytes stay readable and can go out as several smaller records. This can only
be reached on a ring of 4 GiB or larger.

## Architecture

A 64-byte header (cursors + geometry + shared-memory init handshake), a control ring
of 32-byte records `{seq, offset, length}`, and the byte data ring. Records are
published with a release store on `seq` which consumers acquire-load; monotonic
64-bit offsets make wrap-around and lap detection unambiguous. The layout is
identical in local memory and shared memory. Shared-memory creation is gated on the
OS-level "did I create this segment" answer rather than on the in-band init-state slot,
so a name collision with another application can never be initialized over; a segment
that already existed is validated (magic, geometry, mapped size) before it is used, and
the atomic init-state handshake keeps concurrent creator/opener races safe.

## Building and Testing

```bash
cmake -S . -B build
cmake --build build --config Debug
ctest --test-dir build -C Debug --output-on-failure
```

## Benchmarking

```bash
cmake -S . -B build -DBUILD_SLICK_STREAM_BUFFER_BENCH=ON
cmake --build build --config Release
./build/bench/Release/slick-stream-buffer-bench 3 1000000 5   # consumers, messages, reps
```

One producer and N consumers over a single shared-memory segment, in one process so the
threads share the same physical cache lines. The rings are sized so nothing is lost at the
default message count — `missing` in the output must read 0, otherwise the run compares nothing.
Producer and consumer rates come out nearly equal because the two sides are coupled.

Run-to-run spread on an unpinned desktop is around 10%, wider than most changes worth arguing
about. Interleave the variants you compare (A B A B), take the min of several reps, and treat
anything under ~10% as unproven.

Arguments are bounded (consumers 1-1024, messages 1-1e9, reps 1-1000) and rejected outright if
malformed. Exit codes: `0` clean, `1` records were lost, `2` bad arguments, `3` the named
segment already exists and belongs to someone else — the benchmark refuses to become a second
producer on it, and does not remove it, since it may still be in use.

## License

SlickStreamBuffer is released under the [MIT License](LICENSE).

**Made with ⚡ by [SlickQuant](https://github.com/SlickQuant)**
