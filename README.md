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

- C++20 compatible compiler
- [slick-shm](https://github.com/SlickQuant/slick-shm) (fetched automatically when not installed)

## Installation

Header-only. Add the `include` directory to your include path:

```cpp
#include <slick/stream_buffer.h>
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
    GIT_TAG v1.0.0 # See https://github.com/SlickQuant/slick-stream-buffer/releases for latest version
)
FetchContent_MakeAvailable(slick-stream-buffer)

target_link_libraries(your_target PRIVATE slick::stream_buffer)
```

## Usage

### Producer: receive bytes, publish on message boundaries

```cpp
#include <slick/stream_buffer.h>

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
    if (data == nullptr) continue;          // nothing new yet
    handle_package(data, length);           // points directly into the ring
}
```

### Minimal end-to-end example

```cpp
#include <slick/stream_buffer.h>

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

### Producer methods (single thread only)

- `std::pair<uint8_t*, size_t> prepare(size_t n)` — contiguous writable region; throws `std::length_error` if `size() + n > capacity()`
- `void commit(size_t n)` — make n prepared bytes readable
- `published_record consume(size_t n)` — publish the first n readable bytes as one message
  record; returns the record exactly as consumers will see it
  (`{sequence, data, length}`, evaluates to `false` if nothing was published)
- `void discard()` — drop the readable bytes and any prepared region **without publishing**;
  this starts the next connection cleanly but older published records still follow the
  normal lossy overwrite semantics
- `const uint8_t* data()` / `size_t size()` — the readable (committed, unconsumed) region

### Consumer methods

- `std::pair<const uint8_t*, uint32_t> read(uint64_t& cursor)` — next message, or `(nullptr, 0)`
- `std::pair<const uint8_t*, uint32_t> read_last()` — most recently published message
- `uint64_t initial_reading_index()` — cursor for late joiners (skip history)
- `uint64_t loss_count()` — messages skipped due to overwrite (debug-only unless enabled)

## Important Constraints

**Single producer.** All producer methods must be called from one thread. Consumers
are lock-free and independent.

**Lossy semantics.** The producer never blocks. If it laps a slow consumer — by more
than `control_size` messages or `capacity` bytes — the consumer skips ahead and the
loss is counted. Size the rings so this cannot happen in normal operation; define
`SLICK_STREAM_BUFFER_ENABLE_LOSS_DETECTION=1` (default in Debug) and check
`loss_count()`.

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

**Message size** is limited to < 4 GiB per record.

## Architecture

A 64-byte header (cursors + geometry + shared-memory init handshake), a control ring
of 32-byte records `{seq, offset, length}`, and the byte data ring. Records are
published with a release store on `seq` which consumers acquire-load; monotonic
64-bit offsets make wrap-around and lap detection unambiguous. The layout is
identical in local memory and shared memory, and shared-memory creation uses an
atomic init-state handshake so creator/opener races are safe.

## Building and Testing

```bash
cmake -S . -B build
cmake --build build --config Debug
ctest --test-dir build -C Debug --output-on-failure
```

## License

SlickStreamBuffer is released under the [MIT License](LICENSE).

**Made with ⚡ by [SlickQuant](https://github.com/SlickQuant)**
