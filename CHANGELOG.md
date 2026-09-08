# Changelog

## v2.0.0 -

**Breaking**
- The `SLICK_STREAM_BUFFER_ENABLE_LOSS_DETECTION`, `_RESET_DETECTION` and `_CPU_RELAX` macros are
  replaced by a Traits parameter on `read()` (`slick::read_traits`); passing them now warns and
  does nothing. They changed `read()`'s body - and, for loss detection, `sizeof` (576 vs 640
  bytes) - of a header-only class, so translation units built with different values silently
  violated the ODR across a `-DNDEBUG` boundary. `loss_count_` is now an unconditional member, so
  `sizeof` no longer depends on build flags. The traits sit on `read()` rather than on the class,
  so every configuration is still the same `slick::stream_buffer` type and still passes to
  anything taking `stream_buffer&`, `shared_ptr<stream_buffer>` or `dynamic_buffer<stream_buffer>`.
- `loss_count()` now reads 0 unless you read through traits with `count_loss = true` - records are
  still skipped correctly, only the counter is silent. Counting costs ~20% of `read()` on MSVC even
  when nothing is lost. `detect_reset` stays on by default, having measured free once `count_loss`
  is off.
- `consume()` is no longer `noexcept`: it throws `std::length_error` instead of truncating a record
  of 4 GiB or more. The limit was a Debug-only assert, so Release cast the length to `uint32_t` and
  published a wrong one. Checked before any state moves, so the bytes stay readable and can go out
  as several smaller records.
- The pause hint is gone from `read()`'s data-lap retry. That loop skips forward every iteration
  rather than waiting on anything, so backing off only widened the gap it was closing: ~20% *more*
  records lost with it than without. A pause belongs in the caller's own poll loop.
- `cmake_minimum_required` raised to 3.21 - `PROJECT_IS_TOP_LEVEL` needs it, so the stated 3.10
  could never configure - and the C++20 requirement now travels with the target via
  `target_compile_features(... INTERFACE cxx_std_20)`. `CMAKE_CXX_STANDARD` applied only to this
  build, so installed consumers compiled the headers as C++17 inherited from `slick::shm`, which
  silently drops the `[[likely]]`/`[[unlikely]]` hints.
- Not breaking: the shared-memory format is unchanged (`SSB1`), so a segment written by 1.0.5 is
  still readable. Every process sharing one must be rebuilt together, though - `sizeof` changed.

**Fixed**
- A creator could overwrite a shared-memory segment belonging to another application.
  Initialization authority now comes from the OS-backed `is_creator()` of `open_or_create` instead
  of a CAS on the in-band init-state slot, which cannot tell a fresh segment from a zero-filled
  foreign one. An existing segment is validated (magic, geometry, mapped size) before a byte is
  written, and the creator no longer claims ownership of it - so it no longer unlinks someone
  else's segment on POSIX.
- A header declaring more capacity than the mapping holds is rejected instead of yielding
  out-of-bounds control/data pointers.
- Segment sizes are computed in `uint64_t`. On 32-bit builds a valid 4 GiB capacity wrapped in
  `size_t`: shared memory created a 4096-byte mapping that passed every later size check against
  its own truncated value, and the local path's `new uint8_t[capacity_]` truncated to a zero-byte
  allocation. The constructor now throws `std::length_error` when the geometry cannot be addressed.
- The init handshake fails fast when the segment carries a foreign magic, and its 2-second timeout
  is a real wall-clock deadline - it counted 1 ms sleeps before, which stretched to ~31 s on
  Windows at the default timer granularity.

**Added**
- `stream_buffer::remove(name)`, the explicit stale-segment recovery step. Construction over a
  segment stuck in `INITIALIZING` (a creator that died mid-initialization) now fails with a message
  naming it. Recovery is deliberately manual: nothing portable distinguishes a dead creator from a
  slow one, since a PID stamped in the header is meaningless across PID namespaces and unreliable
  under PID reuse.
- `bench/` (`-DBUILD_SLICK_STREAM_BUFFER_BENCH=ON`): one producer, N consumers, one shared segment,
  sized so no record is lost.

**Performance**
- Producer-side shadows for `next_seq_` and `reserve_end_`, matching the committed/consumed ones:
  publishing no longer re-loads counters only the producer writes.
- `read()`'s data-lap check loads `reserve_end_` relaxed instead of acquire - the load publishes
  nothing, since the bytes are ordered by the acquire on `record::seq`. Identical codegen on x86;
  one fewer `ldar` per record on ARM.
- Net effect: ~16% faster end-to-end at 3 consumers than the previous Debug default.

## v1.0.5 - 2026-06-16
- Renamed canonical header from `slick/stream_buffer.h` to `slick/stream_buffer.hpp`.
  The old `.h` path is kept as a backward-compatibility shim that re-exports the new
  header and emits a compiler warning directing users to update their includes.

## v1.0.4 - 2026-06-12
- Separatad slick/dynamic_stream_buffer.h into slick-dynamic-buffer repo due to vcpk dependencies issue
- Added `slick::stream_buffer` as the preferred snake_case spelling of
  `slick::SlickStreamBuffer`, matching the `slick::stream_buffer` CMake target and the
  Boost-style naming of the `slick::dynamic_buffer` adapter. `SlickStreamBuffer`
  remains available for backward compatibility.

## v1.0.3 - 2026-06-11
- `slick/dynamic_stream_buffer.h` is now installed only when Boost is found at
  configure time, and the header emits a clear `#error` when included without
  Boost.Asio on the include path. The core `slick/stream_buffer.h` remains
  Boost-free.

## v1.0.2 - 2026-06-11
- Added `SlickStreamBuffer::discard()` to drop committed-but-unconsumed bytes (and any
  prepared region) without publishing them — for invalidating a partial message after a
  connection drops mid-read. Older published records remain subject to the existing
  lossy overwrite semantics.
- Added `dynamic_stream_buffer::clear()` (matching `beast::flat_buffer::clear()`), which
  forwards to `discard()`.
- Update slick-shm fetching version to 0.1.4
- Auto fetch slick-shm if not found in config cmake

## v1.0.1 - 2026-06-10

- Fixed `SlickStreamBuffer::prepare(0)` so it no longer discards an outstanding
  prepared region before `commit()`.
- Fixed `dynamic_stream_buffer::data()` so its returned buffer size respects the
  adapter `max_size()` cap and stays consistent with `size()`.
- Reduced producer-side atomic load traffic by keeping local committed/consumed
  cursor shadows for the single-producer hot path.
- Added regression tests for zero-length prepare handling and DynamicBuffer
  `data()`/`size()` consistency.

## v1.0.0 - 2026-06-10

Initial release.

- `slick::SlickStreamBuffer` — lock-free single-producer multi-consumer byte stream
  buffer with local memory and shared memory support (slick-shm), monotonic message
  records, independent consumer cursors, lossy overwrite semantics with loss
  detection, and contiguous readable region with wrap relocation.
- `slick::dynamic_stream_buffer` — Boost.Asio `DynamicBuffer_v1` adapter, drop-in
  replacement for `boost::beast::flat_buffer`; `consume(n)` publishes the consumed
  bytes to consumers as one message record (zero-copy fan-out).
- GoogleTest suites for local memory, shared memory, and the asio adapter
  (including a real TCP loopback test); CI for Windows/Linux/macOS.
