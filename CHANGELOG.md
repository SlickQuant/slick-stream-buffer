# Changelog

## unreleased
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
