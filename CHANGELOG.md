# Changelog

## v1.0.0

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
