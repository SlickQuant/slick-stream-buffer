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

#include <gtest/gtest.h>
#include <slick/stream_buffer.h>

#include <atomic>
#include <cstring>
#include <thread>
#include <type_traits>
#include <vector>

using slick::stream_buffer;

// SlickStreamBuffer is the compatibility spelling - it must keep naming the same type
static_assert(std::is_same_v<slick::stream_buffer, slick::SlickStreamBuffer>,
              "slick::stream_buffer must alias slick::SlickStreamBuffer");

namespace {

// prepare + write + commit in one step
void write_bytes(stream_buffer& buf, const void* src, std::size_t n) {
    auto [ptr, sz] = buf.prepare(n);
    ASSERT_NE(ptr, nullptr);
    ASSERT_EQ(sz, n);
    std::memcpy(ptr, src, n);
    buf.commit(n);
}

// publish one message of n bytes filled with the given byte value
void publish_filled(stream_buffer& buf, uint8_t value, std::size_t n) {
    auto [ptr, sz] = buf.prepare(n);
    ASSERT_NE(ptr, nullptr);
    std::memset(ptr, value, n);
    buf.commit(n);
    buf.consume(n);
}

}  // namespace

TEST(StreamBufferTests, EmptyReadReturnsNull) {
    stream_buffer buf(1024, 16);
    uint64_t cursor = 0;
    auto [ptr, len] = buf.read(cursor);
    EXPECT_EQ(ptr, nullptr);
    EXPECT_EQ(len, 0u);
    EXPECT_EQ(cursor, 0u);
    EXPECT_EQ(buf.size(), 0u);
    EXPECT_EQ(buf.capacity(), 1024u);
    EXPECT_EQ(buf.control_size(), 16u);
    EXPECT_TRUE(buf.own_buffer());
    EXPECT_FALSE(buf.use_shm());
}

TEST(StreamBufferTests, InvalidSizesThrow) {
    EXPECT_THROW(stream_buffer(1000, 16), std::invalid_argument);   // capacity not pow2
    EXPECT_THROW(stream_buffer(1024, 15), std::invalid_argument);   // control_size not pow2
    EXPECT_THROW(stream_buffer(0, 16), std::invalid_argument);
    EXPECT_THROW(stream_buffer(1024, 0), std::invalid_argument);
}

TEST(StreamBufferTests, PrepareCommitConsumeReadRoundtrip) {
    stream_buffer buf(1024, 16);
    write_bytes(buf, "hello", 5);
    EXPECT_EQ(buf.size(), 5u);
    EXPECT_EQ(std::memcmp(buf.data(), "hello", 5), 0);

    auto record = buf.consume(5);
    EXPECT_EQ(buf.size(), 0u);

    // consume() returns the record exactly as consumers will see it
    ASSERT_TRUE(static_cast<bool>(record));
    EXPECT_EQ(record.sequence, 0u);
    EXPECT_EQ(record.length, 5u);
    EXPECT_EQ(std::memcmp(record.data, "hello", 5), 0);

    uint64_t cursor = 0;
    auto [ptr, len] = buf.read(cursor);
    ASSERT_NE(ptr, nullptr);
    EXPECT_EQ(len, 5u);
    EXPECT_EQ(std::memcmp(ptr, "hello", 5), 0);
    EXPECT_EQ(cursor, 1u);
    EXPECT_EQ(ptr, record.data);  // identical view, zero-copy

    auto [ptr2, len2] = buf.read(cursor);
    EXPECT_EQ(ptr2, nullptr);
    EXPECT_EQ(len2, 0u);
}

TEST(StreamBufferTests, ConsumeSplitsIntoRecords) {
    stream_buffer buf(1024, 16);
    write_bytes(buf, "0123456789", 10);
    auto r1 = buf.consume(4);
    auto r2 = buf.consume(6);
    EXPECT_EQ(buf.size(), 0u);
    EXPECT_EQ(r1.sequence, 0u);
    EXPECT_EQ(r2.sequence, 1u);
    EXPECT_EQ(r1.length, 4u);
    EXPECT_EQ(r2.length, 6u);

    uint64_t cursor = 0;
    auto [p1, l1] = buf.read(cursor);
    ASSERT_NE(p1, nullptr);
    EXPECT_EQ(l1, 4u);
    EXPECT_EQ(std::memcmp(p1, "0123", 4), 0);

    auto [p2, l2] = buf.read(cursor);
    ASSERT_NE(p2, nullptr);
    EXPECT_EQ(l2, 6u);
    EXPECT_EQ(std::memcmp(p2, "456789", 6), 0);
}

TEST(StreamBufferTests, PartialCommitKeepsRemainderPrepared) {
    stream_buffer buf(1024, 16);
    auto [ptr, sz] = buf.prepare(8);
    ASSERT_EQ(sz, 8u);
    std::memcpy(ptr, "abcdefgh", 8);
    buf.commit(3);
    EXPECT_EQ(buf.size(), 3u);
    buf.commit(5);
    EXPECT_EQ(buf.size(), 8u);
    EXPECT_EQ(std::memcmp(buf.data(), "abcdefgh", 8), 0);
}

TEST(StreamBufferTests, RePrepareWithoutCommit) {
    stream_buffer buf(1024, 16);
    auto [p1, s1] = buf.prepare(8);
    (void)p1; (void)s1;
    EXPECT_EQ(buf.size(), 0u);

    auto [p2, s2] = buf.prepare(16);
    ASSERT_EQ(s2, 16u);
    std::memset(p2, 'x', 16);
    buf.commit(16);
    EXPECT_EQ(buf.size(), 16u);
}

TEST(StreamBufferTests, PrepareZeroDoesNotDiscardPreparedBytes) {
    stream_buffer buf(1024, 16);
    auto [ptr, sz] = buf.prepare(8);
    ASSERT_EQ(sz, 8u);
    std::memcpy(ptr, "abcdefgh", 8);

    auto [empty_ptr, empty_sz] = buf.prepare(0);
    (void)empty_ptr;
    EXPECT_EQ(empty_sz, 0u);

    buf.commit(8);
    EXPECT_EQ(buf.size(), 8u);
    EXPECT_EQ(std::memcmp(buf.data(), "abcdefgh", 8), 0);
}

TEST(StreamBufferTests, CommitMoreThanPreparedClamps) {
    stream_buffer buf(1024, 16);
    auto [ptr, sz] = buf.prepare(8);
    std::memset(ptr, 'y', sz);
    buf.commit(100);
    EXPECT_EQ(buf.size(), 8u);
}

TEST(StreamBufferTests, ConsumeMoreThanSizeClamps) {
    stream_buffer buf(1024, 16);
    write_bytes(buf, "abcde", 5);
    auto record = buf.consume(100);  // clamps to 5, publishes one 5-byte record
    EXPECT_EQ(buf.size(), 0u);
    EXPECT_EQ(record.length, 5u);

    uint64_t cursor = 0;
    auto [ptr, len] = buf.read(cursor);
    ASSERT_NE(ptr, nullptr);
    EXPECT_EQ(len, 5u);
}

TEST(StreamBufferTests, ConsumeZeroIsNoop) {
    stream_buffer buf(1024, 16);
    write_bytes(buf, "abc", 3);
    auto record = buf.consume(0);
    EXPECT_FALSE(static_cast<bool>(record));  // nothing published
    EXPECT_EQ(record.data, nullptr);
    EXPECT_EQ(record.length, 0u);
    EXPECT_EQ(buf.size(), 3u);

    uint64_t cursor = 0;
    auto [ptr, len] = buf.read(cursor);
    EXPECT_EQ(ptr, nullptr);
    EXPECT_EQ(len, 0u);

    // empty buffer: consume(0) publishes nothing either
    buf.consume(3);
    buf.consume(0);
    auto [p1, l1] = buf.read(cursor);
    EXPECT_NE(p1, nullptr);
    EXPECT_EQ(l1, 3u);
    auto [p2, l2] = buf.read(cursor);
    EXPECT_EQ(p2, nullptr);
    EXPECT_EQ(l2, 0u);
}

TEST(StreamBufferTests, PrepareTooLargeThrowsLengthError) {
    stream_buffer buf(64, 16);
    // larger than the whole ring
    EXPECT_THROW(buf.prepare(65), std::length_error);

    // unconsumed bytes + n exceed capacity even though n alone fits
    auto [ptr, sz] = buf.prepare(10);
    std::memset(ptr, 'z', sz);
    buf.commit(10);
    EXPECT_THROW(buf.prepare(55), std::length_error);
    // boundary: exactly capacity is fine
    EXPECT_NO_THROW(buf.prepare(54));
}

TEST(StreamBufferTests, WrapRelocationPreservesUnconsumedBytes) {
    stream_buffer buf(64, 16);

    // fill 48 bytes 0..47, publish the first 40, keep 8 unconsumed
    uint8_t src[80];
    for (int i = 0; i < 80; ++i) src[i] = static_cast<uint8_t>(i);
    write_bytes(buf, src, 48);
    buf.consume(40);

    // read the published record before its bytes get overwritten
    uint64_t cursor = 0;
    auto [p0, l0] = buf.read(cursor);
    ASSERT_NE(p0, nullptr);
    ASSERT_EQ(l0, 40u);
    EXPECT_EQ(std::memcmp(p0, src, 40), 0);

    // prepare(32) doesn't fit at ring position 48 -> relocates the 8 unconsumed bytes to ring start
    auto [p1, s1] = buf.prepare(32);
    ASSERT_EQ(s1, 32u);
    EXPECT_EQ(buf.size(), 8u);
    EXPECT_EQ(std::memcmp(buf.data(), src + 40, 8), 0);  // bytes 40..47 preserved and contiguous

    std::memcpy(p1, src + 48, 32);
    buf.commit(32);
    EXPECT_EQ(buf.size(), 40u);
    EXPECT_EQ(std::memcmp(buf.data(), src + 40, 40), 0);

    // publish the relocated region as one record and read it back
    buf.consume(40);
    auto [p2, l2] = buf.read(cursor);
    ASSERT_NE(p2, nullptr);
    ASSERT_EQ(l2, 40u);
    EXPECT_EQ(std::memcmp(p2, src + 40, 40), 0);
    EXPECT_EQ(buf.loss_count(), 0u);
}

TEST(StreamBufferTests, RecordsAcrossJumpReadInOrder) {
    stream_buffer buf(64, 16);
    uint64_t cursor = 0;

    publish_filled(buf, 'A', 40);   // record 0: ring [0, 40)

    // consume record 0 before its ring bytes can be reused
    auto [p0, l0] = buf.read(cursor);
    ASSERT_NE(p0, nullptr);
    ASSERT_EQ(l0, 40u);

    publish_filled(buf, 'B', 16);   // record 1: ring [40, 56)
    publish_filled(buf, 'C', 16);   // forces jump (pos 56 + 16 > 64); record 2: ring [0, 16)

    auto [p1, l1] = buf.read(cursor);
    ASSERT_NE(p1, nullptr);
    ASSERT_EQ(l1, 16u);
    for (uint32_t i = 0; i < l1; ++i) EXPECT_EQ(p1[i], 'B');

    auto [p2, l2] = buf.read(cursor);
    ASSERT_NE(p2, nullptr);
    ASSERT_EQ(l2, 16u);
    for (uint32_t i = 0; i < l2; ++i) EXPECT_EQ(p2[i], 'C');

    EXPECT_EQ(buf.loss_count(), 0u);
    EXPECT_EQ(cursor, 3u);
}

#if SLICK_STREAM_BUFFER_ENABLE_LOSS_DETECTION
TEST(StreamBufferTests, ControlRingLappingDetectsLoss) {
    stream_buffer buf(256, 4);  // tiny control ring

    for (int i = 0; i < 8; ++i) {
        publish_filled(buf, static_cast<uint8_t>(i), 1);
    }

    // slots now hold records 4..7; a cursor at 0 must skip to 4 and count the loss
    uint64_t cursor = 0;
    auto [ptr, len] = buf.read(cursor);
    ASSERT_NE(ptr, nullptr);
    EXPECT_EQ(len, 1u);
    EXPECT_EQ(ptr[0], 4);
    EXPECT_EQ(buf.loss_count(), 4u);
    EXPECT_EQ(cursor, 5u);

    for (int i = 5; i < 8; ++i) {
        auto [p, l] = buf.read(cursor);
        ASSERT_NE(p, nullptr);
        EXPECT_EQ(p[0], i);
    }
}

TEST(StreamBufferTests, DataRingLappingDetectsLoss) {
    stream_buffer buf(64, 64);  // control ring never laps, data ring does

    for (int i = 0; i < 4; ++i) {
        publish_filled(buf, static_cast<uint8_t>(i), 32);  // records at offsets 0, 32, 64, 96
    }

    // record 0 and 1 bytes were overwritten by records 2 and 3; the validity check must skip them
    uint64_t cursor = 0;
    auto [ptr, len] = buf.read(cursor);
    ASSERT_NE(ptr, nullptr);
    EXPECT_EQ(len, 32u);
    EXPECT_EQ(ptr[0], 2);
    EXPECT_EQ(buf.loss_count(), 2u);

    auto [p3, l3] = buf.read(cursor);
    ASSERT_NE(p3, nullptr);
    EXPECT_EQ(p3[0], 3);
}
#endif

TEST(StreamBufferTests, ReadLastReturnsNewestRecord) {
    stream_buffer buf(1024, 16);
    EXPECT_EQ(buf.read_last().first, nullptr);

    publish_filled(buf, 'a', 4);
    publish_filled(buf, 'b', 7);

    auto [ptr, len] = buf.read_last();
    ASSERT_NE(ptr, nullptr);
    EXPECT_EQ(len, 7u);
    EXPECT_EQ(ptr[0], 'b');
}

TEST(StreamBufferTests, LateJoinerInitialReadingIndex) {
    stream_buffer buf(1024, 16);
    publish_filled(buf, 'a', 3);
    publish_filled(buf, 'b', 3);
    publish_filled(buf, 'c', 3);

    uint64_t cursor = buf.initial_reading_index();
    EXPECT_EQ(cursor, 3u);
    EXPECT_EQ(buf.read(cursor).first, nullptr);  // nothing new yet

    publish_filled(buf, 'd', 5);
    auto [ptr, len] = buf.read(cursor);
    ASSERT_NE(ptr, nullptr);
    EXPECT_EQ(len, 5u);
    EXPECT_EQ(ptr[0], 'd');
    EXPECT_EQ(buf.read(cursor).first, nullptr);
}

TEST(StreamBufferTests, MultiConsumerBroadcast) {
    constexpr int kMessages = 500;
    constexpr int kConsumers = 3;
    stream_buffer buf(1 << 16, 1024);  // sized so nothing laps

    std::vector<std::thread> consumers;
    std::vector<int> received(kConsumers, 0);
    std::vector<bool> ok(kConsumers, true);

    for (int c = 0; c < kConsumers; ++c) {
        consumers.emplace_back([&, c] {
            uint64_t cursor = 0;
            uint64_t expected = 0;
            while (received[c] < kMessages) {
                auto [ptr, len] = buf.read(cursor);
                if (ptr == nullptr) {
                    std::this_thread::yield();
                    continue;
                }
                if (len != sizeof(uint64_t)) { ok[c] = false; break; }
                uint64_t value;
                std::memcpy(&value, ptr, sizeof(value));
                if (value != expected) { ok[c] = false; break; }
                ++expected;
                ++received[c];
            }
        });
    }

    for (uint64_t i = 0; i < kMessages; ++i) {
        auto [ptr, sz] = buf.prepare(sizeof(uint64_t));
        ASSERT_NE(ptr, nullptr);
        std::memcpy(ptr, &i, sizeof(i));
        buf.commit(sizeof(i));
        buf.consume(sizeof(i));
    }

    for (auto& t : consumers) t.join();
    for (int c = 0; c < kConsumers; ++c) {
        EXPECT_TRUE(ok[c]) << "consumer " << c << " observed unexpected data";
        EXPECT_EQ(received[c], kMessages);
    }
    EXPECT_EQ(buf.loss_count(), 0u);
}

TEST(StreamBufferTests, DiscardInvalidatesPartialMessage) {
    stream_buffer buf(1024, 16);
    uint64_t cursor = 0;

    // a complete message is published, then a partial message is committed
    // before the connection drops
    write_bytes(buf, "complete", 8);
    buf.consume(8);
    write_bytes(buf, "part", 4);
    ASSERT_EQ(buf.size(), 4u);

    buf.discard();
    EXPECT_EQ(buf.size(), 0u);

    // the published record is still readable because the discarded bytes did not
    // wrap over it
    auto [p0, l0] = buf.read(cursor);
    ASSERT_NE(p0, nullptr);
    ASSERT_EQ(l0, 8u);
    EXPECT_EQ(std::memcmp(p0, "complete", 8), 0);

    // new connection: fresh bytes must not be contaminated by the discarded ones
    write_bytes(buf, "fresh", 5);
    EXPECT_EQ(buf.size(), 5u);
    EXPECT_EQ(std::memcmp(buf.data(), "fresh", 5), 0);
    buf.consume(5);
    auto [p1, l1] = buf.read(cursor);
    ASSERT_NE(p1, nullptr);
    ASSERT_EQ(l1, 5u);
    EXPECT_EQ(std::memcmp(p1, "fresh", 5), 0);
    EXPECT_EQ(buf.loss_count(), 0u);
}

TEST(StreamBufferTests, DiscardKeepsOverwriteLossForLaggingConsumers) {
    stream_buffer buf(64, 16);

    publish_filled(buf, 'A', 40);

    // A wrapped prepare can overwrite bytes of an older published record before
    // the partial message is discarded. A lagging consumer must still observe loss.
    auto [ptr, sz] = buf.prepare(32);
    ASSERT_EQ(sz, 32u);
    std::memset(ptr, 'X', sz);
    buf.discard();

    uint64_t cursor = 0;
    auto [p0, l0] = buf.read(cursor);
    EXPECT_EQ(p0, nullptr);
    EXPECT_EQ(l0, 0u);
    EXPECT_EQ(cursor, 1u);
    EXPECT_EQ(buf.loss_count(), 1u);

    write_bytes(buf, "ok", 2);
    buf.consume(2);
    auto [p1, l1] = buf.read(cursor);
    ASSERT_NE(p1, nullptr);
    ASSERT_EQ(l1, 2u);
    EXPECT_EQ(std::memcmp(p1, "ok", 2), 0);
}

TEST(StreamBufferTests, DiscardDropsPreparedRegion) {
    stream_buffer buf(1024, 16);
    auto [ptr, sz] = buf.prepare(8);
    std::memset(ptr, 'x', sz);

    buf.discard();
    buf.commit(8);  // a stale commit after discard must commit nothing
    EXPECT_EQ(buf.size(), 0u);

    write_bytes(buf, "ok", 2);
    EXPECT_EQ(buf.size(), 2u);
    EXPECT_EQ(std::memcmp(buf.data(), "ok", 2), 0);
}

TEST(StreamBufferTests, DiscardThenRefillAcrossWrap) {
    stream_buffer buf(64, 16);
    uint64_t cursor = 0;

    publish_filled(buf, 'A', 40);
    auto [p0, l0] = buf.read(cursor);
    ASSERT_NE(p0, nullptr);
    ASSERT_EQ(l0, 40u);

    // partial message committed near the end of the ring, then dropped;
    // reserve_end_ stays at its high-water mark and must not cause false loss
    auto [ptr, sz] = buf.prepare(20);
    ASSERT_EQ(sz, 20u);
    std::memset(ptr, 'X', sz);
    buf.commit(20);
    buf.discard();
    EXPECT_EQ(buf.size(), 0u);

    // refill across the wrap and read every record back intact
    for (int i = 0; i < 4; ++i) {
        publish_filled(buf, static_cast<uint8_t>(i), 16);
        auto [p, l] = buf.read(cursor);
        ASSERT_NE(p, nullptr);
        ASSERT_EQ(l, 16u);
        for (uint32_t j = 0; j < l; ++j) EXPECT_EQ(p[j], i);
    }
    EXPECT_EQ(buf.loss_count(), 0u);
}

TEST(StreamBufferTests, DiscardOnEmptyBufferIsNoop) {
    stream_buffer buf(1024, 16);
    buf.discard();
    EXPECT_EQ(buf.size(), 0u);

    publish_filled(buf, 'a', 3);
    buf.discard();  // nothing unconsumed - published record stays readable
    uint64_t cursor = 0;
    auto [ptr, len] = buf.read(cursor);
    ASSERT_NE(ptr, nullptr);
    EXPECT_EQ(len, 3u);
}

TEST(StreamBufferTests, Reset) {
    stream_buffer buf(1024, 16);
    publish_filled(buf, 'a', 10);
    write_bytes(buf, "leftover", 8);
    ASSERT_EQ(buf.size(), 8u);

    buf.reset();
    EXPECT_EQ(buf.size(), 0u);
    EXPECT_EQ(buf.loss_count(), 0u);
    EXPECT_EQ(buf.initial_reading_index(), 0u);
    EXPECT_EQ(buf.read_last().first, nullptr);

    uint64_t cursor = 0;
    EXPECT_EQ(buf.read(cursor).first, nullptr);

    publish_filled(buf, 'z', 6);
    auto [ptr, len] = buf.read(cursor);
    ASSERT_NE(ptr, nullptr);
    EXPECT_EQ(len, 6u);
    EXPECT_EQ(ptr[0], 'z');
}
