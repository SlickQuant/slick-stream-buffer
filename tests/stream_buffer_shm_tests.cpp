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
#include <slick/stream_buffer.hpp>

#include <atomic>
#include <cstring>
#include <limits>
#include <thread>
#include <vector>

using slick::stream_buffer;

namespace {

void publish_message(stream_buffer& buf, const void* src, std::size_t n) {
    auto [ptr, sz] = buf.prepare(n);
    ASSERT_NE(ptr, nullptr);
    std::memcpy(ptr, src, n);
    buf.commit(n);
    buf.consume(n);
}

}  // namespace

TEST(StreamBufferShmTests, CreatorOpenerRoundtrip) {
    stream_buffer server(1024, 16, "ssb_roundtrip");
    stream_buffer client("ssb_roundtrip");
    EXPECT_TRUE(server.own_buffer());
    EXPECT_FALSE(client.own_buffer());
    EXPECT_TRUE(client.use_shm());

    publish_message(server, "hello shm", 9);

    uint64_t cursor = 0;
    auto [ptr, len] = client.read(cursor);
    ASSERT_NE(ptr, nullptr);
    EXPECT_EQ(len, 9u);
    EXPECT_EQ(std::memcmp(ptr, "hello shm", 9), 0);
    EXPECT_EQ(cursor, 1u);

    EXPECT_EQ(client.read(cursor).first, nullptr);

    publish_message(server, "more", 4);
    auto [p2, l2] = client.read(cursor);
    ASSERT_NE(p2, nullptr);
    EXPECT_EQ(l2, 4u);
    EXPECT_EQ(std::memcmp(p2, "more", 4), 0);
}

TEST(StreamBufferShmTests, OpenerReadsGeometry) {
    stream_buffer server(2048, 32, "ssb_geometry");
    stream_buffer client("ssb_geometry");
    EXPECT_EQ(client.capacity(), 2048u);
    EXPECT_EQ(client.control_size(), 32u);
}

TEST(StreamBufferShmTests, GeometryMismatchThrows) {
    stream_buffer server(1024, 16, "ssb_geometry_mismatch");
    EXPECT_THROW({
        try {
            stream_buffer(2048, 16, "ssb_geometry_mismatch");
        } catch (const std::runtime_error& e) {
            EXPECT_TRUE(std::string(e.what()).find("geometry mismatch") != std::string::npos);
            throw;
        }
    }, std::runtime_error);
    EXPECT_THROW(stream_buffer(1024, 32, "ssb_geometry_mismatch"), std::runtime_error);
}

TEST(StreamBufferShmTests, MagicMismatchThrows) {
    // Build a raw segment that claims to be ready but has the wrong magic
    slick::shm::shared_memory raw(
        "ssb_bad_magic", 4096, slick::shm::open_or_create, slick::shm::access_mode::read_write);
    auto* base = reinterpret_cast<uint8_t*>(raw.data());
    ASSERT_NE(base, nullptr);
    *reinterpret_cast<uint32_t*>(base + 44) = 0xDEADBEEF;                  // header magic slot
    reinterpret_cast<std::atomic<uint32_t>*>(base + 48)->store(3);         // INIT_STATE_READY

    EXPECT_THROW({
        try {
            stream_buffer client("ssb_bad_magic");
        } catch (const std::runtime_error& e) {
            EXPECT_TRUE(std::string(e.what()).find("magic mismatch") != std::string::npos);
            throw;
        }
    }, std::runtime_error);

#if !defined(_MSC_VER)
    slick::shm::shared_memory::remove("ssb_bad_magic");
#endif
}

TEST(StreamBufferShmTests, LateJoinerViaShm) {
    stream_buffer server(1024, 16, "ssb_late_joiner");
    publish_message(server, "old1", 4);
    publish_message(server, "old2", 4);

    stream_buffer client("ssb_late_joiner");
    uint64_t cursor = client.initial_reading_index();
    EXPECT_EQ(cursor, 2u);
    EXPECT_EQ(client.read(cursor).first, nullptr);

    publish_message(server, "fresh", 5);
    auto [ptr, len] = client.read(cursor);
    ASSERT_NE(ptr, nullptr);
    EXPECT_EQ(len, 5u);
    EXPECT_EQ(std::memcmp(ptr, "fresh", 5), 0);
}

#if SLICK_STREAM_BUFFER_ENABLE_LOSS_DETECTION
TEST(StreamBufferShmTests, LossyOverwriteSkipsOldData) {
    stream_buffer server(1024, 4, "ssb_lossy");  // tiny control ring
    stream_buffer client("ssb_lossy");

    for (uint8_t i = 0; i < 8; ++i) {
        publish_message(server, &i, 1);
    }

    uint64_t cursor = 0;
    auto [ptr, len] = client.read(cursor);
    ASSERT_NE(ptr, nullptr);
    EXPECT_EQ(len, 1u);
    EXPECT_EQ(ptr[0], 4);
    EXPECT_EQ(client.loss_count(), 4u);
}
#endif

TEST(StreamBufferShmTests, ReadLastViaShm) {
    stream_buffer server(1024, 16, "ssb_read_last");
    stream_buffer client("ssb_read_last");

    publish_message(server, "first", 5);
    publish_message(server, "second", 6);

    auto [ptr, len] = client.read_last();
    ASSERT_NE(ptr, nullptr);
    EXPECT_EQ(len, 6u);
    EXPECT_EQ(std::memcmp(ptr, "second", 6), 0);
}

TEST(StreamBufferShmTests, BroadcastTwoOpeners) {
    constexpr int kMessages = 200;
    stream_buffer server(1 << 16, 1024, "ssb_broadcast");
    stream_buffer client1("ssb_broadcast");
    stream_buffer client2("ssb_broadcast");

    auto consume_all = [&](stream_buffer& client, std::vector<uint64_t>& out) {
        uint64_t cursor = 0;
        while (out.size() < kMessages) {
            auto [ptr, len] = client.read(cursor);
            if (ptr == nullptr) {
                std::this_thread::yield();
                continue;
            }
            uint64_t value = std::numeric_limits<uint64_t>::max();  // sentinel fails the checks below
            if (len == sizeof(uint64_t)) {
                std::memcpy(&value, ptr, sizeof(value));
            }
            out.push_back(value);
        }
    };

    std::vector<uint64_t> received1, received2;
    std::thread c1([&] { consume_all(client1, received1); });
    std::thread c2([&] { consume_all(client2, received2); });

    for (uint64_t i = 0; i < kMessages; ++i) {
        auto [ptr, sz] = server.prepare(sizeof(uint64_t));
        ASSERT_NE(ptr, nullptr);
        std::memcpy(ptr, &i, sizeof(i));
        server.commit(sizeof(i));
        server.consume(sizeof(i));
    }

    c1.join();
    c2.join();

    ASSERT_EQ(received1.size(), kMessages);
    ASSERT_EQ(received2.size(), kMessages);
    for (uint64_t i = 0; i < kMessages; ++i) {
        EXPECT_EQ(received1[i], i);
        EXPECT_EQ(received2[i], i);
    }
}
