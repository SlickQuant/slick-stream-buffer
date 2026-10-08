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
#include <memory>
#include <string>
#include <thread>
#include <vector>

using slick::stream_buffer;

// See the note in stream_buffer_tests.cpp: these assert on loss_count(), which is opt-in.
namespace { struct dbg : slick::read_traits { static constexpr bool count_loss = true; }; }

namespace {

// Mirrors the shared-memory header layout. The raw-segment tests below poke these offsets
// directly, so a layout change has to be reflected here on purpose.
constexpr std::size_t kHeaderSize = 64;
constexpr std::size_t kMagicOffset = 44;
constexpr std::size_t kInitStateOffset = 48;
constexpr std::size_t kCapacityOffset = 32;
constexpr std::size_t kControlSizeOffset = 40;
constexpr uint32_t kMagic = 0x53534231;  // 'SSB1'
constexpr uint32_t kStateInitializing = 2;
constexpr uint32_t kStateReady = 3;

void publish_message(stream_buffer& buf, const void* src, std::size_t n) {
    auto [ptr, sz] = buf.prepare(n);
    ASSERT_NE(ptr, nullptr);
    std::memcpy(ptr, src, n);
    buf.commit(n);
    buf.consume(n);
}

}  // namespace

/// A buffer never unlinks its segment, so on POSIX every name a test uses would outlive the
/// test and hand the next run a stale segment. shm() unlinks a name the first time a test
/// uses it, clearing whatever a crashed run left behind, and TearDown() unlinks it again
/// once the test's buffers are gone.
class StreamBufferShmTests : public ::testing::Test {
protected:
    const char* shm(const char* name) {
        for (const auto& used : names_) {
            if (used == name) {
                return name;
            }
        }
        slick::shm::shared_memory::remove(name);
        names_.emplace_back(name);
        return name;
    }

    void TearDown() override {
        for (const auto& name : names_) {
            slick::shm::shared_memory::remove(name.c_str());
        }
    }

private:
    std::vector<std::string> names_;
};

TEST_F(StreamBufferShmTests, CreatorOpenerRoundtrip) {
    stream_buffer server(1024, 16, shm("ssb_roundtrip"));
    stream_buffer client(shm("ssb_roundtrip"));
    EXPECT_TRUE(server.own_buffer());
    EXPECT_FALSE(client.own_buffer());
    EXPECT_TRUE(client.use_shm());

    publish_message(server, "hello shm", 9);

    uint64_t cursor = 0;
    auto [ptr, len] = client.read<dbg>(cursor);
    ASSERT_NE(ptr, nullptr);
    EXPECT_EQ(len, 9u);
    EXPECT_EQ(std::memcmp(ptr, "hello shm", 9), 0);
    EXPECT_EQ(cursor, 1u);

    EXPECT_EQ(client.read<dbg>(cursor).first, nullptr);

    publish_message(server, "more", 4);
    auto [p2, l2] = client.read<dbg>(cursor);
    ASSERT_NE(p2, nullptr);
    EXPECT_EQ(l2, 4u);
    EXPECT_EQ(std::memcmp(p2, "more", 4), 0);
}

TEST_F(StreamBufferShmTests, OpenerReadsGeometry) {
    stream_buffer server(2048, 32, shm("ssb_geometry"));
    stream_buffer client(shm("ssb_geometry"));
    EXPECT_EQ(client.capacity(), 2048u);
    EXPECT_EQ(client.control_size(), 32u);
}

TEST_F(StreamBufferShmTests, GeometryMismatchThrows) {
    stream_buffer server(1024, 16, shm("ssb_geometry_mismatch"));
    EXPECT_THROW({
        try {
            stream_buffer(2048, 16, shm("ssb_geometry_mismatch"));
        } catch (const std::runtime_error& e) {
            EXPECT_TRUE(std::string(e.what()).find("geometry mismatch") != std::string::npos);
            throw;
        }
    }, std::runtime_error);
    EXPECT_THROW(stream_buffer(1024, 32, shm("ssb_geometry_mismatch")), std::runtime_error);
}

TEST_F(StreamBufferShmTests, MagicMismatchThrows) {
    // Build a raw segment that claims to be ready but has the wrong magic
    slick::shm::shared_memory raw(
        shm("ssb_bad_magic"), 4096, slick::shm::open_or_create, slick::shm::access_mode::read_write);
    auto* base = reinterpret_cast<uint8_t*>(raw.data());
    ASSERT_NE(base, nullptr);
    *reinterpret_cast<uint32_t*>(base + kMagicOffset) = 0xDEADBEEF;
    reinterpret_cast<std::atomic<uint32_t>*>(base + kInitStateOffset)->store(kStateReady);

    EXPECT_THROW({
        try {
            stream_buffer client(shm("ssb_bad_magic"));
        } catch (const std::runtime_error& e) {
            EXPECT_TRUE(std::string(e.what()).find("magic mismatch") != std::string::npos);
            throw;
        }
    }, std::runtime_error);
}

TEST_F(StreamBufferShmTests, LateJoinerViaShm) {
    stream_buffer server(1024, 16, shm("ssb_late_joiner"));
    publish_message(server, "old1", 4);
    publish_message(server, "old2", 4);

    stream_buffer client(shm("ssb_late_joiner"));
    uint64_t cursor = client.initial_reading_index();
    EXPECT_EQ(cursor, 2u);
    EXPECT_EQ(client.read<dbg>(cursor).first, nullptr);

    publish_message(server, "fresh", 5);
    auto [ptr, len] = client.read<dbg>(cursor);
    ASSERT_NE(ptr, nullptr);
    EXPECT_EQ(len, 5u);
    EXPECT_EQ(std::memcmp(ptr, "fresh", 5), 0);
}

TEST_F(StreamBufferShmTests, LossyOverwriteSkipsOldData) {
    stream_buffer server(1024, 4, shm("ssb_lossy"));  // tiny control ring
    stream_buffer client(shm("ssb_lossy"));

    for (uint8_t i = 0; i < 8; ++i) {
        publish_message(server, &i, 1);
    }

    uint64_t cursor = 0;
    auto [ptr, len] = client.read<dbg>(cursor);
    ASSERT_NE(ptr, nullptr);
    EXPECT_EQ(len, 1u);
    EXPECT_EQ(ptr[0], 4);
    EXPECT_EQ(client.loss_count(), 4u);
}

TEST_F(StreamBufferShmTests, ReadLastViaShm) {
    stream_buffer server(1024, 16, shm("ssb_read_last"));
    stream_buffer client(shm("ssb_read_last"));

    publish_message(server, "first", 5);
    publish_message(server, "second", 6);

    auto [ptr, len] = client.read_last();
    ASSERT_NE(ptr, nullptr);
    EXPECT_EQ(len, 6u);
    EXPECT_EQ(std::memcmp(ptr, "second", 6), 0);
}

TEST_F(StreamBufferShmTests, BroadcastTwoOpeners) {
    constexpr int kMessages = 200;
    stream_buffer server(1 << 16, 1024, shm("ssb_broadcast"));
    stream_buffer client1(shm("ssb_broadcast"));
    stream_buffer client2(shm("ssb_broadcast"));

    auto consume_all = [&](stream_buffer& client, std::vector<uint64_t>& out) {
        uint64_t cursor = 0;
        while (out.size() < kMessages) {
            auto [ptr, len] = client.read<dbg>(cursor);
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

namespace {

// Fill a raw segment past the header with a recognizable pattern so any stray write shows up.
std::vector<uint8_t> paint_foreign_payload(uint8_t* base, std::size_t size) {
    std::vector<uint8_t> payload(size - kHeaderSize);
    for (std::size_t i = 0; i < payload.size(); ++i) {
        payload[i] = static_cast<uint8_t>(i * 7 + 1);
    }
    std::memcpy(base + kHeaderSize, payload.data(), payload.size());
    return payload;
}

void expect_header_untouched(const uint8_t* base) {
    for (std::size_t i = 0; i < kHeaderSize; ++i) {
        EXPECT_EQ(base[i], 0u) << "creator wrote header byte " << i << " of a segment it did not create";
    }
}

}  // namespace

// A foreign segment whose init-state slot (byte 48) happens to be zero must not be treated as
// ours to initialize - open_or_create only grants that right to the process that created it.
TEST_F(StreamBufferShmTests, CreatorDoesNotOverwriteForeignZeroedSegment) {
    constexpr std::size_t kSize = 64 * 1024;
    slick::shm::shared_memory raw(
        shm("ssb_foreign_zeroed"), kSize, slick::shm::open_or_create, slick::shm::access_mode::read_write);
    auto* base = reinterpret_cast<uint8_t*>(raw.data());
    ASSERT_NE(base, nullptr);
    const std::vector<uint8_t> payload = paint_foreign_payload(base, kSize);

    EXPECT_THROW(stream_buffer(1024, 16, shm("ssb_foreign_zeroed")), std::runtime_error);

    expect_header_untouched(base);
    EXPECT_EQ(std::memcmp(base + kHeaderSize, payload.data(), payload.size()), 0);
}

// Same collision, but the foreign segment carries a magic of its own - that is provably not ours,
// so the creator rejects it immediately instead of waiting out the init handshake.
TEST_F(StreamBufferShmTests, CreatorRejectsForeignMagicSegment) {
    constexpr std::size_t kSize = 64 * 1024;
    slick::shm::shared_memory raw(
        shm("ssb_foreign_magic"), kSize, slick::shm::open_or_create, slick::shm::access_mode::read_write);
    auto* base = reinterpret_cast<uint8_t*>(raw.data());
    ASSERT_NE(base, nullptr);
    const std::vector<uint8_t> payload = paint_foreign_payload(base, kSize);
    *reinterpret_cast<uint32_t*>(base + kMagicOffset) = 0xDEADBEEF;

    EXPECT_THROW({
        try {
            stream_buffer(1024, 16, shm("ssb_foreign_magic"));
        } catch (const std::runtime_error& e) {
            EXPECT_TRUE(std::string(e.what()).find("magic mismatch") != std::string::npos);
            throw;
        }
    }, std::runtime_error);

    EXPECT_EQ(*reinterpret_cast<uint32_t*>(base + kMagicOffset), 0xDEADBEEFu);
    EXPECT_EQ(std::memcmp(base + kHeaderSize, payload.data(), payload.size()), 0);
}

// A header whose declared geometry does not fit in the mapped segment must be rejected rather
// than trusted into out-of-bounds control/data pointers.
TEST_F(StreamBufferShmTests, UndersizedSegmentThrows) {
    slick::shm::shared_memory raw(
        shm("ssb_undersized"), 4096, slick::shm::open_or_create, slick::shm::access_mode::read_write);
    auto* base = reinterpret_cast<uint8_t*>(raw.data());
    ASSERT_NE(base, nullptr);
    *reinterpret_cast<uint64_t*>(base + kCapacityOffset) = 1ull << 20;  // far beyond 4 KiB
    *reinterpret_cast<uint32_t*>(base + kControlSizeOffset) = 16;
    *reinterpret_cast<uint32_t*>(base + kMagicOffset) = kMagic;
    reinterpret_cast<std::atomic<uint32_t>*>(base + kInitStateOffset)->store(kStateReady);

    EXPECT_THROW({
        try {
            stream_buffer client(shm("ssb_undersized"));
        } catch (const std::runtime_error& e) {
            EXPECT_TRUE(std::string(e.what()).find("smaller than the geometry") != std::string::npos);
            throw;
        }
    }, std::runtime_error);
}

// A second creator with matching geometry attaches to the live segment: it must not claim
// ownership and must not reinitialize the cursors out from under the first one.
TEST_F(StreamBufferShmTests, SecondCreatorAttachesWithoutReinitializing) {
    stream_buffer server(1024, 16, shm("ssb_second_creator"));
    publish_message(server, "published", 9);

    stream_buffer attached(1024, 16, shm("ssb_second_creator"));
    EXPECT_FALSE(attached.own_buffer());
    EXPECT_TRUE(attached.use_shm());
    EXPECT_EQ(attached.capacity(), 1024u);
    EXPECT_EQ(attached.control_size(), 16u);

    uint64_t cursor = 0;
    auto [ptr, len] = attached.read<dbg>(cursor);
    ASSERT_NE(ptr, nullptr);
    EXPECT_EQ(len, 9u);
    EXPECT_EQ(std::memcmp(ptr, "published", 9), 0);
}

// A creator that dies between INIT_STATE_INITIALIZING and INIT_STATE_READY leaves the segment
// wedged. Construction must fail with a message that names the recovery step rather than a bare
// timeout - there is no portable way to tell a dead creator from a slow one.
TEST_F(StreamBufferShmTests, WedgedSegmentReportsRecoveryStep) {
    slick::shm::shared_memory raw(
        shm("ssb_wedged_msg"), 4096, slick::shm::open_or_create, slick::shm::access_mode::read_write);
    auto* base = reinterpret_cast<uint8_t*>(raw.data());
    ASSERT_NE(base, nullptr);
    reinterpret_cast<std::atomic<uint32_t>*>(base + kInitStateOffset)->store(kStateInitializing);

    EXPECT_THROW({
        try {
            stream_buffer client(shm("ssb_wedged_msg"));
        } catch (const std::runtime_error& e) {
            const std::string what = e.what();
            EXPECT_TRUE(what.find("INITIALIZING") != std::string::npos) << what;
            EXPECT_TRUE(what.find("remove(\"ssb_wedged_msg\")") != std::string::npos) << what;
            throw;
        }
    }, std::runtime_error);
}

// The documented recovery: once nothing is using the wedged segment, remove() clears the name and
// the next creator starts from a fresh segment it genuinely owns.
TEST_F(StreamBufferShmTests, RemoveClearsWedgedSegment) {
    {
        slick::shm::shared_memory wedged(
            shm("ssb_wedged"), 4096, slick::shm::open_or_create, slick::shm::access_mode::read_write);
        auto* base = reinterpret_cast<uint8_t*>(wedged.data());
        ASSERT_NE(base, nullptr);
        reinterpret_cast<std::atomic<uint32_t>*>(base + kInitStateOffset)->store(kStateInitializing);
    }  // last handle closed - on Windows the section is gone here, on POSIX the name remains

    stream_buffer::remove(shm("ssb_wedged"));

    stream_buffer server(1024, 16, shm("ssb_wedged"));
    EXPECT_TRUE(server.own_buffer());
    publish_message(server, "recovered", 9);

    stream_buffer client(shm("ssb_wedged"));
    uint64_t cursor = 0;
    auto [ptr, len] = client.read<dbg>(cursor);
    ASSERT_NE(ptr, nullptr);
    EXPECT_EQ(len, 9u);
    EXPECT_EQ(std::memcmp(ptr, "recovered", 9), 0);
}

// read() resynchronizes a consumer whose cursor outlived a reset(). The condition is a slot
// holding a sequence at or above next_seq_, which no live producer can publish, so it is staged
// here by writing the control slot directly.
TEST_F(StreamBufferShmTests, StaleCursorAfterResetResynchronizes) {
    stream_buffer server(1024, 16, shm("ssb_reset_detect"));
    publish_message(server, "first", 5);
    publish_message(server, "second", 6);  // next_seq_ == 2

    stream_buffer client(shm("ssb_reset_detect"));

    slick::shm::shared_memory raw(
        shm("ssb_reset_detect"), slick::shm::open_existing, slick::shm::access_mode::read_write);
    auto* base = reinterpret_cast<uint8_t*>(raw.data());
    ASSERT_NE(base, nullptr);
    uint8_t* slot = base + kHeaderSize + 3 * 32;                        // control_[3], 32B records
    *reinterpret_cast<uint64_t*>(slot + 8) = 0;                         // offset -> record 0's bytes
    *reinterpret_cast<uint32_t*>(slot + 16) = 5;                        // length
    reinterpret_cast<std::atomic<uint64_t>*>(slot)->store(99, std::memory_order_release);

    uint64_t cursor = 3;  // a cursor from before the (simulated) reset
    auto [ptr, len] = client.read<dbg>(cursor);
    ASSERT_NE(ptr, nullptr);
    EXPECT_EQ(len, 5u);
    EXPECT_EQ(std::memcmp(ptr, "first", 5), 0);
    EXPECT_EQ(cursor, 1u) << "the consumer should have restarted from the beginning";
}

TEST_F(StreamBufferShmTests, CreatorDestructionKeepsSegmentForSurvivingPeers) {
    // Regression: the buffer that created the segment unlinked its name on destruction.
    // A peer still attached kept reading the orphaned mapping, while a restarted producer
    // silently created a fresh segment under the same name, so the two never met again.
    auto creator = std::make_unique<stream_buffer>(1024, 16, shm("ssb_creator_exit"));
    ASSERT_TRUE(creator->own_buffer());
    stream_buffer survivor(shm("ssb_creator_exit"));

    publish_message(*creator, "before", 6);
    creator.reset();

    // The restarted producer must attach to the segment the survivor still maps.
    stream_buffer restarted(1024, 16, shm("ssb_creator_exit"));
    EXPECT_FALSE(restarted.own_buffer());
    publish_message(restarted, "after", 5);

    uint64_t cursor = 0;
    auto [p1, l1] = survivor.read<dbg>(cursor);
    ASSERT_NE(p1, nullptr);
    EXPECT_EQ(l1, 6u);
    EXPECT_EQ(std::memcmp(p1, "before", 6), 0);
    auto [p2, l2] = survivor.read<dbg>(cursor);
    ASSERT_NE(p2, nullptr);
    EXPECT_EQ(l2, 5u);
    EXPECT_EQ(std::memcmp(p2, "after", 5), 0);
}

TEST_F(StreamBufferShmTests, RemoveShmUnlinksNameButKeepsMappings) {
    stream_buffer local(1024, 16);
    EXPECT_FALSE(local.remove_shm());

    stream_buffer coordinator(1024, 16, shm("ssb_remove_shm"));
    stream_buffer peer(shm("ssb_remove_shm"));
    EXPECT_TRUE(coordinator.remove_shm());

    // Mappings opened before the removal keep working.
    publish_message(coordinator, "still here", 10);
    uint64_t cursor = 0;
    auto [ptr, len] = peer.read<dbg>(cursor);
    ASSERT_NE(ptr, nullptr);
    EXPECT_EQ(len, 10u);
    EXPECT_EQ(std::memcmp(ptr, "still here", 10), 0);

#if !defined(_WIN32)
    // On POSIX the name is gone, so the next buffer starts a fresh segment. Windows has no
    // name to remove: the segment lives until its last handle closes.
    EXPECT_FALSE(slick::shm::shared_memory::exists("ssb_remove_shm"));
    stream_buffer fresh(1024, 16, shm("ssb_remove_shm"));
    EXPECT_TRUE(fresh.own_buffer());
    EXPECT_EQ(fresh.initial_reading_index(), 0u);
#endif
}
