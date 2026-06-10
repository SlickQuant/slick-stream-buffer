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
#include <slick/dynamic_stream_buffer.h>

#include <boost/asio/buffer.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/read.hpp>
#include <boost/asio/write.hpp>

#include <cstring>
#include <string>
#include <vector>

namespace net = boost::asio;
using net::ip::tcp;
using slick::SlickStreamBuffer;
using slick::dynamic_stream_buffer;

// The whole point of the adapter: beast/asio must accept it as a DynamicBuffer.
// (Modern Beast uses asio's DynamicBuffer_v1 trait as its DynamicBuffer concept.)
static_assert(net::is_dynamic_buffer_v1<dynamic_stream_buffer>::value,
              "dynamic_stream_buffer must satisfy asio's DynamicBuffer_v1 requirements");

TEST(DynamicStreamBufferTests, BufferCopyRoundtrip) {
    SlickStreamBuffer sb(1024, 16);
    dynamic_stream_buffer dyn(sb);

    const std::string src = "dynamic buffer roundtrip";
    const std::size_t copied = net::buffer_copy(dyn.prepare(src.size()), net::buffer(src));
    ASSERT_EQ(copied, src.size());
    dyn.commit(copied);

    EXPECT_EQ(dyn.size(), src.size());
    auto readable = dyn.data();
    ASSERT_EQ(readable.size(), src.size());
    EXPECT_EQ(std::memcmp(readable.data(), src.data(), src.size()), 0);

    auto record = dyn.consume(src.size());
    EXPECT_EQ(dyn.size(), 0u);

    // consume() returns the published record and the bytes are readable on the
    // underlying stream buffer - both views are the same memory
    ASSERT_TRUE(static_cast<bool>(record));
    EXPECT_EQ(record.length, src.size());

    uint64_t cursor = 0;
    auto [ptr, len] = dyn.stream_buffer().read(cursor);
    ASSERT_NE(ptr, nullptr);
    ASSERT_EQ(len, src.size());
    EXPECT_EQ(std::memcmp(ptr, src.data(), len), 0);
    EXPECT_EQ(ptr, record.data);
}

TEST(DynamicStreamBufferTests, MaxSizeClampsAndThrows) {
    SlickStreamBuffer sb(1024, 16);
    dynamic_stream_buffer dyn(sb, 16);

    EXPECT_EQ(dyn.max_size(), 16u);
    EXPECT_NO_THROW(dyn.prepare(16));
    EXPECT_THROW(dyn.prepare(17), std::length_error);

    net::buffer_copy(dyn.prepare(10), net::buffer(std::string(10, 'x')));
    dyn.commit(10);
    EXPECT_THROW(dyn.prepare(7), std::length_error);   // 10 committed + 7 > 16
    EXPECT_NO_THROW(dyn.prepare(6));

    // max_size is clamped to the ring capacity
    dynamic_stream_buffer dyn2(sb);
    EXPECT_EQ(dyn2.max_size(), sb.capacity());
}

TEST(DynamicStreamBufferTests, TcpLoopbackZeroCopyRead) {
    net::io_context ioc;
    tcp::acceptor acceptor(ioc, tcp::endpoint(net::ip::make_address("127.0.0.1"), 0));
    acceptor.listen();

    tcp::socket writer(ioc);
    writer.connect(tcp::endpoint(net::ip::make_address("127.0.0.1"), acceptor.local_endpoint().port()));
    tcp::socket reader = acceptor.accept();

    // length-prefixed messages, written as one stream
    const std::vector<std::string> messages = { "alpha", "bravo charlie", "d", "echo foxtrot golf" };
    std::string stream_bytes;
    for (const auto& m : messages) {
        uint32_t len = static_cast<uint32_t>(m.size());
        stream_bytes.append(reinterpret_cast<const char*>(&len), sizeof(len));
        stream_bytes.append(m);
    }
    net::write(writer, net::buffer(stream_bytes));

    // receive into the stream buffer and publish one record per complete message
    SlickStreamBuffer sb(1 << 12, 64);
    dynamic_stream_buffer dyn(sb);
    uint64_t cursor = 0;
    std::size_t verified = 0;

    while (verified < messages.size()) {
        const std::size_t n = reader.read_some(dyn.prepare(1024));
        dyn.commit(n);

        // parse: publish every complete length-prefixed message
        for (;;) {
            const std::size_t readable = dyn.size();
            if (readable < sizeof(uint32_t)) break;
            uint32_t len;
            std::memcpy(&len, dyn.data().data(), sizeof(len));
            if (readable < sizeof(len) + len) break;
            dyn.consume(sizeof(len) + len);  // publishes prefix + payload as one record
        }

        // drain published records and verify against the source messages (zero-copy)
        for (;;) {
            auto [ptr, len] = sb.read(cursor);
            if (ptr == nullptr) break;
            ASSERT_LT(verified, messages.size());
            const std::string& expected = messages[verified];
            ASSERT_EQ(len, sizeof(uint32_t) + expected.size());
            uint32_t payload_len;
            std::memcpy(&payload_len, ptr, sizeof(payload_len));
            ASSERT_EQ(payload_len, expected.size());
            EXPECT_EQ(std::memcmp(ptr + sizeof(payload_len), expected.data(), expected.size()), 0);
            ++verified;
        }
    }
    EXPECT_EQ(verified, messages.size());
    EXPECT_EQ(sb.loss_count(), 0u);
}

TEST(DynamicStreamBufferTests, AsioReadComposedOp) {
    net::io_context ioc;
    tcp::acceptor acceptor(ioc, tcp::endpoint(net::ip::make_address("127.0.0.1"), 0));
    acceptor.listen();

    tcp::socket writer(ioc);
    writer.connect(tcp::endpoint(net::ip::make_address("127.0.0.1"), acceptor.local_endpoint().port()));
    tcp::socket reader = acceptor.accept();

    const std::string payload(2000, '\0');
    std::string src = payload;
    for (std::size_t i = 0; i < src.size(); ++i) src[i] = static_cast<char>(i & 0xff);
    net::write(writer, net::buffer(src));

    // asio's composed read copies the DynamicBuffer_v1 by value internally -
    // this proves the adapter survives that
    SlickStreamBuffer sb(1 << 12, 16);
    dynamic_stream_buffer dyn(sb);
    const std::size_t n = net::read(reader, dyn, net::transfer_exactly(src.size()));
    ASSERT_EQ(n, src.size());
    ASSERT_EQ(dyn.size(), src.size());
    EXPECT_EQ(std::memcmp(dyn.data().data(), src.data(), src.size()), 0);

    dyn.consume(src.size());
    uint64_t cursor = 0;
    auto [ptr, len] = sb.read(cursor);
    ASSERT_NE(ptr, nullptr);
    ASSERT_EQ(len, src.size());
    EXPECT_EQ(std::memcmp(ptr, src.data(), len), 0);
}
