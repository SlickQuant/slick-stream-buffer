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

// One producer and N consumers over a single shared-memory segment, all in one process: the
// threads share the same physical cache lines, so the coherence traffic matches the
// cross-process case while staying easy to run.
//
// The rings are sized so nothing is ever lost at the default message count. That matters: in
// the lossy regime consumers run a different code path (skip + cpu_relax) and the numbers stop
// comparing anything useful. `missing` in the output must read 0 - if it does not, lower
// --messages or raise the ring sizes.
//
// Producer and consumer rates come out nearly equal because the system is coupled - consumers
// keep pace with the producer, so both sides measure the same end-to-end rate.
//
// Run-to-run spread on an unpinned desktop is around 10%, which is larger than most changes
// worth arguing about. Interleave the variants you are comparing (A B A B), take the min of
// several reps, and treat anything under ~10% as unproven.
//
// Usage: stream_buffer_bench [consumers] [messages] [reps] [shm_name]
//
// Exit codes: 0 clean run, 1 records were lost (numbers not comparable), 2 bad arguments,
// 3 the named segment already exists and belongs to someone else - the benchmark refuses to
// become a second producer on it, and does not remove it, since it may be in use.

#include <slick/stream_buffer.hpp>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <thread>
#include <vector>

using slick::stream_buffer;

namespace {

// strtoull()/atoi() report malformed input as 0 and wrap a leading '-' into a huge value. Left
// unchecked, "0 consumers" or "0 reps" produce empty result vectors that the reporting below
// dereferences, and "0 messages" divides by zero into inf timings.
bool parse_positive(const char* text, uint64_t max, uint64_t& out) {
    if (text == nullptr || *text == '\0' || std::strchr(text, '-') != nullptr) {
        return false;
    }
    errno = 0;
    char* end = nullptr;
    const unsigned long long value = std::strtoull(text, &end, 10);
    if (end == text || *end != '\0' || errno == ERANGE || value == 0 || value > max) {
        return false;
    }
    out = static_cast<uint64_t>(value);
    return true;
}

bool parse_arg(int argc, char** argv, int index, uint64_t max, const char* what, uint64_t& out) {
    if (argc <= index) {
        return true;  // keep the default
    }
    if (parse_positive(argv[index], max, out)) {
        return true;
    }
    std::printf("ERROR: %s must be a positive integer no greater than %llu, got '%s'\n",
                what, static_cast<unsigned long long>(max), argv[index]);
    return false;
}

}  // namespace

int main(int argc, char** argv) {
    // Bounds are sanity limits, not tuning advice: one thread per consumer, and enough headroom
    // that the timing arithmetic below cannot overflow.
    uint64_t consumer_arg = 3, message_arg = 1000000, rep_arg = 5;
    if (!parse_arg(argc, argv, 1, 1024, "consumers", consumer_arg) ||
        !parse_arg(argc, argv, 2, 1000000000ull, "messages", message_arg) ||
        !parse_arg(argc, argv, 3, 1000, "reps", rep_arg)) {
        std::printf("Usage: %s [consumers] [messages] [reps] [shm_name]\n", argv[0]);
        return 2;
    }
    const int consumers = static_cast<int>(consumer_arg);   // bounded above, so int is safe
    const int reps = static_cast<int>(rep_arg);
    const uint64_t messages = message_arg;
    const char* name = argc > 4 ? argv[4] : "ssb_bench";

    constexpr uint64_t kCapacity = 1ull << 25;     // 32 MiB data ring
    constexpr uint32_t kControlSize = 1u << 20;    // 1M records - no loss up to 1M messages
    constexpr std::size_t kMessageBytes = 16;

    std::vector<double> producer_ns, consumer_ns;

    uint64_t total_missing = 0;

    for (int rep = 0; rep < reps; ++rep) {
        stream_buffer server(kCapacity, kControlSize, name);
        if (!server.own_buffer()) {
            // The creator constructor attaches to a segment that already exists rather than
            // overwriting it, so producing here would put a second producer on somebody else's
            // buffer - which the single-producer contract forbids and which would invalidate
            // every number below. Not remove()d on purpose: that segment may be in use.
            std::printf("ERROR: shared memory segment '%s' already exists and is in use by\n"
                        "       another process. Pass a different name as argv[4], or clear a\n"
                        "       leftover segment with slick::stream_buffer::remove(\"%s\").\n",
                        name, name);
            return 3;
        }
        std::atomic<bool> go{ false };
        std::atomic<int> ready{ 0 };
        std::vector<std::thread> threads;
        std::vector<double> per_consumer(consumers, 0);

        std::vector<uint64_t> received(consumers, 0);

        for (int c = 0; c < consumers; ++c) {
            threads.emplace_back([&, c] {
                stream_buffer client(name);
                uint64_t cursor = 0;
                uint64_t got = 0;
                ready.fetch_add(1);
                while (!go.load(std::memory_order_acquire)) {}
                const auto t0 = std::chrono::steady_clock::now();
                // Read with the default traits: the point is to measure what production runs.
                // Loss is detected from the received count instead of loss_count(), which the
                // default traits leave at 0.
                // The cursor, not the record count, is the completion condition: read() advances
                // it past records that were lapped without returning them, so a run that loses
                // even one record would never reach got == messages and would spin forever once
                // the producer stopped - taking the loss report below down with it.
                while (cursor < messages) {
                    if (client.read(cursor).first) {
                        ++got;
                    }
                }
                const auto t1 = std::chrono::steady_clock::now();
                per_consumer[c] =
                    std::chrono::duration<double, std::nano>(t1 - t0).count() / double(messages);
                received[c] = got;

            });
        }
        while (ready.load() != consumers) {}

        const uint8_t payload[kMessageBytes] = {};
        go.store(true, std::memory_order_release);
        const auto t0 = std::chrono::steady_clock::now();
        for (uint64_t i = 0; i < messages; ++i) {
            auto [ptr, sz] = server.prepare(sizeof(payload));
            std::memcpy(ptr, payload, sizeof(payload));
            server.commit(sizeof(payload));
            server.consume(sizeof(payload));
        }
        const auto t1 = std::chrono::steady_clock::now();
        for (auto& t : threads) {
            t.join();
        }

        producer_ns.push_back(
            std::chrono::duration<double, std::nano>(t1 - t0).count() / double(messages));
        consumer_ns.push_back(*std::max_element(per_consumer.begin(), per_consumer.end()));
        for (const uint64_t r : received) {
            total_missing += messages - r;
        }
    }

    std::sort(producer_ns.begin(), producer_ns.end());
    std::sort(consumer_ns.begin(), consumer_ns.end());
    std::printf(
        "consumers=%d messages=%llu reps=%d | producer ns/msg: min %6.2f med %6.2f"
        " | slowest consumer ns/msg: min %6.2f med %6.2f | missing %llu\n",
        consumers, static_cast<unsigned long long>(messages), reps,
        producer_ns.front(), producer_ns[producer_ns.size() / 2],
        consumer_ns.front(), consumer_ns[consumer_ns.size() / 2],
        static_cast<unsigned long long>(total_missing));

    if (total_missing != 0) {
        std::printf("WARNING: %llu records never reached a consumer - the numbers above are not\n"
                    "         comparable. Lower the message count or raise the ring sizes until\n"
                    "         this reads 0.\n",
                    static_cast<unsigned long long>(total_missing));
        return 1;
    }
    return 0;
}
