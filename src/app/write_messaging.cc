/**
 * @file write_messaging.cpp
 * @author jim90247 (jim90247@gmail.com)
 * @brief A small app for testing RDMA WRITE-based messaging
 * @version 0.1
 * @date 2021-07-25
 *
 * @copyright Copyright (c) 2021
 *
 */

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <infiniband/verbs.h>
#include <unistd.h>

#include <chrono>
#include <cstdint>

#include "messaging/rdma_messaging.h"

DEFINE_string(endpoint, "tcp://192.168.223.1:7889", "Zmq endpoint");
DEFINE_bool(server, false, "Run as server");

// Experiment parameters
DEFINE_uint64(inbound_gc_period, 4096,
              "Number of rounds between two ReleaseInboundMessageBuffer calls");
DEFINE_uint64(outbound_batch, 1, "Number of requests to accumulate before one RDMA write (flush)");
DEFINE_uint64(round, 16 << 20, "Rounds");

const size_t kBufferSize = 1 << 20;
const size_t kMessageSize = 16;
const size_t kLatencyMeasurePeriod = 10001;

void ServerMain() {
    unsigned char *buffer = new unsigned char[kBufferSize]();
    RdmaServer *rdma_server = new RdmaServer(nullptr, 0, reinterpret_cast<char *>(buffer),
                                             kBufferSize, 128, 128, IBV_QPT_RC);
    rdma_server->Listen(FLAGS_endpoint.c_str());
    rdmamsg::RdmaWriteMessagingEndpoint *msg_ep =
        new rdmamsg::RdmaWriteMessagingEndpoint(rdma_server, buffer, kBufferSize);

    for (unsigned long round = 0, flush_round = 0, refresh_round = 0; round < FLAGS_round;
         round++) {
        // Wait for request
        rdmamsg::InboundMessage request = {.data = nullptr, .size = 0};
        while (request.size == 0) {
            request = msg_ep->CheckInboundMessage();
        }

        // Process request
        for (int offset = 0; offset < request.size; offset += sizeof(unsigned long)) {
        }

        if (++refresh_round >= FLAGS_inbound_gc_period) {
            msg_ep->ReleaseInboundMessageBuffer();
            refresh_round = 0;
        }

        // Send response
        *reinterpret_cast<unsigned long *>(
            msg_ep->AllocateOutboundMessageBuffer(sizeof(unsigned long))) = round;
        if (++flush_round >= FLAGS_outbound_batch) {
            msg_ep->FlushOutboundMessage();
            flush_round = 0;
        }
    }
}

void ClientMain() {
    unsigned char *buffer = new unsigned char[kBufferSize]();
    RdmaClient *rdma_client = new RdmaClient(nullptr, 0, reinterpret_cast<char *>(buffer),
                                             kBufferSize, 128, 128, IBV_QPT_RC);
    rdma_client->Connect(FLAGS_endpoint.c_str());
    rdmamsg::RdmaWriteMessagingEndpoint *msg_ep =
        new rdmamsg::RdmaWriteMessagingEndpoint(rdma_client, buffer, kBufferSize);

    unsigned long sent_round = 0, completed_round = 0, flush_round = 0, refresh_round = 0,
                  latency_measure_send_round = 0, latency_measure_recv_round = 0;
    std::vector<std::chrono::time_point<std::chrono::steady_clock>> start_time;
    std::vector<std::chrono::time_point<std::chrono::steady_clock>> end_time;

    auto start = std::chrono::steady_clock::now();
    while (completed_round < FLAGS_round) {
        // Send request
        if (sent_round < FLAGS_round) {
            *reinterpret_cast<unsigned long *>(
                msg_ep->AllocateOutboundMessageBuffer(sizeof(unsigned long))) = sent_round;
            sent_round++;
            if (++flush_round >= FLAGS_outbound_batch) {
                msg_ep->FlushOutboundMessage();
                flush_round = 0;
            }
            if (++latency_measure_send_round >= kLatencyMeasurePeriod) {
                start_time.push_back(std::chrono::steady_clock::now());
                latency_measure_send_round = 0;
            }
        }
        // Check for response
        rdmamsg::InboundMessage response = msg_ep->CheckInboundMessage();
        if (response.size > 0) {
            // DCHECK_EQ(sizeof(unsigned long), response.size);
            // DCHECK_EQ(completed_round, *reinterpret_cast<unsigned long *>(response.data));
            completed_round++;
            LOG_EVERY_N(INFO, FLAGS_round / 10)
                << "Progress: " << completed_round << " / " << FLAGS_round;
            if (++latency_measure_recv_round >= kLatencyMeasurePeriod) {
                // TODO(jim90247): Measure the latency earlier. Currently we can only receive one
                // response every time we send a request. We should be able to process multiple
                // requests between two requests are sent.
                end_time.push_back(std::chrono::steady_clock::now());
                latency_measure_recv_round = 0;
            }
            if (++refresh_round >= FLAGS_inbound_gc_period) {
                msg_ep->ReleaseInboundMessageBuffer();
                refresh_round = 0;
            }
        }
    }
    auto end = std::chrono::steady_clock::now();
    long duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
    LOG(INFO) << FLAGS_round << " requests completed in " << duration_ms
              << " ms. Messages per second: " << FLAGS_round / (duration_ms / 1000.0);

    long total_latency_ns = 0;
    for (int i = 0; i < start_time.size(); i++) {
        total_latency_ns +=
            std::chrono::duration_cast<std::chrono::nanoseconds>(end_time[i] - start_time[i])
                .count();
    }
    LOG(INFO) << "Average latency: " << (double)total_latency_ns / start_time.size()
              << " nanoseconds";
}

int main(int argc, char **argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    google::InitGoogleLogging(argv[0]);

    if (FLAGS_server) {
        ServerMain();
    } else {
        ClientMain();
    }
    return 0;
}
