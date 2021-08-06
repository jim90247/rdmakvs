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

const size_t kBufferSize = 1 << 20;
const size_t kMessageSize = 16;
const unsigned long kRound = 16 << 20;
const unsigned long kInboundReleasePeriod = 32 << 10;
const unsigned long kOutboundFlushBatch = 1;

void ServerMain() {
    unsigned char *buffer = new unsigned char[kBufferSize]();
    RdmaServer *rdma_server = new RdmaServer(nullptr, 0, reinterpret_cast<char *>(buffer),
                                             kBufferSize, 128, 128, IBV_QPT_RC);
    rdma_server->Listen(FLAGS_endpoint.c_str());
    rdmamsg::RdmaWriteMessagingEndpoint *msg_ep =
        new rdmamsg::RdmaWriteMessagingEndpoint(rdma_server, buffer, kBufferSize);

    for (unsigned long round = 0, flush_round = 0, refresh_round = 0; round < kRound; round++) {
        // Wait for request
        rdmamsg::InboundMessage request = {.data = nullptr, .size = 0};
        while (request.size == 0) {
            request = msg_ep->CheckInboundMessage();
        }
        if (++refresh_round >= kInboundReleasePeriod) {
            msg_ep->ReleaseInboundMessageBuffer(0);
            refresh_round = 0;
        }

        // Process request
        for (int offset = 0; offset < request.size; offset += sizeof(unsigned long)) {
        }

        // Send response
        *reinterpret_cast<unsigned long *>(
            msg_ep->AllocateOutboundMessageBuffer(0, sizeof(unsigned long))) = round;
        if (++flush_round >= kOutboundFlushBatch) {
            msg_ep->FlushOutboundMessage(0);
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

    unsigned long sent_round = 0, completed_round = 0, flush_round = 0, refresh_round = 0;
    auto start = std::chrono::steady_clock::now();
    while (completed_round < kRound) {
        // Send request
        if (sent_round < kRound) {
            *reinterpret_cast<unsigned long *>(
                msg_ep->AllocateOutboundMessageBuffer(0, sizeof(unsigned long))) = sent_round;
            sent_round++;
            if (++flush_round >= kOutboundFlushBatch) {
                msg_ep->FlushOutboundMessage(0);
                flush_round = 0;
            }
        }
        // Check for response
        rdmamsg::InboundMessage response = msg_ep->CheckInboundMessage();
        if (response.size > 0) {
            // DCHECK_EQ(sizeof(unsigned long), response.size);
            // DCHECK_EQ(completed_round, *reinterpret_cast<unsigned long *>(response.data));
            completed_round++;
            LOG_EVERY_N(INFO, kRound / 10) << "Progress: " << completed_round << " / " << kRound;
            if (++refresh_round >= kInboundReleasePeriod) {
                msg_ep->ReleaseInboundMessageBuffer(0);
                refresh_round = 0;
            }
        }
    }
    auto end = std::chrono::steady_clock::now();
    long duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
    LOG(INFO) << kRound << " requests completed in " << duration_ms
              << " ms. Messages per second: " << kRound / (duration_ms / 1000.0);
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
