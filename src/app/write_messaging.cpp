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

#include <cstdint>

#include "src/messaging/rdma_messaging.hpp"

DEFINE_string(endpoint, "tcp://192.168.223.1:7889", "Zmq endpoint");
DEFINE_bool(server, false, "Run as server");

const size_t kBufferSize = 1 << 11;
const int kRound = 1000;

void ServerMain() {
    unsigned char *buffer = new unsigned char[kBufferSize]();
    RdmaServer *rdma_server = new RdmaServer(nullptr, 0, reinterpret_cast<char *>(buffer),
                                             kBufferSize, 128, 128, IBV_QPT_RC);
    rdma_server->Listen(FLAGS_endpoint.c_str());

    rdmamsg::RdmaWriteMessagingEndpoint *msg_ep =
        new rdmamsg::RdmaWriteMessagingEndpoint(rdma_server, buffer, kBufferSize);

    int data = 0;
    int64_t track_id = 0;
    while (data <= kRound) {
        LOG_EVERY_N(INFO, kRound / 10) << "Sending message (" << data << ")";
        *reinterpret_cast<int *>(msg_ep->AllocateOutboundMessageBuffer(sizeof(int))) = data;
        track_id = msg_ep->FlushOutboundMessage();
        msg_ep->BlockUntilComplete(track_id);
        data++;
    }
}

void ClientMain() {
    unsigned char *buffer = new unsigned char[kBufferSize]();
    RdmaClient *rdma_client = new RdmaClient(nullptr, 0, reinterpret_cast<char *>(buffer),
                                             kBufferSize, 128, 128, IBV_QPT_RC);
    rdma_client->Connect(FLAGS_endpoint.c_str());

    rdmamsg::RdmaWriteMessagingEndpoint *msg_ep =
        new rdmamsg::RdmaWriteMessagingEndpoint(rdma_client, buffer, kBufferSize);

    int data = 0;
    while (data <= kRound) {
        auto inbound_msg = msg_ep->CheckInboundMessage();
        while (inbound_msg.size == 0) {
            usleep(1);
            inbound_msg = msg_ep->CheckInboundMessage();
        }
        CHECK_EQ(inbound_msg.size, sizeof(int));
        CHECK_EQ(*reinterpret_cast<int *>(inbound_msg.data), data);

        msg_ep->ReleaseInboundMessageBuffer();

        LOG_EVERY_N(INFO, kRound / 10) << "Message content check passed (" << data << ")";
        data++;
    }
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
