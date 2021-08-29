#include <gflags/gflags.h>
#include <glog/logging.h>

#include <algorithm>
#include <cstdint>
#include <random>
#include <string>

#include "app/messaging_check/common.h"
#include "messaging/rdma_messaging.h"
#include "network/rdma.h"

DEFINE_uint64(buffer_size, 1 << 20, "Per-thread buffer size");
DEFINE_int32(rounds, 1 << 24, "Rounds");
DEFINE_int32(message_size, 5, "Message size in bytes");
DEFINE_string(endpoint, "tcp://192.168.223.1:7899", "Zmq endpoint");

int main(int argc, char **argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    google::InitGoogleLogging(argv[0]);

    volatile unsigned char *buffer = new volatile unsigned char[FLAGS_buffer_size]();
    RdmaClient *client =
        new RdmaClient(nullptr, 0, buffer, FLAGS_buffer_size, 128, 128, IBV_QPT_RC);

    client->Connect(FLAGS_endpoint.c_str());
    LOG(INFO) << "Connections established";

    auto msg_endpoint =
        rdmamsg::RdmaWriteMessagingEndpoint(client, buffer, 0, 0, 0, FLAGS_buffer_size);

    for (int r = 0; r < FLAGS_rounds; r++) {
        std::string expected = RandomString(FLAGS_message_size);
        auto msg = msg_endpoint.CheckInboundMessage();
        while (msg.size == 0) {
            msg = msg_endpoint.CheckInboundMessage();
        }
        volatile char *received = static_cast<volatile char *>(msg.data);
        for (int i = 0; i < FLAGS_message_size; i++) {
            // FIXME: this check fails
            CHECK(expected[i] == received[i]) << "round: " << r << ", offset = "
                                              << reinterpret_cast<std::uintptr_t>(msg.data) -
                                                     reinterpret_cast<std::uintptr_t>(buffer);
        }
        msg_endpoint.ReleaseInboundMessageBuffer();
    }

    return 0;
}
