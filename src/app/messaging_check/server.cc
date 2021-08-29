#include <gflags/gflags.h>
#include <glog/logging.h>

#include <algorithm>
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
    RdmaServer *server = new RdmaServer(FLAGS_endpoint.c_str(), nullptr, 0, buffer,
                                        FLAGS_buffer_size, 128, 128, IBV_QPT_RC);

    server->Listen();
    LOG(INFO) << "Connections established";

    auto msg_endpoint =
        rdmamsg::RdmaWriteMessagingEndpoint(server, buffer, 0, 0, 0, FLAGS_buffer_size);

    for (int r = 0; r < FLAGS_rounds; r++) {
        volatile void *msg_buffer = msg_endpoint.AllocateOutboundMessageBuffer(FLAGS_message_size);
        std::string s = RandomString(FLAGS_message_size);
        std::copy(s.data(), s.data() + FLAGS_message_size,
                  static_cast<volatile char *>(msg_buffer));
        auto wr = msg_endpoint.FlushOutboundMessage();
        msg_endpoint.BlockUntilComplete(wr);
        msg_endpoint.ReleaseInboundMessageBuffer();
    }

    return 0;
}
