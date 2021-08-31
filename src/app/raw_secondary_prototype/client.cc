#include <gflags/gflags.h>
#include <glog/logging.h>
#include <infiniband/verbs.h>

#include "app/raw_secondary_prototype/common.h"
#include "network/rdma.h"

DEFINE_uint64(buffer_size, 1 << 20, "Buffer size");

void ClientMain(RdmaClient &client, volatile unsigned char *const buf) {}

int main(int argc, char **argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    google::InitGoogleLogging(argv[0]);

    volatile unsigned char *buffer = new volatile unsigned char[FLAGS_buffer_size]();
    RdmaClient client(nullptr, 0, buffer, FLAGS_buffer_size, 128, 128, IBV_QPT_RC);

    client.Connect(FLAGS_endpoint.c_str());
    LOG(INFO) << "Client connected to server.";

    ClientMain(std::ref(client), buffer);

    return 0;
}