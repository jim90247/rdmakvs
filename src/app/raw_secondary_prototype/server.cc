#include <gflags/gflags.h>
#include <glog/logging.h>
#include <infiniband/verbs.h>

#include <cstring>

#include "app/raw_secondary_prototype/common.h"
#include "network/rdma.h"

DEFINE_uint64(buffer_size, 1 << 20, "Buffer size");

void ServerMain(RdmaServer &server, volatile unsigned char *const buf) {
    KeyValuePair kvp = KeyValuePair::Create(123, 4, "123");
    kvp.SerializeTo(buf);
    auto wr = server.Write(false, 0, 0, 0, sizeof(KeyValuePair));
    server.WaitForCompletion(0, true, wr);
}

int main(int argc, char **argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    google::InitGoogleLogging(argv[0]);

    volatile unsigned char *buffer = new volatile unsigned char[FLAGS_buffer_size]();
    RdmaServer server(FLAGS_endpoint.c_str(), nullptr, 0, buffer, FLAGS_buffer_size, 128, 128,
                      IBV_QPT_RC);

    server.Listen();
    LOG(INFO) << "Server connected to remote.";

    ServerMain(std::ref(server), buffer);

    return 0;
}