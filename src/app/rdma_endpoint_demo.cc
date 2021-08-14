#include <gflags/gflags.h>
#include <glog/logging.h>
#include <infiniband/verbs.h>
#include <unistd.h>

#include <cstdint>
#include <cstdio>

#include "network/rdma.h"

using std::uint64_t;

DEFINE_string(endpoint, "tcp://192.168.223.1:7889", "Zmq endpoint");
DEFINE_bool(server, false, "Run as server");

const size_t kBufferSize = 1 << 10;
const size_t kSecretOffset = 128;

void ServerMain() {
    volatile char *buffer = new volatile char[kBufferSize]();
    RdmaServer *rdma_server =
        new RdmaServer(nullptr, 0, reinterpret_cast<volatile unsigned char *>(buffer), kBufferSize,
                       128, 128, IBV_QPT_RC);
    rdma_server->Listen(FLAGS_endpoint.c_str());
    LOG(INFO) << "Connected to peer";

    LOG(INFO) << "Sleep 3 seconds to wait for peer to post a RECV";
    sleep(3);

    std::snprintf(const_cast<char *>(buffer), kBufferSize - 1, "hello");
    std::snprintf(const_cast<char *>(buffer + kSecretOffset), kBufferSize - kSecretOffset - 1,
                  "secret");

    rdma_server->Write(false, 0, 0, 0, 5, 0 /* unsignaled */);
    rdma_server->Send(0, 0, 5,
                      IBV_SEND_SIGNALED | IBV_SEND_FENCE /* wait for previous WRITE to complete */);

    // Sleep again to wait for peer's operations to complete
    sleep(3);
}

void ClientMain() {
    volatile char *buffer = new volatile char[kBufferSize]();
    RdmaClient *rdma_client =
        new RdmaClient(nullptr, 0, reinterpret_cast<volatile unsigned char *>(buffer), kBufferSize,
                       128, 128, IBV_QPT_RC);
    rdma_client->Connect(FLAGS_endpoint.c_str());
    LOG(INFO) << "Connected to peer";

    // Receive "hello" at offset 5
    uint64_t recv_wr_id = rdma_client->Recv(0, 5, 5);
    rdma_client->WaitForCompletion(0, true, recv_wr_id);

    CHECK_STREQ("hellohello", const_cast<char *>(buffer));
    uint64_t read_wr_id = rdma_client->Read(0, kSecretOffset, kSecretOffset, 6);
    rdma_client->WaitForCompletion(0, true, read_wr_id);
    CHECK_STREQ("secret", const_cast<char *>(buffer + kSecretOffset));

    LOG(INFO) << "Client check passed";
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