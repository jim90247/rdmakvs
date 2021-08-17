#include <gflags/gflags.h>
#include <glog/logging.h>
#include <infiniband/verbs.h>
#include <unistd.h>

#include <chrono>
#include <cstdint>
#include <cstdio>
#include <thread>
#include <vector>

#include "network/rdma.h"

using std::uint64_t;
using namespace std::chrono_literals;

DEFINE_string(endpoint, "tcp://192.168.223.1:7889", "Zmq endpoint");
DEFINE_bool(server, false, "Run as server");
DEFINE_int32(clients, 1, "Number of clients");
DEFINE_int32(client_id, -1, "ID of client");

const size_t kBufferSize = 1 << 20;
const size_t kSecretOffset = 128;
const size_t kClientMemoryStride = 1 << 10;

void ServerThreadMain(volatile char *buffer, RdmaServer *server, int client_id) {
    const static size_t kRecvOffset = 64;
    const size_t base_offset = client_id * kClientMemoryStride;

    // Prepare content to send
    snprintf(const_cast<char *>(buffer + base_offset), kClientMemoryStride, "r%03d w%03d s%03d",
             client_id, client_id, client_id);

    uint64_t recv_wr_id = server->Recv(client_id, base_offset + kRecvOffset, 3);

    LOG(INFO) << "Wait 3 seconds for client " << client_id << " to post RECV";
    std::this_thread::sleep_for(3s);

    server->Write(false, client_id, base_offset + 5, 5, 5, 0 /* unsignaled */);
    uint64_t send_wr_id = server->Send(client_id, base_offset + 10, 4,
                 IBV_SEND_SIGNALED | IBV_SEND_FENCE /* wait for previous WRITE to complete */);
    server->WaitForCompletion(client_id, true, send_wr_id);

    // Wait for client response
    server->WaitForCompletion(client_id, true, recv_wr_id);
    char expected[4];
    std::snprintf(expected, sizeof(expected), "%03d", client_id);
    CHECK_STREQ(const_cast<char *>(buffer + base_offset + kRecvOffset), expected);

    LOG(INFO) << "Client " << client_id << " completed successfully";
}

void ServerMain() {
    volatile char *buffer = new volatile char[kBufferSize]();

    RdmaServer *rdma_server = new RdmaServer(FLAGS_endpoint.c_str(), nullptr, 0,
                                             reinterpret_cast<volatile unsigned char *>(buffer),
                                             kBufferSize, 128, 128, IBV_QPT_RC);
    for (int cid = 0; cid < FLAGS_clients; cid++) {
        rdma_server->Listen();
        LOG(INFO) << "Connected to peer " << cid;
    }
    LOG(INFO) << "All clients connected, starting tests";

    std::vector<std::thread> threads;
    for (int cid = 0; cid < FLAGS_clients; cid++) {
        threads.push_back(std::thread(ServerThreadMain, buffer, rdma_server, cid));
    }

    for (auto &t : threads) {
        t.join();
    }
    LOG(INFO) << "All server threads completed successfully";
}

void ClientMain() {
    volatile char *buffer = new volatile char[kBufferSize]();
    RdmaClient *rdma_client =
        new RdmaClient(nullptr, 0, reinterpret_cast<volatile unsigned char *>(buffer), kBufferSize,
                       128, 128, IBV_QPT_RC);
    rdma_client->Connect(FLAGS_endpoint.c_str());
    LOG(INFO) << "Connected to peer";

    // Receive "sXXX" at offset 10, where XXX is client id (padded with 0 at front)
    uint64_t recv_wr_id = rdma_client->Recv(0, 10, 4);
    rdma_client->WaitForCompletion(0, true, recv_wr_id);
    // Read "rXXX " at offset 0
    uint64_t read_wr_id = rdma_client->Read(0, 0, FLAGS_client_id * kClientMemoryStride, 5);
    rdma_client->WaitForCompletion(0, true, read_wr_id);

    uint64_t send_wr_id = rdma_client->Send(0, 1, 3);
    rdma_client->WaitForCompletion(0, true, send_wr_id);

    char expected[15];
    std::snprintf(expected, sizeof(expected), "r%03d w%03d s%03d", FLAGS_client_id, FLAGS_client_id,
                  FLAGS_client_id);

    CHECK_STREQ(const_cast<char *>(buffer), expected);
    LOG(INFO) << "Client check passed";
}

int main(int argc, char **argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    google::InitGoogleLogging(argv[0]);

    if (FLAGS_server) {
        ServerMain();
    } else {
        // NOTE: launch client from 0 to FLAGS_clients - 1 one by one
        CHECK_GE(FLAGS_client_id, 0);
        CHECK_LT(FLAGS_client_id, FLAGS_clients);
        ClientMain();
    }
    return 0;
}
