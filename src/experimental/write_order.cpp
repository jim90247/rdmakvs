#include <gflags/gflags.h>
#include <infiniband/verbs.h>
#include <unistd.h>

#include <iostream>
#include <unordered_set>

#include "network/rdma.hpp"

DEFINE_bool(server, true, "Is server");
DEFINE_string(server_uri, "tcp://192.168.223.1:7889", "Zmq server URI");
DEFINE_uint64(buffer_size, 32UL << 20, "RDMA buffer size in bytes");
DEFINE_uint64(segments, 64, "Segments to observe WRITE progress");
DEFINE_uint32(interval_us, 10, "Sleep interval (us)");

void ServerMainFunction() {
    char *buffer = new char[FLAGS_buffer_size];
    std::fill(buffer, buffer + FLAGS_buffer_size, '1');
    RdmaServer *server =
        new RdmaServer(nullptr, kAnyIbPort, buffer, FLAGS_buffer_size, 128, 128, IBV_QPT_RC);
    server->Listen(FLAGS_server_uri.c_str());
    uint64_t wr_id = server->Write(false, 0, 0, 0, FLAGS_buffer_size);
    server->WaitForCompletion(true, wr_id);
    std::cout << "RDMA WRITE completed\n";
}

void ClientMainFunction() {
    char *buffer = new char[FLAGS_buffer_size];
    std::fill(buffer, buffer + FLAGS_buffer_size, '0');
    RdmaClient *client =
        new RdmaClient(nullptr, kAnyIbPort, buffer, FLAGS_buffer_size, 128, 128, IBV_QPT_RC);
    client->Connect(FLAGS_server_uri.c_str());

    while (buffer[FLAGS_buffer_size - 1] == '0') {
        usleep(FLAGS_interval_us);
        char snapshot[FLAGS_segments + 1];
        for (size_t offset = 0, i = 0; offset < FLAGS_buffer_size;
             offset += FLAGS_buffer_size / FLAGS_segments, i++) {
            snapshot[i] = buffer[offset];
        }
        snapshot[FLAGS_segments] = '\0';
        std::cout << snapshot << '\n';
    }
}

int main(int argc, char **argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    if (FLAGS_server)
        ServerMainFunction();
    else
        ClientMainFunction();
    return 0;
}