#include <gflags/gflags.h>
#include <glog/logging.h>
#include <infiniband/verbs.h>
#include <unistd.h>

#include <cstdio>
#include <unordered_set>

#include "network/rdma.h"

int main(int argc, char **argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    google::InitGoogleLogging(argv[0]);

    const size_t buffer_size = 1 << 24;
    const size_t message_size = 2;
    volatile char *buffer = new volatile char[buffer_size]();

    sprintf(const_cast<char *>(buffer), "this is client");
    sprintf(const_cast<char *>(buffer + 300), "secret");
    RdmaClient *endpoint =
        new RdmaClient(nullptr, 0, reinterpret_cast<volatile unsigned char *>(buffer), buffer_size,
                       100, 100, IBV_QPT_RC);
    endpoint->Connect("tcp://192.168.223.1:7889");

    /*
        uint64_t wr_id = endpoint->Send(0, 15);
        std::unordered_set<uint64_t> completed_wr;
        endpoint->WaitForCompletion(completed_wr, true, wr_id);
        LOG(INFO) << "Message sent";
    */
    sleep(10);
    LOG(INFO) << "Message written by server: " << buffer + 100;
    return 0;
}