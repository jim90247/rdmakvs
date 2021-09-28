#include <gflags/gflags.h>
#include <glog/logging.h>
#include <glog/raw_logging.h>
#include <infiniband/verbs.h>
#include <unistd.h>

#include <chrono>
#include <cstdint>
#include <cstdio>
#include <future>
#include <random>
#include <thread>
#include <vector>

#include "network/rdma.h"
#include "util/zipf_generator.h"

using std::uint64_t;
using namespace std::chrono_literals;

DEFINE_string(endpoint, "tcp://192.168.223.1:7889", "Zmq endpoint");
DEFINE_bool(server, false, "Run as server");
DEFINE_uint64(server_buf_size, 1 << 20, "Buffer size at server");
DEFINE_int32(client_threads, 1, "Number of client threads");
DEFINE_uint64(client_slots, 32, "Read slots at client");
DEFINE_uint64(read_size, 64, "Number of bytes to fetch with each RDMA read");

unsigned char computeExpectedValue(size_t idx) { return static_cast<unsigned char>(idx * idx); }

void ServerMain() {
    volatile unsigned char *buf = new volatile unsigned char[FLAGS_server_buf_size];
    for (int i = 0; i < FLAGS_server_buf_size; i++) {
        buf[i] = computeExpectedValue(i);
    }
    RdmaEndpoint ep(nullptr, 0, buf, FLAGS_server_buf_size, 128, 128, IBV_QPT_RC);

    ep.BindToZmqEndpoint(FLAGS_endpoint.c_str());
    for (int i = 0; i < FLAGS_client_threads; i++) {
        ep.Listen();
    }
    LOG(INFO) << "all clients connected";

    std::this_thread::sleep_for(1s);
    std::vector<uint64_t> cmpl_signals;
    for (int i = 0; i < FLAGS_client_threads; i++) {
        // receive an one-byte response from client as a completion signal
        cmpl_signals.push_back(ep.Recv(i, 0, 1));
        // send an one-byte signal indicating the start of benchmark
        auto wr = ep.Send(i, 0, 1);
        ep.WaitForCompletion(i, true, wr);
    }
    LOG(INFO) << "benchmark starts";

    for (int i = 0; i < FLAGS_client_threads; i++) {
        ep.WaitForCompletion(i, true, cmpl_signals[i]);
    }
    LOG(INFO) << "all clients completed";
}

/**
 * @brief Main function for each clien thread.
 *
 * @param ep the endpoint with remote connection
 * @param gbuf the RDMA registered buffer
 * @param id the id of this client
 * @param iops_result the performance result of this thread
 */
void ClientThread(RdmaEndpoint &ep, volatile unsigned char *const gbuf, int id,
                  std::promise<double> &&iops_result) {
    size_t base_offset = FLAGS_read_size * FLAGS_client_slots * id;

    {
        // receive an one-byte signal indicating the start of benchmark
        auto wr = ep.Recv(id, base_offset, 1);
        ep.WaitForCompletion(id, true, wr);
    }
    RAW_LOG(INFO, "thread %d start", id);

    std::this_thread::sleep_for(1s);

    {
        // send an one-byte signal indicating this thread has completed benchmark
        auto wr = ep.Send(id, base_offset, 1);
        ep.WaitForCompletion(id, true, wr);
    }
    RAW_LOG(INFO, "thread %d completed", id);
    iops_result.set_value(0.0);
}

void ClientMain() {
    volatile unsigned char *const buf =
        new volatile unsigned char[FLAGS_read_size * FLAGS_client_slots * FLAGS_client_threads]();
    RdmaEndpoint ep(nullptr, 0, buf, FLAGS_read_size * FLAGS_client_slots * FLAGS_client_threads,
                    128, 128, IBV_QPT_RC);

    for (int i = 0; i < FLAGS_client_threads; i++) {
        ep.Connect(FLAGS_endpoint.c_str());
    }
    LOG(INFO) << "all clients connected";

    std::vector<std::thread> threads;
    std::vector<std::future<double>> iopses;
    for (int i = 0; i < FLAGS_client_threads; i++) {
        std::promise<double> p;
        iopses.emplace_back(p.get_future());
        auto t = std::thread(ClientThread, std::ref(ep), buf, i, std::move(p));
        threads.emplace_back(std::move(t));
    }

    for (auto &t : threads) {
        t.join();
    }
    double total_iops = 0;
    for (auto &iops : iopses) {
        total_iops += iops.get();
    }
    LOG(INFO) << "RDMA Read IOPS: " << total_iops;
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
