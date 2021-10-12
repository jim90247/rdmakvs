#include <gflags/gflags.h>
#include <glog/logging.h>
#include <infiniband/verbs.h>
#include <unistd.h>

#include <chrono>
#include <cstdint>
#include <cstdio>
#include <future>
#include <thread>
#include <vector>

#include "network/rdma.h"
#include "util/stats.h"

using std::uint64_t;
using namespace std::chrono_literals;

DEFINE_string(endpoint, "tcp://192.168.223.1:7889", "Zmq endpoint");
DEFINE_bool(server, false, "Run as server");
DEFINE_int32(threads, 1,
             "Total # of client threads (server), or total # of threads for this client (client)");
DEFINE_uint64(thread_buffer_size, 1 << 20, "Buffer size per thread");
DEFINE_uint64(rounds, 16 << 20, "Total requests to send");
DEFINE_int32(batch_size, 1, "WRs per ibv_post_send");
DEFINE_int32(element_size, 1, "Bytes of each element");

size_t buffer_start_offset = 0;

void ServerMain() {
    const size_t buffer_size = FLAGS_thread_buffer_size * FLAGS_threads + buffer_start_offset;
    volatile char *buffer = new volatile char[buffer_size]();

    RdmaEndpoint rdma_server(nullptr, 0, reinterpret_cast<volatile unsigned char *>(buffer),
                             buffer_size, 128, 128, IBV_QPT_RC);
    rdma_server.BindToZmqEndpoint(FLAGS_endpoint.c_str());

    std::vector<uint64_t> recv_wr_ids;
    // Use one queue pair per thread
    for (int t = 0; t < FLAGS_threads; t++) {
        rdma_server.Listen();
        recv_wr_ids.push_back(rdma_server.Recv(t, t * sizeof(int), sizeof(int)));
    }
    LOG(INFO) << "All clients connected";

    // sleep three seconds for clients to post recv
    std::this_thread::sleep_for(3s);

    // send start signal to clients (the cotent is the client id)
    for (int t = 0; t < FLAGS_threads; t++) {
        *reinterpret_cast<volatile int *>(buffer + t * sizeof(int)) = t;
        rdma_server.Send(t, t * sizeof(int), sizeof(int));
    }

    LOG(INFO) << "Start microbenchmark";

    // Wait for clients' response
    for (int i = 0; i < FLAGS_threads; i++) {
        rdma_server.WaitForCompletion(i, true, recv_wr_ids[i]);
        CHECK_EQ(*reinterpret_cast<volatile int *>(buffer + i * sizeof(int)), i);
    }
    LOG(INFO) << "All clients completed successfully";
}

void ClientThreadWriteMain(volatile char *buffer, RdmaEndpoint &client, size_t remote_id,
                           std::promise<double> &&iops_result) {
    size_t local_thread_offset = remote_id * FLAGS_thread_buffer_size;
    size_t remote_thread_offset = remote_id * FLAGS_thread_buffer_size + buffer_start_offset;
    volatile char *thread_buffer = buffer + local_thread_offset;

    // Post recv to receive start signal/client id
    uint64_t start_signal_tracker =
        client.Recv(remote_id, remote_id * FLAGS_thread_buffer_size, sizeof(int));
    client.WaitForCompletion(remote_id, true, start_signal_tracker);

    // the unique id at server side
    int client_id = *reinterpret_cast<volatile int *>(thread_buffer);
    LOG(INFO) << "Client " << client_id << " (remote_id=" << remote_id << ") starts now";

    // start benchmark
    client.InitializeFastWrite(remote_id, FLAGS_batch_size);
    std::vector<std::tuple<uint64_t, uint64_t, uint32_t>> requests(FLAGS_batch_size);
    auto start = std::chrono::steady_clock::now();
    int batch_idx = 0;
    uint64_t offset = 0;
    for (uint64_t round = 0; round < FLAGS_rounds; round++) {
        requests[batch_idx] = std::make_tuple(local_thread_offset + offset,
                                              remote_thread_offset + offset, FLAGS_element_size);
        offset += FLAGS_element_size;
        if (offset >= FLAGS_thread_buffer_size) {
            offset = 0;
        }
        if (++batch_idx >= FLAGS_batch_size) {
            // rely on auto reclaim feature
            client.WriteBatch(true, remote_id, requests, SignalStrategy::kSignalLast,
                              IBV_SEND_INLINE);
            batch_idx = 0;
        }
    }
    auto end = std::chrono::steady_clock::now();

    *reinterpret_cast<volatile int *>(thread_buffer) = client_id;
    uint64_t end_response_tracker = client.Send(remote_id, local_thread_offset, sizeof(int));
    client.WaitForCompletion(remote_id, true, end_response_tracker);

    double iops = ComputeOperationsPerSecond(start, end, FLAGS_rounds);
    iops_result.set_value(iops);
    LOG(INFO) << "Client " << client_id << " completes with IOPS " << iops;
}

void ClientMain() {
    const size_t buffer_size = FLAGS_thread_buffer_size * FLAGS_threads + buffer_start_offset;
    volatile char *buffer = new volatile char[buffer_size]();
    RdmaEndpoint rdma_client(nullptr, 0, reinterpret_cast<volatile unsigned char *>(buffer),
                             buffer_size, 128, 128, IBV_QPT_RC);

    std::vector<std::thread> threads;
    std::vector<std::promise<double>> iops_promises(FLAGS_threads);
    std::vector<std::future<double>> iops_futures;
    for (int t = 0; t < FLAGS_threads; t++) {
        rdma_client.Connect(FLAGS_endpoint.c_str());
        LOG(INFO) << "Connection " << t << " established";
    }
    for (int t = 0; t < FLAGS_threads; t++) {
        iops_futures.push_back(iops_promises[t].get_future());
        threads.push_back(std::thread(ClientThreadWriteMain, buffer, std::ref(rdma_client), t,
                                      std::move(iops_promises[t])));
    }

    for (auto &thread : threads) {
        thread.join();
    }
    double iops_total = 0;
    for (auto &iops_future : iops_futures) {
        iops_total += iops_future.get();
    }
    LOG(INFO) << "Total IOPS: " << iops_total;
}

int main(int argc, char **argv) {
    // TODO: the benchmark overhead is too large: single thread 80% throughput compared to HERD, and
    // 16 threads only 50% throughput, maximum around 70~80 Mops.
    gflags::SetUsageMessage("A micro-benchmark for RDMA Writes.");
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    google::InitGoogleLogging(argv[0]);

    buffer_start_offset = sizeof(int) * FLAGS_threads;
    if (FLAGS_server) {
        ServerMain();
    } else {
        ClientMain();
    }
    return 0;
}
