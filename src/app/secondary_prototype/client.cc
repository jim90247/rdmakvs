#include <gflags/gflags.h>
#include <glog/logging.h>

#include <cstring>
#include <random>
#include <thread>

#include "app/secondary_prototype/common.h"
#include "messaging/rdma_messaging.h"
#include "network/rdma.h"

DEFINE_string(endpoint, "tcp://192.168.223.1:7889", "Zmq endpoint");
DEFINE_int32(threads, 1, "Number of threads.");

bool DoPut() {
    thread_local static auto gen = std::mt19937(42);
    auto distrib = std::uniform_int_distribution<int>(1, 5);
    return distrib(gen) == 1;
}

size_t ComputeRemoteMsgOffset(int client_id) { return client_id * kPerThreadMsgBufferSize; }

void ClientThreadMain(RdmaEndpoint &client, volatile unsigned char *buffer, int local_id) {
    const size_t base_offset = local_id * kClientPerThreadBufferSize;
    const size_t read_buffer_offset = base_offset + kPerThreadMsgBufferSize;

    // Exchange id
    *reinterpret_cast<volatile int *>(buffer + base_offset) = local_id;
    ExchangeInfo(client, local_id, base_offset, sizeof(int), base_offset + sizeof(int),
                 2 * sizeof(int), false);
    const int client_id = *reinterpret_cast<volatile int *>(buffer + base_offset + sizeof(int));
    const int total_server_threads =
        *reinterpret_cast<volatile int *>(buffer + base_offset + 2 * sizeof(int));
    LOG(INFO) << "client " << client_id << "/" << total_server_threads;

    rdmamsg::RdmaWriteMessagingEndpoint msg_client(
        client, buffer, local_id, local_id * kClientPerThreadBufferSize,
        ComputeRemoteMsgOffset(client_id), kPerThreadMsgBufferSize);

    for (int i = 0; i < 20; i++) {
        __int128 key = i % 10;
        if (DoPut()) {
            // Send request
            volatile char *msg_buffer =
                static_cast<volatile char *>(msg_client.AllocateOutboundMessageBuffer(kEntrySize));
            std::stringstream ss;
            ss << "client " << local_id << " value " << i;
            KeyValuePair kv_pair = CreateKvPair(key, ss.str());
            SerializeKeyValuePair(msg_buffer, kv_pair);
            uint64_t wr = msg_client.FlushOutboundMessage();
            msg_client.BlockUntilComplete(wr);

            // Wait for ACK
            rdmamsg::InboundMessage response;
            do {
                response = msg_client.CheckInboundMessage();
            } while (response.size == 0);
            CHECK_EQ(*static_cast<volatile int *>(response.data), 0);
            LOG(INFO) << "local_id " << local_id
                      << " PUT: key = " << static_cast<int64_t>(kv_pair.key)
                      << ", value = " << kv_pair.value;
        } else {
            size_t key_offset =
                ComputeKeyOffset(key) + total_server_threads * kPerThreadMsgBufferSize;
            uint64_t wr_id = client.Read(local_id, read_buffer_offset, key_offset, kEntrySize);
            client.WaitForCompletion(local_id, true, wr_id);
            KeyValuePair kv_pair = ParseKeyValuePair(buffer + read_buffer_offset);
            CHECK(kv_pair.key == key);
            LOG(INFO) << "local_id " << local_id
                      << " GET: key = " << static_cast<int64_t>(kv_pair.key)
                      << ", value = " << kv_pair.value;
        }
    }
}

int main(int argc, char **argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    google::InitGoogleLogging(argv[0]);

    volatile unsigned char *buffer =
        new volatile unsigned char[kClientPerThreadBufferSize * FLAGS_threads]();
    RdmaEndpoint client(nullptr, 0, buffer, kClientPerThreadBufferSize * FLAGS_threads, 128, 128,
                        IBV_QPT_RC);

    // all clients connect to same remote
    for (int i = 0; i < FLAGS_threads; i++) {
        client.Connect(FLAGS_endpoint.c_str());
    }
    LOG(INFO) << "All connections established";

    std::vector<std::thread> threads;
    for (int i = 0; i < FLAGS_threads; i++) {
        threads.emplace_back(std::thread(ClientThreadMain, std::ref(client), buffer, i));
    }
    for (auto &t : threads) {
        t.join();
    }
    return 0;
}
