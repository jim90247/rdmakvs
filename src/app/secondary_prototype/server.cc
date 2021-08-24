#include <gflags/gflags.h>
#include <glog/logging.h>

#include <sstream>
#include <string>
#include <thread>

#include "app/secondary_prototype/common.h"
#include "messaging/rdma_messaging.h"
#include "network/rdma.h"

DEFINE_string(endpoint, "tcp://192.168.223.1:7889", "Zmq endpoint");
DEFINE_int32(threads, 1, "Number of threads.");

size_t ComputeRemoteMsgOffset(int client_local_id) {
    return client_local_id * kClientPerThreadBufferSize;
}

void InitializeKvsData(volatile unsigned char *kvs_buffer) {
    for (int i = 0; i * kEntrySize < kKvsBufferSize; i++) {
        std::stringstream ss;
        ss << "server value " << i;
        KeyValuePair kv_pair = CreateKvPair(i, ss.str());
        SerializeKeyValuePair(kvs_buffer + i * kEntrySize, kv_pair);
    }
}

void ServerThreadMain(RdmaServer *server, volatile unsigned char *buffer, int id) {
    const size_t msg_base_offset = id * kPerThreadMsgBufferSize;
    const size_t kvs_base_offset = FLAGS_threads * kPerThreadMsgBufferSize;

    *reinterpret_cast<volatile int *>(buffer + msg_base_offset + sizeof(int)) = id;
    *reinterpret_cast<volatile int *>(buffer + msg_base_offset + sizeof(int) * 2) = FLAGS_threads;
    ExchangeInfo(server, id, msg_base_offset + sizeof(int),
                 2 * sizeof(int),               // send client_id and total threads
                 msg_base_offset, sizeof(int),  // receive client's local id
                 true);
    int client_local_id = *reinterpret_cast<volatile int *>(buffer + msg_base_offset);
    LOG(INFO) << "Thread " << id << " completes setup (remote id: " << client_local_id << ")";

    rdmamsg::RdmaWriteMessagingEndpoint msg_ep(server, buffer, id, msg_base_offset,
                                               ComputeRemoteMsgOffset(client_local_id),
                                               kPerThreadMsgBufferSize);
    while (true) {
        auto msg = msg_ep.CheckInboundMessage();
        if (msg.size == 0) continue;

        KeyValuePair kv_pair = ParseKeyValuePair(msg.data);
        // copy new key-value pair to KVS table
        SerializeKeyValuePair(buffer + kvs_base_offset + ComputeKeyOffset(kv_pair.key), kv_pair);

        // Send ACK
        volatile void *response_buffer = msg_ep.AllocateOutboundMessageBuffer(sizeof(int));
        *static_cast<volatile int *>(response_buffer) = 0;
        uint64_t wr = msg_ep.FlushOutboundMessage();
        msg_ep.BlockUntilComplete(wr);
    }
}

int main(int argc, char **argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    google::InitGoogleLogging(argv[0]);

    const size_t total_buffer_size = FLAGS_threads * kPerThreadMsgBufferSize + kKvsBufferSize;
    volatile unsigned char *buffer = new volatile unsigned char[total_buffer_size]();
    InitializeKvsData(buffer + FLAGS_threads * kPerThreadMsgBufferSize);
    RdmaServer *server = new RdmaServer(FLAGS_endpoint.c_str(), nullptr, 0, buffer,
                                        total_buffer_size, 128, 128, IBV_QPT_RC);

    for (int i = 0; i < FLAGS_threads; i++) {
        server->Listen();
    }
    LOG(INFO) << "All clients connected";

    std::vector<std::thread> threads;
    for (int i = 0; i < FLAGS_threads; i++) {
        threads.emplace_back(std::thread(ServerThreadMain, server, buffer, i));
    }
    for (auto &t : threads) {
        t.join();
    }
    return 0;
}
