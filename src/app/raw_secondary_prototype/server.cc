#include <gflags/gflags.h>
#include <glog/logging.h>
#include <glog/raw_logging.h>
#include <infiniband/verbs.h>

#include <cstring>
#include <functional>
#include <map>
#include <string>
#include <thread>
#include <vector>

#include "app/raw_secondary_prototype/common.h"
#include "network/rdma.h"
#include "util/node_config.h"

std::vector<IdType> cid2nid;   // client id to client node id
std::vector<IdType> cid2lcid;  // client id to local client id
std::vector<int> cid2threads;  // client id to threads in that client
std::vector<std::string> node2endpoint;

void ReadClientInfo() {
    int nodes = 0;
    std::map<std::string, NodeConfig> client_node_conf = ParseNodeConfig(FLAGS_client_node_config);
    for (auto &p : client_node_conf) {
        if (p.second.type != NT_CLIENT) {
            continue;
        }
        node2endpoint.push_back(p.second.endpoint);
        for (int i = 0; i < p.second.threads; i++) {
            cid2lcid.push_back(i);
            cid2nid.push_back(nodes);
            cid2threads.push_back(p.second.threads);
        }
        nodes++;
    }
    CHECK_EQ(cid2nid.size(), FLAGS_total_client_threads);
}

inline IdType GetRespConnIdx(IdType s_id, IdType c_id) {
    return FLAGS_total_client_threads + cid2nid[c_id] * FLAGS_server_threads + s_id;
}

void ServerMain(RdmaEndpoint &ep, volatile unsigned char *const buf, const IdType id) {
    // These fields should not be modified
    size_t *res_offset = new size_t[FLAGS_total_client_threads],
           *r_res_offset = new size_t[FLAGS_total_client_threads];
    volatile unsigned char **resbuf = new volatile unsigned char *[FLAGS_total_client_threads],
                           **reqbuf = new volatile unsigned char *[FLAGS_total_client_threads];
    const size_t kvs_offset = FLAGS_server_threads * FLAGS_total_client_threads *
                              (FLAGS_req_msg_slot_size + FLAGS_res_msg_slot_size) * FLAGS_msg_slots;
    volatile unsigned char *const kvsbuf = buf + kvs_offset;

    for (int c = 0; c < FLAGS_total_client_threads; c++) {
        res_offset[c] = ComputeServerMsgBufOffset(id, c, BufferType::RES);
        r_res_offset[c] =
            ComputeClientMsgBufOffset(id, cid2lcid[c], cid2threads[c], BufferType::RES);
        resbuf[c] = buf + res_offset[c];
        reqbuf[c] = buf + ComputeServerMsgBufOffset(id, c, BufferType::REQ);
    }

    std::vector<int> slot(FLAGS_total_client_threads, 0);

    int tot_processed = 0, tot_put_rounds = FLAGS_put_rounds * FLAGS_total_client_threads;
    std::vector<int> processed(FLAGS_total_client_threads, 0);

    // Use pre-initialized writes for slightly better performance
    for (int c = 0; c < FLAGS_total_client_threads; c++) {
        ep.InitializeFastWrite(GetRespConnIdx(id, c), 1);
    }

    while (tot_processed < tot_put_rounds) {
        for (int c = 0; c < FLAGS_total_client_threads; c++) {
            if (processed[c] >= FLAGS_put_rounds) {
                continue;
            }
            // check message present
            size_t req_slot_offset = ComputeReqSlotOffset(slot[c]);
            if (!CheckReqMsgPresent(reqbuf[c] + req_slot_offset)) {
                continue;
            }

            // get message
            auto kvp_ptr = ParseKvpFromMsgRaw(reqbuf[c] + req_slot_offset);
#ifndef NDEBUG
            // skip checking when measuring performance
            std::string expected_value = GetValueStr(id, c, processed[c]);
            DCHECK_EQ(processed[c], kvp_ptr->key);
            DCHECK_STREQ(expected_value.c_str(), const_cast<char *>(kvp_ptr->value));
#endif

            // write to key value storage
            size_t key_offset = ComputeKvBufOffset(kvp_ptr->key);
            // TODO: verify the necessity of atomic write
            kvp_ptr->AtomicSerializeTo(kvsbuf + key_offset);

            // serialize response before clearing
            size_t res_slot_offset = ComputeResSlotOffset(slot[c]);
#ifdef NDEBUG
            // return status code
            SerializeScalarAsMsg(resbuf[c] + res_slot_offset, 0, BufferType::RES);
#else
            SerializeKvpAsMsg(resbuf[c] + res_slot_offset, kvp_ptr, BufferType::RES);
#endif

            // clear this slot's incoming buffer for reuse in future
            // TODO: when requests are handled in batches, clear all request buffers for each batch
            // at once
            std::fill(reqbuf[c] + req_slot_offset,
                      reqbuf[c] + req_slot_offset + FLAGS_req_msg_slot_size, 0);

            // send response
            ep.Write(true, GetRespConnIdx(id, c), res_offset[c] + res_slot_offset,
                     r_res_offset[c] + res_slot_offset, FLAGS_res_msg_slot_size);
            // Receiving next request using this same slot is an indicator that this WRITE has
            // completed
            // ep.WaitForCompletion(GetRespConnIdx(id, c), true, wr);

            ++processed[c];
            ++tot_processed;
            slot[c] = (slot[c] + 1) % FLAGS_msg_slots;

            if (processed[c] % (FLAGS_put_rounds / 10) == 0) {
                RAW_DLOG(INFO, "s_id: %d, (c_id: %d) Processed: %d", id, c, processed[c]);
            }
        }
    }

    delete[] res_offset;
    delete[] r_res_offset;
    delete[] reqbuf;
    delete[] resbuf;
}

void InitializeKvs(volatile unsigned char *kvsbuf) {
    for (KeyType k = 0; k < FLAGS_kvs_entries; k++) {
        size_t key_offset = ComputeKvBufOffset(k);
        auto v = std::to_string(k);
        const KeyValuePair kvp = KeyValuePair::Create(k, v.length(), v.c_str());
        kvp.SerializeTo(kvsbuf + key_offset);
    }
}

int main(int argc, char **argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    google::InitGoogleLogging(argv[0]);

    ReadClientInfo();

    size_t buf_size = FLAGS_server_threads * FLAGS_total_client_threads *
                          (FLAGS_req_msg_slot_size + FLAGS_res_msg_slot_size) *
                          FLAGS_msg_slots                          // request/response
                      + sizeof(KeyValuePair) * FLAGS_kvs_entries;  // actual key value store

    volatile unsigned char *buffer = new volatile unsigned char[buf_size]();
    InitializeKvs(buffer + FLAGS_server_threads * FLAGS_total_client_threads *
                               (FLAGS_req_msg_slot_size + FLAGS_res_msg_slot_size) *
                               FLAGS_msg_slots);

    RdmaEndpoint ep(nullptr, 0, buffer, buf_size, 128, 128, IBV_QPT_RC);
    ep.BindToZmqEndpoint(FLAGS_kvs_server.c_str());

    for (int i = 0; i < FLAGS_total_client_threads; i++) {
        ep.Listen();
        DLOG(INFO) << "Request QP " << i << " connected";
    }

    for (int cnode = 0; cnode < node2endpoint.size(); cnode++) {
        for (int i = 0; i < FLAGS_server_threads; i++) {
            ep.Connect(node2endpoint[cnode].c_str());
            DLOG(INFO) << "Response QP " << i << " with client node " << cnode << " connected.";
        }
    }
    std::vector<std::thread> threads;
    for (int i = 0; i < FLAGS_server_threads; i++) {
        std::thread t(ServerMain, std::ref(ep), buffer, i);
        threads.emplace_back(std::move(t));
    }

    for (std::thread &t : threads) {
        t.join();
    }

    return 0;
}