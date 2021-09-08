#include <gflags/gflags.h>
#include <glog/logging.h>
#include <glog/raw_logging.h>
#include <infiniband/verbs.h>

#include <algorithm>
#include <chrono>
#include <future>
#include <map>
#include <queue>
#include <random>
#include <sstream>
#include <thread>
#include <vector>

#include "app/raw_secondary_prototype/common.h"
#include "network/rdma.h"
#include "util/node_config.h"
#include "util/zipf_generator.h"

DEFINE_string(node_name, "", "Node name defined in node config");
DEFINE_uint64(readbuf_slots, 128, "Buffer slots for GET using RDMA read");
DEFINE_double(put_frac, 0.5, "Fraction of operations being PUT");

std::vector<IdType> id2gid;  // client local id to global client id
int local_threads;           // number of threads in this client
std::string local_endpoint;  // local zmq endpoint

void ReadClientInfo() {
    std::map<std::string, NodeConfig> client_node_conf = ParseNodeConfig(FLAGS_client_node_config);
    int id_offset = 0;
    for (auto &p : client_node_conf) {
        if (p.second.type != NT_CLIENT) {
            continue;
        }
        if (p.first != FLAGS_node_name) {
            id_offset += p.second.threads;
            continue;
        }
        for (int i = 0; i < p.second.threads; i++) {
            id2gid.push_back(id_offset + i);
        }
        local_threads = p.second.threads;
        local_endpoint = p.second.endpoint;
    }
    CHECK_GT(id2gid.size(), 0) << "Node '" << FLAGS_node_name << "' not found in "
                               << FLAGS_client_node_config;
}

enum class KvsOp {
    GET,
    PUT,
};

KvsOp NextOp() {
    static thread_local std::mt19937_64 gen(42);
    static thread_local std::uniform_real_distribution<double> distrib(0, 1);
    if (distrib(gen) < FLAGS_put_frac) {
        return KvsOp::PUT;
    } else {
        return KvsOp::GET;
    }
}

KeyType NextKey() {
    static thread_local ZipfGenerator zipf(FLAGS_kvs_entries, 0.99);
    return zipf.GetNumber();
}

inline size_t ComputeReadSlotOffset(int sid) { return sid * sizeof(KeyValuePair); }

void ClientMain(RdmaEndpoint &ep, volatile unsigned char *const buf, IdType id,
                std::promise<double> &&get_iops, std::promise<double> &&put_iops) {
    // These fields should not be modified
    const IdType gid = id2gid[id];
    size_t *req_offset = new size_t[FLAGS_server_threads],
           *r_req_offset = new size_t[FLAGS_server_threads];
    volatile unsigned char **reqbuf = new volatile unsigned char *[FLAGS_server_threads],
                           **resbuf = new volatile unsigned char *[FLAGS_server_threads];

    const size_t r_kvs_offset = FLAGS_server_threads * FLAGS_total_client_threads *
                                (FLAGS_req_msg_slot_size + FLAGS_res_msg_slot_size) *
                                FLAGS_msg_slots;
    const size_t read_offset = FLAGS_server_threads * local_threads *
                                   (FLAGS_req_msg_slot_size + FLAGS_res_msg_slot_size) *
                                   FLAGS_msg_slots +
                               id * FLAGS_readbuf_slots * sizeof(KeyValuePair);
    volatile unsigned char *const readbuf = buf + read_offset;

    for (int s = 0; s < FLAGS_server_threads; s++) {
        req_offset[s] = ComputeClientMsgBufOffset(s, id, local_threads, BufferType::REQ);
        r_req_offset[s] = ComputeServerMsgBufOffset(s, gid, BufferType::REQ);
        reqbuf[s] = buf + req_offset[s];
        resbuf[s] = buf + ComputeClientMsgBufOffset(s, id, local_threads, BufferType::RES);
    }

    std::vector<std::queue<int>> free_slots(FLAGS_server_threads), used_slots(FLAGS_server_threads);
    for (int s = 0; s < FLAGS_server_threads; s++) {
        for (int i = 0; i < FLAGS_msg_slots; i++) {
            free_slots[s].push(i);
        }
    }

    int tot_acked = 0, tot_sent = 0, tot_put_rounds = FLAGS_put_rounds * FLAGS_server_threads;
    std::vector<int> acked(FLAGS_server_threads), sent(FLAGS_server_threads);

    IdType s = 0;
    auto increment_sid = [&s]() {
        s++;
        if (s == FLAGS_server_threads) {
            s = 0;
        }
    };
    int rslot = 0;
    long get_rounds = 0;
    auto increment_rslot = [&rslot, &get_rounds]() {
        rslot++;
        if (rslot == FLAGS_readbuf_slots) {
            rslot = 0;
        }
        get_rounds++;
    };

    auto bench_begin = std::chrono::steady_clock::now();
    while (tot_acked < tot_put_rounds) {
        auto op = NextOp();
        if (op == KvsOp::PUT) {
            if (sent[s] < FLAGS_put_rounds && !free_slots[s].empty()) {
                // create message
                std::string value = GetValueStr(s, gid, sent[s]);
                auto kvp = KeyValuePair::Create(sent[s], value.length() + 1, value.c_str());
                int slot = free_slots[s].front();
                size_t req_slot_offset = ComputeReqSlotOffset(slot);
                size_t res_slot_offset = ComputeResSlotOffset(slot);
                SerializeKvpAsMsg(reqbuf[s] + req_slot_offset, kvp, BufferType::REQ);

                // clear incoming buffer
                std::fill(resbuf[s] + res_slot_offset,
                          resbuf[s] + res_slot_offset + FLAGS_res_msg_slot_size, 0);

                // write
                auto wr = ep.Write(false, id, req_offset[s] + req_slot_offset,
                                   r_req_offset[s] + req_slot_offset, FLAGS_req_msg_slot_size);
                // The response from server can be used as an indicator for the request completion.
                // Therefore this waiting is optional.
                // client.WaitForCompletion(id, true, wr);

                // mark as used
                free_slots[s].pop();
                used_slots[s].push(slot);
                ++sent[s];
                ++tot_sent;

                if (sent[s] % (FLAGS_put_rounds / 10) == 0) {
                    RAW_DLOG(INFO, "c_id: %d, (s_id: %d) Sent: %d", id, s, sent[s]);
                }
            }

            while (!used_slots[s].empty() && acked[s] < FLAGS_put_rounds) {
                // check message present
                int slot = used_slots[s].front();
                size_t slot_offset = ComputeResSlotOffset(slot);
                if (!CheckResMsgPresent(resbuf[s] + slot_offset)) {
                    break;
                }

                // get message
                auto kvp_ptr = ParseKvpFromMsgRaw(resbuf[s] + slot_offset);
#ifndef NDEBUG
                // skip checking when measuring performance
                std::string expected_value = GetValueStr(s, gid, acked[s]);
                DCHECK_EQ(acked[s], kvp_ptr->key);
                DCHECK_STREQ(expected_value.c_str(), const_cast<char *>(kvp_ptr->value));
#endif

                // reclaim
                used_slots[s].pop();
                free_slots[s].push(slot);
                ++acked[s];
                ++tot_acked;

                if (acked[s] % (FLAGS_put_rounds / 10) == 0) {
                    RAW_DLOG(INFO, "c_id: %d, (s_id: %d) Acknowledged: %d (last: '%s')", id, s,
                             acked[s], kvp_ptr->value);
                }
            }
            increment_sid();
        } else if (op == KvsOp::GET) {
            KeyType k = NextKey();

            // send request
            size_t key_offset = ComputeKvBufOffset(k);
            size_t rslot_offset = ComputeReadSlotOffset(rslot);
            auto wr = ep.Read(id, read_offset + rslot_offset, r_kvs_offset + key_offset,
                              sizeof(KeyValuePair));
            ep.WaitForCompletion(id, true, wr);

            // check value
            auto kvp = KeyValuePair::ParseFrom(readbuf + rslot_offset);
            if (kvp.lock == 0) {
                // FIXME: this check fails when multiple client threads on same node perform READ at
                // the same time
                if ((kvp.key & (FLAGS_kvs_entries - 1)) != (k & (FLAGS_kvs_entries - 1))) {
                    RAW_LOG(FATAL, "key=%ld, received value='%s', get rounds=%ld", k, kvp.value,
                            get_rounds);
                }
            }

            increment_rslot();
        }
    }

    auto bench_end = std::chrono::steady_clock::now();
    get_iops.set_value(get_rounds / std::chrono::duration<double>(bench_end - bench_begin).count());
    put_iops.set_value(tot_acked / std::chrono::duration<double>(bench_end - bench_begin).count());
}

int main(int argc, char **argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    google::InitGoogleLogging(argv[0]);

    ReadClientInfo();

    size_t buf_size =
        FLAGS_server_threads * local_threads * (FLAGS_req_msg_slot_size + FLAGS_res_msg_slot_size) *
            FLAGS_msg_slots +                                        // request/response
        local_threads * FLAGS_readbuf_slots * sizeof(KeyValuePair);  // buffer for GET via RDMA read

    // volatile unsigned char *buffer = new volatile unsigned char[buf_size]();
    volatile unsigned char *buffer =
        reinterpret_cast<volatile unsigned char *>(aligned_alloc(4096, buf_size));
    RdmaEndpoint ep(nullptr, 0, buffer, buf_size, 128, 128, IBV_QPT_RC);

    for (int i = 0; i < local_threads; i++) {
        ep.Connect(FLAGS_kvs_server.c_str());
        DLOG(INFO) << "Request QP " << i << " connected.";
    }
    ep.BindToZmqEndpoint(local_endpoint.c_str());
    for (int i = 0; i < FLAGS_server_threads; i++) {
        ep.Listen();
        DLOG(INFO) << "Response QP " << i << " connected.";
    }
    std::vector<std::thread> threads;
    std::vector<std::future<double>> get_iopses, put_iopses;
    for (int i = 0; i < local_threads; i++) {
        std::promise<double> g, p;
        get_iopses.emplace_back(g.get_future());
        put_iopses.emplace_back(p.get_future());
        std::thread t(ClientMain, std::ref(ep), buffer, i, std::move(g), std::move(p));
        threads.emplace_back(std::move(t));
    }

    for (std::thread &t : threads) {
        t.join();
    }

    double tot_get_iops = 0, tot_put_iops = 0;
    for (auto &g : get_iopses) {
        tot_get_iops += g.get();
    }
    for (auto &p : put_iopses) {
        tot_put_iops += p.get();
    }
    LOG(INFO) << "Total GET IOPS: " << tot_get_iops << ", PUT IOPS: " << tot_put_iops;
    return 0;
}