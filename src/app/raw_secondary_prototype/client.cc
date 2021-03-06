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
#include "util/stats.h"
#include "util/zipf_generator.h"

DEFINE_string(node_name, "", "Node name defined in node config");
DEFINE_uint64(readbuf_slots, 32, "Buffer slots for GET using RDMA read");
DEFINE_double(put_frac, 0.5, "Fraction of operations being PUT");

struct PendingGetRequest {
    KeyType key;
    uint64_t wr_id;
    bool used;
};

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

    std::vector<PendingGetRequest> circular_pending_gets(FLAGS_readbuf_slots);
    for (int i = 0; i < FLAGS_readbuf_slots; i++) {
        circular_pending_gets[i].used = false;
    }

    std::vector<int> acked(FLAGS_server_threads), sent(FLAGS_server_threads);
    long put_ops = 0;

    IdType s = 0;
    auto increment_sid = [&s]() {
        s++;
        if (s == FLAGS_server_threads) {
            s = 0;
        }
    };
    int rslot = 0;
    long get_ops = 0;
    auto increment_rslot = [&rslot]() {
        rslot++;
        if (rslot == FLAGS_readbuf_slots) {
            rslot = 0;
        }
    };

    // Check if the received key falls in the same KVS slot as the expected key.
    auto check_key = [](volatile KeyValuePair *const kvp_ptr, KeyType expected) -> bool {
        if (kvp_ptr->lock == 0) {
            // FIXME: this check fails when multiple client threads on same node perform
            // READ at the same time
            if ((kvp_ptr->key & (FLAGS_kvs_entries - 1)) != (expected & (FLAGS_kvs_entries - 1))) {
                RAW_DLOG(FATAL, "expected key=%ld, received value='%s'", expected, kvp_ptr->value);
                return false;
            }
        }
        return true;
    };

    // Use pre-initialized writes for slightly better performance
    ep.InitializeFastWrite(id, 1);

    auto bench_begin = std::chrono::steady_clock::now();
    while (get_ops + put_ops < FLAGS_total_ops) {
        auto op = NextOp();
        if (op == KvsOp::PUT) {
            while (!used_slots[s].empty()) {
                // check message present
                int slot = used_slots[s].front();
                size_t slot_offset = ComputeResSlotOffset(slot);
                if (!CheckResMsgPresent(resbuf[s] + slot_offset)) {
                    if (free_slots[s].empty()) {
                        // keep waiting for message until this slot can be freed
                        continue;
                    } else {
                        // no need to block until this slot becomes available since there's still
                        // other free slots available
                        break;
                    }
                }

                // get message
#ifdef NDEBUG
                int rc = ParseScalarFromMsg<int>(resbuf[s] + slot_offset);
                RAW_CHECK(rc == 0, "Got a non-zero return code.");
#else
                auto kvp_ptr = ParseKvpFromMsgRaw(resbuf[s] + slot_offset);
                std::string expected_value = GetValueStr(s, gid, acked[s]);
                DCHECK_EQ(acked[s], kvp_ptr->key);
                DCHECK_STREQ(expected_value.c_str(), const_cast<char *>(kvp_ptr->value));
#endif

                // reclaim
                used_slots[s].pop();
                free_slots[s].push(slot);
                ++acked[s];
            }

            RAW_DCHECK(!free_slots[s].empty(), "there should be at least one free slots available");
            if (!free_slots[s].empty()) {
                // create message
                std::string value = GetValueStr(s, gid, sent[s]);
                auto kvp = KeyValuePair::Create(sent[s], value.length() + 1, value.c_str());
                int slot = free_slots[s].front();
                size_t req_slot_offset = ComputeReqSlotOffset(slot);
                size_t res_slot_offset = ComputeResSlotOffset(slot);
                SerializeKvpAsMsg(reqbuf[s] + req_slot_offset, kvp, BufferType::REQ);

                // clear incoming buffer
                // TODO: when requests are sent in batches, clear all response buffers for each
                // batch at once
                std::fill(resbuf[s] + res_slot_offset,
                          resbuf[s] + res_slot_offset + FLAGS_res_msg_slot_size, 0);

                // write
                ep.Write(true, id, req_offset[s] + req_slot_offset,
                         r_req_offset[s] + req_slot_offset, FLAGS_req_msg_slot_size);
                // The response from server can be used as an indicator for the request completion.
                // Therefore this waiting is optional.
                // client.WaitForCompletion(id, true, wr);

                // mark as used
                free_slots[s].pop();
                used_slots[s].push(slot);
                ++sent[s];
                ++put_ops;
            }

            increment_sid();
        } else if (op == KvsOp::GET) {
            KeyType k = NextKey();

            if (circular_pending_gets[rslot].used) {
                ep.WaitForCompletion(id, true, circular_pending_gets[rslot].wr_id);

                // check value
                auto kvp_ptr = KeyValuePair::ParseFromRaw(readbuf + ComputeReadSlotOffset(rslot));
                check_key(kvp_ptr, circular_pending_gets[rslot].key);
            }

            // issue READ request
            auto wr = ep.Read_v2(id, read_offset + ComputeReadSlotOffset(rslot),
                                 r_kvs_offset + ComputeKvBufOffset(k), sizeof(KeyValuePair));
            // store request information for later check
            circular_pending_gets[rslot] = {.key = k, .wr_id = wr, .used = true};
            increment_rslot();
            get_ops++;
        }

        if ((get_ops + put_ops) % (FLAGS_total_ops / 10) == 0) {
            RAW_DLOG(INFO, "client %2d, get ops: %ld, put ops: %ld", id, get_ops, put_ops);
        }
    }
    // process remaining pending GET requests
    ep.FlushPendingReads(id);

    // wait for pending GET requests to complete
    for (int _ = 0; _ < FLAGS_readbuf_slots; _++) {
        if (!circular_pending_gets[rslot].used) {
            continue;
        }
        ep.WaitForCompletion(id, true, circular_pending_gets[rslot].wr_id);
        // check value
        auto kvp_ptr = KeyValuePair::ParseFromRaw(readbuf + ComputeReadSlotOffset(rslot));
        check_key(kvp_ptr, circular_pending_gets[rslot].key);
        increment_rslot();
    }

    // wait for in-progress PUT requests to complete
    for (int _ = 0; _ < FLAGS_server_threads; _++) {
        while (!used_slots[s].empty()) {
            int slot = used_slots[s].front();
            size_t slot_offset = ComputeResSlotOffset(slot);
            // wait until message arrives
            while (!CheckResMsgPresent(resbuf[s] + slot_offset))
                ;

                // get message
#ifdef NDEBUG
            int rc = ParseScalarFromMsg<int>(resbuf[s] + slot_offset);
            RAW_CHECK(rc == 0, "Got a non-zero return code.");
#else
            auto kvp_ptr = ParseKvpFromMsgRaw(resbuf[s] + slot_offset);
            std::string expected_value = GetValueStr(s, gid, acked[s]);
            DCHECK_EQ(acked[s], kvp_ptr->key);
            DCHECK_STREQ(expected_value.c_str(), const_cast<char *>(kvp_ptr->value));
#endif

            // reclaim
            used_slots[s].pop();
            free_slots[s].push(slot);
            ++acked[s];
        }
        increment_sid();
    }

    auto bench_end = std::chrono::steady_clock::now();
    get_iops.set_value(ComputeOperationsPerSecond(bench_begin, bench_end, get_ops));
    put_iops.set_value(ComputeOperationsPerSecond(bench_begin, bench_end, put_ops));

    delete[] req_offset;
    delete[] r_req_offset;
    delete[] reqbuf;
    delete[] resbuf;
}

int main(int argc, char **argv) {
    gflags::SetUsageMessage("Secondary key-value store client prototype.");
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

    // set batch size after all connections are established
    ep.SetReadBatchSize(1);

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