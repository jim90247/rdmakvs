#include <gflags/gflags.h>
#include <glog/logging.h>
#include <glog/raw_logging.h>
#include <infiniband/verbs.h>

#include <cstring>
#include <functional>
#include <thread>
#include <vector>

#include "app/raw_secondary_prototype/common.h"
#include "network/rdma.h"

// TODO: compute id when multiple client nodes is possible
inline IdType GetClientNodeId(IdType c_id) { return 0; }

inline IdType GetRespConnIdx(IdType s_id, IdType c_id) {
    return FLAGS_client_threads + GetClientNodeId(c_id) * FLAGS_server_threads + s_id;
}

void ServerMain(RdmaEndpoint &ep, volatile unsigned char *const buf, const IdType id) {
    // These fields should not be modified
    size_t *out_offset = new size_t[FLAGS_client_threads],
           *r_in_offset = new size_t[FLAGS_client_threads];
    volatile unsigned char **outbuf = new volatile unsigned char *[FLAGS_client_threads],
                           **inbuf = new volatile unsigned char *[FLAGS_client_threads];

    for (int c = 0; c < FLAGS_client_threads; c++) {
        out_offset[c] = ComputeMsgBufOffset(id, c, false);
        r_in_offset[c] = ComputeMsgBufOffset(id, c, true);
        outbuf[c] = buf + out_offset[c];
        inbuf[c] = buf + ComputeMsgBufOffset(id, c, true);
    }

    std::vector<int> slot(FLAGS_client_threads, 0);

    int tot_processed = 0, tot_rounds = FLAGS_rounds * FLAGS_client_threads;
    std::vector<int> processed(FLAGS_client_threads, 0);

    while (tot_processed < tot_rounds) {
        for (int c = 0; c < FLAGS_client_threads; c++) {
            if (processed[c] >= FLAGS_rounds) {
                continue;
            }
            // check message present
            size_t slot_offset = ComputeSlotOffset(slot[c]);
            if (!CheckMsgPresent(inbuf[c] + slot_offset)) {
                continue;
            }

            // get message
            auto kvp = ParseKvpFromMsg(inbuf[c] + slot_offset);
            std::string expected_value = GetValueStr(id, c, processed[c]);
            CHECK_EQ(processed[c], kvp.key);
            CHECK_STREQ(expected_value.c_str(), kvp.value);

            // clear this slot's incoming buffer for reuse in future
            std::fill(inbuf[c] + slot_offset, inbuf[c] + slot_offset + FLAGS_msg_slot_size, 0);

            // send response
            SerializeKvpAsMsg(outbuf[c] + slot_offset, kvp);
            auto wr = ep.Write(false, GetRespConnIdx(id, c), out_offset[c] + slot_offset,
                               r_in_offset[c] + slot_offset, FLAGS_msg_slot_size);
            ep.WaitForCompletion(GetRespConnIdx(id, c), true, wr);

            ++processed[c];
            ++tot_processed;
            slot[c] = (slot[c] + 1) % FLAGS_msg_slots;

            if (processed[c] % (FLAGS_rounds / 10) == 0) {
                RAW_LOG(INFO, "s_id: %d, (c_id: %d) Processed: %d", id, c, processed[c]);
            }
        }
    }
}

int main(int argc, char **argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    google::InitGoogleLogging(argv[0]);

    size_t buf_size =
        FLAGS_server_threads * FLAGS_client_threads * FLAGS_msg_slot_size * FLAGS_msg_slots * 2;

    volatile unsigned char *buffer = new volatile unsigned char[buf_size]();
    RdmaEndpoint ep(nullptr, 0, buffer, buf_size, 128, 128, IBV_QPT_RC);
    ep.BindToZmqEndpoint(FLAGS_kvs_server.c_str());

    for (int i = 0; i < FLAGS_client_threads; i++) {
        ep.Listen();
        DLOG(INFO) << "Request QP " << i << " connected";
    }

    for (int cnode = 0; cnode < FLAGS_client_nodes; cnode++) {
        for (int i = 0; i < FLAGS_server_threads; i++) {
            ep.Connect(FLAGS_kvs_client.c_str());
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