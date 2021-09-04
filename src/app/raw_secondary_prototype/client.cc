#include <gflags/gflags.h>
#include <glog/logging.h>
#include <glog/raw_logging.h>
#include <infiniband/verbs.h>

#include <algorithm>
#include <queue>
#include <sstream>
#include <thread>
#include <vector>

#include "app/raw_secondary_prototype/common.h"
#include "network/rdma.h"

void ClientMain(RdmaClient &client, volatile unsigned char *const buf, IdType id) {
    // These fields should not be modified
    size_t *out_offset = new size_t[FLAGS_server_threads],
           *r_in_offset = new size_t[FLAGS_server_threads];
    volatile unsigned char **outbuf = new volatile unsigned char *[FLAGS_server_threads],
                           **inbuf = new volatile unsigned char *[FLAGS_server_threads];

    for (int s = 0; s < FLAGS_server_threads; s++) {
        out_offset[s] = ComputeMsgBufOffset(s, id, false);
        r_in_offset[s] = ComputeMsgBufOffset(s, id, true);
        outbuf[s] = buf + out_offset[s];
        inbuf[s] = buf + ComputeMsgBufOffset(s, id, true);
    }

    std::vector<std::queue<int>> free_slots(FLAGS_server_threads), used_slots(FLAGS_server_threads);
    for (int s = 0; s < FLAGS_server_threads; s++) {
        for (int i = 0; i < FLAGS_msg_slots; i++) {
            free_slots[s].push(i);
        }
    }

    int tot_acked = 0, tot_sent = 0, tot_rounds = FLAGS_rounds * FLAGS_server_threads;
    std::vector<int> acked(FLAGS_server_threads), sent(FLAGS_server_threads);

    while (tot_acked < tot_rounds) {
        for (int s = 0; s < FLAGS_server_threads; s++) {
            if (sent[s] < FLAGS_rounds && !free_slots[s].empty()) {
                // create message
                std::string value = GetValueStr(s, id, sent[s]);
                auto kvp = KeyValuePair::Create(sent[s], value.length() + 1, value.c_str());
                int slot = free_slots[s].front();
                size_t slot_offset = ComputeSlotOffset(slot);
                SerializeKvpAsMsg(outbuf[s] + slot_offset, kvp);

                // clear incoming buffer
                std::fill(inbuf[s] + slot_offset, inbuf[s] + slot_offset + FLAGS_msg_slot_size, 0);

                // write
                auto wr = client.Write(false, id, out_offset[s] + slot_offset,
                                       r_in_offset[s] + slot_offset, FLAGS_msg_slot_size);
                // The response from server can be used as an indicator for the request completion.
                // Therefore this waiting is optional.
                // client.WaitForCompletion(id, true, wr);

                // mark as used
                free_slots[s].pop();
                used_slots[s].push(slot);
                ++sent[s];
                ++tot_sent;

                if (sent[s] % (FLAGS_rounds / 10) == 0) {
                    RAW_LOG(INFO, "c_id: %d, (s_id: %d) Sent: %d", id, s, sent[s]);
                }
            }

            while (!used_slots[s].empty() && acked[s] < FLAGS_rounds) {
                // check message present
                int slot = used_slots[s].front();
                size_t slot_offset = ComputeSlotOffset(slot);
                if (!CheckMsgPresent(inbuf[s] + slot_offset)) {
                    break;
                }

                // get message
                auto kvp = ParseKvpFromMsg(inbuf[s] + slot_offset);
                std::string expected_value = GetValueStr(s, id, acked[s]);
                CHECK_EQ(acked[s], kvp.key);
                CHECK_STREQ(expected_value.c_str(), kvp.value);

                // reclaim
                used_slots[s].pop();
                free_slots[s].push(slot);
                ++acked[s];
                ++tot_acked;

                if (acked[s] % (FLAGS_rounds / 10) == 0) {
                    RAW_LOG(INFO, "c_id: %d, (s_id: %d) Acknowledged: %d (last: '%s')", id, s,
                            acked[s], kvp.value);
                }
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
    RdmaClient client(nullptr, 0, buffer, buf_size, 128, 128, IBV_QPT_RC);

    for (int i = 0; i < FLAGS_client_threads; i++) {
        client.Connect(FLAGS_endpoint.c_str());
        DLOG(INFO) << "Thread " << i << " connected to server.";
    }
    std::vector<std::thread> threads;
    for (int i = 0; i < FLAGS_client_threads; i++) {
        std::thread t(ClientMain, std::ref(client), buffer, i);
        threads.emplace_back(std::move(t));
    }

    for (std::thread &t : threads) {
        t.join();
    }

    return 0;
}