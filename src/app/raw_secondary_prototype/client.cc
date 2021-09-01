#include <gflags/gflags.h>
#include <glog/logging.h>
#include <infiniband/verbs.h>

#include <algorithm>
#include <queue>
#include <sstream>

#include "app/raw_secondary_prototype/common.h"
#include "network/rdma.h"

inline std::string GetValueStr(int i) {
    std::stringstream ss;
    ss << "'key=" << i << "'";
    return ss.str();
}

void ClientMain(RdmaClient &client, volatile unsigned char *const buf) {
    const size_t out_offset = 0, in_offset = FLAGS_msg_slot_size * FLAGS_msg_slots;
    volatile unsigned char *const outbuf = buf;
    volatile unsigned char *const inbuf = buf + in_offset;

    std::queue<int> free_slots, used_slots;
    for (int i = 0; i < FLAGS_msg_slots; i++) {
        free_slots.push(i);
    }

    int acked = 0, sent = 0;
    while (acked < FLAGS_rounds) {
        if (sent < FLAGS_rounds && !free_slots.empty()) {
            // create message
            std::string value = GetValueStr(sent);
            auto kvp = KeyValuePair::Create(sent, value.length() + 1, value.c_str());
            int slot = free_slots.front();
            size_t slot_offset = ComputeSlotOffset(slot);
            SerializeKvpAsMsg(outbuf + slot_offset, kvp);

            // clear incoming buffer
            std::fill(inbuf + slot_offset, inbuf + slot_offset + FLAGS_msg_slot_size, 0);

            // write
            auto wr = client.Write(false, 0, out_offset + slot_offset, in_offset + slot_offset,
                                   FLAGS_msg_slot_size);
            // The response from server can be used as an indicator for the request completion.
            // Therefore this waiting is optional.
            // client.WaitForCompletion(0, true, wr);

            // mark as used
            free_slots.pop();
            used_slots.push(slot);
            ++sent;

            LOG_EVERY_N(INFO, FLAGS_rounds / 10) << "Sent: " << sent;
        }

        while (!used_slots.empty() && acked < FLAGS_rounds) {
            // check message present
            int slot = used_slots.front();
            size_t slot_offset = ComputeSlotOffset(slot);
            if (!CheckMsgPresent(inbuf + slot_offset)) {
                break;
            }

            // get message
            auto kvp = ParseKvpFromMsg(inbuf + slot_offset);
            std::string expected_value = GetValueStr(acked);
            CHECK_EQ(acked, kvp.key);
            CHECK_STREQ(expected_value.c_str(), kvp.value);

            // reclaim
            used_slots.pop();
            free_slots.push(slot);
            ++acked;

            LOG_EVERY_N(INFO, FLAGS_rounds / 10) << "Acknowledged: " << acked;
        }
    }
}

int main(int argc, char **argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    google::InitGoogleLogging(argv[0]);

    size_t buf_size = FLAGS_msg_slot_size * FLAGS_msg_slots * 2;

    volatile unsigned char *buffer = new volatile unsigned char[buf_size]();
    RdmaClient client(nullptr, 0, buffer, buf_size, 128, 128, IBV_QPT_RC);

    client.Connect(FLAGS_endpoint.c_str());
    LOG(INFO) << "Client connected to server.";

    ClientMain(std::ref(client), buffer);

    return 0;
}