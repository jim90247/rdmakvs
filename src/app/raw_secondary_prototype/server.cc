#include <gflags/gflags.h>
#include <glog/logging.h>
#include <infiniband/verbs.h>

#include <cstring>

#include "app/raw_secondary_prototype/common.h"
#include "network/rdma.h"

inline std::string GetValueStr(int i) {
    std::stringstream ss;
    ss << "'key=" << i << "'";
    return ss.str();
}

void ServerMain(RdmaServer &server, volatile unsigned char *const buf) {
    const size_t out_offset = 0, in_offset = FLAGS_msg_slot_size * FLAGS_msg_slots;
    volatile unsigned char *const outbuf = buf;
    volatile unsigned char *const inbuf = buf + in_offset;

    int slot = 0;
    int processed = 0;

    while (processed < FLAGS_rounds) {
        // check message present
        size_t slot_offset = ComputeSlotOffset(slot);
        if (!CheckMsgPresent(inbuf + slot_offset)) {
            continue;
        }

        // get message
        auto kvp = ParseKvpFromMsg(inbuf + slot_offset);
        std::string expected_value = GetValueStr(processed);
        CHECK_EQ(processed, kvp.key);
        CHECK_STREQ(expected_value.c_str(), kvp.value);

        // send response
        SerializeKvpAsMsg(outbuf + slot_offset, kvp);
        auto wr = server.Write(false, 0, out_offset + slot_offset, in_offset + slot_offset,
                               FLAGS_msg_slot_size);
        server.WaitForCompletion(0, true, wr);

        // clear this slot's incoming buffer for reuse in future
        std::fill(inbuf + slot_offset, inbuf + slot_offset + FLAGS_msg_slot_size, 0);

        processed++;
        slot = (slot + 1) % FLAGS_msg_slots;

        LOG_EVERY_N(INFO, FLAGS_rounds / 10) << "Processed count: " << processed;
    }
}

int main(int argc, char **argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    google::InitGoogleLogging(argv[0]);

    size_t buf_size = FLAGS_msg_slot_size * FLAGS_msg_slots * 2;

    volatile unsigned char *buffer = new volatile unsigned char[buf_size]();
    RdmaServer server(FLAGS_endpoint.c_str(), nullptr, 0, buffer, buf_size, 128, 128, IBV_QPT_RC);

    server.Listen();
    LOG(INFO) << "Server connected to remote.";

    ServerMain(std::ref(server), buffer);

    return 0;
}