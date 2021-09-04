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

inline std::string GetValueStr(int i) {
    std::stringstream ss;
    ss << "'key=" << i << "'";
    return ss.str();
}

void ServerMain(RdmaServer &server, volatile unsigned char *const buf, const IdType id) {
    const size_t out_offset = ComputeMsgBufOffset(id, id, false),
                 in_offset = ComputeMsgBufOffset(id, id, true);
    volatile unsigned char *const outbuf = buf + out_offset;
    volatile unsigned char *const inbuf = buf + in_offset;

    // const IdType r_id = ExchangeId(server, buf, id, out_offset, in_offset, false);
    const size_t r_in_offset = ComputeMsgBufOffset(id, id, true);

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

        // clear this slot's incoming buffer for reuse in future
        std::fill(inbuf + slot_offset, inbuf + slot_offset + FLAGS_msg_slot_size, 0);

        // send response
        SerializeKvpAsMsg(outbuf + slot_offset, kvp);
        auto wr = server.Write(false, id, out_offset + slot_offset, r_in_offset + slot_offset,
                               FLAGS_msg_slot_size);
        server.WaitForCompletion(id, true, wr);

        processed++;
        slot = (slot + 1) % FLAGS_msg_slots;

        if (processed % (FLAGS_rounds / 10) == 0) {
            RAW_LOG(INFO, "Id: %d, (r_id: %d) Processed: %d", id, id, processed);
        }
    }
}

int main(int argc, char **argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    google::InitGoogleLogging(argv[0]);

    size_t buf_size =
        FLAGS_server_threads * FLAGS_client_threads * FLAGS_msg_slot_size * FLAGS_msg_slots * 2;

    volatile unsigned char *buffer = new volatile unsigned char[buf_size]();
    RdmaServer server(FLAGS_endpoint.c_str(), nullptr, 0, buffer, buf_size, 128, 128, IBV_QPT_RC);

    for (int i = 0; i < FLAGS_server_threads; i++) {
        server.Listen();
        DLOG(INFO) << "Client " << i << " connected";
    }
    std::vector<std::thread> threads;
    for (int i = 0; i < FLAGS_server_threads; i++) {
        std::thread t(ServerMain, std::ref(server), buffer, i);
        threads.emplace_back(std::move(t));
    }

    for (std::thread &t : threads) {
        t.join();
    }

    return 0;
}