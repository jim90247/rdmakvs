#include "app/raw_secondary_prototype/common.h"

#include <algorithm>

#include "glog/logging.h"

DEFINE_string(endpoint, "tcp://192.168.223.1:7889", "Zmq endpoint");
DEFINE_uint64(msg_slot_size, 128, "Size of each message slot");
DEFINE_uint64(msg_slots, 4096, "Message slots for in/out message each");
DEFINE_int32(rounds, 100, "Rounds");
DEFINE_int32(server_threads, 1, "Server threads");
DEFINE_int32(client_threads, 1, "Client threads");

KeyValuePair KeyValuePair::ParseFrom(volatile unsigned char* buf) {
    KeyValuePair kvp;
    std::copy(buf, buf + sizeof(KeyValuePair), reinterpret_cast<char*>(&kvp));
    return kvp;
}

KeyValuePair KeyValuePair::Create(KeyType key, ValueSizeType size, const char* value) {
    CHECK_NOTNULL(value);
    KeyValuePair kvp{.key = key, .size = size, .signal = 1};
    std::copy(value, value + size, kvp.value);
    return kvp;
}

void KeyValuePair::SerializeTo(volatile unsigned char* const buf) const {
    auto ptr = reinterpret_cast<const unsigned char*>(this);
    std::copy(ptr, ptr + sizeof(KeyValuePair), buf);
}

KeyValuePair ParseKvpFromMsg(volatile unsigned char* const buf) {
    return KeyValuePair::ParseFrom(buf + sizeof(MsgSizeType));
}

void SerializeKvpAsMsg(volatile unsigned char* const buf, const KeyValuePair& kvp) {
    *reinterpret_cast<volatile MsgSizeType*>(buf) = static_cast<MsgSizeType>(sizeof(KeyValuePair));
    kvp.SerializeTo(buf + sizeof(MsgSizeType));
    // trailing byte
    buf[FLAGS_msg_slot_size - 1] = 0xff;
}

IdType ExchangeId(RdmaEndpoint& ep, volatile unsigned char* const buf, IdType id,
                  size_t send_offset, size_t recv_offset, bool send_first) {
    *reinterpret_cast<volatile IdType*>(buf + send_offset) = id;
    uint64_t s_wr, r_wr;
    if (send_first) {
        s_wr = ep.Send(id, send_offset, sizeof(IdType));
        r_wr = ep.Recv(id, recv_offset, sizeof(IdType));
    } else {
        r_wr = ep.Recv(id, recv_offset, sizeof(IdType));
        s_wr = ep.Send(id, send_offset, sizeof(IdType));
    }
    ep.WaitForCompletion(id, true, s_wr);
    ep.WaitForCompletion(id, true, r_wr);
    IdType remote_id = *reinterpret_cast<volatile IdType*>(buf + recv_offset);

    // clear used buffer
    *reinterpret_cast<volatile IdType*>(buf + send_offset) = 0;
    *reinterpret_cast<volatile IdType*>(buf + recv_offset) = 0;

    return remote_id;
}

size_t ComputeMsgBufOffset(IdType s_id, IdType c_id, bool in) {
    int x = in ? 1 : 0;
    return ((FLAGS_server_threads * s_id + c_id) * 2 + x) * FLAGS_msg_slots * FLAGS_msg_slot_size;
}
