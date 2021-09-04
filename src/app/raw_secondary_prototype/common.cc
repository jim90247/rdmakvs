#include "app/raw_secondary_prototype/common.h"

#include <algorithm>

#include "glog/logging.h"

DEFINE_string(kvs_server, "tcp://192.168.223.1:7889", "Key value store server Zmq endpoint");
DEFINE_string(kvs_client, "tcp://192.168.223.2:7889", "Key value store client Zmq endpoint");
DEFINE_uint64(msg_slot_size, 128, "Size of each message slot");
DEFINE_uint64(msg_slots, 4096, "Message slots for in/out message each");
DEFINE_int32(rounds, 100, "Rounds");
DEFINE_int32(server_threads, 1, "Server threads");
DEFINE_int32(client_threads, 1, "Client threads");
DEFINE_int32(client_nodes, 1, "Number of nodes that client threads comes from");

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

size_t ComputeMsgBufOffset(IdType s_id, IdType c_id, bool in) {
    int x = in ? 1 : 0;
    return ((FLAGS_client_threads * s_id + c_id) * 2 + x) * FLAGS_msg_slots * FLAGS_msg_slot_size;
}
