#include "app/raw_secondary_prototype/common.h"

#include <algorithm>
#include <cstdio>

#include "glog/logging.h"

// TODO: parse server zmq endpoint from config file
DEFINE_string(kvs_server, "tcp://192.168.223.1:7889", "Key value store server Zmq endpoint");
DEFINE_uint64(msg_slot_size, 128, "Size of each message slot");
DEFINE_uint64(msg_slots, 4096, "Message slots for in/out message each");
DEFINE_int32(put_rounds, 100, "PUT Rounds");
// TODO: parse server threads from config file
DEFINE_int32(server_threads, 1, "Server threads");
// TODO: compute total client threads from config file
DEFINE_int32(total_client_threads, 1, "Total client threads");
DEFINE_string(client_node_config, "nodeconf.json", "Path to client node config file");

DEFINE_uint64(kvs_entries, 65536, "Number of key-value pairs to store. Must be a power of 2.");

static bool CheckPowerOfTwo(const char* flagname, gflags::uint64 value) {
    if ((value & (value - 1)) == 0) {
        return true;
    }
    printf("%s is not a power of 2!", flagname);
    return false;
}

DEFINE_validator(kvs_entries, &CheckPowerOfTwo);

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
    // TODO: implement locking to prevent write collision
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
