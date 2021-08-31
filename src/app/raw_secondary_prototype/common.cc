#include "app/raw_secondary_prototype/common.h"

#include <algorithm>

#include "glog/logging.h"

DEFINE_string(endpoint, "tcp://192.168.223.1:7889", "Zmq endpoint");
DEFINE_uint64(msg_slot_size, 128, "Size of each message slot");
DEFINE_uint64(msg_slots, 4096, "Message slots for in/out message each");
DEFINE_int32(rounds, 100, "Rounds");

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
