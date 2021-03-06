#include "app/raw_secondary_prototype/common.h"

#include <glog/raw_logging.h>

#include <algorithm>
#include <cstdio>

#include "glog/logging.h"

// TODO: parse server zmq endpoint from config file
DEFINE_string(kvs_server, "tcp://192.168.223.1:7889", "Key value store server Zmq endpoint");
DEFINE_uint64(req_msg_slot_size, 64, "Size of each request message slot");
DEFINE_uint64(res_msg_slot_size,
#ifdef NDEBUG
              9,  // message size (4) + status code (4) + trailing byte (1)
#else
              64,
#endif
              "Size of each response message slot");
DEFINE_uint64(msg_slots, 4096, "Message slots for in/out message each");
DEFINE_int64(total_ops, 1000, "Total number of operations to perform including GETs and PUTs");
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
    printf("%s is not a power of 2!\n", flagname);
    return false;
}

DEFINE_validator(kvs_entries, &CheckPowerOfTwo);

KeyValuePair KeyValuePair::ParseFrom(volatile unsigned char* const buf) {
    KeyValuePair kvp;
    std::copy(buf, buf + sizeof(KeyValuePair), reinterpret_cast<char*>(&kvp));
    return kvp;
}

volatile KeyValuePair* KeyValuePair::ParseFromRaw(volatile unsigned char* const buf) {
    return reinterpret_cast<volatile KeyValuePair*>(buf);
}

KeyValuePair KeyValuePair::Create(KeyType key, ValueSizeType size, const char* value) {
    DCHECK_NOTNULL(value);
    DCHECK_LE(size, kMaxValueSize);
    KeyValuePair kvp{.key = key, .size = size, .lock = 0};
    std::copy(value, value + size, kvp.value);
    return kvp;
}

void KeyValuePair::SerializeTo(volatile unsigned char* const buf) const {
    auto ptr = reinterpret_cast<const unsigned char*>(this);
    std::copy(ptr, ptr + sizeof(KeyValuePair), buf);
}

void KeyValuePair::SerializeTo(volatile unsigned char* const buf) volatile {
    auto ptr = reinterpret_cast<volatile unsigned char*>(this);
    std::copy(ptr, ptr + sizeof(KeyValuePair), buf);
}

// TODO: verify the effectiveness of this implementation
void KeyValuePair::AtomicSerializeTo(volatile unsigned char* const buf) const {
    volatile unsigned char* l = buf + offsetof(KeyValuePair, lock);
    while (__sync_val_compare_and_swap(l, 0, 1) == 1)
        ;
    auto ptr = reinterpret_cast<const unsigned char*>(this);
    std::copy(ptr, ptr + offsetof(KeyValuePair, lock), buf);
    // unlock key, should not fail as this is the only thread operating on the key
    unsigned char prev = __sync_val_compare_and_swap(l, 1, 0);
    DCHECK_EQ(prev, 1);
}

// TODO: verify the effectiveness of this implementation
void KeyValuePair::AtomicSerializeTo(volatile unsigned char* const buf) volatile {
    volatile unsigned char* l = buf + offsetof(KeyValuePair, lock);
    while (__sync_val_compare_and_swap(l, 0, 1) == 1)
        ;
    auto ptr = reinterpret_cast<volatile unsigned char*>(this);
    std::copy(ptr, ptr + offsetof(KeyValuePair, lock), buf);
    // unlock key, should not fail as this is the only thread operating on the key
    unsigned char prev = __sync_val_compare_and_swap(l, 1, 0);
    DCHECK_EQ(prev, 1);
}

KeyValuePair ParseKvpFromMsg(volatile unsigned char* const buf) {
    return KeyValuePair::ParseFrom(buf + sizeof(MsgSizeType));
}

volatile KeyValuePair* ParseKvpFromMsgRaw(volatile unsigned char* const buf) {
    return KeyValuePair::ParseFromRaw(buf + sizeof(MsgSizeType));
}

void SerializeKvpAsMsg(volatile unsigned char* const buf, const KeyValuePair& kvp, BufferType bt) {
    if (bt == BufferType::REQ) {
        RAW_DCHECK(sizeof(MsgSizeType) + sizeof(KeyValuePair) + 1 <= FLAGS_req_msg_slot_size,
                   "Key value pair is too large to fit in request buffer.");
    } else {
        RAW_DCHECK(sizeof(MsgSizeType) + sizeof(KeyValuePair) + 1 <= FLAGS_res_msg_slot_size,
                   "Key value pair is too large to fit in response buffer.");
    }
    *reinterpret_cast<volatile MsgSizeType*>(buf) = static_cast<MsgSizeType>(sizeof(KeyValuePair));
    kvp.SerializeTo(buf + sizeof(MsgSizeType));
    // trailing byte
    if (bt == BufferType::REQ) {
        buf[FLAGS_req_msg_slot_size - 1] = 0xff;
    } else {
        buf[FLAGS_res_msg_slot_size - 1] = 0xff;
    }
}

void SerializeKvpAsMsg(volatile unsigned char* const buf, volatile KeyValuePair* const kvp_ptr,
                       BufferType bt) {
    if (bt == BufferType::REQ) {
        RAW_DCHECK(sizeof(MsgSizeType) + sizeof(KeyValuePair) + 1 <= FLAGS_req_msg_slot_size,
                   "Key value pair is too large to fit in request buffer.");
    } else {
        RAW_DCHECK(sizeof(MsgSizeType) + sizeof(KeyValuePair) + 1 <= FLAGS_res_msg_slot_size,
                   "Key value pair is too large to fit in response buffer.");
    }
    *reinterpret_cast<volatile MsgSizeType*>(buf) = static_cast<MsgSizeType>(sizeof(KeyValuePair));
    kvp_ptr->SerializeTo(buf + sizeof(MsgSizeType));
    // trailing byte
    if (bt == BufferType::REQ) {
        buf[FLAGS_req_msg_slot_size - 1] = 0xff;
    } else {
        buf[FLAGS_res_msg_slot_size - 1] = 0xff;
    }
}