#include <gflags/gflags.h>
#include <gflags/gflags_declare.h>

#include <cstdint>

#include "network/rdma.h"

DECLARE_string(endpoint);
DECLARE_uint64(msg_slot_size);
DECLARE_uint64(msg_slots);
DECLARE_int32(rounds);
DECLARE_int32(threads);

using std::int64_t;
using std::size_t;
using std::uint64_t;
using KeyType = long;
using ValueSizeType = int;
using MsgSizeType = int;
using IdType = int;

constexpr ValueSizeType kMaxValueSize = 64;

struct KeyValuePair {
    KeyType key;
    ValueSizeType size;
    char value[kMaxValueSize];
    int signal;

    static KeyValuePair Create(KeyType key, ValueSizeType size, const char* value);
    static KeyValuePair ParseFrom(volatile unsigned char* buf);
    void SerializeTo(volatile unsigned char* const buf) const;
};

KeyValuePair ParseKvpFromMsg(volatile unsigned char* const buf);

void SerializeKvpAsMsg(volatile unsigned char* const buf, const KeyValuePair& kvp);

inline size_t ComputeSlotOffset(int sid) { return sid * FLAGS_msg_slot_size; }

inline bool CheckMsgPresent(volatile unsigned char* const buf) {
    return buf[FLAGS_msg_slot_size - 1] != 0;
}

IdType ExchangeId(RdmaEndpoint& ep, volatile unsigned char* const buf, IdType id,
                  size_t send_offset, size_t recv_offset, bool send_first);

size_t ComputeMsgBufOffset(IdType id, bool in);
