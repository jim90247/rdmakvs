#include <gflags/gflags.h>
#include <gflags/gflags_declare.h>

#include <cstdint>
#include <sstream>
#include <string>

#include "network/rdma.h"

DECLARE_string(kvs_server);
DECLARE_uint64(msg_slot_size);
DECLARE_uint64(msg_slots);
DECLARE_int32(put_rounds);
DECLARE_int32(server_threads);
DECLARE_int32(total_client_threads);
DECLARE_string(client_node_config);

DECLARE_uint64(kvs_entries);

using std::int64_t;
using std::size_t;
using std::uint64_t;
using KeyType = long;
using ValueSizeType = int;
using MsgSizeType = int;
using IdType = int;

constexpr ValueSizeType kMaxValueSize = 32;

struct KeyValuePair {
    KeyType key;
    ValueSizeType size;
    char value[kMaxValueSize];
    unsigned char lock;

    static KeyValuePair Create(KeyType key, ValueSizeType size, const char* value);
    static KeyValuePair ParseFrom(volatile unsigned char* buf);
    static volatile KeyValuePair* ParseFromRaw(volatile unsigned char* const buf);
    void SerializeTo(volatile unsigned char* const buf) const;
    void SerializeTo(volatile unsigned char* const buf) volatile;
    void AtomicSerializeTo(volatile unsigned char* const buf) const;
    void AtomicSerializeTo(volatile unsigned char* const buf) volatile;
};

KeyValuePair ParseKvpFromMsg(volatile unsigned char* const buf);

volatile KeyValuePair* ParseKvpFromMsgRaw(volatile unsigned char* const buf);

void SerializeKvpAsMsg(volatile unsigned char* const buf, const KeyValuePair& kvp);

void SerializeKvpAsMsg(volatile unsigned char* const buf, volatile KeyValuePair* const kvp_ptr);

inline size_t ComputeSlotOffset(int sid) { return sid * FLAGS_msg_slot_size; }

inline bool CheckMsgPresent(volatile unsigned char* const buf) {
    return buf[FLAGS_msg_slot_size - 1] != 0;
}

inline size_t ComputeServerMsgBufOffset(IdType s_id, IdType c_id, bool in) {
    int x = in ? 1 : 0;
    return ((FLAGS_total_client_threads * s_id + c_id) * 2 + x) * FLAGS_msg_slots *
           FLAGS_msg_slot_size;
}

inline size_t ComputeClientMsgBufOffset(IdType s_id, IdType c_id, int client_threads, bool in) {
    int x = in ? 1 : 0;
    return ((client_threads * s_id + c_id) * 2 + x) * FLAGS_msg_slots * FLAGS_msg_slot_size;
}

inline size_t ComputeKvBufOffset(KeyType key) {
    return sizeof(KeyValuePair) * (key & (FLAGS_kvs_entries - 1));
}

#ifdef NDEBUG
// use fixed value string for better performance
inline std::string GetValueStr(int s, int c, int r) { return "fixed"; }
#else
inline std::string GetValueStr(int s, int c, int r) {
    std::stringstream ss;
    ss << "s=" << s << ", c=" << c << ", r=" << r;
    return ss.str();
}
#endif