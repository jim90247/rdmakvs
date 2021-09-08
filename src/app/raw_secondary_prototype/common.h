#include <gflags/gflags.h>
#include <gflags/gflags_declare.h>
#include <glog/raw_logging.h>

#include <cstdint>
#include <sstream>
#include <string>

#include "network/rdma.h"

DECLARE_string(kvs_server);
DECLARE_uint64(req_msg_slot_size);
DECLARE_uint64(res_msg_slot_size);
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

constexpr ValueSizeType kMaxValueSize =
#ifdef NDEBUG
    8
#else
    32
#endif
    ;

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

enum class BufferType { REQ, RES };

KeyValuePair ParseKvpFromMsg(volatile unsigned char* const buf);

volatile KeyValuePair* ParseKvpFromMsgRaw(volatile unsigned char* const buf);

template <typename T>
T ParseScalarFromMsg(volatile unsigned char* const buf) {
    RAW_DCHECK(*reinterpret_cast<volatile MsgSizeType*>(buf) == sizeof(T),
               "Message size does not match scalar type size.");
    return *reinterpret_cast<volatile T*>(buf + sizeof(MsgSizeType));
}

void SerializeKvpAsMsg(volatile unsigned char* const buf, const KeyValuePair& kvp, BufferType bt);

void SerializeKvpAsMsg(volatile unsigned char* const buf, volatile KeyValuePair* const kvp_ptr,
                       BufferType bt);

template <typename T>
void SerializeScalarAsMsg(volatile unsigned char* const buf, T scalar, BufferType bt) {
    if (bt == BufferType::REQ) {
        RAW_DCHECK(sizeof(MsgSizeType) + sizeof(T) + 1 <= FLAGS_req_msg_slot_size,
                   "Key value pair is too large to fit in request buffer.");
    } else {
        RAW_DCHECK(sizeof(MsgSizeType) + sizeof(T) + 1 <= FLAGS_res_msg_slot_size,
                   "Key value pair is too large to fit in response buffer.");
    }
    *reinterpret_cast<volatile MsgSizeType*>(buf) = static_cast<MsgSizeType>(sizeof(T));
    *reinterpret_cast<volatile T*>(buf + sizeof(MsgSizeType)) = scalar;
    // trailing byte
    if (bt == BufferType::REQ) {
        buf[FLAGS_req_msg_slot_size - 1] = 0xff;
    } else {
        buf[FLAGS_res_msg_slot_size - 1] = 0xff;
    }
}

inline size_t ComputeReqSlotOffset(int sid) { return sid * FLAGS_req_msg_slot_size; }

inline size_t ComputeResSlotOffset(int sid) { return sid * FLAGS_res_msg_slot_size; }

inline bool CheckReqMsgPresent(volatile unsigned char* const buf) {
    return buf[FLAGS_req_msg_slot_size - 1] != 0;
}

inline bool CheckResMsgPresent(volatile unsigned char* const buf) {
    return buf[FLAGS_res_msg_slot_size - 1] != 0;
}

inline size_t ComputeServerMsgBufOffset(IdType s_id, IdType c_id, BufferType bt) {
    int x = (bt == BufferType::REQ) ? 0 : 1;
    // request buffer goes before response buffer
    return ((FLAGS_total_client_threads * s_id + c_id) *
                (FLAGS_req_msg_slot_size + FLAGS_res_msg_slot_size) +
            x * FLAGS_req_msg_slot_size) *
           FLAGS_msg_slots;
}

inline size_t ComputeClientMsgBufOffset(IdType s_id, IdType c_id, int client_threads,
                                        BufferType bt) {
    int x = (bt == BufferType::REQ) ? 0 : 1;
    // request buffer goes before response buffer
    return ((client_threads * s_id + c_id) * (FLAGS_req_msg_slot_size + FLAGS_res_msg_slot_size) +
            x * FLAGS_req_msg_slot_size) *
           FLAGS_msg_slots;
}

inline size_t ComputeKvBufOffset(KeyType key) {
    return sizeof(KeyValuePair) * (key & (FLAGS_kvs_entries - 1));
}

inline std::string GetValueStr(int s, int c, int r) {
#ifdef NDEBUG
    // use fixed value string for better performance
    return "fixed";
#else
    std::stringstream ss;
    ss << "s=" << s << ", c=" << c << ", r=" << r;
    return ss.str();
#endif
}