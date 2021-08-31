#include <gflags/gflags.h>
#include <cstdint>
#include "network/rdma.h"

DECLARE_string(endpoint);

using std::int64_t;
using KeyType = long;
using ValueSizeType = int;

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
