#include <cstddef>
#include <cstdint>
#include <string>

#include "network/rdma.h"

using std::size_t;

static const int kKeySize = 16;    // fixed key size: 16 bytes
static const int kValueSize = 32;  // fixed value size: 32 bytes
static const size_t kEntrySize = kKeySize + sizeof(kValueSize) + kValueSize;
static const unsigned int kKeyMaskBits = 8;
static const unsigned int kKeyMask = (1 << kKeyMaskBits) - 1;

static const size_t kClientPerThreadReadBufferSize = 1 << 20;
static const size_t kPerThreadMsgBufferSize = 1 << 20;
static const size_t kClientPerThreadBufferSize =
    kClientPerThreadReadBufferSize + kPerThreadMsgBufferSize;

static const size_t kKvsBufferSize = (1 << kKeyMaskBits) * kEntrySize;

size_t ComputeKeyOffset(__int128 key);

struct KeyValuePair {
    __int128 key;
    int value_size = kValueSize;
    char value[kValueSize];
};

KeyValuePair CreateKvPair(int key, std::string value);
KeyValuePair ParseKeyValuePair(volatile void *ptr);
void SerializeKeyValuePair(volatile void *ptr, const KeyValuePair &kv_pair);
void ExchangeInfo(RdmaEndpoint *endpoint, int remote_id, size_t send_offset, size_t send_size, size_t recv_offset,
                  size_t recv_size, bool send_first);
