#include "app/secondary_prototype/common.h"

#include <glog/logging.h>

#include <algorithm>
#include <array>
#include <cstring>

KeyValuePair CreateKvPair(int key, std::string str_value) {
    KeyValuePair kv_pair;
    kv_pair.key = key;
    std::fill(kv_pair.value, kv_pair.value + kValueSize, 0);
    str_value.copy(kv_pair.value, kValueSize);
    return kv_pair;
}

size_t ComputeKeyOffset(__int128 key) { return (uint64_t)(kKeyMask & key) * kEntrySize; }

template <typename T>
static T *AdvancePointer(T *ptr, size_t delta_bytes) {
    return (unsigned char *)ptr + delta_bytes;
}

KeyValuePair ParseKeyValuePair(volatile void *ptr) {
    KeyValuePair kv_pair;
    volatile char *cptr = static_cast<volatile char *>(ptr);
    std::copy(cptr, cptr + kEntrySize, reinterpret_cast<char *>(&kv_pair));
    return kv_pair;
}

void SerializeKeyValuePair(volatile void *ptr, const KeyValuePair &kv_pair) {
    const char *cptr = reinterpret_cast<const char *>(&kv_pair);
    std::copy(cptr, cptr + kEntrySize, static_cast<volatile char *>(ptr));
}

void ExchangeInfo(RdmaEndpoint *endpoint, int remote_id, size_t send_offset, size_t send_size,
                  size_t recv_offset, size_t recv_size, bool send_first) {
    uint64_t wr_first = endpoint->Recv(remote_id, recv_offset, recv_size);
    uint64_t wr_second = endpoint->Send(remote_id, send_offset, send_size);
    if (send_first) {
        std::swap(wr_first, wr_second);
    }
    endpoint->WaitForCompletion(remote_id, true, wr_first);
    endpoint->WaitForCompletion(remote_id, true, wr_second);
}
