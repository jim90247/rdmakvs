#include "app/raw_secondary_prototype/common.h"

#include <algorithm>

#include "glog/logging.h"

DEFINE_string(endpoint, "tcp://192.168.223.1:7889", "Zmq endpoint");

KeyValuePair KeyValuePair::ParseFrom(volatile unsigned char* buf) {
    KeyValuePair kvp;
    std::copy(buf, buf + sizeof(KeyValuePair), reinterpret_cast<char*>(&kvp));
    return kvp;
}
void KeyValuePair::SerializeTo(volatile unsigned char* const buf) const {
    auto ptr = reinterpret_cast<const unsigned char*>(this);
    std::copy(ptr, ptr + sizeof(KeyValuePair), buf);
}
