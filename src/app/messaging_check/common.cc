#include "app/messaging_check/common.h"

#include <random>

std::string RandomString(int len) {
    static const char alphanum[] = "0123456789abcdefghijklmnopqrstuvwxyz";
    thread_local static std::mt19937_64 gen(42);
    thread_local static std::uniform_int_distribution<long> distrib(0, sizeof(alphanum) - 1);

    std::string s;
    s.reserve(len);
    for (int i = 0; i < len; i++) {
        s += alphanum[distrib(gen)];
    }
    return s;
}