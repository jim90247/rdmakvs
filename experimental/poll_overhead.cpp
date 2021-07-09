/**
 * @file poll_overhead.cpp
 * @author jim90247 (jim90247@gmail.com)
 * @brief An example app estimating the RDMA WRITE message buffer polling performance.
 * @version 0.1
 * @date 2021-07-09
 * 
 * @copyright Copyright (c) 2021
 * 
 */
#include <gflags/gflags.h>

#include <chrono>
#include <iostream>

DEFINE_uint64(msg_size, 32, "Message size in bytes");
DEFINE_uint64(msg_count, 1UL << 20, "Message count");
DEFINE_int32(msg_size_poll_cycle, 1, "# of polls for each message size");
DEFINE_int32(msg_data_poll_cycle, 10, "# of polls for each message data");

int main(int argc, char **argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    char *buffer = new char[(sizeof(int) + FLAGS_msg_size + 1) * FLAGS_msg_count]();

    auto start = std::chrono::steady_clock::now();

    for (int i = 0; i < FLAGS_msg_count; i++) {
        size_t offset = (sizeof(int) + FLAGS_msg_size + 1) * i;
        int p = 0;
        while (true) {
            if (++p == FLAGS_msg_size_poll_cycle) {
                *(int *)(buffer + offset) = FLAGS_msg_size;
            }
            if (*(int *)(buffer + offset) != 0) {
                break;
            }
        }
        p = 0;
        while (true) {
            if (++p == FLAGS_msg_data_poll_cycle) {
                buffer[offset + sizeof(int) + FLAGS_msg_size + 1] = 0xff;
            }
            if (buffer[offset + sizeof(int) + FLAGS_msg_size + 1] != 0) {
                break;
            }
        }
    }

    auto end = std::chrono::steady_clock::now();

    std::cout << FLAGS_msg_count << " requests of size " << FLAGS_msg_size << " bytes completed in "
              << std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count() << " ms"
              << std::endl;
    std::cout << "IOPS: "
              << static_cast<double>(FLAGS_msg_count) /
                     std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count() *
                     1000
              << std::endl;
    return 0;
}