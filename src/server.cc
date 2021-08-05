#include <bits/stdint-uintn.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <infiniband/verbs.h>

#include <chrono>
#include <cstdio>
#include <tuple>
#include <unordered_set>

#include "network/rdma.h"

int main(int argc, char **argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    google::InitGoogleLogging(argv[0]);

    const size_t buffer_size = 1 << 23;
    const size_t message_size = 2;
    char *buffer = new char[buffer_size]();
    sprintf(buffer + 100, "this is server");

    RdmaServer *endpoint = new RdmaServer(nullptr, 0, buffer, buffer_size, 128, 128, IBV_QPT_RC);
    endpoint->Listen("tcp://192.168.223.1:7889");

    uint64_t wr_id, wr_id2;
    const int kBatchSize = 4;
    std::vector<std::tuple<uint64_t, uint64_t, uint32_t>> batch(kBatchSize);
    size_t offset;
    uint64_t local_offsets[kBatchSize], remote_offsets[kBatchSize];
    uint32_t lengths[kBatchSize];
    /*
        LOG(INFO) << "Basic testing";
        wr_id = endpoint->Recv(0, 100);
        LOG(INFO) << "Recv posted";
        endpoint->WaitForCompletion(completed_wr, true, wr_id);

        LOG(INFO) << "Message from client: " << buffer;

        wr_id = endpoint->Write(0, 100, 100, 15);
        wr_id2 = endpoint->Read(0, 50, 300, 6);
        endpoint->WaitForCompletion(completed_wr, true, wr_id);
        endpoint->WaitForCompletion(completed_wr, true, wr_id2);

        LOG(INFO) << "Secret: " << buffer + 50;

        LOG(INFO) << "Test simple write";
        wr_id = endpoint->Write(0, 100, 100, 15);
        endpoint->WaitForCompletion(completed_wr, true, wr_id);
        LOG(INFO) << "Simple write completed";

        LOG(INFO) << "Test tuple batch write";
        offset = 0;
        for (int i = 0; i < kBatchSize; i++, offset += message_size)
            batch[i] = std::make_tuple(offset, offset, message_size);
        auto wr_ids = endpoint->WriteBatch(0, batch, SignalStrategy::kSignalLast,  IBV_SEND_INLINE);
        endpoint->WaitForCompletion(completed_wr, true, wr_ids.back());
        LOG(INFO) << "Tuple batch write completed";

        LOG(INFO) << "Test pointer batch write";
        for (int i = 0; i < kBatchSize; i++) {
            local_offsets[i] = message_size * i;
            lengths[i] = message_size;
        }
        wr_ids = endpoint->WriteBatch(0, kBatchSize, local_offsets, local_offsets, lengths,
                                      IBV_SEND_SIGNALED | IBV_SEND_INLINE);
        endpoint->WaitForCompletion(completed_wr, true, wr_ids.back());
        LOG(INFO) << "Test pointer batch write completed";

        // Warm-up
        for (size_t offset = 0, i = 0; offset < buffer_size; offset += message_size, i++) {
            if (i % 64 == 0) {
                if (i > 0) endpoint->WaitForCompletion(completed_wr, true, wr_id);
                wr_id = endpoint->Write(0, offset, offset, message_size,
                                        IBV_SEND_SIGNALED | IBV_SEND_INLINE);
            } else {
                endpoint->Write(0, offset, offset, message_size, IBV_SEND_INLINE);
            }
        }

        endpoint->WaitForCompletion(completed_wr, true, wr_id);
    */
    // Single write benchmark
    /*
        LOG(INFO) << "Simple write benchmark";

        auto start = std::chrono::steady_clock::now();

        endpoint->InitializeFastWrite(0, 1);
        for (size_t offset = 0, i = 0; offset < buffer_size; offset += message_size, i++) {
            if (i % 64 == 0) {
                if (i > 0) endpoint->WaitForCompletion(completed_wr, true, wr_id);
                wr_id = endpoint->Write(true, 0, offset, offset, message_size,
                                        IBV_SEND_SIGNALED | IBV_SEND_INLINE);
            } else {
                endpoint->Write(true, 0, offset, offset, message_size, IBV_SEND_INLINE);
            }
        }
        endpoint->WaitForCompletion(completed_wr, true, wr_id);

        auto end = std::chrono::steady_clock::now();

        LOG(INFO) << buffer_size / message_size << " requests of size " << message_size
                  << " bytes completed in "
                  << std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count()
                  << " ms";
        LOG(INFO) << "IOPS: "
                  << static_cast<double>(buffer_size / message_size) /
                         std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count()
       * 1000;
    */
    // Tuple batch write benchmark
    /*
        auto start_tuple = std::chrono::steady_clock::now();

        for (int b = 0; (b + 1) * kBatchSize * message_size <= buffer_size; b++) {
            size_t offset = kBatchSize * b * message_size;
            for (int i = 0; i < kBatchSize; i++, offset += message_size) {
                batch[i] = std::make_tuple(offset, offset, message_size);
            }
            if (b % (64 / kBatchSize) == 0) {
                if (b > 0) endpoint->WaitForCompletion(completed_wr, true, wr_id);
                wr_id =
                    endpoint->WriteBatch(false, 0, batch, SignalStrategy::kSignalLast,
       IBV_SEND_INLINE) .back(); } else { endpoint->WriteBatch(false, 0, batch,
       SignalStrategy::kSignalNone, IBV_SEND_INLINE);
            }
        }
        endpoint->WaitForCompletion(completed_wr, true, wr_id);

        auto end_tuple = std::chrono::steady_clock::now();

        LOG(INFO)
            << buffer_size / message_size << " requests of size " << message_size
            << " bytes completed in "
            << std::chrono::duration_cast<std::chrono::milliseconds>(end_tuple -
       start_tuple).count()
            << " ms (batch size " << kBatchSize << ")";
        LOG(INFO) << "IOPS: "
                  << static_cast<double>(buffer_size / message_size) /
                         std::chrono::duration_cast<std::chrono::milliseconds>(end_tuple -
       start_tuple) .count() * 1000;
    */
    // Fast tuple batch write benchmark

    auto start_tuple2 = std::chrono::steady_clock::now();

    endpoint->InitializeFastWrite(0, kBatchSize);
    for (int b = 0; (b + 1) * kBatchSize * message_size <= buffer_size; b++) {
        size_t offset = kBatchSize * b * message_size;
        for (int i = 0; i < kBatchSize; i++, offset += message_size) {
            batch[i] = std::make_tuple(offset, offset, message_size);
        }
        if (b % (64 / kBatchSize) == 0) {
            if (b > 0) endpoint->WaitForCompletion(true, wr_id);
            wr_id =
                endpoint->WriteBatch(true, 0, batch, SignalStrategy::kSignalLast, IBV_SEND_INLINE)
                    .back();
        } else {
            endpoint->WriteBatch(true, 0, batch, SignalStrategy::kSignalNone, IBV_SEND_INLINE);
        }
    }
    endpoint->WaitForCompletion(true, wr_id);

    auto end_tuple2 = std::chrono::steady_clock::now();

    LOG(INFO)
        << buffer_size / message_size << " requests of size " << message_size
        << " bytes completed in "
        << std::chrono::duration_cast<std::chrono::milliseconds>(end_tuple2 - start_tuple2).count()
        << " ms (batch size " << kBatchSize << ")";
    LOG(INFO) << "IOPS: "
              << static_cast<double>(buffer_size / message_size) /
                     std::chrono::duration_cast<std::chrono::milliseconds>(end_tuple2 -
                                                                           start_tuple2)
                         .count() *
                     1000;

    return 0;
}