#include "gmock/gmock.h"
#include "src/network/rdma.hpp"

class MockRdmaEndpoint : public IRdmaEndpoint {
   public:
    MOCK_METHOD(uint64_t, Write,
                (bool initialized, size_t remote_id, uint64_t local_offset, uint64_t remote_offset,
                 uint32_t length, unsigned int flags),
                (override));
    MOCK_METHOD(void, InitializeFastWrite, (size_t remote_id, size_t batch_size), (override));
    MOCK_METHOD((std::vector<uint64_t>), WriteBatch,
                (bool initialized, size_t remote_id,
                 (const std::vector<std::tuple<uint64_t, uint64_t, uint32_t>> &)requests,
                 SignalStrategy signal_strategy, unsigned int flags),
                (override));
    MOCK_METHOD(uint64_t, Read,
                (size_t remote_id, uint64_t local_offset, uint64_t remote_offset, uint32_t length,
                 unsigned int flags),
                (override));
    MOCK_METHOD(uint64_t, Send, (uint64_t offset, uint32_t length, unsigned int flags), (override));
    MOCK_METHOD(uint64_t, Recv, (uint64_t offset, uint32_t length), (override));
    MOCK_METHOD(void, CompareAndSwap, (void *addr), (override));
    MOCK_METHOD(void, WaitForCompletion, (bool poll_until_found, uint64_t target_wr_id),
                (override));
    MOCK_METHOD(void, ClearCompletedRecords, (), (override));
};
