#include "src/messaging/rdma_messaging.hpp"

#include <glog/logging.h>
#include <infiniband/verbs.h>

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <tuple>

namespace rdmamsg {

IRdmaMessagingEndpoint::~IRdmaMessagingEndpoint() {}

RdmaWriteMessagingEndpoint::RdmaWriteMessagingEndpoint(IRdmaEndpoint* endpoint,
                                                       unsigned char* rdma_buffer,
                                                       size_t rdma_buffer_size)
    : rdma_buffer_(rdma_buffer),
      outbound_buffer_start_(0),
      outbound_buffer_end_(rdma_buffer_size / 2),
      inbound_buffer_start_(rdma_buffer_size / 2),
      inbound_buffer_end_(rdma_buffer_size) {
    CHECK_NOTNULL(endpoint);
    CHECK_NOTNULL(rdma_buffer_);
    CHECK_GT(rdma_buffer_size, 0);
    // requires inbound and outbound buffer are same size to simplify offset calculation
    CHECK_EQ(outbound_buffer_end_ - outbound_buffer_start_,
             inbound_buffer_end_ - inbound_buffer_start_);

    endpoint_ = endpoint;
    local_outbound_buffer_head_ = local_outbound_buffer_tail_ = outbound_buffer_start_;
    remote_outbound_buffer_head_ = remote_outbound_buffer_tail_ = inbound_buffer_start_;
    inbound_buffer_head_ = inbound_buffer_tail_ = inbound_buffer_start_;

    // We use at most 2 RDMA_WRITE to flush message (1 for usual case, 2 when wrap around).
    // Batched send (send multiple messages at the same time) is achieved by manually triggering
    // FlushOutboundMessage.
    endpoint_->InitializeFastWrite(0, 2);
}

void* RdmaWriteMessagingEndpoint::allocateOutboundMessageBuffer(int message_size) {
    void* ptr = nullptr;
    const size_t full_message_size = GetFullMessageSize(message_size);

    // Allocate a contiguous memory region
    if (local_outbound_buffer_head_ >= local_outbound_buffer_tail_) {
        // [    t    h ]
        // [ooooxxxxxoo]
        if (outbound_buffer_end_ - local_outbound_buffer_head_ >= full_message_size) {
            // Fill message size
            *reinterpret_cast<int*>(rdma_buffer_ + local_outbound_buffer_head_) = message_size;

            ptr = reinterpret_cast<void*>(rdma_buffer_ + local_outbound_buffer_head_ + sizeof(int));
            local_outbound_buffer_head_ += full_message_size;

            // Set trailing byte to 0xff for polling
            rdma_buffer_[local_outbound_buffer_head_ - 1] = 0xff;
        } else if (local_outbound_buffer_tail_ - outbound_buffer_start_ >= full_message_size) {
            // Mark "Wrap" if the remaining buffer size is enough to store an 1-byte message. If the
            // remaining buffer size is not enough, poller should automatically wrap around to the
            // start of the buffer.
            if (outbound_buffer_end_ - local_outbound_buffer_head_ >= GetFullMessageSize(1)) {
                *reinterpret_cast<int*>(rdma_buffer_ + local_outbound_buffer_head_) = kWrapMarker;
            }

            // Fill message size
            *reinterpret_cast<int*>(rdma_buffer_ + outbound_buffer_start_) = message_size;

            // Wrap around
            ptr = reinterpret_cast<void*>(rdma_buffer_ + outbound_buffer_start_ + sizeof(int));
            local_outbound_buffer_head_ = outbound_buffer_start_ + full_message_size;

            // Set trailing byte to 0xff for polling
            rdma_buffer_[local_outbound_buffer_head_ - 1] = 0xff;
        }
    } else {
        // [ h     t   ]
        // [xooooooxxxx]
        if (local_outbound_buffer_tail_ - local_outbound_buffer_head_ >= full_message_size) {
            // Fill message size
            *reinterpret_cast<int*>(rdma_buffer_ + local_outbound_buffer_head_) = message_size;

            ptr = reinterpret_cast<void*>(rdma_buffer_ + local_outbound_buffer_head_ + sizeof(int));
            local_outbound_buffer_head_ += full_message_size;

            // Set trailing byte to 0xff for polling
            rdma_buffer_[local_outbound_buffer_head_ - 1] = 0xff;
        }
    }

    DCHECK_LE(local_outbound_buffer_head_, outbound_buffer_end_);
    DCHECK_GE(local_outbound_buffer_head_, outbound_buffer_start_);
    return ptr;
}

int64_t RdmaWriteMessagingEndpoint::FlushOutboundMessage() {
    if (local_outbound_buffer_tail_ == local_outbound_buffer_head_) {
        return kNoOutboundMessage;
    }

    size_t total_message_size =
        GetDirtyMemorySize(local_outbound_buffer_head_, local_outbound_buffer_tail_,
                           outbound_buffer_start_, outbound_buffer_end_);
    size_t remote_avail_size =
        (inbound_buffer_end_ - inbound_buffer_start_) -
        GetDirtyMemorySize(remote_outbound_buffer_head_, remote_outbound_buffer_tail_,
                           inbound_buffer_start_, inbound_buffer_end_);
    if (total_message_size > remote_avail_size) {
        // [TODO] Reclaim available memory at remote side
        return kRemoteMemoryNotEnough;
    }

    int64_t track_id = kNoOutboundMessage;

    if (local_outbound_buffer_head_ > local_outbound_buffer_tail_) {
        track_id = endpoint_->Write(
            true, 0, local_outbound_buffer_tail_, remote_outbound_buffer_head_,
            local_outbound_buffer_head_ - local_outbound_buffer_tail_, IBV_SEND_SIGNALED);
    } else {
        std::vector<std::tuple<uint64_t, uint64_t, uint32_t>> requests = {
            std::make_tuple(local_outbound_buffer_tail_, remote_outbound_buffer_head_,
                            outbound_buffer_end_ - local_outbound_buffer_tail_),
            // remote outbound buffer start should be inbound buffer start
            std::make_tuple(outbound_buffer_start_, inbound_buffer_start_,
                            local_outbound_buffer_head_ - outbound_buffer_start_)};

        track_id = endpoint_->WriteBatch(true, 0, requests, SignalStrategy::kSignalLast, 0).back();
    }

    local_outbound_buffer_tail_ = local_outbound_buffer_head_;
    remote_outbound_buffer_head_ =
        (local_outbound_buffer_head_ - outbound_buffer_start_) + inbound_buffer_start_;
    return track_id;
}

void RdmaWriteMessagingEndpoint::BlockUntilComplete(int64_t flush_id) {
    endpoint_->WaitForCompletion(true, flush_id);
}

InboundMessage RdmaWriteMessagingEndpoint::CheckInboundMessage() {
    // Determine the offset to check
    if (inbound_buffer_end_ - inbound_buffer_head_ < GetFullMessageSize(1)) {
        // Automatically wrap around since remaining buffer at the end is too small to store an
        // message
        inbound_buffer_head_ = inbound_buffer_start_;
    }
    int message_body_size = *reinterpret_cast<int*>(rdma_buffer_ + inbound_buffer_head_);
    if (message_body_size == kWrapMarker) {
        // Wrap around
        inbound_buffer_head_ = inbound_buffer_start_;
        message_body_size = *reinterpret_cast<int*>(rdma_buffer_ + inbound_buffer_head_);
    }

    // Check the offset
    if (message_body_size == 0) {
        // No message
        return {.data = nullptr, .size = 0};
    }
    size_t full_message_size = GetFullMessageSize(message_body_size);

    // Poll for full message
    while (rdma_buffer_[inbound_buffer_head_ + full_message_size - 1] != 0xff) {
        // busy waiting
    }

    size_t data_offset = inbound_buffer_head_ + sizeof(int);
    inbound_buffer_head_ += full_message_size;
    return {.data = reinterpret_cast<void*>(rdma_buffer_ + data_offset), .size = message_body_size};
}

size_t RdmaWriteMessagingEndpoint::GetDirtyMemorySize(size_t head, size_t tail, size_t lower_bound,
                                                      size_t upper_bound) const {
    if (head >= tail) {
        return head - tail;
    } else {
        return (head - lower_bound) + (upper_bound - tail);
    }
}

size_t RdmaWriteMessagingEndpoint::GetFullMessageSize(size_t message_body_size) const {
    // size header + message body + trailing 1 byte to identify WRITE complete
    return sizeof(int) + message_body_size + 1;
}

}  // namespace rdmamsg
