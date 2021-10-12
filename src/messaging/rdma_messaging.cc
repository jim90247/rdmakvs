#include "messaging/rdma_messaging.h"

#include <glog/logging.h>
#include <infiniband/verbs.h>
#include <unistd.h>

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <tuple>

namespace rdmamsg {

RdmaWriteMessagingEndpoint::RdmaWriteMessagingEndpoint(RdmaEndpoint& endpoint,
                                                       volatile unsigned char* rdma_buffer,
                                                       int peer_id, size_t local_buffer_offset,
                                                       size_t remote_buffer_offset,
                                                       size_t messaging_buffer_size)
    : endpoint_(endpoint),
      peer_id_(peer_id),
      rdma_buffer_(rdma_buffer),
      local_offset_(local_buffer_offset),
      remote_offset_(remote_buffer_offset),
      messaging_buffer_size_(messaging_buffer_size),
      outbound_buffer_start_(local_offset_ + kMessagingMetadataSize),
      outbound_buffer_end_(local_offset_ + kMessagingMetadataSize +
                           (messaging_buffer_size_ - kMessagingMetadataSize) / 2),
      inbound_buffer_start_(local_offset_ + kMessagingMetadataSize +
                            (messaging_buffer_size_ - kMessagingMetadataSize) / 2),
      inbound_buffer_end_(local_offset_ + messaging_buffer_size_),
      remote_buffer_start_(remote_offset_ + kMessagingMetadataSize +
                           (messaging_buffer_size_ - kMessagingMetadataSize) / 2),
      // Place local inbound buffer tail at the beginning of the rdma buffer
      inbound_buffer_tail_ptr_(reinterpret_cast<volatile size_t*>(rdma_buffer_ + local_offset_ +
                                                                  kInboundBufferTailPtrOffset)),
      // Place remote buffer tail at the beginning of the rdma buffer
      remote_buffer_tail_ptr_(reinterpret_cast<volatile size_t*>(rdma_buffer_ + local_offset_ +
                                                                 kRemoteBufferTailPtrOffset)) {
    CHECK_NOTNULL(rdma_buffer_);
    // Buffer must be large enough to store local and remote inbound buffer tail
    CHECK_GT(messaging_buffer_size_, kMessagingMetadataSize);
    // requires inbound and outbound buffer are same size to simplify offset calculation
    CHECK_EQ(outbound_buffer_end_ - outbound_buffer_start_,
             inbound_buffer_end_ - inbound_buffer_start_);

    outbound_buffer_head_ = outbound_buffer_tail_ = outbound_buffer_start_;
    inbound_buffer_head_ = inbound_buffer_start_;
    remote_buffer_head_ = remote_buffer_start_;

    *inbound_buffer_tail_ptr_ = inbound_buffer_start_;
    *remote_buffer_tail_ptr_ = inbound_buffer_start_;

    // We use at most 2 RDMA_WRITE to flush message (1 for usual case, 2 when wrap around).
    // Batched send (send multiple messages at the same time) is achieved by manually triggering
    // FlushOutboundMessage.
    endpoint_.InitializeFastWrite(peer_id_, 2);
}

volatile void* RdmaWriteMessagingEndpoint::AllocateOutboundMessageBuffer(int message_size) {
    volatile void* ptr = nullptr;
    const size_t full_message_size = GetFullMessageSize(message_size);

    // Allocate a contiguous memory region
    if (outbound_buffer_head_ >= outbound_buffer_tail_) {
        // [    t    h ]
        // [ooooxxxxxoo]
        if (outbound_buffer_end_ - outbound_buffer_head_ >= full_message_size) {
            // Fill message size
            *reinterpret_cast<volatile int*>(rdma_buffer_ + outbound_buffer_head_) = message_size;

            ptr = reinterpret_cast<volatile void*>(rdma_buffer_ + outbound_buffer_head_ +
                                                   sizeof(int));
            outbound_buffer_head_ += full_message_size;

            // Set trailing byte to 0xff for polling
            rdma_buffer_[outbound_buffer_head_ - 1] = 0xff;
        } else if (outbound_buffer_tail_ - outbound_buffer_start_ >= full_message_size) {
            // Mark "Wrap" if the remaining buffer size is enough to store an 1-byte message. If the
            // remaining buffer size is not enough, poller should automatically wrap around to the
            // start of the buffer.
            if (outbound_buffer_end_ - outbound_buffer_head_ >= GetFullMessageSize(1)) {
                *reinterpret_cast<volatile int*>(rdma_buffer_ + outbound_buffer_head_) =
                    kWrapMarker;
            }

            // Fill message size
            *reinterpret_cast<volatile int*>(rdma_buffer_ + outbound_buffer_start_) = message_size;

            // Wrap around
            ptr = reinterpret_cast<volatile void*>(rdma_buffer_ + outbound_buffer_start_ +
                                                   sizeof(int));
            outbound_buffer_head_ = outbound_buffer_start_ + full_message_size;

            // Set trailing byte to 0xff for polling
            rdma_buffer_[outbound_buffer_head_ - 1] = 0xff;
        }
    } else {
        // [ h     t   ]
        // [xooooooxxxx]
        if (outbound_buffer_tail_ - outbound_buffer_head_ >= full_message_size) {
            // Fill message size
            *reinterpret_cast<volatile int*>(rdma_buffer_ + outbound_buffer_head_) = message_size;

            ptr = reinterpret_cast<volatile void*>(rdma_buffer_ + outbound_buffer_head_ +
                                                   sizeof(int));
            outbound_buffer_head_ += full_message_size;

            // Set trailing byte to 0xff for polling
            rdma_buffer_[outbound_buffer_head_ - 1] = 0xff;
        }
    }

    DCHECK_LE(outbound_buffer_head_, outbound_buffer_end_);
    DCHECK_GE(outbound_buffer_head_, outbound_buffer_start_);
    return ptr;
}

void RdmaWriteMessagingEndpoint::ReleaseInboundMessageBuffer() {
    if (*inbound_buffer_tail_ptr_ <= inbound_buffer_head_) {
        // [  t    h ]
        // [ooxxxxxoo]
        std::fill(rdma_buffer_ + *inbound_buffer_tail_ptr_, rdma_buffer_ + inbound_buffer_head_,
                  static_cast<unsigned char>(0));
    } else {
        // [  h    t ]
        // [xxoooooxx]
        std::fill(rdma_buffer_ + *inbound_buffer_tail_ptr_, rdma_buffer_ + inbound_buffer_end_,
                  static_cast<unsigned char>(0));
        std::fill(rdma_buffer_ + inbound_buffer_start_, rdma_buffer_ + inbound_buffer_head_,
                  static_cast<unsigned char>(0));
    }

    // TODO: what is the most appropriate memory order here?
    __atomic_store(inbound_buffer_tail_ptr_, &inbound_buffer_head_, __ATOMIC_RELAXED);
}

int64_t RdmaWriteMessagingEndpoint::FlushOutboundMessage() {
    if (outbound_buffer_tail_ == outbound_buffer_head_) {
        return kNoOutboundMessage;
    }

    size_t total_message_size = GetDirtyMemorySize(outbound_buffer_head_, outbound_buffer_tail_,
                                                   outbound_buffer_start_, outbound_buffer_end_);
    size_t remote_avail_size =
        ((messaging_buffer_size_ - kMessagingMetadataSize) / 2) -
        GetDirtyMemorySize(
            remote_buffer_head_, *remote_buffer_tail_ptr_, remote_buffer_start_,
            remote_buffer_start_ + (messaging_buffer_size_ - kMessagingMetadataSize) / 2);

    int refresh_count = 0;
    // Also refresh when size equals, since we cannot differentiate empty buffer (100% available)
    // and 100% full buffer (head == tail means "empty" in our settings)
    while (remote_avail_size <= total_message_size && refresh_count < kMaxRefreshCount) {
        // Update local copy of remote buffer tail (`*remote_buffer_tail_ptr_`)
        uint64_t tracker = endpoint_.Read(peer_id_, remote_offset_ + kRemoteBufferTailPtrOffset,
                                          local_offset_ + kInboundBufferTailPtrOffset,
                                          sizeof(size_t), IBV_SEND_SIGNALED);
        endpoint_.WaitForCompletion(peer_id_, true, tracker);
        remote_avail_size =
            ((messaging_buffer_size_ - kMessagingMetadataSize) / 2) -
            GetDirtyMemorySize(
                remote_buffer_head_, *remote_buffer_tail_ptr_, remote_buffer_start_,
                remote_buffer_start_ + (messaging_buffer_size_ - kMessagingMetadataSize) / 2);
        refresh_count++;
    }

    LOG_IF(FATAL, refresh_count == kMaxRefreshCount && remote_avail_size <= total_message_size)
        << "Remote peer did not refresh its buffer tail! Aborting...";

    int64_t track_id = kNoOutboundMessage;

    if (outbound_buffer_head_ > outbound_buffer_tail_) {
        track_id =
            endpoint_.Write(true, peer_id_, outbound_buffer_tail_, remote_buffer_head_,
                            outbound_buffer_head_ - outbound_buffer_tail_, IBV_SEND_SIGNALED);
    } else {
        std::vector<std::tuple<uint64_t, uint64_t, uint32_t>> requests = {
            std::make_tuple(outbound_buffer_tail_, remote_buffer_head_,
                            outbound_buffer_end_ - outbound_buffer_tail_),
            std::make_tuple(outbound_buffer_start_, remote_buffer_start_,
                            outbound_buffer_head_ - outbound_buffer_start_)};

        track_id =
            endpoint_.WriteBatch(true, peer_id_, requests, SignalStrategy::kSignalLast, 0).back();
    }

    outbound_buffer_tail_ = outbound_buffer_head_;
    remote_buffer_head_ = (outbound_buffer_head_ - outbound_buffer_start_) + remote_buffer_start_;
    return track_id;
}

void RdmaWriteMessagingEndpoint::BlockUntilComplete(int64_t flush_id) {
    endpoint_.WaitForCompletion(peer_id_, true, flush_id);
}

InboundMessage RdmaWriteMessagingEndpoint::CheckInboundMessage() {
    // Determine the offset to check
    if (inbound_buffer_end_ - inbound_buffer_head_ < GetFullMessageSize(1)) {
        // Automatically wrap around since remaining buffer at the end is too small to store an
        // message
        inbound_buffer_head_ = inbound_buffer_start_;
    }
    int message_body_size = *reinterpret_cast<volatile int*>(rdma_buffer_ + inbound_buffer_head_);
    if (message_body_size == kWrapMarker) {
        // Wrap around
        inbound_buffer_head_ = inbound_buffer_start_;
        message_body_size = *reinterpret_cast<volatile int*>(rdma_buffer_ + inbound_buffer_head_);
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
    return {.data = reinterpret_cast<volatile void*>(rdma_buffer_ + data_offset),
            .size = message_body_size};
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
