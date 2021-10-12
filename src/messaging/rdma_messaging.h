#include <cstddef>
#include <cstdint>
#include <queue>
#include <vector>

#include "network/rdma.h"

using std::int64_t;
using std::size_t;

namespace rdmamsg {

struct InboundMessage {
    volatile void* data;
    int size;
};

/**
 * @brief A wrapper that uses a subset of RdmaEndpoint buffer to provide messaging between a pair of
 * RdmaConnections.
 */
class [[deprecated]] RdmaWriteMessagingEndpoint {
   public:
    const static int64_t kNoOutboundMessage = -1;

    RdmaWriteMessagingEndpoint(RdmaEndpoint& endpoint, volatile unsigned char* rdma_buffer,
                               int peer_id, size_t local_buffer_offset, size_t remote_buffer_offset,
                               size_t messaging_buffer_size);
    /**
     * @brief Allocate a region of memory for writing message content
     *
     * @param message_size the size of the memory region
     * @return the memory region, or `nullptr` if the request cannot be satisfied
     * @note Do not free the returned buffer! The returned memory region is not necessarily a new
     * memory buffer allocated via malloc() or similar function.
     */
    virtual volatile void* AllocateOutboundMessageBuffer(int message_size);

    /**
     * @brief Release inbound message buffer used by previous `CheckInboundMessage` calls
     *
     * @note Old data won't be available anymore
     */
    virtual void ReleaseInboundMessageBuffer();

    /**
     * @brief Send all the allocated messages
     *
     * @return an unique identifier that can be used to track if the flush completes, or negative
     * number for errors
     */
    virtual int64_t FlushOutboundMessage();

    /**
     * @brief Block until the flush complete
     *
     * @param flush_id the message id to wait for
     */
    virtual void BlockUntilComplete(int64_t flush_id);

    /**
     * @brief Check for inbound message
     *
     * @return the message
     */
    virtual InboundMessage CheckInboundMessage();

    struct OutboundMessage {
        uint64_t id;
        size_t offset_start;
        size_t offset_end;
    };

   private:
    RdmaEndpoint& endpoint_;
    const int peer_id_;
    std::queue<OutboundMessage> outbound_pending_message_;
    volatile unsigned char* rdma_buffer_;
    const size_t local_offset_;
    const size_t remote_offset_;
    const size_t messaging_buffer_size_;

    // When polling size get this number, it means that remaining buffer at the end of the message
    // are empty, and should retry polling again at the start of the buffer
    const static int kWrapMarker = -1;

    const size_t outbound_buffer_start_;
    const size_t outbound_buffer_end_;
    size_t outbound_buffer_head_;
    size_t outbound_buffer_tail_;

    const size_t inbound_buffer_start_;
    const size_t inbound_buffer_end_;
    size_t inbound_buffer_head_;

    const size_t remote_buffer_start_;
    size_t remote_buffer_head_;

    const static size_t kMessagingMetadataSize = sizeof(size_t) + sizeof(size_t);
    // Located at the beginning of the rdma buffer. Remote peer can read this value via RDMA READ to
    // know the available memory region of this messaging endpoint.
    const static size_t kInboundBufferTailPtrOffset = 0;
    volatile size_t* const inbound_buffer_tail_ptr_;
    // Located at the beginning of the rdma buffer, right after inbound buffer tail. We can read the
    // inbound buffer tail of remote peer to this memory using RDMA READ.
    const static size_t kRemoteBufferTailPtrOffset = sizeof(size_t);
    volatile size_t* const remote_buffer_tail_ptr_;

    const static int kMaxRefreshCount = 1 << 20;

    size_t GetFullMessageSize(size_t message_body_size) const;
    size_t GetDirtyMemorySize(size_t head, size_t tail, size_t lower_bound,
                              size_t upper_bound) const;
};
}  // namespace rdmamsg
