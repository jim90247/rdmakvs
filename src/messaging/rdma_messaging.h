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

class IRdmaMessagingEndpoint {
   public:
    const static int64_t kNoOutboundMessage = -1;
    const static int kAnyPeer = -1;

    /**
     * @brief Allocate a region of memory for writing message content for a specific peer
     *
     * @param peer_id the id of the peer
     * @param message_size the size of the memory region
     * @return the memory region, or `nullptr` if the request cannot be satisfied
     * @note Do not free the returned buffer! The returned memory region is not necessarily a new
     * memory buffer allocated via malloc() or similar function.
     */
    virtual volatile void* AllocateOutboundMessageBuffer(int peer_id, int message_size) = 0;

    /**
     * @brief Send all the allocated messages for a specific peer
     *
     * @param peer_id the id of the peer
     * @return an unique identifier that can be used to track if the flush completes, or
     * negative number for errors
     */
    virtual int64_t FlushOutboundMessage(int peer_id) = 0;

    /**
     * @brief Block until the flush complete
     *
     * @param flush_id the message id to wait for
     */
    virtual void BlockUntilComplete(int64_t flush_id) = 0;

    /**
     * @brief Check for inbound message
     *
     * @param peer_id the id of the peer to check for, or `kAnyPeer` to check any peer
     * @return the message
     */
    virtual InboundMessage CheckInboundMessage(int peer_id = kAnyPeer) = 0;

    /**
     * @brief Release inbound message buffer of a specific peer used by previous
     * `CheckInboundMessage` calls
     *
     * @param peer_id the id of the peer
     * @note Old data won't be available anymore
     */
    virtual void ReleaseInboundMessageBuffer(int peer_id) = 0;

    ~IRdmaMessagingEndpoint();
};

class RdmaWriteMessagingEndpoint : public IRdmaMessagingEndpoint {
   public:
    RdmaWriteMessagingEndpoint(IRdmaEndpoint* endpoint, volatile unsigned char* rdma_buffer,
                               size_t rdma_buffer_size);
    virtual volatile void* AllocateOutboundMessageBuffer(int peer_id, int message_size) override;
    virtual void ReleaseInboundMessageBuffer(int peer_id) override;
    virtual int64_t FlushOutboundMessage(int peer_id) override;
    virtual void BlockUntilComplete(int64_t flush_id) override;
    virtual InboundMessage CheckInboundMessage(int peer_id = kAnyPeer) override;

    struct OutboundMessage {
        uint64_t id;
        size_t offset_start;
        size_t offset_end;
    };

   private:
    IRdmaEndpoint* endpoint_;
    std::queue<OutboundMessage> outbound_pending_message_;
    volatile unsigned char* rdma_buffer_;

    // When polling size get this number, it means that remaining buffer at the end of the message
    // are empty, and should retry polling again at the start of the buffer
    const static int kWrapMarker = -1;

    const size_t outbound_buffer_start_;
    const size_t outbound_buffer_end_;
    size_t outbound_buffer_head_;
    size_t outbound_buffer_tail_;
    size_t remote_buffer_head_;

    const size_t inbound_buffer_start_;
    const size_t inbound_buffer_end_;
    size_t inbound_buffer_head_;

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
