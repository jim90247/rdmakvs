#include <cstddef>
#include <cstdint>
#include <queue>
#include <vector>

#include "src/network/rdma.hpp"

using std::size_t;

namespace rdmamsg {

struct InboundMessage {
    void* data;
    int size;
};

const int64_t kNoOutboundMessage = -1;
const int64_t kRemoteMemoryNotEnough = -2;

class IRdmaMessagingEndpoint {
   public:
    /**
     * @brief Allocate a region of memory for writing message content
     *
     * @param message_size the size of the memory region
     * @return void* the memory region, or `nullptr` if the request cannot be satisfied
     * @note Do not free the returned buffer! The returned memory region is not necessarily a new
     * memory buffer allocated via malloc() or similar function.
     */
    virtual void* allocateOutboundMessageBuffer(int message_size) = 0;

    /**
     * @brief Send all the allocated messages
     *
     * @return int64_t an unique identifier that can be used to track if the flush completes, or
     * negative number for errors
     */
    virtual int64_t FlushOutboundMessage() = 0;

    /**
     * @brief Block until the flush complete
     *
     * @param flush_id the message id to wait for
     */
    virtual void BlockUntilComplete(int64_t flush_id) = 0;

    /**
     * @brief Check for inbound message
     *
     * @return MessagingResponse the message
     */
    virtual InboundMessage CheckInboundMessage() = 0;

    ~IRdmaMessagingEndpoint();
};

class RdmaWriteMessagingEndpoint : IRdmaMessagingEndpoint {
   public:
    RdmaWriteMessagingEndpoint(IRdmaEndpoint* endpoint, unsigned char* rdma_buffer,
                               size_t rdma_buffer_size);
    virtual void* allocateOutboundMessageBuffer(int message_size) override;
    virtual int64_t FlushOutboundMessage() override;
    virtual void BlockUntilComplete(int64_t flush_id) override;
    virtual InboundMessage CheckInboundMessage() override;

    struct OutboundMessage {
        uint64_t id;
        size_t offset_start;
        size_t offset_end;
    };

   private:
    IRdmaEndpoint* endpoint_;
    std::queue<OutboundMessage> outbound_pending_message_;
    unsigned char* rdma_buffer_;

    // When polling size get this number, it means that remaining buffer at the end of the message
    // are empty, and should retry polling again at the start of the buffer
    const int kWrapMarker = -1;

    const size_t outbound_buffer_start_;
    const size_t outbound_buffer_end_;
    size_t local_outbound_buffer_head_;
    size_t local_outbound_buffer_tail_;
    size_t remote_outbound_buffer_head_;
    size_t remote_outbound_buffer_tail_;

    const size_t inbound_buffer_start_;
    const size_t inbound_buffer_end_;
    size_t inbound_buffer_head_;
    size_t inbound_buffer_tail_;

    size_t GetFullMessageSize(size_t message_body_size) const;
    size_t GetDirtyMemorySize(size_t head, size_t tail, size_t lower_bound,
                              size_t upper_bound) const;
};
}  // namespace rdmamsg
