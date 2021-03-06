#pragma once

#include <infiniband/verbs.h>
#include <zmq.h>

#include <cstdint>
#include <tuple>
#include <unordered_set>
#include <vector>

#include "proto/rdma.pb.h"

using std::int64_t;
using std::uint16_t;
using std::uint32_t;
using std::uint64_t;
using std::uint8_t;

enum class SignalStrategy {
    kSignalNone = 0,
    kSignalLast,
    kSignalAll,
};

const uint8_t kAnyIbPort = 0;

// base class for any rdma service. establish ibv context, queue pair, etc.
class RdmaEndpoint {
   public:
    /**
     * @brief Construct a new Rdma Endpoint object
     *
     * @param ib_dev_name Name of the specific IB device to use (e.g. "mlx5_0"). NULL for any
     * available one.
     * @param ib_dev_port Specific port number to use. 0 for any available port.
     * @param buffer Buffer where this endpoint has RDMA access to.
     * @param buffer_size Size of the buffer.
     * @param max_send_count Max number of outgoing SEND work requests (including IBV_WR_SEND,
     * IBV_WR_RDMA_WRITE, ...) at the same time.
     * @param max_recv_count Max number of outgoing RECV work requests at the same time.
     * @param qp_type Queue pair type. Default is IBV_QPT_RC.
     */
    RdmaEndpoint(char *ib_dev_name, uint8_t ib_dev_port, volatile unsigned char *buffer,
                 size_t buffer_size, uint32_t max_send_count, uint32_t max_recv_count,
                 ibv_qp_type qp_type = IBV_QPT_RC);
    virtual ~RdmaEndpoint();
    virtual uint64_t Write(bool initialized, size_t remote_id, uint64_t local_offset,
                           uint64_t remote_offset, uint32_t length,
                           unsigned int flags = IBV_SEND_SIGNALED);
    virtual void InitializeFastWrite(size_t remote_id, size_t batch_size);
    virtual std::vector<uint64_t> WriteBatch(
        bool initialized, size_t remote_id,
        const std::vector<std::tuple<uint64_t, uint64_t, uint32_t>> &requests,
        SignalStrategy signal_strategy, unsigned int flags = 0);
    virtual uint64_t Read(size_t remote_id, uint64_t local_offset, uint64_t remote_offset,
                          uint32_t length, unsigned int flags = IBV_SEND_SIGNALED);

    /**
     * @brief Sets the batch size of RDMA READ for all connections and initializes templates.
     *
     * @param batch the batch size
     */
    virtual void SetReadBatchSize(int batch);

    /**
     * @brief Performs RDMA read using pre-allocated buffer.
     *
     * @param remote_id the id of the connection to perform RDMA operation
     * @param local_offset the offset of the local buffer to receive data
     * @param remote_offset the offset of the remote buffer
     * @param length the length of data to read
     * @param flags the send flags, default is IBV_SEND_SIGNALED
     * @return the work request id
     */
    virtual uint64_t Read_v2(size_t remote_id, uint64_t local_offset, uint64_t remote_offset,
                             uint32_t length, unsigned int flags = IBV_SEND_SIGNALED);

    /**
     * @brief Sends out the pending RDMA Read requests. Useful for ensuring all requests are issued
     * when using batch size greater than 1.
     *
     * @param remote_id the id of the connection to perform RDMA operation
     * @return true if all the remaining requests are flushed successfully
     */
    virtual bool FlushPendingReads(size_t remote_id);

    virtual uint64_t Send(size_t remote_id, uint64_t offset, uint32_t length,
                          unsigned int flags = IBV_SEND_SIGNALED);
    virtual uint64_t Recv(size_t remote_id, uint64_t offset, uint32_t length);
    virtual void CompareAndSwap(void *addr);
    virtual void WaitForCompletion(size_t remote_id, bool poll_until_found, uint64_t target_wr_id);
    virtual void ClearCompletedRecords(size_t remote_id);

    virtual void BindToZmqEndpoint(const char *endpoint);
    virtual void Listen();
    virtual void Connect(const char *endpoint);

   protected:
    /**
     * @brief Construct a new Rdma Endpoint object
     *
     * @note dummy constructor just for testing
     */
    RdmaEndpoint();
    uint8_t ib_dev_port_;
    struct ibv_port_attr ib_dev_port_info_;
    struct ibv_context *ctx_;
    // Shared protected domain between all connections
    struct ibv_pd *pd_;
    // Shared memory region between all connections. Unless with special protocol to avoid race
    // conditions, the whole buffer should be divided into subregions, and each region should belong
    // to only one connection.
    struct ibv_mr *mr_;

    // Buffer associated with local memory region
    volatile unsigned char *buf_;
    // Size of the buffer associated with local memory region
    size_t buf_size_;

    const static size_t kZmqMessageBufferSize = 1024;
    void *zmq_context_;
    void *zmq_server_socket_;  // ZMQ socket to listen incoming connections
    void *zmq_client_socket_;  // ZMQ socket to add connection with remote

    const static size_t kMaxBatchSize = 32;
    // These templates are filled with metadata that can be reused across multiple requests.
    // Use these templates to reduce  request initialization overhead.
    struct RdmaRequestTemplate {
        // for RDMA_WRITE
        struct ibv_send_wr send_wr_template[kMaxBatchSize];
        struct ibv_sge sge_template[kMaxBatchSize];

        // for RDMA_READ
        int read_batch_idx = 0;
        struct ibv_send_wr read_wr_template[kMaxBatchSize];
        struct ibv_sge read_sge_template[kMaxBatchSize];
    };

    // must be set with SetReadBatchSize()
    int read_batch_size_ = -1;

    // Work request usage status of a RDMA connection
    // TODO: supports multiple threads per connection. Use case includes multiple threads accessing
    // the same remote using the same connection.
    struct RdmaWorkRequestStatus {
        // Next work request id
        uint64_t next_wr_id = 0;
        // Number of in-progress signaled work requests
        int64_t in_progress_signaled_wrs = 0;
        // Completed work request ids
        std::unordered_set<uint64_t> completed_wr_ids;
    };

    struct RdmaConnection {
        // Each queue pair corresponds to a remote connection
        struct ibv_qp *qp = nullptr;
        // Independent completion queue for each connection
        struct ibv_cq *cq = nullptr;
        // Work request status of the connection
        // NOTE: currently we assume this status will only be modified by one thread
        RdmaWorkRequestStatus wr_status;
        // Local information to share with remote peers
        RdmaPeerInfo local_info;
        // Remote RDMA informations
        RdmaPeerInfo remote_info;
        // Request templates for each connection
        RdmaRequestTemplate req_template;
    };
    std::vector<RdmaConnection> connections_;

    // Initializes the new queue pair and returns the index of `connections_` for new connection
    size_t PrepareNewConnection();
    // Modifies the queue pair state to ready-to-send
    void ConnectPeer(size_t peer_idx);

   private:
    uint32_t max_recv_count_;
    uint32_t max_send_count_;
    ibv_qp_type qp_type_;

    struct ibv_context *GetIbContextFromDevice(const char *device_name, const uint8_t port);
    void PopulateLocalInfo(size_t peer_idx);
    void PrepareCompletionQueue(size_t peer_idx);
    void PrepareQueuePair(size_t peer_idx);
    // Gets the remote peer's RDMA connection information
    void ExchangePeerInfo(void *zmq_socket, size_t peer_idx, bool send_first);

    inline void FillOutWriteWorkRequest(
        size_t remote_id, const std::vector<std::tuple<uint64_t, uint64_t, uint32_t>> &requests,
        unsigned int flags);
    // TODO: update all operations to use the request template and remove the work request pointer
    int PostSendWithAutoReclaim(size_t remote_id, struct ibv_send_wr *wr = nullptr);
};
