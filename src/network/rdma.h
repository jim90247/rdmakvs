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

/**
 * @brief Defines interface for an RDMA endpoint.
 */
class IRdmaEndpoint {
   public:
    virtual uint64_t Write(bool initialized, size_t remote_id, uint64_t local_offset,
                           uint64_t remote_offset, uint32_t length, unsigned int flags) = 0;
    virtual void InitializeFastWrite(size_t remote_id, size_t batch_size) = 0;
    virtual std::vector<uint64_t> WriteBatch(
        bool initialized, size_t remote_id,
        const std::vector<std::tuple<uint64_t, uint64_t, uint32_t>> &requests,
        SignalStrategy signal_strategy, unsigned int flags) = 0;
    virtual uint64_t Read(size_t remote_id, uint64_t local_offset, uint64_t remote_offset,
                          uint32_t length, unsigned int flags) = 0;
    virtual uint64_t Send(size_t remote_id, uint64_t offset, uint32_t length,
                          unsigned int flags) = 0;
    virtual uint64_t Recv(size_t remote_id, uint64_t offset, uint32_t length) = 0;
    virtual void CompareAndSwap(void *addr) = 0;
    virtual void WaitForCompletion(size_t remote_id, bool poll_until_found,
                                   uint64_t target_wr_id) = 0;
    virtual void ClearCompletedRecords(size_t remote_id) = 0;
    virtual ~IRdmaEndpoint();
};

// base class for any rdma service. establish ibv context, queue pair, etc.
class RdmaEndpoint : public IRdmaEndpoint {
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
    ~RdmaEndpoint();
    uint64_t Write(bool initialized, size_t remote_id, uint64_t local_offset,
                   uint64_t remote_offset, uint32_t length,
                   unsigned int flags = IBV_SEND_SIGNALED) override;
    void InitializeFastWrite(size_t remote_id, size_t batch_size) override;
    std::vector<uint64_t> WriteBatch(
        bool initialized, size_t remote_id,
        const std::vector<std::tuple<uint64_t, uint64_t, uint32_t>> &requests,
        SignalStrategy signal_strategy, unsigned int flags = 0) override;
    uint64_t Read(size_t remote_id, uint64_t local_offset, uint64_t remote_offset, uint32_t length,
                  unsigned int flags = IBV_SEND_SIGNALED) override;
    uint64_t Send(size_t remote_id, uint64_t offset, uint32_t length,
                  unsigned int flags = IBV_SEND_SIGNALED) override;
    uint64_t Recv(size_t remote_id, uint64_t offset, uint32_t length) override;
    void CompareAndSwap(void *addr) override;
    void WaitForCompletion(size_t remote_id, bool poll_until_found, uint64_t target_wr_id) override;
    void ClearCompletedRecords(size_t remote_id) override;

   protected:
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
    void *zmq_socket_;

    const static size_t kMaxBatchSize = 32;
    // These templates are filled with metadata that can be reused across multiple requests.
    // Use these templates to reduce  request initialization overhead.
    struct RdmaRequestTemplate {
        struct ibv_send_wr send_wr_template[kMaxBatchSize];
        struct ibv_sge sge_template[kMaxBatchSize];
    };

    // Work request usage status of a RDMA connection
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
    // Gets the remote peer's RDMA connection information
    void ExchangePeerInfo(size_t peer_idx, bool send_first);
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

    inline void FillOutWriteWorkRequest(
        size_t remote_id, const std::vector<std::tuple<uint64_t, uint64_t, uint32_t>> &requests,
        unsigned int flags);
    // TODO: update all operations to use the request template and remove the work request pointer
    int PostSendWithAutoReclaim(size_t remote_id, struct ibv_send_wr *wr = nullptr);
};

// wait for clients to connect, implement connect and disconnect
class RdmaServer : public RdmaEndpoint {
   public:
    RdmaServer(char *ib_dev_name, uint8_t ib_dev_port, volatile unsigned char *buffer,
               size_t buffer_size, uint32_t max_send_count, uint32_t max_recv_count,
               ibv_qp_type qp_type);
    ~RdmaServer();
    void Listen(const char *endpoint);
};

// connect to an rdma server
class RdmaClient : public RdmaEndpoint {
   public:
    RdmaClient(char *ib_dev_name, uint8_t ib_dev_port, volatile unsigned char *buffer,
               size_t buffer_size, uint32_t max_send_count, uint32_t max_recv_count,
               ibv_qp_type qp_type);
    ~RdmaClient();
    void Connect(const char *endpoint);
    void Disconnect();
};