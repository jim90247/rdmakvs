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
    virtual uint64_t Send(uint64_t offset, uint32_t length, unsigned int flags) = 0;
    virtual uint64_t Recv(uint64_t offset, uint32_t length) = 0;
    virtual void CompareAndSwap(void *addr) = 0;
    virtual void WaitForCompletion(bool poll_until_found, uint64_t target_wr_id) = 0;
    virtual void ClearCompletedRecords() = 0;
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
    RdmaEndpoint(char *ib_dev_name, uint8_t ib_dev_port, char *buffer, size_t buffer_size,
                 uint32_t max_send_count, uint32_t max_recv_count,
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
    uint64_t Send(uint64_t offset, uint32_t length,
                  unsigned int flags = IBV_SEND_SIGNALED) override;
    uint64_t Recv(uint64_t offset, uint32_t length) override;
    void CompareAndSwap(void *addr) override;
    void WaitForCompletion(bool poll_until_found, uint64_t target_wr_id) override;
    void ClearCompletedRecords() override;

   protected:
    uint8_t ib_dev_port_;
    struct ibv_port_attr ib_dev_port_info_;
    struct ibv_context *ctx_;
    // Shared protected domain between all connections
    struct ibv_pd *pd_;
    // Shared completion queue between all connections (queue pairs)
    struct ibv_cq *cq_;
    // Shared memory region between all connections. Unless with special protocol to avoid race
    // conditions, the whole buffer should be divided into subregions, and each region should belong
    // to only one connection.
    struct ibv_mr *mr_;

    // Buffer associated with local memory region
    char *buf_;
    // Size of the buffer associated with local memory region
    size_t buf_size_;

    const static size_t kZmqMessageBufferSize = 1024;
    void *zmq_context_;
    void *zmq_socket_;

    struct RdmaConnection {
        // Each queue pair corresponds to a remote connection
        struct ibv_qp *qp;
        // Local information to share with remote peers
        RdmaPeerInfo local_info;
        // Remote RDMA informations
        RdmaPeerInfo remote_info;
    };
    std::vector<RdmaConnection> connections_;

    void PopulateLocalInfo();
    size_t ExchangePeerInfo(void *zmq_socket, bool send_first);

    struct ibv_qp *PrepareQueuePair(uint32_t max_send_count, uint32_t max_recv_count,
                                    ibv_qp_type qp_type);
    void ConnectQueuePair(ibv_qp *qp, RdmaPeerQueuePairInfo remote_qp_info);

   private:
    uint64_t next_wr_id_;
    int64_t num_signaled_wr_in_progress_;
    std::unordered_set<uint64_t> completed_wr_;

    struct ibv_context *GetIbContextFromDevice(const char *device_name, const uint8_t port);

    const static size_t kMaxBatchSize = 32;
    struct ibv_send_wr send_wr_template_[kMaxBatchSize];
    struct ibv_sge sg_template_[kMaxBatchSize];
    inline void PopulateWriteWorkRequest(struct ibv_sge *sg, struct ibv_send_wr *wr,
                                         size_t remote_id, uint64_t local_offset,
                                         uint64_t remote_offset, uint32_t length,
                                         struct ibv_send_wr *next, unsigned int flags);
    inline void FillOutWriteWorkRequest(
        struct ibv_sge *sg, struct ibv_send_wr *wr, size_t remote_id,
        const std::vector<std::tuple<uint64_t, uint64_t, uint32_t>> &requests, unsigned int flags);
    int PostSendWithAutoReclaim(struct ibv_qp *qp, struct ibv_send_wr *wr);
};

// wait for clients to connect, implement connect and disconnect
class RdmaServer : public RdmaEndpoint {
   public:
    RdmaServer(char *ib_dev_name, uint8_t ib_dev_port, char *buffer, size_t buffer_size,
               uint32_t max_send_count, uint32_t max_recv_count, ibv_qp_type qp_type);
    ~RdmaServer();
    void Listen(const char *endpoint);
};

// connect to an rdma server
class RdmaClient : public RdmaEndpoint {
   public:
    RdmaClient(char *ib_dev_name, uint8_t ib_dev_port, char *buffer, size_t buffer_size,
               uint32_t max_send_count, uint32_t max_recv_count, ibv_qp_type qp_type);
    ~RdmaClient();
    void Connect(const char *endpoint);
    void Disconnect();
};