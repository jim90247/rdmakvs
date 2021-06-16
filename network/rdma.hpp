#pragma once

#include <bits/stdint-uintn.h>
#include <infiniband/verbs.h>
#include <network/rdma.pb.h>
#include <zmq.h>

#include <network/common.hpp>
#include <tuple>
#include <unordered_set>
#include <vector>

enum class SignalStrategy {
    kSignalNone = 0,
    kSignalLast,
    kSignalAll,
};

// base class for any rdma service. establish ibv context, queue pair, etc.
class RdmaEndpoint {
   public:
    RdmaEndpoint(char *ib_dev_name, uint8_t ib_dev_port, char *buffer, size_t buffer_size,
                 uint32_t max_send_count, uint32_t max_recv_count, ibv_qp_type qp_type,
                 uint64_t wr_offset, uint64_t wr_count);
    ~RdmaEndpoint();
    uint64_t Write(size_t remote_id, uint64_t local_offset, uint64_t remote_offset, uint32_t length,
                   unsigned int flags = IBV_SEND_SIGNALED, ibv_send_wr **bad_wr = nullptr);
    std::vector<uint64_t> WriteBatch(
        size_t remote_id, const std::vector<std::tuple<uint64_t, uint64_t, uint32_t>> &requests,
        SignalStrategy signal_strategy, unsigned int flags = 0, ibv_send_wr **bad_wr = nullptr);
    uint64_t Read(size_t remote_id, uint64_t local_offset, uint64_t remote_offset, uint32_t length,
                  unsigned int flags = IBV_SEND_SIGNALED, ibv_send_wr **bad_wr = nullptr);
    uint64_t Send(uint64_t offset, uint32_t length, unsigned int flags = IBV_SEND_SIGNALED,
                  ibv_send_wr **bad_wr = nullptr);
    uint64_t Recv(uint64_t offset, uint32_t length, ibv_recv_wr **bad_wr = nullptr);
    void CompareAndSwap(void *addr);
    void WaitForCompletion(std::unordered_set<uint64_t> &completed_wr, bool poll_until_found,
                           uint64_t target_wr_id);

   protected:
    uint8_t ib_dev_port_;
    struct ibv_port_attr ib_dev_port_info_;
    struct ibv_context *ctx_;
    struct ibv_pd *pd_;
    struct ibv_cq *cq_;
    struct ibv_qp *qp_;
    struct ibv_mr *mr_;

    char *buf_;        // Buffer associated with local memory region
    size_t buf_size_;  // Size of the buffer associated with local memory region

    const size_t kZmqMessageBufferSize = 1024;
    void *zmq_context_;
    void *zmq_socket_;
    RdmaPeerInfo local_info_;  // Local information to share with remote peers
    std::vector<RdmaPeerInfo> remote_info_;

    void PopulateLocalInfo();
    size_t ExchangePeerInfo(void *zmq_socket, bool send_first);

    struct ibv_qp *PrepareQueuePair(uint32_t max_send_count, uint32_t max_recv_count,
                                    ibv_qp_type qp_type);
    void ConnectQueuePair(ibv_qp *qp, RdmaPeerQueuePairInfo remote_qp_info);

   private:
    const uint64_t kWorkRequestIdOffset;
    const uint64_t kWorkRequestIdRegionSize;
    uint64_t next_wr_id_;
    int64_t num_wr_in_progress_;

    struct ibv_context *GetIbContextFromDevice(const char *device_name, const uint8_t port);
    uint64_t IncrementWorkRequestId();

    const size_t kMaxBatchSize = 32;
    inline void PopulateWriteWorkRequest(struct ibv_sge *sg, struct ibv_send_wr *wr,
                                         size_t remote_id, uint64_t local_offset,
                                         uint64_t remote_offset, uint32_t length,
                                         struct ibv_send_wr *next, unsigned int flags);
};

// wait for clients to connect, implement connect and disconnect
class RdmaServer : public Server, public RdmaEndpoint {
   public:
    RdmaServer(char *ib_dev_name, uint8_t ib_dev_port, char *buffer, size_t buffer_size,
               uint32_t max_send_count, uint32_t max_recv_count, ibv_qp_type qp_type,
               uint64_t wr_offset, uint64_t wr_count);
    ~RdmaServer();
    virtual void Listen(const char *endpoint);
};

// connect to an rdma server
class RdmaClient : public Client, public RdmaEndpoint {
   public:
    RdmaClient(char *ib_dev_name, uint8_t ib_dev_port, char *buffer, size_t buffer_size,
               uint32_t max_send_count, uint32_t max_recv_count, ibv_qp_type qp_type,
               uint64_t wr_offset, uint64_t wr_count);
    ~RdmaClient();
    virtual void Connect(const char *endpoint);
    virtual void Disconnect();
};