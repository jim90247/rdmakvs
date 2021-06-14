#pragma once

#include <infiniband/verbs.h>
#include <network/rdma.pb.h>
#include <zmq.h>

#include <network/common.hpp>
#include <vector>

// base class for any rdma service. establish ibv context, queue pair, etc.
class RdmaEndpoint {
   public:
    RdmaEndpoint(char *ib_dev_name, uint8_t ib_dev_port, size_t buffer_size,
                 uint32_t max_send_count, uint32_t max_recv_count, ibv_qp_type qp_type);
    ~RdmaEndpoint();
    void Write(void *addr);
    void Read(void *addr);
    void Send();
    void Recv();
    void CompareAndSwap(void *addr);
    void Poll();

   protected:
    const size_t kZmqMessageBufferSize = 1024;
    uint8_t ib_dev_port_;
    struct ibv_port_attr ib_dev_port_info_ = {};
    struct ibv_context *ctx_;
    struct ibv_pd *pd_;
    struct ibv_cq *cq_;
    struct ibv_qp *qp_;
    struct ibv_mr *mr_;
    uint8_t *buf_;             // Buffer associated with local memory region
    size_t buf_size_;          // Size of the buffer associated with local memory region
    RdmaPeerInfo local_info_;  // Local information to share with remote peers
    std::vector<RdmaPeerInfo> remote_info_;

    void PopulateLocalInfo();
    struct ibv_qp *PrepareQueuePair(uint32_t max_send_count, uint32_t max_recv_count,
                                    ibv_qp_type qp_type);
    void ConnectQueuePair(ibv_qp *qp, RdmaPeerQueuePairInfo remote_qp_info);

   private:
    struct ibv_context *GetIbContextFromDevice(const char *device_name, const uint8_t port);
};

// wait for clients to connect, implement connect and disconnect
class RdmaServer : public Server, public RdmaEndpoint {
   public:
    RdmaServer(char *ib_dev_name, uint8_t ib_dev_port, size_t buffer_size, uint32_t max_send_count,
               uint32_t max_recv_count, ibv_qp_type qp_type);
    ~RdmaServer();
    virtual void Listen(const char *endpoint);

   protected:
    void *zmq_context_;
    void *zmq_service_socket_;
};

// connect to an rdma server
class RdmaClient : public Client, public RdmaEndpoint {
   public:
    RdmaClient(char *ib_dev_name, uint8_t ib_dev_port, size_t buffer_size, uint32_t max_send_count,
               uint32_t max_recv_count, ibv_qp_type qp_type);
    ~RdmaClient();
    virtual void Connect(const char *endpoint);
    virtual void Disconnect();

   protected:
    void *zmq_context_;
    void *zmq_socket_;
};