#pragma once

#include <infiniband/verbs.h>
#include <zmq.h>

#include <network/common.hpp>

struct QpPublishInfo {
    uint32_t queue_pair_number;
    uint32_t packet_serial_number;
    uint16_t local_identifier;
};

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
    uint8_t ib_dev_port_;
    struct ibv_port_attr ib_dev_port_info_ = {};
    struct ibv_context *ctx_;
    struct ibv_pd *pd_;
    struct ibv_cq *cq_;
    struct ibv_qp *qp_;
    struct ibv_mr *mr_;
    uint8_t *buf_;
    size_t buf_size_;
    struct QpPublishInfo local_qp_info_;

    struct ibv_qp *PrepareQueuePair(uint32_t max_send_count, uint32_t max_recv_count,
                                    ibv_qp_type qp_type);
    void ConnectQueuePair(ibv_qp *qp, QpPublishInfo remote_qp_info);

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