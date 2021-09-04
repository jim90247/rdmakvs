#include <glog/logging.h>

#include "network/rdma.h"

RdmaServer::RdmaServer(const char *endpoint, char *ib_dev_name, uint8_t ib_dev_port,
                       volatile unsigned char *buffer, size_t buffer_size, uint32_t max_send_count,
                       uint32_t max_recv_count, ibv_qp_type qp_type)
    : RdmaEndpoint(ib_dev_name, ib_dev_port, buffer, buffer_size, max_send_count, max_recv_count,
                   qp_type) {
    zmq_socket_ = zmq_socket(zmq_context_, ZMQ_REP);
    CHECK(zmq_socket_ != nullptr) << "Failed to open zeromq REP socket: "
                                  << zmq_strerror(zmq_errno());
    CHECK_EQ(zmq_bind(zmq_socket_, endpoint), 0)
        << "Failed to bind " << endpoint << ": " << zmq_strerror(zmq_errno());
}

RdmaServer::~RdmaServer() {
    zmq_close(zmq_socket_);
}

void RdmaServer::Listen() {
    size_t remote_id = PrepareNewConnection();
    ExchangePeerInfo(remote_id, false);
    ConnectPeer(remote_id);

    LOG(INFO) << "Queue pair " << remote_id << " is ready to send";
}