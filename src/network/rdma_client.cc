#include <glog/logging.h>

#include "network/rdma.h"

RdmaClient::RdmaClient(char *ib_dev_name, uint8_t ib_dev_port, volatile unsigned char *buffer,
                       size_t buffer_size, uint32_t max_send_count, uint32_t max_recv_count,
                       ibv_qp_type qp_type)
    : RdmaEndpoint(ib_dev_name, ib_dev_port, buffer, buffer_size, max_send_count, max_recv_count,
                   qp_type) {
    zmq_socket_ = zmq_socket(zmq_context_, ZMQ_REQ);
    CHECK(zmq_socket_ != nullptr) << "Failed to open zeromq REQ socket: "
                                  << zmq_strerror(zmq_errno());
}

RdmaClient::~RdmaClient() {
    zmq_close(zmq_socket_);
}

void RdmaClient::Connect(const char *endpoint) {
    CHECK_EQ(zmq_connect(zmq_socket_, endpoint), 0)
        << "Failed to connect " << endpoint << ": " << zmq_strerror(zmq_errno());

    size_t remote_id = PrepareNewConnection();
    ExchangePeerInfo(remote_id, true);
    ConnectPeer(remote_id);

    LOG(INFO) << "Queue pair is ready to send";
}

void RdmaClient::Disconnect() {}