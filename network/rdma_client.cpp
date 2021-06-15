#include <glog/logging.h>

#include <network/rdma.hpp>

RdmaClient::RdmaClient(char *ib_dev_name, uint8_t ib_dev_port, char *buffer, size_t buffer_size,
                       uint32_t max_send_count, uint32_t max_recv_count, ibv_qp_type qp_type,
                       uint64_t wr_offset, uint64_t wr_count)
    : RdmaEndpoint(ib_dev_name, ib_dev_port, buffer, buffer_size, max_send_count, max_recv_count,
                   qp_type, wr_offset, wr_count) {
    zmq_socket_ = zmq_socket(zmq_context_, ZMQ_REQ);
    CHECK(zmq_socket_ != nullptr) << "Failed to open zeromq REQ socket: "
                                  << zmq_strerror(zmq_errno());
}

RdmaClient::~RdmaClient() {
    zmq_close(zmq_socket_);
    zmq_ctx_destroy(zmq_context_);
}

void RdmaClient::Connect(const char *endpoint) {
    CHECK_EQ(zmq_connect(zmq_socket_, endpoint), 0)
        << "Failed to connect " << endpoint << ": " << zmq_strerror(zmq_errno());

    size_t remote_id = ExchangePeerInfo(zmq_socket_, true);

    ConnectQueuePair(qp_, remote_info_[remote_id].queue_pair());
    LOG(INFO) << "Queue pair is ready to send";
}

void RdmaClient::Disconnect() {}