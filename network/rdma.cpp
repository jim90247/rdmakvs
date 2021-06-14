#include <glog/logging.h>
#include <infiniband/verbs.h>
#include <network/rdma.pb.h>
#include <sys/param.h>
#include <zmq.h>

#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <network/rdma.hpp>
#include <stdexcept>
#include <string>

RdmaEndpoint::RdmaEndpoint(char *ib_dev_name, uint8_t ib_dev_port, size_t buffer_size,
                           uint32_t max_send_count, uint32_t max_recv_count, ibv_qp_type qp_type)
    : ib_dev_port_(ib_dev_port), buf_size_(buffer_size) {
    buf_size_ = roundup(buf_size_, sysconf(_SC_PAGESIZE));
    buf_ = new uint8_t[buf_size_]();  // initialize with 0
    CHECK(buf_ != nullptr) << "Failed to allocate buffer of size " << buf_size_ << " bytes";

    ctx_ = GetIbContextFromDevice(ib_dev_name, ib_dev_port_);
    CHECK(ctx_ != nullptr) << "Failed to get InfiniBand context";

    pd_ = ibv_alloc_pd(ctx_);
    CHECK(pd_ != nullptr) << "Failed to allocate protected domain";

    CHECK(ibv_query_port(ctx_, ib_dev_port_, &ib_dev_port_info_) == 0)
        << "Failed to query port " << ib_dev_port_ << " (" << ib_dev_name << ")";

    mr_ = ibv_reg_mr(pd_, buf_, buf_size_,
                     IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE);
    CHECK(mr_ != nullptr) << "Failed to register memory region";

    cq_ = ibv_create_cq(ctx_, max_send_count + max_recv_count, nullptr, nullptr, 0);
    CHECK(cq_ != nullptr) << "Failed to create completion queue";

    qp_ = PrepareQueuePair(max_send_count, max_recv_count, qp_type);
    LOG(INFO) << "Queue pair is ready for connection";

    PopulateLocalInfo();
}

RdmaEndpoint::~RdmaEndpoint() {
    if (qp_ != nullptr) {
        ibv_destroy_qp(qp_);
    }
    if (cq_ != nullptr) {
        ibv_destroy_cq(cq_);
    }
    if (mr_ != nullptr) {
        ibv_dereg_mr(mr_);
    }
    if (pd_ != nullptr) {
        ibv_dealloc_pd(pd_);
    }
    if (ctx_ != nullptr) {
        ibv_close_device(ctx_);
    }
    if (buf_ != nullptr) {
        delete[] buf_;
    }
}

struct ibv_context *RdmaEndpoint::GetIbContextFromDevice(const char *target_device_name,
                                                         const uint8_t target_port) {
    int num_devices = 0;
    ibv_device **device_list = nullptr;

    device_list = ibv_get_device_list(&num_devices);
    CHECK(device_list != nullptr && num_devices > 0) << "No RDMA device found";

    LOG_IF(INFO, target_device_name == nullptr)
        << "Device name is not specified, using first ACTIVE device.";
    LOG_IF(INFO, target_port == 0)
        << "Port number is not specified, using first ACTIVE port on device.";

    for (int i = 0; i < num_devices; i++) {
        const char *device_name = ibv_get_device_name(device_list[i]);
        if (target_device_name != nullptr && std::strcmp(device_name, target_device_name)) {
            // not target device
            continue;
        }

        ibv_context *ctx = ibv_open_device(device_list[i]);
        ibv_device_attr device_attr;
        if (ibv_query_device(ctx, &device_attr) != 0) {
            LOG(ERROR) << "Failed to query device " << device_name;
            continue;
        }

        uint8_t port_start = 1, port_end = device_attr.phys_port_cnt;
        if (target_port != 0) {
            port_start = port_end = target_port;
        }
        for (uint8_t port = port_start; port <= port_end; port++) {
            ibv_port_attr port_attr;
            if (ibv_query_port(ctx, port, &port_attr) != 0) {
                LOG(ERROR) << "Failed to query port " << port << " of device " << device_name;
                continue;
            }
            if (port_attr.state == IBV_PORT_ACTIVE) {
                // Found, update protected members
                ib_dev_port_ = port;

                LOG(INFO) << "Using " << device_name << ", port " << (unsigned int)ib_dev_port_;
                return ctx;
            }
        }

        ibv_close_device(ctx);
    }
    return nullptr;
}

void RdmaEndpoint::PopulateLocalInfo() {
    local_info_.Clear();

    RdmaPeerQueuePairInfo qp_info;
    qp_info.set_queue_pair_number(qp_->qp_num);
    qp_info.set_packet_serial_number(lrand48() & 0xffffff);
    qp_info.set_local_identifier(ib_dev_port_info_.lid);
    local_info_.set_allocated_queue_pair(&qp_info);

    RdmaPeerMemoryRegionInfo *mr_info;
    mr_info = local_info_.add_memory_regions();
    mr_info->set_address(reinterpret_cast<uint64_t>(mr_->addr));
    mr_info->set_remote_key(mr_->rkey);
    mr_info->set_size(buf_size_);
}

struct ibv_qp *RdmaEndpoint::PrepareQueuePair(uint32_t max_send_count, uint32_t max_recv_count,
                                              ibv_qp_type qp_type) {
    struct ibv_qp_init_attr qp_init_attr = {
        .send_cq = cq_,
        .recv_cq = cq_,
        .cap =
            {
                .max_send_wr = max_send_count,
                .max_recv_wr = max_recv_count,
                .max_send_sge = 1,
                .max_recv_sge = 1,
            },
        .qp_type = qp_type,
    };

    struct ibv_qp *qp = ibv_create_qp(pd_, &qp_init_attr);
    CHECK(qp != nullptr) << "Failed to create queue pair";

    struct ibv_qp_attr qp_attr = {
        .qp_state = IBV_QPS_INIT,
        .qp_access_flags = IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE,
        .pkey_index = 0,
        .port_num = ib_dev_port_,
    };

    CHECK(ibv_modify_qp(qp, &qp_attr,
                        IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS) == 0)
        << "Failed to modify queue pair to INIT";
    return qp;
}

void RdmaEndpoint::ConnectQueuePair(ibv_qp *qp, RdmaPeerQueuePairInfo remote_qp_info) {
    struct ibv_qp_attr qp_attr = {
        .qp_state = IBV_QPS_RTR,
        .path_mtu = IBV_MTU_4096,
        .rq_psn = remote_qp_info.packet_serial_number(),
        .dest_qp_num = remote_qp_info.queue_pair_number(),
        .ah_attr =
            {
                .dlid = static_cast<uint16_t>(remote_qp_info.local_identifier()),
                .sl = 0,
                .src_path_bits = 0,
                .is_global = 0,
                .port_num = ib_dev_port_,
            },
        .max_dest_rd_atomic = 1,
        .min_rnr_timer = 12,
    };

    CHECK(ibv_modify_qp(qp, &qp_attr,
                        IBV_QP_STATE | IBV_QP_PATH_MTU | IBV_QP_DEST_QPN | IBV_QP_RQ_PSN |
                            IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER | IBV_QP_AV) == 0)
        << "Failed to modify queue pair to RTR";

    qp_attr.qp_state = IBV_QPS_RTS;
    qp_attr.timeout = 14;
    qp_attr.retry_cnt = 7;
    qp_attr.rnr_retry = 7;
    qp_attr.sq_psn = local_info_.queue_pair().packet_serial_number();
    qp_attr.max_rd_atomic = 1;
    CHECK(ibv_modify_qp(qp, &qp_attr,
                        IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT | IBV_QP_RNR_RETRY |
                            IBV_QP_SQ_PSN | IBV_QP_MAX_QP_RD_ATOMIC) == 0)
        << "Failed to modify queue pair to RTS";
}

void RdmaEndpoint::Write(void *addr) {}
void RdmaEndpoint::Read(void *addr) {}
void RdmaEndpoint::Send() {}
void RdmaEndpoint::Recv() {}
void RdmaEndpoint::CompareAndSwap(void *addr) {}
void RdmaEndpoint::Poll() {}

RdmaServer::RdmaServer(char *ib_dev_name, uint8_t ib_dev_port, size_t buffer_size,
                       uint32_t max_send_count, uint32_t max_recv_count, ibv_qp_type qp_type)
    : RdmaEndpoint(ib_dev_name, ib_dev_port, buffer_size, max_send_count, max_recv_count, qp_type) {
    zmq_context_ = zmq_ctx_new();
    zmq_service_socket_ = zmq_socket(zmq_context_, ZMQ_REP);
}

RdmaServer::~RdmaServer() {
    zmq_close(zmq_service_socket_);
    zmq_ctx_destroy(zmq_context_);
}

void RdmaServer::Listen(const char *endpoint) {
    RdmaPeerInfo remote_info;
    char buffer[kZmqMessageBufferSize];
    std::string serialized_local_info = local_info_.SerializeAsString();

    CHECK(zmq_bind(zmq_service_socket_, endpoint) == 0) << "Failed to bind " << endpoint;

    zmq_recv(zmq_service_socket_, buffer, sizeof(buffer), 0);
    remote_info.ParseFromString(buffer);
    remote_info_.push_back(remote_info);

    zmq_send(zmq_service_socket_, serialized_local_info.c_str(), serialized_local_info.length(), 0);

    ConnectQueuePair(qp_, remote_info.queue_pair());
    LOG(INFO) << "Queue pair is ready to send";
}

RdmaClient::RdmaClient(char *ib_dev_name, uint8_t ib_dev_port, size_t buffer_size,
                       uint32_t max_send_count, uint32_t max_recv_count, ibv_qp_type qp_type)
    : RdmaEndpoint(ib_dev_name, ib_dev_port, buffer_size, max_send_count, max_recv_count, qp_type) {
    zmq_context_ = zmq_ctx_new();
    zmq_socket_ = zmq_socket(zmq_context_, ZMQ_REQ);
}

RdmaClient::~RdmaClient() {
    zmq_close(zmq_socket_);
    zmq_ctx_destroy(zmq_context_);
}

void RdmaClient::Connect(const char *endpoint) {
    RdmaPeerInfo remote_info;
    char buffer[kZmqMessageBufferSize];
    std::string serialized_local_info = local_info_.SerializeAsString();

    CHECK(zmq_connect(zmq_socket_, endpoint) == 0) << "Failed to connect " << endpoint;

    zmq_send(zmq_socket_, serialized_local_info.c_str(), serialized_local_info.length(), 0);

    zmq_recv(zmq_socket_, buffer, sizeof(buffer), 0);
    remote_info.ParseFromString(buffer);
    remote_info_.push_back(remote_info);

    ConnectQueuePair(qp_, remote_info.queue_pair());
    LOG(INFO) << "Queue pair is ready to send";
}

void RdmaClient::Disconnect() {}