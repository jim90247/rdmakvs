#include <glog/logging.h>
#include <glog/raw_logging.h>
#include <infiniband/verbs.h>
#include <sys/param.h>
#include <zmq.h>

#include <cerrno>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <random>
#include <stdexcept>
#include <string>

#include "network/rdma.h"

RdmaEndpoint::RdmaEndpoint(char *ib_dev_name, uint8_t ib_dev_port, volatile unsigned char *buffer,
                           size_t buffer_size, uint32_t max_send_count, uint32_t max_recv_count,
                           ibv_qp_type qp_type)
    : ib_dev_port_(ib_dev_port),
      buf_(buffer),
      buf_size_(buffer_size),
      max_send_count_(max_send_count),
      max_recv_count_(max_recv_count),
      qp_type_(qp_type) {
    CHECK(buf_ != nullptr) << "Provided buffer pointer is a nullptr";

    zmq_context_ = zmq_ctx_new();
    CHECK(zmq_context_ != nullptr)
        << "Failed to create zeromq context: " << zmq_strerror(zmq_errno());
    zmq_server_socket_ = zmq_socket(zmq_context_, ZMQ_REP);
    CHECK_NOTNULL(zmq_server_socket_);
    zmq_client_socket_ = zmq_socket(zmq_context_, ZMQ_REQ);
    CHECK_NOTNULL(zmq_client_socket_);

    ctx_ = GetIbContextFromDevice(ib_dev_name, ib_dev_port_);
    CHECK(ctx_ != nullptr) << "Failed to get InfiniBand context";

    pd_ = ibv_alloc_pd(ctx_);
    CHECK(pd_ != nullptr) << "Failed to allocate protected domain";

    CHECK_EQ(ibv_query_port(ctx_, ib_dev_port_, &ib_dev_port_info_), 0)
        << "Failed to query port " << ib_dev_port_ << " (" << ib_dev_name << ")";

    mr_ = ibv_reg_mr(pd_, const_cast<unsigned char *>(buf_), buf_size_,
                     IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE);
    CHECK(mr_ != nullptr) << "Failed to register memory region";
}

RdmaEndpoint::RdmaEndpoint() {}

RdmaEndpoint::~RdmaEndpoint() {
    for (RdmaConnection &connection : connections_) {
        if (connection.qp != nullptr) {
            ibv_destroy_qp(connection.qp);
        }
        if (connection.cq != nullptr) {
            ibv_destroy_cq(connection.cq);
        }
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

    zmq_close(zmq_server_socket_);
    zmq_close(zmq_client_socket_);
    zmq_ctx_destroy(zmq_context_);
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

size_t RdmaEndpoint::PrepareNewConnection() {
    size_t new_peer_idx = connections_.size();
    connections_.push_back(RdmaConnection());

    PrepareCompletionQueue(new_peer_idx);
    PrepareQueuePair(new_peer_idx);
    PopulateLocalInfo(new_peer_idx);
    return new_peer_idx;
}

void RdmaEndpoint::PopulateLocalInfo(size_t peer_idx) {
    // For packet serial number generation. Following the implementation in perftest to use random
    // numbers.
    // Using lrand48() & 0xffffff will result in a bug that set_packet_serial_number() does not set
    // the value.
    static thread_local std::mt19937 generator;
    std::uniform_int_distribution<uint32_t> distribution(0, 1 << 20);

    RdmaConnection &connection = connections_.at(peer_idx);

    RdmaPeerQueuePairInfo *qp_info = connection.local_info.mutable_queue_pair();
    qp_info->set_queue_pair_number(connection.qp->qp_num);
    qp_info->set_packet_serial_number(distribution(generator));
    qp_info->set_local_identifier(ib_dev_port_info_.lid);

    RdmaPeerMemoryRegionInfo *mr_info = connection.local_info.add_memory_regions();
    mr_info->set_address(reinterpret_cast<uint64_t>(mr_->addr));
    mr_info->set_remote_key(mr_->rkey);
    mr_info->set_size(buf_size_);

    DLOG(INFO) << "Local information to share with peers: "
               << connection.local_info.ShortDebugString() << " (connection index: " << peer_idx
               << ")";
}

void RdmaEndpoint::PrepareCompletionQueue(size_t peer_idx) {
    RdmaConnection &connection = connections_.at(peer_idx);
    connection.cq = ibv_create_cq(ctx_, max_send_count_ + max_recv_count_, nullptr, nullptr, 0);
    CHECK(connection.cq != nullptr) << "Failed to create completion queue";
}

void RdmaEndpoint::PrepareQueuePair(size_t peer_idx) {
    RdmaConnection &connection = connections_.at(peer_idx);
    CHECK(connection.cq != nullptr) << "Completion queue of connection " << peer_idx
                                    << " should be initialized before queue pair setup";
    struct ibv_qp_init_attr qp_init_attr = {
        .send_cq = connection.cq,
        .recv_cq = connection.cq,
        .cap =
            {
                .max_send_wr = max_send_count_,
                .max_recv_wr = max_recv_count_,
                .max_send_sge = 1,
                .max_recv_sge = 1,
            },
        .qp_type = qp_type_,
    };

    connection.qp = ibv_create_qp(pd_, &qp_init_attr);
    CHECK(connection.qp != nullptr) << "Failed to create queue pair";

    struct ibv_qp_attr qp_attr = {
        .qp_state = IBV_QPS_INIT,
        .qp_access_flags = IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE,
        .pkey_index = 0,
        .port_num = ib_dev_port_,
    };

    CHECK_EQ(ibv_modify_qp(connection.qp, &qp_attr,
                           IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS),
             0)
        << "Failed to modify queue pair to INIT";
}

void RdmaEndpoint::ExchangePeerInfo(void *zmq_socket, size_t peer_idx, bool send_first) {
    char remote_info_cstr[kZmqMessageBufferSize];
    RdmaConnection &connection = connections_.at(peer_idx);
    std::string local_info_str = connection.local_info.SerializeAsString();

    if (send_first) {
        zmq_send(zmq_socket, local_info_str.data(), local_info_str.size(), 0);
        zmq_recv(zmq_socket, remote_info_cstr, sizeof(remote_info_cstr), 0);
    } else {
        zmq_recv(zmq_socket, remote_info_cstr, sizeof(remote_info_cstr), 0);
        zmq_send(zmq_socket, local_info_str.data(), local_info_str.size(), 0);
    }

    connection.remote_info.ParseFromString(remote_info_cstr);

    DLOG(INFO) << "Remote peer " << peer_idx
               << " information: " << connection.remote_info.ShortDebugString();
}

void RdmaEndpoint::ConnectPeer(size_t peer_idx) {
    struct ibv_device_attr dev_attr;
    int rc = ibv_query_device(ctx_, &dev_attr);
    if (rc != 0) {
        RAW_LOG(FATAL, "Failed to query device: %s", strerror(rc));
    }

    RdmaConnection &connection = connections_.at(peer_idx);
    struct ibv_qp_attr qp_attr = {
        .qp_state = IBV_QPS_RTR,
        .path_mtu = IBV_MTU_4096,
        .rq_psn = connection.remote_info.queue_pair().packet_serial_number(),
        .dest_qp_num = connection.remote_info.queue_pair().queue_pair_number(),
        .ah_attr =
            {
                .dlid =
                    static_cast<uint16_t>(connection.remote_info.queue_pair().local_identifier()),
                .sl = 0,
                .src_path_bits = 0,
                .is_global = 0,
                .port_num = ib_dev_port_,
            },
        // assume using same IB NIC on both local and remote
        .max_dest_rd_atomic = static_cast<std::uint8_t>(dev_attr.max_qp_rd_atom),
        .min_rnr_timer = 12,
    };

    CHECK_EQ(ibv_modify_qp(connection.qp, &qp_attr,
                           IBV_QP_STATE | IBV_QP_PATH_MTU | IBV_QP_DEST_QPN | IBV_QP_RQ_PSN |
                               IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER | IBV_QP_AV),
             0)
        << "Failed to modify queue pair to RTR";

    qp_attr.qp_state = IBV_QPS_RTS;
    qp_attr.timeout = 14;
    qp_attr.retry_cnt = 7;
    qp_attr.rnr_retry = 7;
    qp_attr.sq_psn = connection.local_info.queue_pair().packet_serial_number();
    qp_attr.max_rd_atomic = dev_attr.max_qp_rd_atom;
    CHECK_EQ(ibv_modify_qp(connection.qp, &qp_attr,
                           IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT | IBV_QP_RNR_RETRY |
                               IBV_QP_SQ_PSN | IBV_QP_MAX_QP_RD_ATOMIC),
             0)
        << "Failed to modify queue pair to RTS";
}

void RdmaEndpoint::InitializeFastWrite(size_t remote_id, size_t batch_size) {
    CHECK_LE(batch_size, kMaxBatchSize) << "Exceed max batch size (" << kMaxBatchSize << ")";
    RdmaConnection &connection = connections_.at(remote_id);

    for (size_t i = 0; i < batch_size; i++) {
        connection.req_template.sge_template[i].lkey = mr_->lkey;

        connection.req_template.send_wr_template[i] = {
            .next =
                i == batch_size - 1 ? nullptr : &connection.req_template.send_wr_template[i + 1],
            .sg_list = &connection.req_template.sge_template[i],
            .num_sge = 1,
            .opcode = IBV_WR_RDMA_WRITE,
            .wr = {.rdma = {.rkey = connection.remote_info.memory_regions(0).remote_key()}},
        };
    }
}

void RdmaEndpoint::FillOutWriteWorkRequest(
    size_t remote_id, const std::vector<std::tuple<uint64_t, uint64_t, uint32_t>> &requests,
    unsigned int flags) {
    int batch_size = requests.size();
    RdmaConnection &connection = connections_.at(remote_id);
    auto remote_mr = connection.remote_info.memory_regions(0);

    struct ibv_sge *sge = connection.req_template.sge_template;
    struct ibv_send_wr *wr = connection.req_template.send_wr_template;

    for (int i = 0; i < batch_size; i++) {
        uint64_t local_offset, remote_offset;
        uint32_t length;
        std::tie(local_offset, remote_offset, length) = requests[i];

        // Use debug logging to reduce performance impact
        DLOG_IF(FATAL, local_offset + length > buf_size_)
            << "Local offset " << local_offset << "out of bound";
        DLOG_IF(FATAL, remote_offset + length > remote_mr.size())
            << "Remote offset " << remote_offset << " out of bound";

        sge[i].length = length;
        sge[i].addr = reinterpret_cast<uint64_t>(buf_) + local_offset;

        wr[i].wr_id = connection.wr_status.next_wr_id++;
        wr[i].send_flags = flags;
        wr[i].wr.rdma.remote_addr = remote_mr.address() + remote_offset;
    }
}

int RdmaEndpoint::PostSendWithAutoReclaim(size_t remote_id, struct ibv_send_wr *wr) {
    struct ibv_send_wr *bad_wr;
    if (wr == nullptr) {
        wr = connections_.at(remote_id).req_template.send_wr_template;
    }

    struct ibv_qp *qp = connections_.at(remote_id).qp;
    int rc = ibv_post_send(qp, wr, &bad_wr);

    // Automatic reclaim is not as efficient as manual reclaim.
    // Whenever possible (hints are available in higher-level application context), try manual
    // reclaim for better performance.
    while (rc == ENOMEM) {
        // Reclaim some completion queue resources
        WaitForCompletion(remote_id, false, 0);
        // Retry from the last failed one
        rc = ibv_post_send(qp, bad_wr, &bad_wr);
    }

    return rc;
}

uint64_t RdmaEndpoint::Write(bool initialized, size_t remote_id, uint64_t local_offset,
                             uint64_t remote_offset, uint32_t length, unsigned int flags) {
    if (!initialized) {
        InitializeFastWrite(remote_id, 1);
    }

    // Fill template
    FillOutWriteWorkRequest(remote_id, {std::make_tuple(local_offset, remote_offset, length)},
                            flags);

    int rc = PostSendWithAutoReclaim(remote_id);

    DLOG_IF(ERROR, rc != 0) << "Error posting IBV_WR_RDMA_WRITE work request: " << strerror(rc);
    if (rc == 0 && (flags & IBV_SEND_SIGNALED)) {
        connections_.at(remote_id).wr_status.in_progress_signaled_wrs++;
    }

    return connections_.at(remote_id).req_template.send_wr_template[0].wr_id;
}

std::vector<uint64_t> RdmaEndpoint::WriteBatch(
    bool initialized, size_t remote_id,
    const std::vector<std::tuple<uint64_t, uint64_t, uint32_t>> &requests,
    SignalStrategy signal_strategy, unsigned int flags) {
    RdmaConnection &connection = connections_.at(remote_id);
    size_t batch_size = requests.size();
    DLOG_IF(FATAL, batch_size > kMaxBatchSize) << "Exceed max batch size (" << kMaxBatchSize << ")";

    if (!initialized) {
        InitializeFastWrite(remote_id, batch_size);
    }

    std::vector<uint64_t> wr_ids;

    switch (signal_strategy) {
        case SignalStrategy::kSignalNone:
        case SignalStrategy::kSignalLast:
            flags &= ~IBV_SEND_SIGNALED;
            break;
        case SignalStrategy::kSignalAll:
            flags |= IBV_SEND_SIGNALED;
            break;
    }

    // Fill template
    FillOutWriteWorkRequest(remote_id, requests, flags);
    struct ibv_send_wr *wr = connection.req_template.send_wr_template;

    if (signal_strategy == SignalStrategy::kSignalLast) {
        wr[batch_size - 1].send_flags |= IBV_SEND_SIGNALED;
    }

    int rc = PostSendWithAutoReclaim(remote_id);
    LOG_IF(ERROR, rc != 0) << "Error posting IBV_WR_RDMA_WRITE work request: " << strerror(rc);

    if (rc == 0) {
        switch (signal_strategy) {
            case SignalStrategy::kSignalLast:
                connection.wr_status.in_progress_signaled_wrs += 1;
                wr_ids.push_back(wr[batch_size - 1].wr_id);
                break;
            case SignalStrategy::kSignalAll:
                connection.wr_status.in_progress_signaled_wrs += batch_size;
                for (int i = 0; i < batch_size; i++) wr_ids.push_back(wr[i].wr_id);
                break;
            default:
                break;
        }
    }

    return wr_ids;
}

uint64_t RdmaEndpoint::Read(size_t remote_id, uint64_t local_offset, uint64_t remote_offset,
                            uint32_t length, unsigned int flags) {
    RdmaConnection &connection = connections_.at(remote_id);
    auto remote_mr = connection.remote_info.memory_regions(0);
    DLOG_IF(FATAL, local_offset + length > buf_size_)
        << "Local offset " << local_offset << "out of bound";
    DLOG_IF(FATAL, remote_offset + length > remote_mr.size())
        << "Remote offset " << remote_offset << " out of bound";

    struct ibv_sge sg = {
        .addr = reinterpret_cast<uint64_t>(buf_) + local_offset,
        .length = length,
        .lkey = mr_->lkey,
    };

    struct ibv_send_wr wr = {
        .wr_id = connection.wr_status.next_wr_id++,
        .next = nullptr,
        .sg_list = &sg,
        .num_sge = 1,
        .opcode = IBV_WR_RDMA_READ,
        .send_flags = flags,
        .wr =
            {
                .rdma =
                    {
                        .remote_addr = remote_mr.address() + remote_offset,
                        .rkey = remote_mr.remote_key(),
                    },
            },
    };
    int rc = PostSendWithAutoReclaim(remote_id, &wr);

    LOG_IF(ERROR, rc != 0) << "Error posting IBV_WR_RDMA_READ work request: " << strerror(rc);
    if (rc == 0 && (flags & IBV_SEND_SIGNALED)) {
        connection.wr_status.in_progress_signaled_wrs += 1;
    }

    return wr.wr_id;
}

void RdmaEndpoint::SetReadBatchSize(int batch) {
    if (batch < 1 || batch > kMaxBatchSize) {
        RAW_LOG(FATAL, "Invalid RDMA read batch size: %d (valid range = [1, %lu]).", batch,
                kMaxBatchSize);
    }
    read_batch_size_ = batch;
    for (RdmaConnection &connection : connections_) {
        for (size_t i = 0; i < batch; i++) {
            connection.req_template.read_sge_template[i].lkey = mr_->lkey;

            connection.req_template.read_wr_template[i] = {
                .next =
                    (i == batch - 1) ? nullptr : &connection.req_template.read_wr_template[i + 1],
                .sg_list = &connection.req_template.read_sge_template[i],
                .num_sge = 1,
                .opcode = IBV_WR_RDMA_READ,
                .wr = {.rdma = {.rkey = connection.remote_info.memory_regions(0).remote_key()}},
            };
        }
    }
}

uint64_t RdmaEndpoint::Read_v2(size_t remote_id, uint64_t local_offset, uint64_t remote_offset,
                               uint32_t length, unsigned int flags) {
    RAW_DCHECK(read_batch_size_ != -1,
               "read_batch_size_ must be setted via SetReadBatchSize before calling Read_v2");
    RdmaConnection &c = connections_.at(remote_id);
    auto remote_mr = c.remote_info.memory_regions(0);

    RAW_DCHECK(local_offset + length <= buf_size_, "local offset out of bound");
    RAW_DCHECK(remote_offset + length <= remote_mr.size(), "remote offset out of bound");

    int &b = c.req_template.read_batch_idx;
    uint64_t wr_id = c.wr_status.next_wr_id++;
    c.req_template.read_sge_template[b].addr = reinterpret_cast<uint64_t>(buf_) + local_offset;
    c.req_template.read_sge_template[b].length = length;
    c.req_template.read_wr_template[b].wr_id = wr_id;
    c.req_template.read_wr_template[b].send_flags = flags;
    c.req_template.read_wr_template[b].wr.rdma.remote_addr = remote_mr.address() + remote_offset;

    b++;
    if (b == read_batch_size_) {
        b = 0;
        int rc = PostSendWithAutoReclaim(remote_id, c.req_template.read_wr_template);
        if (rc != 0) {
            RAW_LOG(ERROR, "Error posting IBV_WR_RDMA_READ work request (%s)", strerror(rc));
        } else {
            for (int i = 0; i < read_batch_size_; i++) {
                if (c.req_template.read_wr_template[i].send_flags & IBV_SEND_SIGNALED) {
                    c.wr_status.in_progress_signaled_wrs += 1;
                }
            }
        }
    }

    return wr_id;
}

bool RdmaEndpoint::FlushPendingReads(size_t remote_id) {
    RdmaConnection &c = connections_.at(remote_id);
    int &b = c.req_template.read_batch_idx;
    if (b == 0) {
        // nothing to do if there's no pending request
        return true;
    }
    // break the linked list
    c.req_template.read_wr_template[b].next = nullptr;

    int rc = PostSendWithAutoReclaim(remote_id, c.req_template.read_wr_template);
    if (rc != 0) {
        RAW_LOG(ERROR, "Error posting IBV_WR_RDMA_READ work request (%s)", strerror(rc));
    } else {
        for (int i = 0; i < b; i++) {
            if (c.req_template.read_wr_template[i].send_flags & IBV_SEND_SIGNALED) {
                c.wr_status.in_progress_signaled_wrs += 1;
            }
        }
    }

    if (b < read_batch_size_ - 1) {
        // recover the linked list
        c.req_template.read_wr_template[b].next = &c.req_template.read_wr_template[b + 1];
    }
    b = 0;

    return rc == 0;
}

uint64_t RdmaEndpoint::Send(size_t remote_id, uint64_t offset, uint32_t length,
                            unsigned int flags) {
    DLOG_IF(FATAL, offset + length >= buf_size_) << "Local offset " << offset << "out of bound";
    RdmaWorkRequestStatus &wr_status = connections_.at(remote_id).wr_status;

    struct ibv_sge sg = {
        .addr = reinterpret_cast<uint64_t>(buf_) + offset,
        .length = length,
        .lkey = mr_->lkey,
    };

    struct ibv_send_wr wr = {
        .wr_id = wr_status.next_wr_id++,
        .next = nullptr,
        .sg_list = &sg,
        .num_sge = 1,
        .opcode = IBV_WR_SEND,
        .send_flags = flags,
    };

    int rc = PostSendWithAutoReclaim(remote_id, &wr);

    LOG_IF(ERROR, rc != 0) << "Error posting IBV_WR_SEND work request: " << strerror(rc);
    if (rc == 0 && (flags & IBV_SEND_SIGNALED)) wr_status.in_progress_signaled_wrs += 1;

    return wr.wr_id;
}

uint64_t RdmaEndpoint::Recv(size_t remote_id, uint64_t offset, uint32_t length) {
    // TODO: consider using shared receive queue?
    DLOG_IF(FATAL, offset + length >= buf_size_) << "Local offset " << offset << "out of bound";

    RdmaWorkRequestStatus &wr_status = connections_.at(remote_id).wr_status;

    struct ibv_sge sg = {
        .addr = reinterpret_cast<uint64_t>(buf_) + offset,
        .length = length,
        .lkey = mr_->lkey,
    };

    struct ibv_recv_wr *bad_wr, wr = {
                                    .wr_id = wr_status.next_wr_id++,
                                    .next = nullptr,
                                    .sg_list = &sg,
                                    .num_sge = 1,
                                };

    int rc = ibv_post_recv(connections_.at(remote_id).qp, &wr, &bad_wr);

    LOG_IF(ERROR, rc != 0) << "Error posting recv wr: " << strerror(rc);
    if (rc == 0) wr_status.in_progress_signaled_wrs += 1;

    return wr.wr_id;
}

void RdmaEndpoint::CompareAndSwap(void *addr) { LOG(FATAL) << "CompareAndSwap is not implemented"; }

void RdmaEndpoint::WaitForCompletion(size_t remote_id, bool poll_until_found,
                                     uint64_t target_wr_id) {
    RdmaWorkRequestStatus &wr_status = connections_.at(remote_id).wr_status;
    // Early exit if target work request already completed
    if (poll_until_found &&
        wr_status.completed_wr_ids.find(target_wr_id) != wr_status.completed_wr_ids.end()) {
        return;
    }

    struct ibv_cq *cq = connections_.at(remote_id).cq;

    const static int kBatch = 32;
    struct ibv_wc wc_list[kBatch];
    int n = 0;
    do {
        n = ibv_poll_cq(cq, kBatch, wc_list);

        CHECK_GE(n, 0) << "Error polling completion queue " << n;

        for (int i = 0; i < n; i++) {
            DLOG_IF(ERROR, wc_list[i].status != IBV_WC_SUCCESS)
                << "Work request " << wc_list[i].wr_id
                << " completed with error: " << ibv_wc_status_str(wc_list[i].status);
            // TODO: Use a more efficient way to store completed work request
            wr_status.completed_wr_ids.insert(wc_list[i].wr_id);
            wr_status.in_progress_signaled_wrs--;
        }
    } while (wr_status.in_progress_signaled_wrs >
                 0 /* TODO: how should this condition be updated in multi-thread model? */
             && (n == 0 || (poll_until_found && wr_status.completed_wr_ids.find(target_wr_id) ==
                                                    wr_status.completed_wr_ids.end())));
}

void RdmaEndpoint::ClearCompletedRecords(size_t remote_id) {
    connections_.at(remote_id).wr_status.completed_wr_ids.clear();
}

void RdmaEndpoint::BindToZmqEndpoint(const char *endpoint) {
    CHECK_EQ(zmq_bind(zmq_server_socket_, endpoint), 0)
        << "Failed to bind to " << endpoint << ": " << zmq_strerror(zmq_errno());
}

void RdmaEndpoint::Listen() {
    size_t remote_id = PrepareNewConnection();
    ExchangePeerInfo(zmq_server_socket_, remote_id, false);
    ConnectPeer(remote_id);

    LOG(INFO) << "Queue pair " << remote_id << " is ready to send";
}

void RdmaEndpoint::Connect(const char *endpoint) {
    CHECK_EQ(zmq_connect(zmq_client_socket_, endpoint), 0)
        << "Failed to connect " << endpoint << ": " << zmq_strerror(zmq_errno());

    size_t remote_id = PrepareNewConnection();
    ExchangePeerInfo(zmq_client_socket_, remote_id, true);
    ConnectPeer(remote_id);

    CHECK_EQ(zmq_disconnect(zmq_client_socket_, endpoint), 0);

    LOG(INFO) << "Queue pair " << remote_id << " is ready to send";
}
