#include <glog/logging.h>
#include <infiniband/verbs.h>
#include <network/rdma.pb.h>
#include <sys/param.h>
#include <zmq.h>

#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <network/rdma.hpp>
#include <random>
#include <stdexcept>
#include <string>

RdmaEndpoint::RdmaEndpoint(char *ib_dev_name, uint8_t ib_dev_port, char *buffer, size_t buffer_size,
                           uint32_t max_send_count, uint32_t max_recv_count, ibv_qp_type qp_type,
                           uint64_t wr_offset, uint64_t wr_count)
    : ib_dev_port_(ib_dev_port),
      buf_(buffer),
      buf_size_(buffer_size),
      next_wr_id_(wr_offset),
      num_wr_in_progress_(0),
      kWorkRequestIdOffset(wr_offset),
      kWorkRequestIdRegionSize(wr_count),
      zmq_socket_(nullptr) {
    CHECK(buf_ != nullptr) << "Provided buffer pointer is a nullptr";

    zmq_context_ = zmq_ctx_new();
    CHECK(zmq_context_ != nullptr)
        << "Failed to create zeromq context: " << zmq_strerror(zmq_errno());

    ctx_ = GetIbContextFromDevice(ib_dev_name, ib_dev_port_);
    CHECK(ctx_ != nullptr) << "Failed to get InfiniBand context";

    pd_ = ibv_alloc_pd(ctx_);
    CHECK(pd_ != nullptr) << "Failed to allocate protected domain";

    CHECK_EQ(ibv_query_port(ctx_, ib_dev_port_, &ib_dev_port_info_), 0)
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
    // For packet serial number generation. Following the implementation in perftest to use random
    // numbers.
    // Using lrand48() & 0xffffff will result in a bug that set_packet_serial_number() does not set
    // the value.
    static thread_local std::mt19937 generator;
    std::uniform_int_distribution<uint32_t> distribution(0, 1 << 20);

    RdmaPeerQueuePairInfo *qp_info = local_info_.mutable_queue_pair();
    qp_info->set_queue_pair_number(qp_->qp_num);
    qp_info->set_packet_serial_number(distribution(generator));
    qp_info->set_local_identifier(ib_dev_port_info_.lid);

    RdmaPeerMemoryRegionInfo *mr_info = local_info_.add_memory_regions();
    mr_info->set_address(reinterpret_cast<uint64_t>(mr_->addr));
    mr_info->set_remote_key(mr_->rkey);
    mr_info->set_size(buf_size_);

    LOG(INFO) << "Local information to share with peers: " << local_info_.ShortDebugString();
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

    CHECK_EQ(ibv_modify_qp(qp, &qp_attr,
                           IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS),
             0)
        << "Failed to modify queue pair to INIT";
    return qp;
}

size_t RdmaEndpoint::ExchangePeerInfo(void *zmq_socket, bool send_first) {
    char remote_info_cstr[kZmqMessageBufferSize];
    RdmaPeerInfo remote_info;
    std::string local_info_str = local_info_.SerializeAsString();

    if (send_first) {
        zmq_send(zmq_socket_, local_info_str.data(), local_info_str.size(), 0);
        zmq_recv(zmq_socket_, remote_info_cstr, sizeof(remote_info_cstr), 0);
    } else {
        zmq_recv(zmq_socket_, remote_info_cstr, sizeof(remote_info_cstr), 0);
        zmq_send(zmq_socket_, local_info_str.data(), local_info_str.size(), 0);
    }

    remote_info.ParseFromString(remote_info_cstr);
    remote_info_.push_back(remote_info);

    LOG(INFO) << "Remote peer " << remote_info_.size() - 1
              << " information: " << remote_info.ShortDebugString();

    return remote_info_.size() - 1;
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

    CHECK_EQ(ibv_modify_qp(qp, &qp_attr,
                           IBV_QP_STATE | IBV_QP_PATH_MTU | IBV_QP_DEST_QPN | IBV_QP_RQ_PSN |
                               IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER | IBV_QP_AV),
             0)
        << "Failed to modify queue pair to RTR";

    qp_attr.qp_state = IBV_QPS_RTS;
    qp_attr.timeout = 14;
    qp_attr.retry_cnt = 7;
    qp_attr.rnr_retry = 7;
    qp_attr.sq_psn = local_info_.queue_pair().packet_serial_number();
    qp_attr.max_rd_atomic = 1;
    CHECK_EQ(ibv_modify_qp(qp, &qp_attr,
                           IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT | IBV_QP_RNR_RETRY |
                               IBV_QP_SQ_PSN | IBV_QP_MAX_QP_RD_ATOMIC),
             0)
        << "Failed to modify queue pair to RTS";
}

inline void RdmaEndpoint::PopulateWriteWorkRequest(struct ibv_sge *sg, struct ibv_send_wr *wr,
                                                   size_t remote_id, uint64_t local_offset,
                                                   uint64_t remote_offset, uint32_t length,
                                                   struct ibv_send_wr *next, unsigned int flags) {
    CHECK_NOTNULL(sg);
    CHECK_NOTNULL(wr);

    *sg = {
        .addr = reinterpret_cast<uint64_t>(buf_) + local_offset,
        .length = length,
        .lkey = mr_->lkey,
    };

    *wr = {
        .wr_id = IncrementWorkRequestId(),
        .next = next,
        .sg_list = sg,
        .num_sge = 1,
        .opcode = IBV_WR_RDMA_WRITE,
        .send_flags = flags,
        .wr =
            {
                .rdma =
                    {
                        .remote_addr =
                            remote_info_[remote_id].memory_regions(0).address() + remote_offset,
                        .rkey = remote_info_[remote_id].memory_regions(0).remote_key(),
                    },
            },
    };
}

uint64_t RdmaEndpoint::Write(size_t remote_id, uint64_t local_offset, uint64_t remote_offset,
                             uint32_t length, unsigned int flags, ibv_send_wr **bad_wr) {
    CHECK_LT(remote_id, remote_info_.size()) << "Remote id " << remote_id << " out of bound";
    CHECK_LE(local_offset + length, buf_size_) << "Local offset " << local_offset << "out of bound";
    CHECK_LE(remote_offset + length, remote_info_[remote_id].memory_regions(0).size())
        << "Remote offset " << remote_offset << " out of bound";

    struct ibv_sge sg;
    struct ibv_send_wr *local_bad_wr, wr;

    PopulateWriteWorkRequest(&sg, &wr, remote_id, local_offset, remote_offset, length, nullptr,
                             flags);

    int rc = ibv_post_send(qp_, &wr, bad_wr == nullptr ? &local_bad_wr : bad_wr);

    LOG_IF(ERROR, rc != 0) << "Error posting IBV_WR_RDMA_WRITE work request: " << strerror(rc);
    if (rc == 0 && (flags & IBV_SEND_SIGNALED)) num_wr_in_progress_ += 1;

    return wr.wr_id;
}

std::vector<uint64_t> RdmaEndpoint::WriteBatch(
    size_t remote_id, const std::vector<std::tuple<uint64_t, uint64_t, uint32_t>> &requests,
    SignalStrategy signal_strategy, unsigned int flags, ibv_send_wr **bad_wr) {
    CHECK_LT(remote_id, remote_info_.size()) << "Remote id " << remote_id << " out of bound";

    size_t n = requests.size();
    CHECK_LE(n, kMaxBatchSize) << "Exceed max batch size (" << kMaxBatchSize << ")";

    struct ibv_send_wr *local_bad_wr, wr_list[kMaxBatchSize];
    struct ibv_sge sg_list[kMaxBatchSize];
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

    for (size_t wr_idx = 0; wr_idx < n; wr_idx++) {
        uint64_t local_offset, remote_offset;
        uint32_t length;
        std::tie(local_offset, remote_offset, length) = requests[wr_idx];

        CHECK_LE(local_offset + length, buf_size_)
            << "Local offset " << local_offset << "out of bound";
        CHECK_LE(remote_offset + length, remote_info_[remote_id].memory_regions(0).size())
            << "Remote offset " << remote_offset << " out of bound";

        if (wr_idx == n - 1 && signal_strategy == SignalStrategy::kSignalLast)
            flags |= IBV_SEND_SIGNALED;

        PopulateWriteWorkRequest(&sg_list[wr_idx], &wr_list[wr_idx], remote_id, local_offset,
                                 remote_offset, length,
                                 wr_idx == n - 1 ? nullptr : &wr_list[wr_idx + 1], flags);
    }

    int rc = ibv_post_send(qp_, wr_list, bad_wr == nullptr ? &local_bad_wr : bad_wr);

    LOG_IF(ERROR, rc != 0) << "Error posting IBV_WR_RDMA_WRITE work request: " << strerror(rc);

    if (rc == 0) {
        switch (signal_strategy) {
            case SignalStrategy::kSignalLast:
                num_wr_in_progress_ += 1;
                wr_ids.push_back(wr_list[n - 1].wr_id);
                break;
            case SignalStrategy::kSignalAll:
                num_wr_in_progress_ += n;
                for (auto wr : wr_list) wr_ids.push_back(wr.wr_id);
                break;
            default:
                break;
        }
    }

    return wr_ids;
}

uint64_t RdmaEndpoint::Read(size_t remote_id, uint64_t local_offset, uint64_t remote_offset,
                            uint32_t length, unsigned int flags, ibv_send_wr **bad_wr) {
    CHECK_LT(remote_id, remote_info_.size()) << "Remote id " << remote_id << " out of bound";
    CHECK_LE(local_offset + length, buf_size_) << "Local offset " << local_offset << "out of bound";
    CHECK_LE(remote_offset + length, remote_info_[remote_id].memory_regions(0).size())
        << "Remote offset " << remote_offset << " out of bound";

    struct ibv_sge sg = {
        .addr = reinterpret_cast<uint64_t>(buf_) + local_offset,
        .length = length,
        .lkey = mr_->lkey,
    };

    struct ibv_send_wr *local_bad_wr,
        wr = {
            .wr_id = IncrementWorkRequestId(),
            .next = nullptr,
            .sg_list = &sg,
            .num_sge = 1,
            .opcode = IBV_WR_RDMA_READ,
            .send_flags = flags,
            .wr =
                {
                    .rdma =
                        {
                            .remote_addr =
                                remote_info_[remote_id].memory_regions(0).address() + remote_offset,
                            .rkey = remote_info_[remote_id].memory_regions(0).remote_key(),
                        },
                },
        };
    int rc = ibv_post_send(qp_, &wr, bad_wr == nullptr ? &local_bad_wr : bad_wr);

    LOG_IF(ERROR, rc != 0) << "Error posting IBV_WR_RDMA_WRITE work request: " << strerror(rc);
    if (rc == 0 && (flags & IBV_SEND_SIGNALED)) num_wr_in_progress_ += 1;

    return wr.wr_id;
}

uint64_t RdmaEndpoint::Send(uint64_t offset, uint32_t length, unsigned int flags,
                            ibv_send_wr **bad_wr) {
    CHECK_LE(offset + length, buf_size_) << "Local offset " << offset << "out of bound";

    struct ibv_sge sg = {
        .addr = reinterpret_cast<uint64_t>(buf_) + offset,
        .length = length,
        .lkey = mr_->lkey,
    };

    struct ibv_send_wr *local_bad_wr, wr = {
                                          .wr_id = IncrementWorkRequestId(),
                                          .next = nullptr,
                                          .sg_list = &sg,
                                          .num_sge = 1,
                                          .opcode = IBV_WR_SEND,
                                          .send_flags = flags,
                                      };

    int rc = ibv_post_send(qp_, &wr, bad_wr == nullptr ? &local_bad_wr : bad_wr);

    LOG_IF(ERROR, rc != 0) << "Error posting IBV_WR_SEND work request: " << strerror(rc);
    if (rc == 0 && (flags & IBV_SEND_SIGNALED)) num_wr_in_progress_ += 1;

    return wr.wr_id;
}

uint64_t RdmaEndpoint::Recv(uint64_t offset, uint32_t length, ibv_recv_wr **bad_wr) {
    CHECK_LE(offset + length, buf_size_) << "Local offset " << offset << "out of bound";

    struct ibv_sge sg = {
        .addr = reinterpret_cast<uint64_t>(buf_) + offset,
        .length = length,
        .lkey = mr_->lkey,
    };

    struct ibv_recv_wr *local_bad_wr, wr = {
                                          .wr_id = IncrementWorkRequestId(),
                                          .next = nullptr,
                                          .sg_list = &sg,
                                          .num_sge = 1,
                                      };

    int rc = ibv_post_recv(qp_, &wr, bad_wr == nullptr ? &local_bad_wr : bad_wr);

    LOG_IF(ERROR, rc != 0) << "Error posting recv wr: " << strerror(rc);
    if (rc == 0) num_wr_in_progress_ += 1;

    return wr.wr_id;
}

void RdmaEndpoint::CompareAndSwap(void *addr) {}

void RdmaEndpoint::WaitForCompletion(std::unordered_set<uint64_t> &completed_wr,
                                     bool poll_until_found, uint64_t target_wr_id) {
    // Early exit if target work request already completed
    if (poll_until_found && completed_wr.find(target_wr_id) != completed_wr.end()) return;

    const int kBatch = 32;
    struct ibv_wc wc_list[kBatch];
    int n = 0;
    do {
        n = ibv_poll_cq(cq_, kBatch, wc_list);

        CHECK_GE(n, 0) << "Error polling completion queue " << n;

        for (int i = 0; i < n; i++) {
            LOG_IF(ERROR, wc_list[i].status != IBV_WC_SUCCESS)
                << "Work request " << wc_list[i].wr_id
                << " completed with error: " << ibv_wc_status_str(wc_list[i].status);
            completed_wr.insert(wc_list[i].wr_id);
            num_wr_in_progress_--;
        }
    } while (
        num_wr_in_progress_ > 0 &&
        (n == 0 || (poll_until_found && completed_wr.find(target_wr_id) == completed_wr.end())));
}

uint64_t RdmaEndpoint::IncrementWorkRequestId() {
    uint64_t current = next_wr_id_;
    next_wr_id_ =
        (next_wr_id_ + 1 - kWorkRequestIdOffset) % kWorkRequestIdRegionSize + kWorkRequestIdOffset;
    return current;
}
