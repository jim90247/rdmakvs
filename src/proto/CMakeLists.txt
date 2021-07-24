find_package(Protobuf REQUIRED)

set(PROTOS rdma.proto)
protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS ${PROTOS})

add_library(proto ${PROTO_HDRS} ${PROTO_SRCS})
