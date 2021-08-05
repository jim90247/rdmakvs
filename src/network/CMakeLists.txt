cmake_minimum_required(VERSION 3.10)
project(network CXX)

set(SOURCE_FILES
    rdma.h
    rdma_client.cc
    rdma_endpoint.cc
    rdma_server.cc
)
add_library(network ${SOURCE_FILES})

target_link_libraries(network gflags glog ibverbs zmq protobuf proto)
