cmake_minimum_required(VERSION 3.10)
project(network CXX)

set(SOURCE_FILES
    common.hpp
    rdma.hpp
    rdma_client.cpp
    rdma_endpoint.cpp
    rdma_server.cpp
)
add_library(network ${SOURCE_FILES})

target_link_libraries(network gflags glog ibverbs zmq protobuf tcmalloc proto)
