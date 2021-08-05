cmake_minimum_required(VERSION 3.10)
project(messaging CXX)

set(SOURCE_FILES
    rdma_messaging.h
    rdma_messaging.cc
)
add_library(messaging ${SOURCE_FILES})

target_link_libraries(messaging network proto)
