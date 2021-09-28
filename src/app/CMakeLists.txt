cmake_minimum_required(VERSION 3.10)
project(app)

add_executable(rdma_demo rdma_endpoint_demo.cc)
target_link_libraries(rdma_demo network glog gflags pthread)

add_executable(read_bench read_bench.cc)
target_link_libraries(read_bench network glog gflags pthread util)

add_executable(write_bench write_bench.cc)
target_link_libraries(write_bench network glog gflags pthread)

add_executable(write_msg write_messaging.cc)
target_link_libraries(write_msg network messaging glog gflags)

install(TARGETS rdma_demo read_bench write_bench write_msg DESTINATION ${RDMAKVS_INSTALL_BIN_DIR}/app)

add_subdirectory(secondary_prototype)
add_subdirectory(raw_secondary_prototype)
