cmake_minimum_required(VERSION 3.10)
project(app)

add_executable(rdma_demo rdma_endpoint_demo.cc)
target_link_libraries(rdma_demo network glog gflags pthread)
install(TARGETS rdma_demo DESTINATION ${RDMAKVS_INSTALL_BIN_DIR}/app)

add_executable(write_bench write_bench.cc)
target_link_libraries(write_bench network glog gflags pthread)
install(TARGETS write_bench DESTINATION ${RDMAKVS_INSTALL_BIN_DIR}/app)

add_executable(write_msg write_messaging.cc)
target_link_libraries(write_msg network messaging glog gflags)
install(TARGETS write_msg DESTINATION ${RDMAKVS_INSTALL_BIN_DIR}/app)
