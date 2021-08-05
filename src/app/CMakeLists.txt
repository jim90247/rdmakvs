cmake_minimum_required(VERSION 3.10)
project(app)

add_executable(write_msg write_messaging.cc)
target_link_libraries(write_msg network messaging glog gflags)
install(TARGETS write_msg DESTINATION ${RDMAKVS_INSTALL_BIN_DIR}/app)
