cmake_minimum_required(VERSION 3.10)

set(LIBRARIES network glog gflags pthread)

set(COMMON_SRCS common.h common.cc)

add_executable(raw_secondary_server server.cc ${COMMON_SRCS})
target_link_libraries(raw_secondary_server ${LIBRARIES})

add_executable(raw_secondary_client client.cc ${COMMON_SRCS})
target_link_libraries(raw_secondary_client ${LIBRARIES})

install(TARGETS raw_secondary_server raw_secondary_client DESTINATION ${RDMAKVS_INSTALL_BIN_DIR}/app/raw_secondary_prototype)
