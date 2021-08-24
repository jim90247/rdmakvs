cmake_minimum_required(VERSION 3.10)

set(LIBRARIES network messaging glog gflags pthread)

set(COMMON_SRCS common.h common.cc)

add_executable(secondary_server server.cc ${COMMON_SRCS})
target_link_libraries(secondary_server ${LIBRARIES})

add_executable(secondary_client client.cc ${COMMON_SRCS})
target_link_libraries(secondary_client ${LIBRARIES})

install(TARGETS secondary_server secondary_client DESTINATION ${RDMAKVS_INSTALL_BIN_DIR}/app/secondary_prototype)
