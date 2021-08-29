cmake_minimum_required(VERSION 3.10)

set(LIBRARIES network messaging glog gflags pthread)

set(COMMON_SRCS common.cc common.h)

add_executable(msgchk_server server.cc ${COMMON_SRCS})
target_link_libraries(msgchk_server ${LIBRARIES})

add_executable(msgchk_client client.cc ${COMMON_SRCS})
target_link_libraries(msgchk_client ${LIBRARIES})

install(TARGETS msgchk_server msgchk_client DESTINATION ${RDMAKVS_INSTALL_BIN_DIR}/app/messaging_check)
