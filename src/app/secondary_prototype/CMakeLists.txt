cmake_minimum_required(VERSION 3.10)

set(LIBRARIES network messaging glog gflags pthread)

add_executable(secondary_server server.cc)
target_link_libraries(secondary_server ${LIBRARIES})

add_executable(secondary_client client.cc)
target_link_libraries(secondary_client ${LIBRARIES})

install(TARGETS secondary_server secondary_client DESTINATION ${RDMAKVS_INSTALL_BIN_DIR}/app/secondary_prototype)
