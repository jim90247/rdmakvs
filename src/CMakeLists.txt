cmake_minimum_required(VERSION 3.10)
project(src)

add_subdirectory(proto)
add_subdirectory(network)
add_subdirectory(messaging)

add_subdirectory(app)

set(SERVER_SOURCE_FILES server.cc)
set(CLIENT_SOURCE_FILES client.cc)

add_executable(server ${SERVER_SOURCE_FILES})
target_link_libraries(server network)

add_executable(client ${CLIENT_SOURCE_FILES})
target_link_libraries(client network)

install(TARGETS server DESTINATION ${RDMAKVS_INSTALL_BIN_DIR})
install(TARGETS client DESTINATION ${RDMAKVS_INSTALL_BIN_DIR})
