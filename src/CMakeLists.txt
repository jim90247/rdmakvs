cmake_minimum_required(VERSION 3.10)
project(src)

include_directories(${CMAKE_CURRENT_BINARY_DIR})
add_subdirectory(proto)
add_subdirectory(network)

set(SERVER_SOURCE_FILES server.cpp)
set(CLIENT_SOURCE_FILES client.cpp)

add_executable(server ${SERVER_SOURCE_FILES})
target_link_libraries(server network)

add_executable(client ${CLIENT_SOURCE_FILES})
target_link_libraries(client network)

install(TARGETS server DESTINATION ${RDMAKVS_INSTALL_BIN_DIR})
install(TARGETS client DESTINATION ${RDMAKVS_INSTALL_BIN_DIR})
