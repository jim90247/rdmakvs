cmake_minimum_required(VERSION 3.10)
project(rdmakvs VERSION 0.1)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED True)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++14 -O3")

set(CMAKE_INSTALL_PREFIX ${PROJECT_SOURCE_DIR})
set(RDMAKVS_INSTALL_INCLUDE_DIR ${PROJECT_SOURCE_DIR}/include)
set(RDMAKVS_INSTALL_BIN_DIR ${PROJECT_SOURCE_DIR}/bin)
set(RDMAKVS_INSTALL_LIB_DIR ${PROJECT_SOURCE_DIR}/lib)

set(RDMAKVS_HEADERS_DIR ${PROJECT_SOURCE_DIR})

include_directories(${RDMAKVS_INSTALL_INCLUDE_DIR})
include_directories(${RDMAKVS_HEADERS_DIR})

add_subdirectory(src)

include(FetchContent)
FetchContent_Declare(
  googletest
  URL https://github.com/google/googletest/archive/609281088cfefc76f9d0ce82e1ff6c30cc3591e5.zip
)
FetchContent_MakeAvailable(googletest)

enable_testing()

add_subdirectory(tests)
