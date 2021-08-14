cmake_minimum_required(VERSION 3.10)
project(rdmakvs VERSION 0.1)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED True)
set(CMAKE_CXX_EXTENSIONS OFF)
add_compile_options(-O3)

if (NOT DEBUG)
  add_compile_definitions(NDEBUG)
endif()

set(CMAKE_INSTALL_PREFIX ${PROJECT_SOURCE_DIR})
set(RDMAKVS_INSTALL_BIN_DIR ${PROJECT_SOURCE_DIR}/bin)

set(RDMAKVS_HEADERS_DIR ${PROJECT_SOURCE_DIR}/include)
include_directories(${PROJECT_SOURCE_DIR}/src)
include_directories(${RDMAKVS_HEADERS_DIR})

# for protobuf generated files
include_directories(${CMAKE_CURRENT_BINARY_DIR}/src)

add_subdirectory(src)

if (TEST)
  include(FetchContent)
  FetchContent_Declare(
    googletest
    URL https://github.com/google/googletest/archive/609281088cfefc76f9d0ce82e1ff6c30cc3591e5.zip
  )
  FetchContent_MakeAvailable(googletest)

  enable_testing()

  include_directories(${PROJECT_SOURCE_DIR}/tests)
  add_subdirectory(tests)
endif()