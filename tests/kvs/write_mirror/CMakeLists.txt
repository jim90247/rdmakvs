add_executable(
  hello_test
  client.cpp
)
target_link_libraries(
  hello_test
  gtest_main
  gmock
  network
)

include(GoogleTest)
gtest_discover_tests(hello_test)
