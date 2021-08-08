add_executable(
  client_test
  client_test.cpp
)
target_link_libraries(
  client_test
  gtest_main
  gmock
  network
)

include(GoogleTest)
gtest_discover_tests(client_test)
