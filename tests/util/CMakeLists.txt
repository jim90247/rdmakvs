add_executable(
  stats_test
  stats_test.cc
)
target_link_libraries(
  stats_test
  gmock
  gtest_main
  util
)

include(GoogleTest)
gtest_discover_tests(stats_test)
