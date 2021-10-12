#include "util/stats.h"

#include <chrono>
#include <cmath>

#include "gmock/gmock-matchers.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

using namespace ::testing;

TEST(ComputeOperationsPerSecondTest, HandlesPositiveOperation) {
    std::chrono::time_point<std::chrono::steady_clock> start;
    std::chrono::time_point<std::chrono::steady_clock> end = start + std::chrono::seconds(2);
    double operations = 100;
    double expected = 50;

    double result = ComputeOperationsPerSecond(start, end, operations);

    EXPECT_THAT(result, NanSensitiveDoubleEq(expected));
}

TEST(ComputeOperationsPerSecondTest, HandlesZeroOperation) {
    std::chrono::time_point<std::chrono::steady_clock> start;
    std::chrono::time_point<std::chrono::steady_clock> end = start + std::chrono::seconds(2);
    double operations = 0;
    double expected = 0;

    double result = ComputeOperationsPerSecond(start, end, operations);

    EXPECT_THAT(result, NanSensitiveDoubleEq(expected));
}

TEST(ComputeOperationsPerSecondTest, HandlesZeroDuration) {
    std::chrono::time_point<std::chrono::steady_clock> start;
    std::chrono::time_point<std::chrono::steady_clock> end = start;
    double operations = 100;
    double expected = std::nan("ComputeOperationsPerSecondTest");

    double result = ComputeOperationsPerSecond(start, end, operations);

    EXPECT_THAT(result, NanSensitiveDoubleEq(expected));
}
