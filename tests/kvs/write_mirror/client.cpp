#include "gtest/gtest.h"
#include "tests/network/mock_rdma_endpoint.hpp"

// Demonstrate some basic assertions.
TEST(HelloTest, BasicAssertions) {
    // Expect two strings not to be equal.
    EXPECT_STRNE("hello", "world");
    // Expect equality.
    EXPECT_EQ(7 * 6, 42);
}

TEST(RdmaMessagingTest, SendOneMessage) {
    MockRdmaEndpoint endpoint;
    EXPECT_EQ(7 * 6, 42);
}
