#include "mock_client/deterministic_data.h"

#include <gtest/gtest.h>

namespace mock_client {
namespace {

TEST(DeterministicDataTest, SameInputsProduceSameIdentityFields) {
    const auto first = MakeObjectIdentity(23, "tenant/a", 3, 17, 1, 4096);
    const auto second = MakeObjectIdentity(23, "tenant/a", 3, 17, 1, 4096);

    EXPECT_EQ(first.key, second.key);
    EXPECT_EQ(first.generation, second.generation);
    EXPECT_EQ(first.logical_size, second.logical_size);
    EXPECT_EQ(first.checksum, second.checksum);
    EXPECT_EQ(DeterministicSize(23, "tenant/a", 3, 17, 512, 8192),
              DeterministicSize(23, "tenant/a", 3, 17, 512, 8192));
    EXPECT_EQ(DeterministicChecksum(23, "tenant/a", 3, 17, 1, 4096),
              DeterministicChecksum(23, "tenant/a", 3, 17, 1, 4096));
}

TEST(DeterministicDataTest, GenerationContributesToChecksum) {
    EXPECT_NE(DeterministicChecksum(23, "tenant/a", 3, 17, 1, 4096),
             DeterministicChecksum(23, "tenant/a", 3, 17, 2, 4096));
}

TEST(DeterministicDataTest, NamespaceIsEncodedInKeyPrefix) {
    const auto first = DeterministicKey(23, "tenant/a", 3, 17);
    const auto second = DeterministicKey(23, "tenant-b", 3, 17);
    const auto first_prefix = first.substr(0, first.find("/s"));
    const auto second_prefix = second.substr(0, second.find("/s"));

    EXPECT_NE(first_prefix, second_prefix);
    EXPECT_NE(first, second);
}

TEST(DeterministicDataTest, SizeStaysWithinInclusiveRange) {
    const auto size = DeterministicSize(23, "tenant/a", 3, 17, 512, 8192);

    EXPECT_GE(size, 512);
    EXPECT_LE(size, 8192);
}

}  // namespace
}  // namespace mock_client
