#include <gtest/gtest.h>
#include "fake_master_api.h"
#include "mock_client/master_session.h"
namespace mock_client {
TEST(MasterSessionTest, RemountUsesOriginalDescriptors) {
    class Api : public FakeMasterApi {
       public:
        std::vector<mooncake::Segment> seen;
        VoidResult ReMountSegment(
            const std::vector<mooncake::Segment>& s) override {
            seen = s;
            return {};
        }
    } api;
    mooncake::Segment segment;
    segment.id = {1, 2};
    segment.name = "s";
    segment.base = 4096;
    segment.size = 4096;
    MasterSession session(api, std::vector<std::string>{"a", "b"}, {segment});
    EXPECT_TRUE(session.HandlePingStatus(mooncake::ClientStatus::NEED_REMOUNT));
    ASSERT_EQ(api.seen.size(), 1);
    EXPECT_EQ(api.seen[0], segment);
}
TEST(MasterSessionTest, RejectsOverlappingSyntheticRanges) {
    MockClientDefinition definition;
    definition.uuid = "1-1";
    definition.host_id = "host";
    MockSegmentConfig a;
    a.id = "1-2";
    a.name = "a";
    a.base_address = 4096;
    a.capacity_bytes = 8192;
    a.protocol = "tcp";
    a.transport_endpoint = "fake:1";
    definition.segments = {a, a};
    definition.segments[1].id = "1-3";
    definition.segments[1].name = "b";
    definition.segments[1].base_address = 12288;
    ASSERT_TRUE(SyntheticSegmentConfig::Build(definition));
    definition.segments[1].base_address = 8192;
    EXPECT_FALSE(SyntheticSegmentConfig::Build(definition));
}
}  // namespace mock_client
namespace mock_client {
TEST(MasterSessionTest, MountsAllSegmentsAndOfflinePingSendsNothing) {
    FakeMasterApi api;
    mooncake::Segment a, b;
    a.id = {1, 2};
    b.id = {1, 3};
    MasterSession session(api, std::vector<std::string>{"a", "b"}, {a, b});
    ASSERT_TRUE(session.ConnectAndMount());
    EXPECT_EQ(api.mounted.size(), 2);
    session.Disconnect();
    auto count = api.calls.size();
    EXPECT_FALSE(session.PingOnce());
    EXPECT_EQ(api.calls.size(), count);
}
TEST(MasterSessionTest,
     FailoverTriesHealthyEndpointAndRemountsWithoutMountReplay) {
    class Api : public FakeMasterApi {
       public:
        mooncake::ErrorCode Connect(const std::string& e) override {
            endpoints.push_back(e);
            return e == "a" ? mooncake::ErrorCode::RPC_FAIL
                            : mooncake::ErrorCode::OK;
        }
    } api;
    mooncake::Segment s;
    s.id = {1, 2};
    s.name = "s";
    s.base = 4096;
    s.size = 4096;
    MasterSession session(api, std::vector<std::string>{"a", "b"}, {s});
    ASSERT_TRUE(session.ReconnectAndRemount());
    EXPECT_EQ(api.endpoints, (std::vector<std::string>{"a", "b"}));
    EXPECT_TRUE(api.mounted.empty());
    EXPECT_EQ(api.remounted, (std::vector<mooncake::Segment>{s}));
}
TEST(MasterSessionTest,
     RejectsMisalignedOverflowAndDuplicateSyntheticSegments) {
    auto c = ExecutorConfig().clients.front();
    ASSERT_TRUE(SyntheticSegmentConfig::Build(c));
    c.segments.front().base_address = 4097;
    EXPECT_FALSE(SyntheticSegmentConfig::Build(c));
    c.segments.front().base_address = UINT64_MAX - 4095;
    EXPECT_FALSE(SyntheticSegmentConfig::Build(c));
}
TEST(MasterSessionTest, UnmountErrorRemainsVisibleAndRetryable) {
    FakeMasterApi api;
    mooncake::Segment s;
    s.id = {1, 2};
    MasterSession session(api, "a", {s});
    ASSERT_TRUE(session.ConnectAndMount());
    api.mount_result = tl::make_unexpected(mooncake::ErrorCode::RPC_TIMEOUT);
    EXPECT_FALSE(session.Unmount());
    EXPECT_EQ(session.last_error(), mooncake::ErrorCode::RPC_TIMEOUT);
    api.mount_result = {};
    EXPECT_TRUE(session.Unmount());
}
TEST(MasterSessionTest, LostMountResponseStillUnmountsAcceptedSegment) {
    FakeMasterApi api;
    auto segments =
        SyntheticSegmentConfig::Build(ExecutorConfig().clients.front());
    ASSERT_TRUE(segments);
    api.SetRpcHook([](const RpcEvent& event) {
        RpcAction action;
        action.drop_response = !event.before && event.rpc == "MountSegment";
        return action;
    });
    MasterSession session(api, "a", *segments);
    EXPECT_FALSE(session.ConnectAndMount());
    EXPECT_EQ(api.calls,
              (std::vector<std::string>{"Connect", "MountSegment",
                                        "UnmountSegment", "Disconnect"}));
    EXPECT_EQ(session.last_error(), mooncake::ErrorCode::RPC_TIMEOUT);
    EXPECT_FALSE(session.connected());
}
TEST(MasterSessionTest, FailedMountRollbackRetainsCleanupErrorAndSegments) {
    class Api : public FakeMasterApi {
       public:
        std::vector<mooncake::UUID> unmounted;
        bool fail_cleanup = true;
        VoidResult MountSegment(const mooncake::Segment& segment) override {
            calls.push_back("MountSegment");
            mounted.push_back(segment);
            if (mounted.size() == 2)
                return tl::make_unexpected(
                    mooncake::ErrorCode::NO_AVAILABLE_HANDLE);
            return {};
        }
        VoidResult UnmountSegment(const mooncake::UUID& id) override {
            calls.push_back("UnmountSegment");
            unmounted.push_back(id);
            if (fail_cleanup)
                return tl::make_unexpected(mooncake::ErrorCode::RPC_TIMEOUT);
            return {};
        }
    } api;
    mooncake::Segment a, b;
    a.id = {1, 2};
    b.id = {1, 3};
    MasterSession session(api, "a", {a, b});
    EXPECT_FALSE(session.ConnectAndMount());
    EXPECT_EQ(session.last_error(), mooncake::ErrorCode::RPC_TIMEOUT);
    EXPECT_TRUE(session.connected());
    const auto calls = api.calls;
    EXPECT_FALSE(session.ConnectAndMount());
    EXPECT_EQ(api.calls, calls);
    EXPECT_EQ(session.last_error(), mooncake::ErrorCode::RPC_TIMEOUT);
    api.fail_cleanup = false;
    EXPECT_TRUE(session.Unmount());
    EXPECT_EQ(api.unmounted, (std::vector<mooncake::UUID>{a.id, a.id}));
    EXPECT_EQ(session.last_error(), mooncake::ErrorCode::OK);
}
TEST(MasterSessionTest, RejectsMalformedUuidRatherThanParsingPrefix) {
    auto c = ExecutorConfig().clients.front();
    c.segments.front().id = "1-2junk";
    EXPECT_FALSE(SyntheticSegmentConfig::Build(c));
    c.segments.front().id = "00000000-0000-4000-8000-000000000101";
    auto segments = SyntheticSegmentConfig::Build(c);
    ASSERT_TRUE(segments);
    EXPECT_NE(segments->front().id, (mooncake::UUID{0, 0}));
}
}  // namespace mock_client
