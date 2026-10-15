#include <gtest/gtest.h>
#include "mock_client/operation_executor.h"
#include "fake_master_api.h"
namespace mock_client {
TEST(OperationExecutorTest, SinglePutCompletesEachObjectAndRecordsShadow) {
    FakeMasterApi api;
    api.starts = {std::vector{MemoryDescriptor()},
                  std::vector{MemoryDescriptor(18)}};
    ShadowModel shadow;
    OperationExecutor executor(api, ExecutorConfig(), shadow);
    const auto r = executor.Execute(
        Request(LogicalOperation::Put, OperationForm::Single, 2));
    EXPECT_EQ(api.calls, (std::vector<std::string>{"PutStart", "PutEnd",
                                                   "PutStart", "PutEnd"}));
    ASSERT_EQ(r.items.size(), 2);
    EXPECT_EQ(r.logical_ops, 2);
    EXPECT_EQ(api.lengths, (std::vector<std::vector<uint64_t>>{{64}, {64}}));
    ASSERT_NE(shadow.Lookup("key0"), nullptr);
    EXPECT_TRUE(shadow.Lookup("key0")->exists);
}
TEST(OperationExecutorTest,
     BatchPutEndsAcceptedAndRevokesOnlyDefiniteEndFailure) {
    FakeMasterApi api;
    api.starts = {std::vector{MemoryDescriptor()},
                  tl::make_unexpected(mooncake::ErrorCode::NO_AVAILABLE_HANDLE),
                  std::vector{MemoryDescriptor(18)}};
    api.ends = {VoidResult{},
                tl::make_unexpected(mooncake::ErrorCode::INTERNAL_ERROR)};
    api.revokes = {VoidResult{}};
    ShadowModel shadow;
    OperationExecutor executor(api, ExecutorConfig(), shadow);
    auto r = executor.Execute(
        Request(LogicalOperation::Put, OperationForm::Batch, 3));
    EXPECT_EQ(api.calls,
              (std::vector<std::string>{"BatchPutStart", "BatchPutEnd",
                                        "BatchPutRevoke"}));
    EXPECT_EQ(api.ended_keys, (std::vector<std::string>{"key0", "key2"}));
    EXPECT_EQ(api.revoked_keys, (std::vector<std::string>{"key2"}));
    ASSERT_EQ(r.items.size(), 3);
    EXPECT_EQ(r.items[1].error, mooncake::ErrorCode::NO_AVAILABLE_HANDLE);
    EXPECT_TRUE(shadow.CanMutate("key1"));
    ASSERT_NE(shadow.Lookup("key2"), nullptr);
    EXPECT_FALSE(shadow.Lookup("key2")->exists);
    EXPECT_FALSE(r.transport_unknown);
}
TEST(OperationExecutorTest, TimeoutFreezesKeyWithoutReplayOrRevoke) {
    FakeMasterApi api;
    ShadowModel shadow;
    OperationExecutor executor(api, ExecutorConfig(), shadow);
    auto request = Request(LogicalOperation::Put);
    auto result = executor.Execute(request);
    EXPECT_TRUE(result.transport_unknown);
    EXPECT_FALSE(shadow.CanMutate("key0"));
    executor.Execute(request);
    EXPECT_EQ(api.calls, (std::vector<std::string>{"PutStart"}));
}
TEST(OperationExecutorTest, UpsertUsesOwnSingleAndBatchLifecycle) {
    for (auto form : {OperationForm::Single, OperationForm::Batch}) {
        FakeMasterApi api;
        api.starts = {std::vector{MemoryDescriptor()}};
        api.ends = {VoidResult{}};
        ShadowModel shadow;
        OperationExecutor executor(api, ExecutorConfig(), shadow);
        executor.Execute(Request(LogicalOperation::Upsert, form));
        EXPECT_EQ(api.calls,
                  form == OperationForm::Single
                      ? (std::vector<std::string>{"UpsertStart", "UpsertEnd"})
                      : (std::vector<std::string>{"BatchUpsertStart",
                                                  "BatchUpsertEnd"}));
        ASSERT_NE(shadow.Lookup("key0"), nullptr);
        EXPECT_TRUE(shadow.Lookup("key0")->exists);
    }
}
TEST(OperationExecutorTest, GetsAndRemovesUseSelectedForm) {
    for (auto form : {OperationForm::Single, OperationForm::Batch}) {
        FakeMasterApi api;
        api.gets = {tl::make_unexpected(mooncake::ErrorCode::OBJECT_NOT_FOUND),
                    tl::make_unexpected(mooncake::ErrorCode::OBJECT_NOT_FOUND)};
        api.removes = {VoidResult{}, VoidResult{}};
        ShadowModel shadow;
        shadow.ApplyRemove("key0");
        shadow.ApplyRemove("key1");
        OperationExecutor executor(api, ExecutorConfig(), shadow);
        auto g = executor.Execute(Request(LogicalOperation::Get, form, 2));
        auto r = executor.Execute(Request(LogicalOperation::Remove, form, 2));
        EXPECT_EQ(api.calls, form == OperationForm::Single
                                 ? (std::vector<std::string>{
                                       "GetReplicaList", "GetReplicaList",
                                       "Remove", "Remove"})
                                 : (std::vector<std::string>{
                                       "BatchGetReplicaList", "BatchRemove"}));
        ASSERT_EQ(g.items.size(), 2);
        ASSERT_EQ(r.items.size(), 2);
    }
}
TEST(OperationExecutorTest, FirstReadChecksAcceptedDescriptorIdentityAndSize) {
    for (int corrupt = 0; corrupt < 4; ++corrupt) {
        FakeMasterApi api;
        api.starts = {std::vector{MemoryDescriptor()}};
        ShadowModel shadow;
        OperationExecutor executor(api, ExecutorConfig(), shadow);
        executor.Execute(Request(LogicalOperation::Put));
        auto d = MemoryDescriptor();
        d.status = mooncake::ReplicaStatus::COMPLETE;
        if (corrupt == 0) d.id = 99;
        if (corrupt == 1)
            d.get_memory_descriptor().buffer_descriptor.size_ = 32;
        if (corrupt == 2)
            d.get_memory_descriptor().buffer_descriptor.buffer_address_ = 9999;
        if (corrupt == 3)
            d.get_memory_descriptor().buffer_descriptor.transport_endpoint_ =
                "other:1";
        api.gets = {mooncake::GetReplicaListResponse{std::vector{d}, 0, 123}};
        auto result = executor.Execute(Request(LogicalOperation::Get));
        ASSERT_EQ(result.items.size(), 1);
        EXPECT_NE(result.items[0].error, mooncake::ErrorCode::OK);
    }
}
TEST(OperationExecutorTest,
     LostEndResolvesPendingGenerationFromReadWithoutReplay) {
    FakeMasterApi api;
    api.starts = {std::vector{MemoryDescriptor()}};
    api.end_result = tl::make_unexpected(mooncake::ErrorCode::RPC_TIMEOUT);
    ShadowModel shadow;
    OperationExecutor executor(api, ExecutorConfig(), shadow);
    executor.Execute(Request(LogicalOperation::Put));
    EXPECT_FALSE(shadow.CanMutate("key0"));
    auto d = MemoryDescriptor();
    d.status = mooncake::ReplicaStatus::COMPLETE;
    api.gets = {mooncake::GetReplicaListResponse{std::vector{d}, 0, 123}};
    auto r = executor.Execute(Request(LogicalOperation::Get));
    ASSERT_EQ(r.items.size(), 1);
    EXPECT_EQ(r.items[0].error, mooncake::ErrorCode::OK);
    ASSERT_NE(shadow.Lookup("key0"), nullptr);
    EXPECT_TRUE(shadow.CanMutate("key0"));
    EXPECT_EQ(shadow.Lookup("key0")->identity.generation, 1);
    EXPECT_EQ(api.calls, (std::vector<std::string>{"PutStart", "PutEnd",
                                                   "GetReplicaList"}));
}
}  // namespace mock_client
namespace mock_client {
TEST(OperationExecutorTest,
     HookInjectionIsNotSentAndResponseDropFreezesKnownStart) {
    FakeMasterApi api;
    ShadowModel shadow;
    OperationExecutor executor(api, ExecutorConfig(), shadow);
    std::vector<RpcEvent> events;
    api.SetRpcHook([&](const RpcEvent& e) {
        events.push_back(e);
        RpcAction a;
        if (e.before)
            a.error_before_send = mooncake::ErrorCode::NO_AVAILABLE_HANDLE;
        return a;
    });
    executor.Execute(Request(LogicalOperation::Put));
    EXPECT_TRUE(api.calls.empty());
    ASSERT_EQ(events.size(), 2);
    EXPECT_FALSE(events.back().sent);
    EXPECT_FALSE(events.back().response_received);
    api.starts = {std::vector{MemoryDescriptor()}};
    events.clear();
    api.SetRpcHook([&](const RpcEvent& e) {
        events.push_back(e);
        RpcAction a;
        if (!e.before && e.rpc == "PutEnd") a.drop_response = true;
        return a;
    });
    auto r = executor.Execute(Request(LogicalOperation::Put));
    EXPECT_TRUE(r.transport_unknown);
    EXPECT_FALSE(shadow.CanMutate("key0"));
    EXPECT_EQ(api.calls, (std::vector<std::string>{"PutStart", "PutEnd"}));
    ASSERT_EQ(events.size(), 4);
    EXPECT_TRUE(events.back().sent);
    EXPECT_TRUE(events.back().response_received);
    EXPECT_EQ(events.front().request_id, events.back().request_id);
    EXPECT_LT(events.front().attempt_id, events.back().attempt_id);
}
TEST(OperationExecutorTest,
     ValidationReadsHaveNoLogicalWorkloadCountAndTransportIsNotMismatch) {
    FakeMasterApi api;
    api.starts = {std::vector{MemoryDescriptor()}};
    ShadowModel shadow;
    OperationExecutor executor(api, ExecutorConfig(), shadow);
    executor.Execute(Request(LogicalOperation::Put));
    api.gets = {tl::make_unexpected(mooncake::ErrorCode::RPC_TIMEOUT)};
    auto r = executor.ValidateAll();
    EXPECT_EQ(r.logical_ops, 0);
    EXPECT_TRUE(r.transport_unknown);
    ASSERT_EQ(r.items.size(), 1);
    EXPECT_FALSE(r.items.front().validation_mismatch);
}
TEST(OperationExecutorTest, MissingBatchRepliesFreezeAllAffectedKeys) {
    FakeMasterApi api;
    ShadowModel shadow;
    OperationExecutor executor(api, ExecutorConfig(), shadow);
    auto r = executor.Execute(
        Request(LogicalOperation::Put, OperationForm::Batch, 2));
    EXPECT_TRUE(r.transport_unknown);
    EXPECT_FALSE(shadow.CanMutate("key0"));
    EXPECT_FALSE(shadow.CanMutate("key1"));
    EXPECT_EQ(api.calls, (std::vector<std::string>{"BatchPutStart"}));
}
TEST(OperationExecutorTest, ReplicationPolicyIsPassedUnchanged) {
    FakeMasterApi api;
    api.starts = {std::vector{MemoryDescriptor(), MemoryDescriptor(18)}};
    auto config = ExecutorConfig();
    config.replication.replica_num = 2;
    config.replication.preferred_segments = {"segment"};
    config.replication.with_hard_pin = true;
    ShadowModel shadow;
    OperationExecutor executor(api, config, shadow);
    auto r = executor.Execute(Request(LogicalOperation::Put));
    EXPECT_TRUE(r.ok());
    EXPECT_EQ(api.replication.replica_num, 2);
    EXPECT_EQ(api.replication.preferred_segments,
              (std::vector<std::string>{"segment"}));
    EXPECT_TRUE(api.replication.with_hard_pin);
}
}  // namespace mock_client
namespace mock_client {
TEST(OperationExecutorTest,
     ReconcilesSessionFrozenKeysEvenWithoutPendingMutation) {
    FakeMasterApi api;
    api.starts = {std::vector{MemoryDescriptor()}};
    ShadowModel shadow;
    OperationExecutor executor(api, ExecutorConfig(), shadow);
    executor.Execute(Request(LogicalOperation::Put));
    shadow.MarkUnknown("key0");
    auto d = MemoryDescriptor();
    d.status = mooncake::ReplicaStatus::COMPLETE;
    api.gets = {mooncake::GetReplicaListResponse{std::vector{d}, 0, 123}};
    auto result = executor.ReconcileUnknown();
    EXPECT_EQ(result.items.size(), 1);
    EXPECT_TRUE(shadow.CanMutate("key0"));
}
TEST(OperationExecutorTest, ReconcilesAbsentSessionFrozenKeys) {
    FakeMasterApi api;
    api.starts = {std::vector{MemoryDescriptor()}};
    ShadowModel shadow;
    OperationExecutor executor(api, ExecutorConfig(), shadow);
    executor.Execute(Request(LogicalOperation::Put));
    shadow.MarkUnknown("key0");
    api.gets = {tl::make_unexpected(mooncake::ErrorCode::OBJECT_NOT_FOUND)};
    auto result = executor.ReconcileUnknown();
    EXPECT_TRUE(result.ok());
    ASSERT_NE(shadow.Lookup("key0"), nullptr);
    EXPECT_FALSE(shadow.Lookup("key0")->exists);
    EXPECT_TRUE(shadow.CanMutate("key0"));
    EXPECT_EQ(shadow.Lookup("key0")->identity.generation, 1);
}
TEST(OperationExecutorTest, CapabilityProbeValidatesAndCleansOnlyProbeKey) {
    FakeMasterApi api;
    api.starts = {std::vector{MemoryDescriptor()}};
    auto d = MemoryDescriptor();
    d.status = mooncake::ReplicaStatus::COMPLETE;
    api.gets = {mooncake::GetReplicaListResponse{std::vector{d}, 0, 123}};
    ShadowModel shadow;
    OperationExecutor executor(api, ExecutorConfig(), shadow);
    auto result = executor.CapabilityProbe({"probe", 1, 64, 123});
    EXPECT_TRUE(result.ok());
    EXPECT_EQ(result.logical_ops, 0);
    EXPECT_EQ(api.calls,
              (std::vector<std::string>{"PutStart", "PutEnd", "GetReplicaList",
                                        "Remove"}));
    ASSERT_NE(shadow.Lookup("probe"), nullptr);
    EXPECT_FALSE(shadow.Lookup("probe")->exists);
}
TEST(OperationExecutorTest,
     FirstReadRejectsWrongVariantStatusCountAndChecksum) {
    for (int corrupt = 0; corrupt < 4; ++corrupt) {
        FakeMasterApi api;
        api.starts = {std::vector{MemoryDescriptor()}};
        ShadowModel shadow;
        OperationExecutor executor(api, ExecutorConfig(), shadow);
        executor.Execute(Request(LogicalOperation::Put));
        auto d = MemoryDescriptor();
        d.status = mooncake::ReplicaStatus::COMPLETE;
        if (corrupt == 0) d.descriptor_variant = mooncake::DiskDescriptor{};
        if (corrupt == 1) d.status = mooncake::ReplicaStatus::PROCESSING;
        auto response =
            mooncake::GetReplicaListResponse{std::vector{d}, 0, 123};
        if (corrupt == 2) response.replicas.clear();
        if (corrupt == 3) response.object_checksum = 456;
        api.gets = {response};
        auto result = executor.Execute(Request(LogicalOperation::Get));
        ASSERT_EQ(result.items.size(), 1);
        EXPECT_TRUE(result.items[0].validation_mismatch);
    }
}
}  // namespace mock_client
