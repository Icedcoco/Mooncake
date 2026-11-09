#pragma once
#include "mock_client/master_api.h"
#include "mock_client/operation_executor.h"
namespace mock_client {
// Network-boundary fake; the executor and shadow model remain real.
class FakeMasterApi : public MasterApi {
   public:
    std::vector<std::string> calls, endpoints, keys;
    std::vector<mooncake::Segment> mounted, remounted;
    mooncake::ReplicateConfig replication;
    std::vector<std::vector<uint64_t>> lengths;
    mooncake::ErrorCode connect_error = mooncake::ErrorCode::OK;
    VoidResult mount_result{}, end_result{}, revoke_result{}, remove_result{};
    PingResult ping_result{
        mooncake::PingResponse{0, mooncake::ClientStatus::NEED_REMOUNT}};
    StartResults starts;
    VoidResults ends, revokes, removes;
    GetResults gets;
    void Disconnect() override { calls.push_back("Disconnect"); }
    mooncake::ErrorCode Connect(const std::string& endpoint) override {
        calls.push_back("Connect");
        endpoints.push_back(endpoint);
        return connect_error;
    }
    VoidResult MountSegment(const mooncake::Segment& segment) override {
        calls.push_back("MountSegment");
        mounted.push_back(segment);
        return mount_result;
    }
    VoidResult ReMountSegment(
        const std::vector<mooncake::Segment>& segments) override {
        calls.push_back("ReMountSegment");
        remounted = segments;
        return mount_result;
    }
    VoidResult UnmountSegment(const mooncake::UUID&) override {
        calls.push_back("UnmountSegment");
        return mount_result;
    }
    PingResult Ping() override {
        calls.push_back("Ping");
        return ping_result;
    }
#define SINGLE_START(Name)                                                     \
    StartResult Name(const std::string& key, const std::vector<size_t>& sizes, \
                     const mooncake::ReplicateConfig& config) override {       \
        calls.push_back(#Name);                                                \
        keys.push_back(key);                                                   \
        lengths.emplace_back(sizes.begin(), sizes.end());                      \
        replication = config;                                                  \
        return starts.empty() ? StartResult(tl::make_unexpected(               \
                                    mooncake::ErrorCode::RPC_TIMEOUT))         \
                              : starts.at(keys.size() - 1);                    \
    }
    SINGLE_START(PutStart)
    SINGLE_START(UpsertStart)
#undef SINGLE_START
#define BATCH_START(Name)                                                 \
    StartResults Name(const std::vector<std::string>& k,                  \
                      const std::vector<std::vector<uint64_t>>& sizes,    \
                      const mooncake::ReplicateConfig& config) override { \
        calls.push_back(#Name);                                           \
        keys = k;                                                         \
        lengths = sizes;                                                  \
        replication = config;                                             \
        return starts;                                                    \
    }
    BATCH_START(BatchPutStart)
    BATCH_START(BatchUpsertStart)
#undef BATCH_START
#define END(Name)                                                            \
    VoidResult Name(const mooncake::ObjectMeta&, mooncake::ReplicaType type) \
        override {                                                           \
        calls.push_back(#Name);                                              \
        EXPECT_EQ(type, mooncake::ReplicaType::MEMORY);                      \
        return end_result;                                                   \
    }
    END(PutEnd)
    END(UpsertEnd)
#undef END
#define REVOKE(Name)                                                           \
    VoidResult Name(const std::string&, mooncake::ReplicaType type) override { \
        calls.push_back(#Name);                                                \
        EXPECT_EQ(type, mooncake::ReplicaType::MEMORY);                        \
        return revoke_result;                                                  \
    }
    REVOKE(PutRevoke)
    REVOKE(UpsertRevoke)
#undef REVOKE
#define BATCH_END(Name)                                              \
    VoidResults Name(const std::vector<mooncake::ObjectMeta>& metas) \
        override {                                                   \
        calls.push_back(#Name);                                      \
        for (const auto& m : metas) ended_keys.push_back(m.key);     \
        return ends;                                                 \
    }
    std::vector<std::string> ended_keys, revoked_keys;
    BATCH_END(BatchPutEnd)
    BATCH_END(BatchUpsertEnd)
#undef BATCH_END
#define BATCH_REVOKE(Name)                                         \
    VoidResults Name(const std::vector<std::string>& k) override { \
        calls.push_back(#Name);                                    \
        revoked_keys = k;                                          \
        return revokes;                                            \
    }
    BATCH_REVOKE(BatchPutRevoke)
    BATCH_REVOKE(BatchUpsertRevoke)
#undef BATCH_REVOKE
    GetResult GetReplicaList(const std::string&) override {
        calls.push_back("GetReplicaList");
        return gets.at(0);
    }
    GetResults BatchGetReplicaList(const std::vector<std::string>& k) override {
        calls.push_back("BatchGetReplicaList");
        keys = k;
        return gets;
    }
    VoidResult Remove(const std::string&, bool) override {
        calls.push_back("Remove");
        return remove_result;
    }
    VoidResults BatchRemove(const std::vector<std::string>& k, bool) override {
        calls.push_back("BatchRemove");
        keys = k;
        return removes;
    }
};
inline mooncake::Replica::Descriptor MemoryDescriptor(uint64_t id = 17) {
    mooncake::Replica::Descriptor d;
    d.id = id;
    d.status = mooncake::ReplicaStatus::PROCESSING;
    d.descriptor_variant =
        mooncake::MemoryDescriptor{{64, 4096, "tcp", "fake:1"}};
    return d;
}
inline MockClientConfig ExecutorConfig() {
    MockClientConfig c;
    MockClientDefinition client;
    client.uuid = "1-2";
    client.host_id = "host";
    client.segments.push_back(
        {"1-3", "segment", 4096, "tcp", "fake:1", 4096, "host"});
    c.clients.push_back(client);
    return c;
}
inline OperationRequest Request(LogicalOperation op,
                                OperationForm form = OperationForm::Single,
                                size_t n = 1) {
    OperationRequest r{{op, form, 1, {1, 2}}, {}, "workload"};
    for (size_t i = 0; i < n; ++i)
        r.objects.push_back({"key" + std::to_string(i), 1, 64, 123 + i});
    return r;
}
}  // namespace mock_client
