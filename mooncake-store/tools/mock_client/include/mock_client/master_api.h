#pragma once
#include <functional>
#include <memory>
#include <chrono>
#include <optional>
#include <type_traits>
#include "rpc_types.h"
namespace mooncake {
class MasterClient;
}
namespace mock_client {
using VoidResult = tl::expected<void, mooncake::ErrorCode>;
using VoidResults = std::vector<VoidResult>;
using StartResult = tl::expected<std::vector<mooncake::Replica::Descriptor>,
                                 mooncake::ErrorCode>;
using StartResults = std::vector<StartResult>;
using GetResult =
    tl::expected<mooncake::GetReplicaListResponse, mooncake::ErrorCode>;
using GetResults = std::vector<GetResult>;
using PingResult = tl::expected<mooncake::PingResponse, mooncake::ErrorCode>;
// One context spans all rounds of a logical request. API instances are confined
// to a single worker; context/hook changes require quiescing its calls.
struct RpcContext {
    std::string request_id, purpose = "session", logical_op = "none",
                            operation_form = "single";
};
struct RpcEvent : RpcContext {
    std::string rpc;
    bool before = false, sent = false, response_received = false,
         response_dropped = false;
    uint64_t attempt_id = 0, batch_items = 0, latency_us = 0;
    uint64_t successful_items = 0, failed_items = 0, unknown_items = 0;
    mooncake::ErrorCode error_code = mooncake::ErrorCode::OK;
    std::vector<mooncake::ErrorCode> item_errors;
};
struct RpcAction {
    std::optional<mooncake::ErrorCode> error_before_send;
    bool drop_response = false;
};
using RpcHook = std::function<RpcAction(const RpcEvent&)>;
bool IsTransportError(mooncake::ErrorCode error);
class MasterApi {
   public:
    virtual ~MasterApi() = default;
    void SetRpcHook(RpcHook hook) { hook_ = std::move(hook); }
    void SetRpcContext(RpcContext context) { context_ = std::move(context); }
    const RpcContext& rpc_context() const { return context_; }
    // Hook runs before send and after the physical result, including per-item
    // errors. A drop returned by the after-hook hides the result from callers;
    // loggers must record that action as response_dropped=true/result=unknown.
    template <class F>
    auto InvokeRpc(const std::string& name, size_t count, F call) {
        using R = decltype(call());
        auto failure = [count](mooncake::ErrorCode error) -> R {
            if constexpr (requires(R r) { r.has_value(); })
                return tl::make_unexpected(error);
            else
                return R(count, tl::make_unexpected(error));
        };
        RpcEvent event;
        static_cast<RpcContext&>(event) = context_;
        event.rpc = name;
        event.before = true;
        event.batch_items = count;
        event.attempt_id = ++attempt_id_;
        auto start = std::chrono::steady_clock::now();
        const auto action = hook_ ? hook_(event) : RpcAction{};
        R result = action.error_before_send ? failure(*action.error_before_send)
                                            : call();
        event.before = false;
        event.sent = !action.error_before_send;
        auto record = [&](const auto& item) {
            const auto error = item ? mooncake::ErrorCode::OK : item.error();
            event.item_errors.push_back(error);
            if (IsTransportError(error))
                ++event.unknown_items;
            else if (error != mooncake::ErrorCode::OK)
                ++event.failed_items;
            else
                ++event.successful_items;
            if (event.error_code == mooncake::ErrorCode::OK)
                event.error_code = error;
        };
        if constexpr (requires(R r) { r.has_value(); })
            record(result);
        else {
            if (result.size() != count)
                result = failure(mooncake::ErrorCode::RPC_FAIL);
            for (const auto& item : result) record(item);
        }
        event.response_received = event.sent && event.unknown_items == 0;
        event.latency_us =
            std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::steady_clock::now() - start)
                .count();
        const auto after = hook_ ? hook_(event) : RpcAction{};
        if (after.drop_response && event.sent)
            return failure(mooncake::ErrorCode::RPC_TIMEOUT);
        return result;
    }
    virtual void Disconnect() = 0;
    virtual mooncake::ErrorCode Connect(const std::string& endpoint) = 0;
    virtual VoidResult MountSegment(const mooncake::Segment& segment) = 0;
    virtual VoidResult ReMountSegment(
        const std::vector<mooncake::Segment>& segments) = 0;
    virtual VoidResult UnmountSegment(const mooncake::UUID& id) = 0;
    virtual PingResult Ping() = 0;
    virtual StartResult PutStart(const std::string& key,
                                 const std::vector<size_t>& lengths,
                                 const mooncake::ReplicateConfig& config) = 0;
    virtual StartResults BatchPutStart(
        const std::vector<std::string>& keys,
        const std::vector<std::vector<uint64_t>>& lengths,
        const mooncake::ReplicateConfig& config) = 0;
    virtual VoidResult PutEnd(const mooncake::ObjectMeta& meta,
                              mooncake::ReplicaType type) = 0;
    virtual VoidResult PutRevoke(const std::string& key,
                                 mooncake::ReplicaType type) = 0;
    virtual VoidResults BatchPutEnd(
        const std::vector<mooncake::ObjectMeta>& metas) = 0;
    virtual VoidResults BatchPutRevoke(
        const std::vector<std::string>& keys) = 0;
    virtual StartResult UpsertStart(
        const std::string& key, const std::vector<size_t>& lengths,
        const mooncake::ReplicateConfig& config) = 0;
    virtual StartResults BatchUpsertStart(
        const std::vector<std::string>& keys,
        const std::vector<std::vector<uint64_t>>& lengths,
        const mooncake::ReplicateConfig& config) = 0;
    virtual VoidResult UpsertEnd(const mooncake::ObjectMeta& meta,
                                 mooncake::ReplicaType type) = 0;
    virtual VoidResult UpsertRevoke(const std::string& key,
                                    mooncake::ReplicaType type) = 0;
    virtual VoidResults BatchUpsertEnd(
        const std::vector<mooncake::ObjectMeta>& metas) = 0;
    virtual VoidResults BatchUpsertRevoke(
        const std::vector<std::string>& keys) = 0;
    virtual GetResult GetReplicaList(const std::string& key) = 0;
    virtual GetResults BatchGetReplicaList(
        const std::vector<std::string>& keys) = 0;
    virtual VoidResult Remove(const std::string& key, bool force) = 0;
    virtual VoidResults BatchRemove(const std::vector<std::string>& keys,
                                    bool force) = 0;

   private:
    RpcHook hook_;
    RpcContext context_;
    uint64_t attempt_id_ = 0;
};
class MasterClientApi final : public MasterApi {
   public:
    explicit MasterClientApi(const mooncake::UUID& id,
                             std::string tenant = "default", RpcHook hook = {});
    ~MasterClientApi() override;
    void Disconnect() override;
    mooncake::ErrorCode Connect(const std::string& endpoint) override;
    VoidResult MountSegment(const mooncake::Segment& segment) override;
    VoidResult ReMountSegment(
        const std::vector<mooncake::Segment>& segments) override;
    VoidResult UnmountSegment(const mooncake::UUID& id) override;
    PingResult Ping() override;
    StartResult PutStart(const std::string& key,
                         const std::vector<size_t>& lengths,
                         const mooncake::ReplicateConfig& config) override;
    StartResults BatchPutStart(
        const std::vector<std::string>& keys,
        const std::vector<std::vector<uint64_t>>& lengths,
        const mooncake::ReplicateConfig& config) override;
    VoidResult PutEnd(const mooncake::ObjectMeta& meta,
                      mooncake::ReplicaType type) override;
    VoidResult PutRevoke(const std::string& key,
                         mooncake::ReplicaType type) override;
    VoidResults BatchPutEnd(
        const std::vector<mooncake::ObjectMeta>& metas) override;
    VoidResults BatchPutRevoke(const std::vector<std::string>& keys) override;
    StartResult UpsertStart(const std::string& key,
                            const std::vector<size_t>& lengths,
                            const mooncake::ReplicateConfig& config) override;
    StartResults BatchUpsertStart(
        const std::vector<std::string>& keys,
        const std::vector<std::vector<uint64_t>>& lengths,
        const mooncake::ReplicateConfig& config) override;
    VoidResult UpsertEnd(const mooncake::ObjectMeta& meta,
                         mooncake::ReplicaType type) override;
    VoidResult UpsertRevoke(const std::string& key,
                            mooncake::ReplicaType type) override;
    VoidResults BatchUpsertEnd(
        const std::vector<mooncake::ObjectMeta>& metas) override;
    VoidResults BatchUpsertRevoke(
        const std::vector<std::string>& keys) override;
    GetResult GetReplicaList(const std::string& key) override;
    GetResults BatchGetReplicaList(
        const std::vector<std::string>& keys) override;
    VoidResult Remove(const std::string& key, bool force) override;
    VoidResults BatchRemove(const std::vector<std::string>& keys,
                            bool force) override;

   private:
    std::unique_ptr<mooncake::MasterClient> client_;
    mooncake::UUID id_;
    std::string tenant_;
};
}  // namespace mock_client
