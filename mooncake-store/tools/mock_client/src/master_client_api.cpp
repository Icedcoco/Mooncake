#include "mock_client/master_api.h"
#include "master_client.h"
namespace mock_client {
MasterClientApi::MasterClientApi(const mooncake::UUID& id, std::string tenant,
                                 RpcHook hook)
    : id_(id), tenant_(std::move(tenant)) {
    SetRpcHook(std::move(hook));
}
MasterClientApi::~MasterClientApi() = default;
void MasterClientApi::Disconnect() { client_.reset(); }
mooncake::ErrorCode MasterClientApi::Connect(const std::string& endpoint) {
    Disconnect();
    client_ = std::make_unique<mooncake::MasterClient>(id_, nullptr, tenant_);
    // Bounded endpoint readiness probes; the session owns endpoint failover.
    client_->EnableHaConnectionPolicy();
    return client_->Connect(endpoint);
}
VoidResult MasterClientApi::MountSegment(const mooncake::Segment& segment) {
    if (!client_) return tl::make_unexpected(mooncake::ErrorCode::RPC_FAIL);
    return client_->MountSegment(segment);
}
VoidResult MasterClientApi::ReMountSegment(
    const std::vector<mooncake::Segment>& segments) {
    if (!client_) return tl::make_unexpected(mooncake::ErrorCode::RPC_FAIL);
    return client_->ReMountSegment(segments);
}
VoidResult MasterClientApi::UnmountSegment(const mooncake::UUID& id) {
    if (!client_) return tl::make_unexpected(mooncake::ErrorCode::RPC_FAIL);
    return client_->UnmountSegment(id);
}
PingResult MasterClientApi::Ping() {
    if (!client_) return tl::make_unexpected(mooncake::ErrorCode::RPC_FAIL);
    return client_->Ping();
}
StartResult MasterClientApi::PutStart(const std::string& key,
                                      const std::vector<size_t>& lengths,
                                      const mooncake::ReplicateConfig& config) {
    if (!client_) return tl::make_unexpected(mooncake::ErrorCode::RPC_FAIL);
    return client_->PutStart(key, lengths, config);
}
StartResults MasterClientApi::BatchPutStart(
    const std::vector<std::string>& keys,
    const std::vector<std::vector<uint64_t>>& lengths,
    const mooncake::ReplicateConfig& config) {
    if (!client_)
        return StartResults(keys.size(),
                            tl::make_unexpected(mooncake::ErrorCode::RPC_FAIL));
    return client_->BatchPutStart(keys, lengths, config);
}
VoidResult MasterClientApi::PutEnd(const mooncake::ObjectMeta& meta,
                                   mooncake::ReplicaType type) {
    if (!client_) return tl::make_unexpected(mooncake::ErrorCode::RPC_FAIL);
    return client_->PutEnd(meta, type);
}
VoidResult MasterClientApi::PutRevoke(const std::string& key,
                                      mooncake::ReplicaType type) {
    if (!client_) return tl::make_unexpected(mooncake::ErrorCode::RPC_FAIL);
    return client_->PutRevoke(key, type);
}
VoidResults MasterClientApi::BatchPutEnd(
    const std::vector<mooncake::ObjectMeta>& metas) {
    if (!client_)
        return VoidResults(metas.size(),
                           tl::make_unexpected(mooncake::ErrorCode::RPC_FAIL));
    return client_->BatchPutEnd(metas, mooncake::ReplicaType::MEMORY);
}
VoidResults MasterClientApi::BatchPutRevoke(
    const std::vector<std::string>& keys) {
    if (!client_)
        return VoidResults(keys.size(),
                           tl::make_unexpected(mooncake::ErrorCode::RPC_FAIL));
    return client_->BatchPutRevoke(keys, mooncake::ReplicaType::MEMORY);
}
StartResult MasterClientApi::UpsertStart(
    const std::string& key, const std::vector<size_t>& lengths,
    const mooncake::ReplicateConfig& config) {
    if (!client_) return tl::make_unexpected(mooncake::ErrorCode::RPC_FAIL);
    return client_->UpsertStart(key, lengths, config);
}
StartResults MasterClientApi::BatchUpsertStart(
    const std::vector<std::string>& keys,
    const std::vector<std::vector<uint64_t>>& lengths,
    const mooncake::ReplicateConfig& config) {
    if (!client_)
        return StartResults(keys.size(),
                            tl::make_unexpected(mooncake::ErrorCode::RPC_FAIL));
    return client_->BatchUpsertStart(keys, lengths, config);
}
VoidResult MasterClientApi::UpsertEnd(const mooncake::ObjectMeta& meta,
                                      mooncake::ReplicaType type) {
    if (!client_) return tl::make_unexpected(mooncake::ErrorCode::RPC_FAIL);
    return client_->UpsertEnd(meta, type);
}
VoidResult MasterClientApi::UpsertRevoke(const std::string& key,
                                         mooncake::ReplicaType type) {
    if (!client_) return tl::make_unexpected(mooncake::ErrorCode::RPC_FAIL);
    return client_->UpsertRevoke(key, type);
}
VoidResults MasterClientApi::BatchUpsertEnd(
    const std::vector<mooncake::ObjectMeta>& metas) {
    if (!client_)
        return VoidResults(metas.size(),
                           tl::make_unexpected(mooncake::ErrorCode::RPC_FAIL));
    return client_->BatchUpsertEnd(metas);
}
VoidResults MasterClientApi::BatchUpsertRevoke(
    const std::vector<std::string>& keys) {
    if (!client_)
        return VoidResults(keys.size(),
                           tl::make_unexpected(mooncake::ErrorCode::RPC_FAIL));
    return client_->BatchUpsertRevoke(keys);
}
GetResult MasterClientApi::GetReplicaList(const std::string& key) {
    if (!client_) return tl::make_unexpected(mooncake::ErrorCode::RPC_FAIL);
    return client_->GetReplicaList(key);
}
GetResults MasterClientApi::BatchGetReplicaList(
    const std::vector<std::string>& keys) {
    if (!client_)
        return GetResults(keys.size(),
                          tl::make_unexpected(mooncake::ErrorCode::RPC_FAIL));
    return client_->BatchGetReplicaList(keys);
}
VoidResult MasterClientApi::Remove(const std::string& key, bool force) {
    if (!client_) return tl::make_unexpected(mooncake::ErrorCode::RPC_FAIL);
    return client_->Remove(key, force);
}
VoidResults MasterClientApi::BatchRemove(const std::vector<std::string>& keys,
                                         bool force) {
    if (!client_)
        return VoidResults(keys.size(),
                           tl::make_unexpected(mooncake::ErrorCode::RPC_FAIL));
    return client_->BatchRemove(keys, force);
}
}  // namespace mock_client
