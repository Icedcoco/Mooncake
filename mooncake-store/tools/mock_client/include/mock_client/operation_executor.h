#pragma once
#include <map>
#include "mock_client/master_api.h"
#include "mock_client/config.h"
#include "mock_client/shadow_model.h"
namespace mock_client {
struct OperationRequest {
    OperationSelection selection;
    std::vector<ObjectIdentity> objects;
    std::string purpose;
};
struct ItemResult {
    std::string key;
    mooncake::ErrorCode error = mooncake::ErrorCode::OK;
    bool validation_mismatch = false;
};
struct OperationResult {
    std::string request_id;
    uint64_t logical_ops = 0, batch_items = 0;
    std::vector<ItemResult> items;
    bool transport_unknown = false;
    bool ok() const;
};
// Each worker owns its executor/API/shadow. Session transitions must quiesce
// its worker; no mutation is replayed when a response is uncertain.
class OperationExecutor {
   public:
    OperationExecutor(MasterApi& api, MockClientConfig config,
                      ShadowModel& shadow);
    OperationResult Execute(const OperationRequest& request);
    OperationResult ValidateAll(std::string purpose = "validation");
    OperationResult ReconcileUnknown(std::string purpose = "validation");
    OperationResult CapabilityProbe(const ObjectIdentity& identity);
    ShadowModel& shadow_model() { return shadow_; }

   private:
    struct Allocation {
        ObjectIdentity identity;
        std::vector<mooncake::Replica::Descriptor> descriptors;
    };
    struct Pending {
        Allocation allocation;
        bool remove = false;
    };
    bool Matches(const Allocation& allocation,
                 const mooncake::GetReplicaListResponse& response) const;
    bool ValidDescriptors(const Allocation& allocation, bool complete) const;
    bool Apply(const Allocation& allocation, bool upsert);
    ItemResult Observe(const std::string& key, const GetResult& response);
    OperationResult ValidateKeys(const std::vector<std::string>& keys,
                                 const std::string& purpose);
    void Write(const OperationRequest& request,
               const std::vector<size_t>& indices, OperationResult& result);
    void Unknown(const ObjectIdentity& identity,
                 const std::vector<mooncake::Replica::Descriptor>& descriptors,
                 bool remove);
    MasterApi& api_;
    MockClientConfig config_;
    ShadowModel& shadow_;
    std::map<std::string, Allocation, std::less<>> allocations_;
    std::map<std::string, Pending, std::less<>> pending_;
    uint64_t request_sequence_ = 0;
};
}  // namespace mock_client
