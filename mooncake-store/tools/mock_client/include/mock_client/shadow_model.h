#pragma once

#include <cstdint>
#include <map>
#include <string>
#include <string_view>
#include <vector>

#include "mock_client/deterministic_data.h"
#include "rpc_types.h"

namespace mock_client {

struct ReplicaSummary {
    mooncake::ReplicaStatus status;
    std::string segment_name;
    std::string segment_endpoint;
    uint64_t descriptor_id;
};

struct ShadowEntry {
    ObjectIdentity identity;
    bool exists;
    uint32_t expected_replica_count;
    std::string owner_client_id;
    std::string owner_segment;
    std::vector<ReplicaSummary> replicas;
    bool uncertain;
};

enum class ShadowObservation { Unknown, Absent, Present };

class ShadowModel {
   public:
    // Apply methods record a mutation known to have succeeded. A new mutation
    // must advance a key's expected generation by exactly one after its first
    // generation. They reject uncertain keys; only ApplyUpsert/ApplyPut
    // advance the expected generation.
    bool ApplyPut(const ObjectIdentity& identity,
                  uint32_t expected_replica_count,
                  std::string_view owner_client_id = {},
                  std::string_view owner_segment = {});
    bool ApplyUpsert(const ObjectIdentity& identity,
                     uint32_t expected_replica_count,
                     std::string_view owner_client_id = {},
                     std::string_view owner_segment = {});
    bool ApplyRemove(std::string_view key);

    // MarkUnknown freezes a key after a mutation with unknown outcome.
    void MarkUnknown(std::string_view key);
    bool CanMutate(std::string_view key) const;

    // An observed absence fully resolves uncertainty. Presence alone does not
    // identify the expected generation/checksum and therefore stays frozen;
    // use ReconcileReplicaList to confirm a complete known present state.
    bool ResolveUnknown(std::string_view key,
                        ShadowObservation observed_state);

    const ShadowEntry* Lookup(std::string_view key) const;
    std::vector<std::string> KeysForValidation() const;

    // Compare a successful metadata query with the expected state and retain
    // only normalized replica summaries (never the input descriptors).
    bool ReconcileReplicaList(
        std::string_view key, const mooncake::GetReplicaListResponse& response);

   private:
    static ReplicaSummary Summarize(
        const mooncake::Replica::Descriptor& descriptor);
    static bool SameReplicaSummary(const ReplicaSummary& expected,
                                   const ReplicaSummary& observed);
    static void SetAbsent(ShadowEntry& entry);

    std::map<std::string, ShadowEntry, std::less<>> entries_;
};

}  // namespace mock_client
