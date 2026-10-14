#include "mock_client/shadow_model.h"

#include <limits>
#include <optional>
#include <utility>

namespace mock_client {
namespace {

uint64_t GenerationAfter(uint64_t current) {
    if (current == std::numeric_limits<uint64_t>::max()) {
        return current;
    }
    return current + 1;
}

bool HasNextGeneration(const ShadowEntry& entry,
                       const ObjectIdentity& identity) {
    const auto expected = GenerationAfter(entry.identity.generation);
    return expected != entry.identity.generation &&
           identity.generation == expected;
}

}  // namespace

bool ShadowModel::ApplyPut(const ObjectIdentity& identity,
                           uint32_t expected_replica_count,
                           std::string_view owner_client_id,
                           std::string_view owner_segment) {
    if (identity.key.empty() || identity.generation == 0 ||
        !CanMutate(identity.key)) {
        return false;
    }

    const auto existing = entries_.find(identity.key);
    if (existing != entries_.end() &&
        !HasNextGeneration(existing->second, identity)) {
        return false;
    }

    ShadowEntry entry{identity,
                      true,
                      expected_replica_count,
                      std::string(owner_client_id),
                      std::string(owner_segment),
                      {},
                      false};
    entries_.insert_or_assign(identity.key, std::move(entry));
    return true;
}

bool ShadowModel::ApplyUpsert(const ObjectIdentity& identity,
                              uint32_t expected_replica_count,
                              std::string_view owner_client_id,
                              std::string_view owner_segment) {
    if (identity.key.empty() || identity.generation == 0 ||
        !CanMutate(identity.key)) {
        return false;
    }

    const auto existing = entries_.find(identity.key);
    if (existing != entries_.end() &&
        !HasNextGeneration(existing->second, identity)) {
        return false;
    }

    ShadowEntry entry{identity,
                      true,
                      expected_replica_count,
                      std::string(owner_client_id),
                      std::string(owner_segment),
                      {},
                      false};
    entries_.insert_or_assign(identity.key, std::move(entry));
    return true;
}

bool ShadowModel::ApplyRemove(std::string_view key) {
    if (key.empty() || !CanMutate(key)) {
        return false;
    }

    auto [it, inserted] = entries_.try_emplace(
        std::string(key),
        ShadowEntry{ObjectIdentity{std::string(key), 0, 0, std::nullopt},
                    false, 0, {}, {}, {}, false});
    if (!inserted) {
        SetAbsent(it->second);
    }
    return true;
}

void ShadowModel::MarkUnknown(std::string_view key) {
    if (key.empty()) {
        return;
    }
    auto [it, inserted] = entries_.try_emplace(
        std::string(key),
        ShadowEntry{ObjectIdentity{std::string(key), 0, 0, std::nullopt},
                    false, 0, {}, {}, {}, true});
    if (!inserted) {
        it->second.uncertain = true;
    }
}

bool ShadowModel::CanMutate(std::string_view key) const {
    const auto it = entries_.find(key);
    return it == entries_.end() || !it->second.uncertain;
}

bool ShadowModel::ResolveUnknown(std::string_view key,
                                 ShadowObservation observed_state) {
    const auto it = entries_.find(key);
    if (it == entries_.end() || !it->second.uncertain) {
        return false;
    }

    if (observed_state == ShadowObservation::Absent) {
        SetAbsent(it->second);
        return true;
    }
    // Presence by itself cannot distinguish an unapplied mutation from one
    // that changed the generation, size, or checksum.
    return false;
}

const ShadowEntry* ShadowModel::Lookup(std::string_view key) const {
    const auto it = entries_.find(key);
    return it == entries_.end() ? nullptr : &it->second;
}

std::vector<std::string> ShadowModel::KeysForValidation() const {
    std::vector<std::string> keys;
    keys.reserve(entries_.size());
    for (const auto& [key, entry] : entries_) {
        (void)entry;
        keys.push_back(key);
    }
    return keys;
}

bool ShadowModel::ReconcileReplicaList(
    std::string_view key, const mooncake::GetReplicaListResponse& response) {
    auto it = entries_.find(key);
    if (it == entries_.end()) {
        return false;
    }

    ShadowEntry& entry = it->second;
    const bool observed_present = !response.replicas.empty() ||
                                  response.object_checksum.has_value();
    if (!observed_present) {
        if (!entry.uncertain && entry.exists) {
            return false;
        }
        SetAbsent(entry);
        return true;
    }

    if (!entry.exists || response.replicas.size() !=
                             entry.expected_replica_count) {
        return false;
    }
    if (entry.identity.checksum.has_value() &&
        response.object_checksum != entry.identity.checksum) {
        return false;
    }

    std::vector<ReplicaSummary> observed;
    observed.reserve(response.replicas.size());
    for (const auto& descriptor : response.replicas) {
        observed.push_back(Summarize(descriptor));
    }
    if (!entry.replicas.empty()) {
        if (entry.replicas.size() != observed.size()) {
            return false;
        }
        for (std::size_t index = 0; index < observed.size(); ++index) {
            if (!SameReplicaSummary(entry.replicas[index], observed[index])) {
                return false;
            }
        }
    }

    entry.replicas = std::move(observed);
    entry.uncertain = false;
    return true;
}

ReplicaSummary ShadowModel::Summarize(
    const mooncake::Replica::Descriptor& descriptor) {
    ReplicaSummary summary{descriptor.status, {}, {}, descriptor.id};
    if (descriptor.is_memory_replica()) {
        summary.segment_endpoint =
            descriptor.get_memory_descriptor().buffer_descriptor
                .transport_endpoint_;
    } else if (descriptor.is_nof_replica()) {
        summary.segment_endpoint =
            descriptor.get_nof_descriptor().buffer_descriptor
                .transport_endpoint_;
    } else if (descriptor.is_local_disk_replica()) {
        summary.segment_endpoint =
            descriptor.get_local_disk_descriptor().transport_endpoint;
    }
    return summary;
}

bool ShadowModel::SameReplicaSummary(const ReplicaSummary& expected,
                                    const ReplicaSummary& observed) {
    return expected.status == observed.status &&
           (expected.segment_name.empty() ||
            expected.segment_name == observed.segment_name) &&
           (expected.segment_endpoint.empty() ||
            expected.segment_endpoint == observed.segment_endpoint) &&
           (expected.descriptor_id == 0 ||
            expected.descriptor_id == observed.descriptor_id);
}

void ShadowModel::SetAbsent(ShadowEntry& entry) {
    entry.exists = false;
    entry.expected_replica_count = 0;
    entry.owner_client_id.clear();
    entry.owner_segment.clear();
    entry.replicas.clear();
    entry.uncertain = false;
}

}  // namespace mock_client
