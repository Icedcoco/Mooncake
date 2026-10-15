#include "mock_client/operation_executor.h"
#include <algorithm>
#include <limits>
#include <set>
namespace mock_client {
namespace {
using mooncake::ErrorCode;
const char* Name(LogicalOperation op) {
    switch (op) {
        case LogicalOperation::Put:
            return "put";
        case LogicalOperation::Upsert:
            return "upsert";
        case LogicalOperation::Get:
            return "get";
        case LogicalOperation::Remove:
            return "remove";
    }
    return "none";
}
}  // namespace
bool OperationResult::ok() const {
    return !transport_unknown &&
           std::all_of(items.begin(), items.end(), [](const auto& item) {
               return item.error == ErrorCode::OK && !item.validation_mismatch;
           });
}
OperationExecutor::OperationExecutor(MasterApi& a, MockClientConfig c,
                                     ShadowModel& s)
    : api_(a), config_(std::move(c)), shadow_(s) {}
void OperationExecutor::Unknown(
    const ObjectIdentity& identity,
    const std::vector<mooncake::Replica::Descriptor>& descriptors,
    bool remove) {
    pending_.insert_or_assign(
        identity.key, Pending{Allocation{identity, descriptors}, remove});
    shadow_.MarkUnknown(identity.key);
}
bool OperationExecutor::ValidDescriptors(const Allocation& a,
                                         bool complete) const {
    if (a.descriptors.size() != config_.replication.replica_num ||
        a.descriptors.empty())
        return false;
    std::set<uint64_t> ids;
    for (const auto& d : a.descriptors) {
        if (!d.is_memory_replica() || !ids.insert(d.id).second ||
            (complete && d.status != mooncake::ReplicaStatus::COMPLETE))
            return false;
        const auto& b = d.get_memory_descriptor().buffer_descriptor;
        if (b.size_ != a.identity.logical_size || !b.size_) return false;
        bool range = false;
        for (const auto& client : config_.clients)
            for (const auto& s : client.segments) {
                if (b.transport_endpoint_ == s.transport_endpoint &&
                    b.protocol_ == s.protocol &&
                    b.buffer_address_ >= s.base_address &&
                    b.buffer_address_ - s.base_address <= s.capacity_bytes &&
                    b.size_ <=
                        s.capacity_bytes - (b.buffer_address_ - s.base_address))
                    range = true;
            }
        if (!range) return false;
    }
    return true;
}
bool OperationExecutor::Matches(
    const Allocation& expected,
    const mooncake::GetReplicaListResponse& response) const {
    Allocation observed{expected.identity, response.replicas};
    if (response.object_checksum != expected.identity.checksum ||
        !ValidDescriptors(observed, true))
        return false;
    if (expected.descriptors.empty())
        return expected.identity.checksum.has_value();
    for (const auto& e : expected.descriptors) {
        auto it =
            std::find_if(response.replicas.begin(), response.replicas.end(),
                         [&](const auto& d) { return d.id == e.id; });
        if (it == response.replicas.end() || !e.is_memory_replica())
            return false;
        const auto& a = e.get_memory_descriptor().buffer_descriptor;
        const auto& b = it->get_memory_descriptor().buffer_descriptor;
        if (a.size_ != b.size_ || a.buffer_address_ != b.buffer_address_ ||
            a.protocol_ != b.protocol_ ||
            a.transport_endpoint_ != b.transport_endpoint_)
            return false;
    }
    return true;
}
bool OperationExecutor::Apply(const Allocation& a, bool upsert) {
    const auto owner =
        config_.clients.empty() ? std::string{} : config_.clients.front().uuid;
    bool ok = upsert ? shadow_.ApplyUpsert(
                           a.identity, config_.replication.replica_num, owner)
                     : shadow_.ApplyPut(a.identity,
                                        config_.replication.replica_num, owner);
    if (ok) allocations_.insert_or_assign(a.identity.key, a);
    return ok;
}
ItemResult OperationExecutor::Observe(const std::string& key,
                                      const GetResult& response) {
    ItemResult item{key};
    auto pending = pending_.find(key);
    const auto* entry = shadow_.Lookup(key);
    if (!response) {
        if (response.error() == ErrorCode::OBJECT_NOT_FOUND) {
            if (pending != pending_.end() || (entry && entry->uncertain)) {
                shadow_.ResolveUnknown(key, ShadowObservation::Absent);
                pending_.erase(key);
                allocations_.erase(key);
                return item;
            }
            if (entry && !entry->exists) return item;
            item.validation_mismatch = true;
        }
        item.error = response.error();
        return item;
    }
    if (entry && entry->uncertain &&
        std::any_of(response->replicas.begin(), response->replicas.end(),
                    [](const auto& descriptor) {
                        return descriptor.status !=
                               mooncake::ReplicaStatus::COMPLETE;
                    })) {
        item.error = ErrorCode::REPLICA_IS_NOT_READY;
        return item;
    }
    // A completed pending write can advance exactly once after a lost End
    // reply.
    if (pending != pending_.end() && !pending->second.remove &&
        Matches(pending->second.allocation, *response)) {
        const auto a = pending->second.allocation;
        ShadowModel candidate = shadow_;
        candidate.ResolveUnknown(key, ShadowObservation::Absent);
        if (candidate.ApplyUpsert(a.identity,
                                  config_.replication.replica_num)) {
            shadow_ = std::move(candidate);
            allocations_.insert_or_assign(key, a);
            pending_.erase(pending);
            return item;
        }
    }
    auto known = allocations_.find(key);
    if (known != allocations_.end() && entry && entry->exists &&
        Matches(known->second, *response)) {
        if (shadow_.ReconcileReplicaList(key, *response)) {
            pending_.erase(key);
            return item;
        }
    }
    item.error = ErrorCode::INVALID_REPLICA;
    item.validation_mismatch = true;
    return item;
}
void OperationExecutor::Write(const OperationRequest& r,
                              const std::vector<size_t>& indices,
                              OperationResult& out) {
    const bool upsert = r.selection.operation == LogicalOperation::Upsert;
    const bool batch = r.selection.form == OperationForm::Batch;
    std::vector<std::string> keys;
    std::vector<std::vector<uint64_t>> lengths;
    for (auto i : indices) {
        keys.push_back(r.objects[i].key);
        lengths.push_back({r.objects[i].logical_size});
    }
    StartResults starts;
    if (batch)
        starts = api_.InvokeRpc(
            upsert ? "BatchUpsertStart" : "BatchPutStart", keys.size(), [&] {
                return upsert ? api_.BatchUpsertStart(keys, lengths,
                                                      config_.replication)
                              : api_.BatchPutStart(keys, lengths,
                                                   config_.replication);
            });
    else
        starts.push_back(
            api_.InvokeRpc(upsert ? "UpsertStart" : "PutStart", 1, [&] {
                const std::vector<size_t> size{static_cast<size_t>(
                    r.objects[indices.front()].logical_size)};
                return upsert ? api_.UpsertStart(keys.front(), size,
                                                 config_.replication)
                              : api_.PutStart(keys.front(), size,
                                              config_.replication);
            }));
    std::vector<size_t> accepted, revoke;
    std::vector<mooncake::ObjectMeta> metas;
    for (size_t j = 0; j < indices.size(); ++j) {
        auto i = indices[j];
        const auto& object = r.objects[i];
        if (!starts[j]) {
            out.items[i].error = starts[j].error();
            if (IsTransportError(starts[j].error())) {
                Unknown(object, {}, false);
                out.transport_unknown = true;
            }
            continue;
        }
        Allocation a{object, *starts[j]};
        if (!ValidDescriptors(a, false)) {
            out.items[i].error = ErrorCode::INVALID_REPLICA;
            out.items[i].validation_mismatch = true;
            revoke.push_back(j);
            continue;
        }
        accepted.push_back(j);
        metas.push_back({object.key, object.checksum});
    }
    VoidResults ends;
    if (!accepted.empty()) {
        if (batch)
            ends = api_.InvokeRpc(upsert ? "BatchUpsertEnd" : "BatchPutEnd",
                                  metas.size(), [&] {
                                      return upsert ? api_.BatchUpsertEnd(metas)
                                                    : api_.BatchPutEnd(metas);
                                  });
        else
            ends.push_back(
                api_.InvokeRpc(upsert ? "UpsertEnd" : "PutEnd", 1, [&] {
                    return upsert
                               ? api_.UpsertEnd(metas.front(),
                                                mooncake::ReplicaType::MEMORY)
                               : api_.PutEnd(metas.front(),
                                             mooncake::ReplicaType::MEMORY);
                }));
    }
    for (size_t j = 0; j < accepted.size(); ++j) {
        const auto position = accepted[j], i = indices[position];
        const auto& object = r.objects[i];
        if (ends[j]) {
            if (!Apply({object, *starts[position]}, upsert)) {
                out.items[i].error = ErrorCode::INTERNAL_ERROR;
                Unknown(object, *starts[position], false);
                out.transport_unknown = true;
            }
        } else {
            out.items[i].error = ends[j].error();
            if (IsTransportError(ends[j].error())) {
                Unknown(object, *starts[position], false);
                out.transport_unknown = true;
            } else
                revoke.push_back(position);
        }
    }
    if (revoke.empty()) return;
    std::vector<std::string> revoke_keys;
    for (auto j : revoke) revoke_keys.push_back(keys[j]);
    VoidResults revoked;
    if (batch)
        revoked = api_.InvokeRpc(
            upsert ? "BatchUpsertRevoke" : "BatchPutRevoke", revoke_keys.size(),
            [&] {
                return upsert ? api_.BatchUpsertRevoke(revoke_keys)
                              : api_.BatchPutRevoke(revoke_keys);
            });
    else
        revoked.push_back(
            api_.InvokeRpc(upsert ? "UpsertRevoke" : "PutRevoke", 1, [&] {
                return upsert ? api_.UpsertRevoke(revoke_keys.front(),
                                                  mooncake::ReplicaType::MEMORY)
                              : api_.PutRevoke(revoke_keys.front(),
                                               mooncake::ReplicaType::MEMORY);
            }));
    for (size_t k = 0; k < revoke.size(); ++k) {
        const auto i = indices[revoke[k]];
        if (revoked[k]) {
            // MEMORY revoke deletes processing replicas, including the old
            // replicas reused by UpsertStart. It does not restore old data.
            shadow_.ApplyRemove(r.objects[i].key);
            allocations_.erase(r.objects[i].key);
        } else {
            auto j = revoke[k], i = indices[j];
            Unknown(r.objects[i], *starts[j], false);
            out.items[i].error = revoked[k].error();
            out.transport_unknown = true;
        }
    }
}
OperationResult OperationExecutor::Execute(const OperationRequest& r) {
    OperationResult out;
    out.request_id = "request-" + std::to_string(++request_sequence_);
    out.logical_ops = r.purpose == "workload" ? r.objects.size() : 0;
    out.batch_items =
        r.selection.form == OperationForm::Batch ? r.objects.size() : 0;
    api_.SetRpcContext(
        {out.request_id, r.purpose, Name(r.selection.operation),
         r.selection.form == OperationForm::Batch ? "batch" : "single"});
    std::vector<size_t> active;
    std::set<std::string> seen;
    const bool mutation = r.selection.operation != LogicalOperation::Get;
    for (size_t i = 0; i < r.objects.size(); ++i) {
        const auto& object = r.objects[i];
        out.items.push_back({object.key});
        bool valid = !object.key.empty() && seen.insert(object.key).second;
        if (mutation) {
            const auto* previous = shadow_.Lookup(object.key);
            if (!shadow_.CanMutate(object.key)) {
                out.items.back().error =
                    ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS;
                continue;
            }
            if (r.selection.operation != LogicalOperation::Remove) {
                valid =
                    valid && object.generation > 0 && object.logical_size > 0 &&
                    object.logical_size <= std::numeric_limits<size_t>::max() &&
                    config_.replication.replica_num > 0 &&
                    !config_.replication.nof_replica_num &&
                    !config_.replication.dfs_replica_num;
                if (previous)
                    valid =
                        valid &&
                        previous->identity.generation !=
                            std::numeric_limits<uint64_t>::max() &&
                        object.generation == previous->identity.generation + 1;
            }
        }
        if (!valid) {
            out.items.back().error = ErrorCode::INVALID_PARAMS;
            continue;
        }
        active.push_back(i);
    }
    if (active.empty()) return out;
    const bool batch = r.selection.form == OperationForm::Batch;
    const auto op = r.selection.operation;
    auto run = [&](const std::vector<size_t>& indices) {
        if (op == LogicalOperation::Put || op == LogicalOperation::Upsert) {
            Write(r, indices, out);
            return;
        }
        std::vector<std::string> keys;
        for (auto i : indices) keys.push_back(r.objects[i].key);
        if (op == LogicalOperation::Get) {
            GetResults responses;
            if (batch)
                responses = api_.InvokeRpc(
                    "BatchGetReplicaList", keys.size(),
                    [&] { return api_.BatchGetReplicaList(keys); });
            else
                responses.push_back(api_.InvokeRpc("GetReplicaList", 1, [&] {
                    return api_.GetReplicaList(keys.front());
                }));
            for (size_t j = 0; j < indices.size(); ++j) {
                out.items[indices[j]] = Observe(keys[j], responses[j]);
                if (IsTransportError(out.items[indices[j]].error))
                    out.transport_unknown = true;
            }
        } else {
            VoidResults responses;
            if (batch)
                responses = api_.InvokeRpc("BatchRemove", keys.size(), [&] {
                    return api_.BatchRemove(keys, true);
                });
            else
                responses.push_back(api_.InvokeRpc("Remove", 1, [&] {
                    return api_.Remove(keys.front(), true);
                }));
            for (size_t j = 0; j < indices.size(); ++j) {
                const auto i = indices[j];
                if (responses[j] ||
                    responses[j].error() == ErrorCode::OBJECT_NOT_FOUND) {
                    shadow_.ApplyRemove(keys[j]);
                    allocations_.erase(keys[j]);
                } else {
                    out.items[i].error = responses[j].error();
                    if (IsTransportError(responses[j].error())) {
                        Unknown(r.objects[i], {}, true);
                        out.transport_unknown = true;
                    }
                }
            }
        }
    };
    if (batch)
        run(active);
    else
        for (auto i : active) run({i});
    return out;
}
OperationResult OperationExecutor::ValidateKeys(
    const std::vector<std::string>& keys, const std::string& purpose) {
    OperationResult out;
    for (size_t offset = 0; offset < keys.size(); offset += 128) {
        OperationRequest r{
            {LogicalOperation::Get, OperationForm::Batch, 1, {1, 128}},
            {},
            purpose};
        for (size_t i = offset; i < std::min(keys.size(), offset + 128); ++i)
            r.objects.push_back({keys[i], 0, 0, {}});
        auto part = Execute(r);
        out.items.insert(out.items.end(), part.items.begin(), part.items.end());
        out.transport_unknown |= part.transport_unknown;
        out.batch_items += part.batch_items;
        out.request_id = part.request_id;
    }
    return out;
}
OperationResult OperationExecutor::ValidateAll(std::string purpose) {
    return ValidateKeys(shadow_.KeysForValidation(), purpose);
}
OperationResult OperationExecutor::ReconcileUnknown(std::string purpose) {
    std::vector<std::string> keys;
    for (const auto& key : shadow_.KeysForValidation()) {
        const auto* entry = shadow_.Lookup(key);
        if (entry && entry->uncertain) keys.push_back(key);
    }
    return ValidateKeys(keys, purpose);
}
OperationResult OperationExecutor::CapabilityProbe(
    const ObjectIdentity& identity) {
    auto put =
        Execute({{LogicalOperation::Put, OperationForm::Single, 1, {1, 1}},
                 {identity},
                 "capability_probe"});
    if (!put.ok()) return put;
    auto get =
        Execute({{LogicalOperation::Get, OperationForm::Single, 1, {1, 1}},
                 {identity},
                 "capability_probe"});
    auto remove =
        Execute({{LogicalOperation::Remove, OperationForm::Single, 1, {1, 1}},
                 {identity},
                 "capability_probe"});
    if (!get.ok()) return get;
    return remove;
}
}  // namespace mock_client
