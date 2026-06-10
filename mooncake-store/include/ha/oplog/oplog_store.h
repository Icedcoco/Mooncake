// mooncake-store/include/oplog_store.h
#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "ha/oplog/oplog_change_notifier.h"
#include "ha/oplog/oplog_manager.h"
#include "types.h"

namespace mooncake {

// Normalize and validate cluster_id for OpLog key prefix construction.
// Strips trailing slashes, then validates the remaining string.
// Returns true if valid (or empty after normalization), false otherwise.
inline bool NormalizeAndValidateClusterId(std::string& cluster_id) {
    while (!cluster_id.empty() && cluster_id.back() == '/') {
        cluster_id.pop_back();
    }
    return cluster_id.empty() || IsValidClusterIdComponent(cluster_id);
}

// Abstract interface for OpLog persistent storage.
// Implementations: EtcdOpLogStore, (future) HdfsOpLogStore, etc.
class OpLogStore {
   public:
    virtual ~OpLogStore() = default;
    virtual ErrorCode Init() = 0;

    // Write
    virtual ErrorCode WriteOpLog(const OpLogEntry& entry, bool sync = true) = 0;

    // Read
    virtual ErrorCode ReadOpLog(uint64_t sequence_id, OpLogEntry& entry) = 0;
    virtual ErrorCode ReadOpLogSince(uint64_t start_sequence_id, size_t limit,
                                     std::vector<OpLogEntry>& entries) = 0;

    // Sequence ID management
    virtual ErrorCode GetLatestSequenceId(uint64_t& sequence_id) = 0;
    virtual ErrorCode GetMaxSequenceId(uint64_t& sequence_id) = 0;
    virtual ErrorCode UpdateLatestSequenceId(uint64_t sequence_id) = 0;

    // Snapshot
    virtual ErrorCode RecordSnapshotSequenceId(const std::string& snapshot_id,
                                               uint64_t sequence_id) = 0;
    virtual ErrorCode GetSnapshotSequenceId(const std::string& snapshot_id,
                                            uint64_t& sequence_id) = 0;

    // Cleanup
    virtual ErrorCode CleanupOpLogBefore(uint64_t before_sequence_id) = 0;

    // ------------------------------------------------------------------
    // Stage 2: durable batch record API.
    //
    // These methods describe the *future* batched OpLog contract that
    // Stage 3 will implement on top of etcd, that Stage 4 will drive from
    // the LogWriter, and that Stage 5 will replay from on standby. They are
    // intentionally NOT pure virtual: backends that have not opted in
    // (localfs, legacy etcd sequence schema, the in-memory mock used in
    // unit tests, etc.) inherit a safe default that refuses with
    // UNAVAILABLE_IN_CURRENT_MODE. This keeps the basic-available HA gate
    // honest — `ha_oplog_format=batched` cannot silently fall through to a
    // backend that does not actually support batched durable history.
    // ------------------------------------------------------------------

    // Append a single durable batch record. Implementations must enforce
    // ordering (batch_id == previous_latest_batch + 1), atomicity (the
    // batch payload and any "latest" pointer advance together), and
    // idempotency for retries with the same payload.
    virtual ErrorCode AppendBatch(const OpLogBatchRecord& batch) {
        (void)batch;
        return ErrorCode::UNAVAILABLE_IN_CURRENT_MODE;
    }

    // Read a specific batch by (shard_id, batch_id). Implementations should
    // return OPLOG_ENTRY_NOT_FOUND when the batch does not exist.
    virtual ErrorCode ReadBatch(uint32_t shard_id, uint64_t batch_id,
                                OpLogBatchRecord& batch) {
        (void)shard_id;
        (void)batch_id;
        (void)batch;
        return ErrorCode::UNAVAILABLE_IN_CURRENT_MODE;
    }

    // Read up to `limit` batches with batch_id strictly greater than
    // `start_batch_id`, in batch_id-ascending order. Used by standby replay
    // to make forward progress.
    virtual ErrorCode ReadBatchesSince(uint32_t shard_id,
                                       uint64_t start_batch_id, size_t limit,
                                       std::vector<OpLogBatchRecord>& batches) {
        (void)shard_id;
        (void)start_batch_id;
        (void)limit;
        (void)batches;
        return ErrorCode::UNAVAILABLE_IN_CURRENT_MODE;
    }

    // Return the largest batch_id durably committed for the shard. When the
    // shard exists but has no batches yet, implementations should return OK
    // with batch_id=0; only true backend errors should produce non-OK status.
    virtual ErrorCode GetLatestBatchId(uint32_t shard_id, uint64_t& batch_id) {
        (void)shard_id;
        (void)batch_id;
        return ErrorCode::UNAVAILABLE_IN_CURRENT_MODE;
    }

    // Create a change notifier for this store.
    // Each backend provides its own notifier (e.g., etcd watch, polling).
    // Returns nullptr if the backend does not support change notification.
    virtual std::unique_ptr<OpLogChangeNotifier> CreateChangeNotifier(
        const std::string& cluster_id) {
        (void)cluster_id;
        return nullptr;
    }
};

}  // namespace mooncake
