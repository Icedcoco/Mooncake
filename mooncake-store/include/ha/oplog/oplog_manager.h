#pragma once

#include <cstdint>
#include <deque>
#include <future>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <vector>

#include <ylt/util/tl/expected.hpp>

#include "ha/ha_types.h"
#include "types.h"

namespace mooncake {

// Forward declaration
class OpLogStore;
class OpLogLogWriter;

// Operation types for hot-standby replication.
// This is a minimal subset that can be extended later.
enum class OpType : uint8_t {
    PUT_END = 1,
    PUT_REVOKE = 2,
    REMOVE = 3,
    // Deprecated: LEASE_RENEW is intentionally not recorded in OpLog in the
    // current etcd-based hot-standby design (Standby relies on Primary DELETE
    // operations).
    LEASE_RENEW = 4,
    // Segment events (for standby segment registry)
    SEGMENT_MOUNT = 5,
    SEGMENT_UNMOUNT = 6,
    SEGMENT_UPDATE = 7,
};

/**
 * Payload for SEGMENT_MOUNT OpLog entry.
 */
struct SegmentMountOp {
    std::string segment_name;
    std::string transport_endpoint;
    uint64_t capacity{0};
    bool is_memory_segment{false};
    std::string file_path;  // empty for memory segments

    YLT_REFL(SegmentMountOp, segment_name, transport_endpoint, capacity,
             is_memory_segment, file_path);
};

/**
 * Payload for SEGMENT_UNMOUNT OpLog entry.
 */
struct SegmentUnmountOp {
    std::string transport_endpoint;

    YLT_REFL(SegmentUnmountOp, transport_endpoint);
};

/**
 * Payload for SEGMENT_UPDATE OpLog entry.
 */
struct SegmentUpdateOp {
    std::string segment_name;
    std::string transport_endpoint;
    uint64_t capacity{0};
    bool is_memory_segment{false};
    std::string file_path;

    YLT_REFL(SegmentUpdateOp, segment_name, transport_endpoint, capacity,
             is_memory_segment, file_path);
};

// A single operation log entry.
// Note: Payload contains JSON serialized MetadataPayload (defined in
// metadata_store.h) for PUT_END operations, allowing Standby to restore
// complete metadata.
struct OpLogEntry {
    uint64_t sequence_id{0};   // Monotonically increasing global sequence
    uint64_t timestamp_ms{0};  // Logical timestamp in milliseconds
    OpType op_type{OpType::PUT_END};
    std::string tenant_id{"default"};  // Tenant identifier
    std::string object_key;            // Target object key
    std::string payload;               // Serialized extra data (optional)
    uint32_t checksum{0};  // Checksum of payload (implementation-defined)
    uint32_t prefix_hash{
        0};  // Hash of the entire key (for verification and optimization)

    // Stage 2 batch coordinates. These are 0-valued for entries produced by
    // the legacy single-entry path (durable truth is still sequence_id in
    // that mode). When the entry is produced by a batched LogWriter (Stage 4
    // and beyond), shard_id/batch_id identify the durable batch record this
    // entry belongs to, and local_index is the entry's 0-based position
    // within that batch. Older on-the-wire payloads without these fields
    // continue to parse, with the fields defaulting to 0 on the consumer.
    uint32_t shard_id{0};
    uint64_t batch_id{0};
    uint32_t local_index{0};
};

// Stage 2: durable position of a single OpLog entry inside the batched
// history. Once Stage 4 swaps the write path to batched mode, this triple
// becomes the canonical "where in the durable log" identifier, replacing
// the role currently played by sequence_id alone. The struct is intentionally
// a plain value type so it can be passed by value, compared, and stored
// without lifetime concerns.
struct OpLogBatchPosition {
    uint32_t shard_id{0};
    uint64_t batch_id{0};
    uint32_t local_index{0};
};

// Stage 2: durable batch record. One of these is what an etcd batch backend
// (Stage 3) writes as a single transactional record, what the LogWriter
// (Stage 4) builds in memory before flushing, and what standby replay
// (Stage 5) consumes one batch at a time. The batch_checksum covers the
// batch header plus the stable fields of every entry; per-entry payload
// integrity continues to be guarded by OpLogEntry::checksum.
struct OpLogBatchRecord {
    uint32_t shard_id{0};
    uint64_t batch_id{0};
    ViewVersionId producer_view_version{0};
    ha::OwnerToken owner_token;
    std::vector<OpLogEntry> entries;
    uint32_t batch_checksum{0};
};

// Stage 4: per-entry durability mode for batched writes. See plan §7.2.
enum class OpLogDurabilityMode {
    // Return EnqueueResult immediately. durable_result resolves when the
    // containing batch is durable (or has failed). PUT_END uses this.
    kAsync,
    // Block until the containing batch is durable (or has failed). REMOVE
    // and segment lifecycle ops use this so the caller knows the mutation
    // is durable before freeing/reusing memory.
    kWaitBatchDurable,
};

// Stage 4: result of a batched enqueue.
//   - position: the durable (shard_id, batch_id, local_index) triple this
//     entry will occupy once its containing batch is durable. Position is
//     assigned synchronously so callers can use it for tracing or
//     continuation without waiting for the future.
//   - durable_result: resolves to OK when the batch is durable, or to the
//     backend error code when the batch fails. For kWaitBatchDurable, the
//     future is already satisfied by the time Enqueue returns.
struct EnqueueResult {
    OpLogBatchPosition position;
    std::shared_future<ErrorCode> durable_result;
};

/**
 * @brief In-memory operation log manager.
 *
 * This class is intentionally simple: it keeps a bounded deque of OpLogEntry
 * and provides append / get-since primitives. It can later be extended to
 * or to spill to disk if needed. OpLog entries are persisted to the
 * configured OpLogStore backend (etcd, local filesystem, etc.).
 */
class OpLogManager {
   public:
    OpLogManager();

    // Set the OpLogStore for writing OpLog to persistent storage (optional).
    // If not set, OpLog will only be stored in memory buffer.
    void SetOpLogStore(std::shared_ptr<OpLogStore> oplog_store);

    // Stage 4: install a LogWriter and switch the manager to batched write
    // mode. When a LogWriter is set, Append() routes PUT_END through the
    // writer (kAsync), and AppendAndPersist() routes all dirty mutations
    // through the writer (kWaitBatchDurable). The legacy WriteOpLog path
    // is bypassed entirely while batched mode is active. The writer must
    // outlive the manager; setting a nullptr reverts to legacy mode.
    void SetLogWriter(std::shared_ptr<OpLogLogWriter> log_writer);

    // Stage 4: returns true when a LogWriter is currently driving writes.
    bool IsBatchedMode() const;

    // Stage 4: batched-mode Append. Behaves as the legacy Append() but
    // routes through the LogWriter: PUT_END is enqueued with
    // kAsync (returns immediately, durable_result resolves later); other
    // op types are enqueued with kWaitBatchDurable (blocks until the
    // containing batch is durable or fails). The returned EnqueueResult
    // carries the durable (shard, batch_id, local_index) and the
    // durable_result future. Tests use this API to verify batched
    // behavior directly. Requires IsBatchedMode() == true; returns an
    // EnqueueResult with INTERNAL_ERROR future otherwise.
    EnqueueResult AppendBatched(OpType type, const std::string& tenant_id,
                                const std::string& key,
                                const std::string& payload);

    // Append a new entry and return the assigned sequence_id.
    // This is a best-effort (async) path: the entry is buffered in memory
    // and enqueued to etcd without waiting for persistence.
    // Only suitable for idempotent, lag-tolerant operations (PUT_END).
    // For operations that MUST be durable before returning (REMOVE, etc.),
    // use AppendAndPersist() instead.
    uint64_t Append(OpType type, const std::string& key,
                    const std::string& payload = std::string());

    // Allocate a new OpLogEntry with a reserved sequence_id, append it to the
    // in-memory buffer, and return the full entry.
    //
    // IMPORTANT: This will advance last_seq_id_ even if the caller later fails
    // to persist it to etcd. This supports "seq pre-allocation" semantics where
    // retries use the same (smaller) sequence_id.
    OpLogEntry AllocateEntry(OpType type, const std::string& key,
                             const std::string& payload = std::string());

    // Persist an already-allocated entry to the store using its sequence_id.
    // Does NOT modify sequence counters.
    ErrorCode PersistEntry(const OpLogEntry& entry) const;

    // Append a new entry and durably persist it to the store (if OpLogStore is
    // set).
    //
    // This is intended for operations that may free/reuse memory (e.g. REMOVE),
    // where best-effort replication is unsafe: Standby must observe the DELETE
    // before promotion, otherwise it may return stale descriptors that point to
    // reused memory and cause silent data corruption.
    //
    // Design (updated for seq pre-allocation):
    // - sequence_id is allocated first and never reused.
    // - If etcd write fails, caller may retry PersistEntry with the same
    //   entry (sequence_id fixed and "smaller" than later entries).
    tl::expected<uint64_t, ErrorCode> AppendAndPersist(
        OpType type, const std::string& key,
        const std::string& payload = std::string());

    // NEW: tenant-aware overloads (4-param, NO default payload)
    uint64_t Append(OpType type, const std::string& tenant_id,
                    const std::string& key, const std::string& payload);
    OpLogEntry AllocateEntry(OpType type, const std::string& tenant_id,
                             const std::string& key,
                             const std::string& payload);
    tl::expected<uint64_t, ErrorCode> AppendAndPersist(
        OpType type, const std::string& tenant_id, const std::string& key,
        const std::string& payload);

    // Get the latest assigned sequence id. Returns 0 if no entry exists.
    uint64_t GetLastSequenceId() const;

    // Set the initial sequence ID (used when promoting Standby to Primary).
    // This ensures the new Primary's OpLogManager continues from the correct
    // sequence_id.
    void SetInitialSequenceId(uint64_t sequence_id);

    // Current number of entries in the buffer.
    size_t GetEntryCount() const;

    // Clean up OpLog entries in etcd before a given sequence_id.
    // Delegates to EtcdOpLogStore::CleanupOpLogBefore.
    ErrorCode CleanupOpLogBefore(uint64_t before_sequence_id);

    // Verify checksum of an OpLogEntry payload.
    // Returns true if checksum matches, false otherwise.
    // This is public so OpLogReplicator and OpLogApplier can validate entries.
    static bool VerifyChecksum(const OpLogEntry& entry);

    // Basic DoS protection for externally sourced OpLog entries (etcd watch /
    // reads). Enforce conservative bounds on key/payload sizes before
    // parsing/applying.
    static constexpr size_t kMaxObjectKeySize = 4096;            // 4 KiB
    static constexpr size_t kMaxPayloadSize = 10 * 1024 * 1024;  // 10 MiB

    // Validate OpLogEntry key/payload sizes. If invalid, returns false and
    // optionally sets a human-readable reason.
    static bool ValidateEntrySize(const OpLogEntry& entry,
                                  std::string* reason = nullptr);

   private:
    static uint64_t NowMs();
    static uint32_t ComputeChecksum(const std::string& data);
    static uint32_t ComputePrefixHash(const std::string& key);

    mutable std::shared_mutex mutex_;
    std::deque<OpLogEntry> buffer_;
    uint64_t first_seq_id_{1};  // sequence_id of buffer_.front()
    uint64_t last_seq_id_{0};   // last assigned sequence_id

    // Note: We removed key_sequence_map_ and key_remove_time_map_.
    // Global sequence_id is sufficient for ordering guarantee.
    // All operations are applied in sequence_id order, which ensures
    // consistency.

    // Optional OpLog store for persistent storage
    std::shared_ptr<OpLogStore> oplog_store_;

    // Stage 4: optional LogWriter that drives the batched write pipeline.
    // When non-null, Append/AppendAndPersist route through it instead of
    // oplog_store_->WriteOpLog.
    std::shared_ptr<OpLogLogWriter> log_writer_;

    // Simple bounds to avoid unbounded memory growth.
    static constexpr size_t kMaxBufferEntries_ = 100000;
};

}  // namespace mooncake
